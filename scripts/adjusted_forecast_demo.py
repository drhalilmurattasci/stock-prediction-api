"""Fail-closed local seal-and-serve proof for one adjusted MSFT forecast.

Planning is read-only and content-addressed.  Execution revalidates that plan,
runs exactly one least-privilege adjusted snapshot sealer, then proves the
authenticated, idempotent POST route and its durable forecast-run archive.
This controller never loads or forwards a vendor credential.
"""

from __future__ import annotations

import argparse
import asyncio
import hmac
import json
import math
import subprocess
import sys
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Protocol, cast
from uuid import UUID

import httpx
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.config import Settings
from app.core.logging import configure_logging
from app.core.security import API_KEY_HEADER
from app.db.session import build_engine, build_sessionmaker
from app.schemas.common import DISCLAIMER
from app.schemas.forecast import DataSourceLineage, ForecastRequest, ForecastResponse
from app.services.adjusted_forecast_snapshot_builder import (
    ADJUSTED_AVAILABILITY_RULE_SET_HASH,
    ADJUSTED_RESOLUTION_POLICY_HASH,
    DEFAULT_ADJUSTED_SNAPSHOT_BUILD_POLICY,
)
from app.services.forecast_run_store import ArchivedForecastRun, SqlForecastRunStore
from app.services.forecast_runs import (
    RUN_SCHEMA_VERSION,
    canonical_output,
    canonical_request,
    idempotency_digest,
    output_hash,
    parse_output,
    request_hash,
)
from app.services.forecast_serving import SqlForecastInputSnapshotRepository
from app.services.forecast_snapshots import (
    ForecastInputSnapshotRecord,
    validate_and_resolve_snapshot,
)
from app.services.market_calendar import latest_completed_xnys_session
from ingestion.locks import exclusive_vendor_operation
from ingestion.tasks.seal_adjusted_forecast_snapshot import (
    AUTHORIZATION_SENTINEL as INNER_AUTHORIZATION_SENTINEL,
)
from ml.models.baselines import NaiveForecaster
from scripts.adjusted_forecast_plan import (
    API_ORIGIN,
    API_PATH,
    FORECAST_HORIZON,
    FORECAST_HORIZON_UNIT,
    FORECAST_INTERVAL_COVERAGES,
    FORECAST_MODEL,
    FORECAST_TARGET,
    ActionCollectionReceiptBinding,
    AdjustedForecastPlanRefused,
    AdjustedForecastSealPlan,
    _configured_api_keys,
    _get_plan_settings,
    _safe_settings,
    plan_adjusted_forecast_seal,
)
from scripts.forecast_demo import (
    _API_IMAGE_OVERRIDE_ENV,
    _BUILDER_IMAGE_OVERRIDE_ENV,
    _PLAN_ID_PATTERN,
    HTTP_TIMEOUT_SECONDS,
    SNAPSHOT_CONTAINER_TIMEOUT_SECONDS,
    ForecastDemoRefused,
    HttpGet,
    HttpResult,
    RuntimeImageAttestation,
    _absent_snapshot_id,
    _assert_no_ambient_vendor_environment,
    _attest_runtime_images,
    _cleanup_one_shot_container,
    _compose_command,
    _default_http_get,
    _image_revision,
    _revalidate_api_container,
    _run_docker,
    _sanitized_subprocess_environment,
    _strict_integer,
    _validate_local_docker,
    _wait_for_api,
    _wrong_api_key,
)
from scripts.vendor_backfill import (
    BACKFILL_MULTIPLIER,
    BACKFILL_SYMBOL,
    BACKFILL_TIMESPAN,
    REQUIRED_SESSIONS,
)

AUTHORIZATION_SENTINEL = "stockapi-msft-adjusted-seal-serve-only"
SNAPSHOT_ONE_SHOT_MODULE = "ingestion.tasks.seal_adjusted_forecast_snapshot"
FORECAST_SERIES_BASIS = "split_dividend_adjusted"
FORECAST_COVERAGE = FORECAST_INTERVAL_COVERAGES[0]

HttpPost = Callable[
    [str, dict[str, object], str | None, str | None],
    Awaitable[HttpResult],
]
AdjustedPlanner = Callable[..., Awaitable[AdjustedForecastSealPlan]]
SnapshotSealer = Callable[
    [AdjustedForecastSealPlan, RuntimeImageAttestation],
    Awaitable[dict[str, object]],
]
RuntimeAttestor = Callable[[str], RuntimeImageAttestation]
RuntimeRevalidator = Callable[[RuntimeImageAttestation], None]
LockFn = Callable[[Settings], AbstractAsyncContextManager[None]]


class _StopAfterUnknownSeal(RuntimeError):
    """Internal control flow used only to release the outer mutation lock."""


@dataclass(frozen=True, slots=True)
class AdjustedSealReceipt:
    """Strict result emitted by the isolated builder-role process."""

    end_session: date
    coverage_start: date
    coverage_end: date
    factor_cutoff: datetime
    factor_set_id: str
    factor_set_recorded_at: datetime
    factor_available_at: datetime
    factor_input_count: int
    snapshot_as_of: datetime
    snapshot_id: str
    snapshot_status: str
    snapshot_availability_checked_at: datetime
    snapshot_observation_count: int
    snapshot_target_time_count: int

    def public_result(self) -> dict[str, object]:
        return {
            "symbol": BACKFILL_SYMBOL,
            "end_session": self.end_session.isoformat(),
            "coverage_start": self.coverage_start.isoformat(),
            "coverage_end": self.coverage_end.isoformat(),
            "factor_cutoff": _timestamp(self.factor_cutoff),
            "factor_set_id": self.factor_set_id,
            "factor_set_recorded_at": _timestamp(self.factor_set_recorded_at),
            "factor_available_at": _timestamp(self.factor_available_at),
            "factor_input_count": self.factor_input_count,
            "snapshot_as_of": _timestamp(self.snapshot_as_of),
            "snapshot_id": self.snapshot_id,
            "snapshot_status": self.snapshot_status,
            "snapshot_availability_checked_at": _timestamp(self.snapshot_availability_checked_at),
            "snapshot_observation_count": self.snapshot_observation_count,
            "snapshot_target_time_count": self.snapshot_target_time_count,
            "resolution_policy_hash": ADJUSTED_RESOLUTION_POLICY_HASH,
            "availability_rule_set_hash": ADJUSTED_AVAILABILITY_RULE_SET_HASH,
        }


@dataclass(frozen=True, slots=True)
class ValidatedAdjustedSnapshotEvidence:
    """Forecast facts independently reconstructed from the sealed bytes."""

    target_times: tuple[datetime, ...]
    data_sources: tuple[DataSourceLineage, ...]
    max_available_at: datetime
    expected_points: tuple[float, ...]
    expected_quantiles: tuple[tuple[float, tuple[float, ...]], ...]


class AdjustedForecastDemoStore(Protocol):
    """Runtime-role reads used only after the privileged sealer exits."""

    async def database_now(self) -> datetime: ...

    async def get_snapshot(self, snapshot_id: str) -> ForecastInputSnapshotRecord | None: ...

    async def read_archived_run(
        self,
        forecast_id: UUID,
        request: ForecastRequest,
    ) -> ArchivedForecastRun: ...


StoreFactory = Callable[
    [Settings],
    AbstractAsyncContextManager[AdjustedForecastDemoStore],
]


class SqlAdjustedForecastDemoStore:
    """Read adjusted snapshots and archived API runs as ``stockapi_app``."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._engine: AsyncEngine = build_engine(settings)
        self._maker: async_sessionmaker[AsyncSession] = build_sessionmaker(self._engine)
        self._repository = SqlForecastInputSnapshotRepository(
            self._maker,
            trusted_availability_rule_set_hash=ADJUSTED_AVAILABILITY_RULE_SET_HASH,
        )
        self._run_store = SqlForecastRunStore(
            sessionmaker=self._maker,
            identity_secret=settings.jwt_secret,
            resolution_policy_hash=ADJUSTED_RESOLUTION_POLICY_HASH,
            availability_rule_set_hash=ADJUSTED_AVAILABILITY_RULE_SET_HASH,
        )

    async def __aenter__(self) -> SqlAdjustedForecastDemoStore:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self._engine.dispose()

    async def database_now(self) -> datetime:
        async with self._maker() as session:
            value = (await session.execute(select(func.clock_timestamp()))).scalar_one()
        return _aware(value, "database clock")

    async def get_snapshot(self, snapshot_id: str) -> ForecastInputSnapshotRecord | None:
        return await self._repository.get(snapshot_id)

    async def read_archived_run(
        self,
        forecast_id: UUID,
        request: ForecastRequest,
    ) -> ArchivedForecastRun:
        return await self._run_store.read_validated(
            forecast_id,
            expected_request=request,
            expected_origin_kind="api",
        )


@asynccontextmanager
async def _sql_store(settings: Settings) -> AsyncIterator[AdjustedForecastDemoStore]:
    async with SqlAdjustedForecastDemoStore(settings) as store:
        yield store


def _aware(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ForecastDemoRefused(f"{label} must be timezone-aware")
    return value.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    return _aware(value, "timestamp").isoformat()


def _parse_timestamp(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise ForecastDemoRefused(f"adjusted sealer {label} is malformed")
    try:
        return _aware(datetime.fromisoformat(value.replace("Z", "+00:00")), label)
    except (TypeError, ValueError):
        raise ForecastDemoRefused(f"adjusted sealer {label} is malformed") from None


def _parse_date(value: object, label: str) -> date:
    if not isinstance(value, str):
        raise ForecastDemoRefused(f"adjusted sealer {label} is malformed")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise ForecastDemoRefused(f"adjusted sealer {label} is malformed") from None


def _content_id(value: object, label: str) -> str:
    if not isinstance(value, str) or _PLAN_ID_PATTERN.fullmatch(value) is None:
        raise ForecastDemoRefused(f"adjusted sealer {label} is malformed")
    return value


def _request(snapshot_id: str) -> ForecastRequest:
    return ForecastRequest(
        symbol=BACKFILL_SYMBOL,
        horizon=FORECAST_HORIZON,
        horizon_unit=FORECAST_HORIZON_UNIT,
        target=FORECAST_TARGET,
        snapshot_id=snapshot_id,
        model=FORECAST_MODEL,
        interval_coverages=list(FORECAST_INTERVAL_COVERAGES),
    )


def _request_body(request: ForecastRequest) -> dict[str, object]:
    return cast(
        dict[str, object],
        request.model_dump(mode="json", exclude_none=True),
    )


async def _default_http_post(
    path: str,
    body: dict[str, object],
    api_key: str | None,
    idempotency_key: str | None,
) -> HttpResult:
    headers: dict[str, str] = {}
    if api_key is not None:
        headers[API_KEY_HEADER] = api_key
    if idempotency_key is not None:
        headers["Idempotency-Key"] = idempotency_key
    async with httpx.AsyncClient(
        base_url=API_ORIGIN,
        timeout=HTTP_TIMEOUT_SECONDS,
        trust_env=False,
    ) as client:
        response = await client.post(path, json=body, headers=headers)
    return HttpResult(
        status_code=response.status_code,
        content=response.content,
        authenticate_header=response.headers.get("WWW-Authenticate"),
    )


async def _seal_adjusted_snapshot_once(
    plan: AdjustedForecastSealPlan,
    attestation: RuntimeImageAttestation,
) -> dict[str, object]:
    """Run exactly one attested builder container with the inner sentinel."""

    if (
        _PLAN_ID_PATTERN.fullmatch(plan.plan_id) is None
        or plan.factor_cutoff is None
        or plan.expected_factor_set_id is None
    ):
        raise ForecastDemoRefused("one-shot adjusted sealer requires the exact ready plan")
    environment = _sanitized_subprocess_environment()
    environment[_API_IMAGE_OVERRIDE_ENV] = attestation.api_image_id
    environment[_BUILDER_IMAGE_OVERRIDE_ENV] = attestation.builder_image_id
    _validate_local_docker(environment)
    if not hmac.compare_digest(
        _image_revision(attestation.builder_image_id, environment),
        attestation.tool_revision,
    ):
        raise ForecastDemoRefused("the attested builder image changed before adjusted seal")
    container_name = "stockapi-adjusted-forecast-demo-" + plan.plan_id.removeprefix("sha256:")[:16]
    existing = _run_docker(("inspect", container_name), environment=environment)
    if existing.returncode == 0:
        raise ForecastDemoRefused("a prior one-shot adjusted sealer container still exists")
    command = [
        *_compose_command(),
        "run",
        "--pull",
        "never",
        "--rm",
        "--no-deps",
        "--name",
        container_name,
        "--label",
        f"stockapi.forecast-demo.plan-id={plan.plan_id}",
        "snapshot-builder",
        "python",
        "-m",
        SNAPSHOT_ONE_SHOT_MODULE,
        "--factor-cutoff",
        _timestamp(plan.factor_cutoff),
        "--end",
        plan.end_session.isoformat(),
        "--expected-factor-set-id",
        plan.expected_factor_set_id,
        "--tool-revision",
        attestation.tool_revision,
        "--authorization",
        INNER_AUTHORIZATION_SENTINEL,
    ]

    def _run() -> subprocess.CompletedProcess[str]:
        return _run_docker(
            command,
            environment=environment,
            timeout=SNAPSHOT_CONTAINER_TIMEOUT_SECONDS,
        )

    try:
        try:
            completed = await asyncio.to_thread(_run)
        except (OSError, subprocess.SubprocessError):
            raise ForecastDemoRefused("the one-shot adjusted sealer could not run") from None
        if completed.returncode != 0:
            raise ForecastDemoRefused("the one-shot adjusted sealer failed")
        lines = [line for line in completed.stdout.splitlines() if line.strip()]
        if len(lines) != 1:
            raise ForecastDemoRefused("the one-shot adjusted sealer output is malformed")
        try:
            value = json.loads(lines[0])
        except (ValueError, json.JSONDecodeError):
            raise ForecastDemoRefused("the one-shot adjusted sealer output is malformed") from None
        if not isinstance(value, dict):
            raise ForecastDemoRefused("the one-shot adjusted sealer result is malformed")
        return cast(dict[str, object], value)
    finally:
        await asyncio.to_thread(
            _cleanup_one_shot_container,
            container_name,
            plan.plan_id,
            environment,
        )


def _validate_action_binding(
    binding: ActionCollectionReceiptBinding | None,
    action_type: str,
) -> ActionCollectionReceiptBinding:
    if binding is None or binding.action_type != action_type:
        raise ForecastDemoRefused("the adjusted plan lost its action receipt bindings")
    return binding


def _validated_task_result(
    result: dict[str, object],
    plan: AdjustedForecastSealPlan,
) -> AdjustedSealReceipt:
    expected_keys = {
        "status",
        "symbol",
        "end_session",
        "coverage_start",
        "coverage_end",
        "factor_cutoff",
        "factor_set_id",
        "factor_set_recorded_at",
        "factor_available_at",
        "factor_input_count",
        "snapshot_as_of",
        "snapshot_id",
        "snapshot_status",
        "snapshot_availability_checked_at",
        "snapshot_observation_count",
        "snapshot_target_time_count",
        "resolution_policy_hash",
        "availability_rule_set_hash",
    }
    if set(result) != expected_keys:
        raise ForecastDemoRefused("adjusted sealer result schema is malformed")
    if plan.factor_cutoff is None or plan.expected_factor_set_id is None:
        raise ForecastDemoRefused("the adjusted plan no longer contains factor evidence")
    receipt = AdjustedSealReceipt(
        end_session=_parse_date(result["end_session"], "end_session"),
        coverage_start=_parse_date(result["coverage_start"], "coverage_start"),
        coverage_end=_parse_date(result["coverage_end"], "coverage_end"),
        factor_cutoff=_parse_timestamp(result["factor_cutoff"], "factor_cutoff"),
        factor_set_id=_content_id(result["factor_set_id"], "factor_set_id"),
        factor_set_recorded_at=_parse_timestamp(
            result["factor_set_recorded_at"], "factor_set_recorded_at"
        ),
        factor_available_at=_parse_timestamp(result["factor_available_at"], "factor_available_at"),
        factor_input_count=_strict_integer(result["factor_input_count"], "factor input count"),
        snapshot_as_of=_parse_timestamp(result["snapshot_as_of"], "snapshot_as_of"),
        snapshot_id=_content_id(result["snapshot_id"], "snapshot_id"),
        snapshot_status=cast(str, result["snapshot_status"]),
        snapshot_availability_checked_at=_parse_timestamp(
            result["snapshot_availability_checked_at"],
            "snapshot_availability_checked_at",
        ),
        snapshot_observation_count=_strict_integer(
            result["snapshot_observation_count"], "snapshot observation count"
        ),
        snapshot_target_time_count=_strict_integer(
            result["snapshot_target_time_count"], "snapshot target-time count"
        ),
    )
    policy = DEFAULT_ADJUSTED_SNAPSHOT_BUILD_POLICY
    if (
        result["status"] != "ok"
        or result["symbol"] != BACKFILL_SYMBOL
        or receipt.end_session != plan.end_session
        or receipt.coverage_start != plan.window_start
        or receipt.coverage_end != plan.end_session
        or receipt.factor_cutoff != plan.factor_cutoff
        or receipt.factor_set_id != plan.expected_factor_set_id
        or receipt.factor_input_count != REQUIRED_SESSIONS
        or receipt.snapshot_as_of != receipt.factor_available_at
        or receipt.snapshot_status not in {"created", "replayed"}
        or not (
            receipt.factor_cutoff
            <= receipt.factor_set_recorded_at
            <= receipt.factor_available_at
            <= receipt.snapshot_availability_checked_at
        )
        or receipt.snapshot_observation_count != receipt.factor_input_count
        or receipt.snapshot_observation_count != REQUIRED_SESSIONS
        or receipt.snapshot_target_time_count != policy.target_time_count
        or result["resolution_policy_hash"] != ADJUSTED_RESOLUTION_POLICY_HASH
        or result["availability_rule_set_hash"] != ADJUSTED_AVAILABILITY_RULE_SET_HASH
    ):
        raise ForecastDemoRefused("adjusted sealer result does not match the exact plan")
    if plan.expected_factor_exists and (
        receipt.factor_set_recorded_at != plan.expected_factor_set_recorded_at
        or (
            plan.expected_factor_available_at is not None
            and receipt.factor_available_at != plan.expected_factor_available_at
        )
    ):
        raise ForecastDemoRefused("replayed factor evidence differs from the exact plan")
    return receipt


def _expected_sources(
    resolved_sources: tuple[DataSourceLineage, ...],
    *,
    plan: AdjustedForecastSealPlan,
    receipt: AdjustedSealReceipt,
) -> tuple[DataSourceLineage, ...]:
    split = _validate_action_binding(plan.split_collection_receipt, "split")
    dividend = _validate_action_binding(plan.dividend_collection_receipt, "dividend")
    if plan.raw_max_available_at is None or len(resolved_sources) != 4:
        raise ForecastDemoRefused("adjusted snapshot lineage is incomplete")
    # Snapshot canonicalization sorts sources by their complete lineage key;
    # validate the resulting order rather than the builder's insertion order.
    expected = (
        (
            "polygon_dividends",
            dividend.collection_id,
            dividend.available_at,
            ["cash_dividend"],
        ),
        ("polygon_open_close", None, plan.raw_max_available_at, ["close"]),
        ("polygon_splits", split.collection_id, split.available_at, ["split_ratio"]),
        (
            "stockapi_adjustment_factors",
            receipt.factor_set_id,
            receipt.factor_available_at,
            ["adjusted_close", "price_factor_f64"],
        ),
    )
    for source, (name, snapshot_id, available_at, fields) in zip(
        resolved_sources, expected, strict=True
    ):
        if (
            source.name != name
            or (snapshot_id is not None and source.snapshot_id != snapshot_id)
            or _PLAN_ID_PATTERN.fullmatch(source.snapshot_id) is None
            or source.max_available_at != available_at
            or source.fields != fields
        ):
            raise ForecastDemoRefused("adjusted snapshot lineage escaped the four-source contract")
    return resolved_sources


def _validate_snapshot_record(
    record: ForecastInputSnapshotRecord | None,
    *,
    plan: AdjustedForecastSealPlan,
    receipt: AdjustedSealReceipt,
) -> ValidatedAdjustedSnapshotEvidence:
    if record is None:
        raise ForecastDemoRefused("the adjusted snapshot is not readable through runtime role")
    request = _request(receipt.snapshot_id)
    resolved = validate_and_resolve_snapshot(
        record,
        request,
        expected_series_basis=FORECAST_SERIES_BASIS,
        expected_resolution_policy_hash=ADJUSTED_RESOLUTION_POLICY_HASH,
        expected_input_timespan=BACKFILL_TIMESPAN,
        expected_input_multiplier=BACKFILL_MULTIPLIER,
        trusted_availability_rule_set_hash=ADJUSTED_AVAILABILITY_RULE_SET_HASH,
    )
    policy = DEFAULT_ADJUSTED_SNAPSHOT_BUILD_POLICY
    if (
        record.snapshot_id != receipt.snapshot_id
        or record.as_of != receipt.snapshot_as_of
        or record.sealed_at != receipt.snapshot_availability_checked_at
        or record.availability_checked_at != receipt.snapshot_availability_checked_at
        or record.availability_status != "passed"
        or record.availability_rule_set_hash != ADJUSTED_AVAILABILITY_RULE_SET_HASH
        or record.observation_count != receipt.snapshot_observation_count
        or not policy.minimum_observations <= record.observation_count <= policy.observation_limit
        or record.target_time_count != receipt.snapshot_target_time_count
        or record.target_time_count != policy.target_time_count
        or record.max_available_at != receipt.factor_available_at
        or not resolved.availability_verified
        or resolved.snapshot_id != receipt.snapshot_id
        or resolved.symbol != BACKFILL_SYMBOL
        or resolved.target != FORECAST_TARGET
        or resolved.series_basis != FORECAST_SERIES_BASIS
        or resolved.as_of != receipt.snapshot_as_of
        or len(resolved.observations) != record.observation_count
        or len(resolved.target_times) != FORECAST_HORIZON
        or any(item.available_at != receipt.factor_available_at for item in resolved.observations)
    ):
        raise ForecastDemoRefused("adjusted snapshot failed independent runtime validation")
    sources = _expected_sources(resolved.data_sources, plan=plan, receipt=receipt)
    fitted = NaiveForecaster().fit([item.value for item in resolved.observations])
    points = tuple(fitted.predict(FORECAST_HORIZON))
    levels = (0.1, 0.5, 0.9)
    raw_quantiles = fitted.predict_quantiles(FORECAST_HORIZON, levels)
    maximum = max(
        [item.available_at for item in resolved.observations]
        + [item.max_available_at for item in sources]
    )
    if maximum != receipt.factor_available_at:
        raise ForecastDemoRefused("adjusted snapshot maximum availability is inconsistent")
    return ValidatedAdjustedSnapshotEvidence(
        target_times=resolved.target_times,
        data_sources=sources,
        max_available_at=maximum,
        expected_points=points,
        expected_quantiles=tuple((level, tuple(raw_quantiles[level])) for level in levels),
    )


def _parse_forecast_response(content: bytes) -> ForecastResponse:
    try:
        return ForecastResponse.model_validate_json(content)
    except (TypeError, ValueError):
        raise ForecastDemoRefused("the authenticated adjusted response is malformed") from None


def _validate_forecast_response(
    response: ForecastResponse,
    *,
    request: ForecastRequest,
    receipt: AdjustedSealReceipt,
    evidence: ValidatedAdjustedSnapshotEvidence,
    tool_revision: str,
) -> None:
    provenance = response.provenance
    calibration = response.calibration
    if (
        response.symbol != request.symbol
        or response.target != request.target
        or response.horizon != request.horizon
        or response.horizon_unit != request.horizon_unit
        or response.as_of != receipt.snapshot_as_of
        or response.currency != "USD"
        or len(response.forecasts) != FORECAST_HORIZON
        or provenance.snapshot_id != receipt.snapshot_id
        or provenance.feature_set_hash != receipt.snapshot_id
        or provenance.model_version != NaiveForecaster().model_version
        or provenance.series_basis != FORECAST_SERIES_BASIS
        or provenance.code_version != tool_revision
        or provenance.max_available_at != evidence.max_available_at
        or provenance.data_sources != list(evidence.data_sources)
        or provenance.lookahead_check.status != "passed"
        or provenance.lookahead_check.violations
        or provenance.lookahead_check.checked_at != provenance.generated_at
        or provenance.lookahead_check.max_feature_available_at != evidence.max_available_at
        or calibration.calibration_set_version != f"uncalibrated:{NaiveForecaster().model_version}"
        or calibration.method != "none"
        or calibration.sample_count != 0
        or calibration.window_start is not None
        or calibration.window_end is not None
        or calibration.by_interval
        or response.disclaimer != DISCLAIMER
    ):
        raise ForecastDemoRefused("the adjusted forecast response failed the exact contract")
    quantile_paths = dict(evidence.expected_quantiles)
    for index, step in enumerate(response.forecasts):
        if (
            step.target_time != evidence.target_times[index]
            or not math.isclose(
                step.point,
                evidence.expected_points[index],
                rel_tol=1e-12,
                abs_tol=1e-9,
            )
            or len(step.quantiles) != 3
            or len(step.intervals) != 1
        ):
            raise ForecastDemoRefused("adjusted forecast path differs from sealed evidence")
        for level, quantile in zip((0.1, 0.5, 0.9), step.quantiles, strict=True):
            if not math.isclose(quantile.level, level, abs_tol=1e-12) or not math.isclose(
                quantile.value,
                quantile_paths[level][index],
                rel_tol=1e-12,
                abs_tol=1e-9,
            ):
                raise ForecastDemoRefused("adjusted quantiles differ from deterministic baseline")
        interval = step.intervals[0]
        if (
            not math.isclose(interval.coverage, FORECAST_COVERAGE, abs_tol=1e-12)
            or not math.isclose(interval.lower_quantile, 0.1, abs_tol=1e-12)
            or not math.isclose(interval.upper_quantile, 0.9, abs_tol=1e-12)
            or not math.isclose(
                interval.lower,
                quantile_paths[0.1][index],
                rel_tol=1e-12,
                abs_tol=1e-9,
            )
            or not math.isclose(
                interval.upper,
                quantile_paths[0.9][index],
                rel_tol=1e-12,
                abs_tol=1e-9,
            )
        ):
            raise ForecastDemoRefused("adjusted interval differs from the plan-bound request")


def _validate_archive(
    archived: ArchivedForecastRun,
    *,
    response: ForecastResponse,
    request: ForecastRequest,
    api_key: str,
    idempotency_key: str,
    settings: Settings,
    receipt: AdjustedSealReceipt,
    tool_revision: str,
) -> None:
    request_payload = canonical_request(request)
    output_payload = canonical_output(response)
    expected_retry = idempotency_digest(
        principal=api_key,
        idempotency_key=idempotency_key,
        secret=settings.jwt_secret,
    )
    if (
        archived.forecast_id != response.provenance.forecast_id
        or archived.schema_version != RUN_SCHEMA_VERSION
        or archived.origin_kind != "api"
        or archived.idempotency_token_digest != expected_retry
        or archived.request_hash != request_hash(request_payload)
        or archived.output_hash != output_hash(output_payload)
        or archived.snapshot_id != receipt.snapshot_id
        or archived.resolution_policy_hash != ADJUSTED_RESOLUTION_POLICY_HASH
        or archived.availability_rule_set_hash != ADJUSTED_AVAILABILITY_RULE_SET_HASH
        or archived.symbol != BACKFILL_SYMBOL
        or archived.target != FORECAST_TARGET
        or archived.horizon != FORECAST_HORIZON
        or archived.horizon_unit != FORECAST_HORIZON_UNIT
        or archived.series_basis != FORECAST_SERIES_BASIS
        or archived.as_of != receipt.snapshot_as_of
        or archived.max_available_at != response.provenance.max_available_at
        or archived.model_version != NaiveForecaster().model_version
        or archived.feature_set_hash != receipt.snapshot_id
        or archived.code_version != tool_revision
        or archived.calibration_method != "none"
        or archived.canonical_request != request_payload
        or archived.canonical_output != output_payload
        or parse_output(archived.canonical_output) != response
    ):
        raise ForecastDemoRefused("the runtime forecast archive differs from the served response")


def _outcome_unknown(
    *,
    plan: AdjustedForecastSealPlan,
    attestation: RuntimeImageAttestation,
    phase: str,
    exc: Exception,
) -> dict[str, object]:
    return {
        "status": "seal_outcome_unknown",
        "plan_id": plan.plan_id,
        "tool_revision": plan.tool_revision,
        "symbol": BACKFILL_SYMBOL,
        "end_session": plan.end_session.isoformat(),
        "factor_cutoff": (None if plan.factor_cutoff is None else _timestamp(plan.factor_cutoff)),
        "expected_factor_set_id": plan.expected_factor_set_id,
        "builder_image_id": attestation.builder_image_id,
        "proof_phase": phase,
        "failure_type": type(exc).__name__,
    }


async def execute_adjusted_forecast_demo(
    *,
    end_session: date,
    plan_id: str,
    authorization: str,
    settings: Settings | None = None,
    store_factory: StoreFactory = _sql_store,
    http_get: HttpGet = _default_http_get,
    http_post: HttpPost = _default_http_post,
    snapshot_sealer: SnapshotSealer = _seal_adjusted_snapshot_once,
    runtime_attestor: RuntimeAttestor = _attest_runtime_images,
    runtime_revalidator: RuntimeRevalidator = _revalidate_api_container,
    lock_fn: LockFn = exclusive_vendor_operation,
    planner: AdjustedPlanner = plan_adjusted_forecast_seal,
) -> dict[str, object]:
    """Seal/replay adjusted evidence, serve twice, and verify the archive."""

    if authorization != AUTHORIZATION_SENTINEL:
        raise ForecastDemoRefused(f"authorization must be exactly {AUTHORIZATION_SENTINEL}")
    if _PLAN_ID_PATTERN.fullmatch(plan_id) is None:
        raise ForecastDemoRefused("plan_id must be a sha256 digest from plan mode")
    safe_settings = _safe_settings(settings or _get_plan_settings())
    sealed_receipt: AdjustedSealReceipt | None = None
    attestation: RuntimeImageAttestation | None = None
    pending_unknown: dict[str, object] | None = None
    proof_phase = "pre_seal"
    proof_http_status: int | None = None
    try:
        async with lock_fn(safe_settings):
            plan = await planner(end_session=end_session, settings=safe_settings)
            if plan.plan_id != plan_id:
                raise ForecastDemoRefused("database or configuration no longer matches plan_id")
            if (
                not plan.ready
                or plan.factor_cutoff is None
                or plan.expected_factor_set_id is None
                or plan.split_collection_receipt is None
                or plan.dividend_collection_receipt is None
            ):
                raise ForecastDemoRefused("the adjusted seal-and-serve plan is not ready")
            api_keys = _configured_api_keys(safe_settings)
            if len(api_keys) != 1:
                raise ForecastDemoRefused("execute requires exactly one configured API key")
            api_key = api_keys[0]
            attestation = await asyncio.to_thread(runtime_attestor, plan.tool_revision)
            await _wait_for_api(http_get)

            async with store_factory(safe_settings) as store:
                absent_id = await _absent_snapshot_id(store, plan.plan_id)  # type: ignore[arg-type]
                absent_request = _request(absent_id)
                absent_body = _request_body(absent_request)
                unauthenticated = await http_post(API_PATH, absent_body, None, None)
                if (
                    unauthenticated.status_code != 401
                    or unauthenticated.authenticate_header != API_KEY_HEADER
                ):
                    raise ForecastDemoRefused(
                        "the adjusted POST route did not enforce API-key auth"
                    )
                wrong_key = await http_post(
                    API_PATH,
                    absent_body,
                    _wrong_api_key(api_key, plan.plan_id),
                    None,
                )
                if wrong_key.status_code != 401 or wrong_key.authenticate_header != API_KEY_HEADER:
                    raise ForecastDemoRefused("the adjusted POST route accepted an invalid API key")
                missing = await http_post(API_PATH, absent_body, api_key, None)
                if missing.status_code != 404:
                    raise ForecastDemoRefused(
                        "the authenticated adjusted missing-snapshot probe did not 404"
                    )

                proof_phase = "one_shot_adjusted_seal"
                try:
                    task_result = await snapshot_sealer(plan, attestation)
                    proof_phase = "adjusted_seal_receipt_validation"
                    sealed_receipt = _validated_task_result(task_result, plan)
                except Exception as exc:  # noqa: BLE001 - outcome may have committed.
                    pending_unknown = _outcome_unknown(
                        plan=plan,
                        attestation=attestation,
                        phase=proof_phase,
                        exc=exc,
                    )
                    raise _StopAfterUnknownSeal from None

                proof_phase = "runtime_snapshot_read"
                record = await store.get_snapshot(sealed_receipt.snapshot_id)
                proof_phase = "snapshot_validation"
                evidence = _validate_snapshot_record(
                    record,
                    plan=plan,
                    receipt=sealed_receipt,
                )

                request = _request(sealed_receipt.snapshot_id)
                request_body = _request_body(request)
                proof_phase = "authenticated_forecast_request"
                served = await http_post(
                    API_PATH,
                    request_body,
                    api_key,
                    plan.idempotency_key,
                )
                proof_http_status = served.status_code
                if served.status_code != 200:
                    raise ForecastDemoRefused("the authenticated adjusted POST did not return 200")
                proof_phase = "forecast_response_parse"
                response = _parse_forecast_response(served.content)
                proof_phase = "forecast_response_validation"
                _validate_forecast_response(
                    response,
                    request=request,
                    receipt=sealed_receipt,
                    evidence=evidence,
                    tool_revision=plan.tool_revision,
                )

                proof_phase = "idempotency_replay_request"
                replay = await http_post(
                    API_PATH,
                    request_body,
                    api_key,
                    plan.idempotency_key,
                )
                proof_http_status = replay.status_code
                if replay.status_code != 200:
                    raise ForecastDemoRefused("the adjusted idempotency replay did not return 200")
                proof_phase = "idempotency_replay_validation"
                replay_response = _parse_forecast_response(replay.content)
                if (
                    replay.content != served.content
                    or replay_response != response
                    or replay_response.provenance.forecast_id != response.provenance.forecast_id
                ):
                    raise ForecastDemoRefused("same-key adjusted retry did not replay exactly")

                proof_phase = "forecast_archive_read"
                archived = await store.read_archived_run(
                    response.provenance.forecast_id,
                    request,
                )
                proof_phase = "forecast_archive_validation"
                _validate_archive(
                    archived,
                    response=response,
                    request=request,
                    api_key=api_key,
                    idempotency_key=plan.idempotency_key,
                    settings=safe_settings,
                    receipt=sealed_receipt,
                    tool_revision=plan.tool_revision,
                )

            proof_phase = "api_container_revalidation"
            await asyncio.to_thread(runtime_revalidator, attestation)
            proof_phase = "completion_database_clock"
            async with store_factory(safe_settings) as store:
                final_database_now = await store.database_now()
            session_still_current = latest_completed_xnys_session(final_database_now) == end_session
            proof_phase = "vendor_lock_release"
            return {
                "status": "ok" if session_still_current else "sealed_session_advanced",
                "plan_id": plan.plan_id,
                "tool_revision": plan.tool_revision,
                **sealed_receipt.public_result(),
                "api_image_id": attestation.api_image_id,
                "builder_image_id": attestation.builder_image_id,
                "unauthenticated_http_status": unauthenticated.status_code,
                "wrong_key_http_status": wrong_key.status_code,
                "missing_snapshot_http_status": missing.status_code,
                "authenticated_http_status": served.status_code,
                "replay_http_status": replay.status_code,
                "forecast_id": str(response.provenance.forecast_id),
                "idempotency_replay": "identical",
                "archive_status": "validated",
                "model_version": response.provenance.model_version,
                "forecast_count": len(response.forecasts),
                "lookahead_status": response.provenance.lookahead_check.status,
                "calibration_method": response.calibration.method,
                "session_currency_at_completion": (
                    "current" if session_still_current else "advanced_after_seal"
                ),
            }
    except Exception as exc:  # noqa: BLE001 - never expose credential-bearing text.
        if pending_unknown is not None:
            if isinstance(exc, _StopAfterUnknownSeal):
                return pending_unknown
            return {
                **pending_unknown,
                "seal_proof_phase": pending_unknown["proof_phase"],
                "proof_phase": "vendor_lock_release",
                "lock_release_failure_type": type(exc).__name__,
            }
        if sealed_receipt is None or attestation is None:
            raise
        return {
            "status": "sealed_proof_failed",
            "plan_id": plan_id,
            "tool_revision": attestation.tool_revision,
            **sealed_receipt.public_result(),
            "api_image_id": attestation.api_image_id,
            "builder_image_id": attestation.builder_image_id,
            "proof_phase": proof_phase,
            "failure_type": type(exc).__name__,
            "http_status": proof_http_status,
        }


def _iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError("date must be YYYY-MM-DD") from None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan = subparsers.add_parser("plan", help="read-only adjusted seal-and-serve plan")
    plan.add_argument("--end", required=True, type=_iso_date)
    execute = subparsers.add_parser("execute", help="run one exact adjusted local proof")
    execute.add_argument("--end", required=True, type=_iso_date)
    execute.add_argument("--plan-id", required=True)
    execute.add_argument("--authorization", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    configure_logging("INFO", json_logs=False, exception_details=False)
    try:
        _assert_no_ambient_vendor_environment()
        if args.command == "plan":
            result = asyncio.run(plan_adjusted_forecast_seal(end_session=args.end)).public_result()
        else:
            result = asyncio.run(
                execute_adjusted_forecast_demo(
                    end_session=args.end,
                    plan_id=args.plan_id,
                    authorization=args.authorization,
                )
            )
    except (ForecastDemoRefused, AdjustedForecastPlanRefused) as exc:
        print(f"adjusted forecast demo refused: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001 - never expose exception text.
        print(f"adjusted forecast demo failed: {type(exc).__name__}", file=sys.stderr)
        return 1
    if result.get("status") in {
        "seal_outcome_unknown",
        "sealed_proof_failed",
        "sealed_session_advanced",
    }:
        print(json.dumps(result, sort_keys=True, separators=(",", ":")), file=sys.stderr)
        return 3
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
