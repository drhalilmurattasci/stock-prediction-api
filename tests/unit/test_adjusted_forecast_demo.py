"""Focused fail-closed tests for the adjusted seal-and-serve controller."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

import httpx
import pytest

import scripts.adjusted_forecast_demo as demo
from app.config import Settings
from app.schemas.forecast import ForecastRequest, ForecastResponse
from app.services.adjusted_forecast_snapshot_builder import (
    ADJUSTED_AVAILABILITY_RULE_SET_HASH,
    ADJUSTED_RESOLUTION_POLICY_HASH,
    DEFAULT_ADJUSTED_SNAPSHOT_BUILD_POLICY,
)
from app.services.forecast_run_store import ArchivedForecastRun
from app.services.forecast_runs import (
    RUN_SCHEMA_VERSION,
    canonical_output,
    canonical_request,
    idempotency_digest,
    opportunity_hash,
    output_hash,
    request_hash,
)
from app.services.forecast_snapshots import (
    ForecastInputSnapshotPayload,
    SnapshotAvailabilityEvidence,
    SnapshotObservation,
    SnapshotSourceLineage,
    build_snapshot_record,
    validate_and_resolve_snapshot,
)
from app.services.forecasting import ForecastRunIdentity, assemble_baseline_forecast_response
from ml.models.baselines import NaiveForecaster
from scripts.adjusted_forecast_plan import (
    ActionCollectionReceiptBinding,
    AdjustedForecastSealPlan,
)

END = date(2026, 7, 13)
WINDOW_START = date(2025, 7, 7)
REVISION = "a" * 40
PLAN_ID = "sha256:" + "b" * 64
FACTOR_ID = "sha256:" + "c" * 64
RAW_SOURCE_ID = "sha256:" + "d" * 64
SPLIT_ID = "sha256:" + "e" * 64
DIVIDEND_ID = "sha256:" + "f" * 64
RAW_AVAILABLE = datetime(2026, 7, 13, 20, 8, tzinfo=UTC)
SPLIT_AVAILABLE = datetime(2026, 7, 13, 20, 9, tzinfo=UTC)
DIVIDEND_AVAILABLE = datetime(2026, 7, 13, 20, 10, tzinfo=UTC)
FACTOR_CUTOFF = DIVIDEND_AVAILABLE
FACTOR_RECORDED = datetime(2026, 7, 13, 20, 11, tzinfo=UTC)
FACTOR_AVAILABLE = datetime(2026, 7, 13, 20, 12, tzinfo=UTC)
SNAPSHOT_CHECKED = datetime(2026, 7, 13, 20, 13, tzinfo=UTC)
DATABASE_NOW = datetime(2026, 7, 13, 21, tzinfo=UTC)
FINAL_DATABASE_NOW = datetime(2026, 7, 14, 12, tzinfo=UTC)
ATTESTATION = demo.RuntimeImageAttestation(
    tool_revision=REVISION,
    api_image_id="sha256:" + "1" * 64,
    builder_image_id="sha256:" + "2" * 64,
    api_container_id="3" * 64,
)


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "app_env": "local",
        "database_url": (
            "postgresql+asyncpg://stockapi_app:test-secret@localhost:5432/stockapi_test"
        ),
        "api_keys": "local-demo-key",
        "jwt_secret": "test-jwt-binding-secret-32-characters-long",
        "forecast_adjusted_close_resolution_policy_hash": (ADJUSTED_RESOLUTION_POLICY_HASH),
        "forecast_adjusted_close_trusted_availability_rule_set_hash": (
            ADJUSTED_AVAILABILITY_RULE_SET_HASH
        ),
    }
    values.update(overrides)
    return Settings(**values)  # type: ignore[arg-type]


def _plan(
    *,
    expected_exists: bool = False,
    expected_recorded_at: datetime | None = None,
    expected_available_at: datetime | None = None,
) -> AdjustedForecastSealPlan:
    return AdjustedForecastSealPlan(
        end_session=END,
        tool_revision=REVISION,
        acquisition_plan_id="sha256:" + "9" * 64,
        window_start=WINDOW_START,
        database_now=DATABASE_NOW,
        raw_receipt_count=258,
        raw_max_available_at=RAW_AVAILABLE,
        split_collection_receipt=ActionCollectionReceiptBinding(
            action_type="split",
            collection_id=SPLIT_ID,
            collection_recorded_at=SPLIT_AVAILABLE - timedelta(seconds=1),
            available_at=SPLIT_AVAILABLE,
            event_count=0,
        ),
        dividend_collection_receipt=ActionCollectionReceiptBinding(
            action_type="dividend",
            collection_id=DIVIDEND_ID,
            collection_recorded_at=DIVIDEND_AVAILABLE - timedelta(seconds=1),
            available_at=DIVIDEND_AVAILABLE,
            event_count=4,
        ),
        factor_cutoff=FACTOR_CUTOFF,
        expected_factor_set_id=FACTOR_ID,
        expected_factor_exists=expected_exists,
        expected_factor_set_recorded_at=expected_recorded_at,
        expected_factor_available_at=expected_available_at,
        incompatible_factor_set_ids=(),
        api_key_count=1,
        resolution_pin_matches=True,
        availability_pin_matches=True,
        blockers=(),
        plan_id=PLAN_ID,
    )


def _task_result(*, snapshot_id: str, snapshot_status: str = "created") -> dict[str, object]:
    return {
        "status": "ok",
        "symbol": "MSFT",
        "end_session": END.isoformat(),
        "coverage_start": WINDOW_START.isoformat(),
        "coverage_end": END.isoformat(),
        "factor_cutoff": FACTOR_CUTOFF.isoformat(),
        "factor_set_id": FACTOR_ID,
        "factor_set_recorded_at": FACTOR_RECORDED.isoformat(),
        "factor_available_at": FACTOR_AVAILABLE.isoformat(),
        "factor_input_count": 258,
        "snapshot_as_of": FACTOR_AVAILABLE.isoformat(),
        "snapshot_id": snapshot_id,
        "snapshot_status": snapshot_status,
        "snapshot_availability_checked_at": SNAPSHOT_CHECKED.isoformat(),
        "snapshot_observation_count": 258,
        "snapshot_target_time_count": (DEFAULT_ADJUSTED_SNAPSHOT_BUILD_POLICY.target_time_count),
        "resolution_policy_hash": ADJUSTED_RESOLUTION_POLICY_HASH,
        "availability_rule_set_hash": ADJUSTED_AVAILABILITY_RULE_SET_HASH,
    }


def _snapshot_record():
    observations = tuple(
        SnapshotObservation(
            observed_at=datetime(2025, 10, 29, 20, tzinfo=UTC) + timedelta(days=index),
            available_at=FACTOR_AVAILABLE,
            value=100.0 + (index * index) / 1000,
        )
        for index in range(258)
    )
    target_times = tuple(
        datetime(2026, 7, 14, 20, tzinfo=UTC) + timedelta(days=index)
        for index in range(DEFAULT_ADJUSTED_SNAPSHOT_BUILD_POLICY.target_time_count)
    )
    # Deliberately use builder insertion order; canonicalization must reorder it.
    payload = ForecastInputSnapshotPayload(
        resolution_policy_hash=ADJUSTED_RESOLUTION_POLICY_HASH,
        symbol="MSFT",
        target="adjusted_close",
        horizon_unit="trading_day",
        series_basis="split_dividend_adjusted",
        input_timespan="day",
        input_multiplier=1,
        as_of=FACTOR_AVAILABLE,
        currency="USD",
        observations=observations,
        target_times=target_times,
        data_sources=(
            SnapshotSourceLineage(
                name="polygon_open_close",
                snapshot_id=RAW_SOURCE_ID,
                max_available_at=RAW_AVAILABLE,
                fields=("close",),
            ),
            SnapshotSourceLineage(
                name="polygon_splits",
                snapshot_id=SPLIT_ID,
                max_available_at=SPLIT_AVAILABLE,
                fields=("split_ratio",),
            ),
            SnapshotSourceLineage(
                name="polygon_dividends",
                snapshot_id=DIVIDEND_ID,
                max_available_at=DIVIDEND_AVAILABLE,
                fields=("cash_dividend",),
            ),
            SnapshotSourceLineage(
                name="stockapi_adjustment_factors",
                snapshot_id=FACTOR_ID,
                max_available_at=FACTOR_AVAILABLE,
                fields=("adjusted_close", "price_factor_f64"),
            ),
        ),
        availability=SnapshotAvailabilityEvidence(
            status="passed",
            rule_set_hash=ADJUSTED_AVAILABILITY_RULE_SET_HASH,
            checked_at=SNAPSHOT_CHECKED,
        ),
    )
    return build_snapshot_record(payload, sealed_at=SNAPSHOT_CHECKED)


def test_task_result_is_exact_and_allows_partial_factor_receipt_recovery() -> None:
    record = _snapshot_record()
    result = _task_result(snapshot_id=record.snapshot_id)

    receipt = demo._validated_task_result(result, _plan())
    assert receipt.factor_set_id == FACTOR_ID
    assert receipt.snapshot_id == record.snapshot_id

    # The factor row existed at plan time but had no availability receipt.  A
    # successful sealer must preserve its row timestamp and may add the receipt.
    partial = _plan(
        expected_exists=True,
        expected_recorded_at=FACTOR_RECORDED,
        expected_available_at=None,
    )
    assert demo._validated_task_result(result, partial).factor_available_at == FACTOR_AVAILABLE

    wrong_recorded = dict(
        result,
        factor_set_recorded_at=(FACTOR_CUTOFF + timedelta(seconds=30)).isoformat(),
    )
    with pytest.raises(demo.ForecastDemoRefused, match="replayed factor evidence"):
        demo._validated_task_result(wrong_recorded, partial)

    complete = replace(partial, expected_factor_available_at=FACTOR_AVAILABLE)
    wrong_available = dict(
        result,
        factor_available_at=SNAPSHOT_CHECKED.isoformat(),
        snapshot_as_of=SNAPSHOT_CHECKED.isoformat(),
    )
    with pytest.raises(demo.ForecastDemoRefused, match="replayed factor evidence"):
        demo._validated_task_result(wrong_available, complete)


def test_task_result_rejects_schema_drift_and_boolean_counts() -> None:
    record = _snapshot_record()
    result = _task_result(snapshot_id=record.snapshot_id)
    result["factor_recorded_at"] = result.pop("factor_set_recorded_at")
    with pytest.raises(demo.ForecastDemoRefused, match="schema"):
        demo._validated_task_result(result, _plan())

    malformed = _task_result(snapshot_id=record.snapshot_id)
    malformed["factor_input_count"] = True
    with pytest.raises(demo.ForecastDemoRefused, match="factor input count"):
        demo._validated_task_result(malformed, _plan())

    oversized = _task_result(snapshot_id=record.snapshot_id)
    oversized["snapshot_observation_count"] = 259
    with pytest.raises(demo.ForecastDemoRefused, match="exact plan"):
        demo._validated_task_result(oversized, _plan())


def test_runtime_validation_uses_canonical_four_source_order() -> None:
    record = _snapshot_record()
    receipt = demo._validated_task_result(
        _task_result(snapshot_id=record.snapshot_id),
        _plan(),
    )

    evidence = demo._validate_snapshot_record(record, plan=_plan(), receipt=receipt)

    assert [source.name for source in evidence.data_sources] == [
        "polygon_dividends",
        "polygon_open_close",
        "polygon_splits",
        "stockapi_adjustment_factors",
    ]
    assert evidence.max_available_at == FACTOR_AVAILABLE
    assert evidence.data_sources[-1].fields == [
        "adjusted_close",
        "price_factor_f64",
    ]


def test_response_binds_lookahead_check_to_archive_completion_time() -> None:
    record = _snapshot_record()
    plan = _plan()
    receipt = demo._validated_task_result(
        _task_result(snapshot_id=record.snapshot_id),
        plan,
    )
    evidence = demo._validate_snapshot_record(record, plan=plan, receipt=receipt)
    response = _response(record)
    demo._validate_forecast_response(
        response,
        request=demo._request(record.snapshot_id),
        receipt=receipt,
        evidence=evidence,
        tool_revision=REVISION,
    )

    provenance = response.provenance.model_copy(
        update={
            "lookahead_check": response.provenance.lookahead_check.model_copy(
                update={"checked_at": response.provenance.generated_at + timedelta(microseconds=1)}
            )
        }
    )
    mismatched = response.model_copy(update={"provenance": provenance})
    with pytest.raises(demo.ForecastDemoRefused, match="exact contract"):
        demo._validate_forecast_response(
            mismatched,
            request=demo._request(record.snapshot_id),
            receipt=receipt,
            evidence=evidence,
            tool_revision=REVISION,
        )


def _response(record) -> ForecastResponse:
    request = demo._request(record.snapshot_id)
    resolved = validate_and_resolve_snapshot(
        record,
        request,
        expected_series_basis=demo.FORECAST_SERIES_BASIS,
        expected_resolution_policy_hash=ADJUSTED_RESOLUTION_POLICY_HASH,
        expected_input_timespan="day",
        expected_input_multiplier=1,
        trusted_availability_rule_set_hash=ADJUSTED_AVAILABILITY_RULE_SET_HASH,
    )
    return assemble_baseline_forecast_response(
        request,
        resolved,
        forecaster_factory=NaiveForecaster,
        identity=ForecastRunIdentity(
            forecast_id=UUID("12345678-1234-5678-1234-567812345678"),
            generated_at=SNAPSHOT_CHECKED + timedelta(seconds=1),
            model_version=NaiveForecaster().model_version,
            feature_set_hash=record.snapshot_id,
            code_version=REVISION,
        ),
    )


def _archive(
    request: ForecastRequest,
    response: ForecastResponse,
    settings: Settings,
    idempotency_key: str,
) -> ArchivedForecastRun:
    request_payload = canonical_request(request)
    output_payload = canonical_output(response)
    return ArchivedForecastRun(
        forecast_id=response.provenance.forecast_id,
        schema_version=RUN_SCHEMA_VERSION,
        origin_kind="api",
        idempotency_token_digest=idempotency_digest(
            principal="local-demo-key",
            idempotency_key=idempotency_key,
            secret=settings.jwt_secret,
        ),
        request_hash=request_hash(request_payload),
        opportunity_hash=opportunity_hash(
            response,
            resolution_policy_hash=ADJUSTED_RESOLUTION_POLICY_HASH,
            availability_rule_set_hash=ADJUSTED_AVAILABILITY_RULE_SET_HASH,
            origin_kind="api",
        ),
        output_hash=output_hash(output_payload),
        snapshot_id=response.provenance.snapshot_id,
        resolution_policy_hash=ADJUSTED_RESOLUTION_POLICY_HASH,
        availability_rule_set_hash=ADJUSTED_AVAILABILITY_RULE_SET_HASH,
        symbol=response.symbol,
        target=response.target,
        horizon=response.horizon,
        horizon_unit=response.horizon_unit,
        series_basis=response.provenance.series_basis,
        as_of=response.as_of,
        max_available_at=response.provenance.max_available_at,
        model_version=response.provenance.model_version,
        feature_set_hash=response.provenance.feature_set_hash,
        code_version=response.provenance.code_version,
        calibration_set_version=response.calibration.calibration_set_version,
        calibration_method=response.calibration.method,
        generated_at=response.provenance.generated_at,
        recorded_at=response.provenance.generated_at + timedelta(microseconds=1),
        canonical_request=request_payload,
        canonical_output=output_payload,
    )


class FakeStore:
    def __init__(self, record, archived: ArchivedForecastRun | None = None) -> None:
        self.record = record
        self.archived = archived

    async def database_now(self) -> datetime:
        return FINAL_DATABASE_NOW

    async def get_snapshot(self, snapshot_id: str):
        if self.record is not None and snapshot_id == self.record.snapshot_id:
            return self.record
        return None

    async def read_archived_run(
        self,
        forecast_id: UUID,
        request: ForecastRequest,
    ) -> ArchivedForecastRun:
        assert self.archived is not None
        assert forecast_id == self.archived.forecast_id
        assert canonical_request(request) == self.archived.canonical_request
        return self.archived


def _store_factory(store: FakeStore):
    @asynccontextmanager
    async def factory(settings: Settings) -> AsyncIterator[FakeStore]:
        del settings
        yield store

    return factory


@asynccontextmanager
async def _no_lock(settings: Settings) -> AsyncIterator[None]:
    del settings
    yield


def _runtime_attestor(tool_revision: str) -> demo.RuntimeImageAttestation:
    assert tool_revision == REVISION
    return ATTESTATION


def _runtime_revalidator(attestation: demo.RuntimeImageAttestation) -> None:
    assert attestation == ATTESTATION


@pytest.mark.asyncio
async def test_one_shot_translates_outer_authorization_to_exact_inner_sentinel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record = _snapshot_record()
    calls: list[tuple[str, ...]] = []

    def run_docker(arguments: object, **kwargs: object):
        del kwargs
        call: tuple[str, ...] = tuple(arguments)  # type: ignore[arg-type]
        calls.append(call)
        if call[0] == "inspect":
            return SimpleNamespace(returncode=1, stdout="")
        return SimpleNamespace(
            returncode=0,
            stdout=demo.json.dumps(
                _task_result(snapshot_id=record.snapshot_id),
                separators=(",", ":"),
            ),
        )

    monkeypatch.setattr(demo, "_run_docker", run_docker)
    monkeypatch.setattr(demo, "_compose_command", lambda: ("compose",))
    monkeypatch.setattr(demo, "_validate_local_docker", lambda environment: None)
    monkeypatch.setattr(demo, "_image_revision", lambda image_id, environment: REVISION)
    monkeypatch.setattr(demo, "_cleanup_one_shot_container", lambda *args: None)

    result = await demo._seal_adjusted_snapshot_once(_plan(), ATTESTATION)

    assert result["factor_set_id"] == FACTOR_ID
    command = next(call for call in calls if call[0] == "compose")
    assert command[command.index("--authorization") + 1] == (demo.INNER_AUTHORIZATION_SENTINEL)
    assert demo.AUTHORIZATION_SENTINEL not in command
    assert command[command.index("--expected-factor-set-id") + 1] == FACTOR_ID
    assert command[command.index("--factor-cutoff") + 1] == FACTOR_CUTOFF.isoformat()
    assert command[command.index("run") : command.index("run") + 3] == (
        "run",
        "--pull",
        "never",
    )


@pytest.mark.asyncio
async def test_loopback_post_disables_proxy_and_binds_retry_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://proxy.invalid:9999")
    captured: dict[str, object] = {}

    class FakeClient:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc_info: object) -> None:
            return None

        async def post(self, *args: object, **kwargs: object):
            del args
            captured.update(kwargs)
            return httpx.Response(200, content=b"ok")

    monkeypatch.setattr(demo.httpx, "AsyncClient", FakeClient)
    result = await demo._default_http_post(
        demo.API_PATH,
        {"symbol": "MSFT"},
        "local-demo-key",
        "retry-key",
    )

    assert result.status_code == 200
    assert captured["trust_env"] is False
    assert captured["headers"] == {
        "X-API-Key": "local-demo-key",
        "Idempotency-Key": "retry-key",
    }


@pytest.mark.asyncio
async def test_execute_proves_auth_same_key_replay_and_runtime_archive() -> None:
    plan = _plan()
    settings = _settings()
    record = _snapshot_record()
    response = _response(record)
    response_bytes = response.model_dump_json().encode("utf-8")
    request = demo._request(record.snapshot_id)
    archived = _archive(request, response, settings, plan.idempotency_key)
    store = FakeStore(record, archived)
    events: list[str] = []
    sealed = False

    async def planner(**kwargs: object) -> AdjustedForecastSealPlan:
        del kwargs
        return plan

    async def http_get(*args: object, **kwargs: object) -> demo.HttpResult:
        del args, kwargs
        events.append("health")
        return demo.HttpResult(200, b"")

    async def http_post(
        path: str,
        body: dict[str, object],
        api_key: str | None,
        idempotency_key: str | None,
    ) -> demo.HttpResult:
        assert path == demo.API_PATH
        if api_key is None:
            events.append("unauthenticated")
            return demo.HttpResult(401, b"", "X-API-Key")
        if api_key != "local-demo-key":
            events.append("wrong-key")
            return demo.HttpResult(401, b"", "X-API-Key")
        if body["snapshot_id"] != record.snapshot_id:
            events.append("missing")
            assert idempotency_key is None
            return demo.HttpResult(404, b"")
        assert sealed is True
        assert body == demo._request_body(request)
        assert idempotency_key == plan.idempotency_key
        events.append("served" if events.count("served") == 0 else "replayed")
        return demo.HttpResult(200, response_bytes)

    async def sealer(
        actual_plan: AdjustedForecastSealPlan,
        attestation: demo.RuntimeImageAttestation,
    ) -> dict[str, object]:
        nonlocal sealed
        assert actual_plan == plan
        assert attestation == ATTESTATION
        events.append("seal")
        sealed = True
        return _task_result(snapshot_id=record.snapshot_id)

    def revalidate(attestation: demo.RuntimeImageAttestation) -> None:
        _runtime_revalidator(attestation)
        events.append("revalidated")

    result = await demo.execute_adjusted_forecast_demo(
        end_session=END,
        plan_id=plan.plan_id,
        authorization=demo.AUTHORIZATION_SENTINEL,
        settings=settings,
        store_factory=_store_factory(store),
        http_get=http_get,
        http_post=http_post,
        snapshot_sealer=sealer,
        runtime_attestor=_runtime_attestor,
        runtime_revalidator=revalidate,
        lock_fn=_no_lock,
        planner=planner,
    )

    assert events == [
        "health",
        "unauthenticated",
        "wrong-key",
        "missing",
        "seal",
        "served",
        "replayed",
        "revalidated",
    ]
    assert result["status"] == "ok"
    assert result["idempotency_replay"] == "identical"
    assert result["archive_status"] == "validated"
    assert result["forecast_id"] == str(response.provenance.forecast_id)
    assert "local-demo-key" not in str(result)


def _probe_http_post(events: list[str]):
    async def http_post(
        path: str,
        body: dict[str, object],
        api_key: str | None,
        idempotency_key: str | None,
    ) -> demo.HttpResult:
        del path, body, idempotency_key
        if api_key is None:
            events.append("unauthenticated")
            return demo.HttpResult(401, b"", "X-API-Key")
        if api_key != "local-demo-key":
            events.append("wrong-key")
            return demo.HttpResult(401, b"", "X-API-Key")
        events.append("missing")
        return demo.HttpResult(404, b"")

    return http_post


async def _healthy_get(*args: object, **kwargs: object) -> demo.HttpResult:
    del args, kwargs
    return demo.HttpResult(200, b"")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "phase", "failure_type"),
    [
        ("raise", "one_shot_adjusted_seal", "RuntimeError"),
        (
            "malformed_receipt",
            "adjusted_seal_receipt_validation",
            "ForecastDemoRefused",
        ),
    ],
)
async def test_one_shot_or_receipt_failure_reports_sanitized_unknown_outcome(
    mode: str,
    phase: str,
    failure_type: str,
) -> None:
    plan = _plan()
    events: list[str] = []

    async def planner(**kwargs: object) -> AdjustedForecastSealPlan:
        del kwargs
        return plan

    async def sealer(
        actual_plan: AdjustedForecastSealPlan,
        attestation: demo.RuntimeImageAttestation,
    ) -> dict[str, object]:
        assert actual_plan == plan
        assert attestation == ATTESTATION
        if mode == "raise":
            raise RuntimeError("secret-canary")
        return {"status": "secret-canary"}

    result = await demo.execute_adjusted_forecast_demo(
        end_session=END,
        plan_id=plan.plan_id,
        authorization=demo.AUTHORIZATION_SENTINEL,
        settings=_settings(),
        store_factory=_store_factory(FakeStore(None)),
        http_get=_healthy_get,
        http_post=_probe_http_post(events),
        snapshot_sealer=sealer,
        runtime_attestor=_runtime_attestor,
        runtime_revalidator=_runtime_revalidator,
        lock_fn=_no_lock,
        planner=planner,
    )

    assert events == ["unauthenticated", "wrong-key", "missing"]
    assert result == {
        "status": "seal_outcome_unknown",
        "plan_id": PLAN_ID,
        "tool_revision": REVISION,
        "symbol": "MSFT",
        "end_session": END.isoformat(),
        "factor_cutoff": FACTOR_CUTOFF.isoformat(),
        "expected_factor_set_id": FACTOR_ID,
        "builder_image_id": ATTESTATION.builder_image_id,
        "proof_phase": phase,
        "failure_type": failure_type,
    }
    assert "secret-canary" not in str(result)


@pytest.mark.asyncio
async def test_unknown_seal_outcome_survives_a_failed_lock_release() -> None:
    plan = _plan()

    async def planner(**kwargs: object) -> AdjustedForecastSealPlan:
        del kwargs
        return plan

    async def sealer(*args: object) -> dict[str, object]:
        del args
        raise RuntimeError("sealer-secret")

    @asynccontextmanager
    async def failing_release(settings: Settings) -> AsyncIterator[None]:
        del settings
        try:
            yield
        finally:
            raise OSError("unlock-secret")

    result = await demo.execute_adjusted_forecast_demo(
        end_session=END,
        plan_id=plan.plan_id,
        authorization=demo.AUTHORIZATION_SENTINEL,
        settings=_settings(),
        store_factory=_store_factory(FakeStore(None)),
        http_get=_healthy_get,
        http_post=_probe_http_post([]),
        snapshot_sealer=sealer,
        runtime_attestor=_runtime_attestor,
        runtime_revalidator=_runtime_revalidator,
        lock_fn=failing_release,
        planner=planner,
    )

    assert result["status"] == "seal_outcome_unknown"
    assert result["proof_phase"] == "vendor_lock_release"
    assert result["seal_proof_phase"] == "one_shot_adjusted_seal"
    assert result["failure_type"] == "RuntimeError"
    assert result["lock_release_failure_type"] == "OSError"
    assert result["expected_factor_set_id"] == FACTOR_ID
    assert "sealer-secret" not in str(result)
    assert "unlock-secret" not in str(result)


@pytest.mark.asyncio
async def test_failure_after_valid_receipt_returns_complete_recovery_proof() -> None:
    plan = _plan()
    record = _snapshot_record()

    async def planner(**kwargs: object) -> AdjustedForecastSealPlan:
        del kwargs
        return plan

    async def sealer(
        actual_plan: AdjustedForecastSealPlan,
        attestation: demo.RuntimeImageAttestation,
    ) -> dict[str, object]:
        assert actual_plan == plan
        assert attestation == ATTESTATION
        return _task_result(snapshot_id=record.snapshot_id)

    result = await demo.execute_adjusted_forecast_demo(
        end_session=END,
        plan_id=plan.plan_id,
        authorization=demo.AUTHORIZATION_SENTINEL,
        settings=_settings(),
        store_factory=_store_factory(FakeStore(None)),
        http_get=_healthy_get,
        http_post=_probe_http_post([]),
        snapshot_sealer=sealer,
        runtime_attestor=_runtime_attestor,
        runtime_revalidator=_runtime_revalidator,
        lock_fn=_no_lock,
        planner=planner,
    )

    assert result["status"] == "sealed_proof_failed"
    assert result["snapshot_id"] == record.snapshot_id
    assert result["factor_set_id"] == FACTOR_ID
    assert result["factor_set_recorded_at"] == FACTOR_RECORDED.isoformat()
    assert result["factor_available_at"] == FACTOR_AVAILABLE.isoformat()
    assert result["proof_phase"] == "snapshot_validation"
    assert result["failure_type"] == "ForecastDemoRefused"
    assert result["http_status"] is None


@pytest.mark.asyncio
async def test_outer_authorization_and_plan_digest_fail_before_planning() -> None:
    called = False

    async def planner(**kwargs: object) -> AdjustedForecastSealPlan:
        nonlocal called
        del kwargs
        called = True
        return _plan()

    with pytest.raises(demo.ForecastDemoRefused, match="authorization"):
        await demo.execute_adjusted_forecast_demo(
            end_session=END,
            plan_id=PLAN_ID,
            authorization=demo.INNER_AUTHORIZATION_SENTINEL,
            settings=_settings(),
            planner=planner,
        )
    with pytest.raises(demo.ForecastDemoRefused, match="plan_id"):
        await demo.execute_adjusted_forecast_demo(
            end_session=END,
            plan_id="not-a-plan",
            authorization=demo.AUTHORIZATION_SENTINEL,
            settings=_settings(),
            planner=planner,
        )
    assert called is False


def test_cli_surfaces_unknown_outcome_without_exception_text(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def unknown(**kwargs: object) -> dict[str, object]:
        del kwargs
        return {
            "status": "seal_outcome_unknown",
            "plan_id": PLAN_ID,
            "failure_type": "RuntimeError",
        }

    monkeypatch.setattr(demo, "execute_adjusted_forecast_demo", unknown)
    exit_code = demo.main(
        [
            "execute",
            "--end",
            END.isoformat(),
            "--plan-id",
            PLAN_ID,
            "--authorization",
            demo.AUTHORIZATION_SENTINEL,
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 3
    assert "seal_outcome_unknown" in captured.err
    assert captured.out == ""
