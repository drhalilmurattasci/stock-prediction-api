"""Snapshot-backed forecast serving: fail-closed gates and honest assembly."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

import pytest
from sqlalchemy.dialects import postgresql

from app.config import Settings
from app.core.exceptions import AppError, NotFoundError, NotImplementedYet
from app.schemas.forecast import ForecastRequest, ForecastResponse
from app.services.forecast_serving import (
    ForecastServingPolicy,
    SnapshotForecastService,
    build_forecast_service,
    build_latest_snapshot_statement,
    read_build_revision,
)
from app.services.forecast_snapshots import (
    ForecastInputSnapshotPayload,
    ForecastInputSnapshotRecord,
    ForecastInputSnapshotSelector,
    SnapshotAvailabilityEvidence,
    SnapshotObservation,
    SnapshotSourceLineage,
    build_snapshot_record,
)
from app.services.forecasting import UnavailableForecastService, get_forecast_service

AS_OF = datetime(2026, 7, 10, 21, tzinfo=UTC)
SEALED_AT = AS_OF + timedelta(minutes=2)
GENERATED_AT = AS_OF + timedelta(hours=1)
POLICY_HASH = "sha256:" + "a" * 64
RULE_SET_HASH = "sha256:" + "b" * 64
FORECAST_ID = UUID("33333333-3333-3333-3333-333333333333")

VERIFIED_EVIDENCE = SnapshotAvailabilityEvidence(
    status="passed",
    rule_set_hash=RULE_SET_HASH,
    checked_at=AS_OF + timedelta(minutes=1),
)


def _payload(**overrides: object) -> ForecastInputSnapshotPayload:
    fields: dict[str, object] = {
        "resolution_policy_hash": POLICY_HASH,
        "symbol": "AAPL",
        "target": "close",
        "horizon_unit": "trading_day",
        "series_basis": "raw",
        "input_timespan": "day",
        "input_multiplier": 1,
        "as_of": AS_OF,
        "currency": "USD",
        # Enough daily history for the empirical-residual baselines to estimate
        # per-step quantile errors at horizon 2 (they need >=2 prefix errors).
        "observations": tuple(
            SnapshotObservation(
                observed_at=AS_OF - timedelta(days=offset, hours=1),
                available_at=AS_OF - timedelta(days=offset),
                value=90.0 + float(offset % 3) + (10 - offset),
            )
            for offset in range(10, 0, -1)
        ),
        "target_times": (AS_OF + timedelta(days=3), AS_OF + timedelta(days=4)),
        "data_sources": (
            SnapshotSourceLineage(
                name="fixture-market-data",
                snapshot_id="fixture-source-v1",
                max_available_at=AS_OF - timedelta(hours=1),
                fields=("close", "volume"),
            ),
        ),
        "availability": VERIFIED_EVIDENCE,
    }
    fields.update(overrides)
    return ForecastInputSnapshotPayload(**fields)  # type: ignore[arg-type]


def _record(**payload_overrides: object) -> ForecastInputSnapshotRecord:
    return build_snapshot_record(_payload(**payload_overrides), sealed_at=SEALED_AT)


def _request(**overrides: object) -> ForecastRequest:
    fields: dict[str, object] = {
        "symbol": "AAPL",
        "horizon": 2,
        "horizon_unit": "trading_day",
        "target": "close",
        "model": "baseline_naive",
        "interval_coverages": [0.8],
    }
    fields.update(overrides)
    return ForecastRequest.model_validate(fields)


@dataclass
class FakeRepository:
    record: ForecastInputSnapshotRecord | None
    get_calls: list[str] = field(default_factory=list)
    latest_calls: list[ForecastInputSnapshotSelector] = field(default_factory=list)

    async def get(self, snapshot_id: str) -> ForecastInputSnapshotRecord | None:
        self.get_calls.append(snapshot_id)
        return self.record

    async def latest(
        self,
        selector: ForecastInputSnapshotSelector,
    ) -> ForecastInputSnapshotRecord | None:
        self.latest_calls.append(selector)
        return self.record


@dataclass
class FakeRunStore:
    repository: FakeRepository
    calls: list[tuple[ForecastRequest, str | None, str | None]] = field(default_factory=list)

    async def execute(
        self,
        request: ForecastRequest,
        *,
        idempotency_key: str | None,
        principal: str | None,
        producer,
    ) -> ForecastResponse:
        self.calls.append((request, idempotency_key, principal))
        return await producer(self.repository)


def _service(
    record: ForecastInputSnapshotRecord | None,
    *,
    trusted_hash: str = RULE_SET_HASH,
    seasonal_period: int = 2,
) -> tuple[SnapshotForecastService, FakeRepository]:
    repository = FakeRepository(record)
    service = SnapshotForecastService(
        repository=repository,
        policy=ForecastServingPolicy(
            resolution_policy_hash=POLICY_HASH,
            trusted_availability_rule_set_hash=trusted_hash,
            seasonal_period=seasonal_period,
        ),
        clock=lambda: GENERATED_AT,
        id_factory=lambda: FORECAST_ID,
        code_version="test-code-version",
    )
    return service, repository


async def test_serves_verified_snapshot_with_honest_provenance() -> None:
    record = _record()
    service, repository = _service(record)

    response = await service.forecast(_request())

    assert repository.latest_calls == [
        ForecastInputSnapshotSelector(
            resolution_policy_hash=POLICY_HASH,
            symbol="AAPL",
            target="close",
            horizon_unit="trading_day",
            series_basis="raw",
            input_timespan="day",
            input_multiplier=1,
            cutoff=None,
        )
    ]
    assert response.symbol == "AAPL"
    assert len(response.forecasts) == 2
    assert response.provenance.snapshot_id == record.snapshot_id
    assert response.provenance.feature_set_hash == record.snapshot_id
    assert response.provenance.model_version == "baseline-naive@1"
    assert response.provenance.forecast_id == FORECAST_ID
    assert response.provenance.generated_at == GENERATED_AT
    assert response.provenance.code_version == "test-code-version"
    assert response.provenance.lookahead_check.status == "passed"
    assert response.provenance.lookahead_check.violations == []
    assert response.as_of == AS_OF


async def test_auto_routes_to_the_naive_default() -> None:
    service, _ = _service(_record())
    response = await service.forecast(_request(model="auto"))
    assert response.provenance.model_version == "baseline-naive@1"


@pytest.mark.parametrize(
    ("model", "expected_version"),
    [
        ("baseline_drift", "baseline-drift@1"),
        ("baseline_seasonal_naive", "baseline-seasonal-naive-s2@1"),
    ],
)
async def test_explicit_baseline_selectors_route_and_stamp_versions(
    model: str, expected_version: str
) -> None:
    service, _ = _service(_record())
    response = await service.forecast(_request(model=model))
    assert response.provenance.model_version == expected_version


async def test_pinned_snapshot_id_is_loaded_by_get_and_bound() -> None:
    record = _record()
    service, repository = _service(record)

    response = await service.forecast(_request(snapshot_id=record.snapshot_id))

    assert repository.get_calls == [record.snapshot_id]
    assert repository.latest_calls == []
    assert response.provenance.snapshot_id == record.snapshot_id


async def test_pinned_snapshot_mismatch_is_a_409_conflict() -> None:
    pinned = "sha256:" + "c" * 64
    service, _ = _service(_record())  # repository returns a differently-hashed record

    with pytest.raises(AppError) as excinfo:
        await service.forecast(_request(snapshot_id=pinned))

    assert excinfo.value.status_code == 409
    assert excinfo.value.code == "snapshot_validation_failed"


async def test_missing_snapshot_is_a_404() -> None:
    service, _ = _service(None)
    with pytest.raises(NotFoundError):
        await service.forecast(_request())


@pytest.mark.parametrize(
    ("overrides", "code"),
    [
        ({"horizon_unit": "calendar_day"}, "horizon_unit_not_servable"),
        ({"target": "adjusted_close"}, "target_not_servable"),
    ],
)
async def test_policy_v1_refuses_unversioned_horizon_and_adjusted_semantics(
    overrides: dict[str, object],
    code: str,
) -> None:
    service, _ = _service(_record())
    with pytest.raises(AppError) as excinfo:
        await service.forecast(_request(**overrides))
    assert excinfo.value.status_code == 409
    assert excinfo.value.code == code


@pytest.mark.parametrize(
    "availability",
    [
        SnapshotAvailabilityEvidence(status="not_run"),  # never verified upstream
        VERIFIED_EVIDENCE,  # verified against a rule set the server does not trust
    ],
)
async def test_unverified_availability_is_refused_not_footnoted(
    availability: SnapshotAvailabilityEvidence,
) -> None:
    trusted = RULE_SET_HASH if availability.status == "not_run" else "sha256:" + "d" * 64
    service, _ = _service(_record(availability=availability), trusted_hash=trusted)

    with pytest.raises(AppError) as excinfo:
        await service.forecast(_request())

    assert excinfo.value.status_code == 503
    assert excinfo.value.code == "snapshot_not_verified"


@pytest.mark.parametrize("model", ["arima", "chronos"])
async def test_unimplemented_model_selectors_are_501_not_conflict(model: str) -> None:
    service, _ = _service(_record())
    with pytest.raises(NotImplementedYet):
        await service.forecast(_request(model=model))


async def test_infeasible_forecast_on_verified_snapshot_is_409_not_500() -> None:
    # Seasonal period longer than the snapshot's history: the model cannot fit,
    # and that is a structured client-visible refusal, not an internal error.
    service, _ = _service(_record(), seasonal_period=20)
    with pytest.raises(AppError) as excinfo:
        await service.forecast(_request(model="baseline_seasonal_naive"))
    assert excinfo.value.status_code == 409
    assert excinfo.value.code == "forecast_not_computable"


async def test_clock_skew_is_clamped_to_as_of_not_500() -> None:
    record = _record()
    repository = FakeRepository(record)
    service = SnapshotForecastService(
        repository=repository,
        policy=ForecastServingPolicy(
            resolution_policy_hash=POLICY_HASH,
            trusted_availability_rule_set_hash=RULE_SET_HASH,
        ),
        clock=lambda: AS_OF - timedelta(hours=1),  # host clock behind the data
        id_factory=lambda: FORECAST_ID,
    )
    response = await service.forecast(_request())
    assert response.provenance.generated_at == AS_OF


async def test_idempotency_key_is_refused_until_a_run_store_exists() -> None:
    service, repository = _service(_record())

    with pytest.raises(NotImplementedYet) as excinfo:
        await service.forecast(_request(), idempotency_key="retry-1")

    assert repository.get_calls == [] and repository.latest_calls == []
    assert "retry-1" not in str(excinfo.value.details)


async def test_persisted_run_store_owns_snapshot_resolution_and_accepts_keyed_retry() -> None:
    bound_repository = FakeRepository(_record())
    run_store = FakeRunStore(bound_repository)
    fallback_repository = FakeRepository(None)
    service = SnapshotForecastService(
        repository=fallback_repository,
        policy=ForecastServingPolicy(
            resolution_policy_hash=POLICY_HASH,
            trusted_availability_rule_set_hash=RULE_SET_HASH,
        ),
        clock=lambda: GENERATED_AT,
        id_factory=lambda: FORECAST_ID,
        run_store=run_store,
    )

    response = await service.forecast(
        _request(),
        idempotency_key="retry-1",
        principal="api-principal",
    )

    assert response.provenance.forecast_id == FORECAST_ID
    assert run_store.calls == [(_request(), "retry-1", "api-principal")]
    assert bound_repository.latest_calls
    assert fallback_repository.get_calls == []
    assert fallback_repository.latest_calls == []


def test_latest_statement_filters_exact_series_and_orders_newest_first() -> None:
    selector = ForecastInputSnapshotSelector(
        resolution_policy_hash=POLICY_HASH,
        symbol="AAPL",
        target="close",
        horizon_unit="trading_day",
        series_basis="raw",
        input_timespan="day",
        input_multiplier=1,
        cutoff=AS_OF,
    )
    sql = str(
        build_latest_snapshot_statement(
            selector, trusted_availability_rule_set_hash=RULE_SET_HASH
        ).compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "forecast_input_snapshots.schema_version = 1" in sql
    assert f"forecast_input_snapshots.resolution_policy_hash = '{POLICY_HASH}'" in sql
    assert "forecast_input_snapshots.symbol = 'AAPL'" in sql
    assert "forecast_input_snapshots.series_basis = 'raw'" in sql
    assert "forecast_input_snapshots.as_of <= '2026-07-10 21:00:00+00:00'" in sql
    # An unverified newer snapshot must not outage a series with an older
    # servable one: only trusted, passed snapshots qualify for latest().
    assert "forecast_input_snapshots.availability_status = 'passed'" in sql
    assert f"forecast_input_snapshots.availability_rule_set_hash = '{RULE_SET_HASH}'" in sql
    assert "ORDER BY forecast_input_snapshots.as_of DESC" in sql
    assert "LIMIT 1" in sql


def test_build_forecast_service_is_fail_closed_by_configuration() -> None:
    sessionmaker = object()  # never touched before a request executes

    # Fully unset -> serving was never enabled -> None (route stays 501).
    assert build_forecast_service(Settings(app_env="test"), sessionmaker) is None  # type: ignore[arg-type]
    blank = Settings(
        app_env="test",
        forecast_resolution_policy_hash="",
        forecast_trusted_availability_rule_set_hash="   ",
    )
    assert build_forecast_service(blank, sessionmaker) is None  # type: ignore[arg-type]

    # Partially or malformed configured -> loud operator error, not silent 501.
    for overrides in (
        {"forecast_resolution_policy_hash": POLICY_HASH},
        {"forecast_trusted_availability_rule_set_hash": RULE_SET_HASH},
        {
            "forecast_resolution_policy_hash": "sha256:not-hex",
            "forecast_trusted_availability_rule_set_hash": RULE_SET_HASH,
        },
    ):
        with pytest.raises(AppError) as excinfo:
            build_forecast_service(Settings(app_env="test", **overrides), sessionmaker)  # type: ignore[arg-type]
        assert excinfo.value.code == "forecast_serving_misconfigured"

    service = build_forecast_service(
        Settings(
            app_env="test",
            forecast_resolution_policy_hash=POLICY_HASH,
            forecast_trusted_availability_rule_set_hash=RULE_SET_HASH,
            forecast_seasonal_period=7,
        ),
        sessionmaker,  # type: ignore[arg-type]
    )
    assert isinstance(service, SnapshotForecastService)
    assert service.policy.seasonal_period == 7
    assert service.run_store is not None


def test_build_revision_is_honest_and_strict(tmp_path) -> None:
    revision_file = tmp_path / ".stockapi-build-revision"
    assert read_build_revision(revision_file) is None

    revision_file.write_text("unattested\n", encoding="utf-8")
    assert read_build_revision(revision_file) is None

    revision_file.write_text("abc123-build.7\n", encoding="utf-8")
    assert read_build_revision(revision_file) == "abc123-build.7"

    revision_file.write_text("bad revision\n", encoding="utf-8")
    with pytest.raises(AppError) as excinfo:
        read_build_revision(revision_file)
    assert excinfo.value.code == "forecast_build_revision_invalid"


def test_dependency_returns_fail_closed_service_until_configured() -> None:
    unconfigured = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(settings=Settings(app_env="test"), sessionmaker=object())
        )
    )
    assert isinstance(get_forecast_service(unconfigured), UnavailableForecastService)  # type: ignore[arg-type]

    configured = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                settings=Settings(
                    app_env="test",
                    forecast_resolution_policy_hash=POLICY_HASH,
                    forecast_trusted_availability_rule_set_hash=RULE_SET_HASH,
                ),
                sessionmaker=object(),
            )
        )
    )
    assert isinstance(get_forecast_service(configured), SnapshotForecastService)  # type: ignore[arg-type]
