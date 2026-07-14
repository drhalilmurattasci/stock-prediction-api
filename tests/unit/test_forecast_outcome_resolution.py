"""Policy, SQL shape, and failure posture for realized-outcome resolution."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta, timezone

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import OperationalError

from app.core.exceptions import AppError
from app.services.forecast_outcome_resolution import (
    OUTCOME_AVAILABILITY_RULE_SET_HASH,
    ForecastOutcomeResolutionPolicy,
    OutcomeResolutionMisconfigured,
    OutcomeResolutionPolicyError,
    OutcomeResolutionPolicyMismatch,
    SqlOutcomeBarVersionResolver,
    build_exact_bar_version_statement,
    outcome_availability_rule_set_document,
)
from ingestion.locks import bar_series_lock_id

TARGET = datetime(2026, 7, 13, 20, tzinfo=UTC)
LAG_SECONDS = 24 * 60 * 60
POLICY = ForecastOutcomeResolutionPolicy(resolution_lag_seconds=LAG_SECONDS)
CUTOFF = POLICY.cutoff_for(TARGET)


def _row(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "symbol": "MSFT",
        "timespan": "day",
        "multiplier": 1,
        "observed_at": TARGET,
        "source": "polygon_open_close",
        "adjustment_basis": "raw",
        "fetched_at": TARGET + timedelta(minutes=1),
        "source_as_of": TARGET + timedelta(minutes=2),
        "version_recorded_at": TARGET + timedelta(minutes=3),
        "available_at": TARGET + timedelta(minutes=4),
        "value": 503.25,
    }
    values.update(overrides)
    return values


class _Result:
    def __init__(self, *, scalar: object = None, rows: tuple[dict[str, object], ...] = ()):
        self.scalar = scalar
        self.rows = rows

    def scalar_one(self) -> object:
        return self.scalar

    def mappings(self) -> _Result:
        return self

    def all(self) -> tuple[dict[str, object], ...]:
        return self.rows


class _Transaction:
    async def __aenter__(self) -> _Transaction:
        return self

    async def __aexit__(self, *_error: object) -> bool:
        return False


class _Session:
    def __init__(self, maker: _SessionMaker) -> None:
        self.maker = maker

    async def __aenter__(self) -> _Session:
        return self

    async def __aexit__(self, *_error: object) -> bool:
        return False

    def begin(self) -> _Transaction:
        return _Transaction()

    async def execute(self, statement: object, _params: object = None) -> _Result:
        self.maker.statements.append(str(statement))
        if self.maker.error is not None:
            error = self.maker.error
            self.maker.error = None
            raise error
        if "clock_timestamp" in str(statement):
            return _Result(scalar=self.maker.database_now)
        return _Result(rows=self.maker.rows)


class _SessionMaker:
    def __init__(
        self,
        *,
        database_now: datetime = CUTOFF,
        rows: tuple[dict[str, object], ...] = (),
        error: Exception | None = None,
    ) -> None:
        self.database_now = database_now
        self.rows = rows
        self.error = error
        self.statements: list[str] = []

    def __call__(self) -> _Session:
        return _Session(self)


class _DriverError(Exception):
    def __init__(self, sqlstate: str | None) -> None:
        super().__init__("secret database host and statement")
        self.sqlstate = sqlstate


def _operational(sqlstate: str | None) -> OperationalError:
    return OperationalError("secret SQL", {}, _DriverError(sqlstate))


def _resolver(maker: _SessionMaker) -> SqlOutcomeBarVersionResolver:
    return SqlOutcomeBarVersionResolver(
        sessionmaker=maker,  # type: ignore[arg-type]
        policy=POLICY,
    )


def test_policy_documents_are_content_addressed_and_lag_is_load_bearing() -> None:
    assert OUTCOME_AVAILABILITY_RULE_SET_HASH == (
        "sha256:cfd2d129386375b8663f71f5752b70630cf8dbde21cc18596985de41a58ca705"
    )
    assert POLICY.outcome_resolution_policy_hash == (
        "sha256:0dd21c600bd7ef2e98fd88d2604223624d42a5eaf0d7e7b309bf079ca3e90d28"
    )
    assert POLICY.outcome_resolution_policy_document["availability_rule_set_hash"] == (
        OUTCOME_AVAILABILITY_RULE_SET_HASH
    )
    document = outcome_availability_rule_set_document()
    assert "distinct_candidates_tied_at_maximum_are_rejected" in document["rules"]
    visibility = document["version_visibility"]
    assert isinstance(visibility, dict)
    assert visibility["reconstruction_lanes"] == [
        "bars.current",
        "bars_revisions.previous",
        "bars_revisions.incoming",
    ]
    assert visibility["receipt_order"] == "irrelevant_to_version_selection"
    document["rules"] = []
    assert outcome_availability_rule_set_document()["rules"] != []
    assert (
        ForecastOutcomeResolutionPolicy(
            resolution_lag_seconds=LAG_SECONDS + 1
        ).outcome_resolution_policy_hash
        != POLICY.outcome_resolution_policy_hash
    )
    with pytest.raises(FrozenInstanceError):
        POLICY.resolution_lag_seconds = 1  # type: ignore[misc]


def test_bar_series_fence_identity_matches_the_database_golden_vector() -> None:
    assert bar_series_lock_id("MSFT", "polygon_open_close", "day") == 919990277418442247


@pytest.mark.parametrize("lag", [True, 0, -1, 366 * 24 * 60 * 60 + 1, 1.5])
def test_policy_requires_an_explicit_positive_bounded_integer_lag(lag: object) -> None:
    with pytest.raises(OutcomeResolutionMisconfigured):
        ForecastOutcomeResolutionPolicy(resolution_lag_seconds=lag)  # type: ignore[arg-type]


def test_cutoff_is_exact_reproducible_utc_and_covered_by_policy_identity() -> None:
    local_target = datetime(2026, 7, 13, 16, tzinfo=timezone(timedelta(hours=-4)))
    cutoff = POLICY.cutoff_for(local_target)
    assert cutoff == CUTOFF
    assert cutoff.tzinfo is UTC
    assert (
        POLICY.validate_request(
            symbol="MSFT",
            target_time=local_target,
            resolution_cutoff=cutoff,
        ).target_time
        == TARGET
    )
    with pytest.raises(OutcomeResolutionPolicyMismatch, match="policy-derived"):
        POLICY.validate_request(
            symbol="MSFT",
            target_time=TARGET,
            resolution_cutoff=cutoff + timedelta(microseconds=1),
        )
    with pytest.raises(OutcomeResolutionPolicyMismatch):
        POLICY.validate_hashes(
            outcome_resolution_policy_hash="sha256:" + "0" * 64,
            availability_rule_set_hash=POLICY.availability_rule_set_hash,
        )


def test_policy_accepts_exact_early_close_and_rejects_non_session_or_wrong_close() -> None:
    # The Friday after US Thanksgiving is an official 13:00 ET close.
    early_close = datetime(2026, 11, 27, 18, tzinfo=UTC)
    assert (
        POLICY.validate_request(
            symbol="MSFT",
            target_time=early_close,
            resolution_cutoff=POLICY.cutoff_for(early_close),
        ).target_time
        == early_close
    )

    holiday = datetime(2026, 11, 26, 21, tzinfo=UTC)
    with pytest.raises(OutcomeResolutionPolicyError, match="not an XNYS session"):
        POLICY.validate_request(
            symbol="MSFT",
            target_time=holiday,
            resolution_cutoff=POLICY.cutoff_for(holiday),
        )
    with pytest.raises(OutcomeResolutionPolicyError, match="exact XNYS"):
        POLICY.validate_request(
            symbol="MSFT",
            target_time=TARGET - timedelta(minutes=1),
            resolution_cutoff=POLICY.cutoff_for(TARGET - timedelta(minutes=1)),
        )
    with pytest.raises(OutcomeResolutionPolicyError, match="uppercase"):
        POLICY.validate_request(
            symbol="msft",
            target_time=TARGET,
            resolution_cutoff=CUTOFF,
        )


def test_sql_unions_all_stored_shapes_and_preserves_ambiguous_maximum() -> None:
    request = POLICY.validate_request(
        symbol="MSFT",
        target_time=TARGET,
        resolution_cutoff=CUTOFF,
    )
    sql = str(
        build_exact_bar_version_statement(request).compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert sql.count(" UNION SELECT ") == 2
    assert "UNION ALL" not in sql
    assert "JOIN bar_version_availability ON" in sql
    assert "bar_version_availability.symbol = outcome_bar_versions.symbol" in sql
    assert "bar_version_availability.timespan = outcome_bar_versions.timespan" in sql
    assert "bar_version_availability.multiplier = outcome_bar_versions.multiplier" in sql
    assert "bar_version_availability.ts = outcome_bar_versions.observed_at" in sql
    assert "bar_version_availability.source = outcome_bar_versions.source" in sql
    assert (
        "bar_version_availability.adjustment_basis = outcome_bar_versions.adjustment_basis" in sql
    )
    assert (
        "bar_version_availability.version_recorded_at = "
        "outcome_bar_versions.version_recorded_at" in sql
    )
    assert "bar_version_availability.available_at <=" in sql
    assert "rank() OVER (ORDER BY outcome_bar_versions.version_recorded_at DESC)" in sql
    assert "ranked_outcome_bar_versions.version_rank = 1" in sql
    assert "LIMIT 2" in sql
    assert "bars.ts = '2026-07-13 20:00:00+00:00'" in sql
    assert "bars.source = 'polygon_open_close'" in sql
    assert "bars.adjustment_basis = 'raw'" in sql


async def test_resolver_exposes_hashes_and_returns_exact_detached_evidence() -> None:
    maker = _SessionMaker(rows=(_row(),))
    resolver = _resolver(maker)

    evidence = await resolver.resolve(
        symbol="MSFT",
        target_time=TARGET,
        resolution_cutoff=CUTOFF,
    )

    assert resolver.outcome_resolution_policy_hash == POLICY.outcome_resolution_policy_hash
    assert resolver.availability_rule_set_hash == OUTCOME_AVAILABILITY_RULE_SET_HASH
    assert evidence.symbol == "MSFT"
    assert evidence.observed_at == TARGET
    assert evidence.field == "close"
    assert evidence.value == 503.25
    assert evidence.available_at == TARGET + timedelta(minutes=4)
    assert len(maker.statements) == 5
    assert maker.statements[0] == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
    assert "SET LOCAL lock_timeout" in maker.statements[1]
    assert "pg_advisory_xact_lock" in maker.statements[2]
    assert "clock_timestamp" in maker.statements[3]
    assert "bar_version_availability" in maker.statements[4]


async def test_database_clock_maturity_gate_precedes_source_query() -> None:
    maker = _SessionMaker(database_now=CUTOFF - timedelta(microseconds=1), rows=(_row(),))
    with pytest.raises(AppError) as excinfo:
        await _resolver(maker).resolve(
            symbol="MSFT",
            target_time=TARGET,
            resolution_cutoff=CUTOFF,
        )
    assert excinfo.value.code == "forecast_outcome_resolution_not_ready"
    assert excinfo.value.status_code == 409
    assert excinfo.value.details == {"retryable": True}
    assert len(maker.statements) == 4
    assert maker.statements[0] == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
    assert "SET LOCAL lock_timeout" in maker.statements[1]
    assert "pg_advisory_xact_lock" in maker.statements[2]
    assert "clock_timestamp" in maker.statements[3]
    assert "bar_version_availability" not in " ".join(maker.statements)


@pytest.mark.parametrize(
    ("rows", "code", "status_code"),
    [
        ((), "forecast_outcome_source_unavailable", 409),
        ((_row(), _row(value=504.0)), "forecast_outcome_source_ambiguous", 500),
        (
            (_row(available_at=CUTOFF + timedelta(seconds=1)),),
            "forecast_outcome_source_corrupt",
            500,
        ),
        ((_row(value=float("nan")),), "forecast_outcome_source_corrupt", 500),
    ],
)
async def test_resolver_fails_closed_on_missing_ambiguous_or_corrupt_source(
    rows: tuple[dict[str, object], ...],
    code: str,
    status_code: int,
) -> None:
    with pytest.raises(AppError) as excinfo:
        await _resolver(_SessionMaker(rows=rows)).resolve(
            symbol="MSFT",
            target_time=TARGET,
            resolution_cutoff=CUTOFF,
        )
    assert excinfo.value.code == code
    assert excinfo.value.status_code == status_code
    assert excinfo.value.details == {"retryable": False}


async def test_policy_mismatch_is_rejected_before_opening_a_database_session() -> None:
    maker = _SessionMaker(rows=(_row(),))
    with pytest.raises(AppError) as excinfo:
        await _resolver(maker).resolve(
            symbol="MSFT",
            target_time=TARGET,
            resolution_cutoff=CUTOFF + timedelta(seconds=1),
        )
    assert excinfo.value.code == "forecast_outcome_resolution_policy_mismatch"
    assert maker.statements == []


@pytest.mark.parametrize(
    ("sqlstate", "code", "retryable"),
    [
        ("08006", "forecast_outcome_source_store_unavailable", True),
        ("42P01", "forecast_outcome_source_configuration_invalid", False),
    ],
)
async def test_database_errors_are_classified_without_leaking_driver_text(
    sqlstate: str,
    code: str,
    retryable: bool,
) -> None:
    maker = _SessionMaker(error=_operational(sqlstate))
    with pytest.raises(AppError) as excinfo:
        await _resolver(maker).resolve(
            symbol="MSFT",
            target_time=TARGET,
            resolution_cutoff=CUTOFF,
        )
    assert excinfo.value.code == code
    assert excinfo.value.details == {"retryable": retryable}
    assert "secret" not in excinfo.value.message
    assert "secret" not in str(excinfo.value)
