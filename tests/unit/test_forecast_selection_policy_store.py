"""Failure posture for the immutable prospective selection-policy registry."""

from __future__ import annotations

import hashlib
import json
from dataclasses import FrozenInstanceError
from datetime import UTC, date, datetime

import pytest
from sqlalchemy.exc import IntegrityError, OperationalError

from app.core.exceptions import AppError
from app.db.models.forecast_evidence import ForecastSelectionPolicyRegistration
from app.services.forecast_selection_policies import (
    MINIMUM_SEAL_LEAD_SECONDS,
    ForecastSelectionWindow,
    ProspectiveForecastSelectionPolicy,
)
from app.services.forecast_selection_policy_store import SqlForecastSelectionPolicyStore

RECORDED = datetime(2026, 7, 16, 12, tzinfo=UTC)


def _policy(**updates: object) -> ProspectiveForecastSelectionPolicy:
    values: dict[str, object] = {
        "symbols": ("MSFT",),
        "target": "close",
        "series_basis": "raw",
        "horizon_unit": "trading_day",
        "currency": "USD",
        "model_selector": "baseline_naive",
        "model_version": "baseline-naive@1",
        "horizon": 5,
        "selected_steps": (1, 2, 3, 4, 5),
        "interval_coverages_millis": (500, 800, 950),
        "fit_window": ForecastSelectionWindow(date(2026, 7, 20), date(2026, 9, 30)),
        "heldout_window": ForecastSelectionWindow(date(2026, 10, 1), date(2026, 10, 30)),
        "minimum_fit_member_count": 200,
        "minimum_heldout_member_count": 100,
        "minimum_seal_lead_seconds": MINIMUM_SEAL_LEAD_SECONDS,
        "cadence": "xnys_session_daily",
        "snapshot_binding": "explicit_snapshot_id",
        "selection_rule": "complete_selected_step_bundle_within_one_utc_target_window",
        "resolution_lag_seconds": 172_800,
        "forecast_resolution_policy_hash": "sha256:" + "a" * 64,
        "forecast_availability_rule_set_hash": "sha256:" + "b" * 64,
        "outcome_resolution_policy_hash": "sha256:" + "c" * 64,
        "outcome_availability_rule_set_hash": "sha256:" + "d" * 64,
    }
    values.update(updates)
    return ProspectiveForecastSelectionPolicy(**values)  # type: ignore[arg-type]


class _DriverFailure(Exception):
    def __init__(
        self,
        sqlstate: str | None,
        *,
        constraint_name: str | None = None,
    ) -> None:
        super().__init__("secret host=db.internal role=stockapi_app")
        self.sqlstate = sqlstate
        self.constraint_name = constraint_name


def _operational(sqlstate: str | None = None) -> OperationalError:
    return OperationalError("secret COMMIT statement", {}, _DriverFailure(sqlstate))


def _integrity(constraint: str, sqlstate: str = "23505") -> IntegrityError:
    return IntegrityError(
        "secret INSERT statement",
        {},
        _DriverFailure(sqlstate, constraint_name=constraint),
    )


def _stamp(row: ForecastSelectionPolicyRegistration) -> None:
    document = json.loads(row.canonical_policy)
    epoch = document["policy_epoch"]
    windows = document["windows"]
    row.schema_version = document["schema_version"]
    row.forecast_resolution_policy_hash = epoch["forecast_resolution_policy_hash"]
    row.forecast_availability_rule_set_hash = epoch["forecast_availability_rule_set_hash"]
    row.outcome_resolution_policy_hash = epoch["outcome_resolution_policy_hash"]
    row.outcome_availability_rule_set_hash = epoch["outcome_availability_rule_set_hash"]
    row.resolution_lag_seconds = epoch["resolution_lag_seconds"]
    row.fit_window_start = date.fromisoformat(windows["fit"]["start"])
    row.fit_window_end = date.fromisoformat(windows["fit"]["end"])
    row.heldout_window_start = date.fromisoformat(windows["heldout"]["start"])
    row.heldout_window_end = date.fromisoformat(windows["heldout"]["end"])
    row.minimum_fit_member_count = windows["fit"]["minimum_member_count"]
    row.minimum_heldout_member_count = windows["heldout"]["minimum_member_count"]
    row.minimum_seal_lead_seconds = document["minimum_seal_lead_seconds"]
    row.selected_steps = document["study"]["selected_steps"]
    row.recorded_at = RECORDED
    row.creator_xid = 123


class _Transaction:
    def __init__(self, session: _Session) -> None:
        self.session = session

    async def __aenter__(self) -> _Transaction:
        return self

    async def __aexit__(self, *error: object) -> bool:
        if any(error):
            self.session.pending = None
            return False
        row = self.session.pending
        if row is None:
            return False
        failure = self.session.database.commit_failure
        if failure is None or failure[1]:
            self.session.database.commit(row)
        self.session.pending = None
        if failure is not None:
            self.session.database.commit_failure = None
            raise failure[0]
        return False


class _Session:
    def __init__(self, database: _Database) -> None:
        self.database = database
        self.pending: ForecastSelectionPolicyRegistration | None = None

    async def __aenter__(self) -> _Session:
        return self

    async def __aexit__(self, *_error: object) -> bool:
        return False

    def begin(self) -> _Transaction:
        return _Transaction(self)

    async def get(
        self,
        model: object,
        identity: str,
    ) -> ForecastSelectionPolicyRegistration | None:
        assert model is ForecastSelectionPolicyRegistration
        self.database.lookups.append(identity)
        if self.database.read_failures:
            failure = self.database.read_failures.pop(0)
            if failure is not None:
                raise failure
        if self.pending is not None and self.pending.policy_hash == identity:
            return self.pending
        return self.database.records.get(identity)

    async def execute(self, statement: object, params: dict[str, bytes]) -> _ScalarResult:
        canonical = params["canonical_policy"]
        policy_hash = "sha256:" + hashlib.sha256(canonical).hexdigest()
        row = ForecastSelectionPolicyRegistration(
            policy_hash=policy_hash,
            canonical_policy=canonical,
        )
        _stamp(row)
        self.database.registration_attempts += 1
        self.database.registration_calls.append((str(statement), dict(params)))
        if self.database.race_constraint is not None:
            constraint = self.database.race_constraint
            self.database.race_constraint = None
            self.database.commit(row)
            raise _integrity(constraint)
        if self.database.execute_failure is not None:
            failure, visible = self.database.execute_failure
            self.database.execute_failure = None
            if visible:
                self.database.commit(row)
            raise failure
        self.pending = row
        return _ScalarResult(policy_hash)


class _ScalarResult:
    def __init__(self, value: str) -> None:
        self.value = value

    def scalar_one(self) -> str:
        return self.value


class _Database:
    def __init__(self) -> None:
        self.records: dict[str, ForecastSelectionPolicyRegistration] = {}
        self.session_count = 0
        self.registration_attempts = 0
        self.registration_calls: list[tuple[str, dict[str, bytes]]] = []
        self.lookups: list[str] = []
        self.race_constraint: str | None = None
        self.execute_failure: tuple[Exception, bool] | None = None
        self.commit_failure: tuple[Exception, bool] | None = None
        self.read_failures: list[Exception | None] = []

    def __call__(self) -> _Session:
        self.session_count += 1
        return _Session(self)

    def commit(self, row: ForecastSelectionPolicyRegistration) -> None:
        self.records[row.policy_hash] = row


def _store(database: _Database) -> SqlForecastSelectionPolicyStore:
    return SqlForecastSelectionPolicyStore(sessionmaker=database)  # type: ignore[arg-type]


async def test_register_uses_definer_boundary_and_fresh_committed_reread() -> None:
    database = _Database()
    policy = _policy()

    proof = await _store(database).register(policy)

    assert proof.policy == policy
    assert proof.record.policy_hash == policy.selection_policy_hash
    assert proof.record.canonical_policy == policy.canonical_policy
    assert proof.record.fit_window_start == policy.fit_window.start
    assert proof.record.heldout_window_end == policy.heldout_window.end
    assert proof.record.selected_steps == policy.selected_steps
    assert proof.record.recorded_at == RECORDED
    assert proof.record.creator_xid == 123
    assert len(database.registration_calls) == 1
    statement, params = database.registration_calls[0]
    assert "public.register_forecast_selection_policy" in statement
    assert params == {"canonical_policy": policy.canonical_policy}
    assert database.session_count == 3
    with pytest.raises(FrozenInstanceError):
        proof.record.creator_xid = 1  # type: ignore[misc]


async def test_exact_registration_replays_without_another_insert() -> None:
    database = _Database()
    store = _store(database)
    first = await store.register(_policy())

    replay = await store.register(_policy())

    assert replay == first
    assert database.registration_attempts == 1


@pytest.mark.parametrize(
    "constraint",
    [
        "pk_forecast_selection_policies",
        "uq_forecast_selection_policies_outcome_epoch",
    ],
)
async def test_named_duplicate_race_replays_exact_winner(constraint: str) -> None:
    database = _Database()
    database.race_constraint = constraint

    proof = await _store(database).register(_policy())

    assert proof.policy == _policy()
    assert database.registration_attempts == 1


async def test_unseen_named_race_is_retryable_write_conflict() -> None:
    database = _Database()
    database.execute_failure = (
        _integrity("pk_forecast_selection_policies"),
        False,
    )

    with pytest.raises(AppError) as excinfo:
        await _store(database).register(_policy())

    assert excinfo.value.code == "forecast_selection_policy_write_conflict"
    assert excinfo.value.details == {"retryable": True}


@pytest.mark.parametrize(
    "field",
    [
        "resolution_lag_seconds",
        "minimum_fit_member_count",
        "minimum_seal_lead_seconds",
        "selected_steps",
    ],
)
async def test_corrupt_registration_is_never_replayed_as_success(field: str) -> None:
    database = _Database()
    store = _store(database)
    proof = await store.register(_policy())
    row = database.records[proof.record.policy_hash]
    value = getattr(row, field)
    setattr(row, field, [1] if isinstance(value, list) else value + 1)

    with pytest.raises(AppError) as excinfo:
        await store.register(_policy())

    assert excinfo.value.code == "forecast_selection_policy_evidence_corrupt"
    assert excinfo.value.details == {"retryable": False}


async def test_historical_epoch_is_registered_without_current_environment_coupling() -> None:
    database = _Database()
    historical = _policy(
        resolution_lag_seconds=99,
        outcome_resolution_policy_hash="sha256:" + "e" * 64,
        outcome_availability_rule_set_hash="sha256:" + "f" * 64,
    )

    proof = await _store(database).register(historical)

    assert proof.policy == historical
    assert proof.record.resolution_lag_seconds == 99


async def test_invalid_policy_fails_before_opening_a_session() -> None:
    database = _Database()

    with pytest.raises(AppError) as excinfo:
        await _store(database).register(object())  # type: ignore[arg-type]

    assert excinfo.value.code == "forecast_selection_policy_invalid"
    assert excinfo.value.details == {"retryable": False}
    assert database.session_count == 0


async def test_unknown_commit_reconciles_visible_exact_registration() -> None:
    database = _Database()
    database.commit_failure = (_operational(), True)

    proof = await _store(database).register(_policy())

    assert database.lookups[-1] == proof.record.policy_hash


async def test_unknown_invisible_commit_reports_outcome_unknown() -> None:
    database = _Database()
    database.commit_failure = (_operational(), False)

    with pytest.raises(AppError) as excinfo:
        await _store(database).register(_policy())

    assert excinfo.value.code == "forecast_selection_policy_commit_unknown"
    assert excinfo.value.details == {"outcome_unknown": True, "retryable": True}
    assert "db.internal" not in excinfo.value.message


@pytest.mark.parametrize("failure_stage", ["execute", "commit"])
async def test_sqlstate_40003_reconciles_visible_registration(failure_stage: str) -> None:
    database = _Database()
    failure = (_operational("40003"), True)
    if failure_stage == "execute":
        database.execute_failure = failure
    else:
        database.commit_failure = failure

    proof = await _store(database).register(_policy())

    assert proof.policy == _policy()


@pytest.mark.parametrize("failure_stage", ["execute", "commit"])
async def test_sqlstate_40003_without_visible_row_reports_unknown(failure_stage: str) -> None:
    database = _Database()
    failure = (_operational("40003"), False)
    if failure_stage == "execute":
        database.execute_failure = failure
    else:
        database.commit_failure = failure

    with pytest.raises(AppError) as excinfo:
        await _store(database).register(_policy())

    assert excinfo.value.code == "forecast_selection_policy_commit_unknown"
    assert excinfo.value.details == {"outcome_unknown": True, "retryable": True}


async def test_known_rollback_is_retryable_but_not_unknown() -> None:
    database = _Database()
    database.commit_failure = (_operational("40001"), False)

    with pytest.raises(AppError) as excinfo:
        await _store(database).register(_policy())

    assert excinfo.value.code == "forecast_selection_policy_store_unavailable"
    assert excinfo.value.details == {"retryable": True}


async def test_integrity_and_configuration_errors_are_classified_and_redacted() -> None:
    database = _Database()
    database.execute_failure = (_integrity("unexpected_check", "23514"), False)

    with pytest.raises(AppError) as integrity:
        await _store(database).register(_policy())

    assert integrity.value.code == "forecast_selection_policy_integrity_failed"
    assert integrity.value.details == {"retryable": False}
    assert "db.internal" not in integrity.value.message

    database = _Database()
    database.read_failures.append(_operational("42501"))

    with pytest.raises(AppError) as configuration:
        await _store(database).register(_policy())

    assert configuration.value.code == "forecast_selection_policy_configuration_invalid"
    assert configuration.value.details == {"retryable": False}
    assert "stockapi_app" not in configuration.value.message
