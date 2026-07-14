"""Failure posture and ORM shape for the immutable outcome-policy registry."""

from __future__ import annotations

import hashlib
import json
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest
from sqlalchemy import DateTime, LargeBinary
from sqlalchemy.exc import IntegrityError, OperationalError

from app.core.exceptions import AppError
from app.db.models.forecast_evidence import ForecastOutcomeResolutionPolicyRegistration
from app.services.forecast_outcome_policy_store import SqlForecastOutcomePolicyStore
from app.services.forecast_outcome_resolution import ForecastOutcomeResolutionPolicy

RECORDED = datetime(2026, 7, 14, 12, tzinfo=UTC)
POLICY = ForecastOutcomeResolutionPolicy(resolution_lag_seconds=86_400)


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


def _stamp(row: ForecastOutcomeResolutionPolicyRegistration) -> None:
    document = json.loads(row.canonical_policy)
    row.availability_rule_set_hash = document["availability_rule_set_hash"]
    row.schema_version = document["schema_version"]
    row.resolution_lag_seconds = document["cutoff"]["resolution_lag_seconds"]
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
        self.pending: ForecastOutcomeResolutionPolicyRegistration | None = None

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
    ) -> ForecastOutcomeResolutionPolicyRegistration | None:
        assert model is ForecastOutcomeResolutionPolicyRegistration
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
        row = ForecastOutcomeResolutionPolicyRegistration(
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
        self.records: dict[str, ForecastOutcomeResolutionPolicyRegistration] = {}
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

    def commit(self, row: ForecastOutcomeResolutionPolicyRegistration) -> None:
        self.records[row.policy_hash] = row


def _store(database: _Database) -> SqlForecastOutcomePolicyStore:
    return SqlForecastOutcomePolicyStore(sessionmaker=database)  # type: ignore[arg-type]


def test_registry_model_is_content_addressed_and_fk_suitable() -> None:
    table = ForecastOutcomeResolutionPolicyRegistration.__table__

    assert table.name == "forecast_outcome_resolution_policies"
    assert tuple(column.name for column in table.primary_key) == ("policy_hash",)
    assert isinstance(table.c.canonical_policy.type, LargeBinary)
    assert isinstance(table.c.recorded_at.type, DateTime)
    assert table.c.recorded_at.type.timezone is True
    constraints = {str(item.name): item for item in table.constraints}
    assert {
        "ck_forecast_outcome_resolution_policies_policy_hash_matches_payload",
        "ck_forecast_outcome_resolution_policies_resolution_lag_bounded",
        "uq_forecast_outcome_resolution_policies_policy_rules",
    } <= constraints.keys()
    composite = constraints["uq_forecast_outcome_resolution_policies_policy_rules"]
    assert tuple(column.name for column in composite.columns) == (
        "policy_hash",
        "availability_rule_set_hash",
    )


async def test_register_uses_security_definer_boundary_and_fresh_committed_reread() -> None:
    database = _Database()

    proof = await _store(database).register(POLICY)

    canonical = json.dumps(
        POLICY.outcome_resolution_policy_document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode()
    assert proof.policy == POLICY
    assert proof.record.canonical_policy == canonical
    assert proof.record.policy_hash == "sha256:" + hashlib.sha256(canonical).hexdigest()
    assert proof.record.availability_rule_set_hash == POLICY.availability_rule_set_hash
    assert proof.record.resolution_lag_seconds == 86_400
    assert proof.record.recorded_at == RECORDED
    assert proof.record.creator_xid == 123
    assert len(database.registration_calls) == 1
    statement, params = database.registration_calls[0]
    assert "public.register_forecast_outcome_resolution_policy" in statement
    assert params == {"canonical_policy": canonical}
    assert database.session_count == 3
    with pytest.raises(FrozenInstanceError):
        proof.record.creator_xid = 1  # type: ignore[misc]


async def test_exact_registration_replays_without_another_insert() -> None:
    database = _Database()
    store = _store(database)
    first = await store.register(POLICY)

    replay = await store.register(POLICY)

    assert replay == first
    assert database.registration_attempts == 1


@pytest.mark.parametrize(
    "constraint",
    [
        "pk_forecast_outcome_resolution_policies",
        "uq_forecast_outcome_resolution_policies_policy_rules",
    ],
)
async def test_named_duplicate_race_replays_exact_winner(constraint: str) -> None:
    database = _Database()
    database.race_constraint = constraint

    proof = await _store(database).register(POLICY)

    assert proof.policy == POLICY
    assert database.registration_attempts == 1


async def test_unseen_named_race_is_retryable_write_conflict() -> None:
    database = _Database()
    database.execute_failure = (
        _integrity("pk_forecast_outcome_resolution_policies"),
        False,
    )

    with pytest.raises(AppError) as excinfo:
        await _store(database).register(POLICY)

    assert excinfo.value.code == "forecast_outcome_policy_write_conflict"
    assert excinfo.value.details == {"retryable": True}


async def test_corrupt_registration_is_never_replayed_as_success() -> None:
    database = _Database()
    store = _store(database)
    proof = await store.register(POLICY)
    database.records[proof.record.policy_hash].resolution_lag_seconds += 1

    with pytest.raises(AppError) as excinfo:
        await store.register(POLICY)

    assert excinfo.value.code == "forecast_outcome_policy_evidence_corrupt"
    assert excinfo.value.details == {"retryable": False}


async def test_invalid_policy_fails_before_opening_a_session() -> None:
    database = _Database()

    with pytest.raises(AppError) as excinfo:
        await _store(database).register(object())  # type: ignore[arg-type]

    assert excinfo.value.code == "forecast_outcome_policy_invalid"
    assert excinfo.value.details == {"retryable": False}
    assert database.session_count == 0


async def test_unknown_commit_reconciles_visible_exact_registration() -> None:
    database = _Database()
    database.commit_failure = (_operational(), True)

    proof = await _store(database).register(POLICY)

    assert proof.policy == POLICY
    assert database.lookups[-1] == proof.record.policy_hash


async def test_unknown_invisible_commit_reports_outcome_unknown() -> None:
    database = _Database()
    database.commit_failure = (_operational(), False)

    with pytest.raises(AppError) as excinfo:
        await _store(database).register(POLICY)

    assert excinfo.value.code == "forecast_outcome_policy_commit_unknown"
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

    proof = await _store(database).register(POLICY)

    assert proof.policy == POLICY


@pytest.mark.parametrize("failure_stage", ["execute", "commit"])
async def test_sqlstate_40003_without_visible_row_reports_unknown(failure_stage: str) -> None:
    database = _Database()
    failure = (_operational("40003"), False)
    if failure_stage == "execute":
        database.execute_failure = failure
    else:
        database.commit_failure = failure

    with pytest.raises(AppError) as excinfo:
        await _store(database).register(POLICY)

    assert excinfo.value.code == "forecast_outcome_policy_commit_unknown"
    assert excinfo.value.details == {"outcome_unknown": True, "retryable": True}


async def test_known_rollback_is_retryable_but_not_unknown() -> None:
    database = _Database()
    database.commit_failure = (_operational("40001"), False)

    with pytest.raises(AppError) as excinfo:
        await _store(database).register(POLICY)

    assert excinfo.value.code == "forecast_outcome_policy_store_unavailable"
    assert excinfo.value.details == {"retryable": True}


async def test_integrity_and_configuration_errors_are_classified_and_redacted() -> None:
    database = _Database()
    database.execute_failure = (_integrity("unexpected_check", "23514"), False)

    with pytest.raises(AppError) as integrity:
        await _store(database).register(POLICY)

    assert integrity.value.code == "forecast_outcome_policy_integrity_failed"
    assert integrity.value.details == {"retryable": False}
    assert "db.internal" not in integrity.value.message

    database = _Database()
    database.read_failures.append(_operational("42501"))

    with pytest.raises(AppError) as configuration:
        await _store(database).register(POLICY)

    assert configuration.value.code == "forecast_outcome_policy_configuration_invalid"
    assert configuration.value.details == {"retryable": False}
    assert "stockapi_app" not in configuration.value.message
