"""Transactional and failure-posture tests for forecast cohort persistence."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from sqlalchemy.exc import IntegrityError, OperationalError

from app.core.exceptions import AppError
from app.db.models.forecast_evidence import (
    ForecastOutcomeCohortAvailability,
    ForecastOutcomeCohortManifest,
    ForecastOutcomeCohortMember,
)
from app.services.forecast_cohort_store import SqlForecastCohortStore
from app.services.forecast_cohorts import (
    ForecastCohortManifest,
    ForecastCohortMember,
    ForecastCohortRecord,
    ForecastCohortSeal,
    build_cohort_record,
    parse_cohort_manifest,
    validate_cohort_seal,
)

NOW = datetime(2026, 7, 14, 12, tzinfo=UTC)
TARGET = NOW + timedelta(days=1)


def _manifest() -> ForecastCohortManifest:
    return ForecastCohortManifest(
        purpose="heldout_evaluation",
        selection_policy_hash="sha256:" + "a" * 64,
        outcome_resolution_policy_hash="sha256:" + "b" * 64,
        availability_rule_set_hash="sha256:" + "c" * 64,
        members=(
            ForecastCohortMember(
                forecast_id=UUID("11111111-1111-1111-1111-111111111111"),
                step=1,
                target_time=TARGET,
                opportunity_hash="sha256:" + "d" * 64,
                output_hash="sha256:" + "e" * 64,
            ),
        ),
    )


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


def _integrity(constraint: str, sqlstate: str) -> IntegrityError:
    return IntegrityError(
        "secret INSERT statement",
        {},
        _DriverFailure(sqlstate, constraint_name=constraint),
    )


def _operational(sqlstate: str | None = None) -> OperationalError:
    return OperationalError("secret COMMIT statement", {}, _DriverFailure(sqlstate))


def _record(row: ForecastOutcomeCohortManifest) -> ForecastCohortRecord:
    return ForecastCohortRecord(
        cohort_id=row.cohort_id,
        schema_version=row.schema_version,
        purpose=row.purpose,  # type: ignore[arg-type]
        selection_policy_hash=row.selection_policy_hash,
        outcome_resolution_policy_hash=row.outcome_resolution_policy_hash,
        availability_rule_set_hash=row.availability_rule_set_hash,
        member_count=row.member_count,
        earliest_target_time=row.earliest_target_time,
        latest_target_time=row.latest_target_time,
        recorded_at=row.recorded_at,
        creator_xid=row.creator_xid,
        canonical_manifest=row.canonical_manifest,
    )


def _apply_record(
    row: ForecastOutcomeCohortManifest,
    record: ForecastCohortRecord,
) -> ForecastOutcomeCohortManifest:
    for name in record.__dataclass_fields__:
        setattr(row, name, getattr(record, name))
    return row


def _apply_seal(
    row: ForecastOutcomeCohortAvailability,
    seal: ForecastCohortSeal,
) -> ForecastOutcomeCohortAvailability:
    row.cohort_id = seal.cohort_id
    row.manifest_recorded_at = seal.manifest_recorded_at
    row.sealed_at = seal.sealed_at
    row.sealer_xid = seal.sealer_xid
    return row


class _Transaction:
    def __init__(self, session: _Session) -> None:
        self.session = session

    async def __aenter__(self) -> _Transaction:
        self.session.xid = self.session.database.next_xid()
        return self

    async def __aexit__(self, *error: object) -> bool:
        if any(error):
            self.session.pending = None
            return False
        pending = self.session.pending
        if pending is None:
            return False
        kind, row = pending
        failure = self.session.database.commit_failures.pop(kind, None)
        if failure is None or failure[1]:
            self.session.database.commit(kind, row)
        self.session.pending = None
        if failure is not None:
            raise failure[0]
        return False


class _Session:
    def __init__(self, database: _Database) -> None:
        self.database = database
        self.pending: tuple[str, object] | None = None
        self.xid = 0

    async def __aenter__(self) -> _Session:
        return self

    async def __aexit__(self, *_error: object) -> bool:
        return False

    def begin(self) -> _Transaction:
        return _Transaction(self)

    async def get(self, model: object, identity: str) -> object | None:
        failure = self.database.get_failures.pop(0) if self.database.get_failures else None
        if failure is not None:
            raise failure
        if model is ForecastOutcomeCohortManifest:
            return self.database.records.get(identity)
        if model is ForecastOutcomeCohortAvailability:
            return self.database.seals.get(identity)
        raise AssertionError("unexpected model")

    async def execute(self, _statement: object) -> _ScalarResult:
        return _ScalarResult(self.database.members)

    def add(self, row: object) -> None:
        if self.pending is not None:
            raise AssertionError("one evidence row per transaction")
        kind = "manifest" if isinstance(row, ForecastOutcomeCohortManifest) else "seal"
        self.pending = (kind, row)

    async def flush(self) -> None:
        assert self.pending is not None
        kind, row = self.pending
        self.database.insert_attempts.append(kind)
        failure = self.database.flush_failures.pop(kind, None)
        if failure is not None:
            if kind in self.database.flush_completion_visible:
                self.database.flush_completion_visible.remove(kind)
                winner = self.database.stamp(kind, row, self.database.next_xid())
                self.database.commit(kind, winner)
            raise failure
        if kind in self.database.races:
            self.database.races.remove(kind)
            winner = self.database.stamp(kind, row, self.database.next_xid())
            self.database.commit(kind, winner)
            constraint = (
                "pk_forecast_outcome_cohort_manifests"
                if kind == "manifest"
                else "pk_forecast_outcome_cohort_availability"
            )
            raise _integrity(constraint, "23505")
        self.pending = (kind, self.database.stamp(kind, row, self.xid))

    async def refresh(self, _row: object) -> None:
        return None


class _Database:
    def __init__(self) -> None:
        self.records: dict[str, ForecastOutcomeCohortManifest] = {}
        self.seals: dict[str, ForecastOutcomeCohortAvailability] = {}
        self.members: list[ForecastOutcomeCohortMember] = []
        self.insert_attempts: list[str] = []
        self.session_count = 0
        self._xid = 100
        self.races: set[str] = set()
        self.flush_failures: dict[str, Exception] = {}
        self.flush_completion_visible: set[str] = set()
        self.commit_failures: dict[str, tuple[Exception, bool]] = {}
        self.get_failures: list[Exception | None] = []

    def __call__(self) -> _Session:
        self.session_count += 1
        return _Session(self)

    def next_xid(self) -> int:
        self._xid += 1
        return self._xid

    def stamp(self, kind: str, row: object, xid: int) -> object:
        if kind == "manifest":
            assert isinstance(row, ForecastOutcomeCohortManifest)
            manifest = parse_cohort_manifest(row.canonical_manifest)
            record = build_cohort_record(
                manifest,
                recorded_at=NOW + timedelta(minutes=1),
                creator_xid=xid,
            )
            return _apply_record(row, record)
        assert isinstance(row, ForecastOutcomeCohortAvailability)
        stored = self.records[row.cohort_id]
        record = _record(stored)
        seal = ForecastCohortSeal(
            cohort_id=record.cohort_id,
            manifest_recorded_at=record.recorded_at,
            sealed_at=NOW + timedelta(minutes=2),
            sealer_xid=xid,
        )
        validate_cohort_seal(record, seal)
        return _apply_seal(row, seal)

    def commit(self, kind: str, row: object) -> None:
        if kind == "manifest":
            assert isinstance(row, ForecastOutcomeCohortManifest)
            self.records[row.cohort_id] = row
            manifest = parse_cohort_manifest(row.canonical_manifest)
            self.members.extend(
                ForecastOutcomeCohortMember(
                    cohort_id=row.cohort_id,
                    forecast_id=member.forecast_id,
                    step=member.step,
                    target_time=member.target_time,
                    opportunity_hash=member.opportunity_hash,
                    output_hash=member.output_hash,
                )
                for member in manifest.members
            )
        else:
            assert isinstance(row, ForecastOutcomeCohortAvailability)
            self.seals[row.cohort_id] = row


class _ScalarResult:
    def __init__(self, rows: list[ForecastOutcomeCohortMember]) -> None:
        self.rows = rows

    def scalars(self) -> _ScalarResult:
        return self

    def all(self) -> list[ForecastOutcomeCohortMember]:
        return list(self.rows)


def _store(database: _Database) -> SqlForecastCohortStore:
    return SqlForecastCohortStore(sessionmaker=database)  # type: ignore[arg-type]


async def test_publish_uses_database_stamps_in_two_transactions_and_rereads_proof() -> None:
    database = _Database()

    proof = await _store(database).publish(_manifest())

    assert proof.record.recorded_at == NOW + timedelta(minutes=1)
    assert proof.seal.sealed_at == NOW + timedelta(minutes=2)
    assert proof.record.creator_xid > 0
    assert proof.seal.sealer_xid > 0
    assert proof.record.creator_xid != proof.seal.sealer_xid
    assert proof.seal.manifest_recorded_at == proof.record.recorded_at
    assert validate_cohort_seal(proof.record, proof.seal) == proof.manifest
    assert database.insert_attempts == ["manifest", "seal"]
    assert database.session_count == 3
    with pytest.raises(FrozenInstanceError):
        proof.record = proof.record  # type: ignore[misc]


async def test_exact_content_addressed_replay_does_not_insert_again() -> None:
    database = _Database()
    store = _store(database)
    first = await store.publish(_manifest())

    replay = await store.publish(_manifest())

    assert replay == first
    assert database.insert_attempts == ["manifest", "seal"]


@pytest.mark.parametrize("stage", ["manifest", "seal"])
async def test_duplicate_race_replays_the_exact_concurrent_winner(stage: str) -> None:
    database = _Database()
    database.races.add(stage)

    proof = await _store(database).publish(_manifest())

    assert validate_cohort_seal(proof.record, proof.seal) == proof.manifest
    assert database.insert_attempts == ["manifest", "seal"]


@pytest.mark.parametrize("stage", ["manifest", "seal"])
async def test_unknown_commit_reconciles_a_visible_exact_row(stage: str) -> None:
    database = _Database()
    database.commit_failures[stage] = (_operational(), True)

    proof = await _store(database).publish(_manifest())

    assert validate_cohort_seal(proof.record, proof.seal) == proof.manifest


@pytest.mark.parametrize("stage", ["manifest", "seal"])
async def test_unknown_invisible_commit_is_reported_honestly_and_is_safe_to_retry(
    stage: str,
) -> None:
    database = _Database()
    database.commit_failures[stage] = (_operational(), False)

    with pytest.raises(AppError) as excinfo:
        await _store(database).publish(_manifest())

    assert excinfo.value.code == "forecast_cohort_commit_unknown"
    assert excinfo.value.details == {
        "outcome_unknown": True,
        "retryable": True,
        "stage": stage,
    }
    assert "db.internal" not in excinfo.value.message


@pytest.mark.parametrize("stage", ["manifest", "seal"])
async def test_sqlstate_40003_commit_reconciles_a_visible_exact_row(stage: str) -> None:
    database = _Database()
    database.commit_failures[stage] = (_operational("40003"), True)

    proof = await _store(database).publish(_manifest())

    assert validate_cohort_seal(proof.record, proof.seal) == proof.manifest


@pytest.mark.parametrize("stage", ["manifest", "seal"])
async def test_sqlstate_40003_without_visible_row_reports_commit_unknown(stage: str) -> None:
    database = _Database()
    database.commit_failures[stage] = (_operational("40003"), False)

    with pytest.raises(AppError) as excinfo:
        await _store(database).publish(_manifest())

    assert excinfo.value.code == "forecast_cohort_commit_unknown"
    assert excinfo.value.details == {
        "outcome_unknown": True,
        "retryable": True,
        "stage": stage,
    }


@pytest.mark.parametrize("stage", ["manifest", "seal"])
async def test_sqlstate_40003_during_execute_reconciles_visible_exact_row(stage: str) -> None:
    database = _Database()
    database.flush_failures[stage] = _operational("40003")
    database.flush_completion_visible.add(stage)

    proof = await _store(database).publish(_manifest())

    assert validate_cohort_seal(proof.record, proof.seal) == proof.manifest


@pytest.mark.parametrize("stage", ["manifest", "seal"])
async def test_sqlstate_40003_during_execute_without_row_reports_unknown(stage: str) -> None:
    database = _Database()
    database.flush_failures[stage] = _operational("40003")

    with pytest.raises(AppError) as excinfo:
        await _store(database).publish(_manifest())

    assert excinfo.value.code == "forecast_cohort_commit_unknown"
    assert excinfo.value.details == {
        "outcome_unknown": True,
        "retryable": True,
        "stage": stage,
    }


async def test_known_rollback_is_retryable_but_not_reported_as_unknown() -> None:
    database = _Database()
    database.commit_failures["manifest"] = (_operational("40001"), False)

    with pytest.raises(AppError) as excinfo:
        await _store(database).publish(_manifest())

    assert excinfo.value.code == "forecast_cohort_store_unavailable"
    assert excinfo.value.details == {"retryable": True}


async def test_configuration_error_is_non_retryable_and_redacted() -> None:
    database = _Database()
    database.get_failures.append(_operational("42501"))

    with pytest.raises(AppError) as excinfo:
        await _store(database).publish(_manifest())

    assert excinfo.value.code == "forecast_cohort_configuration_invalid"
    assert excinfo.value.details == {"retryable": False}
    assert "db.internal" not in excinfo.value.message
    assert "stockapi_app" not in excinfo.value.message


@pytest.mark.parametrize(
    ("stage", "sqlstate", "constraint", "expected_code"),
    [
        (
            "manifest",
            "23514",
            "ck_forecast_outcome_cohort_manifests_time_order",
            "forecast_cohort_deadline_expired",
        ),
        (
            "manifest",
            "23503",
            "fk_forecast_outcome_cohort_members_forecast_id_forecast_runs",
            "forecast_cohort_source_unavailable",
        ),
        ("seal", "55000", "cohort_late", "forecast_cohort_deadline_expired"),
    ],
)
async def test_structured_integrity_errors_are_classified_without_driver_text(
    stage: str,
    sqlstate: str,
    constraint: str,
    expected_code: str,
) -> None:
    database = _Database()
    database.flush_failures[stage] = (
        _operational(sqlstate) if sqlstate == "55000" else _integrity(constraint, sqlstate)
    )

    with pytest.raises(AppError) as excinfo:
        await _store(database).publish(_manifest())

    assert excinfo.value.code == expected_code
    assert excinfo.value.details == {"retryable": False, "stage": stage}
    assert "db.internal" not in excinfo.value.message
    assert "stockapi_app" not in excinfo.value.message


@pytest.mark.parametrize(
    ("stage", "failure"),
    [
        ("manifest", _operational("55000")),
        (
            "seal",
            _integrity(
                "fk_forecast_outcome_cohort_availability_cohort_id_"
                "forecast_outcome_cohort_manifests",
                "23503",
            ),
        ),
    ],
)
async def test_integrity_sqlstates_are_not_overclassified_outside_their_stage(
    stage: str,
    failure: Exception,
) -> None:
    database = _Database()
    database.flush_failures[stage] = failure

    with pytest.raises(AppError) as excinfo:
        await _store(database).publish(_manifest())

    assert excinfo.value.code == "forecast_cohort_integrity_failed"
    assert excinfo.value.details == {"retryable": False, "stage": stage}
    assert "db.internal" not in excinfo.value.message


async def test_ambiguous_unique_violation_is_not_treated_as_primary_key_race() -> None:
    database = _Database()
    database.flush_failures["manifest"] = _integrity("", "23505")

    with pytest.raises(AppError) as excinfo:
        await _store(database).publish(_manifest())

    assert excinfo.value.code == "forecast_cohort_integrity_failed"
    assert excinfo.value.details == {"retryable": False, "stage": "manifest"}


async def test_corrupt_exact_replay_is_never_returned_as_success() -> None:
    database = _Database()
    store = _store(database)
    first = await store.publish(_manifest())
    database.records[first.record.cohort_id].member_count += 1

    with pytest.raises(AppError) as excinfo:
        await store.publish(_manifest())

    assert excinfo.value.code == "forecast_cohort_evidence_corrupt"
    assert excinfo.value.details == {"retryable": False}


async def test_invalid_manifest_fails_before_opening_a_database_session() -> None:
    database = _Database()
    invalid = replace(_manifest(), members=())

    with pytest.raises(AppError) as excinfo:
        await _store(database).publish(invalid)

    assert excinfo.value.code == "forecast_cohort_manifest_invalid"
    assert database.session_count == 0


async def test_read_validated_returns_a_fresh_exact_sealed_proof() -> None:
    database = _Database()
    store = _store(database)
    published = await store.publish(_manifest())

    reread = await store.read_validated(published.record.cohort_id)

    assert reread == published
    assert validate_cohort_seal(reread.record, reread.seal) == reread.manifest
    assert database.insert_attempts == ["manifest", "seal"]
    assert database.session_count == 4


async def test_read_validated_distinguishes_a_missing_manifest_from_an_outage() -> None:
    database = _Database()
    store = _store(database)
    published = await store.publish(_manifest())
    del database.records[published.record.cohort_id]

    with pytest.raises(AppError) as excinfo:
        await store.read_validated(published.record.cohort_id)

    assert excinfo.value.code == "forecast_cohort_evidence_missing"
    assert excinfo.value.status_code == 404
    assert excinfo.value.details == {
        "cohort_id": published.record.cohort_id,
        "retryable": False,
    }


async def test_read_validated_reports_a_missing_seal_as_incomplete_and_retryable() -> None:
    database = _Database()
    store = _store(database)
    published = await store.publish(_manifest())
    del database.seals[published.record.cohort_id]

    with pytest.raises(AppError) as excinfo:
        await store.read_validated(published.record.cohort_id)

    assert excinfo.value.code == "forecast_cohort_evidence_incomplete"
    assert excinfo.value.status_code == 503
    assert excinfo.value.details == {"retryable": True}


@pytest.mark.parametrize("corruption", ["member_count", "member_projection", "seal_xid"])
async def test_read_validated_refuses_corrupt_membership_or_seal_projection(
    corruption: str,
) -> None:
    database = _Database()
    store = _store(database)
    published = await store.publish(_manifest())
    if corruption == "member_count":
        database.records[published.record.cohort_id].member_count += 1
    elif corruption == "member_projection":
        database.members[0].output_hash = "sha256:" + "0" * 64
    else:
        database.seals[published.record.cohort_id].sealer_xid = published.record.creator_xid

    with pytest.raises(AppError) as excinfo:
        await store.read_validated(published.record.cohort_id)

    assert excinfo.value.code == "forecast_cohort_evidence_corrupt"
    assert excinfo.value.status_code == 500
    assert excinfo.value.details == {"retryable": False}
