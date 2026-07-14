"""Transactional and failure-posture tests for realized-outcome persistence."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from sqlalchemy.exc import IntegrityError, OperationalError

from app.core.exceptions import AppError
from app.db.models.forecast_evidence import (
    ForecastRealizedOutcome,
    ForecastRealizedOutcomePublication,
)
from app.services.forecast_outcome_store import (
    ForecastOutcomePublicationSource,
    SqlForecastOutcomeStore,
)
from app.services.forecast_outcomes import (
    BarVersionEvidence,
    RealizedOutcomePayload,
    RealizedOutcomeRecord,
    build_outcome_record,
    parse_outcome_payload,
    validate_outcome_record,
)

POLICY_HASH = "sha256:" + "a" * 64
RULES_HASH = "sha256:" + "b" * 64
TARGET = datetime(2026, 7, 13, 20, tzinfo=UTC)
AVAILABLE = TARGET + timedelta(minutes=4)
CUTOFF = TARGET + timedelta(days=1)
SEALED = CUTOFF + timedelta(minutes=1)
PUBLISHED = SEALED + timedelta(seconds=1)
PUBLICATION_SOURCE = ForecastOutcomePublicationSource(
    cohort_id="sha256:" + "c" * 64,
    forecast_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
    step=1,
)


def _payload(*, cutoff: datetime = CUTOFF) -> RealizedOutcomePayload:
    return RealizedOutcomePayload(
        outcome_resolution_policy_hash=POLICY_HASH,
        availability_rule_set_hash=RULES_HASH,
        resolution_cutoff=cutoff,
        symbol="MSFT",
        target="close",
        series_basis="raw",
        target_time=TARGET,
        currency="USD",
        realized_value=512.25,
        source_version=BarVersionEvidence(
            symbol="MSFT",
            timespan="day",
            multiplier=1,
            observed_at=TARGET,
            source="polygon_open_close",
            adjustment_basis="raw",
            fetched_at=TARGET + timedelta(minutes=1),
            source_as_of=TARGET + timedelta(minutes=2),
            version_recorded_at=TARGET + timedelta(minutes=3),
            available_at=AVAILABLE,
            field="close",
            value=512.25,
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


def _integrity(constraint: str, sqlstate: str = "23505") -> IntegrityError:
    return IntegrityError(
        "secret INSERT statement",
        {},
        _DriverFailure(sqlstate, constraint_name=constraint),
    )


def _operational(sqlstate: str | None = None) -> OperationalError:
    return OperationalError("secret COMMIT statement", {}, _DriverFailure(sqlstate))


def _apply_record(
    row: ForecastRealizedOutcome,
    record: RealizedOutcomeRecord,
) -> ForecastRealizedOutcome:
    for name in record.__dataclass_fields__:
        setattr(row, name, getattr(record, name))
    return row


def _publication(
    outcome_id: str,
    source: ForecastOutcomePublicationSource,
) -> ForecastRealizedOutcomePublication:
    return ForecastRealizedOutcomePublication(
        outcome_id=outcome_id,
        cohort_id=source.cohort_id,
        forecast_id=source.forecast_id,
        step=source.step,
        published_at=PUBLISHED,
        publisher_xid=456,
    )


def _publication_key(
    row: ForecastRealizedOutcomePublication,
) -> tuple[str, str, UUID, int]:
    return row.outcome_id, row.cohort_id, row.forecast_id, row.step


class _ScalarResult:
    def __init__(self, row: ForecastRealizedOutcome | None) -> None:
        self.row = row

    def one_or_none(self) -> ForecastRealizedOutcome | None:
        return self.row


class _Result:
    def __init__(
        self,
        row: ForecastRealizedOutcome | None = None,
        scalar: object = None,
    ) -> None:
        self.row = row
        self.scalar = scalar

    def scalars(self) -> _ScalarResult:
        return _ScalarResult(self.row)

    def scalar_one(self) -> object:
        return self.scalar


class _Transaction:
    def __init__(self, session: _Session) -> None:
        self.session = session

    async def __aenter__(self) -> _Transaction:
        return self

    async def __aexit__(self, *error: object) -> bool:
        if any(error):
            self.session.pending_outcome = None
            self.session.pending_publication = None
            return False
        outcome = self.session.pending_outcome
        publication = self.session.pending_publication
        if outcome is None and publication is None:
            return False
        failure = self.session.database.commit_failure
        if failure is None or failure[1]:
            self.session.database.commit_pair(
                outcome,
                None if self.session.database.omit_publication_on_visible_commit else publication,
            )
        self.session.pending_outcome = None
        self.session.pending_publication = None
        if failure is not None:
            self.session.database.commit_failure = None
            raise failure[0]
        return False


class _Session:
    def __init__(self, database: _Database) -> None:
        self.database = database
        self.pending_outcome: ForecastRealizedOutcome | None = None
        self.pending_publication: ForecastRealizedOutcomePublication | None = None

    async def __aenter__(self) -> _Session:
        return self

    async def __aexit__(self, *_error: object) -> bool:
        return False

    def begin(self) -> _Transaction:
        return _Transaction(self)

    async def get(
        self,
        model: object,
        identity: object,
    ) -> ForecastRealizedOutcome | ForecastRealizedOutcomePublication | None:
        if self.database.read_failures:
            failure = self.database.read_failures.pop(0)
            if failure is not None:
                raise failure
        if model is ForecastRealizedOutcome:
            assert isinstance(identity, str)
            self.database.lookup_log.append(("id", identity))
            if self.pending_outcome is not None and self.pending_outcome.outcome_id == identity:
                return self.pending_outcome
            return self.database.records.get(identity)
        assert model is ForecastRealizedOutcomePublication
        assert isinstance(identity, tuple)
        self.database.lookup_log.append(("publication", identity))
        if (
            self.pending_publication is not None
            and _publication_key(self.pending_publication) == identity
        ):
            return self.pending_publication
        return self.database.publications.get(identity)

    async def execute(
        self,
        statement: object,
        supplied_params: dict[str, object] | None = None,
    ) -> _Result:
        sql = str(statement)
        if sql.startswith("SET ") or "pg_advisory_xact_lock" in sql:
            self.database.transaction_statements.append(sql)
            return _Result()
        if "publish_forecast_realized_outcome" in sql:
            assert supplied_params is not None
            self.database.publication_calls.append(dict(supplied_params))
            self.database.insert_attempts += 1
            canonical = supplied_params["canonical_evidence"]
            outcome_id = supplied_params["outcome_id"]
            assert isinstance(canonical, bytes)
            assert isinstance(outcome_id, str)
            candidate = ForecastRealizedOutcome(
                outcome_id=outcome_id,
                canonical_evidence=canonical,
            )
            payload = parse_outcome_payload(canonical)
            record = build_outcome_record(payload, sealed_at=SEALED)
            candidate = self.database.records.get(outcome_id) or _apply_record(candidate, record)
            forecast_id = supplied_params["forecast_id"]
            forecast_step = supplied_params["forecast_step"]
            assert isinstance(forecast_id, UUID)
            assert isinstance(forecast_step, int)
            publication = _publication(
                outcome_id,
                ForecastOutcomePublicationSource(
                    cohort_id=str(supplied_params["cohort_id"]),
                    forecast_id=forecast_id,
                    step=forecast_step,
                ),
            )
            self.pending_outcome = candidate
            self.pending_publication = publication
            if self.database.execute_failure is not None:
                failure, visible = self.database.execute_failure
                self.database.execute_failure = None
                if visible:
                    self.database.commit_pair(
                        candidate,
                        None if self.database.omit_publication_on_visible_commit else publication,
                    )
                raise failure
            if self.database.race_constraint is not None:
                constraint = self.database.race_constraint
                self.database.race_constraint = None
                winner_payload = self.database.race_winner or parse_outcome_payload(canonical)
                winner_record = build_outcome_record(
                    winner_payload,
                    sealed_at=max(
                        SEALED,
                        winner_payload.resolution_cutoff + timedelta(minutes=1),
                    ),
                )
                winner = ForecastRealizedOutcome(
                    outcome_id=winner_record.outcome_id,
                    canonical_evidence=winner_record.canonical_evidence,
                )
                winner = _apply_record(winner, winner_record)
                self.database.commit_pair(
                    winner,
                    _publication(winner.outcome_id, PUBLICATION_SOURCE),
                )
                raise _integrity(constraint)
            return _Result(scalar=outcome_id)
        params = statement.compile().params  # type: ignore[attr-defined]
        key = (
            params["outcome_resolution_policy_hash_1"],
            params["availability_rule_set_hash_1"],
            params["symbol_1"],
            params["target_1"],
            params["series_basis_1"],
            params["target_time_1"],
        )
        self.database.lookup_log.append(("semantic", key))
        if self.database.read_failures:
            failure = self.database.read_failures.pop(0)
            if failure is not None:
                raise failure
        return _Result(self.database.semantic_records.get(key))


class _Database:
    def __init__(self) -> None:
        self.records: dict[str, ForecastRealizedOutcome] = {}
        self.semantic_records: dict[tuple[object, ...], ForecastRealizedOutcome] = {}
        self.publications: dict[tuple[str, str, UUID, int], ForecastRealizedOutcomePublication] = {}
        self.session_count = 0
        self.insert_attempts = 0
        self.publication_calls: list[dict[str, object]] = []
        self.transaction_statements: list[str] = []
        self.lookup_log: list[tuple[str, object]] = []
        self.race_constraint: str | None = None
        self.race_winner: RealizedOutcomePayload | None = None
        self.execute_failure: tuple[Exception, bool] | None = None
        self.commit_failure: tuple[Exception, bool] | None = None
        self.omit_publication_on_visible_commit = False
        self.read_failures: list[Exception | None] = []

    def __call__(self) -> _Session:
        self.session_count += 1
        return _Session(self)

    def commit(self, row: ForecastRealizedOutcome) -> None:
        self.records[row.outcome_id] = row
        key = (
            row.outcome_resolution_policy_hash,
            row.availability_rule_set_hash,
            row.symbol,
            row.target,
            row.series_basis,
            row.target_time,
        )
        self.semantic_records[key] = row

    def commit_pair(
        self,
        outcome: ForecastRealizedOutcome | None,
        publication: ForecastRealizedOutcomePublication | None,
    ) -> None:
        if outcome is not None:
            self.commit(outcome)
        if publication is not None:
            self.publications[_publication_key(publication)] = publication


class _StoreHarness:
    def __init__(self, database: _Database) -> None:
        self.store = SqlForecastOutcomeStore(
            sessionmaker=database,  # type: ignore[arg-type]
            outcome_resolution_policy_hash=POLICY_HASH,
            availability_rule_set_hash=RULES_HASH,
        )

    async def publish(
        self,
        payload: RealizedOutcomePayload,
        *,
        source: ForecastOutcomePublicationSource = PUBLICATION_SOURCE,
    ):
        return await self.store.publish(payload, source=source)


def _store(database: _Database) -> _StoreHarness:
    return _StoreHarness(database)


async def test_publish_uses_trigger_only_fields_and_fresh_committed_reread() -> None:
    database = _Database()

    proof = await _store(database).publish(_payload())

    assert proof.record.sealed_at == SEALED
    assert proof.record.bar_available_at == AVAILABLE
    assert proof.publication.outcome_id == proof.record.outcome_id
    assert proof.publication.cohort_id == PUBLICATION_SOURCE.cohort_id
    assert proof.publication.forecast_id == PUBLICATION_SOURCE.forecast_id
    assert proof.publication.step == PUBLICATION_SOURCE.step
    assert proof.publication.published_at == PUBLISHED
    assert proof.publication.publisher_xid == 456
    assert (
        validate_outcome_record(
            proof.record,
            expected_outcome_resolution_policy_hash=POLICY_HASH,
            expected_availability_rule_set_hash=RULES_HASH,
        )
        == proof.payload
    )
    assert database.publication_calls == [
        {
            "cohort_id": PUBLICATION_SOURCE.cohort_id,
            "forecast_id": PUBLICATION_SOURCE.forecast_id,
            "forecast_step": PUBLICATION_SOURCE.step,
            "outcome_id": proof.record.outcome_id,
            "canonical_evidence": proof.record.canonical_evidence,
        }
    ]
    assert database.transaction_statements[0].startswith(
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
    )
    assert database.insert_attempts == 1
    # One preflight, one short write transaction, and one post-commit reread.
    assert database.session_count == 3
    with pytest.raises(FrozenInstanceError):
        proof.record = proof.record  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        proof.publication.step = 2  # type: ignore[misc]


async def test_exact_content_and_semantic_replay_does_not_insert_again() -> None:
    database = _Database()
    store = _store(database)
    first = await store.publish(_payload())

    replay = await store.publish(_payload())

    assert replay == first
    assert database.insert_attempts == 1


async def test_replay_without_requested_link_reenters_publisher_and_adds_provenance() -> None:
    database = _Database()
    store = _store(database)
    first = await store.publish(_payload())
    second_source = ForecastOutcomePublicationSource(
        cohort_id="sha256:" + "d" * 64,
        forecast_id=UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        step=2,
    )

    linked = await store.publish(_payload(), source=second_source)

    assert linked.payload == first.payload
    assert linked.record == first.record
    assert linked.publication.cohort_id == second_source.cohort_id
    assert linked.publication.forecast_id == second_source.forecast_id
    assert linked.publication.step == second_source.step
    assert database.insert_attempts == 2
    assert set(database.publications) == {
        _publication_key(_publication(first.record.outcome_id, PUBLICATION_SOURCE)),
        _publication_key(_publication(first.record.outcome_id, second_source)),
    }


@pytest.mark.parametrize(
    "constraint",
    [
        "pk_forecast_realized_outcomes",
        "uq_forecast_realized_outcomes_semantic_key",
    ],
)
async def test_named_duplicate_race_replays_the_exact_winner(constraint: str) -> None:
    database = _Database()
    database.race_constraint = constraint

    proof = await _store(database).publish(_payload())

    assert proof.payload == _payload()
    assert database.insert_attempts == 1


async def test_semantic_race_with_different_valid_evidence_is_a_conflict() -> None:
    database = _Database()
    database.race_constraint = "uq_forecast_realized_outcomes_semantic_key"
    database.race_winner = _payload(cutoff=CUTOFF + timedelta(hours=1))

    with pytest.raises(AppError) as excinfo:
        await _store(database).publish(_payload())

    assert excinfo.value.code == "forecast_outcome_semantic_conflict"
    assert excinfo.value.details == {"retryable": False}


async def test_preflight_detects_an_existing_semantic_conflict() -> None:
    database = _Database()
    winner_payload = _payload(cutoff=CUTOFF + timedelta(hours=1))
    winner_record = build_outcome_record(winner_payload, sealed_at=SEALED + timedelta(hours=1))
    winner = _apply_record(
        ForecastRealizedOutcome(
            outcome_id=winner_record.outcome_id,
            canonical_evidence=winner_record.canonical_evidence,
        ),
        winner_record,
    )
    database.commit(winner)

    with pytest.raises(AppError) as excinfo:
        await _store(database).publish(_payload())

    assert excinfo.value.code == "forecast_outcome_semantic_conflict"
    assert database.insert_attempts == 0


async def test_unknown_commit_reconciles_visible_exact_row_by_content_id() -> None:
    database = _Database()
    database.commit_failure = (_operational(), True)

    proof = await _store(database).publish(_payload())

    assert proof.payload == _payload()
    assert database.lookup_log[-2:][0][0] == "id"
    assert database.lookup_log[-1][0] == "publication"


async def test_unknown_invisible_commit_checks_id_then_semantics_and_reports_unknown() -> None:
    database = _Database()
    database.commit_failure = (_operational(), False)

    with pytest.raises(AppError) as excinfo:
        await _store(database).publish(_payload())

    assert excinfo.value.code == "forecast_outcome_commit_unknown"
    assert excinfo.value.details == {"outcome_unknown": True, "retryable": True}
    assert [kind for kind, _ in database.lookup_log[-2:]] == ["id", "semantic"]
    assert "db.internal" not in excinfo.value.message


@pytest.mark.parametrize("failure_stage", ["execute", "commit"])
async def test_sqlstate_40003_reconciles_visible_exact_proof(failure_stage: str) -> None:
    database = _Database()
    failure = (_operational("40003"), True)
    if failure_stage == "execute":
        database.execute_failure = failure
    else:
        database.commit_failure = failure

    proof = await _store(database).publish(_payload())

    assert proof.payload == _payload()
    assert database.lookup_log[-1][0] == "publication"


@pytest.mark.parametrize("failure_stage", ["execute", "commit"])
async def test_sqlstate_40003_without_visible_proof_reports_commit_unknown(
    failure_stage: str,
) -> None:
    database = _Database()
    failure = (_operational("40003"), False)
    if failure_stage == "execute":
        database.execute_failure = failure
    else:
        database.commit_failure = failure

    with pytest.raises(AppError) as excinfo:
        await _store(database).publish(_payload())

    assert excinfo.value.code == "forecast_outcome_commit_unknown"
    assert excinfo.value.details == {"outcome_unknown": True, "retryable": True}
    assert [kind for kind, _ in database.lookup_log[-2:]] == ["id", "semantic"]


async def test_unknown_commit_with_visible_outcome_but_missing_link_is_not_success() -> None:
    database = _Database()
    database.commit_failure = (_operational(), True)
    database.omit_publication_on_visible_commit = True

    with pytest.raises(AppError) as excinfo:
        await _store(database).publish(_payload())

    assert excinfo.value.code == "forecast_outcome_commit_unknown"
    assert excinfo.value.details == {"outcome_unknown": True, "retryable": True}
    assert len(database.records) == 1
    assert database.publications == {}


async def test_known_rollback_is_retryable_but_not_reported_as_unknown() -> None:
    database = _Database()
    database.commit_failure = (_operational("40001"), False)

    with pytest.raises(AppError) as excinfo:
        await _store(database).publish(_payload())

    assert excinfo.value.code == "forecast_outcome_store_unavailable"
    assert excinfo.value.details == {"retryable": True}


@pytest.mark.parametrize(
    ("failure", "expected_code", "retryable"),
    [
        (
            _integrity(
                "ck_forecast_realized_outcomes_evidence_time_order",
                "23514",
            ),
            "forecast_outcome_resolution_not_ready",
            True,
        ),
        (
            _integrity(
                "fk_forecast_realized_outcomes_exact_bar_receipt_bar_version_availability",
                "23503",
            ),
            "forecast_outcome_source_unavailable",
            False,
        ),
        (
            _integrity("unrelated_unique", "23505"),
            "forecast_outcome_integrity_failed",
            False,
        ),
    ],
)
async def test_integrity_failures_are_structured_and_redacted(
    failure: Exception,
    expected_code: str,
    retryable: bool,
) -> None:
    database = _Database()
    database.execute_failure = (failure, False)

    with pytest.raises(AppError) as excinfo:
        await _store(database).publish(_payload())

    assert excinfo.value.code == expected_code
    assert excinfo.value.details == {"retryable": retryable}
    assert "db.internal" not in excinfo.value.message
    assert "stockapi_app" not in excinfo.value.message


async def test_unseen_named_race_is_a_retryable_write_conflict() -> None:
    database = _Database()
    database.execute_failure = (_integrity("pk_forecast_realized_outcomes"), False)

    with pytest.raises(AppError) as excinfo:
        await _store(database).publish(_payload())

    assert excinfo.value.code == "forecast_outcome_write_conflict"
    assert excinfo.value.details == {"retryable": True}


async def test_configuration_error_is_nonretryable_and_redacted() -> None:
    database = _Database()
    database.read_failures.append(_operational("42501"))

    with pytest.raises(AppError) as excinfo:
        await _store(database).publish(_payload())

    assert excinfo.value.code == "forecast_outcome_configuration_invalid"
    assert excinfo.value.details == {"retryable": False}
    assert "db.internal" not in excinfo.value.message


async def test_corrupt_exact_replay_is_never_returned_as_success() -> None:
    database = _Database()
    store = _store(database)
    first = await store.publish(_payload())
    database.records[first.record.outcome_id].realized_value += 1.0

    with pytest.raises(AppError) as excinfo:
        await store.publish(_payload())

    assert excinfo.value.code == "forecast_outcome_evidence_corrupt"
    assert excinfo.value.details == {"retryable": False}


async def test_corrupt_or_mismatched_publication_is_never_replayed_as_success() -> None:
    database = _Database()
    store = _store(database)
    first = await store.publish(_payload())
    key = _publication_key(_publication(first.record.outcome_id, PUBLICATION_SOURCE))
    database.publications[key].cohort_id = "sha256:" + "e" * 64

    with pytest.raises(AppError) as excinfo:
        await store.publish(_payload())

    assert excinfo.value.code == "forecast_outcome_evidence_corrupt"
    assert excinfo.value.details == {"retryable": False}


@pytest.mark.parametrize(
    ("store", "payload", "expected_code"),
    [
        (
            SqlForecastOutcomeStore(
                sessionmaker=None,  # type: ignore[arg-type]
                outcome_resolution_policy_hash="not-a-hash",
                availability_rule_set_hash=RULES_HASH,
            ),
            _payload(),
            "forecast_outcome_configuration_invalid",
        ),
        (
            SqlForecastOutcomeStore(
                sessionmaker=None,  # type: ignore[arg-type]
                outcome_resolution_policy_hash=POLICY_HASH,
                availability_rule_set_hash=RULES_HASH,
            ),
            replace(_payload(), currency="usd"),
            "forecast_outcome_invalid",
        ),
        (
            SqlForecastOutcomeStore(
                sessionmaker=None,  # type: ignore[arg-type]
                outcome_resolution_policy_hash="sha256:" + "c" * 64,
                availability_rule_set_hash=RULES_HASH,
            ),
            _payload(),
            "forecast_outcome_policy_mismatch",
        ),
    ],
)
async def test_invalid_input_or_policy_fails_before_opening_a_session(
    store: SqlForecastOutcomeStore,
    payload: RealizedOutcomePayload,
    expected_code: str,
) -> None:
    with pytest.raises(AppError) as excinfo:
        await store.publish(payload, source=PUBLICATION_SOURCE)

    assert excinfo.value.code == expected_code
    assert excinfo.value.details == {"retryable": False}
