"""Snapshot and failure-posture tests for calibration-evidence reads."""

from __future__ import annotations

from dataclasses import fields

import pytest
from sqlalchemy.exc import OperationalError

import app.services.forecast_calibration_evidence_store as evidence_store_module
from app.core.exceptions import AppError
from app.db.models.forecast_evidence import (
    ForecastOutcomeCohortAvailability,
    ForecastOutcomeCohortManifest,
    ForecastOutcomeCohortMember,
    ForecastRealizedOutcome,
    ForecastRealizedOutcomePublication,
)
from app.db.models.predictions import ForecastRun
from app.services.forecast_calibration_evidence import join_calibration_evidence
from app.services.forecast_calibration_evidence_store import (
    _MAX_CANONICAL_EVIDENCE_BYTES,
    SqlForecastCalibrationEvidenceReader,
)
from tests.unit.test_forecast_calibration_evidence import _join_material


class _Result:
    def __init__(self, rows: list[object]) -> None:
        self.rows = rows

    def one_or_none(self) -> object | None:
        if len(self.rows) > 1:
            raise AssertionError("expected at most one row")
        return self.rows[0] if self.rows else None

    def scalars(self) -> _Result:
        return self

    def all(self) -> list[object]:
        return list(self.rows)

    def scalar_one(self) -> object:
        if len(self.rows) != 1:
            raise AssertionError("expected exactly one scalar row")
        return self.rows[0]


class _Transaction:
    def __init__(self, session: _Session) -> None:
        self.session = session

    async def __aenter__(self) -> _Transaction:
        assert not self.session.in_transaction
        self.session.in_transaction = True
        self.session.database.transaction_count += 1
        return self

    async def __aexit__(self, *_error: object) -> bool:
        self.session.in_transaction = False
        return False


class _Session:
    def __init__(self, database: _Database) -> None:
        self.database = database
        self.closed = False
        self.in_transaction = False

    async def __aenter__(self) -> _Session:
        return self

    async def __aexit__(self, *_error: object) -> bool:
        assert not self.in_transaction
        self.closed = True
        return False

    def begin(self) -> _Transaction:
        return _Transaction(self)

    async def execute(
        self,
        statement: object,
        params: object | None = None,
    ) -> _Result:
        sql = str(statement)
        self.database.statements.append(sql)
        self.database.params.append(params)
        call_number = len(self.database.statements)
        failure = self.database.failures.pop(call_number, None)
        if failure is not None:
            raise failure
        if sql.startswith("SET TRANSACTION"):
            return _Result([])
        if not self.database.results:
            raise AssertionError(f"unexpected query: {sql}")
        return _Result(self.database.results.pop(0))


class _Database:
    def __init__(self, results: list[list[object]]) -> None:
        self.results = results
        self.statements: list[str] = []
        self.params: list[object | None] = []
        self.sessions: list[_Session] = []
        self.transaction_count = 0
        self.failures: dict[int, Exception] = {}

    def __call__(self) -> _Session:
        session = _Session(self)
        self.sessions.append(session)
        return session


class _DriverFailure(Exception):
    def __init__(self, sqlstate: str) -> None:
        super().__init__("secret host=db.internal role=stockapi_app")
        self.sqlstate = sqlstate


def _operational(sqlstate: str) -> OperationalError:
    return OperationalError(
        "secret SELECT canonical_evidence",
        {},
        _DriverFailure(sqlstate),
    )


def _model_values(value: object) -> dict[str, object]:
    return {field.name: getattr(value, field.name) for field in fields(value)}


def _database(*, count: int = 4) -> tuple[_Database, object, tuple[object, ...]]:
    cohort, proofs = _join_material([101.0 + index for index in range(count)])
    manifest_row = ForecastOutcomeCohortManifest(**_model_values(cohort.record))
    seal_row = ForecastOutcomeCohortAvailability(**_model_values(cohort.seal))
    member_rows = [
        ForecastOutcomeCohortMember(
            cohort_id=cohort.record.cohort_id,
            forecast_id=member.forecast_id,
            step=member.step,
            target_time=member.target_time,
            opportunity_hash=member.opportunity_hash,
            output_hash=member.output_hash,
        )
        for member in cohort.manifest.members
    ]
    run_rows = [ForecastRun(**_model_values(proof.run)) for proof in proofs]
    publication_rows: list[object] = []
    for proof in proofs:
        publication_rows.append(
            (
                ForecastRealizedOutcomePublication(**_model_values(proof.outcome.publication)),
                ForecastRealizedOutcome(**_model_values(proof.outcome.record)),
            )
        )
    database = _Database(
        [
            [(manifest_row, seal_row)],
            [1024],
            list(member_rows),
            list(run_rows),
            publication_rows,
        ]
    )
    return database, cohort, proofs


def _reader(database: _Database) -> SqlForecastCalibrationEvidenceReader:
    return SqlForecastCalibrationEvidenceReader(
        sessionmaker=database,  # type: ignore[arg-type]
    )


async def test_read_uses_one_read_only_snapshot_then_validates_after_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, cohort, proofs = _database()
    original_join = evidence_store_module.join_calibration_evidence

    def guarded_join(*args: object):
        assert database.sessions
        assert all(session.closed and not session.in_transaction for session in database.sessions)
        return original_join(*args)  # type: ignore[arg-type]

    monkeypatch.setattr(evidence_store_module, "join_calibration_evidence", guarded_join)

    actual = await _reader(database).read_validated(cohort.record.cohort_id)  # type: ignore[union-attr]
    expected = join_calibration_evidence(cohort, proofs)  # type: ignore[arg-type]

    assert actual == expected
    assert database.transaction_count == 1
    assert len(database.sessions) == 1
    assert len(database.statements) == 6
    assert database.statements[0] == ("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
    assert "forecast_outcome_cohort_manifests" in database.statements[1]
    assert "canonical_evidence_bytes" in database.statements[2]
    assert ")::bigint AS canonical_evidence_bytes" in database.statements[2]
    assert database.params[2] == {"cohort_id": cohort.record.cohort_id}  # type: ignore[union-attr]
    assert "forecast_outcome_cohort_members" in database.statements[3]
    assert "forecast_runs" in database.statements[4]
    assert "forecast_realized_outcome_publications" in database.statements[5]
    assert database.results == []


async def test_missing_publication_is_retryable_not_ready() -> None:
    database, cohort, _proofs = _database()
    database.results[4] = database.results[4][:-1]

    with pytest.raises(AppError) as caught:
        await _reader(database).read_validated(cohort.record.cohort_id)  # type: ignore[union-attr]

    assert caught.value.code == "forecast_calibration_evidence_not_ready"
    assert caught.value.status_code == 409
    assert caught.value.details == {
        "cohort_id": cohort.record.cohort_id,  # type: ignore[union-attr]
        "missing_member_count": 1,
        "retryable": True,
    }


@pytest.mark.parametrize("shape", ["missing_outcome", "duplicate_publication"])
async def test_impossible_publication_shapes_are_corruption(shape: str) -> None:
    database, cohort, _proofs = _database()
    publication, outcome = database.results[4][0]  # type: ignore[misc]
    if shape == "missing_outcome":
        database.results[4][0] = (publication, None)
    else:
        database.results[4].append((publication, outcome))

    with pytest.raises(AppError) as caught:
        await _reader(database).read_validated(cohort.record.cohort_id)  # type: ignore[union-attr]

    assert caught.value.code == "forecast_calibration_evidence_corrupt"
    assert caught.value.status_code == 500
    assert caught.value.details == {"retryable": False}


@pytest.mark.parametrize("shape", ["missing_run", "member_projection_drift"])
async def test_archive_or_member_projection_gaps_are_corruption(shape: str) -> None:
    database, cohort, _proofs = _database()
    if shape == "missing_run":
        database.results[3] = database.results[3][:-1]
    else:
        member = database.results[2][0]
        assert isinstance(member, ForecastOutcomeCohortMember)
        member.output_hash = "sha256:" + "f" * 64

    with pytest.raises(AppError) as caught:
        await _reader(database).read_validated(cohort.record.cohort_id)  # type: ignore[union-attr]

    assert caught.value.code == "forecast_calibration_evidence_corrupt"
    assert caught.value.status_code == 500
    assert caught.value.details == {"retryable": False}


@pytest.mark.parametrize(
    ("header", "code", "status_code", "retryable"),
    [
        ("missing", "forecast_cohort_evidence_missing", 404, False),
        ("unsealed", "forecast_cohort_evidence_incomplete", 503, True),
    ],
)
async def test_missing_and_unsealed_cohort_keep_existing_taxonomy(
    header: str,
    code: str,
    status_code: int,
    retryable: bool,
) -> None:
    database, cohort, _proofs = _database()
    if header == "missing":
        database.results[0] = []
    else:
        manifest, _seal = database.results[0][0]  # type: ignore[misc]
        database.results[0] = [(manifest, None)]

    with pytest.raises(AppError) as caught:
        await _reader(database).read_validated(cohort.record.cohort_id)  # type: ignore[union-attr]

    assert caught.value.code == code
    assert caught.value.status_code == status_code
    assert caught.value.details["retryable"] is retryable  # type: ignore[index]


async def test_invalid_identity_fails_before_opening_a_session() -> None:
    database, _cohort, _proofs = _database()

    with pytest.raises(AppError) as caught:
        await _reader(database).read_validated("not-a-content-hash")

    assert caught.value.code == "forecast_calibration_evidence_request_invalid"
    assert caught.value.status_code == 422
    assert database.sessions == []
    assert database.statements == []


async def test_cumulative_size_preflight_refuses_before_materializing_heavy_rows() -> None:
    database, cohort, _proofs = _database()
    database.results[1] = [_MAX_CANONICAL_EVIDENCE_BYTES + 1]

    with pytest.raises(AppError) as caught:
        await _reader(database).read_validated(cohort.record.cohort_id)  # type: ignore[union-attr]

    assert caught.value.code == "forecast_calibration_evidence_too_large"
    assert caught.value.status_code == 422
    assert caught.value.details == {
        "max_canonical_bytes": _MAX_CANONICAL_EVIDENCE_BYTES,
        "retryable": False,
    }
    assert len(database.statements) == 3
    assert "canonical_evidence_bytes" in database.statements[-1]
    assert len(database.results) == 3


@pytest.mark.parametrize(
    ("sqlstate", "code", "retryable"),
    [
        ("08006", "forecast_calibration_evidence_store_unavailable", True),
        ("42P01", "forecast_calibration_evidence_configuration_invalid", False),
    ],
)
async def test_database_failures_are_classified_and_redacted(
    sqlstate: str,
    code: str,
    retryable: bool,
) -> None:
    database, cohort, _proofs = _database()
    database.failures[2] = _operational(sqlstate)

    with pytest.raises(AppError) as caught:
        await _reader(database).read_validated(cohort.record.cohort_id)  # type: ignore[union-attr]

    assert caught.value.code == code
    assert caught.value.details == {"retryable": retryable}
    assert "secret" not in caught.value.message
    assert "secret" not in str(caught.value.details)
