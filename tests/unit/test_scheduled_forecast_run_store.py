"""Race and readback tests for scheduled forecast-run persistence."""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID

import pytest
from sqlalchemy.exc import OperationalError

from app.core.exceptions import AppError
from app.db.models.predictions import ForecastRun
from app.schemas.forecast import ForecastRequest, ForecastResponse
from app.services.forecast_run_store import ArchivedForecastRun, SqlForecastRunStore
from app.services.forecast_runs import canonical_output, canonical_request, request_hash
from tests.unit.test_forecast_runs import (
    AS_OF,
    POLICY_HASH,
    RULE_SET_HASH,
    _FakeSessionMaker,
    _integrity_error,
    _matching_request,
    _operational_error,
    _response,
)

_SCHEDULED_UNIQUE = "uq_forecast_runs_scheduled_opportunity"


def _store(maker: _FakeSessionMaker) -> SqlForecastRunStore:
    return SqlForecastRunStore(
        sessionmaker=maker,  # type: ignore[arg-type]
        identity_secret="fixture-archive-secret",
        resolution_policy_hash=POLICY_HASH,
        availability_rule_set_hash=RULE_SET_HASH,
        origin_kind="scheduled_evaluation",
    )


def _persisted_row(
    store: SqlForecastRunStore,
    request: ForecastRequest,
    response: ForecastResponse,
) -> ForecastRun:
    payload = canonical_request(request)
    row = store._row(
        request_payload=payload,
        request_identity=request_hash(payload),
        retry_identity=None,
        response=response,
    )
    row.recorded_at = AS_OF + timedelta(minutes=6)
    return row


def _different_run(response: ForecastResponse) -> ForecastResponse:
    provenance = response.provenance.model_copy(
        update={"forecast_id": UUID("44444444-4444-4444-4444-444444444444")}
    )
    return response.model_copy(update={"provenance": provenance})


async def test_scheduled_unique_loser_replays_the_validated_winner() -> None:
    maker = _FakeSessionMaker(AS_OF + timedelta(minutes=5))
    store = _store(maker)
    winner_response = _response()
    request = _matching_request(winner_response)
    winner = _persisted_row(store, request, winner_response)
    candidate = _different_run(winner_response)
    maker.flush_error = _integrity_error(_SCHEDULED_UNIQUE, "23505")
    maker.reconcile_row = winner

    async def _producer() -> ForecastResponse:
        return candidate

    replayed = await store.execute(
        request,
        idempotency_key=None,
        principal=None,
        producer=_producer,
    )

    assert replayed.provenance.forecast_id == winner_response.provenance.forecast_id
    assert replayed.provenance.forecast_id != candidate.provenance.forecast_id
    assert canonical_output(replayed) == canonical_output(winner_response)


async def test_scheduled_unique_refuses_a_winner_for_a_different_canonical_request() -> None:
    maker = _FakeSessionMaker(AS_OF + timedelta(minutes=5))
    store = _store(maker)
    response = _response()
    request = _matching_request(response)
    different_request = request.model_copy(update={"model": "baseline_naive"})
    maker.flush_error = _integrity_error(_SCHEDULED_UNIQUE, "23505")
    maker.reconcile_row = _persisted_row(store, different_request, response)

    async def _producer() -> ForecastResponse:
        return _different_run(response)

    with pytest.raises(AppError) as excinfo:
        await store.execute(
            request,
            idempotency_key=None,
            principal=None,
            producer=_producer,
        )

    assert excinfo.value.code == "forecast_archive_scheduled_request_conflict"
    assert excinfo.value.status_code == 409
    assert excinfo.value.details == {"retryable": False}


async def test_scheduled_unique_refuses_corrupt_winner_evidence() -> None:
    maker = _FakeSessionMaker(AS_OF + timedelta(minutes=5))
    store = _store(maker)
    response = _response()
    request = _matching_request(response)
    winner = _persisted_row(store, request, response)
    winner.canonical_output = bytes(winner.canonical_output) + b" "
    maker.flush_error = _integrity_error(_SCHEDULED_UNIQUE, "23505")
    maker.reconcile_row = winner

    async def _producer() -> ForecastResponse:
        return _different_run(response)

    with pytest.raises(AppError) as excinfo:
        await store.execute(
            request,
            idempotency_key=None,
            principal=None,
            producer=_producer,
        )

    assert excinfo.value.code == "forecast_archive_corrupt"
    assert excinfo.value.details == {
        "forecast_id": str(winner.forecast_id),
        "retryable": False,
    }


async def test_scheduled_unique_without_a_visible_winner_is_retryable() -> None:
    maker = _FakeSessionMaker(
        AS_OF + timedelta(minutes=5),
        flush_error=_integrity_error(_SCHEDULED_UNIQUE, "23505"),
    )
    store = _store(maker)
    response = _response()

    async def _producer() -> ForecastResponse:
        return response

    with pytest.raises(AppError) as excinfo:
        await store.execute(
            _matching_request(response),
            idempotency_key=None,
            principal=None,
            producer=_producer,
        )

    assert excinfo.value.code == "forecast_archive_write_conflict"
    assert excinfo.value.status_code == 503
    assert excinfo.value.details == {"retryable": True}


async def test_scheduled_unknown_commit_reconciles_by_opportunity_not_forecast_id() -> None:
    maker = _FakeSessionMaker(
        AS_OF + timedelta(minutes=5),
        commit_error=OperationalError("COMMIT", {}, Exception("connection lost")),
    )
    store = _store(maker)
    winner_response = _response()
    request = _matching_request(winner_response)
    maker.reconcile_row = _persisted_row(store, request, winner_response)
    candidate = _different_run(winner_response)

    async def _producer() -> ForecastResponse:
        return candidate

    reconciled = await store.execute(
        request,
        idempotency_key=None,
        principal=None,
        producer=_producer,
    )

    assert reconciled.provenance.forecast_id == winner_response.provenance.forecast_id
    assert reconciled.provenance.forecast_id != candidate.provenance.forecast_id


async def test_scheduled_unknown_commit_without_visible_row_is_safe_to_retry() -> None:
    maker = _FakeSessionMaker(
        AS_OF + timedelta(minutes=5),
        commit_error=OperationalError("COMMIT", {}, Exception("connection lost")),
    )
    store = _store(maker)
    response = _response()

    async def _producer() -> ForecastResponse:
        return response

    with pytest.raises(AppError) as excinfo:
        await store.execute(
            _matching_request(response),
            idempotency_key=None,
            principal=None,
            producer=_producer,
        )

    assert excinfo.value.code == "forecast_archive_commit_unknown"
    assert excinfo.value.details == {"outcome_unknown": True, "retryable": True}


async def test_sqlstate_40003_commit_reconciles_a_visible_scheduled_winner() -> None:
    maker = _FakeSessionMaker(
        AS_OF + timedelta(minutes=5),
        commit_error=_operational_error("40003"),
    )
    store = _store(maker)
    winner_response = _response()
    request = _matching_request(winner_response)
    maker.reconcile_row = _persisted_row(store, request, winner_response)
    candidate = _different_run(winner_response)

    async def _producer() -> ForecastResponse:
        return candidate

    reconciled = await store.execute(
        request,
        idempotency_key=None,
        principal=None,
        producer=_producer,
    )

    assert reconciled.provenance.forecast_id == winner_response.provenance.forecast_id
    assert reconciled.provenance.forecast_id != candidate.provenance.forecast_id


async def test_sqlstate_40003_without_visible_scheduled_winner_reports_unknown() -> None:
    maker = _FakeSessionMaker(
        AS_OF + timedelta(minutes=5),
        commit_error=_operational_error("40003"),
    )
    store = _store(maker)
    response = _response()

    async def _producer() -> ForecastResponse:
        return response

    with pytest.raises(AppError) as excinfo:
        await store.execute(
            _matching_request(response),
            idempotency_key=None,
            principal=None,
            producer=_producer,
        )

    assert excinfo.value.code == "forecast_archive_commit_unknown"
    assert excinfo.value.details == {"outcome_unknown": True, "retryable": True}


async def test_sqlstate_40003_during_execute_reconciles_visible_scheduled_winner() -> None:
    maker = _FakeSessionMaker(AS_OF + timedelta(minutes=5))
    maker.flush_error = _operational_error("40003")  # type: ignore[assignment]
    store = _store(maker)
    winner_response = _response()
    request = _matching_request(winner_response)
    maker.reconcile_row = _persisted_row(store, request, winner_response)
    candidate = _different_run(winner_response)

    async def _producer() -> ForecastResponse:
        return candidate

    reconciled = await store.execute(
        request,
        idempotency_key=None,
        principal=None,
        producer=_producer,
    )

    assert reconciled.provenance.forecast_id == winner_response.provenance.forecast_id
    assert reconciled.provenance.forecast_id != candidate.provenance.forecast_id


async def test_sqlstate_40003_during_execute_without_winner_reports_unknown() -> None:
    maker = _FakeSessionMaker(AS_OF + timedelta(minutes=5))
    maker.flush_error = _operational_error("40003")  # type: ignore[assignment]
    store = _store(maker)
    response = _response()

    async def _producer() -> ForecastResponse:
        return response

    with pytest.raises(AppError) as excinfo:
        await store.execute(
            _matching_request(response),
            idempotency_key=None,
            principal=None,
            producer=_producer,
        )

    assert excinfo.value.code == "forecast_archive_commit_unknown"
    assert excinfo.value.details == {"outcome_unknown": True, "retryable": True}


def _make_row_readable(maker: _FakeSessionMaker, row: ForecastRun) -> None:
    maker.commit_failed = True
    maker.reconcile_added_row = True
    maker.added_rows.append(row)


async def test_read_validated_returns_only_detached_fully_validated_evidence() -> None:
    maker = _FakeSessionMaker(AS_OF + timedelta(minutes=5))
    store = _store(maker)
    response = _response()
    request = _matching_request(response)
    row = _persisted_row(store, request, response)
    _make_row_readable(maker, row)

    archived = await store.read_validated(
        row.forecast_id,
        expected_request=request,
        expected_origin_kind="scheduled_evaluation",
    )

    assert isinstance(archived, ArchivedForecastRun)
    assert archived.forecast_id == row.forecast_id
    assert archived.recorded_at == row.recorded_at
    assert archived.canonical_request == canonical_request(request)


@pytest.mark.parametrize("mismatch", ["forecast_id", "origin", "request"])
async def test_read_validated_refuses_wrong_exact_expectations(mismatch: str) -> None:
    maker = _FakeSessionMaker(AS_OF + timedelta(minutes=5))
    store = _store(maker)
    response = _response()
    request = _matching_request(response)
    row = _persisted_row(store, request, response)
    _make_row_readable(maker, row)
    expected_origin = "api" if mismatch == "origin" else "scheduled_evaluation"
    expected_request = (
        request.model_copy(update={"model": "baseline_naive"}) if mismatch == "request" else request
    )
    expected_forecast_id = (
        UUID("55555555-5555-5555-5555-555555555555")
        if mismatch == "forecast_id"
        else row.forecast_id
    )

    with pytest.raises(AppError) as excinfo:
        await store.read_validated(
            expected_forecast_id,
            expected_request=expected_request,
            expected_origin_kind=expected_origin,
        )

    assert excinfo.value.code == "forecast_archive_read_conflict"
    assert excinfo.value.details == {"retryable": False}


async def test_read_validated_binds_the_store_policy_epoch() -> None:
    maker = _FakeSessionMaker(AS_OF + timedelta(minutes=5))
    writer = _store(maker)
    response = _response()
    request = _matching_request(response)
    row = _persisted_row(writer, request, response)
    _make_row_readable(maker, row)
    reader = SqlForecastRunStore(
        sessionmaker=maker,  # type: ignore[arg-type]
        identity_secret="fixture-archive-secret",
        resolution_policy_hash="sha256:" + "d" * 64,
        availability_rule_set_hash=RULE_SET_HASH,
        origin_kind="scheduled_evaluation",
    )

    with pytest.raises(AppError) as excinfo:
        await reader.read_validated(
            row.forecast_id,
            expected_request=request,
            expected_origin_kind="scheduled_evaluation",
        )

    assert excinfo.value.code == "forecast_archive_read_conflict"
    assert excinfo.value.details == {"retryable": False}


@pytest.mark.parametrize("corruption", ["recorded_at", "opportunity_hash"])
async def test_read_validated_refuses_unpersisted_or_corrupt_evidence(corruption: str) -> None:
    maker = _FakeSessionMaker(AS_OF + timedelta(minutes=5))
    store = _store(maker)
    response = _response()
    request = _matching_request(response)
    row = _persisted_row(store, request, response)
    if corruption == "recorded_at":
        row.recorded_at = None  # type: ignore[assignment]
    else:
        row.opportunity_hash = "sha256:" + "0" * 64
    _make_row_readable(maker, row)

    with pytest.raises(AppError) as excinfo:
        await store.read_validated(
            row.forecast_id,
            expected_request=request,
            expected_origin_kind="scheduled_evaluation",
        )

    assert excinfo.value.code == "forecast_archive_corrupt"
    assert excinfo.value.details == {
        "forecast_id": str(row.forecast_id),
        "retryable": False,
    }


async def test_read_validated_missing_row_is_structured_retryable_unavailable() -> None:
    maker = _FakeSessionMaker(AS_OF + timedelta(minutes=5))
    store = _store(maker)

    with pytest.raises(AppError) as excinfo:
        await store.read_validated(
            UUID("55555555-5555-5555-5555-555555555555"),
            expected_request=_matching_request(),
            expected_origin_kind="scheduled_evaluation",
        )

    assert excinfo.value.code == "forecast_archive_unavailable"
    assert excinfo.value.status_code == 503
    assert excinfo.value.details == {"retryable": True}


async def test_read_self_validated_uses_the_rows_historical_policy_epoch() -> None:
    maker = _FakeSessionMaker(AS_OF + timedelta(minutes=5))
    writer = _store(maker)
    response = _response()
    request = _matching_request(response)
    row = _persisted_row(writer, request, response)
    _make_row_readable(maker, row)
    reader = SqlForecastRunStore(
        sessionmaker=maker,  # type: ignore[arg-type]
        identity_secret="different-unused-reader-secret",
        resolution_policy_hash="sha256:" + "d" * 64,
        availability_rule_set_hash="sha256:" + "f" * 64,
        origin_kind="api",
    )

    archived = await reader.read_self_validated(row.forecast_id)

    assert isinstance(archived, ArchivedForecastRun)
    assert archived.forecast_id == row.forecast_id
    assert archived.origin_kind == "scheduled_evaluation"
    assert archived.resolution_policy_hash == POLICY_HASH
    assert archived.availability_rule_set_hash == RULE_SET_HASH
    assert archived.canonical_request == canonical_request(request)
    assert archived.canonical_output == canonical_output(response)


async def test_read_self_validated_refuses_the_wrong_historical_origin() -> None:
    maker = _FakeSessionMaker(AS_OF + timedelta(minutes=5))
    store = _store(maker)
    response = _response()
    request = _matching_request(response)
    row = _persisted_row(store, request, response)
    _make_row_readable(maker, row)

    with pytest.raises(AppError) as excinfo:
        await store.read_self_validated(
            row.forecast_id,
            expected_origin_kind="api",
        )

    assert excinfo.value.code == "forecast_archive_read_conflict"
    assert excinfo.value.details == {"retryable": False}


@pytest.mark.parametrize("corruption", ["canonical_request", "opportunity_hash"])
async def test_read_self_validated_refuses_corrupt_historical_evidence(
    corruption: str,
) -> None:
    maker = _FakeSessionMaker(AS_OF + timedelta(minutes=5))
    store = _store(maker)
    response = _response()
    request = _matching_request(response)
    row = _persisted_row(store, request, response)
    if corruption == "canonical_request":
        row.canonical_request = bytes(row.canonical_request) + b" "
    else:
        row.opportunity_hash = "sha256:" + "0" * 64
    _make_row_readable(maker, row)

    with pytest.raises(AppError) as excinfo:
        await store.read_self_validated(row.forecast_id)

    assert excinfo.value.code == "forecast_archive_corrupt"
    assert excinfo.value.details == {
        "forecast_id": str(row.forecast_id),
        "retryable": False,
    }


async def test_read_self_validated_distinguishes_a_missing_row_from_an_outage() -> None:
    maker = _FakeSessionMaker(AS_OF + timedelta(minutes=5))
    forecast_id = UUID("55555555-5555-5555-5555-555555555555")

    with pytest.raises(AppError) as excinfo:
        await _store(maker).read_self_validated(forecast_id)

    assert excinfo.value.code == "forecast_archive_evidence_missing"
    assert excinfo.value.status_code == 404
    assert excinfo.value.details == {
        "forecast_id": str(forecast_id),
        "retryable": False,
    }
