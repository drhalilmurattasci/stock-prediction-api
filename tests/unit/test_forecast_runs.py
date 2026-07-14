"""Golden and adversarial tests for pure forecast-run identities."""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

import pytest
from sqlalchemy.exc import (
    IntegrityError,
    OperationalError,
)
from sqlalchemy.exc import (
    TimeoutError as SQLAlchemyTimeoutError,
)

from app.core.exceptions import AppError
from app.schemas.forecast import (
    DataSourceLineage,
    ForecastCalibration,
    ForecastInterval,
    ForecastProvenance,
    ForecastQuantile,
    ForecastRequest,
    ForecastResponse,
    ForecastStep,
    LookaheadCheck,
)
from app.services.forecast_run_store import SqlForecastRunStore
from app.services.forecast_runs import (
    ForecastRunValidationError,
    canonical_output,
    canonical_request,
    idempotency_digest,
    opportunity_hash,
    opportunity_manifest,
    output_hash,
    parse_output,
    parse_request,
    request_hash,
)

AS_OF = datetime(2026, 7, 10, 21, tzinfo=UTC)
POLICY_HASH = "sha256:" + "a" * 64
RULE_SET_HASH = "sha256:" + "b" * 64


def _request() -> ForecastRequest:
    return ForecastRequest(
        symbol="msft",
        horizon=2,
        horizon_unit="trading_day",
        target="close",
        as_of=AS_OF.astimezone(timezone(timedelta(hours=3))),
        snapshot_id="sha256:" + "c" * 64,
        model="auto",
        interval_coverages=[0.95, 0.5, 0.8],
    )


def _response(
    *,
    coverages: tuple[float, ...] = (0.8,),
    target_time: datetime | None = None,
    model_version: str = "baseline-naive@1",
    snapshot_id: str = "sha256:" + "c" * 64,
    calibration_set_version: str = "uncalibrated:baseline-naive@1",
) -> ForecastResponse:
    levels = {0.5}
    for coverage in coverages:
        levels.add((1.0 - coverage) / 2.0)
        levels.add((1.0 + coverage) / 2.0)
    values = {level: 100.0 + (level - 0.5) * 20.0 for level in levels}
    quantiles = [ForecastQuantile(level=level, value=values[level]) for level in sorted(levels)]
    intervals = [
        ForecastInterval(
            coverage=coverage,
            lower_quantile=(1.0 - coverage) / 2.0,
            upper_quantile=(1.0 + coverage) / 2.0,
            lower=values[(1.0 - coverage) / 2.0],
            upper=values[(1.0 + coverage) / 2.0],
        )
        for coverage in coverages
    ]
    provenance = ForecastProvenance(
        forecast_id=UUID("33333333-3333-3333-3333-333333333333"),
        snapshot_id=snapshot_id,
        model_version=model_version,
        series_basis="raw",
        feature_set_hash="C" * 64,
        max_available_at=AS_OF,
        generated_at=AS_OF + timedelta(minutes=1),
        code_version="fixture-code@1",
        data_sources=[
            DataSourceLineage(
                name="z-source",
                snapshot_id="source-z",
                max_available_at=AS_OF,
                fields=["volume", "close"],
            ),
            DataSourceLineage(
                name="a-source",
                snapshot_id="source-a",
                max_available_at=AS_OF - timedelta(minutes=1),
                fields=["close"],
            ),
        ],
        lookahead_check=LookaheadCheck(
            status="passed",
            checked_at=AS_OF + timedelta(minutes=1),
            max_feature_available_at=AS_OF,
        ),
    )
    return ForecastResponse(
        symbol="msft",
        target="close",
        horizon=1,
        horizon_unit="trading_day",
        as_of=AS_OF.astimezone(timezone(timedelta(hours=3))),
        currency="USD",
        forecasts=[
            ForecastStep(
                step=1,
                target_time=target_time or AS_OF + timedelta(days=1),
                point=100.0,
                quantiles=quantiles,
                intervals=intervals,
            )
        ],
        provenance=provenance,
        calibration=ForecastCalibration(
            calibration_set_version=calibration_set_version,
            method="none",
            sample_count=0,
        ),
    )


def _opportunity_hash(response: ForecastResponse, **overrides: str) -> str:
    parameters = {
        "resolution_policy_hash": POLICY_HASH,
        "availability_rule_set_hash": RULE_SET_HASH,
        "origin_kind": "post",
    }
    parameters.update(overrides)
    return opportunity_hash(response, **parameters)


def _matching_request(response: ForecastResponse | None = None) -> ForecastRequest:
    response = response or _response()
    return ForecastRequest(
        symbol=response.symbol,
        horizon=response.horizon,
        horizon_unit=response.horizon_unit,
        target=response.target,
        as_of=response.as_of,
        snapshot_id=response.provenance.snapshot_id,
        model="auto",
        interval_coverages=[0.8],
    )


def _run_store(
    *,
    resolution_policy_hash: str = POLICY_HASH,
    availability_rule_set_hash: str = RULE_SET_HASH,
) -> SqlForecastRunStore:
    return SqlForecastRunStore(
        sessionmaker=None,  # type: ignore[arg-type]
        identity_secret="fixture-archive-secret",
        resolution_policy_hash=resolution_policy_hash,
        availability_rule_set_hash=availability_rule_set_hash,
    )


def test_canonical_request_and_hash_have_a_pinned_golden_vector() -> None:
    canonical = canonical_request(_request())

    assert canonical == (
        b'{"format":"forecast-run-request-v1","payload":'
        b'{"as_of":"2026-07-10T21:00:00.000000Z","horizon":2,'
        b'"horizon_unit":"trading_day","interval_coverages":[0.5,0.8,0.95],'
        b'"model":"auto","snapshot_id":"sha256:cccccccccccccccccccccccccccccccccccc'
        b'cccccccccccccccccccccccccccc","symbol":"MSFT","target":"close"},'
        b'"schema_version":1}'
    )
    assert request_hash(_request()) == request_hash(canonical)
    assert request_hash(canonical) == (
        "sha256:d5a78abc6e47da2df6001320031538fd408132196382742ff683ffd583564998"
    )
    assert parse_request(canonical) == _request()


def test_canonical_output_and_hash_have_a_pinned_golden_vector() -> None:
    canonical = canonical_output(_response())
    parsed = parse_output(canonical)

    assert output_hash(canonical) == (
        "sha256:07ef55dea4ffed0ce533923fa6002993efc2d935708f2e584a24b52d9d591549"
    )
    assert output_hash(parsed) == output_hash(canonical)
    assert parsed.provenance.feature_set_hash == "sha256:" + "c" * 64
    document = json.loads(canonical)
    assert document["format"] == "forecast-run-output-v1"
    assert document["payload"]["as_of"] == "2026-07-10T21:00:00.000000Z"
    assert [row["name"] for row in document["payload"]["provenance"]["data_sources"]] == [
        "a-source",
        "z-source",
    ]
    assert document["payload"]["provenance"]["data_sources"][1]["fields"] == [
        "close",
        "volume",
    ]


def test_output_unordered_collections_normalize_to_identical_bytes() -> None:
    forward = _response(coverages=(0.5, 0.8))
    reversed_response = _response(coverages=(0.8, 0.5))
    reversed_response.provenance.data_sources.reverse()

    assert canonical_output(forward) == canonical_output(reversed_response)


@pytest.mark.parametrize("parser,payload", [(parse_request, _request), (parse_output, _response)])
def test_parsers_reject_noncanonical_and_duplicate_json(
    parser: object,
    payload: object,
) -> None:
    canonical = (
        canonical_request(payload())  # type: ignore[operator]
        if parser is parse_request
        else canonical_output(payload())  # type: ignore[operator]
    )
    duplicate = canonical.replace(b'{"format":', b'{"format":"duplicate","format":', 1)

    with pytest.raises(ForecastRunValidationError, match="duplicate JSON key"):
        parser(duplicate)  # type: ignore[operator]
    with pytest.raises(ForecastRunValidationError, match="not canonical"):
        parser(canonical.replace(b'"schema_version":1', b'"schema_version": 1'))  # type: ignore[operator]


def test_parsers_reject_unknown_fields_and_semantic_tampering() -> None:
    request_document = json.loads(canonical_request(_request()))
    request_document["payload"]["unknown"] = True
    unknown = json.dumps(request_document, sort_keys=True, separators=(",", ":")).encode()
    with pytest.raises(ForecastRunValidationError, match="ForecastRequest validation"):
        parse_request(unknown)

    output_document = json.loads(canonical_output(_response()))
    output_document["payload"]["forecasts"][0]["point"] = 999.0
    tampered = json.dumps(output_document, sort_keys=True, separators=(",", ":")).encode()
    with pytest.raises(ForecastRunValidationError, match="ForecastResponse validation"):
        parse_output(tampered)


def test_non_finite_values_fail_closed_in_models_and_raw_json() -> None:
    response = _response()
    step = response.forecasts[0].model_copy(update={"point": math.nan})
    invalid = response.model_copy(update={"forecasts": [step]})

    with pytest.raises(ForecastRunValidationError, match="revalidation"):
        canonical_output(invalid)

    raw_nan = canonical_output(response).replace(b'"point":100.0', b'"point":NaN')
    with pytest.raises(ForecastRunValidationError, match="not permitted"):
        parse_output(raw_nan)


def test_opportunity_manifest_has_a_golden_hash_and_omits_run_values() -> None:
    response = _response()
    manifest = opportunity_manifest(
        response,
        resolution_policy_hash=POLICY_HASH,
        availability_rule_set_hash=RULE_SET_HASH,
        origin_kind="post",
    )
    document = json.loads(manifest)
    payload = document["payload"]

    assert _opportunity_hash(response) == (
        "sha256:bfa559d84279787ce5ac7d8568e4bb250797fe89a9bb88e6b962b4e967f66230"
    )
    assert payload["snapshot_id"] == response.provenance.snapshot_id
    assert payload["targets"] == [
        {
            "interval_coverages_millis": [800],
            "step": 1,
            "target_time": "2026-07-11T21:00:00.000000Z",
        }
    ]
    serialized = manifest.decode()
    assert str(response.provenance.forecast_id) not in serialized
    assert "generated_at" not in serialized
    assert "checked_at" not in serialized
    assert '"point"' not in serialized
    assert '"quantiles"' not in serialized
    assert '"lower"' not in serialized
    assert '"upper"' not in serialized


def test_opportunity_is_stable_across_run_identity_and_prediction_values() -> None:
    original = _response()
    step = original.forecasts[0]
    changed_quantiles = [
        item.model_copy(update={"value": item.value + 50.0}) for item in step.quantiles
    ]
    changed_intervals = [
        item.model_copy(update={"lower": item.lower + 50.0, "upper": item.upper + 50.0})
        for item in step.intervals
    ]
    changed_step = step.model_copy(
        update={
            "point": step.point + 50.0,
            "quantiles": changed_quantiles,
            "intervals": changed_intervals,
        }
    )
    provenance = original.provenance.model_copy(
        update={
            "forecast_id": UUID("44444444-4444-4444-4444-444444444444"),
            "generated_at": AS_OF + timedelta(minutes=10),
            "lookahead_check": original.provenance.lookahead_check.model_copy(
                update={"checked_at": AS_OF + timedelta(minutes=10)}
            ),
        }
    )
    rerun = original.model_copy(update={"forecasts": [changed_step], "provenance": provenance})

    assert _opportunity_hash(rerun) == _opportunity_hash(original)
    assert output_hash(rerun) != output_hash(original)


@pytest.mark.parametrize(
    "changed",
    [
        _response(snapshot_id="sha256:" + "d" * 64),
        _response(model_version="baseline-naive@2"),
        _response(calibration_set_version="uncalibrated:other@1"),
        _response(target_time=AS_OF + timedelta(days=2)),
        _response(coverages=(0.5,)),
    ],
)
def test_opportunity_binds_resolved_snapshot_model_calibration_targets_and_coverages(
    changed: ForecastResponse,
) -> None:
    assert _opportunity_hash(changed) != _opportunity_hash(_response())


def test_opportunity_binds_both_policies_and_origin_semantics() -> None:
    response = _response()
    baseline = _opportunity_hash(response)

    assert _opportunity_hash(response, resolution_policy_hash="sha256:" + "d" * 64) != baseline
    assert _opportunity_hash(response, availability_rule_set_hash="sha256:" + "e" * 64) != baseline
    assert _opportunity_hash(response, origin_kind="scheduled") != baseline


def test_idempotency_digest_has_a_domain_separated_golden_vector_without_raw_material() -> None:
    digest = idempotency_digest(
        principal="api-key-owner-17",
        idempotency_key="forecast-retry-42",
        secret="fixture-secret-with-enough-entropy",
    )

    assert digest == (
        "hmac-sha256:2f7bba170fdbf518a2310a6ef3f270cd91781bcfdfa658cafb15b0456d88b0e8"
    )
    assert len(digest) == 76
    assert "api-key-owner-17" not in digest
    assert "forecast-retry-42" not in digest
    assert digest != idempotency_digest(
        principal="api-key-owner-1",
        idempotency_key="7forecast-retry-42",
        secret="fixture-secret-with-enough-entropy",
    )
    assert digest != idempotency_digest(
        principal="api-key-owner-17",
        idempotency_key="forecast-retry-43",
        secret="fixture-secret-with-enough-entropy",
    )


def test_idempotency_digest_preserves_opaque_unicode_identity() -> None:
    composed = "caf\u00e9"
    decomposed = "cafe\u0301"

    assert idempotency_digest(
        principal=composed,
        idempotency_key="retry",
        secret="identity-secret",
    ) != idempotency_digest(
        principal=decomposed,
        idempotency_key="retry",
        secret="identity-secret",
    )
    assert idempotency_digest(
        principal="tenant",
        idempotency_key=composed,
        secret="identity-secret",
    ) != idempotency_digest(
        principal="tenant",
        idempotency_key=decomposed,
        secret="identity-secret",
    )


def test_run_store_row_round_trips_without_retaining_raw_retry_material() -> None:
    store = _run_store()
    response = _response()
    request = _matching_request(response)
    request_payload = canonical_request(request)
    retry_identity = store._retry_identity(
        principal="customer-api-key",
        idempotency_key="opaque-retry-token",
    )
    assert retry_identity is not None

    row = store._row(
        request_payload=request_payload,
        request_identity=request_hash(request_payload),
        retry_identity=retry_identity,
        response=response,
    )
    replayed = store._replay(
        row,
        expected_request=request_payload,
        expected_request_hash=request_hash(request_payload),
    )

    assert canonical_output(replayed) == canonical_output(response)
    assert row.output_hash == output_hash(response)
    assert row.opportunity_hash == opportunity_hash(
        response,
        resolution_policy_hash=POLICY_HASH,
        availability_rule_set_hash=RULE_SET_HASH,
        origin_kind="api",
    )
    stored_material = bytes(row.canonical_request) + bytes(row.canonical_output)
    assert b"customer-api-key" not in stored_material
    assert b"opaque-retry-token" not in stored_material
    assert "customer-api-key" not in retry_identity
    assert "opaque-retry-token" not in retry_identity


def test_run_store_replay_revalidates_the_archived_policy_epoch() -> None:
    original_store = _run_store()
    response = _response()
    request = _matching_request(response)
    request_payload = canonical_request(request)
    row = original_store._row(
        request_payload=request_payload,
        request_identity=request_hash(request_payload),
        retry_identity=original_store._retry_identity(principal="p", idempotency_key="k"),
        response=response,
    )
    rotated_store = _run_store(
        resolution_policy_hash="sha256:" + "d" * 64,
        availability_rule_set_hash="sha256:" + "e" * 64,
    )

    replayed = rotated_store._replay(
        row,
        expected_request=request_payload,
        expected_request_hash=request_hash(request_payload),
    )

    assert canonical_output(replayed) == canonical_output(response)


def test_run_store_replay_refuses_changed_request_and_tampered_output() -> None:
    store = _run_store()
    response = _response()
    request = _matching_request(response)
    payload = canonical_request(request)
    row = store._row(
        request_payload=payload,
        request_identity=request_hash(payload),
        retry_identity=store._retry_identity(principal="p", idempotency_key="k"),
        response=response,
    )
    changed = canonical_request(request.model_copy(update={"interval_coverages": [0.5]}))

    with pytest.raises(AppError) as conflict:
        store._replay(
            row,
            expected_request=changed,
            expected_request_hash=request_hash(changed),
        )
    assert conflict.value.code == "idempotency_key_conflict"

    row.canonical_output = bytes(row.canonical_output) + b" "
    with pytest.raises(AppError) as corrupt:
        store._replay(
            row,
            expected_request=payload,
            expected_request_hash=request_hash(payload),
        )
    assert corrupt.value.code == "forecast_archive_corrupt"


def test_run_store_refuses_request_output_mismatch_before_insert() -> None:
    store = _run_store()
    response = _response()
    mismatched = _matching_request(response).model_copy(update={"symbol": "AAPL"})
    payload = canonical_request(mismatched)

    with pytest.raises(ForecastRunValidationError, match="does not match"):
        store._row(
            request_payload=payload,
            request_identity=request_hash(payload),
            retry_identity=None,
            response=response,
        )


def test_run_store_never_accepts_an_unscoped_idempotency_key() -> None:
    store = _run_store()
    with pytest.raises(AppError) as excinfo:
        store._retry_identity(principal=None, idempotency_key="opaque")
    assert excinfo.value.code == "idempotency_principal_required"
    assert "opaque" not in excinfo.value.message


class _FakeResult:
    def __init__(self, *, scalar: object = None, row: object = None) -> None:
        self.scalar = scalar
        self.row = row

    def scalar_one(self) -> object:
        return self.scalar

    def scalars(self) -> _FakeResult:
        return self

    def one_or_none(self) -> object:
        return self.row


class _FakeTransaction:
    def __init__(self, maker: _FakeSessionMaker) -> None:
        self.maker = maker

    async def __aenter__(self) -> _FakeTransaction:
        return self

    async def __aexit__(self, *exc: object) -> bool:
        if not any(exc) and self.maker.commit_error is not None:
            error = self.maker.commit_error
            self.maker.commit_error = None
            self.maker.commit_failed = True
            raise error
        return False


class _FakeSession:
    def __init__(self, maker: _FakeSessionMaker) -> None:
        self.maker = maker

    async def __aenter__(self) -> _FakeSession:
        self.maker.active_sessions += 1
        return self

    async def __aexit__(self, *exc: object) -> bool:
        self.maker.active_sessions -= 1
        return False

    def begin(self) -> _FakeTransaction:
        return _FakeTransaction(self.maker)

    async def execute(self, statement: object, _params: object = None) -> _FakeResult:
        if self.maker.execute_error is not None:
            error = self.maker.execute_error
            self.maker.execute_error = None
            raise error
        sql = str(statement)
        if "clock_timestamp" in sql:
            return _FakeResult(scalar=self.maker.database_time)
        if "pg_try_advisory_xact_lock" in sql:
            return _FakeResult(scalar=True)
        row = self.maker.reconcile_row if self.maker.flush_attempted else None
        return _FakeResult(row=row)

    async def get(self, _model: object, _identity: object) -> object:
        if self.maker.commit_failed and self.maker.reconcile_added_row:
            return self.maker.added_rows[-1]
        return None

    def add(self, row: object) -> None:
        self.maker.added_rows.append(row)

    async def flush(self) -> None:
        self.maker.flush_attempted = True
        if self.maker.flush_error is not None:
            raise self.maker.flush_error


class _FakeSessionMaker:
    def __init__(
        self,
        database_time: datetime,
        *,
        flush_error: IntegrityError | None = None,
        commit_error: OperationalError | None = None,
        execute_error: Exception | None = None,
        reconcile_row: object = None,
        reconcile_added_row: bool = False,
    ) -> None:
        self.database_time = database_time
        self.flush_error = flush_error
        self.commit_error = commit_error
        self.execute_error = execute_error
        self.reconcile_row = reconcile_row
        self.reconcile_added_row = reconcile_added_row
        self.flush_attempted = False
        self.commit_failed = False
        self.active_sessions = 0
        self.session_count = 0
        self.added_rows: list[object] = []

    def __call__(self) -> _FakeSession:
        self.session_count += 1
        return _FakeSession(self)


class _DriverFailure(Exception):
    def __init__(self, sqlstate: str) -> None:
        super().__init__("redacted driver detail")
        self.sqlstate = sqlstate


class _ConstraintViolation(_DriverFailure):
    def __init__(self, constraint_name: str, sqlstate: str) -> None:
        super().__init__(sqlstate)
        self.constraint_name = constraint_name


def _store_with_maker(maker: _FakeSessionMaker) -> SqlForecastRunStore:
    return SqlForecastRunStore(
        sessionmaker=maker,  # type: ignore[arg-type]
        identity_secret="fixture-archive-secret",
        resolution_policy_hash=POLICY_HASH,
        availability_rule_set_hash=RULE_SET_HASH,
    )


def _integrity_error(constraint_name: str, sqlstate: str) -> IntegrityError:
    return IntegrityError(
        "INSERT",
        {},
        _ConstraintViolation(constraint_name, sqlstate),
    )


def _operational_error(sqlstate: str) -> OperationalError:
    return OperationalError("statement", {}, _DriverFailure(sqlstate))


async def test_compute_holds_no_session_and_db_time_finalizes_the_archived_response() -> None:
    database_time = AS_OF + timedelta(minutes=5)
    maker = _FakeSessionMaker(database_time)
    store = _store_with_maker(maker)
    response = _response()
    future = AS_OF + timedelta(days=1)
    provisional = response.model_copy(
        update={
            "provenance": response.provenance.model_copy(
                update={
                    "generated_at": future,
                    "lookahead_check": response.provenance.lookahead_check.model_copy(
                        update={"checked_at": future}
                    ),
                }
            )
        }
    )

    async def _producer() -> ForecastResponse:
        assert maker.active_sessions == 0
        assert maker.session_count == 0
        return provisional

    archived = await store.execute(
        _matching_request(response),
        idempotency_key=None,
        principal=None,
        producer=_producer,
    )

    assert archived.provenance.generated_at == database_time
    assert archived.provenance.lookahead_check.checked_at == database_time
    assert future.isoformat().encode() not in canonical_output(archived)
    assert len(maker.added_rows) == 1
    row = maker.added_rows[0]
    assert row.generated_at == database_time
    stored = parse_output(bytes(row.canonical_output))
    assert canonical_output(stored) == canonical_output(archived)
    assert maker.active_sessions == 0


async def test_database_completion_time_before_as_of_fails_before_insert() -> None:
    maker = _FakeSessionMaker(AS_OF - timedelta(seconds=1))
    store = _store_with_maker(maker)

    async def _producer() -> ForecastResponse:
        return _response()

    with pytest.raises(AppError) as excinfo:
        await store.execute(
            _matching_request(),
            idempotency_key=None,
            principal=None,
            producer=_producer,
        )
    assert excinfo.value.code == "forecast_archive_clock_invalid"
    assert excinfo.value.details == {"retryable": False}
    assert maker.added_rows == []


@pytest.mark.parametrize(
    ("constraint_name", "sqlstate", "expected_status", "expected_code"),
    [
        ("ck_forecast_runs_time_order", "23514", 503, "forecast_archive_clock_invalid"),
        (
            "ck_forecast_runs_output_hash_matches_payload",
            "23514",
            500,
            "forecast_archive_integrity_failed",
        ),
        (
            "fk_forecast_runs_snapshot_id_forecast_input_snapshots",
            "23503",
            503,
            "forecast_snapshot_unavailable",
        ),
    ],
)
async def test_integrity_failures_are_classified_without_driver_text(
    constraint_name: str,
    sqlstate: str,
    expected_status: int,
    expected_code: str,
) -> None:
    maker = _FakeSessionMaker(
        AS_OF + timedelta(minutes=5),
        flush_error=_integrity_error(constraint_name, sqlstate),
    )
    store = _store_with_maker(maker)

    async def _producer() -> ForecastResponse:
        return _response()

    with pytest.raises(AppError) as excinfo:
        await store.execute(
            _matching_request(),
            idempotency_key=None,
            principal=None,
            producer=_producer,
        )
    assert excinfo.value.status_code == expected_status
    assert excinfo.value.code == expected_code
    assert "redacted driver detail" not in excinfo.value.message


@pytest.mark.parametrize(
    ("failure", "expected_status", "expected_code", "retryable"),
    [
        (
            _operational_error("42501"),
            500,
            "forecast_archive_configuration_invalid",
            False,
        ),
        (
            SQLAlchemyTimeoutError("pool exhausted"),
            503,
            "forecast_archive_unavailable",
            True,
        ),
    ],
)
async def test_prewrite_database_failures_distinguish_config_from_transient_capacity(
    failure: Exception,
    expected_status: int,
    expected_code: str,
    retryable: bool,
) -> None:
    maker = _FakeSessionMaker(
        AS_OF + timedelta(minutes=5),
        execute_error=failure,
    )
    store = _store_with_maker(maker)

    async def _producer() -> ForecastResponse:
        return _response()

    with pytest.raises(AppError) as excinfo:
        await store.execute(
            _matching_request(),
            idempotency_key=None,
            principal=None,
            producer=_producer,
        )
    assert excinfo.value.status_code == expected_status
    assert excinfo.value.code == expected_code
    assert excinfo.value.details == {"retryable": retryable}
    assert "pool exhausted" not in excinfo.value.message
    assert maker.added_rows == []


@pytest.mark.parametrize("changed_request", [False, True])
async def test_idempotency_unique_race_reconciles_to_replay_or_request_conflict(
    changed_request: bool,
) -> None:
    database_time = AS_OF + timedelta(minutes=5)
    maker = _FakeSessionMaker(database_time)
    store = _store_with_maker(maker)
    principal = "credential"
    retry_key = "retry-key"
    retry_identity = store._retry_identity(
        principal=principal,
        idempotency_key=retry_key,
    )
    assert retry_identity is not None
    winner_response = _response()
    winner_request = _matching_request(winner_response)
    winner_payload = canonical_request(winner_request)
    winner = store._row(
        request_payload=winner_payload,
        request_identity=request_hash(winner_payload),
        retry_identity=retry_identity,
        response=winner_response,
    )
    winner.recorded_at = database_time
    maker.flush_error = _integrity_error(
        "uq_forecast_runs_idempotency_token_digest",
        "23505",
    )
    maker.reconcile_row = winner
    request = winner_request
    candidate = winner_response
    if changed_request:
        request = winner_request.model_copy(update={"interval_coverages": [0.5]})
        candidate = _response(coverages=(0.5,))

    async def _producer() -> ForecastResponse:
        return candidate

    if changed_request:
        with pytest.raises(AppError) as excinfo:
            await store.execute(
                request,
                idempotency_key=retry_key,
                principal=principal,
                producer=_producer,
            )
        assert excinfo.value.code == "idempotency_key_conflict"
    else:
        replayed = await store.execute(
            request,
            idempotency_key=retry_key,
            principal=principal,
            producer=_producer,
        )
        assert canonical_output(replayed) == canonical_output(winner_response)


@pytest.mark.parametrize(
    ("idempotency_key", "principal", "retryable"),
    [(None, None, False), ("retry-key", "credential", True)],
)
async def test_commit_unknown_distinguishes_safe_keyed_retry(
    idempotency_key: str | None,
    principal: str | None,
    retryable: bool,
) -> None:
    maker = _FakeSessionMaker(
        AS_OF + timedelta(minutes=5),
        commit_error=OperationalError("COMMIT", {}, Exception("connection lost")),
    )
    store = _store_with_maker(maker)

    async def _producer() -> ForecastResponse:
        assert maker.active_sessions == 0
        return _response()

    with pytest.raises(AppError) as excinfo:
        await store.execute(
            _matching_request(),
            idempotency_key=idempotency_key,
            principal=principal,
            producer=_producer,
        )
    assert excinfo.value.code == "forecast_archive_commit_unknown"
    assert excinfo.value.details == {
        "outcome_unknown": True,
        "retryable": retryable,
    }


async def test_known_rollback_after_flush_is_retryable_not_commit_unknown() -> None:
    maker = _FakeSessionMaker(
        AS_OF + timedelta(minutes=5),
        commit_error=_operational_error("40001"),
    )
    store = _store_with_maker(maker)

    async def _producer() -> ForecastResponse:
        return _response()

    with pytest.raises(AppError) as excinfo:
        await store.execute(
            _matching_request(),
            idempotency_key=None,
            principal=None,
            producer=_producer,
        )
    assert excinfo.value.code == "forecast_archive_unavailable"
    assert excinfo.value.details == {"retryable": True}


async def test_unknown_commit_reconciles_a_visible_unkeyed_row() -> None:
    maker = _FakeSessionMaker(
        AS_OF + timedelta(minutes=5),
        commit_error=OperationalError("COMMIT", {}, Exception("connection lost")),
        reconcile_added_row=True,
    )
    store = _store_with_maker(maker)

    async def _producer() -> ForecastResponse:
        return _response()

    reconciled = await store.execute(
        _matching_request(),
        idempotency_key=None,
        principal=None,
        producer=_producer,
    )
    assert canonical_output(reconciled) == canonical_output(
        parse_output(bytes(maker.added_rows[0].canonical_output))  # type: ignore[attr-defined]
    )
