"""Golden and adversarial tests for pure forecast-run identities."""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

import pytest

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
        repository_factory=lambda session: None,  # type: ignore[arg-type,return-value]
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
