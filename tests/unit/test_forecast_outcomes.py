"""Golden and adversarial tests for canonical realized-outcome evidence."""

from __future__ import annotations

import json
import math
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone

import pytest

from app.services.forecast_outcomes import (
    BarVersionEvidence,
    OutcomeValidationError,
    RealizedOutcomePayload,
    build_outcome_record,
    canonical_outcome_payload,
    outcome_id_for_payload,
    parse_outcome_payload,
    validate_outcome_record,
)

TARGET_TIME = datetime(2026, 7, 10, 20, tzinfo=UTC)
RESOLUTION_CUTOFF = datetime(2026, 7, 11, 12, tzinfo=UTC)
SEALED_AT = RESOLUTION_CUTOFF + timedelta(minutes=1)
POLICY_HASH = "sha256:" + "a" * 64
RULE_SET_HASH = "sha256:" + "b" * 64


def _source(**overrides: object) -> BarVersionEvidence:
    fields: dict[str, object] = {
        "symbol": "MSFT",
        "timespan": "day",
        "multiplier": 1,
        "observed_at": TARGET_TIME,
        "source": "polygon_open_close",
        "adjustment_basis": "raw",
        "fetched_at": TARGET_TIME + timedelta(minutes=1),
        "source_as_of": TARGET_TIME + timedelta(minutes=2),
        "version_recorded_at": TARGET_TIME + timedelta(minutes=3),
        "available_at": TARGET_TIME + timedelta(minutes=4),
        "field": "close",
        "value": 501.25,
    }
    fields.update(overrides)
    return BarVersionEvidence(**fields)  # type: ignore[arg-type]


def _payload(**overrides: object) -> RealizedOutcomePayload:
    fields: dict[str, object] = {
        "outcome_resolution_policy_hash": POLICY_HASH,
        "availability_rule_set_hash": RULE_SET_HASH,
        "resolution_cutoff": RESOLUTION_CUTOFF,
        "symbol": "MSFT",
        "target": "close",
        "series_basis": "raw",
        "target_time": TARGET_TIME,
        "currency": "USD",
        "realized_value": 501.25,
        "source_version": _source(),
    }
    fields.update(overrides)
    return RealizedOutcomePayload(**fields)  # type: ignore[arg-type]


def _record(**overrides: object):
    return build_outcome_record(_payload(**overrides), sealed_at=SEALED_AT)


def test_canonical_payload_and_outcome_id_have_a_pinned_golden_vector() -> None:
    canonical = canonical_outcome_payload(_payload())

    expected = (
        '{"format":"forecast-realized-outcome-v1","payload":'
        f'{{"availability_rule_set_hash":"{RULE_SET_HASH}","currency":"USD",'
        f'"outcome_resolution_policy_hash":"{POLICY_HASH}",'
        '"realized_value_f64":"407f540000000000",'
        '"resolution_cutoff":"2026-07-11T12:00:00.000000Z","series_basis":"raw",'
        '"source_version":{"adjustment_basis":"raw",'
        '"available_at":"2026-07-10T20:04:00.000000Z",'
        '"fetched_at":"2026-07-10T20:01:00.000000Z","field":"close",'
        '"multiplier":1,"observed_at":"2026-07-10T20:00:00.000000Z",'
        '"source":"polygon_open_close",'
        '"source_as_of":"2026-07-10T20:02:00.000000Z","symbol":"MSFT",'
        '"timespan":"day","value_f64":"407f540000000000",'
        '"version_recorded_at":"2026-07-10T20:03:00.000000Z"},'
        '"symbol":"MSFT","target":"close",'
        '"target_time":"2026-07-10T20:00:00.000000Z"},"schema_version":1}'
    ).encode()
    assert canonical == expected
    assert _record().outcome_id == (
        "sha256:dcdfdf3fb19ec1499a1fd24d7dcead40b0b86646abc648a96d4dbd7e92283e5d"
    )
    assert outcome_id_for_payload(canonical) == _record().outcome_id


def test_timezone_and_negative_zero_normalize_stably() -> None:
    offset = timezone(timedelta(hours=3))
    zero_source = _source(value=0.0)
    baseline = _payload(realized_value=0.0, source_version=zero_source)
    offset_source = replace(
        zero_source,
        observed_at=zero_source.observed_at.astimezone(offset),
        fetched_at=zero_source.fetched_at.astimezone(offset),
        source_as_of=zero_source.source_as_of.astimezone(offset),
        version_recorded_at=zero_source.version_recorded_at.astimezone(offset),
        available_at=zero_source.available_at.astimezone(offset),
        value=-0.0,
    )
    equivalent = replace(
        baseline,
        resolution_cutoff=RESOLUTION_CUTOFF.astimezone(offset),
        target_time=TARGET_TIME.astimezone(offset),
        realized_value=-0.0,
        source_version=offset_source,
    )

    assert canonical_outcome_payload(equivalent) == canonical_outcome_payload(baseline)
    assert b'"realized_value_f64":"0000000000000000"' in canonical_outcome_payload(baseline)


def test_seal_time_is_receipt_metadata_not_part_of_the_content_identity() -> None:
    first = build_outcome_record(_payload(), sealed_at=RESOLUTION_CUTOFF)
    later = build_outcome_record(_payload(), sealed_at=SEALED_AT + timedelta(days=1))

    assert first.outcome_id == later.outcome_id
    assert first.canonical_evidence == later.canonical_evidence
    assert first.sealed_at != later.sealed_at


def test_strict_parser_round_trips_only_canonical_exact_key_bytes() -> None:
    canonical = canonical_outcome_payload(_payload())
    parsed = parse_outcome_payload(canonical)

    assert canonical_outcome_payload(parsed) == canonical
    duplicate = canonical.replace(b'{"format":', b'{"format":"duplicate","format":', 1)
    with pytest.raises(OutcomeValidationError, match="duplicate JSON key"):
        parse_outcome_payload(duplicate)

    document = json.loads(canonical)
    document["payload"]["unknown"] = True
    unknown = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    with pytest.raises(OutcomeValidationError, match="unknown keys"):
        parse_outcome_payload(unknown)

    noncanonical = canonical.replace(b'"schema_version":1', b'"schema_version": 1')
    with pytest.raises(OutcomeValidationError, match="not canonical"):
        parse_outcome_payload(noncanonical)
    with pytest.raises(OutcomeValidationError, match="strict UTF-8 JSON"):
        parse_outcome_payload(b"\xff")


@pytest.mark.parametrize(
    "bits",
    [
        "7ff8000000000000",  # NaN
        "7ff0000000000000",  # +Infinity
        "fff0000000000000",  # -Infinity
    ],
)
def test_raw_nonfinite_f64_bit_patterns_fail_closed(bits: str) -> None:
    canonical = canonical_outcome_payload(_payload())
    invalid = canonical.replace(
        b'"realized_value_f64":"407f540000000000"',
        f'"realized_value_f64":"{bits}"'.encode(),
    )

    with pytest.raises(OutcomeValidationError, match="must be finite"):
        parse_outcome_payload(invalid)


@pytest.mark.parametrize("bits", ["407F540000000000", "407f54", "g07f540000000000"])
def test_f64_bits_must_be_exact_lowercase_binary64(bits: str) -> None:
    canonical = canonical_outcome_payload(_payload())
    invalid = canonical.replace(
        b'"realized_value_f64":"407f540000000000"',
        f'"realized_value_f64":"{bits}"'.encode(),
    )

    with pytest.raises(OutcomeValidationError, match="16 lowercase hexadecimal"):
        parse_outcome_payload(invalid)


def test_raw_negative_zero_bits_are_finite_but_not_canonical() -> None:
    zero = canonical_outcome_payload(
        _payload(realized_value=0.0, source_version=_source(value=0.0))
    )
    invalid = zero.replace(
        b'"realized_value_f64":"0000000000000000"',
        b'"realized_value_f64":"8000000000000000"',
        1,
    )

    with pytest.raises(OutcomeValidationError, match="not canonical"):
        parse_outcome_payload(invalid)


@pytest.mark.parametrize("bad", [math.nan, math.inf, -math.inf, -1.0, True])
def test_outcome_and_source_values_must_be_finite_nonnegative_reals(bad: object) -> None:
    with pytest.raises(OutcomeValidationError, match="finite|nonnegative"):
        canonical_outcome_payload(_payload(realized_value=bad, source_version=_source(value=bad)))


def _shifted_source(delta: timedelta, *, symbol: str = "MSFT") -> BarVersionEvidence:
    return _source(
        symbol=symbol,
        observed_at=TARGET_TIME + delta,
        fetched_at=TARGET_TIME + delta + timedelta(minutes=1),
        source_as_of=TARGET_TIME + delta + timedelta(minutes=2),
        version_recorded_at=TARGET_TIME + delta + timedelta(minutes=3),
        available_at=TARGET_TIME + delta + timedelta(minutes=4),
    )


def test_every_semantic_change_changes_the_content_identity() -> None:
    baseline = _record().outcome_id
    changed_payloads = [
        _payload(outcome_resolution_policy_hash="sha256:" + "c" * 64),
        _payload(availability_rule_set_hash="sha256:" + "d" * 64),
        _payload(resolution_cutoff=RESOLUTION_CUTOFF + timedelta(minutes=1)),
        _payload(source_version=_source(fetched_at=TARGET_TIME + timedelta(minutes=1, seconds=1))),
        _payload(
            source_version=_source(
                version_recorded_at=TARGET_TIME + timedelta(minutes=3, seconds=1)
            )
        ),
        _payload(realized_value=502.0, source_version=_source(value=502.0)),
        _payload(
            symbol="NVDA",
            target_time=TARGET_TIME + timedelta(days=1),
            resolution_cutoff=RESOLUTION_CUTOFF + timedelta(days=1),
            source_version=_shifted_source(timedelta(days=1), symbol="NVDA"),
        ),
    ]

    identities = {
        build_outcome_record(payload, sealed_at=SEALED_AT + timedelta(days=2)).outcome_id
        for payload in changed_payloads
    }
    assert baseline not in identities
    assert len(identities) == len(changed_payloads)


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (_payload(currency="EUR"), "currency must be USD"),
        (_payload(source_version=_source(symbol="AAPL")), "symbol does not match"),
        (
            _payload(source_version=_source(observed_at=TARGET_TIME - timedelta(seconds=1))),
            "timestamp does not match",
        ),
        (_payload(source_version=_source(value=500.0)), "value does not match"),
        (_payload(source_version=_source(timespan="hour")), "one-day"),
        (_payload(source_version=_source(multiplier=2)), "one-day"),
        (_payload(source_version=_source(field="open")), "raw close source field"),
        (_payload(source_version=_source(adjustment_basis="split_adjusted")), "raw close"),
        (
            _payload(source_version=_source(source="another_source")),
            "polygon_open_close source contract",
        ),
        (
            _payload(source_version=_source(fetched_at=TARGET_TIME - timedelta(seconds=1))),
            "timestamps are out of order",
        ),
        (
            _payload(
                resolution_cutoff=TARGET_TIME + timedelta(minutes=3),
            ),
            "not available by resolution_cutoff",
        ),
    ],
)
def test_exact_bar_version_receipt_and_cutoff_invariants_fail_closed(
    payload: RealizedOutcomePayload,
    message: str,
) -> None:
    with pytest.raises(OutcomeValidationError, match=message):
        canonical_outcome_payload(payload)


@pytest.mark.parametrize(
    "source",
    [
        _source(source_as_of=TARGET_TIME),
        _source(version_recorded_at=TARGET_TIME + timedelta(minutes=1)),
        _source(available_at=TARGET_TIME + timedelta(minutes=2)),
    ],
)
def test_every_internal_availability_ordering_edge_is_enforced(
    source: BarVersionEvidence,
) -> None:
    with pytest.raises(OutcomeValidationError, match="timestamps are out of order"):
        canonical_outcome_payload(_payload(source_version=source))


def test_v1_refuses_to_pretend_other_targets_or_bases_are_supported() -> None:
    with pytest.raises(OutcomeValidationError, match="raw close only"):
        canonical_outcome_payload(_payload(target="return"))
    with pytest.raises(OutcomeValidationError, match="raw close only"):
        canonical_outcome_payload(_payload(series_basis="split_adjusted"))


def test_record_requires_database_seal_not_earlier_than_explicit_cutoff() -> None:
    equal = build_outcome_record(_payload(), sealed_at=RESOLUTION_CUTOFF)
    assert equal.sealed_at == RESOLUTION_CUTOFF

    with pytest.raises(OutcomeValidationError, match="earlier than resolution_cutoff"):
        build_outcome_record(
            _payload(),
            sealed_at=RESOLUTION_CUTOFF - timedelta(microseconds=1),
        )


def test_record_validation_binds_bytes_header_digest_and_both_trust_identities() -> None:
    record = _record()
    payload = validate_outcome_record(
        record,
        expected_outcome_resolution_policy_hash=POLICY_HASH,
        expected_availability_rule_set_hash=RULE_SET_HASH,
    )
    assert payload == parse_outcome_payload(record.canonical_evidence)

    with pytest.raises(OutcomeValidationError, match="resolution policy is not trusted"):
        validate_outcome_record(
            record,
            expected_outcome_resolution_policy_hash="sha256:" + "c" * 64,
            expected_availability_rule_set_hash=RULE_SET_HASH,
        )
    with pytest.raises(OutcomeValidationError, match="availability rule set is not trusted"):
        validate_outcome_record(
            record,
            expected_outcome_resolution_policy_hash=POLICY_HASH,
            expected_availability_rule_set_hash="sha256:" + "d" * 64,
        )


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda row: replace(row, outcome_id="sha256:" + "0" * 64), "outcome_id"),
        (lambda row: replace(row, symbol="AAPL"), "header"),
        (lambda row: replace(row, realized_value=1.0), "header"),
        (
            lambda row: replace(
                row,
                bar_version_recorded_at=row.bar_version_recorded_at + timedelta(microseconds=1),
            ),
            "header",
        ),
        (
            lambda row: replace(row, canonical_evidence=row.canonical_evidence + b" "),
            "canonical",
        ),
    ],
)
def test_tampered_payload_hash_or_denormalized_header_is_rejected(
    mutate,
    message: str,
) -> None:
    with pytest.raises(OutcomeValidationError, match=message):
        validate_outcome_record(
            mutate(_record()),
            expected_outcome_resolution_policy_hash=POLICY_HASH,
            expected_availability_rule_set_hash=RULE_SET_HASH,
        )


@pytest.mark.parametrize(
    "field",
    ["outcome_resolution_policy_hash", "availability_rule_set_hash"],
)
def test_policy_identities_have_no_implicit_or_noncanonical_fallback(field: str) -> None:
    with pytest.raises(OutcomeValidationError, match="canonical sha256"):
        canonical_outcome_payload(_payload(**{field: "policy-v1"}))

    with pytest.raises(TypeError):
        validate_outcome_record(_record())  # type: ignore[call-arg]


def test_payload_contains_evidence_only_not_forecast_or_scoring_claims() -> None:
    document = json.loads(canonical_outcome_payload(_payload()))

    assert set(document) == {"format", "payload", "schema_version"}
    assert set(document["payload"]) == {
        "availability_rule_set_hash",
        "currency",
        "outcome_resolution_policy_hash",
        "realized_value_f64",
        "resolution_cutoff",
        "series_basis",
        "source_version",
        "symbol",
        "target",
        "target_time",
    }
    serialized = json.dumps(document)
    assert "forecast_id" not in serialized
    assert "metric" not in serialized
    assert "score" not in serialized
    assert "cohort" not in serialized
