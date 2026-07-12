"""Canonical snapshot hashing, request binding, and storage-shape tests."""

from __future__ import annotations

import json
import math
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import DateTime, LargeBinary

from app.db.base import Base
from app.db.models import ForecastInputSnapshot
from app.schemas.forecast import ForecastRequest
from app.services.forecast_snapshots import (
    ForecastInputSnapshotPayload,
    SnapshotAvailabilityEvidence,
    SnapshotObservation,
    SnapshotSourceLineage,
    SnapshotValidationError,
    build_snapshot_record,
    canonical_snapshot_payload,
    parse_snapshot_payload,
    snapshot_id_for_payload,
    validate_and_resolve_snapshot,
)

AS_OF = datetime(2026, 7, 10, 21, tzinfo=UTC)
SEALED_AT = AS_OF + timedelta(minutes=2)
POLICY_HASH = "sha256:" + "a" * 64
RULE_SET_HASH = "sha256:" + "b" * 64


def _payload(**overrides: object) -> ForecastInputSnapshotPayload:
    fields: dict[str, object] = {
        "resolution_policy_hash": POLICY_HASH,
        "symbol": "AAPL",
        "target": "close",
        "horizon_unit": "calendar_day",
        "series_basis": "raw",
        "input_timespan": "day",
        "input_multiplier": 1,
        "as_of": AS_OF,
        "currency": "USD",
        "observations": (
            SnapshotObservation(
                observed_at=AS_OF - timedelta(days=2, hours=1),
                available_at=AS_OF - timedelta(days=2),
                value=98.0,
            ),
            SnapshotObservation(
                observed_at=AS_OF - timedelta(days=1, hours=1),
                available_at=AS_OF - timedelta(days=1),
                value=101.0,
            ),
        ),
        "target_times": (AS_OF + timedelta(days=1), AS_OF + timedelta(days=2)),
        "data_sources": (
            SnapshotSourceLineage(
                name="fixture-market-data",
                snapshot_id="fixture-source-v1",
                max_available_at=AS_OF - timedelta(hours=1),
                fields=("close", "volume"),
            ),
        ),
        "availability": SnapshotAvailabilityEvidence(status="not_run"),
    }
    fields.update(overrides)
    return ForecastInputSnapshotPayload(**fields)  # type: ignore[arg-type]


def _record(**payload_overrides: object):
    return build_snapshot_record(_payload(**payload_overrides), sealed_at=SEALED_AT)


def _request(snapshot_id: str | None, **overrides: object) -> ForecastRequest:
    fields: dict[str, object] = {
        "symbol": "AAPL",
        "horizon": 2,
        "horizon_unit": "calendar_day",
        "target": "close",
        "snapshot_id": snapshot_id,
        "model": "baseline_naive",
        "interval_coverages": [0.8],
    }
    fields.update(overrides)
    return ForecastRequest.model_validate(fields)


def test_canonical_payload_and_sha256_have_a_pinned_golden_vector() -> None:
    canonical = canonical_snapshot_payload(_payload())

    assert canonical == (
        b'{"as_of":"2026-07-10T21:00:00.000000Z","availability":'
        b'{"checked_at":null,"rule_set_hash":null,"status":"not_run"},'
        b'"currency":"USD","data_sources":[{"fields":["close","volume"],'
        b'"max_available_at":"2026-07-10T20:00:00.000000Z",'
        b'"name":"fixture-market-data","snapshot_id":"fixture-source-v1"}],'
        b'"format":"forecast-input-snapshot-v1","horizon_unit":"calendar_day",'
        b'"input_multiplier":1,"input_timespan":"day",'
        b'"observations":[{"available_at":"2026-07-08T21:00:00.000000Z",'
        b'"observed_at":"2026-07-08T20:00:00.000000Z","value_f64":"4058800000000000"},'
        b'{"available_at":"2026-07-09T21:00:00.000000Z",'
        b'"observed_at":"2026-07-09T20:00:00.000000Z","value_f64":"4059400000000000"}],'
        b'"resolution_policy_hash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
        b'aaaaaaaaaaaaaaaa","schema_version":1,"series_basis":"raw","symbol":"AAPL",'
        b'"target":"close","target_times":["2026-07-11T21:00:00.000000Z",'
        b'"2026-07-12T21:00:00.000000Z"]}'
    )
    assert snapshot_id_for_payload(canonical) == (
        "sha256:14b0397bae00d77c98b2049ee09760d29b91fef21b2e044c6cbc273fd9641e57"
    )


def test_order_offsets_source_duplicates_and_negative_zero_canonicalize_identically() -> None:
    offset = timezone(timedelta(hours=3))
    baseline_observations = (
        replace(_payload().observations[0], value=0.0),
        _payload().observations[1],
    )
    observations = tuple(
        replace(
            item,
            observed_at=item.observed_at.astimezone(offset),
            available_at=item.available_at.astimezone(offset),
            value=-0.0 if item.value == 0.0 else item.value,
        )
        for item in reversed(baseline_observations)
    )
    baseline = replace(
        _payload(),
        observations=baseline_observations,
    )
    permuted = replace(
        _payload(),
        as_of=AS_OF.astimezone(offset),
        observations=observations,
        target_times=tuple(value.astimezone(offset) for value in reversed(_payload().target_times)),
        data_sources=(
            replace(_payload().data_sources[0], fields=("volume",)),
            replace(_payload().data_sources[0], fields=("close", "volume")),
        ),
    )

    assert canonical_snapshot_payload(permuted) == canonical_snapshot_payload(baseline)


@pytest.mark.parametrize(
    "changed",
    [
        replace(_payload(), symbol="MSFT"),
        replace(_payload(), series_basis="raw", currency="EUR"),
        replace(_payload(), input_timespan="hour"),
        replace(_payload(), input_multiplier=2),
        replace(
            _payload(),
            observations=(
                _payload().observations[0],
                replace(_payload().observations[1], value=102.0),
            ),
        ),
        replace(
            _payload(),
            observations=(
                _payload().observations[0],
                replace(
                    _payload().observations[1],
                    available_at=_payload().observations[1].available_at + timedelta(seconds=1),
                ),
            ),
        ),
        replace(
            _payload(),
            target_times=(AS_OF + timedelta(days=1), AS_OF + timedelta(days=3)),
        ),
        replace(
            _payload(),
            data_sources=(replace(_payload().data_sources[0], snapshot_id="fixture-source-v2"),),
        ),
    ],
)
def test_every_semantic_change_changes_the_snapshot_id(
    changed: ForecastInputSnapshotPayload,
) -> None:
    assert _record().snapshot_id != build_snapshot_record(changed, sealed_at=SEALED_AT).snapshot_id


def test_strict_parser_round_trips_only_canonical_bytes() -> None:
    canonical = canonical_snapshot_payload(_payload())
    parsed = parse_snapshot_payload(canonical)

    assert canonical_snapshot_payload(parsed) == canonical
    with pytest.raises(SnapshotValidationError, match="strict UTF-8 JSON"):
        parse_snapshot_payload(b'{"x": 1, "x": 2}')
    with pytest.raises(SnapshotValidationError, match="unknown keys"):
        parse_snapshot_payload(canonical[:-1] + b',"unknown":1}')
    noncanonical = canonical.replace(b'"schema_version":1', b'"schema_version": 1')
    record = replace(
        _record(),
        canonical_payload=noncanonical,
        snapshot_id=snapshot_id_for_payload(noncanonical),
    )
    with pytest.raises(SnapshotValidationError, match="not canonical"):
        validate_and_resolve_snapshot(
            record,
            _request(record.snapshot_id),
            expected_series_basis="raw",
            expected_resolution_policy_hash=POLICY_HASH,
        )


def test_resolver_binds_exact_payload_and_keeps_unproven_availability_not_run() -> None:
    record = _record()
    resolved = validate_and_resolve_snapshot(
        record,
        _request(record.snapshot_id),
        expected_series_basis="raw",
        expected_resolution_policy_hash=POLICY_HASH,
    )

    assert resolved.snapshot_id == record.snapshot_id
    assert resolved.symbol == "AAPL"
    assert [item.value for item in resolved.observations] == [98.0, 101.0]
    assert resolved.target_times == _payload().target_times
    assert resolved.data_sources[0].fields == ["close", "volume"]
    assert resolved.availability_verified is False


def test_only_an_explicitly_trusted_persisted_rule_set_can_pass_availability() -> None:
    evidence = SnapshotAvailabilityEvidence(
        status="passed",
        rule_set_hash=RULE_SET_HASH,
        checked_at=AS_OF + timedelta(minutes=1),
    )
    record = _record(availability=evidence)
    request = _request(record.snapshot_id)

    untrusted = validate_and_resolve_snapshot(
        record,
        request,
        expected_series_basis="raw",
        expected_resolution_policy_hash=POLICY_HASH,
    )
    trusted = validate_and_resolve_snapshot(
        record,
        request,
        expected_series_basis="raw",
        expected_resolution_policy_hash=POLICY_HASH,
        trusted_availability_rule_set_hash=RULE_SET_HASH,
    )

    assert untrusted.availability_verified is False
    assert trusted.availability_verified is True


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda record: replace(record, observation_count=99), "header"),
        (lambda record: replace(record, schema_version=True), "header"),
        (lambda record: replace(record, snapshot_id="sha256:" + "0" * 64), "snapshot_id"),
        (lambda record: replace(record, symbol="MSFT"), "header"),
        (
            lambda record: replace(record, canonical_payload=record.canonical_payload + b" "),
            "not canonical",
        ),
    ],
)
def test_tampered_hash_payload_or_header_fails_closed(mutate, message: str) -> None:
    original = _record()
    tampered = mutate(original)
    with pytest.raises(SnapshotValidationError, match=message):
        validate_and_resolve_snapshot(
            tampered,
            _request(tampered.snapshot_id),
            expected_series_basis="raw",
            expected_resolution_policy_hash=POLICY_HASH,
        )


@pytest.mark.parametrize(
    ("request_overrides", "basis", "policy", "message"),
    [
        ({"symbol": "MSFT"}, "raw", POLICY_HASH, "symbol"),
        ({"target": "return"}, "raw", POLICY_HASH, "target"),
        ({"horizon_unit": "hour"}, "raw", POLICY_HASH, "horizon unit"),
        ({"snapshot_id": "sha256:" + "0" * 64}, "raw", POLICY_HASH, "pinned"),
        ({}, "split_adjusted", POLICY_HASH, "series basis"),
        ({}, "raw", "sha256:" + "c" * 64, "resolution policy"),
        ({"horizon": 3}, "raw", POLICY_HASH, "target horizon"),
    ],
)
def test_request_selector_policy_and_horizon_mismatches_fail_closed(
    request_overrides: dict[str, object],
    basis: str,
    policy: str,
    message: str,
) -> None:
    record = _record()
    requested_snapshot_id = request_overrides.get("snapshot_id", record.snapshot_id)
    remaining_overrides = {
        key: value for key, value in request_overrides.items() if key != "snapshot_id"
    }
    with pytest.raises(SnapshotValidationError, match=message):
        validate_and_resolve_snapshot(
            record,
            _request(requested_snapshot_id, **remaining_overrides),  # type: ignore[arg-type]
            expected_series_basis=basis,
            expected_resolution_policy_hash=policy,
        )


def test_unpinned_request_enforces_as_of_cutoff_but_pinned_id_takes_precedence() -> None:
    record = _record()
    with pytest.raises(SnapshotValidationError, match="later than"):
        validate_and_resolve_snapshot(
            record,
            _request(None, as_of=AS_OF - timedelta(seconds=1)),
            expected_series_basis="raw",
            expected_resolution_policy_hash=POLICY_HASH,
        )

    resolved = validate_and_resolve_snapshot(
        record,
        _request(record.snapshot_id, as_of=AS_OF - timedelta(days=1)),
        expected_series_basis="raw",
        expected_resolution_policy_hash=POLICY_HASH,
    )
    assert resolved.as_of == AS_OF


@pytest.mark.parametrize(
    ("timespan", "multiplier", "message"),
    [("hour", 1, "input_timespan"), ("day", 2, "input_multiplier")],
)
def test_input_series_frequency_is_bound_explicitly(
    timespan: str,
    multiplier: int,
    message: str,
) -> None:
    record = _record()
    with pytest.raises(SnapshotValidationError, match=message):
        validate_and_resolve_snapshot(
            record,
            _request(record.snapshot_id),
            expected_series_basis="raw",
            expected_resolution_policy_hash=POLICY_HASH,
            expected_input_timespan=timespan,
            expected_input_multiplier=multiplier,
        )


@pytest.mark.parametrize(
    "payload",
    [
        replace(_payload(), observations=()),
        replace(
            _payload(),
            observations=(
                _payload().observations[0],
                replace(_payload().observations[0], value=99.0),
            ),
        ),
        replace(
            _payload(),
            observations=(replace(_payload().observations[0], value=math.nan),),
        ),
        replace(
            _payload(),
            observations=(replace(_payload().observations[0], value=True),),
        ),
        replace(
            _payload(),
            observations=(replace(_payload().observations[0], value=-1.0),),
        ),
        replace(
            _payload(),
            observations=(
                replace(_payload().observations[0], available_at=AS_OF + timedelta(seconds=1)),
            ),
        ),
        replace(_payload(), target_times=()),
        replace(_payload(), target_times=(AS_OF,)),
        replace(_payload(), input_multiplier=10_001),
        replace(_payload(), currency="123"),
        replace(
            _payload(),
            data_sources=(
                replace(
                    _payload().data_sources[0],
                    max_available_at=AS_OF + timedelta(seconds=1),
                ),
            ),
        ),
        replace(
            _payload(),
            data_sources=(
                replace(
                    _payload().data_sources[0],
                    fields="close",  # type: ignore[arg-type]
                ),
            ),
        ),
        replace(
            _payload(),
            availability=SnapshotAvailabilityEvidence(
                status="not_run",
                rule_set_hash=RULE_SET_HASH,
            ),
        ),
        replace(
            _payload(),
            availability=SnapshotAvailabilityEvidence(
                status="passed",
                rule_set_hash=RULE_SET_HASH,
                checked_at=AS_OF - timedelta(seconds=1),
            ),
        ),
    ],
)
def test_invalid_values_ordering_lookahead_or_evidence_are_rejected(
    payload: ForecastInputSnapshotPayload,
) -> None:
    with pytest.raises(SnapshotValidationError):
        canonical_snapshot_payload(payload)


def test_forecast_snapshot_model_and_migration_pin_insert_only_storage_contract() -> None:
    assert Base.metadata.tables["forecast_input_snapshots"] is ForecastInputSnapshot.__table__
    assert tuple(column.name for column in ForecastInputSnapshot.__table__.primary_key) == (
        "snapshot_id",
    )
    assert isinstance(ForecastInputSnapshot.__table__.c.canonical_payload.type, LargeBinary)
    for column_name in (
        "as_of",
        "sealed_at",
        "first_observed_at",
        "last_observed_at",
        "max_available_at",
        "availability_checked_at",
    ):
        column_type = ForecastInputSnapshot.__table__.c[column_name].type
        assert isinstance(column_type, DateTime)
        assert column_type.timezone is True

    constraints = {
        str(constraint.name) for constraint in ForecastInputSnapshot.__table__.constraints
    }
    assert {
        "ck_forecast_input_snapshots_payload_hash_matches_id",
        "ck_forecast_input_snapshots_availability_cutoff",
        "uq_forecast_input_snapshots_semantic_key",
    } <= constraints
    migration = Path("migrations/versions/0005_forecast_input_snapshots.py").read_text(
        encoding="utf-8"
    )
    assert 'down_revision: str | None = "0004_bars_finiteness"' in migration
    assert "CREATE EXTENSION IF NOT EXISTS pgcrypto" in migration
    assert "digest(canonical_payload, 'sha256')" in migration
    assert "NEW.sealed_at := clock_timestamp()" in migration
    assert "BEFORE UPDATE OR DELETE" in migration
    assert "BEFORE TRUNCATE" in migration


def test_parser_rejects_nonfinite_json_constants_before_schema_validation() -> None:
    document = json.loads(canonical_snapshot_payload(_payload()))
    document["unknown"] = math.nan
    raw = json.dumps(document, allow_nan=True, separators=(",", ":")).encode()

    with pytest.raises(SnapshotValidationError, match="strict UTF-8 JSON"):
        parse_snapshot_payload(raw)


def test_malformed_depth_or_unicode_cannot_escape_the_validation_boundary() -> None:
    deeply_nested = b"[" * 1_100 + b"0" + b"]" * 1_100
    with pytest.raises(SnapshotValidationError):
        parse_snapshot_payload(deeply_nested)

    invalid_source = replace(
        _payload().data_sources[0],
        name="\ud800",
    )
    with pytest.raises(SnapshotValidationError, match="source name"):
        canonical_snapshot_payload(replace(_payload(), data_sources=(invalid_source,)))
