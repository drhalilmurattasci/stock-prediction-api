"""Fail-closed tests for the pure adjusted-OHLCV read kernel."""

from __future__ import annotations

import hashlib
import json
import math
import struct
import sys
from collections.abc import Sequence
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, date, datetime, time, timedelta, timezone
from decimal import Decimal

import pytest

from app.services.adjusted_prices import (
    ADJUSTED_PRICE_BASIS,
    AdjustedPriceError,
    AdjustedPriceWindow,
    AdjustmentFactorSetReceipt,
    CorporateActionCollectionReceipt,
    RawOhlcvVersion,
)
from app.services.adjusted_prices import (
    adjust_ohlcv_window as _adjust_ohlcv_window,
)
from app.services.adjustment_factors import (
    AdjustmentFactorSet,
    DividendActionVersion,
    RawCloseVersion,
    SplitActionVersion,
    build_adjustment_factor_set,
)

DATES = (
    date(2026, 7, 6),
    date(2026, 7, 7),
    date(2026, 7, 8),
    date(2026, 7, 9),
    date(2026, 7, 10),
)
CUTOFF = datetime(2026, 7, 10, 21, tzinfo=UTC)
SPLIT_COLLECTION_ID = "sha256:" + "1" * 64
DIVIDEND_COLLECTION_ID = "sha256:" + "2" * 64
SPLIT_VERSION_ID = "sha256:" + "3" * 64
DIVIDEND_VERSION_ID = "sha256:" + "4" * 64
CLOSES = (100.0, 102.0, 100.0, 104.0, 52.0)


def _collection_receipts() -> tuple[
    CorporateActionCollectionReceipt,
    CorporateActionCollectionReceipt,
]:
    return (
        CorporateActionCollectionReceipt(
            collection_id=SPLIT_COLLECTION_ID,
            collection_recorded_at=datetime(2026, 7, 10, 20, 7, tzinfo=UTC),
            available_at=datetime(2026, 7, 10, 20, 8, tzinfo=UTC),
        ),
        CorporateActionCollectionReceipt(
            collection_id=DIVIDEND_COLLECTION_ID,
            collection_recorded_at=datetime(2026, 7, 10, 20, 8, tzinfo=UTC),
            available_at=datetime(2026, 7, 10, 20, 9, tzinfo=UTC),
        ),
    )


def _factor_receipt(artifact: AdjustmentFactorSet) -> AdjustmentFactorSetReceipt:
    return AdjustmentFactorSetReceipt(
        factor_set_id=artifact.factor_set_id,
        factor_set_recorded_at=datetime(2026, 7, 10, 21, 1, tzinfo=UTC),
        available_at=datetime(2026, 7, 10, 21, 2, tzinfo=UTC),
    )


def adjust_ohlcv_window(
    *,
    factor_set: AdjustmentFactorSet,
    raw_rows: Sequence[RawOhlcvVersion],
    start_ordinal: int = 0,
    stop_ordinal: int | None = None,
    split_collection_receipt: CorporateActionCollectionReceipt | None = None,
    dividend_collection_receipt: CorporateActionCollectionReceipt | None = None,
    factor_set_receipt: AdjustmentFactorSetReceipt | None = None,
) -> AdjustedPriceWindow:
    """Supply valid persisted action receipts by default in focused tests."""

    default_split, default_dividend = _collection_receipts()
    return _adjust_ohlcv_window(
        factor_set=factor_set,
        raw_rows=raw_rows,
        split_collection_receipt=split_collection_receipt or default_split,
        dividend_collection_receipt=dividend_collection_receipt or default_dividend,
        factor_set_receipt=factor_set_receipt or _factor_receipt(factor_set),
        start_ordinal=start_ordinal,
        stop_ordinal=stop_ordinal,
    )


def _raw_closes() -> tuple[RawCloseVersion, ...]:
    return tuple(
        RawCloseVersion(
            observation_date=session,
            observed_at=datetime.combine(session, time(20), tzinfo=UTC),
            timespan="day",
            multiplier=1,
            source="polygon_open_close",
            adjustment_basis="raw",
            version_recorded_at=datetime.combine(session, time(20, 5), tzinfo=UTC),
            available_at=datetime.combine(session, time(20, 6), tzinfo=UTC),
            close=Decimal(str(close)),
        )
        for session, close in zip(DATES, CLOSES, strict=True)
    )


def _artifact(
    *,
    splits: tuple[SplitActionVersion, ...] | None = None,
) -> AdjustmentFactorSet:
    return build_adjustment_factor_set(
        symbol="MSFT",
        cutoff=CUTOFF,
        raw_closes=_raw_closes(),
        split_collection_id=SPLIT_COLLECTION_ID,
        splits=(
            SplitActionVersion(
                provider_event_id="split-event",
                version_id=SPLIT_VERSION_ID,
                effective_date=DATES[-1],
                split_from=Decimal("1"),
                split_to=Decimal("2"),
                adjustment_type="forward_split",
            ),
        )
        if splits is None
        else splits,
        dividend_collection_id=DIVIDEND_COLLECTION_ID,
        dividends=(
            DividendActionVersion(
                provider_event_id="dividend-event",
                version_id=DIVIDEND_VERSION_ID,
                ex_dividend_date=DATES[2],
                cash_amount=Decimal("2"),
                currency="USD",
                distribution_type="recurring",
            ),
        ),
    )


def _raw_rows() -> tuple[RawOhlcvVersion, ...]:
    opens = (99.0, 101.0, 99.0, 103.0, 52.0)
    highs = (101.0, 103.0, 102.0, 105.0, 53.0)
    lows = (98.0, 100.0, 98.0, 102.0, 51.0)
    return tuple(
        RawOhlcvVersion(
            symbol="MSFT",
            timestamp=raw.observed_at,
            timespan="day",
            multiplier=1,
            source="polygon_open_close",
            adjustment_basis="raw",
            version_recorded_at=raw.version_recorded_at,
            available_at=raw.available_at,
            open=open_value,
            high=high,
            low=low,
            close=close,
            volume=1000.0 + ordinal * 100.0,
            vwap=None if ordinal == 1 else close - 0.25,
            trade_count=None if ordinal == 2 else 100 + ordinal,
        )
        for ordinal, (raw, open_value, high, low, close) in enumerate(
            zip(_raw_closes(), opens, highs, lows, CLOSES, strict=True)
        )
    )


def _mutate_published_factor(
    artifact: AdjustmentFactorSet,
    *,
    ordinal: int,
    field: str,
    bits: str,
) -> AdjustmentFactorSet:
    document = json.loads(artifact.canonical_payload)
    document["factors"][ordinal][field] = bits
    payload = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode()
    factors = list(artifact.factors)
    if field == "price_factor_f64_be":
        factors[ordinal] = replace(factors[ordinal], price_factor_f64_be=bits)
    elif field == "volume_factor_f64_be":
        factors[ordinal] = replace(factors[ordinal], volume_factor_f64_be=bits)
    else:  # pragma: no cover - private test-helper contract
        raise AssertionError(f"unsupported factor field: {field}")
    return replace(
        artifact,
        canonical_payload=payload,
        factor_set_id="sha256:" + hashlib.sha256(payload).hexdigest(),
        factors=tuple(factors),
    )


def test_golden_adjusted_ohlcv_and_lineage_use_published_binary64_factors() -> None:
    artifact = _artifact()
    raw = _raw_rows()

    result = adjust_ohlcv_window(factor_set=artifact, raw_rows=raw)

    assert result.start_ordinal == 0
    assert result.stop_ordinal == 5
    assert [row.raw_input_ordinal for row in result.rows] == list(range(5))
    assert all(row.adjustment_basis == ADJUSTED_PRICE_BASIS for row in result.rows)
    for ordinal, (input_row, adjusted, factor) in enumerate(
        zip(raw, result.rows, artifact.factors, strict=True)
    ):
        price_factor = struct.unpack(">d", bytes.fromhex(factor.price_factor_f64_be))[0]
        volume_factor = struct.unpack(">d", bytes.fromhex(factor.volume_factor_f64_be))[0]
        assert adjusted.open == input_row.open * price_factor
        assert adjusted.high == input_row.high * price_factor
        assert adjusted.low == input_row.low * price_factor
        assert adjusted.close == input_row.close * price_factor
        assert adjusted.volume == input_row.volume * volume_factor
        assert adjusted.vwap == (None if input_row.vwap is None else input_row.vwap * price_factor)
        assert adjusted.trade_count == input_row.trade_count
        assert adjusted.raw_version_recorded_at == input_row.version_recorded_at
        assert adjusted.raw_available_at == input_row.available_at
        assert adjusted.available_at == datetime(2026, 7, 10, 21, 2, tzinfo=UTC)
        assert adjusted.price_factor_f64_be == factor.price_factor_f64_be
        assert adjusted.volume_factor_f64_be == factor.volume_factor_f64_be
        assert adjusted.raw_input_ordinal == ordinal

    assert result.lineage.factor_set_id == artifact.factor_set_id
    assert result.lineage.factor_set_recorded_at == datetime(2026, 7, 10, 21, 1, tzinfo=UTC)
    assert result.lineage.factor_set_available_at == datetime(2026, 7, 10, 21, 2, tzinfo=UTC)
    assert result.lineage.policy_version == artifact.policy_version
    assert result.lineage.policy_hash == artifact.policy_hash
    assert result.lineage.cutoff == CUTOFF
    assert result.lineage.anchor_date == DATES[-1]
    assert result.lineage.split_collection_id == SPLIT_COLLECTION_ID
    assert result.lineage.dividend_collection_id == DIVIDEND_COLLECTION_ID
    assert result.lineage.action_version_ids == (SPLIT_VERSION_ID, DIVIDEND_VERSION_ID)
    assert result.lineage.split_collection_recorded_at == datetime(2026, 7, 10, 20, 7, tzinfo=UTC)
    assert result.lineage.split_collection_available_at == datetime(2026, 7, 10, 20, 8, tzinfo=UTC)
    assert result.lineage.dividend_collection_recorded_at == datetime(
        2026, 7, 10, 20, 8, tzinfo=UTC
    )
    assert result.lineage.dividend_collection_available_at == datetime(
        2026, 7, 10, 20, 9, tzinfo=UTC
    )
    assert result.lineage.max_input_available_at == datetime(2026, 7, 10, 20, 9, tzinfo=UTC)
    assert result.lineage.data_available_at == datetime(2026, 7, 10, 21, 2, tzinfo=UTC)
    assert result.lineage.raw_input_count == len(raw)
    assert result.lineage.adjustment_basis == ADJUSTED_PRICE_BASIS


def test_decimal_factor_projections_are_bound_to_the_canonical_payload() -> None:
    artifact = _artifact()
    poisoned = replace(
        artifact,
        factors=tuple(
            replace(
                factor,
                price_factor_decimal="not-a-number",
                volume_factor_decimal="Infinity",
            )
            for factor in artifact.factors
        ),
    )

    with pytest.raises(AdjustedPriceError, match="factor projection"):
        adjust_ohlcv_window(factor_set=poisoned, raw_rows=_raw_rows())


@pytest.mark.parametrize(
    ("section", "field", "replacement"),
    [
        ("factors", "price_factor_decimal", "999"),
        ("factors", "volume_factor_decimal", "999"),
        ("raw_inputs", "close_decimal", "999"),
    ],
)
def test_rehashed_decimal_only_payload_tampering_is_rejected(
    section: str,
    field: str,
    replacement: str,
) -> None:
    artifact = _artifact()
    document = json.loads(artifact.canonical_payload)
    document[section][0][field] = replacement
    payload = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    rehashed = replace(
        artifact,
        canonical_payload=payload,
        factor_set_id="sha256:" + hashlib.sha256(payload).hexdigest(),
    )

    with pytest.raises(AdjustedPriceError, match="projection"):
        adjust_ohlcv_window(factor_set=rehashed, raw_rows=_raw_rows())


@pytest.mark.parametrize("location", ["root", "raw", "factor", "action"])
def test_rehashed_extra_canonical_fields_are_rejected(location: str) -> None:
    artifact = _artifact()
    document = json.loads(artifact.canonical_payload)
    if location == "root":
        document["extra"] = "unsupported"
    elif location == "raw":
        document["raw_inputs"][0]["extra"] = "unsupported"
    elif location == "factor":
        document["factors"][0]["extra"] = "unsupported"
    else:
        document["actions"]["splits"]["versions"][0]["extra"] = "unsupported"
    payload = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    rehashed = replace(
        artifact,
        canonical_payload=payload,
        factor_set_id="sha256:" + hashlib.sha256(payload).hexdigest(),
    )

    with pytest.raises(AdjustedPriceError, match="structure|projection|action row"):
        adjust_ohlcv_window(factor_set=rehashed, raw_rows=_raw_rows())


def test_rehashed_noncanonical_json_bytes_are_rejected() -> None:
    artifact = _artifact()
    payload = artifact.canonical_payload.replace(b"{", b"{ ", 1)
    rehashed = replace(
        artifact,
        canonical_payload=payload,
        factor_set_id="sha256:" + hashlib.sha256(payload).hexdigest(),
    )

    with pytest.raises(AdjustedPriceError, match="exact canonical JSON"):
        adjust_ohlcv_window(factor_set=rehashed, raw_rows=_raw_rows())


@pytest.mark.parametrize(
    ("index", "changes"),
    [
        (0, {"symbol": "AAPL"}),
        (0, {"timestamp": _raw_rows()[0].timestamp + timedelta(microseconds=1)}),
        (0, {"timespan": "hour"}),
        (0, {"multiplier": 2}),
        (0, {"source": "polygon"}),
        (0, {"adjustment_basis": "split_adjusted"}),
        (
            0,
            {"version_recorded_at": _raw_rows()[0].version_recorded_at + timedelta(microseconds=1)},
        ),
        (0, {"available_at": _raw_rows()[0].available_at + timedelta(microseconds=1)}),
        (0, {"close": math.nextafter(_raw_rows()[0].close, math.inf)}),
    ],
)
def test_every_raw_receipt_identity_and_exact_close_are_bound_by_ordinal(
    index: int,
    changes: dict[str, object],
) -> None:
    rows = list(_raw_rows())
    rows[index] = replace(rows[index], **changes)  # type: ignore[arg-type]

    with pytest.raises(AdjustedPriceError):
        adjust_ohlcv_window(factor_set=_artifact(), raw_rows=tuple(rows))


def test_equivalent_timezone_offsets_normalize_to_the_same_exact_instants() -> None:
    offset = timezone(timedelta(hours=3))
    rows = tuple(
        replace(
            row,
            timestamp=row.timestamp.astimezone(offset),
            version_recorded_at=row.version_recorded_at.astimezone(offset),
            available_at=row.available_at.astimezone(offset),
        )
        for row in _raw_rows()
    )

    result = adjust_ohlcv_window(factor_set=_artifact(), raw_rows=rows)

    assert all(row.timestamp.tzinfo is UTC for row in result.rows)
    assert all(row.raw_version_recorded_at.tzinfo is UTC for row in result.rows)
    assert all(row.raw_available_at.tzinfo is UTC for row in result.rows)
    assert all(row.available_at.tzinfo is UTC for row in result.rows)


@pytest.mark.parametrize(
    ("receipt_name", "changes"),
    [
        ("split", {"collection_id": DIVIDEND_COLLECTION_ID}),
        (
            "split",
            {"collection_recorded_at": datetime(2026, 7, 10, 20, 7)},
        ),
        (
            "dividend",
            {"available_at": CUTOFF + timedelta(microseconds=1)},
        ),
        (
            "dividend",
            {
                "collection_recorded_at": datetime(2026, 7, 10, 20, 10, tzinfo=UTC),
                "available_at": datetime(2026, 7, 10, 20, 9, tzinfo=UTC),
            },
        ),
    ],
)
def test_action_collection_receipts_are_exact_visible_factor_inputs(
    receipt_name: str,
    changes: dict[str, object],
) -> None:
    split, dividend = _collection_receipts()
    changed = replace(split if receipt_name == "split" else dividend, **changes)  # type: ignore[arg-type]
    kwargs = (
        {"split_collection_receipt": changed}
        if receipt_name == "split"
        else {"dividend_collection_receipt": changed}
    )

    with pytest.raises(AdjustedPriceError):
        adjust_ohlcv_window(
            factor_set=_artifact(),
            raw_rows=_raw_rows(),
            **kwargs,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"factor_set_id": "sha256:" + "0" * 64},
        {"factor_set_recorded_at": datetime(2026, 7, 10, 21, 1)},
        {
            "factor_set_recorded_at": datetime(2026, 7, 10, 21, 3, tzinfo=UTC),
            "available_at": datetime(2026, 7, 10, 21, 2, tzinfo=UTC),
        },
        {"available_at": datetime(2026, 7, 10, 20, 8, tzinfo=UTC)},
    ],
)
def test_exact_later_factor_publication_receipt_is_required(
    changes: dict[str, object],
) -> None:
    artifact = _artifact()
    receipt = replace(_factor_receipt(artifact), **changes)  # type: ignore[arg-type]

    with pytest.raises(AdjustedPriceError, match="factor-set"):
        adjust_ohlcv_window(
            factor_set=artifact,
            raw_rows=_raw_rows(),
            factor_set_receipt=receipt,
        )


@pytest.mark.parametrize(
    "rows",
    [
        _raw_rows()[:-1],
        (*_raw_rows(), _raw_rows()[-1]),
        tuple(reversed(_raw_rows())),
    ],
)
def test_raw_rows_must_be_the_complete_one_to_one_chronological_window(
    rows: tuple[RawOhlcvVersion, ...],
) -> None:
    with pytest.raises(AdjustedPriceError):
        adjust_ohlcv_window(factor_set=_artifact(), raw_rows=rows)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("open", float("nan")),
        ("high", float("inf")),
        ("low", -1.0),
        ("volume", -1.0),
        ("vwap", float("inf")),
        ("trade_count", -1),
        ("trade_count", True),
        ("open", 99),
    ],
)
def test_raw_numeric_values_must_match_the_strict_finite_database_contract(
    field: str,
    value: object,
) -> None:
    rows = list(_raw_rows())
    rows[0] = replace(rows[0], **{field: value})  # type: ignore[arg-type]

    with pytest.raises(AdjustedPriceError):
        adjust_ohlcv_window(factor_set=_artifact(), raw_rows=tuple(rows))


def test_raw_ohlc_shape_is_revalidated() -> None:
    rows = list(_raw_rows())
    rows[0] = replace(rows[0], high=98.5)

    with pytest.raises(AdjustedPriceError, match="raw OHLC"):
        adjust_ohlcv_window(factor_set=_artifact(), raw_rows=tuple(rows))


@pytest.mark.parametrize(
    "bits",
    [
        "not-binary64-hex",
        "0000000000000000",
        "bff0000000000000",
        "7ff0000000000000",
        "7ff8000000000000",
        "3FF0000000000000",
    ],
)
def test_published_factor_bits_must_be_canonical_positive_finite_binary64(bits: str) -> None:
    artifact = _mutate_published_factor(
        _artifact(),
        ordinal=0,
        field="price_factor_f64_be",
        bits=bits,
    )

    with pytest.raises(AdjustedPriceError, match="published price factor"):
        adjust_ohlcv_window(factor_set=artifact, raw_rows=_raw_rows())


def test_artifact_projection_and_content_address_must_be_intact() -> None:
    artifact = _artifact()
    with pytest.raises(AdjustedPriceError, match="content address"):
        adjust_ohlcv_window(
            factor_set=replace(artifact, factor_set_id="sha256:" + "0" * 64),
            raw_rows=_raw_rows(),
        )

    with pytest.raises(AdjustedPriceError, match="action-version projection"):
        adjust_ohlcv_window(
            factor_set=replace(
                artifact,
                action_version_ids=("sha256:" + "f" * 64,),
            ),
            raw_rows=_raw_rows(),
        )

    factors = list(artifact.factors)
    factors[0] = replace(factors[0], price_factor_f64_be="3ff0000000000000")
    with pytest.raises(AdjustedPriceError, match="projection"):
        adjust_ohlcv_window(
            factor_set=replace(artifact, factors=tuple(factors)),
            raw_rows=_raw_rows(),
        )


def test_adjusted_overflow_outside_requested_slice_still_fails_closed() -> None:
    reverse_split = SplitActionVersion(
        provider_event_id="reverse-split",
        version_id=SPLIT_VERSION_ID,
        effective_date=DATES[-1],
        split_from=Decimal("2"),
        split_to=Decimal("1"),
        adjustment_type="reverse_split",
    )
    rows = list(_raw_rows())
    rows[0] = replace(
        rows[0],
        open=sys.float_info.max,
        high=sys.float_info.max,
        low=0.0,
    )

    with pytest.raises(AdjustedPriceError, match="adjusted open"):
        adjust_ohlcv_window(
            factor_set=_artifact(splits=(reverse_split,)),
            raw_rows=tuple(rows),
            start_ordinal=4,
            stop_ordinal=5,
        )


def test_positive_adjusted_value_cannot_silently_underflow_to_zero() -> None:
    artifact = _mutate_published_factor(
        _artifact(),
        ordinal=0,
        field="price_factor_f64_be",
        bits="0000000000000001",
    )
    rows = list(_raw_rows())
    rows[0] = replace(rows[0], open=math.nextafter(0.0, 1.0), low=0.0)

    with pytest.raises(AdjustedPriceError, match="adjusted open"):
        adjust_ohlcv_window(factor_set=artifact, raw_rows=tuple(rows))


def test_slicing_happens_only_after_the_full_window_is_validated() -> None:
    artifact = _artifact()
    rows = _raw_rows()
    full = adjust_ohlcv_window(factor_set=artifact, raw_rows=rows)
    sliced = adjust_ohlcv_window(
        factor_set=artifact,
        raw_rows=rows,
        start_ordinal=1,
        stop_ordinal=4,
    )

    assert sliced.rows == full.rows[1:4]
    assert sliced.lineage == full.lineage
    assert sliced.start_ordinal == 1
    assert sliced.stop_ordinal == 4

    invalid_outside_slice = list(rows)
    invalid_outside_slice[-1] = replace(
        invalid_outside_slice[-1],
        available_at=invalid_outside_slice[-1].available_at + timedelta(microseconds=1),
    )
    with pytest.raises(AdjustedPriceError):
        adjust_ohlcv_window(
            factor_set=artifact,
            raw_rows=tuple(invalid_outside_slice),
            start_ordinal=1,
            stop_ordinal=4,
        )


@pytest.mark.parametrize(
    ("start", "stop"),
    [
        (-1, None),
        (True, None),
        (6, None),
        (2, 1),
        (0, 6),
        (0, True),
    ],
)
def test_slice_bounds_are_explicit_bounded_ordinals(
    start: int,
    stop: int | None,
) -> None:
    with pytest.raises(AdjustedPriceError):
        adjust_ohlcv_window(
            factor_set=_artifact(),
            raw_rows=_raw_rows(),
            start_ordinal=start,
            stop_ordinal=stop,
        )


def test_result_rows_and_lineage_are_frozen() -> None:
    result = adjust_ohlcv_window(factor_set=_artifact(), raw_rows=_raw_rows())

    with pytest.raises(FrozenInstanceError):
        result.rows[0].close = 0.0  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        result.lineage.factor_set_id = "sha256:" + "0" * 64  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        result.start_ordinal = 1  # type: ignore[misc]
