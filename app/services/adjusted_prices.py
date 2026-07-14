"""Pure adjusted-OHLCV projection from one immutable factor artifact.

This module deliberately performs no database or provider I/O.  Its input is
the complete set of exact raw bar receipts bound by an
``AdjustmentFactorSet``.  Every receipt is validated before an optional
ordinal slice is returned, so pagination cannot accidentally re-anchor or
recompute adjustment factors over a partial window.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import struct
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_EVEN, Decimal, DecimalException, localcontext
from typing import Any

from app.services.adjustment_factors import (
    ADJUSTMENT_FACTOR_POLICY_HASH,
    ADJUSTMENT_FACTOR_POLICY_VERSION,
    ADJUSTMENT_FACTOR_SET_FORMAT,
    DECIMAL_PRECISION,
    AdjustmentFactor,
    AdjustmentFactorSet,
    RawCloseVersion,
)

ADJUSTED_PRICE_BASIS = "split_dividend_adjusted"

_BINARY64_HEX = re.compile(r"^[0-9a-f]{16}$")
_HASH_PREFIX = "sha256:"
_ROOT_KEYS = frozenset(
    {
        "actions",
        "anchor_date",
        "cutoff",
        "factors",
        "format",
        "policy_hash",
        "policy_version",
        "raw_inputs",
        "symbol",
    }
)
_ACTION_KEYS = frozenset({"dividends", "splits"})
_ACTION_COLLECTION_KEYS = frozenset({"collection_id", "versions"})
_SPLIT_KEYS = frozenset(
    {
        "adjustment_type",
        "effective_date",
        "provider_event_id",
        "split_from",
        "split_to",
        "version_id",
    }
)
_DIVIDEND_KEYS = frozenset(
    {
        "cash_amount",
        "currency",
        "distribution_type",
        "ex_dividend_date",
        "provider_event_id",
        "version_id",
    }
)


class AdjustedPriceError(ValueError):
    """The supplied artifact and raw OHLCV receipts are not one exact window."""


@dataclass(frozen=True, slots=True)
class RawOhlcvVersion:
    """One exact, receipted raw daily OHLCV version.

    ``timestamp`` is the bar observation instant (the exact regular XNYS
    session close for this v1 factor policy), not a fetch or publication time.
    """

    symbol: str
    timestamp: datetime
    timespan: str
    multiplier: int
    source: str
    adjustment_basis: str
    version_recorded_at: datetime
    available_at: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float | None
    trade_count: int | None


@dataclass(frozen=True, slots=True)
class CorporateActionCollectionReceipt:
    """Exact DB-stamped publication receipt for one action collection."""

    collection_id: str
    collection_recorded_at: datetime
    available_at: datetime


@dataclass(frozen=True, slots=True)
class AdjustmentFactorSetReceipt:
    """Exact later-transaction publication receipt for the factor artifact."""

    factor_set_id: str
    factor_set_recorded_at: datetime
    available_at: datetime


@dataclass(frozen=True, slots=True)
class AdjustedOhlcvRow:
    """One adjusted row tied to an exact raw ordinal and published factors."""

    raw_input_ordinal: int
    symbol: str
    timestamp: datetime
    timespan: str
    multiplier: int
    source: str
    adjustment_basis: str
    raw_version_recorded_at: datetime
    raw_available_at: datetime
    available_at: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float | None
    trade_count: int | None
    price_factor_f64_be: str
    volume_factor_f64_be: str


@dataclass(frozen=True, slots=True)
class AdjustedPriceLineage:
    """Immutable provenance shared by every row derived from one factor set."""

    factor_set_id: str
    factor_set_recorded_at: datetime
    factor_set_available_at: datetime
    policy_version: str
    policy_hash: str
    cutoff: datetime
    anchor_date: date
    split_collection_id: str
    split_collection_recorded_at: datetime
    split_collection_available_at: datetime
    dividend_collection_id: str
    dividend_collection_recorded_at: datetime
    dividend_collection_available_at: datetime
    action_version_ids: tuple[str, ...]
    max_input_available_at: datetime
    data_available_at: datetime
    raw_input_count: int
    adjustment_basis: str = ADJUSTED_PRICE_BASIS


@dataclass(frozen=True, slots=True)
class AdjustedPriceWindow:
    """A validated full-window projection, optionally narrowed after validation."""

    rows: tuple[AdjustedOhlcvRow, ...]
    lineage: AdjustedPriceLineage
    start_ordinal: int
    stop_ordinal: int


def adjust_ohlcv_window(
    *,
    factor_set: AdjustmentFactorSet,
    raw_rows: Sequence[RawOhlcvVersion],
    split_collection_receipt: CorporateActionCollectionReceipt,
    dividend_collection_receipt: CorporateActionCollectionReceipt,
    factor_set_receipt: AdjustmentFactorSetReceipt,
    start_ordinal: int = 0,
    stop_ordinal: int | None = None,
) -> AdjustedPriceWindow:
    """Apply published binary64 factors to one complete exact raw window.

    Callers must supply every raw receipt bound by ``factor_set`` even when
    requesting a slice.  Factor Decimal strings are never parsed: the exact
    published IEEE-754 bit patterns are decoded and used directly.
    """

    artifact = _validated_artifact(factor_set)
    rows = _raw_sequence(raw_rows)
    _validate_cardinality(artifact, rows)
    split_receipt = _collection_receipt(
        split_collection_receipt,
        expected_id=artifact.split_collection_id,
        cutoff=artifact.cutoff,
        label="split collection",
    )
    dividend_receipt = _collection_receipt(
        dividend_collection_receipt,
        expected_id=artifact.dividend_collection_id,
        cutoff=artifact.cutoff,
        label="dividend collection",
    )
    normalized_rows: list[RawOhlcvVersion] = []
    for ordinal, (raw_row, factor_input) in enumerate(zip(rows, artifact.raw_inputs, strict=True)):
        normalized = _validate_raw_row(
            ordinal=ordinal,
            row=raw_row,
            expected=factor_input,
            artifact=artifact,
        )
        normalized_rows.append(normalized)
    input_available_at = max(
        tuple(row.available_at for row in normalized_rows)
        + (split_receipt.available_at, dividend_receipt.available_at)
    )
    factor_receipt = _factor_set_receipt(
        factor_set_receipt,
        expected_id=artifact.factor_set_id,
        minimum_available_at=input_available_at,
    )

    adjusted = [
        _adjust_row(
            ordinal,
            normalized,
            factor,
            derived_available_at=factor_receipt.available_at,
        )
        for ordinal, (normalized, factor) in enumerate(
            zip(normalized_rows, artifact.factors, strict=True)
        )
    ]

    full_window = tuple(adjusted)
    if any(
        left.timestamp >= right.timestamp
        for left, right in zip(full_window, full_window[1:], strict=False)
    ):
        raise AdjustedPriceError("raw rows must be strictly chronological")
    start, stop = _slice_bounds(start_ordinal, stop_ordinal, len(full_window))
    lineage = AdjustedPriceLineage(
        factor_set_id=artifact.factor_set_id,
        factor_set_recorded_at=factor_receipt.factor_set_recorded_at,
        factor_set_available_at=factor_receipt.available_at,
        policy_version=artifact.policy_version,
        policy_hash=artifact.policy_hash,
        cutoff=_utc(artifact.cutoff, "factor cutoff"),
        anchor_date=artifact.anchor_date,
        split_collection_id=artifact.split_collection_id,
        split_collection_recorded_at=split_receipt.collection_recorded_at,
        split_collection_available_at=split_receipt.available_at,
        dividend_collection_id=artifact.dividend_collection_id,
        dividend_collection_recorded_at=dividend_receipt.collection_recorded_at,
        dividend_collection_available_at=dividend_receipt.available_at,
        action_version_ids=tuple(artifact.action_version_ids),
        max_input_available_at=input_available_at,
        data_available_at=factor_receipt.available_at,
        raw_input_count=len(full_window),
    )
    return AdjustedPriceWindow(
        rows=full_window[start:stop],
        lineage=lineage,
        start_ordinal=start,
        stop_ordinal=stop,
    )


def _validated_artifact(value: AdjustmentFactorSet) -> AdjustmentFactorSet:
    if not isinstance(value, AdjustmentFactorSet):
        raise AdjustedPriceError("factor_set must be an AdjustmentFactorSet")
    if value.policy_version != ADJUSTMENT_FACTOR_POLICY_VERSION:
        raise AdjustedPriceError("factor policy version is not supported")
    if value.policy_hash != ADJUSTMENT_FACTOR_POLICY_HASH:
        raise AdjustedPriceError("factor policy hash is not supported")
    if not isinstance(value.canonical_payload, bytes):
        raise AdjustedPriceError("factor canonical payload must be bytes")
    expected_id = _HASH_PREFIX + hashlib.sha256(value.canonical_payload).hexdigest()
    if value.factor_set_id != expected_id:
        raise AdjustedPriceError("factor set content address does not match its payload")
    try:
        document = json.loads(value.canonical_payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdjustedPriceError("factor canonical payload is not valid JSON") from exc
    if not isinstance(document, dict):
        raise AdjustedPriceError("factor canonical payload must be an object")
    if _canonical_json(document) != value.canonical_payload:
        raise AdjustedPriceError("factor payload is not exact canonical JSON")
    _validate_artifact_projection(value, document)
    return value


def _validate_artifact_projection(
    artifact: AdjustmentFactorSet,
    document: dict[str, Any],
) -> None:
    if frozenset(document) != _ROOT_KEYS:
        raise AdjustedPriceError("factor payload root does not have the exact canonical structure")
    try:
        actions = document["actions"]
        split_document = actions["splits"]
        dividend_document = actions["dividends"]
        payload_raw = document["raw_inputs"]
        payload_factors = document["factors"]
    except (KeyError, TypeError) as exc:
        raise AdjustedPriceError("factor payload is missing required projections") from exc
    if (
        document.get("format") != ADJUSTMENT_FACTOR_SET_FORMAT
        or document.get("policy_version") != artifact.policy_version
        or document.get("policy_hash") != artifact.policy_hash
        or document.get("symbol") != artifact.symbol
        or document.get("cutoff") != _timestamp(artifact.cutoff)
        or document.get("anchor_date") != artifact.anchor_date.isoformat()
        or frozenset(actions) != _ACTION_KEYS
        or not isinstance(split_document, dict)
        or not isinstance(dividend_document, dict)
        or frozenset(split_document) != _ACTION_COLLECTION_KEYS
        or frozenset(dividend_document) != _ACTION_COLLECTION_KEYS
        or split_document.get("collection_id") != artifact.split_collection_id
        or dividend_document.get("collection_id") != artifact.dividend_collection_id
    ):
        raise AdjustedPriceError("factor payload root projection does not match the artifact")
    if not isinstance(payload_raw, list) or not isinstance(payload_factors, list):
        raise AdjustedPriceError("factor payload row projections must be arrays")
    split_versions = split_document.get("versions")
    dividend_versions = dividend_document.get("versions")
    if not isinstance(split_versions, list) or not isinstance(dividend_versions, list):
        raise AdjustedPriceError("factor payload action versions must be arrays")
    version_ids: list[str] = []
    for rows, expected_keys, label in (
        (split_versions, _SPLIT_KEYS, "split"),
        (dividend_versions, _DIVIDEND_KEYS, "dividend"),
    ):
        for row in rows:
            if (
                not isinstance(row, dict)
                or frozenset(row) != expected_keys
                or any(type(row[key]) is not str for key in expected_keys)
            ):
                raise AdjustedPriceError(
                    f"factor payload contains an unsupported {label} action row"
                )
            version_id = row["version_id"]
            if re.fullmatch(r"sha256:[0-9a-f]{64}", version_id) is None:
                raise AdjustedPriceError("factor payload action version identity is malformed")
            version_ids.append(version_id)
    expected_version_ids = tuple(sorted(version_ids))
    if len(set(version_ids)) != len(version_ids) or tuple(artifact.action_version_ids) != (
        expected_version_ids
    ):
        raise AdjustedPriceError("factor action-version projection does not match the artifact")
    if len(payload_raw) != len(artifact.raw_inputs) or len(payload_factors) != len(
        artifact.factors
    ):
        raise AdjustedPriceError("factor payload row counts do not match the artifact")
    for ordinal, (raw, payload_row) in enumerate(
        zip(artifact.raw_inputs, payload_raw, strict=True)
    ):
        _validate_payload_raw_projection(ordinal, raw, payload_row)
    for ordinal, (factor, payload_row) in enumerate(
        zip(artifact.factors, payload_factors, strict=True)
    ):
        if not isinstance(factor, AdjustmentFactor) or not isinstance(payload_row, dict):
            raise AdjustedPriceError("factor payload contains an unsupported row")
        if (
            type(factor.raw_input_ordinal) is not int
            or not isinstance(factor.price_factor_decimal, str)
            or not isinstance(factor.price_factor_f64_be, str)
            or not isinstance(factor.volume_factor_decimal, str)
            or not isinstance(factor.volume_factor_f64_be, str)
        ):
            raise AdjustedPriceError("factor artifact contains an unsupported row projection")
        expected_row = {
            "price_factor_decimal": factor.price_factor_decimal,
            "price_factor_f64_be": factor.price_factor_f64_be,
            "raw_input_ordinal": ordinal,
            "volume_factor_decimal": factor.volume_factor_decimal,
            "volume_factor_f64_be": factor.volume_factor_f64_be,
        }
        if factor.raw_input_ordinal != ordinal or _canonical_json(payload_row) != _canonical_json(
            expected_row
        ):
            raise AdjustedPriceError("published factor projection does not match the artifact")


def _validate_payload_raw_projection(
    ordinal: int,
    raw: RawCloseVersion,
    payload_row: object,
) -> None:
    if not isinstance(raw, RawCloseVersion) or not isinstance(payload_row, dict):
        raise AdjustedPriceError("factor payload contains an unsupported raw input")
    if (
        type(raw.observation_date) is not date
        or not isinstance(raw.timespan, str)
        or type(raw.multiplier) is not int
        or not isinstance(raw.source, str)
        or not isinstance(raw.adjustment_basis, str)
        or not isinstance(raw.close, Decimal)
    ):
        raise AdjustedPriceError("factor artifact contains an unsupported raw projection")
    close = _decimal_binary64(raw.close, "factor raw close")
    expected_row = {
        "adjustment_basis": raw.adjustment_basis,
        "available_at": _timestamp(raw.available_at),
        "close_decimal": _decimal_text(raw.close, "factor raw close"),
        "close_f64_be": _float_bits(close),
        "multiplier": raw.multiplier,
        "observation_date": raw.observation_date.isoformat(),
        "observed_at": _timestamp(raw.observed_at),
        "source": raw.source,
        "timespan": raw.timespan,
        "version_recorded_at": _timestamp(raw.version_recorded_at),
    }
    if _canonical_json(payload_row) != _canonical_json(expected_row):
        raise AdjustedPriceError(f"factor raw input projection differs at ordinal {ordinal}")


def _canonical_json(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise AdjustedPriceError("factor payload cannot be represented as canonical JSON") from exc


def _decimal_text(value: Decimal, label: str) -> str:
    try:
        with localcontext() as context:
            context.prec = DECIMAL_PRECISION
            context.rounding = ROUND_HALF_EVEN
            normalized = value.normalize(context=context)
    except DecimalException as exc:
        raise AdjustedPriceError(f"{label} cannot be represented as a canonical decimal") from exc
    rendered = format(normalized, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return "0" if rendered == "-0" else rendered


def _raw_sequence(values: Sequence[RawOhlcvVersion]) -> tuple[RawOhlcvVersion, ...]:
    try:
        rows = tuple(values)
    except TypeError as exc:
        raise AdjustedPriceError("raw_rows must be a finite sequence") from exc
    if not rows:
        raise AdjustedPriceError("raw_rows must not be empty")
    if any(not isinstance(row, RawOhlcvVersion) for row in rows):
        raise AdjustedPriceError("raw_rows contain an unsupported value")
    return rows


def _collection_receipt(
    value: CorporateActionCollectionReceipt,
    *,
    expected_id: str,
    cutoff: datetime,
    label: str,
) -> CorporateActionCollectionReceipt:
    if not isinstance(value, CorporateActionCollectionReceipt):
        raise AdjustedPriceError(f"{label} receipt has an unsupported value")
    recorded_at = _utc(value.collection_recorded_at, f"{label} recorded_at")
    available_at = _utc(value.available_at, f"{label} available_at")
    if value.collection_id != expected_id:
        raise AdjustedPriceError(f"{label} receipt does not match the factor set")
    if not recorded_at <= available_at <= _utc(cutoff, "factor cutoff"):
        raise AdjustedPriceError(f"{label} receipt is outside the factor cutoff")
    return CorporateActionCollectionReceipt(
        collection_id=value.collection_id,
        collection_recorded_at=recorded_at,
        available_at=available_at,
    )


def _factor_set_receipt(
    value: AdjustmentFactorSetReceipt,
    *,
    expected_id: str,
    minimum_available_at: datetime,
) -> AdjustmentFactorSetReceipt:
    if not isinstance(value, AdjustmentFactorSetReceipt):
        raise AdjustedPriceError("factor-set receipt has an unsupported value")
    recorded_at = _utc(value.factor_set_recorded_at, "factor-set recorded_at")
    available_at = _utc(value.available_at, "factor-set available_at")
    if value.factor_set_id != expected_id:
        raise AdjustedPriceError("factor-set receipt does not match the artifact")
    if not minimum_available_at <= recorded_at <= available_at:
        raise AdjustedPriceError("factor-set receipt precedes its content or inputs")
    return AdjustmentFactorSetReceipt(
        factor_set_id=value.factor_set_id,
        factor_set_recorded_at=recorded_at,
        available_at=available_at,
    )


def _validate_cardinality(
    artifact: AdjustmentFactorSet,
    rows: tuple[RawOhlcvVersion, ...],
) -> None:
    if not artifact.raw_inputs:
        raise AdjustedPriceError("factor set raw window must not be empty")
    if len(artifact.raw_inputs) != len(artifact.factors):
        raise AdjustedPriceError("factor set does not contain one factor per raw input")
    if len(rows) != len(artifact.raw_inputs):
        raise AdjustedPriceError("raw_rows must cover the complete factor window")


def _validate_raw_row(
    *,
    ordinal: int,
    row: RawOhlcvVersion,
    expected: RawCloseVersion,
    artifact: AdjustmentFactorSet,
) -> RawOhlcvVersion:
    timestamp = _utc(row.timestamp, "raw timestamp")
    version_recorded_at = _utc(row.version_recorded_at, "raw version_recorded_at")
    available_at = _utc(row.available_at, "raw available_at")
    expected_observed_at = _utc(expected.observed_at, "factor raw observed_at")
    expected_recorded_at = _utc(
        expected.version_recorded_at,
        "factor raw version_recorded_at",
    )
    expected_available_at = _utc(expected.available_at, "factor raw available_at")
    if (
        row.symbol != artifact.symbol
        or timestamp != expected_observed_at
        or timestamp.date() != expected.observation_date
        or row.timespan != expected.timespan
        or row.multiplier != expected.multiplier
        or row.source != expected.source
        or row.adjustment_basis != expected.adjustment_basis
        or version_recorded_at != expected_recorded_at
        or available_at != expected_available_at
    ):
        raise AdjustedPriceError(f"raw receipt identity differs at ordinal {ordinal}")
    if row.timespan != "day" or type(row.multiplier) is not int or row.multiplier != 1:
        raise AdjustedPriceError("adjusted reads require the exact day/1 raw series")
    if row.source != "polygon_open_close" or row.adjustment_basis != "raw":
        raise AdjustedPriceError(
            "adjusted reads require the exact polygon_open_close raw input series"
        )
    if not timestamp <= version_recorded_at <= available_at <= artifact.cutoff:
        raise AdjustedPriceError("raw receipt timestamps are outside the factor cutoff")

    open_value = _finite_nonnegative_float(row.open, "raw open")
    high_value = _finite_nonnegative_float(row.high, "raw high")
    low_value = _finite_nonnegative_float(row.low, "raw low")
    close_value = _finite_nonnegative_float(row.close, "raw close")
    volume_value = _finite_nonnegative_float(row.volume, "raw volume")
    vwap_value = None if row.vwap is None else _finite_nonnegative_float(row.vwap, "raw vwap")
    trade_count = _trade_count(row.trade_count)
    _validate_ohlc(open_value, high_value, low_value, close_value, "raw")
    if _float_bits(close_value) != _float_bits(
        _decimal_binary64(expected.close, "factor raw close")
    ):
        raise AdjustedPriceError(f"raw close differs from the factor input at ordinal {ordinal}")
    return RawOhlcvVersion(
        symbol=row.symbol,
        timestamp=timestamp,
        timespan="day",
        multiplier=1,
        source=row.source,
        adjustment_basis="raw",
        version_recorded_at=version_recorded_at,
        available_at=available_at,
        open=open_value,
        high=high_value,
        low=low_value,
        close=close_value,
        volume=volume_value,
        vwap=vwap_value,
        trade_count=trade_count,
    )


def _adjust_row(
    ordinal: int,
    raw: RawOhlcvVersion,
    factor: AdjustmentFactor,
    *,
    derived_available_at: datetime,
) -> AdjustedOhlcvRow:
    price_factor = _decode_positive_factor(factor.price_factor_f64_be, "price factor")
    volume_factor = _decode_positive_factor(factor.volume_factor_f64_be, "volume factor")
    open_value = _multiply(raw.open, price_factor, "adjusted open")
    high_value = _multiply(raw.high, price_factor, "adjusted high")
    low_value = _multiply(raw.low, price_factor, "adjusted low")
    close_value = _multiply(raw.close, price_factor, "adjusted close")
    volume_value = _multiply(raw.volume, volume_factor, "adjusted volume")
    vwap_value = None if raw.vwap is None else _multiply(raw.vwap, price_factor, "adjusted vwap")
    _validate_ohlc(open_value, high_value, low_value, close_value, "adjusted")
    return AdjustedOhlcvRow(
        raw_input_ordinal=ordinal,
        symbol=raw.symbol,
        timestamp=raw.timestamp,
        timespan=raw.timespan,
        multiplier=raw.multiplier,
        source=raw.source,
        adjustment_basis=ADJUSTED_PRICE_BASIS,
        raw_version_recorded_at=raw.version_recorded_at,
        raw_available_at=raw.available_at,
        available_at=derived_available_at,
        open=open_value,
        high=high_value,
        low=low_value,
        close=close_value,
        volume=volume_value,
        vwap=vwap_value,
        trade_count=raw.trade_count,
        price_factor_f64_be=factor.price_factor_f64_be,
        volume_factor_f64_be=factor.volume_factor_f64_be,
    )


def _decode_positive_factor(bits: object, label: str) -> float:
    if not isinstance(bits, str) or _BINARY64_HEX.fullmatch(bits) is None:
        raise AdjustedPriceError(f"published {label} is not canonical binary64 hex")
    value = struct.unpack(">d", bytes.fromhex(bits))[0]
    if not math.isfinite(value) or value <= 0.0:
        raise AdjustedPriceError(f"published {label} must be finite and positive")
    return value


def _finite_nonnegative_float(value: object, label: str) -> float:
    if type(value) is not float:
        raise AdjustedPriceError(f"{label} must be an exact binary64 float")
    if not math.isfinite(value) or value < 0.0:
        raise AdjustedPriceError(f"{label} must be finite and nonnegative")
    return value


def _trade_count(value: object) -> int | None:
    if value is None:
        return None
    if type(value) is not int or value < 0:
        raise AdjustedPriceError("raw trade_count must be a nonnegative integer or null")
    return value


def _validate_ohlc(open_value: float, high: float, low: float, close: float, label: str) -> None:
    if high < low or high < max(open_value, close) or low > min(open_value, close):
        raise AdjustedPriceError(f"{label} OHLC values are inconsistent")


def _multiply(value: float, factor: float, label: str) -> float:
    result = value * factor
    if not math.isfinite(result) or result < 0.0 or (value > 0.0 and result == 0.0):
        raise AdjustedPriceError(f"{label} cannot be represented as finite binary64")
    return result


def _decimal_binary64(value: object, label: str) -> float:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise AdjustedPriceError(f"{label} must be a finite Decimal")
    try:
        converted = float(value)
    except (OverflowError, ValueError) as exc:
        raise AdjustedPriceError(f"{label} cannot be represented as binary64") from exc
    if not math.isfinite(converted) or converted <= 0.0:
        raise AdjustedPriceError(f"{label} must be positive finite binary64")
    return converted


def _float_bits(value: float) -> str:
    return struct.pack(">d", value).hex()


def _slice_bounds(start: object, stop: object, count: int) -> tuple[int, int]:
    if type(start) is not int or start < 0:
        raise AdjustedPriceError("start_ordinal must be a nonnegative integer")
    normalized_stop = count if stop is None else stop
    if type(normalized_stop) is not int or normalized_stop < start or normalized_stop > count:
        raise AdjustedPriceError("stop_ordinal must be between start_ordinal and window size")
    if start > count:
        raise AdjustedPriceError("start_ordinal must not exceed the window size")
    return start, normalized_stop


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise AdjustedPriceError(f"{label} must be timezone-aware")
    try:
        return value.astimezone(UTC)
    except (OSError, OverflowError, ValueError) as exc:
        raise AdjustedPriceError(f"{label} cannot be normalized to UTC") from exc


def _timestamp(value: object) -> str:
    utc = _utc(value, "factor timestamp")
    return (
        f"{utc.year:04d}-{utc.month:02d}-{utc.day:02d}T{utc.hour:02d}:"
        f"{utc.minute:02d}:{utc.second:02d}.{utc.microsecond:06d}Z"
    )


__all__ = [
    "ADJUSTED_PRICE_BASIS",
    "AdjustedOhlcvRow",
    "AdjustedPriceError",
    "AdjustedPriceLineage",
    "AdjustedPriceWindow",
    "AdjustmentFactorSetReceipt",
    "CorporateActionCollectionReceipt",
    "RawOhlcvVersion",
    "adjust_ohlcv_window",
]
