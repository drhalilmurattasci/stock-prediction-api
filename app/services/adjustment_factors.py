"""Pure, content-addressed split/dividend adjustment-factor artifacts.

Version one is intentionally narrow.  It consumes an already selected,
contiguous XNYS raw-close series plus two complete corporate-action collection
identities.  It performs no database or provider I/O and does not change the
forecast target: these factors are future feature/read infrastructure for the
existing raw-close target.

The newest raw session is the unit anchor.  An action on session ``u`` applies
strictly to observations before ``u``.  Splits use the exact share ratio.  A
recurring cash dividend uses the gross total-return identity
``P_u / (P_u + D)`` where ``P_u`` is the raw post-action close, making the
adjusted return into ``u`` equal ``(P_u + D) / P_prev - 1``.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import struct
import unicodedata
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import (
    ROUND_HALF_EVEN,
    Decimal,
    DecimalException,
    DivisionByZero,
    InvalidOperation,
    Overflow,
    localcontext,
)
from functools import lru_cache
from importlib.metadata import version as package_version
from typing import Any

import exchange_calendars as xcals
import pandas as pd

from data_sources.base import DividendDistributionType, SplitAdjustmentType

ADJUSTMENT_FACTOR_POLICY_VERSION = "split-dividend-gross-total-return-v1"
ADJUSTMENT_FACTOR_SET_FORMAT = "stockapi-adjustment-factor-set-v1"
DECIMAL_PRECISION = 34
DECIMAL_ROUNDING = "ROUND_HALF_EVEN"

_CALENDAR_NAME = "XNYS"
_CALENDAR_ENGINE_VERSION = "4.13.2"
_CALENDAR_START = "1990-01-01"
_CALENDAR_END = "2100-12-31"
_PANDAS_VERSION = package_version("pandas")
_TZDATA_VERSION = package_version("tzdata")
_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9.\-_:]+$")
_CONTENT_ID_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_HASH_PREFIX = "sha256:"
_MAX_IDENTIFIER_LENGTH = 256
_MAX_RAW_INPUTS = 5_000


class AdjustmentFactorError(ValueError):
    """Inputs cannot produce one unambiguous v1 factor artifact."""


@dataclass(frozen=True, slots=True)
class RawCloseVersion:
    """One exact, receipted raw daily-close version selected upstream."""

    observation_date: date
    observed_at: datetime
    timespan: str
    multiplier: int
    source: str
    adjustment_basis: str
    version_recorded_at: datetime
    available_at: datetime
    close: Decimal


@dataclass(frozen=True, slots=True)
class SplitActionVersion:
    """One exact active split version from a complete collection."""

    provider_event_id: str
    version_id: str
    effective_date: date
    split_from: Decimal
    split_to: Decimal
    adjustment_type: SplitAdjustmentType


@dataclass(frozen=True, slots=True)
class DividendActionVersion:
    """One exact active recurring-cash-dividend version."""

    provider_event_id: str
    version_id: str
    ex_dividend_date: date
    cash_amount: Decimal
    currency: str | None
    distribution_type: DividendDistributionType


@dataclass(frozen=True, slots=True)
class AdjustmentFactor:
    """Canonical factors for one raw observation.

    Decimal strings are the policy calculation result.  The big-endian
    binary64 encodings are the exact values a float-based consumer must use;
    consumers must not independently parse/round the Decimal strings under an
    ambient numeric context.
    """

    raw_input_ordinal: int
    price_factor_decimal: str
    volume_factor_decimal: str
    price_factor_f64_be: str
    volume_factor_f64_be: str


@dataclass(frozen=True, slots=True)
class AdjustmentFactorSet:
    """Immutable, content-addressed output of the v1 adjustment policy."""

    factor_set_id: str
    canonical_payload: bytes
    format: str
    policy_version: str
    policy_hash: str
    symbol: str
    cutoff: datetime
    anchor_date: date
    split_collection_id: str
    dividend_collection_id: str
    raw_inputs: tuple[RawCloseVersion, ...]
    action_version_ids: tuple[str, ...]
    factors: tuple[AdjustmentFactor, ...]


_POLICY_DOCUMENT: dict[str, object] = {
    "arithmetic": {
        "calculation": "decimal",
        "evaluation_order": [
            "round_each_input",
            "round_addition",
            "round_cumulative_factor_times_numerator",
            "round_product_divided_by_denominator",
        ],
        "precision": DECIMAL_PRECISION,
        "published_float_conversion": "round_to_nearest_ties_to_even",
        "rounding": DECIMAL_ROUNDING,
        "published_float": "ieee754-binary64-big-endian-hex",
    },
    "calendar": {
        "engine": "exchange_calendars",
        "engine_version": _CALENDAR_ENGINE_VERSION,
        "name": _CALENDAR_NAME,
        "pandas_version": _PANDAS_VERSION,
        "schedule_end": _CALENDAR_END,
        "schedule_start": _CALENDAR_START,
        "tzdata_version": _TZDATA_VERSION,
    },
    "action_date": "must_equal_an_exact_raw_xnys_session",
    "cardinality": "at_most_one_total_action_per_session",
    "date_application": "observation_date<action_date<=anchor_date",
    "dividend": {
        "allowed_distribution_types": ["recurring"],
        "cash": "strictly_positive_usd",
        "price_factor_step": "post_action_raw_close/(post_action_raw_close+cash)",
        "return_identity": "(post_action_raw_close+cash)/previous_raw_close-1",
        "volume_factor_step": "1",
    },
    "format": "stockapi-adjustment-factor-policy-v1",
    "raw_series": {
        "anchor": "newest_raw_xnys_session_factor_one",
        "close": {
            "calculation_value": "decimal34_of_binary64_shortest_round_trip_decimal",
            "published_decimal": "canonical_positive_decimal",
            "published_float": "ieee754-binary64-big-endian-hex",
        },
        "receipt": {
            "availability_order": (
                "observed_at<=version_recorded_at<=available_at<=factor_set_cutoff"
            ),
            "identity_fields": [
                "symbol",
                "timespan",
                "multiplier",
                "observed_at",
                "source",
                "adjustment_basis",
                "version_recorded_at",
                "available_at",
            ],
            "ordering": "observation_date_ascending_with_unique_exact_receipts",
        },
        "sessions": "exact_contiguous_xnys_sessions",
        "source_contract": {
            "adjustment_basis": "raw",
            "multiplier": 1,
            "observed_at": "exact_xnys_regular_session_close_utc",
            "source": "polygon_open_close",
            "timespan": "day",
        },
        "target": "raw_close_unchanged",
    },
    "split": {
        "allowed_adjustment_types": ["forward_split", "reverse_split"],
        "price_factor_step": "split_from/split_to",
        "volume_factor_step": "split_to/split_from",
    },
    "version": ADJUSTMENT_FACTOR_POLICY_VERSION,
}


def _canonical_json(document: object) -> bytes:
    return json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


_POLICY_CANONICAL = _canonical_json(_POLICY_DOCUMENT)
ADJUSTMENT_FACTOR_POLICY_HASH = _HASH_PREFIX + hashlib.sha256(_POLICY_CANONICAL).hexdigest()


def adjustment_factor_policy_document() -> dict[str, object]:
    """Return a detached copy of the exact policy covered by the public hash."""

    value = json.loads(_POLICY_CANONICAL.decode("utf-8"))
    if not isinstance(value, dict):  # pragma: no cover - frozen module invariant
        raise RuntimeError("adjustment-factor policy is not an object")
    return value


def build_adjustment_factor_set(
    *,
    symbol: str,
    cutoff: datetime,
    raw_closes: Sequence[RawCloseVersion],
    split_collection_id: str,
    splits: Sequence[SplitActionVersion],
    dividend_collection_id: str,
    dividends: Sequence[DividendActionVersion],
) -> AdjustmentFactorSet:
    """Build one deterministic factor set without performing external I/O."""

    normalized_symbol = _symbol(symbol)
    normalized_cutoff = _utc(cutoff, "cutoff")
    split_collection = _content_id(split_collection_id, "split_collection_id")
    dividend_collection = _content_id(dividend_collection_id, "dividend_collection_id")
    if split_collection == dividend_collection:
        raise AdjustmentFactorError("corporate-action collection IDs must be distinct")
    raw = _validated_raw_closes(raw_closes, normalized_cutoff)
    normalized_splits = _validated_splits(splits, raw)
    normalized_dividends = _validated_dividends(dividends, raw)
    _validate_action_identity_and_cardinality(normalized_splits, normalized_dividends)

    split_by_date = {item.effective_date: item for item in normalized_splits}
    dividend_by_date = {item.ex_dividend_date: item for item in normalized_dividends}
    price_factor = Decimal(1)
    volume_factor = Decimal(1)
    factors_descending: list[AdjustmentFactor] = []

    for raw_input_ordinal in range(len(raw) - 1, -1, -1):
        row = raw[raw_input_ordinal]
        factors_descending.append(
            _factor_row(
                raw_input_ordinal,
                price_factor=price_factor,
                volume_factor=volume_factor,
            )
        )
        split = split_by_date.get(row.observation_date)
        dividend = dividend_by_date.get(row.observation_date)
        if split is not None:
            price_factor = _mul_div(
                price_factor,
                split.split_from,
                split.split_to,
                "split price factor",
            )
            volume_factor = _mul_div(
                volume_factor,
                split.split_to,
                split.split_from,
                "split volume factor",
            )
        elif dividend is not None:
            denominator = _add(row.close, dividend.cash_amount, "dividend denominator")
            price_factor = _mul_div(
                price_factor,
                row.close,
                denominator,
                "dividend price factor",
            )

    factors = tuple(reversed(factors_descending))
    split_rows = [
        {
            "provider_event_id": item.provider_event_id,
            "adjustment_type": item.adjustment_type,
            "effective_date": item.effective_date.isoformat(),
            "split_from": _decimal_text(item.split_from),
            "split_to": _decimal_text(item.split_to),
            "version_id": item.version_id,
        }
        for item in normalized_splits
    ]
    dividend_rows = [
        {
            "provider_event_id": item.provider_event_id,
            "cash_amount": _decimal_text(item.cash_amount),
            "currency": item.currency,
            "distribution_type": item.distribution_type,
            "ex_dividend_date": item.ex_dividend_date.isoformat(),
            "version_id": item.version_id,
        }
        for item in normalized_dividends
    ]
    payload_document = {
        "actions": {
            "dividends": {
                "collection_id": dividend_collection,
                "versions": dividend_rows,
            },
            "splits": {
                "collection_id": split_collection,
                "versions": split_rows,
            },
        },
        "anchor_date": raw[-1].observation_date.isoformat(),
        "cutoff": _timestamp(normalized_cutoff),
        "factors": [
            {
                "price_factor_decimal": item.price_factor_decimal,
                "price_factor_f64_be": item.price_factor_f64_be,
                "raw_input_ordinal": item.raw_input_ordinal,
                "volume_factor_decimal": item.volume_factor_decimal,
                "volume_factor_f64_be": item.volume_factor_f64_be,
            }
            for item in factors
        ],
        "format": ADJUSTMENT_FACTOR_SET_FORMAT,
        "policy_hash": ADJUSTMENT_FACTOR_POLICY_HASH,
        "policy_version": ADJUSTMENT_FACTOR_POLICY_VERSION,
        "raw_inputs": [
            {
                "adjustment_basis": item.adjustment_basis,
                "available_at": _timestamp(item.available_at),
                "close_decimal": _decimal_text(item.close),
                "close_f64_be": _binary64_bits(item.close, "raw close"),
                "multiplier": item.multiplier,
                "observation_date": item.observation_date.isoformat(),
                "observed_at": _timestamp(item.observed_at),
                "source": item.source,
                "timespan": item.timespan,
                "version_recorded_at": _timestamp(item.version_recorded_at),
            }
            for item in raw
        ],
        "symbol": normalized_symbol,
    }
    canonical_payload = _canonical_json(payload_document)
    factor_set_id = _HASH_PREFIX + hashlib.sha256(canonical_payload).hexdigest()
    action_version_ids = tuple(
        sorted(
            [item.version_id for item in normalized_splits]
            + [item.version_id for item in normalized_dividends]
        )
    )
    return AdjustmentFactorSet(
        factor_set_id=factor_set_id,
        canonical_payload=canonical_payload,
        format=ADJUSTMENT_FACTOR_SET_FORMAT,
        policy_version=ADJUSTMENT_FACTOR_POLICY_VERSION,
        policy_hash=ADJUSTMENT_FACTOR_POLICY_HASH,
        symbol=normalized_symbol,
        cutoff=normalized_cutoff,
        anchor_date=raw[-1].observation_date,
        split_collection_id=split_collection,
        dividend_collection_id=dividend_collection,
        raw_inputs=raw,
        action_version_ids=action_version_ids,
        factors=factors,
    )


def _validated_raw_closes(
    values: Sequence[RawCloseVersion],
    cutoff: datetime,
) -> tuple[RawCloseVersion, ...]:
    try:
        supplied = tuple(values)
    except TypeError as exc:
        raise AdjustmentFactorError("raw_closes must be a finite sequence") from exc
    if not supplied:
        raise AdjustmentFactorError("raw_closes must not be empty")
    if len(supplied) > _MAX_RAW_INPUTS:
        raise AdjustmentFactorError("raw_closes exceed the 5000-row factor-set limit")
    if any(not isinstance(item, RawCloseVersion) for item in supplied):
        raise AdjustmentFactorError("raw_closes contain an unsupported value")

    normalized_rows: list[RawCloseVersion] = []
    for item in supplied:
        observation_date = _date(item.observation_date, "raw observation_date")
        observed_at = _utc(item.observed_at, "raw observed_at")
        version_recorded_at = _utc(
            item.version_recorded_at,
            "raw version_recorded_at",
        )
        available_at = _utc(item.available_at, "raw available_at")
        if item.timespan != "day":
            raise AdjustmentFactorError("raw timespan must be exactly day")
        if type(item.multiplier) is not int or item.multiplier != 1:
            raise AdjustmentFactorError("raw multiplier must be exactly one")
        if item.source != "polygon_open_close":
            raise AdjustmentFactorError("raw source must be exactly polygon_open_close")
        if item.adjustment_basis != "raw":
            raise AdjustmentFactorError("raw adjustment_basis must be exactly raw")
        if observed_at.date() != observation_date:
            raise AdjustmentFactorError("raw observed_at date must equal observation_date")
        if not observed_at <= version_recorded_at <= available_at <= cutoff:
            raise AdjustmentFactorError(
                "raw receipt timestamps must satisfy "
                "observed_at <= version_recorded_at <= available_at <= cutoff"
            )
        normalized_rows.append(
            RawCloseVersion(
                observation_date=observation_date,
                observed_at=observed_at,
                timespan="day",
                multiplier=1,
                source="polygon_open_close",
                adjustment_basis="raw",
                version_recorded_at=version_recorded_at,
                available_at=available_at,
                close=_canonical_raw_close(item.close),
            )
        )
    normalized = tuple(sorted(normalized_rows, key=lambda item: item.observation_date))
    dates = [item.observation_date for item in normalized]
    if len(set(dates)) != len(dates):
        raise AdjustmentFactorError("raw observation dates must be unique")
    _validate_contiguous_xnys(dates)

    calendar = _calendar()
    receipt_keys = [
        (
            item.timespan,
            item.multiplier,
            item.observed_at,
            item.source,
            item.adjustment_basis,
            item.version_recorded_at,
            item.available_at,
        )
        for item in normalized
    ]
    if len(set(receipt_keys)) != len(receipt_keys):
        raise AdjustmentFactorError("raw exact receipt identities must be unique")
    if any(
        left.observed_at >= right.observed_at
        for left, right in zip(normalized, normalized[1:], strict=False)
    ):
        raise AdjustmentFactorError("raw observations must be strictly chronological")
    for item in normalized:
        expected_close = _utc(
            calendar.session_close(pd.Timestamp(item.observation_date)).to_pydatetime(),
            "raw session close",
        )
        if item.observed_at != expected_close:
            raise AdjustmentFactorError("raw observed_at must be the exact XNYS session close")
    anchor_label = pd.Timestamp(normalized[-1].observation_date)
    anchor_close = _utc(
        calendar.session_close(anchor_label).to_pydatetime(),
        "anchor session close",
    )
    if cutoff < anchor_close:
        raise AdjustmentFactorError("cutoff must not precede the anchor session close")
    return normalized


def _validated_splits(
    values: Sequence[SplitActionVersion],
    raw: tuple[RawCloseVersion, ...],
) -> tuple[SplitActionVersion, ...]:
    try:
        supplied = tuple(values)
    except TypeError as exc:
        raise AdjustmentFactorError("splits must be a finite sequence") from exc
    if any(not isinstance(item, SplitActionVersion) for item in supplied):
        raise AdjustmentFactorError("splits contain an unsupported value")
    allowed_types = {"forward_split", "reverse_split"}
    normalized: list[SplitActionVersion] = []
    for item in supplied:
        effective_date = _action_date(
            item.effective_date,
            "split effective_date",
            raw,
        )
        if item.adjustment_type not in allowed_types:
            raise AdjustmentFactorError("split adjustment_type is outside the v1 allowlist")
        normalized.append(
            SplitActionVersion(
                provider_event_id=_identifier(
                    item.provider_event_id,
                    "split provider_event_id",
                ),
                version_id=_content_id(item.version_id, "split version_id"),
                effective_date=effective_date,
                split_from=_positive_decimal(item.split_from, "split_from"),
                split_to=_positive_decimal(item.split_to, "split_to"),
                adjustment_type=item.adjustment_type,
            )
        )
    return tuple(sorted(normalized, key=lambda item: (item.effective_date, item.provider_event_id)))


def _validated_dividends(
    values: Sequence[DividendActionVersion],
    raw: tuple[RawCloseVersion, ...],
) -> tuple[DividendActionVersion, ...]:
    try:
        supplied = tuple(values)
    except TypeError as exc:
        raise AdjustmentFactorError("dividends must be a finite sequence") from exc
    if any(not isinstance(item, DividendActionVersion) for item in supplied):
        raise AdjustmentFactorError("dividends contain an unsupported value")
    normalized: list[DividendActionVersion] = []
    for item in supplied:
        ex_date = _action_date(item.ex_dividend_date, "dividend ex_dividend_date", raw)
        if item.currency != "USD":
            raise AdjustmentFactorError("dividend currency must be exactly USD")
        if item.distribution_type != "recurring":
            raise AdjustmentFactorError("dividend distribution_type is outside the v1 allowlist")
        normalized.append(
            DividendActionVersion(
                provider_event_id=_identifier(
                    item.provider_event_id,
                    "dividend provider_event_id",
                ),
                version_id=_content_id(item.version_id, "dividend version_id"),
                ex_dividend_date=ex_date,
                cash_amount=_positive_decimal(item.cash_amount, "dividend cash_amount"),
                currency="USD",
                distribution_type="recurring",
            )
        )
    return tuple(
        sorted(normalized, key=lambda item: (item.ex_dividend_date, item.provider_event_id))
    )


def _validate_action_identity_and_cardinality(
    splits: tuple[SplitActionVersion, ...],
    dividends: tuple[DividendActionVersion, ...],
) -> None:
    action_ids = [item.provider_event_id for item in splits] + [
        item.provider_event_id for item in dividends
    ]
    if len(set(action_ids)) != len(action_ids):
        raise AdjustmentFactorError("corporate-action IDs must be unique")
    version_ids = [item.version_id for item in splits] + [item.version_id for item in dividends]
    if len(set(version_ids)) != len(version_ids):
        raise AdjustmentFactorError("corporate-action version IDs must be unique")
    action_dates = [item.effective_date for item in splits] + [
        item.ex_dividend_date for item in dividends
    ]
    if len(set(action_dates)) != len(action_dates):
        raise AdjustmentFactorError("v1 permits at most one split or dividend on a session")


def _action_date(
    value: date | None,
    label: str,
    raw: tuple[RawCloseVersion, ...],
) -> date:
    if value is None:
        raise AdjustmentFactorError(f"{label} must be known")
    normalized = _date(value, label)
    start = raw[0].observation_date
    anchor = raw[-1].observation_date
    if normalized < start or normalized > anchor:
        raise AdjustmentFactorError(f"{label} is outside the raw date span")
    if normalized not in {item.observation_date for item in raw}:
        raise AdjustmentFactorError(f"{label} must be an exact raw XNYS session")
    return normalized


def _validate_contiguous_xnys(dates: list[date]) -> None:
    calendar = _calendar()
    try:
        expected = tuple(
            label.date()
            for label in calendar.sessions_in_range(
                pd.Timestamp(dates[0]),
                pd.Timestamp(dates[-1]),
            )
        )
    except (KeyError, OverflowError, ValueError) as exc:
        raise AdjustmentFactorError("raw dates are outside the pinned XNYS schedule") from exc
    if tuple(dates) != expected:
        raise AdjustmentFactorError("raw dates must be exact contiguous XNYS sessions")


@lru_cache(maxsize=1)
def _calendar() -> Any:
    if xcals.__version__ != _CALENDAR_ENGINE_VERSION:
        raise AdjustmentFactorError("exchange_calendars version differs from the adjustment policy")
    try:
        return xcals.get_calendar(
            _CALENDAR_NAME,
            start=_CALENDAR_START,
            end=_CALENDAR_END,
        )
    except (KeyError, ValueError) as exc:
        raise AdjustmentFactorError("the pinned XNYS calendar is unavailable") from exc


def _factor_row(
    raw_input_ordinal: int,
    *,
    price_factor: Decimal,
    volume_factor: Decimal,
) -> AdjustmentFactor:
    price_text = _decimal_text(price_factor)
    volume_text = _decimal_text(volume_factor)
    return AdjustmentFactor(
        raw_input_ordinal=raw_input_ordinal,
        price_factor_decimal=price_text,
        volume_factor_decimal=volume_text,
        price_factor_f64_be=_binary64_bits(price_factor, "price factor"),
        volume_factor_f64_be=_binary64_bits(volume_factor, "volume factor"),
    )


def _canonical_raw_close(value: Decimal) -> Decimal:
    """Normalize one DB float8 close to its unique arithmetic Decimal."""

    supplied = _positive_decimal(value, "raw close")
    try:
        converted = float(supplied)
    except (OverflowError, ValueError) as exc:
        raise AdjustmentFactorError("raw close cannot be represented as binary64") from exc
    if not math.isfinite(converted) or converted <= 0.0:
        raise AdjustmentFactorError("raw close cannot be represented as positive binary64")
    return _positive_decimal(Decimal(str(converted)), "raw close")


def _mul_div(left: Decimal, numerator: Decimal, denominator: Decimal, label: str) -> Decimal:
    try:
        with localcontext() as context:
            context.prec = DECIMAL_PRECISION
            context.rounding = ROUND_HALF_EVEN
            context.traps[DivisionByZero] = True
            context.traps[InvalidOperation] = True
            context.traps[Overflow] = True
            result = +(left * numerator / denominator)
    except DecimalException as exc:
        raise AdjustmentFactorError(f"{label} cannot be represented") from exc
    if not result.is_finite() or result <= 0:
        raise AdjustmentFactorError(f"{label} must remain finite and positive")
    return result


def _add(left: Decimal, right: Decimal, label: str) -> Decimal:
    try:
        with localcontext() as context:
            context.prec = DECIMAL_PRECISION
            context.rounding = ROUND_HALF_EVEN
            context.traps[InvalidOperation] = True
            context.traps[Overflow] = True
            result = +(left + right)
    except DecimalException as exc:
        raise AdjustmentFactorError(f"{label} cannot be represented") from exc
    if not result.is_finite() or result <= 0:
        raise AdjustmentFactorError(f"{label} must remain finite and positive")
    return result


def _positive_decimal(value: Decimal, label: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise AdjustmentFactorError(f"{label} must be a Decimal")
    if not value.is_finite() or value <= 0:
        raise AdjustmentFactorError(f"{label} must be finite and positive")
    try:
        with localcontext() as context:
            context.prec = DECIMAL_PRECISION
            context.rounding = ROUND_HALF_EVEN
            context.traps[InvalidOperation] = True
            context.traps[Overflow] = True
            normalized = +value
    except DecimalException as exc:
        raise AdjustmentFactorError(f"{label} cannot be represented") from exc
    if not normalized.is_finite() or normalized <= 0:
        raise AdjustmentFactorError(f"{label} must be finite and positive")
    return normalized


def _decimal_text(value: Decimal) -> str:
    if value == 0:
        return "0"
    try:
        with localcontext() as context:
            context.prec = DECIMAL_PRECISION
            context.rounding = ROUND_HALF_EVEN
            normalized = value.normalize(context=context)
    except DecimalException as exc:  # pragma: no cover - callers already validate
        raise AdjustmentFactorError("Decimal value cannot be canonicalized") from exc
    rendered = format(normalized, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return "0" if rendered == "-0" else rendered


def _binary64_bits(value: Decimal, label: str) -> str:
    try:
        converted = float(value)
    except (OverflowError, ValueError) as exc:
        raise AdjustmentFactorError(f"{label} cannot be represented as binary64") from exc
    if not math.isfinite(converted) or converted <= 0.0:
        raise AdjustmentFactorError(f"{label} cannot be represented as positive binary64")
    return struct.pack(">d", converted).hex()


def _identifier(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise AdjustmentFactorError(f"{label} must be text")
    normalized = unicodedata.normalize("NFC", value)
    if (
        value != normalized
        or not value
        or value.strip() != value
        or len(value) > _MAX_IDENTIFIER_LENGTH
        or any(0xD800 <= ord(character) <= 0xDFFF for character in value)
    ):
        raise AdjustmentFactorError(f"{label} must be an exact bounded opaque identifier")
    return value


def _content_id(value: object, label: str) -> str:
    identifier = _identifier(value, label)
    if _CONTENT_ID_PATTERN.fullmatch(identifier) is None:
        raise AdjustmentFactorError(f"{label} must be a SHA-256 content address")
    return identifier


def _symbol(value: object) -> str:
    if (
        not isinstance(value, str)
        or value != value.strip().upper()
        or _SYMBOL_PATTERN.fullmatch(value) is None
    ):
        raise AdjustmentFactorError("symbol must be uppercase and canonical")
    return value


def _date(value: object, label: str) -> date:
    if type(value) is not date:
        raise AdjustmentFactorError(f"{label} must be a date")
    return value


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise AdjustmentFactorError(f"{label} must be timezone-aware")
    try:
        return value.astimezone(UTC)
    except (OSError, OverflowError, ValueError) as exc:
        raise AdjustmentFactorError(f"{label} cannot be normalized to UTC") from exc


def _timestamp(value: datetime) -> str:
    utc = _utc(value, "timestamp")
    return (
        f"{utc.year:04d}-{utc.month:02d}-{utc.day:02d}T{utc.hour:02d}:"
        f"{utc.minute:02d}:{utc.second:02d}.{utc.microsecond:06d}Z"
    )


__all__ = [
    "ADJUSTMENT_FACTOR_POLICY_HASH",
    "ADJUSTMENT_FACTOR_POLICY_VERSION",
    "ADJUSTMENT_FACTOR_SET_FORMAT",
    "AdjustmentFactor",
    "AdjustmentFactorError",
    "AdjustmentFactorSet",
    "DividendActionVersion",
    "RawCloseVersion",
    "SplitActionVersion",
    "adjustment_factor_policy_document",
    "build_adjustment_factor_set",
]
