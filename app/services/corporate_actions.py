"""Canonical corporate-action versions and complete bounded collections."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, localcontext
from typing import Literal

from data_sources.base import Dividend, DividendPage, Split, SplitPage

CORPORATE_ACTION_SCHEMA_VERSION = 1
CORPORATE_ACTION_PAGE_LIMIT = 5_000
CORPORATE_ACTION_ORIGIN = "https://api.massive.com"
CORPORATE_ACTION_SOURCE = "polygon"
SPLITS_ENDPOINT = "/stocks/v1/splits"
DIVIDENDS_ENDPOINT = "/stocks/v1/dividends"

_HASH_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9.\-_:]+$")
_PROVIDER_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:\-]+$")


class CorporateActionValidationError(ValueError):
    """Corporate-action content cannot satisfy the pinned evidence policy."""


CORPORATE_ACTION_QUERY_POLICY_DOCUMENT: dict[str, object] = {
    "canonical_evidence": {
        "encoding": "utf8-json-sort-keys-compact-no-nan",
        "provider_identifier_pattern": "^[A-Za-z0-9._:\\-]{1,128}$",
        "storage_projection": "database-derived-from-exact-canonical-bytes",
    },
    "format": "corporate-action-query-policy-v1",
    "origin": CORPORATE_ACTION_ORIGIN,
    "source": CORPORATE_ACTION_SOURCE,
    "page": {
        "count": 1,
        "limit": CORPORATE_ACTION_PAGE_LIMIT,
        "next_url": "must_be_absent",
        "retries": 0,
        "status": "OK",
    },
    "queries": {
        "dividend": {
            "date_field": "ex_dividend_date",
            "endpoint": DIVIDENDS_ENDPOINT,
            "sort": "ex_dividend_date.asc",
        },
        "split": {
            "date_field": "execution_date",
            "endpoint": SPLITS_ENDPOINT,
            "sort": "execution_date.asc",
        },
    },
    "scope": "one_uppercase_symbol_and_inclusive_date_window",
    "version_selection": {
        "eligibility": "post_commit_receipt_available_at_lte_consumer_cutoff",
        "newest_order": [
            "collection_recorded_at_desc",
            "collection_id_desc",
        ],
    },
}


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _content_id(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


CORPORATE_ACTION_QUERY_POLICY_HASH = _content_id(
    _canonical_bytes(CORPORATE_ACTION_QUERY_POLICY_DOCUMENT)
)


@dataclass(frozen=True, slots=True)
class CorporateActionVersionRecord:
    """Normalized columns and canonical bytes for one immutable event version."""

    action_version_id: str
    canonical_event: bytes
    source: str
    action_type: Literal["split", "dividend"]
    provider_event_id: str
    symbol: str
    effective_date: date
    status: Literal["active"] = "active"
    split_from: Decimal | None = None
    split_to: Decimal | None = None
    adjustment_type: str | None = None
    cash_amount: Decimal | None = None
    split_adjusted_cash_amount: Decimal | None = None
    currency: str | None = None
    declaration_date: date | None = None
    record_date: date | None = None
    pay_date: date | None = None
    frequency: int | None = None
    distribution_type: str | None = None
    historical_adjustment_factor: Decimal | None = None
    schema_version: int = CORPORATE_ACTION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not _HASH_PATTERN.fullmatch(self.action_version_id):
            raise CorporateActionValidationError("action_version_id is malformed")
        if _content_id(self.canonical_event) != self.action_version_id:
            raise CorporateActionValidationError("action_version_id does not match its bytes")
        if self.schema_version != CORPORATE_ACTION_SCHEMA_VERSION:
            raise CorporateActionValidationError("action schema_version is not supported")


@dataclass(frozen=True, slots=True)
class CorporateActionCollectionRecord:
    """One complete one-page collection ready for two-phase persistence."""

    collection_id: str
    canonical_manifest: bytes
    action_type: Literal["split", "dividend"]
    endpoint: str
    symbol: str
    coverage_start: date
    coverage_end: date
    provider_request_id: str
    fetched_at: datetime
    members: tuple[CorporateActionVersionRecord, ...]
    source: str = CORPORATE_ACTION_SOURCE
    query_policy_hash: str = CORPORATE_ACTION_QUERY_POLICY_HASH
    page_limit: int = CORPORATE_ACTION_PAGE_LIMIT
    page_count: int = 1
    pagination_exhausted: bool = True
    schema_version: int = CORPORATE_ACTION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not _HASH_PATTERN.fullmatch(self.collection_id):
            raise CorporateActionValidationError("collection_id is malformed")
        if _content_id(self.canonical_manifest) != self.collection_id:
            raise CorporateActionValidationError("collection_id does not match its bytes")
        if self.query_policy_hash != CORPORATE_ACTION_QUERY_POLICY_HASH:
            raise CorporateActionValidationError("collection query policy is not supported")
        if self.schema_version != CORPORATE_ACTION_SCHEMA_VERSION:
            raise CorporateActionValidationError("collection schema_version is not supported")
        if self.coverage_start > self.coverage_end:
            raise CorporateActionValidationError("collection coverage is inverted")
        if self.fetched_at.tzinfo is None or self.fetched_at.utcoffset() is None:
            raise CorporateActionValidationError("collection fetched_at must be timezone-aware")
        if len(self.members) > CORPORATE_ACTION_PAGE_LIMIT:
            raise CorporateActionValidationError("collection exceeds the page limit")
        logical_ids = {
            (member.source, member.action_type, member.provider_event_id) for member in self.members
        }
        if len(logical_ids) != len(self.members):
            raise CorporateActionValidationError("collection repeats a logical provider event")
        for member in self.members:
            if (
                member.source != self.source
                or member.symbol != self.symbol
                or member.action_type != self.action_type
                or not self.coverage_start <= member.effective_date <= self.coverage_end
            ):
                raise CorporateActionValidationError(
                    "collection member escaped its source, symbol, type, or date scope"
                )

    @property
    def event_count(self) -> int:
        return len(self.members)


def build_split_collection(page: SplitPage) -> CorporateActionCollectionRecord:
    """Canonicalize one strictly complete split page."""

    _validate_page(
        page_origin=page.provider_origin,
        endpoint=page.endpoint,
        expected_endpoint=SPLITS_ENDPOINT,
        source=page.source,
        symbol=page.symbol,
        start=page.start,
        end=page.end,
        request_id=page.provider_request_id,
        fetched_at=page.fetched_at,
    )
    members = tuple(
        _split_record(value)
        for value in sorted(
            page.results,
            key=lambda item: (item.execution_date, item.provider_event_id),
        )
    )
    return _collection(
        action_type="split",
        endpoint=SPLITS_ENDPOINT,
        symbol=page.symbol,
        start=page.start,
        end=page.end,
        request_id=page.provider_request_id,
        fetched_at=page.fetched_at,
        members=members,
    )


def build_dividend_collection(page: DividendPage) -> CorporateActionCollectionRecord:
    """Canonicalize one strictly complete dividend page."""

    _validate_page(
        page_origin=page.provider_origin,
        endpoint=page.endpoint,
        expected_endpoint=DIVIDENDS_ENDPOINT,
        source=page.source,
        symbol=page.symbol,
        start=page.start,
        end=page.end,
        request_id=page.provider_request_id,
        fetched_at=page.fetched_at,
    )
    members = tuple(
        _dividend_record(value)
        for value in sorted(
            page.results,
            key=lambda item: (item.ex_dividend_date, item.provider_event_id),
        )
    )
    return _collection(
        action_type="dividend",
        endpoint=DIVIDENDS_ENDPOINT,
        symbol=page.symbol,
        start=page.start,
        end=page.end,
        request_id=page.provider_request_id,
        fetched_at=page.fetched_at,
        members=members,
    )


def _split_record(value: Split) -> CorporateActionVersionRecord:
    provider_event_id = _provider_id(value.provider_event_id, "provider_event_id")
    source = _source(value.source)
    symbol = _symbol(value.symbol)
    document = {
        "action_type": "split",
        "adjustment_type": value.adjustment_type,
        "effective_date": value.execution_date.isoformat(),
        "historical_adjustment_factor": _decimal(value.historical_adjustment_factor),
        "provider_event_id": provider_event_id,
        "schema_version": CORPORATE_ACTION_SCHEMA_VERSION,
        "source": source,
        "split_from": _decimal(value.split_from),
        "split_to": _decimal(value.split_to),
        "status": "active",
        "symbol": symbol,
    }
    payload = _canonical_bytes(document)
    return CorporateActionVersionRecord(
        action_version_id=_content_id(payload),
        canonical_event=payload,
        source=source,
        action_type="split",
        provider_event_id=provider_event_id,
        symbol=symbol,
        effective_date=value.execution_date,
        split_from=value.split_from,
        split_to=value.split_to,
        adjustment_type=value.adjustment_type,
        historical_adjustment_factor=value.historical_adjustment_factor,
    )


def _dividend_record(value: Dividend) -> CorporateActionVersionRecord:
    currency = _text(value.currency, "currency", 3).upper()
    if len(currency) != 3:
        raise CorporateActionValidationError("currency must have exactly three characters")
    provider_event_id = _provider_id(value.provider_event_id, "provider_event_id")
    source = _source(value.source)
    symbol = _symbol(value.symbol)
    document = {
        "action_type": "dividend",
        "cash_amount": _decimal(value.cash_amount),
        "currency": currency,
        "declaration_date": _date_text(value.declaration_date),
        "distribution_type": value.distribution_type,
        "effective_date": value.ex_dividend_date.isoformat(),
        "frequency": value.frequency,
        "historical_adjustment_factor": _decimal(value.historical_adjustment_factor),
        "pay_date": _date_text(value.pay_date),
        "provider_event_id": provider_event_id,
        "record_date": _date_text(value.record_date),
        "schema_version": CORPORATE_ACTION_SCHEMA_VERSION,
        "source": source,
        "split_adjusted_cash_amount": _decimal(value.split_adjusted_cash_amount),
        "status": "active",
        "symbol": symbol,
    }
    payload = _canonical_bytes(document)
    return CorporateActionVersionRecord(
        action_version_id=_content_id(payload),
        canonical_event=payload,
        source=source,
        action_type="dividend",
        provider_event_id=provider_event_id,
        symbol=symbol,
        effective_date=value.ex_dividend_date,
        cash_amount=value.cash_amount,
        split_adjusted_cash_amount=value.split_adjusted_cash_amount,
        currency=currency,
        declaration_date=value.declaration_date,
        record_date=value.record_date,
        pay_date=value.pay_date,
        frequency=value.frequency,
        distribution_type=value.distribution_type,
        historical_adjustment_factor=value.historical_adjustment_factor,
    )


def _collection(
    *,
    action_type: Literal["split", "dividend"],
    endpoint: str,
    symbol: str,
    start: date,
    end: date,
    request_id: str,
    fetched_at: datetime,
    members: tuple[CorporateActionVersionRecord, ...],
) -> CorporateActionCollectionRecord:
    normalized_time = fetched_at.astimezone(UTC)
    document = {
        "action_type": action_type,
        "coverage": {"end": end.isoformat(), "start": start.isoformat()},
        "endpoint": endpoint,
        "event_count": len(members),
        "event_version_ids": [member.action_version_id for member in members],
        "format": "corporate-action-complete-collection-v1",
        "page": {
            "count": 1,
            "limit": CORPORATE_ACTION_PAGE_LIMIT,
            "pagination_exhausted": True,
        },
        "provider_origin": CORPORATE_ACTION_ORIGIN,
        "provider_request_id": request_id,
        "query_policy_hash": CORPORATE_ACTION_QUERY_POLICY_HASH,
        "response_completed_at": _timestamp(normalized_time),
        "schema_version": CORPORATE_ACTION_SCHEMA_VERSION,
        "source": CORPORATE_ACTION_SOURCE,
        "symbol": symbol,
    }
    payload = _canonical_bytes(document)
    return CorporateActionCollectionRecord(
        collection_id=_content_id(payload),
        canonical_manifest=payload,
        action_type=action_type,
        endpoint=endpoint,
        symbol=symbol,
        coverage_start=start,
        coverage_end=end,
        provider_request_id=request_id,
        fetched_at=normalized_time,
        members=members,
    )


def _validate_page(
    *,
    page_origin: str,
    endpoint: str,
    expected_endpoint: str,
    source: str,
    symbol: str,
    start: date,
    end: date,
    request_id: str,
    fetched_at: datetime,
) -> None:
    if page_origin != CORPORATE_ACTION_ORIGIN:
        raise CorporateActionValidationError("corporate-action origin is not supported")
    if endpoint != expected_endpoint:
        raise CorporateActionValidationError("corporate-action endpoint is not supported")
    if _source(source) != CORPORATE_ACTION_SOURCE:
        raise CorporateActionValidationError("corporate-action source is not supported")
    if _symbol(symbol) != symbol:
        raise CorporateActionValidationError("corporate-action symbol is not canonical")
    if start > end:
        raise CorporateActionValidationError("corporate-action date window is inverted")
    _provider_id(request_id, "provider_request_id")
    if fetched_at.tzinfo is None or fetched_at.utcoffset() is None:
        raise CorporateActionValidationError("corporate-action fetched_at must be timezone-aware")


def _source(value: str) -> str:
    normalized = _text(value, "source", 64).lower()
    if normalized != CORPORATE_ACTION_SOURCE:
        raise CorporateActionValidationError("corporate-action source is not supported")
    return normalized


def _symbol(value: str) -> str:
    normalized = _text(value, "symbol", 32).upper()
    if normalized != value or _SYMBOL_PATTERN.fullmatch(normalized) is None:
        raise CorporateActionValidationError("symbol must be uppercase and canonical")
    return normalized


def _text(value: object, name: str, maximum: int) -> str:
    if not isinstance(value, str):
        raise CorporateActionValidationError(f"{name} must be text")
    normalized = value.strip()
    if not normalized or len(normalized) > maximum:
        raise CorporateActionValidationError(f"{name} is empty or too long")
    return normalized


def _provider_id(value: object, name: str) -> str:
    normalized = _text(value, name, 128)
    if _PROVIDER_ID_PATTERN.fullmatch(normalized) is None:
        raise CorporateActionValidationError(f"{name} is not a canonical provider identifier")
    return normalized


def _decimal(value: Decimal) -> str:
    if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
        raise CorporateActionValidationError("corporate-action decimal is invalid")
    digits = len(value.as_tuple().digits)
    if digits > 256 or value.adjusted() not in range(-18, 20):
        raise CorporateActionValidationError(
            "corporate-action decimal is not exactly representable as NUMERIC(38,18)"
        )
    with localcontext() as context:
        context.prec = max(38, digits)
        normalized = format(value.normalize(context=context), "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    integer, _, fraction = normalized.partition(".")
    if (
        normalized.startswith("-")
        or len(integer) > 20
        or len(fraction) > 18
        or len(integer.lstrip("0")) + len(fraction) > 38
        or Decimal(normalized) != value
    ):
        raise CorporateActionValidationError(
            "corporate-action decimal is not exactly representable as NUMERIC(38,18)"
        )
    return normalized


def _date_text(value: date | None) -> str | None:
    return value.isoformat() if value is not None else None


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


__all__ = [
    "CORPORATE_ACTION_ORIGIN",
    "CORPORATE_ACTION_PAGE_LIMIT",
    "CORPORATE_ACTION_QUERY_POLICY_DOCUMENT",
    "CORPORATE_ACTION_QUERY_POLICY_HASH",
    "CORPORATE_ACTION_SCHEMA_VERSION",
    "CORPORATE_ACTION_SOURCE",
    "DIVIDENDS_ENDPOINT",
    "SPLITS_ENDPOINT",
    "CorporateActionCollectionRecord",
    "CorporateActionValidationError",
    "CorporateActionVersionRecord",
    "build_dividend_collection",
    "build_split_collection",
]
