"""Fail-closed reads of immutable, receipt-bound adjusted OHLCV evidence."""

from __future__ import annotations

import json
import struct
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, DecimalException
from typing import Any, NoReturn, cast

from fastapi import status
from sqlalchemy import and_, select, union_all
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql.selectable import CompoundSelect

from app.core.exceptions import AppError, NotFoundError
from app.db.models.adjustment_factors import (
    AdjustmentFactorEntry,
    AdjustmentFactorSetAvailability,
    AdjustmentFactorSetRecord,
)
from app.db.models.bars import Bar, BarRevision, BarVersionAvailability
from app.db.models.corporate_actions import CorporateActionCollectionAvailability
from app.schemas.prices import (
    AdjustedPriceBar,
    AdjustedPriceFilters,
    AdjustedPricesResponse,
    PricePage,
)
from app.schemas.prices import (
    AdjustedPriceLineage as AdjustedPriceLineageSchema,
)
from app.services.adjusted_prices import (
    AdjustedPriceError,
    AdjustedPriceWindow,
    AdjustmentFactorSetReceipt,
    CorporateActionCollectionReceipt,
    RawOhlcvVersion,
    adjust_ohlcv_window,
)
from app.services.adjustment_factors import (
    ADJUSTMENT_FACTOR_SET_FORMAT,
    AdjustmentFactor,
    AdjustmentFactorError,
    AdjustmentFactorSet,
    DividendActionVersion,
    RawCloseVersion,
    SplitActionVersion,
    build_adjustment_factor_set,
)
from data_sources.base import DividendDistributionType, SplitAdjustmentType


class AdjustedPriceFactorSetNotFound(NotFoundError):
    """The caller's exact immutable factor-set identity does not exist."""

    code = "adjusted_price_factor_set_not_found"


class AdjustedPriceFactorSetMismatch(AppError):
    """The factor set exists but does not select the requested symbol."""

    status_code = status.HTTP_409_CONFLICT
    code = "adjusted_price_factor_set_mismatch"


class AdjustedPriceEvidenceUnavailable(AppError):
    """Persisted evidence cannot prove one exact adjusted-price result."""

    status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    code = "adjusted_price_evidence_unavailable"


@dataclass(frozen=True, slots=True)
class LoadedAdjustedPriceEvidence:
    """Every exact immutable input required by the pure adjustment kernel."""

    factor_set: AdjustmentFactorSet
    raw_rows: tuple[RawOhlcvVersion, ...]
    split_collection_receipt: CorporateActionCollectionReceipt
    dividend_collection_receipt: CorporateActionCollectionReceipt
    factor_set_receipt: AdjustmentFactorSetReceipt


@dataclass(frozen=True, slots=True)
class _DecodedFactorSet:
    artifact: AdjustmentFactorSet
    document: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _RawCandidate:
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float | None
    trade_count: int | None


def build_exact_raw_versions_statement(factor_set_id: str) -> CompoundSelect[Any]:
    """Select current or historical storage for every exact bound raw version."""

    current = (
        select(
            AdjustmentFactorEntry.ordinal.label("ordinal"),
            Bar.open.label("open"),
            Bar.high.label("high"),
            Bar.low.label("low"),
            Bar.close.label("close"),
            Bar.volume.label("volume"),
            Bar.vwap.label("vwap"),
            Bar.trade_count.label("trade_count"),
        )
        .select_from(AdjustmentFactorEntry)
        .join(
            Bar,
            and_(
                Bar.symbol == AdjustmentFactorEntry.symbol,
                Bar.timespan == AdjustmentFactorEntry.timespan,
                Bar.multiplier == AdjustmentFactorEntry.multiplier,
                Bar.ts == AdjustmentFactorEntry.observed_at,
                Bar.source == AdjustmentFactorEntry.source,
                Bar.adjustment_basis == AdjustmentFactorEntry.adjustment_basis,
                Bar.recorded_at == AdjustmentFactorEntry.version_recorded_at,
            ),
        )
        .where(AdjustmentFactorEntry.factor_set_id == factor_set_id)
    )
    previous_revision = (
        select(
            AdjustmentFactorEntry.ordinal.label("ordinal"),
            BarRevision.previous_open.label("open"),
            BarRevision.previous_high.label("high"),
            BarRevision.previous_low.label("low"),
            BarRevision.previous_close.label("close"),
            BarRevision.previous_volume.label("volume"),
            BarRevision.previous_vwap.label("vwap"),
            BarRevision.previous_trade_count.label("trade_count"),
        )
        .select_from(AdjustmentFactorEntry)
        .join(
            BarRevision,
            and_(
                BarRevision.symbol == AdjustmentFactorEntry.symbol,
                BarRevision.timespan == AdjustmentFactorEntry.timespan,
                BarRevision.multiplier == AdjustmentFactorEntry.multiplier,
                BarRevision.ts == AdjustmentFactorEntry.observed_at,
                BarRevision.source == AdjustmentFactorEntry.source,
                BarRevision.adjustment_basis == AdjustmentFactorEntry.adjustment_basis,
                BarRevision.previous_recorded_at == AdjustmentFactorEntry.version_recorded_at,
            ),
        )
        .where(AdjustmentFactorEntry.factor_set_id == factor_set_id)
    )
    incoming_revision = (
        select(
            AdjustmentFactorEntry.ordinal.label("ordinal"),
            BarRevision.incoming_open.label("open"),
            BarRevision.incoming_high.label("high"),
            BarRevision.incoming_low.label("low"),
            BarRevision.incoming_close.label("close"),
            BarRevision.incoming_volume.label("volume"),
            BarRevision.incoming_vwap.label("vwap"),
            BarRevision.incoming_trade_count.label("trade_count"),
        )
        .select_from(AdjustmentFactorEntry)
        .join(
            BarRevision,
            and_(
                BarRevision.symbol == AdjustmentFactorEntry.symbol,
                BarRevision.timespan == AdjustmentFactorEntry.timespan,
                BarRevision.multiplier == AdjustmentFactorEntry.multiplier,
                BarRevision.ts == AdjustmentFactorEntry.observed_at,
                BarRevision.source == AdjustmentFactorEntry.source,
                BarRevision.adjustment_basis == AdjustmentFactorEntry.adjustment_basis,
                BarRevision.incoming_recorded_at == AdjustmentFactorEntry.version_recorded_at,
            ),
        )
        .where(AdjustmentFactorEntry.factor_set_id == factor_set_id)
    )
    return union_all(current, previous_revision, incoming_revision).order_by("ordinal")


async def load_adjusted_price_evidence(
    session: AsyncSession,
    factor_set_id: str,
) -> LoadedAdjustedPriceEvidence:
    """Load and cross-check one exact factor set, every receipt, and raw version."""

    split_receipt_alias = aliased(CorporateActionCollectionAvailability)
    dividend_receipt_alias = aliased(CorporateActionCollectionAvailability)
    header_result = await session.execute(
        select(
            AdjustmentFactorSetRecord,
            AdjustmentFactorSetAvailability,
            split_receipt_alias,
            dividend_receipt_alias,
        )
        .outerjoin(
            AdjustmentFactorSetAvailability,
            and_(
                AdjustmentFactorSetAvailability.factor_set_id
                == AdjustmentFactorSetRecord.factor_set_id,
                AdjustmentFactorSetAvailability.factor_set_recorded_at
                == AdjustmentFactorSetRecord.recorded_at,
            ),
        )
        .outerjoin(
            split_receipt_alias,
            and_(
                split_receipt_alias.collection_id == AdjustmentFactorSetRecord.split_collection_id,
                split_receipt_alias.collection_recorded_at
                == AdjustmentFactorSetRecord.split_collection_recorded_at,
                split_receipt_alias.available_at
                == AdjustmentFactorSetRecord.split_collection_available_at,
            ),
        )
        .outerjoin(
            dividend_receipt_alias,
            and_(
                dividend_receipt_alias.collection_id
                == AdjustmentFactorSetRecord.dividend_collection_id,
                dividend_receipt_alias.collection_recorded_at
                == AdjustmentFactorSetRecord.dividend_collection_recorded_at,
                dividend_receipt_alias.available_at
                == AdjustmentFactorSetRecord.dividend_collection_available_at,
            ),
        )
        .where(AdjustmentFactorSetRecord.factor_set_id == factor_set_id)
    )
    header_row = header_result.one_or_none()
    if header_row is None:
        raise AdjustedPriceFactorSetNotFound("The requested adjustment-factor set was not found.")
    record, factor_availability, split_availability, dividend_availability = header_row
    if factor_availability is None or split_availability is None or dividend_availability is None:
        _evidence_unavailable()

    decoded = _decode_factor_set(record.canonical_payload, expected_id=factor_set_id)
    artifact = decoded.artifact
    factor_receipt = AdjustmentFactorSetReceipt(
        factor_set_id=factor_availability.factor_set_id,
        factor_set_recorded_at=_utc(factor_availability.factor_set_recorded_at),
        available_at=_utc(factor_availability.available_at),
    )
    split_receipt = CorporateActionCollectionReceipt(
        collection_id=split_availability.collection_id,
        collection_recorded_at=_utc(split_availability.collection_recorded_at),
        available_at=_utc(split_availability.available_at),
    )
    dividend_receipt = CorporateActionCollectionReceipt(
        collection_id=dividend_availability.collection_id,
        collection_recorded_at=_utc(dividend_availability.collection_recorded_at),
        available_at=_utc(dividend_availability.available_at),
    )

    entries_result = await session.execute(
        select(AdjustmentFactorEntry)
        .join(
            BarVersionAvailability,
            and_(
                BarVersionAvailability.symbol == AdjustmentFactorEntry.symbol,
                BarVersionAvailability.timespan == AdjustmentFactorEntry.timespan,
                BarVersionAvailability.multiplier == AdjustmentFactorEntry.multiplier,
                BarVersionAvailability.ts == AdjustmentFactorEntry.observed_at,
                BarVersionAvailability.source == AdjustmentFactorEntry.source,
                BarVersionAvailability.adjustment_basis == AdjustmentFactorEntry.adjustment_basis,
                BarVersionAvailability.version_recorded_at
                == AdjustmentFactorEntry.version_recorded_at,
                BarVersionAvailability.available_at == AdjustmentFactorEntry.raw_available_at,
            ),
        )
        .where(AdjustmentFactorEntry.factor_set_id == factor_set_id)
        .order_by(AdjustmentFactorEntry.ordinal)
    )
    entries = tuple(entries_result.scalars().all())
    _validate_persisted_projection(
        record=record,
        entries=entries,
        decoded=decoded,
        factor_receipt=factor_receipt,
        split_receipt=split_receipt,
        dividend_receipt=dividend_receipt,
    )

    raw_result = await session.execute(build_exact_raw_versions_statement(factor_set_id))
    raw_rows = _exact_raw_rows(entries, raw_result.all())
    return LoadedAdjustedPriceEvidence(
        factor_set=artifact,
        raw_rows=raw_rows,
        split_collection_receipt=split_receipt,
        dividend_collection_receipt=dividend_receipt,
        factor_set_receipt=factor_receipt,
    )


async def read_adjusted_prices(
    session: AsyncSession,
    symbol: str,
    filters: AdjustedPriceFilters,
) -> AdjustedPricesResponse:
    """Validate the full immutable window, then apply bounds and keyset pagination."""

    evidence = await load_adjusted_price_evidence(session, filters.factor_set_id)
    normalized_symbol = symbol.strip().upper()
    if evidence.factor_set.symbol != normalized_symbol:
        raise AdjustedPriceFactorSetMismatch(
            "The adjustment-factor set does not belong to the requested symbol."
        )
    try:
        full_window = adjust_ohlcv_window(
            factor_set=evidence.factor_set,
            raw_rows=evidence.raw_rows,
            split_collection_receipt=evidence.split_collection_receipt,
            dividend_collection_receipt=evidence.dividend_collection_receipt,
            factor_set_receipt=evidence.factor_set_receipt,
            start_ordinal=0,
            stop_ordinal=None,
        )
    except AdjustedPriceError as exc:
        raise AdjustedPriceEvidenceUnavailable(
            "Adjusted-price evidence is incomplete or invalid."
        ) from exc

    rows = tuple(
        row
        for row in full_window.rows
        if (filters.start is None or row.timestamp >= filters.start)
        and (filters.end is None or row.timestamp < filters.end)
    )
    selected_desc = tuple(reversed(rows))[: filters.limit + 1]
    has_more = len(selected_desc) > filters.limit
    page_rows = tuple(reversed(selected_desc[: filters.limit]))
    next_end = page_rows[0].timestamp if has_more else None
    lineage = _lineage_schema(full_window)
    bars = [
        AdjustedPriceBar(
            raw_input_ordinal=row.raw_input_ordinal,
            timestamp=row.timestamp,
            open=row.open,
            high=row.high,
            low=row.low,
            close=row.close,
            volume=row.volume,
            vwap=row.vwap,
            trade_count=row.trade_count,
            raw_version_recorded_at=row.raw_version_recorded_at,
            raw_available_at=row.raw_available_at,
            available_at=row.available_at,
            price_factor_f64_be=row.price_factor_f64_be,
            volume_factor_f64_be=row.volume_factor_f64_be,
        )
        for row in page_rows
    ]
    return AdjustedPricesResponse(
        symbol=normalized_symbol,
        source="polygon_open_close",
        timespan="day",
        multiplier=1,
        adjustment_basis="split_dividend_adjusted",
        factor_set_id=evidence.factor_set.factor_set_id,
        data_available_at=full_window.lineage.data_available_at,
        count=len(bars),
        page=PricePage(
            limit=filters.limit,
            has_more=has_more,
            next_end=next_end,
        ),
        lineage=lineage,
        bars=bars,
    )


def _decode_factor_set(payload: object, *, expected_id: str) -> _DecodedFactorSet:
    if not isinstance(payload, bytes):
        _evidence_unavailable()
    try:
        document = json.loads(
            payload,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_json_constant,
        )
        root = _object(document, "factor payload")
        actions = _object(root["actions"], "factor actions")
        split_document = _object(actions["splits"], "split collection")
        dividend_document = _object(actions["dividends"], "dividend collection")
        raw_inputs = tuple(
            RawCloseVersion(
                observation_date=_date(_object(row, "raw input")["observation_date"]),
                observed_at=_timestamp(_object(row, "raw input")["observed_at"]),
                timespan=_string(_object(row, "raw input")["timespan"]),
                multiplier=_integer(_object(row, "raw input")["multiplier"]),
                source=_string(_object(row, "raw input")["source"]),
                adjustment_basis=_string(_object(row, "raw input")["adjustment_basis"]),
                version_recorded_at=_timestamp(_object(row, "raw input")["version_recorded_at"]),
                available_at=_timestamp(_object(row, "raw input")["available_at"]),
                close=_decimal(_object(row, "raw input")["close_decimal"]),
            )
            for row in _array(root["raw_inputs"], "raw inputs")
        )
        splits = tuple(
            SplitActionVersion(
                provider_event_id=_string(_object(row, "split")["provider_event_id"]),
                version_id=_string(_object(row, "split")["version_id"]),
                effective_date=_date(_object(row, "split")["effective_date"]),
                split_from=_decimal(_object(row, "split")["split_from"]),
                split_to=_decimal(_object(row, "split")["split_to"]),
                adjustment_type=cast(
                    SplitAdjustmentType,
                    _string(_object(row, "split")["adjustment_type"]),
                ),
            )
            for row in _array(split_document["versions"], "split versions")
        )
        dividends = tuple(
            DividendActionVersion(
                provider_event_id=_string(_object(row, "dividend")["provider_event_id"]),
                version_id=_string(_object(row, "dividend")["version_id"]),
                ex_dividend_date=_date(_object(row, "dividend")["ex_dividend_date"]),
                cash_amount=_decimal(_object(row, "dividend")["cash_amount"]),
                currency=_nullable_string(_object(row, "dividend")["currency"]),
                distribution_type=cast(
                    DividendDistributionType,
                    _string(_object(row, "dividend")["distribution_type"]),
                ),
            )
            for row in _array(dividend_document["versions"], "dividend versions")
        )
        artifact = build_adjustment_factor_set(
            symbol=_string(root["symbol"]),
            cutoff=_timestamp(root["cutoff"]),
            raw_closes=raw_inputs,
            split_collection_id=_string(split_document["collection_id"]),
            splits=splits,
            dividend_collection_id=_string(dividend_document["collection_id"]),
            dividends=dividends,
        )
    except (
        AdjustmentFactorError,
        DecimalException,
        KeyError,
        TypeError,
        UnicodeDecodeError,
        ValueError,
        json.JSONDecodeError,
    ) as exc:
        raise AdjustedPriceEvidenceUnavailable(
            "Adjusted-price evidence is incomplete or invalid."
        ) from exc
    if artifact.factor_set_id != expected_id or artifact.canonical_payload != payload:
        _evidence_unavailable()
    return _DecodedFactorSet(artifact=artifact, document=root)


def _validate_persisted_projection(
    *,
    record: AdjustmentFactorSetRecord,
    entries: tuple[AdjustmentFactorEntry, ...],
    decoded: _DecodedFactorSet,
    factor_receipt: AdjustmentFactorSetReceipt,
    split_receipt: CorporateActionCollectionReceipt,
    dividend_receipt: CorporateActionCollectionReceipt,
) -> None:
    artifact = decoded.artifact
    raw_documents = _array(decoded.document.get("raw_inputs"), "raw inputs")
    factor_documents = _array(decoded.document.get("factors"), "factors")
    max_input_available_at = max(
        *(item.available_at for item in artifact.raw_inputs),
        split_receipt.available_at,
        dividend_receipt.available_at,
    )
    if not (
        record.factor_set_id == artifact.factor_set_id
        and record.format == ADJUSTMENT_FACTOR_SET_FORMAT
        and record.policy_version == artifact.policy_version
        and record.policy_hash == artifact.policy_hash
        and record.symbol == artifact.symbol
        and _utc(record.cutoff) == artifact.cutoff
        and record.anchor_date == artifact.anchor_date
        and record.coverage_start == artifact.raw_inputs[0].observation_date
        and record.coverage_end == artifact.raw_inputs[-1].observation_date
        and record.input_count == len(artifact.raw_inputs)
        and _utc(record.max_input_available_at) == max_input_available_at
        and record.split_collection_id == artifact.split_collection_id
        and _utc(record.split_collection_recorded_at) == split_receipt.collection_recorded_at
        and _utc(record.split_collection_available_at) == split_receipt.available_at
        and record.dividend_collection_id == artifact.dividend_collection_id
        and _utc(record.dividend_collection_recorded_at) == dividend_receipt.collection_recorded_at
        and _utc(record.dividend_collection_available_at) == dividend_receipt.available_at
        and record.canonical_payload == artifact.canonical_payload
        and factor_receipt.factor_set_id == artifact.factor_set_id
        and factor_receipt.factor_set_recorded_at == _utc(record.recorded_at)
        and max_input_available_at
        <= factor_receipt.factor_set_recorded_at
        <= factor_receipt.available_at
    ):
        _evidence_unavailable()
    if len(entries) != len(artifact.raw_inputs) or len(entries) != len(artifact.factors):
        _evidence_unavailable()
    for ordinal, (entry, raw, factor, raw_document, factor_document) in enumerate(
        zip(
            entries,
            artifact.raw_inputs,
            artifact.factors,
            raw_documents,
            factor_documents,
            strict=True,
        )
    ):
        raw_projection = _object(raw_document, "raw input")
        factor_projection = _object(factor_document, "factor")
        if not _entry_matches(
            ordinal,
            entry=entry,
            artifact=artifact,
            raw=raw,
            factor=factor,
            raw_projection=raw_projection,
            factor_projection=factor_projection,
        ):
            _evidence_unavailable()


def _entry_matches(
    ordinal: int,
    *,
    entry: AdjustmentFactorEntry,
    artifact: AdjustmentFactorSet,
    raw: RawCloseVersion,
    factor: AdjustmentFactor,
    raw_projection: dict[str, Any],
    factor_projection: dict[str, Any],
) -> bool:
    return bool(
        entry.ordinal == ordinal
        and entry.factor_set_id == artifact.factor_set_id
        and entry.symbol == artifact.symbol
        and entry.observation_date == raw.observation_date
        and _utc(entry.observed_at) == raw.observed_at
        and entry.timespan == raw.timespan
        and entry.multiplier == raw.multiplier
        and entry.source == raw.source
        and entry.adjustment_basis == raw.adjustment_basis
        and _utc(entry.version_recorded_at) == raw.version_recorded_at
        and _utc(entry.raw_available_at) == raw.available_at
        and entry.raw_close_decimal == raw_projection.get("close_decimal")
        and entry.raw_close_f64_be.hex() == raw_projection.get("close_f64_be")
        and entry.price_factor_decimal == factor.price_factor_decimal
        and entry.price_factor_decimal == factor_projection.get("price_factor_decimal")
        and entry.price_factor_f64_be.hex() == factor.price_factor_f64_be
        and entry.price_factor_f64_be.hex() == factor_projection.get("price_factor_f64_be")
        and entry.volume_factor_decimal == factor.volume_factor_decimal
        and entry.volume_factor_decimal == factor_projection.get("volume_factor_decimal")
        and entry.volume_factor_f64_be.hex() == factor.volume_factor_f64_be
        and entry.volume_factor_f64_be.hex() == factor_projection.get("volume_factor_f64_be")
    )


def _exact_raw_rows(
    entries: tuple[AdjustmentFactorEntry, ...],
    candidate_rows: Sequence[Any],
) -> tuple[RawOhlcvVersion, ...]:
    candidates: dict[int, list[_RawCandidate]] = {entry.ordinal: [] for entry in entries}
    for row in candidate_rows:
        ordinal = row.ordinal
        if type(ordinal) is not int or ordinal not in candidates:
            _evidence_unavailable()
        candidates[ordinal].append(
            _RawCandidate(
                open=row.open,
                high=row.high,
                low=row.low,
                close=row.close,
                volume=row.volume,
                vwap=row.vwap,
                trade_count=row.trade_count,
            )
        )
    result: list[RawOhlcvVersion] = []
    for entry in entries:
        versions = candidates[entry.ordinal]
        if not versions or any(
            _candidate_fingerprint(candidate) != _candidate_fingerprint(versions[0])
            for candidate in versions[1:]
        ):
            _evidence_unavailable()
        candidate = versions[0]
        result.append(
            RawOhlcvVersion(
                symbol=entry.symbol,
                timestamp=_utc(entry.observed_at),
                timespan=entry.timespan,
                multiplier=entry.multiplier,
                source=entry.source,
                adjustment_basis=entry.adjustment_basis,
                version_recorded_at=_utc(entry.version_recorded_at),
                available_at=_utc(entry.raw_available_at),
                open=candidate.open,
                high=candidate.high,
                low=candidate.low,
                close=candidate.close,
                volume=candidate.volume,
                vwap=candidate.vwap,
                trade_count=candidate.trade_count,
            )
        )
    return tuple(result)


def _candidate_fingerprint(value: _RawCandidate) -> tuple[bytes | None, ...]:
    def bits(number: float | None) -> bytes | None:
        return None if number is None else struct.pack(">d", number)

    return (
        bits(value.open),
        bits(value.high),
        bits(value.low),
        bits(value.close),
        bits(value.volume),
        bits(value.vwap),
        None if value.trade_count is None else str(value.trade_count).encode("ascii"),
    )


def _lineage_schema(window: AdjustedPriceWindow) -> AdjustedPriceLineageSchema:
    if not window.rows:
        _evidence_unavailable()
    lineage = window.lineage
    return AdjustedPriceLineageSchema(
        factor_set_id=lineage.factor_set_id,
        factor_set_recorded_at=lineage.factor_set_recorded_at,
        factor_set_available_at=lineage.factor_set_available_at,
        policy_version=lineage.policy_version,
        policy_hash=lineage.policy_hash,
        cutoff=lineage.cutoff,
        anchor_date=lineage.anchor_date,
        raw_coverage_start=window.rows[0].timestamp,
        raw_coverage_end=window.rows[-1].timestamp,
        split_collection_id=lineage.split_collection_id,
        split_collection_recorded_at=lineage.split_collection_recorded_at,
        split_collection_available_at=lineage.split_collection_available_at,
        dividend_collection_id=lineage.dividend_collection_id,
        dividend_collection_recorded_at=lineage.dividend_collection_recorded_at,
        dividend_collection_available_at=lineage.dividend_collection_available_at,
        action_version_ids=lineage.action_version_ids,
        max_input_available_at=lineage.max_input_available_at,
        data_available_at=lineage.data_available_at,
        raw_input_count=lineage.raw_input_count,
        adjustment_basis=lineage.adjustment_basis,
    )


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> NoReturn:
    raise ValueError(f"unsupported JSON constant: {value}")


def _object(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError(f"{label} must be an object")
    return cast(dict[str, Any], value)


def _array(value: object, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be an array")
    return value


def _string(value: object) -> str:
    if type(value) is not str:
        raise ValueError("canonical value must be a string")
    return value


def _nullable_string(value: object) -> str | None:
    return None if value is None else _string(value)


def _integer(value: object) -> int:
    if type(value) is not int:
        raise ValueError("canonical value must be an integer")
    return value


def _decimal(value: object) -> Decimal:
    return Decimal(_string(value))


def _date(value: object) -> date:
    return date.fromisoformat(_string(value))


def _timestamp(value: object) -> datetime:
    return _utc(datetime.fromisoformat(_string(value).replace("Z", "+00:00")))


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        _evidence_unavailable()
    return value.astimezone(UTC)


def _evidence_unavailable() -> NoReturn:
    raise AdjustedPriceEvidenceUnavailable("Adjusted-price evidence is incomplete or invalid.")


__all__ = [
    "AdjustedPriceEvidenceUnavailable",
    "AdjustedPriceFactorSetMismatch",
    "AdjustedPriceFactorSetNotFound",
    "LoadedAdjustedPriceEvidence",
    "build_exact_raw_versions_statement",
    "load_adjusted_price_evidence",
    "read_adjusted_prices",
]
