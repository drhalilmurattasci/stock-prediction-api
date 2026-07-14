"""Exact-evidence loading and pagination tests for adjusted-price serving."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, time
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pydantic import ValidationError
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.adjustment_factors import (
    AdjustmentFactorEntry,
    AdjustmentFactorSetAvailability,
    AdjustmentFactorSetRecord,
)
from app.db.models.corporate_actions import CorporateActionCollectionAvailability
from app.schemas.prices import AdjustedPriceFilters, PricePage
from app.services import adjusted_price_store as store
from app.services.adjusted_price_store import (
    AdjustedPriceEvidenceUnavailable,
    AdjustedPriceFactorSetMismatch,
    AdjustedPriceFactorSetNotFound,
    LoadedAdjustedPriceEvidence,
    build_exact_raw_versions_statement,
    load_adjusted_price_evidence,
    read_adjusted_prices,
)
from app.services.adjusted_prices import (
    AdjustmentFactorSetReceipt,
    CorporateActionCollectionReceipt,
    RawOhlcvVersion,
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
SPLIT_RECORDED = datetime(2026, 7, 10, 20, 7, tzinfo=UTC)
SPLIT_AVAILABLE = datetime(2026, 7, 10, 20, 8, tzinfo=UTC)
DIVIDEND_RECORDED = datetime(2026, 7, 10, 20, 8, tzinfo=UTC)
DIVIDEND_AVAILABLE = datetime(2026, 7, 10, 20, 9, tzinfo=UTC)
FACTOR_RECORDED = datetime(2026, 7, 10, 21, 1, tzinfo=UTC)
FACTOR_AVAILABLE = datetime(2026, 7, 10, 21, 2, tzinfo=UTC)


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


def _artifact() -> AdjustmentFactorSet:
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
        ),
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


def _evidence() -> LoadedAdjustedPriceEvidence:
    artifact = _artifact()
    return LoadedAdjustedPriceEvidence(
        factor_set=artifact,
        raw_rows=_raw_rows(),
        split_collection_receipt=CorporateActionCollectionReceipt(
            collection_id=SPLIT_COLLECTION_ID,
            collection_recorded_at=SPLIT_RECORDED,
            available_at=SPLIT_AVAILABLE,
        ),
        dividend_collection_receipt=CorporateActionCollectionReceipt(
            collection_id=DIVIDEND_COLLECTION_ID,
            collection_recorded_at=DIVIDEND_RECORDED,
            available_at=DIVIDEND_AVAILABLE,
        ),
        factor_set_receipt=AdjustmentFactorSetReceipt(
            factor_set_id=artifact.factor_set_id,
            factor_set_recorded_at=FACTOR_RECORDED,
            available_at=FACTOR_AVAILABLE,
        ),
    )


class _HeaderResult:
    def __init__(self, row: Any) -> None:
        self.row = row

    def one_or_none(self) -> Any:
        return self.row


class _Scalars:
    def __init__(self, rows: list[Any]) -> None:
        self.rows = rows

    def all(self) -> list[Any]:
        return self.rows


class _EntriesResult:
    def __init__(self, rows: list[Any]) -> None:
        self.rows = rows

    def scalars(self) -> _Scalars:
        return _Scalars(self.rows)


class _RawResult:
    def __init__(self, rows: list[Any]) -> None:
        self.rows = rows

    def all(self) -> list[Any]:
        return self.rows


class _Session:
    def __init__(self, results: list[Any]) -> None:
        self.results = results
        self.statements: list[Any] = []

    async def execute(self, statement: Any) -> Any:
        self.statements.append(statement)
        return self.results.pop(0)


def _persisted_results(
    *,
    raw_candidates: list[Any] | None = None,
    header_override: Any = None,
) -> tuple[list[Any], AdjustmentFactorSet]:
    artifact = _artifact()
    document = json.loads(artifact.canonical_payload)
    record = AdjustmentFactorSetRecord(
        factor_set_id=artifact.factor_set_id,
        format=document["format"],
        policy_version=artifact.policy_version,
        policy_hash=artifact.policy_hash,
        symbol=artifact.symbol,
        cutoff=artifact.cutoff,
        anchor_date=artifact.anchor_date,
        coverage_start=artifact.raw_inputs[0].observation_date,
        coverage_end=artifact.raw_inputs[-1].observation_date,
        input_count=len(artifact.raw_inputs),
        max_input_available_at=DIVIDEND_AVAILABLE,
        split_collection_id=SPLIT_COLLECTION_ID,
        split_collection_recorded_at=SPLIT_RECORDED,
        split_collection_available_at=SPLIT_AVAILABLE,
        dividend_collection_id=DIVIDEND_COLLECTION_ID,
        dividend_collection_recorded_at=DIVIDEND_RECORDED,
        dividend_collection_available_at=DIVIDEND_AVAILABLE,
        canonical_payload=artifact.canonical_payload,
        recorded_at=FACTOR_RECORDED,
        creator_xid=1,
    )
    factor_receipt = AdjustmentFactorSetAvailability(
        factor_set_id=artifact.factor_set_id,
        factor_set_recorded_at=FACTOR_RECORDED,
        available_at=FACTOR_AVAILABLE,
    )
    split_receipt = CorporateActionCollectionAvailability(
        collection_id=SPLIT_COLLECTION_ID,
        collection_recorded_at=SPLIT_RECORDED,
        available_at=SPLIT_AVAILABLE,
    )
    dividend_receipt = CorporateActionCollectionAvailability(
        collection_id=DIVIDEND_COLLECTION_ID,
        collection_recorded_at=DIVIDEND_RECORDED,
        available_at=DIVIDEND_AVAILABLE,
    )
    entries = [
        AdjustmentFactorEntry(
            factor_set_id=artifact.factor_set_id,
            ordinal=ordinal,
            symbol=artifact.symbol,
            observation_date=raw.observation_date,
            observed_at=raw.observed_at,
            timespan=raw.timespan,
            multiplier=raw.multiplier,
            source=raw.source,
            adjustment_basis=raw.adjustment_basis,
            version_recorded_at=raw.version_recorded_at,
            raw_available_at=raw.available_at,
            raw_close_decimal=document["raw_inputs"][ordinal]["close_decimal"],
            raw_close_f64_be=bytes.fromhex(document["raw_inputs"][ordinal]["close_f64_be"]),
            price_factor_decimal=factor.price_factor_decimal,
            price_factor_f64_be=bytes.fromhex(factor.price_factor_f64_be),
            volume_factor_decimal=factor.volume_factor_decimal,
            volume_factor_f64_be=bytes.fromhex(factor.volume_factor_f64_be),
            creator_xid=1,
        )
        for ordinal, (raw, factor) in enumerate(
            zip(artifact.raw_inputs, artifact.factors, strict=True)
        )
    ]
    candidates = raw_candidates or [
        SimpleNamespace(
            ordinal=ordinal,
            open=row.open,
            high=row.high,
            low=row.low,
            close=row.close,
            volume=row.volume,
            vwap=row.vwap,
            trade_count=row.trade_count,
        )
        for ordinal, row in enumerate(_raw_rows())
    ]
    header = (
        record,
        factor_receipt,
        split_receipt,
        dividend_receipt,
    )
    if header_override is not None:
        header = header_override
    return [
        _HeaderResult(header),
        _EntriesResult(entries),
        _RawResult(candidates),
    ], artifact


def test_filters_require_an_exact_factor_identity_and_aware_bounds() -> None:
    with pytest.raises(ValidationError):
        AdjustedPriceFilters.model_validate({})
    with pytest.raises(ValidationError):
        AdjustedPriceFilters(factor_set_id="latest")
    with pytest.raises(ValidationError, match="timezone"):
        AdjustedPriceFilters(
            factor_set_id=_artifact().factor_set_id,
            start=datetime(2026, 7, 1),
        )


def test_exact_raw_statement_covers_current_previous_and_incoming_versions() -> None:
    artifact = _artifact()
    sql = str(
        build_exact_raw_versions_statement(artifact.factor_set_id).compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert sql.count("UNION ALL") == 2
    assert "JOIN bars ON" in sql
    assert sql.count("JOIN bars_revisions ON") == 2
    assert "bars.recorded_at = adjustment_factor_entries.version_recorded_at" in sql
    assert (
        "bars_revisions.previous_recorded_at = adjustment_factor_entries.version_recorded_at"
    ) in sql
    assert (
        "bars_revisions.incoming_recorded_at = adjustment_factor_entries.version_recorded_at"
    ) in sql
    assert artifact.factor_set_id in sql


@pytest.mark.asyncio
async def test_loader_reconstructs_exact_persisted_artifact_receipts_and_raw_versions() -> None:
    results, artifact = _persisted_results()
    session = _Session(results)

    loaded = await load_adjusted_price_evidence(cast(AsyncSession, session), artifact.factor_set_id)

    assert loaded.factor_set == artifact
    assert loaded.raw_rows == _raw_rows()
    assert loaded.factor_set_receipt.available_at == FACTOR_AVAILABLE
    assert loaded.split_collection_receipt.available_at == SPLIT_AVAILABLE
    assert loaded.dividend_collection_receipt.available_at == DIVIDEND_AVAILABLE
    assert len(session.statements) == 3
    assert session.results == []


@pytest.mark.asyncio
async def test_loader_fails_closed_when_exact_raw_version_is_missing_or_ambiguous() -> None:
    default_results, artifact = _persisted_results()
    candidates = default_results[-1].rows

    missing_results, _ = _persisted_results(raw_candidates=candidates[:-1])
    with pytest.raises(AdjustedPriceEvidenceUnavailable):
        await load_adjusted_price_evidence(
            cast(AsyncSession, _Session(missing_results)), artifact.factor_set_id
        )

    changed = vars(candidates[0]).copy()
    changed["close"] = 999.0
    conflicting = [*candidates, SimpleNamespace(**changed)]
    ambiguous_results, _ = _persisted_results(raw_candidates=conflicting)
    with pytest.raises(AdjustedPriceEvidenceUnavailable):
        await load_adjusted_price_evidence(
            cast(AsyncSession, _Session(ambiguous_results)), artifact.factor_set_id
        )


@pytest.mark.asyncio
async def test_loader_distinguishes_absent_factor_from_incomplete_receipts() -> None:
    with pytest.raises(AdjustedPriceFactorSetNotFound):
        await load_adjusted_price_evidence(
            cast(AsyncSession, _Session([_HeaderResult(None)])),
            _artifact().factor_set_id,
        )

    results, artifact = _persisted_results()
    record, _, split_receipt, dividend_receipt = results[0].row
    results[0] = _HeaderResult((record, None, split_receipt, dividend_receipt))
    with pytest.raises(AdjustedPriceEvidenceUnavailable):
        await load_adjusted_price_evidence(
            cast(AsyncSession, _Session(results)), artifact.factor_set_id
        )


@pytest.mark.asyncio
async def test_read_validates_full_window_before_time_filtering_and_pagination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = _evidence()

    async def fake_load(session: AsyncSession, factor_set_id: str) -> LoadedAdjustedPriceEvidence:
        assert factor_set_id == evidence.factor_set.factor_set_id
        return evidence

    real_adjust = store.adjust_ohlcv_window
    calls: list[tuple[int, int | None, int]] = []

    def spy_adjust(**kwargs: Any):
        calls.append(
            (
                kwargs["start_ordinal"],
                kwargs["stop_ordinal"],
                len(kwargs["raw_rows"]),
            )
        )
        return real_adjust(**kwargs)

    monkeypatch.setattr(store, "load_adjusted_price_evidence", fake_load)
    monkeypatch.setattr(store, "adjust_ohlcv_window", spy_adjust)

    first = await read_adjusted_prices(
        cast(AsyncSession, object()),
        "msft",
        AdjustedPriceFilters(factor_set_id=evidence.factor_set.factor_set_id, limit=2),
    )

    assert calls == [(0, None, 5)]
    assert [bar.raw_input_ordinal for bar in first.bars] == [3, 4]
    assert first.page == PricePage(
        limit=2,
        has_more=True,
        next_end=datetime(2026, 7, 9, 20, tzinfo=UTC),
    )
    assert first.factor_set_id == evidence.factor_set.factor_set_id
    assert first.data_available_at == FACTOR_AVAILABLE
    assert first.lineage.raw_coverage_start == datetime(2026, 7, 6, 20, tzinfo=UTC)
    assert first.lineage.raw_coverage_end == datetime(2026, 7, 10, 20, tzinfo=UTC)
    assert first.lineage.action_version_ids == (SPLIT_VERSION_ID, DIVIDEND_VERSION_ID)
    assert all(bar.available_at == FACTOR_AVAILABLE for bar in first.bars)

    second = await read_adjusted_prices(
        cast(AsyncSession, object()),
        "MSFT",
        AdjustedPriceFilters(
            factor_set_id=evidence.factor_set.factor_set_id,
            end=first.page.next_end,
            limit=2,
        ),
    )
    assert calls[-1] == (0, None, 5)
    assert [bar.raw_input_ordinal for bar in second.bars] == [1, 2]
    assert not (
        {bar.raw_input_ordinal for bar in first.bars}
        & {bar.raw_input_ordinal for bar in second.bars}
    )


@pytest.mark.asyncio
async def test_read_rejects_symbol_mismatch_without_raw_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = _evidence()

    async def fake_load(session: AsyncSession, factor_set_id: str) -> LoadedAdjustedPriceEvidence:
        return evidence

    monkeypatch.setattr(store, "load_adjusted_price_evidence", fake_load)
    with pytest.raises(AdjustedPriceFactorSetMismatch):
        await read_adjusted_prices(
            cast(AsyncSession, object()),
            "AAPL",
            AdjustedPriceFilters(factor_set_id=evidence.factor_set.factor_set_id),
        )
