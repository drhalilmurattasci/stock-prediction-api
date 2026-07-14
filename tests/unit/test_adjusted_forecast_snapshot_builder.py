"""Factor-backed adjusted-close snapshot policy and builder tests."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import cast

import exchange_calendars
import pandas as pd
import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import app.services.adjusted_forecast_snapshot_builder as builder_module
from app.db.models.forecast_snapshots import ForecastInputSnapshot
from app.services.adjusted_forecast_snapshot_builder import (
    ADJUSTED_AVAILABILITY_RULE_SET_DOCUMENT,
    ADJUSTED_AVAILABILITY_RULE_SET_HASH,
    ADJUSTED_RESOLUTION_POLICY_DOCUMENT,
    ADJUSTED_RESOLUTION_POLICY_HASH,
    DEFAULT_ADJUSTED_SNAPSHOT_BUILD_POLICY,
    AdjustedForecastSnapshotBuilder,
    AdjustedSnapshotBuildSpec,
    assemble_verified_adjusted_snapshot_payload,
    build_eligible_adjustment_factor_statement,
    verify_adjusted_snapshot_evidence,
)
from app.services.adjusted_price_store import LoadedAdjustedPriceEvidence
from app.services.adjusted_prices import (
    AdjustmentFactorSetReceipt,
    CorporateActionCollectionReceipt,
    RawOhlcvVersion,
)
from app.services.adjustment_factors import RawCloseVersion, build_adjustment_factor_set
from app.services.forecast_snapshot_builder import (
    DEFAULT_AVAILABILITY_RULE_SET_HASH,
    DEFAULT_RESOLUTION_POLICY_HASH,
    SnapshotAvailabilityError,
    SnapshotBuildError,
    SnapshotBuildMisconfigured,
    SnapshotInputUnavailable,
    SnapshotSemanticConflict,
)

POLICY = DEFAULT_ADJUSTED_SNAPSHOT_BUILD_POLICY
SNAPSHOT_AS_OF = datetime(2026, 7, 14, 17, tzinfo=UTC)
FACTOR_CUTOFF = datetime(2026, 7, 13, 21, tzinfo=UTC)
FACTOR_RECORDED_AT = datetime(2026, 7, 13, 21, 1, tzinfo=UTC)
FACTOR_AVAILABLE_AT = datetime(2026, 7, 13, 21, 2, tzinfo=UTC)
SPLIT_COLLECTION_ID = "sha256:" + "1" * 64
DIVIDEND_COLLECTION_ID = "sha256:" + "2" * 64


def _spec(as_of: datetime = SNAPSHOT_AS_OF) -> AdjustedSnapshotBuildSpec:
    return AdjustedSnapshotBuildSpec(
        symbol="MSFT",
        target="adjusted_close",
        horizon_unit="trading_day",
        as_of=as_of,
    )


def _evidence(*, count: int = 258) -> LoadedAdjustedPriceEvidence:
    calendar = exchange_calendars.get_calendar(
        "XNYS",
        start="1990-01-01",
        end="2100-12-31",
    )
    labels = calendar.sessions_window(pd.Timestamp("2026-07-13"), -count)
    raw_closes: list[RawCloseVersion] = []
    raw_rows: list[RawOhlcvVersion] = []
    for ordinal, label in enumerate(labels):
        observed_at = calendar.session_close(label).to_pydatetime().astimezone(UTC)
        recorded_at = observed_at + timedelta(minutes=5)
        available_at = observed_at + timedelta(minutes=6)
        close = 100.0 + ordinal / 10.0
        raw_closes.append(
            RawCloseVersion(
                observation_date=label.date(),
                observed_at=observed_at,
                timespan="day",
                multiplier=1,
                source="polygon_open_close",
                adjustment_basis="raw",
                version_recorded_at=recorded_at,
                available_at=available_at,
                close=Decimal(str(close)),
            )
        )
        raw_rows.append(
            RawOhlcvVersion(
                symbol="MSFT",
                timestamp=observed_at,
                timespan="day",
                multiplier=1,
                source="polygon_open_close",
                adjustment_basis="raw",
                version_recorded_at=recorded_at,
                available_at=available_at,
                open=close - 0.5,
                high=close + 1.0,
                low=close - 1.0,
                close=close,
                volume=1_000_000.0 + ordinal,
                vwap=close - 0.1,
                trade_count=10_000 + ordinal,
            )
        )
    artifact = build_adjustment_factor_set(
        symbol="MSFT",
        cutoff=FACTOR_CUTOFF,
        raw_closes=tuple(raw_closes),
        split_collection_id=SPLIT_COLLECTION_ID,
        splits=(),
        dividend_collection_id=DIVIDEND_COLLECTION_ID,
        dividends=(),
    )
    split_receipt = CorporateActionCollectionReceipt(
        collection_id=SPLIT_COLLECTION_ID,
        collection_recorded_at=FACTOR_CUTOFF - timedelta(minutes=12),
        available_at=FACTOR_CUTOFF - timedelta(minutes=11),
    )
    dividend_receipt = CorporateActionCollectionReceipt(
        collection_id=DIVIDEND_COLLECTION_ID,
        collection_recorded_at=FACTOR_CUTOFF - timedelta(minutes=10),
        available_at=FACTOR_CUTOFF - timedelta(minutes=9),
    )
    return LoadedAdjustedPriceEvidence(
        factor_set=artifact,
        raw_rows=tuple(raw_rows),
        split_collection_receipt=split_receipt,
        dividend_collection_receipt=dividend_receipt,
        factor_set_receipt=AdjustmentFactorSetReceipt(
            factor_set_id=artifact.factor_set_id,
            factor_set_recorded_at=FACTOR_RECORDED_AT,
            available_at=FACTOR_AVAILABLE_AT,
        ),
    )


def test_adjusted_policy_hashes_are_distinct_golden_content_identities() -> None:
    assert ADJUSTED_RESOLUTION_POLICY_HASH == (
        "sha256:5874503cee922188f7892e52862833dabf489327d0cf587344a9696c45e97ed7"
    )
    assert ADJUSTED_AVAILABILITY_RULE_SET_HASH == (
        "sha256:f713f17d24d225dd53984a726b08dd6990ec47de151769f68eb63f5efd039dbd"
    )
    assert ADJUSTED_RESOLUTION_POLICY_HASH != DEFAULT_RESOLUTION_POLICY_HASH
    assert ADJUSTED_AVAILABILITY_RULE_SET_HASH != DEFAULT_AVAILABILITY_RULE_SET_HASH
    assert ADJUSTED_RESOLUTION_POLICY_DOCUMENT["availability_rule_set_hash"] == (
        ADJUSTED_AVAILABILITY_RULE_SET_HASH
    )
    rules = ADJUSTED_AVAILABILITY_RULE_SET_DOCUMENT["rules"]
    assert isinstance(rules, list)
    assert "derived_observation_available_at=factor_set_receipt_available_at" in rules
    assert replace(POLICY, observation_limit=511).resolution_policy_hash != (
        ADJUSTED_RESOLUTION_POLICY_HASH
    )


def test_adjusted_policy_requires_exact_pins_target_and_window() -> None:
    POLICY.validate_configured_hashes(
        ADJUSTED_RESOLUTION_POLICY_HASH,
        ADJUSTED_AVAILABILITY_RULE_SET_HASH,
    )
    with pytest.raises(SnapshotBuildMisconfigured, match="resolution-policy"):
        POLICY.validate_configured_hashes(None, ADJUSTED_AVAILABILITY_RULE_SET_HASH)
    with pytest.raises(SnapshotBuildMisconfigured, match="availability"):
        POLICY.validate_configured_hashes(ADJUSTED_RESOLUTION_POLICY_HASH, None)
    with pytest.raises(SnapshotBuildError, match="only split/dividend-adjusted"):
        POLICY.validate_spec(
            AdjustedSnapshotBuildSpec(
                "MSFT",
                "close",  # type: ignore[arg-type]
                "trading_day",
                SNAPSHOT_AS_OF,
            )
        )
    with pytest.raises(SnapshotBuildMisconfigured, match="window bounds"):
        replace(POLICY, minimum_observations=513).validate_spec(_spec())


def test_factor_selector_is_policy_receipt_anchor_and_order_bound() -> None:
    statement = build_eligible_adjustment_factor_statement(
        _spec(),
        anchor_date=date(2026, 7, 13),
    )
    sql = str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "JOIN adjustment_factor_set_availability ON" in sql
    assert "adjustment_factor_sets.symbol = 'MSFT'" in sql
    assert "adjustment_factor_sets.policy_hash = 'sha256:" in sql
    assert "adjustment_factor_sets.cutoff <= '2026-07-14 17:00:00+00:00'" in sql
    assert "adjustment_factor_set_availability.available_at <=" in sql
    assert "adjustment_factor_sets.anchor_date = '2026-07-13'" in sql
    assert "adjustment_factor_sets.input_count >= 258" in sql
    assert (
        "ORDER BY adjustment_factor_sets.cutoff DESC, "
        "adjustment_factor_set_availability.available_at DESC, "
        "adjustment_factor_sets.recorded_at DESC, "
        "adjustment_factor_sets.factor_set_id DESC" in sql
    )
    assert "LIMIT 1" in sql


def test_full_factor_window_is_validated_before_latest_observation_slice() -> None:
    evidence = _evidence(count=300)
    policy = replace(POLICY, observation_limit=258)
    window = verify_adjusted_snapshot_evidence(evidence, _spec(), policy=policy)
    payload = assemble_verified_adjusted_snapshot_payload(
        evidence,
        window,
        _spec(),
        checked_at=SNAPSHOT_AS_OF + timedelta(minutes=1),
        policy=policy,
    )

    assert len(window.rows) == 300
    assert len(payload.observations) == 258
    assert payload.observations[0].observed_at == window.rows[42].timestamp
    assert payload.observations[-1].observed_at == window.rows[-1].timestamp
    assert all(row.available_at == FACTOR_AVAILABLE_AT for row in payload.observations)
    assert len(payload.target_times) == 252
    sources = {source.name: source for source in payload.data_sources}
    assert set(sources) == {
        "polygon_dividends",
        "polygon_open_close",
        "polygon_splits",
        "stockapi_adjustment_factors",
    }
    assert sources["stockapi_adjustment_factors"].snapshot_id == (evidence.factor_set.factor_set_id)
    assert sources["stockapi_adjustment_factors"].max_available_at == (FACTOR_AVAILABLE_AT)
    assert sources["polygon_splits"].snapshot_id == SPLIT_COLLECTION_ID
    assert sources["polygon_dividends"].snapshot_id == DIVIDEND_COLLECTION_ID
    assert payload.series_basis == "split_dividend_adjusted"
    assert payload.target == "adjusted_close"


def test_short_stale_or_post_cutoff_factor_evidence_fails_closed() -> None:
    with pytest.raises(SnapshotInputUnavailable, match="at least 258"):
        verify_adjusted_snapshot_evidence(_evidence(count=257), _spec())

    # After the 2026-07-14 close, a factor anchored on 2026-07-13 is stale.
    with pytest.raises(SnapshotAvailabilityError, match="does not match"):
        verify_adjusted_snapshot_evidence(
            _evidence(),
            _spec(datetime(2026, 7, 14, 21, tzinfo=UTC)),
        )

    evidence = _evidence()
    late = replace(
        evidence,
        factor_set_receipt=replace(
            evidence.factor_set_receipt,
            available_at=SNAPSHOT_AS_OF + timedelta(seconds=1),
        ),
    )
    with pytest.raises(SnapshotAvailabilityError, match="does not match"):
        verify_adjusted_snapshot_evidence(late, _spec())


class _ScalarResult:
    def __init__(self, value: object) -> None:
        self.value = value

    def scalar_one(self) -> object:
        return self.value

    def scalar_one_or_none(self) -> object:
        return self.value


class _Transaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *args: object) -> None:
        return None


class _FakeSession:
    def __init__(self, results: Sequence[object]) -> None:
        self.results = list(results)
        self.added: list[object] = []

    async def __aenter__(self) -> _FakeSession:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    def begin(self) -> _Transaction:
        return _Transaction()

    async def execute(self, statement: object) -> _ScalarResult:
        del statement
        return _ScalarResult(self.results.pop(0))

    def add(self, row: object) -> None:
        self.added.append(row)

    async def flush(self) -> None:
        return None

    async def refresh(self, row: object) -> None:
        del row


class _FakeSessionmaker:
    def __init__(self, sessions: Sequence[_FakeSession]) -> None:
        self.sessions = list(sessions)

    def __call__(self) -> _FakeSession:
        return self.sessions.pop(0)


def _as_sessionmaker(
    value: _FakeSessionmaker,
) -> async_sessionmaker[AsyncSession]:
    return cast(async_sessionmaker[AsyncSession], value)


@pytest.mark.asyncio
async def test_builder_creates_replays_and_rejects_conflicting_immutable_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = _evidence()

    async def fake_load(session: object, factor_set_id: str) -> LoadedAdjustedPriceEvidence:
        del session
        assert factor_set_id == evidence.factor_set.factor_set_id
        return evidence

    async def fake_lock(session: object, lock_id: int) -> None:
        del session, lock_id

    monkeypatch.setattr(builder_module, "load_adjusted_price_evidence", fake_load)
    monkeypatch.setattr(builder_module, "acquire_advisory_xact_lock", fake_lock)

    read = _FakeSession([SNAPSHOT_AS_OF + timedelta(hours=1), evidence.factor_set.factor_set_id])
    write = _FakeSession([SNAPSHOT_AS_OF + timedelta(hours=1), None])
    maker = _FakeSessionmaker([read, write])
    created = await AdjustedForecastSnapshotBuilder(_as_sessionmaker(maker)).build(_spec())

    assert created.created is True
    assert created.observation_count == 258
    assert len(write.added) == 1
    persisted = write.added[0]
    assert isinstance(persisted, ForecastInputSnapshot)

    replay_read = _FakeSession(
        [SNAPSHOT_AS_OF + timedelta(hours=2), evidence.factor_set.factor_set_id]
    )
    replay_write = _FakeSession([SNAPSHOT_AS_OF + timedelta(hours=2), persisted])
    replayed = await AdjustedForecastSnapshotBuilder(
        _as_sessionmaker(_FakeSessionmaker([replay_read, replay_write]))
    ).build(_spec())
    assert replayed.created is False
    assert replayed.snapshot_id == created.snapshot_id
    assert replay_write.added == []

    persisted.canonical_payload += b" "
    conflict_read = _FakeSession(
        [SNAPSHOT_AS_OF + timedelta(hours=3), evidence.factor_set.factor_set_id]
    )
    conflict_write = _FakeSession([SNAPSHOT_AS_OF + timedelta(hours=3), persisted])
    with pytest.raises(SnapshotSemanticConflict, match="invalid"):
        await AdjustedForecastSnapshotBuilder(
            _as_sessionmaker(_FakeSessionmaker([conflict_read, conflict_write]))
        ).build(_spec())
