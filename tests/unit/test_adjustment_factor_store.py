"""Unit proofs for two-phase adjustment-factor persistence and reconciliation."""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import DBAPIError

from app.db.models.adjustment_factors import (
    AdjustmentFactorEntry,
    AdjustmentFactorSetRecord,
)
from app.services.adjustment_factor_store import (
    AdjustmentFactorStoreConflict,
    AdjustmentFactorStoreOutcomeUnknown,
    PublishedAdjustmentFactorSet,
    SqlAdjustmentFactorSetStore,
    _require_factor_set_match,
)
from app.services.adjustment_factors import RawCloseVersion, build_adjustment_factor_set


def _artifact():
    observed_at = datetime(2026, 7, 10, 20, tzinfo=UTC)
    return build_adjustment_factor_set(
        symbol="MSFT",
        cutoff=datetime(2026, 7, 10, 21, tzinfo=UTC),
        raw_closes=(
            RawCloseVersion(
                observation_date=date(2026, 7, 10),
                observed_at=observed_at,
                timespan="day",
                multiplier=1,
                source="polygon_open_close",
                adjustment_basis="raw",
                version_recorded_at=observed_at + timedelta(minutes=5),
                available_at=observed_at + timedelta(minutes=6),
                close=Decimal("52"),
            ),
        ),
        split_collection_id="sha256:" + "1" * 64,
        splits=(),
        dividend_collection_id="sha256:" + "2" * 64,
        dividends=(),
    )


def _stored_projection():
    artifact = _artifact()
    document = json.loads(artifact.canonical_payload)
    raw = document["raw_inputs"][0]
    factor = document["factors"][0]
    split_available = datetime(2026, 7, 10, 20, 7, tzinfo=UTC)
    dividend_available = datetime(2026, 7, 10, 20, 8, tzinfo=UTC)
    header = AdjustmentFactorSetRecord(
        factor_set_id=artifact.factor_set_id,
        format=document["format"],
        policy_version=artifact.policy_version,
        policy_hash=artifact.policy_hash,
        symbol=artifact.symbol,
        cutoff=artifact.cutoff,
        anchor_date=artifact.anchor_date,
        coverage_start=artifact.raw_inputs[0].observation_date,
        coverage_end=artifact.raw_inputs[-1].observation_date,
        input_count=1,
        max_input_available_at=dividend_available,
        split_collection_id=artifact.split_collection_id,
        split_collection_recorded_at=split_available - timedelta(minutes=1),
        split_collection_available_at=split_available,
        dividend_collection_id=artifact.dividend_collection_id,
        dividend_collection_recorded_at=dividend_available - timedelta(minutes=1),
        dividend_collection_available_at=dividend_available,
        canonical_payload=artifact.canonical_payload,
        creator_xid=11,
    )
    entry = AdjustmentFactorEntry(
        factor_set_id=artifact.factor_set_id,
        ordinal=0,
        symbol=artifact.symbol,
        observation_date=date.fromisoformat(raw["observation_date"]),
        observed_at=datetime.fromisoformat(raw["observed_at"].replace("Z", "+00:00")),
        timespan=raw["timespan"],
        multiplier=raw["multiplier"],
        source=raw["source"],
        adjustment_basis=raw["adjustment_basis"],
        version_recorded_at=datetime.fromisoformat(
            raw["version_recorded_at"].replace("Z", "+00:00")
        ),
        raw_available_at=datetime.fromisoformat(raw["available_at"].replace("Z", "+00:00")),
        raw_close_decimal=raw["close_decimal"],
        raw_close_f64_be=bytes.fromhex(raw["close_f64_be"]),
        price_factor_decimal=factor["price_factor_decimal"],
        price_factor_f64_be=bytes.fromhex(factor["price_factor_f64_be"]),
        volume_factor_decimal=factor["volume_factor_decimal"],
        volume_factor_f64_be=bytes.fromhex(factor["volume_factor_f64_be"]),
        creator_xid=11,
    )
    return artifact, header, entry


class _Result:
    def __init__(self, value: object) -> None:
        self.value = value

    def scalar_one(self) -> object:
        return self.value

    def scalar_one_or_none(self) -> object:
        return self.value

    def one(self) -> object:
        return self.value

    def one_or_none(self) -> object:
        return self.value

    def scalars(self) -> _Result:
        return self

    def all(self) -> list[object]:
        return cast(list[object], self.value)


class _Session:
    def __init__(self, outcomes: list[object]) -> None:
        self.outcomes: Iterator[object] = iter(outcomes)
        self.calls: list[tuple[object, object | None]] = []

    async def execute(self, statement: object, params: object | None = None) -> _Result:
        self.calls.append((statement, params))
        outcome = next(self.outcomes)
        if isinstance(outcome, BaseException):
            raise outcome
        return _Result(outcome)


class _Context:
    def __init__(self, session: _Session) -> None:
        self.session = session

    async def __aenter__(self) -> _Session:
        return self.session

    async def __aexit__(self, *exc_info: object) -> None:
        return None


class _Maker:
    def __init__(self, begin_sessions: list[_Session]) -> None:
        self.begin_sessions: Iterator[_Session] = iter(begin_sessions)

    def begin(self) -> _Context:
        return _Context(next(self.begin_sessions))


class _DriverRejection(RuntimeError):
    def __init__(self, sqlstate: str) -> None:
        super().__init__(sqlstate)
        self.sqlstate = sqlstate


@pytest.mark.asyncio
async def test_reconciliation_checks_header_and_every_byte_derived_entry() -> None:
    artifact, header, entry = _stored_projection()
    session = _Session([header, [entry]])

    await _require_factor_set_match(cast(Any, session), artifact)

    assert len(session.calls) == 2


@pytest.mark.asyncio
async def test_reconciliation_rejects_projection_drift() -> None:
    artifact, header, entry = _stored_projection()
    entry.price_factor_decimal = "0.5"
    session = _Session([header, [entry]])

    with pytest.raises(AdjustmentFactorStoreConflict, match="canonical bytes"):
        await _require_factor_set_match(cast(Any, session), artifact)


@pytest.mark.asyncio
async def test_publish_uses_separate_content_and_receipt_transactions() -> None:
    artifact = _artifact()
    content = _Session([artifact.factor_set_id])
    receipt = _Session(
        [
            SimpleNamespace(
                factor_set_id=artifact.factor_set_id,
                factor_set_recorded_at=datetime(2026, 7, 10, 21, 1, tzinfo=UTC),
                available_at=datetime(2026, 7, 10, 21, 2, tzinfo=UTC),
            )
        ]
    )
    published = PublishedAdjustmentFactorSet(
        factor_set_id=artifact.factor_set_id,
        factor_set_recorded_at=datetime(2026, 7, 10, 21, 1, tzinfo=UTC),
        available_at=datetime(2026, 7, 10, 21, 2, tzinfo=UTC),
        input_count=1,
        max_input_available_at=datetime(2026, 7, 10, 20, 8, tzinfo=UTC),
    )
    store = object.__new__(SqlAdjustmentFactorSetStore)
    cast(Any, store)._maker = _Maker([content, receipt])
    cast(Any, store)._require_content_match = AsyncMock()
    cast(Any, store)._reconcile_receipt = AsyncMock(return_value=published)

    result = await store.publish(artifact)

    assert result == published
    assert "publish_adjustment_factor_set" in str(content.calls[0][0])
    assert content.calls[0][1] == {"payload": artifact.canonical_payload}
    assert "publish_adjustment_factor_set_receipt" in str(receipt.calls[0][0])
    assert receipt.calls[0][1] == {"factor_set_id": artifact.factor_set_id}


@pytest.mark.asyncio
async def test_receipt_unknown_commit_replays_visible_receipt() -> None:
    artifact = _artifact()
    failure = DBAPIError("receipt", {}, RuntimeError("connection lost"))
    content = _Session([artifact.factor_set_id])
    receipt = _Session([failure])
    published = PublishedAdjustmentFactorSet(
        factor_set_id=artifact.factor_set_id,
        factor_set_recorded_at=datetime(2026, 7, 10, 21, 1, tzinfo=UTC),
        available_at=datetime(2026, 7, 10, 21, 2, tzinfo=UTC),
        input_count=1,
        max_input_available_at=datetime(2026, 7, 10, 20, 8, tzinfo=UTC),
    )
    store = object.__new__(SqlAdjustmentFactorSetStore)
    cast(Any, store)._maker = _Maker([content, receipt])
    cast(Any, store)._require_content_match = AsyncMock()
    cast(Any, store)._reconcile_receipt = AsyncMock(return_value=published)

    assert await store.publish(artifact) == published


@pytest.mark.asyncio
@pytest.mark.parametrize("sqlstate", ["22007", "22023", "55000"])
async def test_deterministic_content_rejection_is_not_misreported_as_unknown(
    sqlstate: str,
) -> None:
    artifact = _artifact()
    rejection = DBAPIError("content", {}, _DriverRejection(sqlstate))
    store = object.__new__(SqlAdjustmentFactorSetStore)
    cast(Any, store)._maker = _Maker([_Session([rejection])])
    cast(Any, store)._require_content_match = AsyncMock()

    with pytest.raises(AdjustmentFactorStoreConflict, match="rejected"):
        await store.publish(artifact)

    cast(Any, store)._require_content_match.assert_not_awaited()


@pytest.mark.asyncio
async def test_receipt_unknown_commit_is_reported_when_not_reconcilable() -> None:
    artifact = _artifact()
    failure = DBAPIError("receipt", {}, RuntimeError("connection lost"))
    store = object.__new__(SqlAdjustmentFactorSetStore)
    cast(Any, store)._maker = _Maker([_Session([artifact.factor_set_id]), _Session([failure])])
    cast(Any, store)._require_content_match = AsyncMock()
    cast(Any, store)._reconcile_receipt = AsyncMock(return_value=None)

    with pytest.raises(AdjustmentFactorStoreOutcomeUnknown, match="commit outcome"):
        await store.publish(artifact)


@pytest.mark.asyncio
async def test_content_unknown_commit_reconciles_exact_visible_content() -> None:
    artifact = _artifact()
    original = DBAPIError("content", {}, RuntimeError("connection lost"))
    store = object.__new__(SqlAdjustmentFactorSetStore)
    cast(Any, store)._require_content_match = AsyncMock()

    await store._reconcile_content_or_raise(artifact, original)


@pytest.mark.asyncio
async def test_content_unknown_commit_stays_unknown_when_content_is_absent() -> None:
    artifact = _artifact()
    original = DBAPIError("content", {}, RuntimeError("connection lost"))
    store = object.__new__(SqlAdjustmentFactorSetStore)
    cast(Any, store)._require_content_match = AsyncMock(
        side_effect=AdjustmentFactorStoreConflict("adjustment-factor set is absent")
    )

    with pytest.raises(AdjustmentFactorStoreOutcomeUnknown, match="commit outcome"):
        await store._reconcile_content_or_raise(artifact, original)
