"""Unit proofs for corporate-action persistence reconciliation and taxonomy."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import DBAPIError

from app.db.models.corporate_actions import (
    CorporateActionCollection,
    CorporateActionVersion,
)
from app.services.corporate_action_store import (
    CorporateActionStoreConflict,
    CorporateActionStoreOutcomeUnknown,
    SqlCorporateActionCollectionStore,
    _require_collection_match,
)
from app.services.corporate_actions import (
    CORPORATE_ACTION_ORIGIN,
    SPLITS_ENDPOINT,
    build_split_collection,
)
from data_sources.base import Split, SplitPage


def _record():
    fetched_at = datetime(2026, 7, 14, 18, tzinfo=UTC)
    return build_split_collection(
        SplitPage(
            provider_request_id="split-request-1",
            provider_origin=CORPORATE_ACTION_ORIGIN,
            endpoint=SPLITS_ENDPOINT,
            symbol="MSFT",
            start=date(2026, 1, 1),
            end=date(2026, 7, 13),
            source="polygon",
            fetched_at=fetched_at,
            results=(
                Split(
                    provider_event_id="split-1",
                    symbol="MSFT",
                    execution_date=date(2026, 4, 15),
                    split_from=Decimal("1"),
                    split_to=Decimal("2"),
                    adjustment_type="forward_split",
                    historical_adjustment_factor=Decimal("0.5"),
                    source="polygon",
                    fetched_at=fetched_at,
                ),
            ),
        )
    )


def _stored_projection():
    record = _record()
    expected = record.members[0]
    collection = CorporateActionCollection(
        collection_id=record.collection_id,
        schema_version=record.schema_version,
        query_policy_hash=record.query_policy_hash,
        source=record.source,
        endpoint=record.endpoint,
        action_type=record.action_type,
        symbol=record.symbol,
        coverage_start=record.coverage_start,
        coverage_end=record.coverage_end,
        page_limit=record.page_limit,
        page_count=record.page_count,
        event_count=record.event_count,
        pagination_exhausted=record.pagination_exhausted,
        provider_request_id=record.provider_request_id,
        fetched_at=record.fetched_at,
        canonical_manifest=record.canonical_manifest,
        creator_xid=11,
    )
    version = CorporateActionVersion(
        action_version_id=expected.action_version_id,
        schema_version=expected.schema_version,
        source=expected.source,
        action_type=expected.action_type,
        provider_event_id=expected.provider_event_id,
        symbol=expected.symbol,
        effective_date=expected.effective_date,
        status=expected.status,
        split_from=expected.split_from,
        split_to=expected.split_to,
        adjustment_type=expected.adjustment_type,
        cash_amount=expected.cash_amount,
        split_adjusted_cash_amount=expected.split_adjusted_cash_amount,
        currency=expected.currency,
        declaration_date=expected.declaration_date,
        record_date=expected.record_date,
        pay_date=expected.pay_date,
        frequency=expected.frequency,
        distribution_type=expected.distribution_type,
        historical_adjustment_factor=expected.historical_adjustment_factor,
        canonical_event=expected.canonical_event,
        creator_xid=11,
    )
    return record, collection, version


class _Result:
    def __init__(self, value: object) -> None:
        self.value = value

    def scalar_one(self) -> object:
        return self.value

    def scalar_one_or_none(self) -> object:
        return self.value

    def one(self) -> object:
        return self.value

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
    def __init__(self, sessions: list[_Session]) -> None:
        self.sessions: Iterator[_Session] = iter(sessions)

    def begin(self) -> _Context:
        return _Context(next(self.sessions))

    def __call__(self) -> _Context:
        return _Context(next(self.sessions))


class _DriverRejection(RuntimeError):
    def __init__(self, sqlstate: str) -> None:
        super().__init__(sqlstate)
        self.sqlstate = sqlstate


@pytest.mark.asyncio
async def test_reconciliation_checks_every_version_projection() -> None:
    record, collection, version = _stored_projection()
    session = _Session([collection, [(0, version)]])

    await _require_collection_match(cast(Any, session), record)

    version.split_to = Decimal("3")
    drifted = _Session([collection, [(0, version)]])
    with pytest.raises(CorporateActionStoreConflict, match="version projection"):
        await _require_collection_match(cast(Any, drifted), record)


@pytest.mark.asyncio
@pytest.mark.parametrize("sqlstate", ["22007", "22023", "55000"])
async def test_deterministic_publisher_rejection_is_a_conflict(sqlstate: str) -> None:
    record = _record()
    rejection = DBAPIError("publish", {}, _DriverRejection(sqlstate))
    store = object.__new__(SqlCorporateActionCollectionStore)
    cast(Any, store)._maker = _Maker([_Session([rejection])])
    cast(Any, store)._require_content_match = AsyncMock()

    with pytest.raises(CorporateActionStoreConflict, match="rejected"):
        await store.publish(record)

    cast(Any, store)._require_content_match.assert_not_awaited()


@pytest.mark.asyncio
async def test_ambiguous_content_commit_absence_stays_unknown() -> None:
    record = _record()
    failure = DBAPIError("publish", {}, RuntimeError("connection lost"))
    store = object.__new__(SqlCorporateActionCollectionStore)
    cast(Any, store)._require_content_match = AsyncMock(
        side_effect=CorporateActionStoreConflict("corporate-action collection is absent")
    )

    with pytest.raises(CorporateActionStoreOutcomeUnknown, match="commit outcome"):
        await store._reconcile_content_or_raise(record, failure)


@pytest.mark.asyncio
async def test_post_commit_visibility_failure_stays_unknown() -> None:
    record = _record()
    store = object.__new__(SqlCorporateActionCollectionStore)
    cast(Any, store)._maker = _Maker([_Session([record.collection_id])])
    cast(Any, store)._require_content_match = AsyncMock(
        side_effect=DBAPIError("read", {}, RuntimeError("connection lost"))
    )

    with pytest.raises(CorporateActionStoreOutcomeUnknown, match="visibility"):
        await store.publish(record)


@pytest.mark.asyncio
async def test_scope_order_uses_db_recording_not_provider_clock() -> None:
    session = _Session([[]])
    store = object.__new__(SqlCorporateActionCollectionStore)
    cast(Any, store)._maker = _Maker([session])

    await store.scope_coverage(
        action_type="split",
        endpoint=SPLITS_ENDPOINT,
        source="polygon",
        symbol="MSFT",
        coverage_start=date(2026, 1, 1),
        coverage_end=date(2026, 7, 13),
        query_policy_hash=_record().query_policy_hash,
    )

    sql = str(session.calls[0][0])
    order = sql.split(" ORDER BY ", 1)[1]
    assert "corporate_action_collections.recorded_at" in order
    assert "corporate_action_collections.collection_id" in order
    assert "fetched_at" not in order


def test_newest_complete_is_last_db_recorded_collection_not_latest_host_clock() -> None:
    from app.services.corporate_action_store import (  # local: keeps fixture imports compact
        CorporateActionCollectionEvidence,
        CorporateActionScopeCoverage,
    )

    earlier_db = CorporateActionCollectionEvidence(
        collection_id="sha256:" + "1" * 64,
        collection_recorded_at=datetime(2026, 7, 14, 18, tzinfo=UTC),
        fetched_at=datetime(2026, 7, 14, 20, tzinfo=UTC),
        event_count=0,
        available_at=datetime(2026, 7, 14, 18, 1, tzinfo=UTC),
    )
    later_db = CorporateActionCollectionEvidence(
        collection_id="sha256:" + "2" * 64,
        collection_recorded_at=datetime(2026, 7, 14, 19, tzinfo=UTC),
        fetched_at=datetime(2026, 7, 14, 17, tzinfo=UTC),
        event_count=0,
        available_at=datetime(2026, 7, 14, 19, 1, tzinfo=UTC),
    )
    coverage = CorporateActionScopeCoverage(
        action_type="split",
        endpoint=SPLITS_ENDPOINT,
        source="polygon",
        symbol="MSFT",
        coverage_start=date(2026, 1, 1),
        coverage_end=date(2026, 7, 13),
        query_policy_hash=_record().query_policy_hash,
        collections=(earlier_db, later_db),
    )

    assert coverage.newest_complete == later_db
