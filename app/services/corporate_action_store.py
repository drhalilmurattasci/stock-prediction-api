"""Two-phase persistence for complete corporate-action collections."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal

from sqlalchemy import ARRAY, LargeBinary, bindparam, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.db.models.corporate_actions import (
    CorporateActionCollection,
    CorporateActionCollectionAvailability,
    CorporateActionCollectionMember,
    CorporateActionVersion,
)
from app.services.corporate_actions import CorporateActionCollectionRecord


class CorporateActionStoreError(RuntimeError):
    """A collection could not be durably published without ambiguity."""


class CorporateActionStoreConflict(CorporateActionStoreError):
    """Stored content conflicts with the requested content identity."""


class CorporateActionStoreOutcomeUnknown(CorporateActionStoreError):
    """A database failure left commit visibility genuinely unknown."""


@dataclass(frozen=True, slots=True)
class PublishedCorporateActionCollection:
    """Post-commit proof for one exact complete collection."""

    collection_id: str
    collection_recorded_at: datetime
    available_at: datetime
    event_count: int


@dataclass(frozen=True, slots=True)
class CorporateActionCollectionEvidence:
    """One exact-scope collection, with an optional later availability receipt."""

    collection_id: str
    collection_recorded_at: datetime
    fetched_at: datetime
    event_count: int
    available_at: datetime | None


@dataclass(frozen=True, slots=True)
class CorporateActionScopeCoverage:
    """All immutable collections for one exact query scope."""

    action_type: Literal["split", "dividend"]
    endpoint: str
    source: str
    symbol: str
    coverage_start: date
    coverage_end: date
    query_policy_hash: str
    collections: tuple[CorporateActionCollectionEvidence, ...]

    @property
    def complete(self) -> tuple[CorporateActionCollectionEvidence, ...]:
        return tuple(value for value in self.collections if value.available_at is not None)

    @property
    def repairable(self) -> tuple[CorporateActionCollectionEvidence, ...]:
        return tuple(value for value in self.collections if value.available_at is None)

    @property
    def newest_complete(self) -> CorporateActionCollectionEvidence | None:
        complete = self.complete
        return complete[-1] if complete else None


_PUBLISH_CONTENT = text(
    "SELECT public.publish_corporate_action_collection(:manifest, :events)"
).bindparams(
    bindparam("manifest", type_=LargeBinary()),
    bindparam("events", type_=ARRAY(LargeBinary())),
)
_PUBLISH_RECEIPT = text(
    "SELECT collection_id, collection_recorded_at, available_at "
    "FROM public.publish_corporate_action_collection_receipt(:collection_id)"
)
_DETERMINISTIC_PUBLISH_REJECTIONS = frozenset({"22007", "22023", "55000"})


class SqlCorporateActionCollectionStore:
    """Publish immutable collection content, then its later DB receipt."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._maker = async_sessionmaker(engine, expire_on_commit=False)

    async def publish(
        self,
        record: CorporateActionCollectionRecord,
    ) -> PublishedCorporateActionCollection:
        if not isinstance(record, CorporateActionCollectionRecord):
            raise TypeError("record must be a CorporateActionCollectionRecord")

        try:
            async with self._maker.begin() as session:
                returned_id = (
                    await session.execute(
                        _PUBLISH_CONTENT,
                        {
                            "manifest": record.canonical_manifest,
                            "events": [member.canonical_event for member in record.members],
                        },
                    )
                ).scalar_one()
                if returned_id != record.collection_id:
                    raise CorporateActionStoreConflict(
                        "database publisher returned a different collection identity"
                    )
        except IntegrityError as exc:
            raise CorporateActionStoreConflict(
                "corporate-action content violates the database contract"
            ) from exc
        except DBAPIError as exc:
            if _sqlstate(exc) in _DETERMINISTIC_PUBLISH_REJECTIONS:
                raise CorporateActionStoreConflict(
                    "corporate-action content was rejected by the database contract"
                ) from exc
            await self._reconcile_content_or_raise(record, exc)

        try:
            await self._require_content_match(record)
        except DBAPIError as exc:
            raise CorporateActionStoreOutcomeUnknown(
                "corporate-action content visibility is unknown after publication"
            ) from exc

        published = await self.publish_receipt(record.collection_id)
        if published is None:
            raise CorporateActionStoreOutcomeUnknown(
                "corporate-action receipt is not visible after commit"
            )
        if published.event_count != record.event_count:
            raise CorporateActionStoreConflict(
                "published corporate-action event count does not match"
            )
        return published

    async def publish_receipt(
        self,
        collection_id: str,
    ) -> PublishedCorporateActionCollection:
        """Publish or replay only the later receipt for committed content."""

        if not isinstance(collection_id, str) or not collection_id:
            raise TypeError("collection_id must be non-empty text")
        try:
            async with self._maker.begin() as session:
                receipt = (
                    await session.execute(
                        _PUBLISH_RECEIPT,
                        {"collection_id": collection_id},
                    )
                ).one()
                if receipt.collection_id != collection_id:
                    raise CorporateActionStoreConflict(
                        "database receipt publisher returned a different identity"
                    )
        except IntegrityError as exc:
            raise CorporateActionStoreConflict(
                "corporate-action receipt violates the database contract"
            ) from exc
        except DBAPIError as exc:
            if _sqlstate(exc) in _DETERMINISTIC_PUBLISH_REJECTIONS:
                raise CorporateActionStoreConflict(
                    "corporate-action receipt was rejected by the database contract"
                ) from exc
            reconciled = await self._reconcile_receipt(collection_id)
            if reconciled is None:
                raise CorporateActionStoreOutcomeUnknown(
                    "corporate-action receipt commit outcome is unknown"
                ) from exc

        published = await self._reconcile_receipt(collection_id)
        if published is None:
            raise CorporateActionStoreOutcomeUnknown(
                "corporate-action receipt is not visible after commit"
            )
        return published

    async def scope_coverage(
        self,
        *,
        action_type: Literal["split", "dividend"],
        endpoint: str,
        source: str,
        symbol: str,
        coverage_start: date,
        coverage_end: date,
        query_policy_hash: str,
    ) -> CorporateActionScopeCoverage:
        """Read every collection for one exact, policy-bound query scope."""

        statement = (
            select(
                CorporateActionCollection.collection_id,
                CorporateActionCollection.recorded_at,
                CorporateActionCollection.fetched_at,
                CorporateActionCollection.event_count,
                CorporateActionCollectionAvailability.available_at,
            )
            .outerjoin(
                CorporateActionCollectionAvailability,
                CorporateActionCollectionAvailability.collection_id
                == CorporateActionCollection.collection_id,
            )
            .where(
                CorporateActionCollection.action_type == action_type,
                CorporateActionCollection.endpoint == endpoint,
                CorporateActionCollection.source == source,
                CorporateActionCollection.symbol == symbol,
                CorporateActionCollection.coverage_start == coverage_start,
                CorporateActionCollection.coverage_end == coverage_end,
                CorporateActionCollection.query_policy_hash == query_policy_hash,
            )
            .order_by(
                CorporateActionCollection.recorded_at,
                CorporateActionCollection.collection_id,
            )
        )
        async with self._maker() as session:
            rows = (await session.execute(statement)).all()
        return CorporateActionScopeCoverage(
            action_type=action_type,
            endpoint=endpoint,
            source=source,
            symbol=symbol,
            coverage_start=coverage_start,
            coverage_end=coverage_end,
            query_policy_hash=query_policy_hash,
            collections=tuple(
                CorporateActionCollectionEvidence(
                    collection_id=row.collection_id,
                    collection_recorded_at=row.recorded_at,
                    fetched_at=row.fetched_at,
                    event_count=row.event_count,
                    available_at=row.available_at,
                )
                for row in rows
            ),
        )

    async def get(self, collection_id: str) -> PublishedCorporateActionCollection | None:
        async with self._maker() as session:
            row = (
                await session.execute(
                    select(
                        CorporateActionCollection.collection_id,
                        CorporateActionCollection.recorded_at,
                        CorporateActionCollection.event_count,
                        CorporateActionCollectionAvailability.available_at,
                    )
                    .join(
                        CorporateActionCollectionAvailability,
                        CorporateActionCollectionAvailability.collection_id
                        == CorporateActionCollection.collection_id,
                    )
                    .where(CorporateActionCollection.collection_id == collection_id)
                )
            ).one_or_none()
            if row is None:
                return None
            return PublishedCorporateActionCollection(
                collection_id=row.collection_id,
                collection_recorded_at=row.recorded_at,
                available_at=row.available_at,
                event_count=row.event_count,
            )

    async def _reconcile_content_or_raise(
        self,
        record: CorporateActionCollectionRecord,
        original: DBAPIError,
    ) -> None:
        try:
            await self._require_content_match(record)
        except CorporateActionStoreConflict as exc:
            if str(exc) == "corporate-action collection is absent":
                raise CorporateActionStoreOutcomeUnknown(
                    "corporate-action content commit outcome is unknown"
                ) from original
            raise exc from original
        except DBAPIError as exc:
            raise CorporateActionStoreOutcomeUnknown(
                "corporate-action content commit outcome is unknown"
            ) from exc

    async def _reconcile_receipt(
        self,
        collection_id: str,
    ) -> PublishedCorporateActionCollection | None:
        try:
            return await self.get(collection_id)
        except DBAPIError:
            return None

    async def _require_content_match(
        self,
        record: CorporateActionCollectionRecord,
    ) -> None:
        async with self._maker() as session:
            await _require_collection_match(session, record)


async def _require_collection_match(
    session: AsyncSession,
    record: CorporateActionCollectionRecord,
) -> None:
    row = (
        await session.execute(
            select(CorporateActionCollection).where(
                CorporateActionCollection.collection_id == record.collection_id
            )
        )
    ).scalar_one_or_none()
    if row is None:
        raise CorporateActionStoreConflict("corporate-action collection is absent")
    if not (
        row.schema_version == record.schema_version
        and row.query_policy_hash == record.query_policy_hash
        and row.source == record.source
        and row.endpoint == record.endpoint
        and row.action_type == record.action_type
        and row.symbol == record.symbol
        and row.coverage_start == record.coverage_start
        and row.coverage_end == record.coverage_end
        and row.page_limit == record.page_limit
        and row.page_count == record.page_count
        and row.event_count == record.event_count
        and row.pagination_exhausted is record.pagination_exhausted
        and row.provider_request_id == record.provider_request_id
        and row.fetched_at == record.fetched_at
        and row.canonical_manifest == record.canonical_manifest
    ):
        raise CorporateActionStoreConflict(
            "stored corporate-action collection does not match its content identity"
        )
    stored_members = (
        await session.execute(
            select(CorporateActionCollectionMember.ordinal, CorporateActionVersion)
            .join(
                CorporateActionVersion,
                CorporateActionVersion.action_version_id
                == CorporateActionCollectionMember.action_version_id,
            )
            .where(CorporateActionCollectionMember.collection_id == record.collection_id)
            .order_by(CorporateActionCollectionMember.ordinal)
        )
    ).all()
    if len(stored_members) != len(record.members):
        raise CorporateActionStoreConflict(
            "stored corporate-action collection membership does not match"
        )
    for ordinal, (stored_row, expected) in enumerate(
        zip(stored_members, record.members, strict=True)
    ):
        stored_ordinal, stored = stored_row
        if not (
            stored_ordinal == ordinal
            and stored.action_version_id == expected.action_version_id
            and stored.schema_version == expected.schema_version
            and stored.source == expected.source
            and stored.action_type == expected.action_type
            and stored.provider_event_id == expected.provider_event_id
            and stored.symbol == expected.symbol
            and stored.effective_date == expected.effective_date
            and stored.status == expected.status
            and stored.split_from == expected.split_from
            and stored.split_to == expected.split_to
            and stored.adjustment_type == expected.adjustment_type
            and stored.cash_amount == expected.cash_amount
            and stored.split_adjusted_cash_amount == expected.split_adjusted_cash_amount
            and stored.currency == expected.currency
            and stored.declaration_date == expected.declaration_date
            and stored.record_date == expected.record_date
            and stored.pay_date == expected.pay_date
            and stored.frequency == expected.frequency
            and stored.distribution_type == expected.distribution_type
            and stored.historical_adjustment_factor == expected.historical_adjustment_factor
            and stored.canonical_event == expected.canonical_event
        ):
            raise CorporateActionStoreConflict(
                "stored corporate-action version projection does not match canonical content"
            )


def _sqlstate(exc: DBAPIError) -> str | None:
    for name in ("sqlstate", "pgcode"):
        value = getattr(exc.orig, name, None)
        if isinstance(value, str):
            return value
    return None


__all__ = [
    "CorporateActionCollectionEvidence",
    "CorporateActionScopeCoverage",
    "CorporateActionStoreConflict",
    "CorporateActionStoreError",
    "CorporateActionStoreOutcomeUnknown",
    "PublishedCorporateActionCollection",
    "SqlCorporateActionCollectionStore",
]
