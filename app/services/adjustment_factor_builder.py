"""Read-only resolution and privileged publication of adjustment factors.

The resolver takes one explicit database cutoff and one exact corporate-action
coverage window.  It closes its read session before doing Decimal arithmetic
or opening the publisher's short transactions.  The database publisher then
revalidates every selected receipt under series locks, so a concurrent bar or
action correction cannot turn a stale calculation into accepted evidence.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Literal, Protocol, cast

from sqlalchemy import Select, and_, func, literal, select, union
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models.bars import Bar, BarRevision, BarVersionAvailability
from app.db.models.corporate_actions import (
    CorporateActionCollection,
    CorporateActionCollectionAvailability,
    CorporateActionCollectionMember,
    CorporateActionVersion,
)
from app.services.adjustment_factor_store import PublishedAdjustmentFactorSet
from app.services.adjustment_factors import (
    AdjustmentFactorError,
    AdjustmentFactorSet,
    DividendActionVersion,
    RawCloseVersion,
    SplitActionVersion,
    build_adjustment_factor_set,
)
from app.services.corporate_actions import (
    CORPORATE_ACTION_QUERY_POLICY_HASH,
    CORPORATE_ACTION_SOURCE,
    DIVIDENDS_ENDPOINT,
    SPLITS_ENDPOINT,
)
from data_sources.base import DividendDistributionType, SplitAdjustmentType

_RAW_SOURCE = "polygon_open_close"
_RAW_TIMESPAN = "day"
_RAW_MULTIPLIER = 1
_RAW_BASIS = "raw"
_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9.\-_:]+$")
_CALENDAR_START = date(1990, 1, 1)
_CALENDAR_END = date(2100, 12, 31)


class AdjustmentFactorBuildError(RuntimeError):
    """The requested evidence window cannot produce one verified factor set."""


@dataclass(frozen=True, slots=True)
class AdjustmentFactorBuildSpec:
    """One explicit point-in-time factor-set selection."""

    symbol: str
    coverage_start: date
    coverage_end: date
    cutoff: datetime


@dataclass(frozen=True, slots=True)
class AdjustmentFactorBuildResult:
    """Identity and receipt for one published factor artifact."""

    artifact: AdjustmentFactorSet
    publication: PublishedAdjustmentFactorSet


@dataclass(frozen=True, slots=True)
class _SelectedCollection:
    collection_id: str
    recorded_at: datetime
    available_at: datetime
    event_count: int
    versions: tuple[CorporateActionVersion, ...]


class AdjustmentFactorPublisher(Protocol):
    async def publish(self, artifact: AdjustmentFactorSet) -> PublishedAdjustmentFactorSet: ...


def build_factor_raw_inputs_statement(spec: AdjustmentFactorBuildSpec) -> Select[Any]:
    """Newest exact receipted raw close at the cutoff for every stored session."""

    normalized = _validate_spec(spec)
    start = datetime.combine(normalized.coverage_start, time.min, tzinfo=UTC)
    stop = datetime.combine(
        normalized.coverage_end + timedelta(days=1),
        time.min,
        tzinfo=UTC,
    )
    current_filters = (
        Bar.symbol == normalized.symbol,
        Bar.timespan == _RAW_TIMESPAN,
        Bar.multiplier == _RAW_MULTIPLIER,
        Bar.source == _RAW_SOURCE,
        Bar.adjustment_basis == _RAW_BASIS,
        Bar.ts >= start,
        Bar.ts < stop,
    )
    revision_filters = (
        BarRevision.symbol == normalized.symbol,
        BarRevision.timespan == _RAW_TIMESPAN,
        BarRevision.multiplier == _RAW_MULTIPLIER,
        BarRevision.source == _RAW_SOURCE,
        BarRevision.adjustment_basis == _RAW_BASIS,
        BarRevision.ts >= start,
        BarRevision.ts < stop,
    )
    current = select(
        Bar.symbol.label("symbol"),
        Bar.timespan.label("timespan"),
        Bar.multiplier.label("multiplier"),
        Bar.ts.label("observed_at"),
        Bar.source.label("source"),
        Bar.adjustment_basis.label("adjustment_basis"),
        Bar.recorded_at.label("version_recorded_at"),
        Bar.close.label("close"),
    ).where(*current_filters)
    previous = select(
        BarRevision.symbol,
        BarRevision.timespan,
        BarRevision.multiplier,
        BarRevision.ts.label("observed_at"),
        BarRevision.source,
        BarRevision.adjustment_basis,
        BarRevision.previous_recorded_at.label("version_recorded_at"),
        BarRevision.previous_close.label("close"),
    ).where(*revision_filters, BarRevision.previous_recorded_at.is_not(None))
    incoming = select(
        BarRevision.symbol,
        BarRevision.timespan,
        BarRevision.multiplier,
        BarRevision.ts.label("observed_at"),
        BarRevision.source,
        BarRevision.adjustment_basis,
        BarRevision.incoming_recorded_at.label("version_recorded_at"),
        BarRevision.incoming_close.label("close"),
    ).where(*revision_filters, BarRevision.incoming_recorded_at.is_not(None))
    versions = union(current, previous, incoming).subquery("factor_stored_bar_versions")
    receipt = BarVersionAvailability
    visible = (
        select(
            *versions.c,
            receipt.available_at.label("available_at"),
            func.count()
            .over(partition_by=(versions.c.observed_at, versions.c.version_recorded_at))
            .label("greatest_count"),
            func.row_number()
            .over(
                partition_by=versions.c.observed_at,
                order_by=(
                    versions.c.version_recorded_at.desc(),
                    receipt.available_at.desc(),
                ),
            )
            .label("version_rank"),
        )
        .join(
            receipt,
            and_(
                receipt.symbol == versions.c.symbol,
                receipt.timespan == versions.c.timespan,
                receipt.multiplier == versions.c.multiplier,
                receipt.ts == versions.c.observed_at,
                receipt.source == versions.c.source,
                receipt.adjustment_basis == versions.c.adjustment_basis,
                receipt.version_recorded_at == versions.c.version_recorded_at,
            ),
        )
        .where(receipt.available_at <= normalized.cutoff)
        .subquery("factor_visible_bar_versions")
    )
    return (
        select(
            visible.c.symbol,
            visible.c.timespan,
            visible.c.multiplier,
            visible.c.observed_at,
            visible.c.source,
            visible.c.adjustment_basis,
            visible.c.version_recorded_at,
            visible.c.available_at,
            visible.c.close,
            visible.c.greatest_count,
            literal(1).label("active_version_count"),
        )
        .where(visible.c.version_rank == 1)
        .order_by(visible.c.observed_at)
    )


@dataclass(frozen=True, slots=True)
class AdjustmentFactorBuilder:
    """Resolve exact evidence, calculate without a connection, then publish."""

    sessionmaker: async_sessionmaker[AsyncSession]
    publisher: AdjustmentFactorPublisher

    async def build(self, spec: AdjustmentFactorBuildSpec) -> AdjustmentFactorBuildResult:
        normalized = _validate_spec(spec)
        async with self.sessionmaker() as session:
            database_now = _utc(
                (await session.execute(select(func.clock_timestamp()))).scalar_one(),
                "database clock",
            )
            if normalized.cutoff > database_now:
                raise AdjustmentFactorBuildError("factor cutoff is later than the database clock")
            split = await _load_collection(session, normalized, "split")
            dividend = await _load_collection(session, normalized, "dividend")
            result = await session.execute(build_factor_raw_inputs_statement(normalized))
            raw_rows = tuple(result.mappings())

        raw = _raw_inputs(raw_rows, normalized)
        splits = _split_inputs(split, normalized)
        dividends = _dividend_inputs(dividend, normalized)
        try:
            artifact = build_adjustment_factor_set(
                symbol=normalized.symbol,
                cutoff=normalized.cutoff,
                raw_closes=raw,
                split_collection_id=split.collection_id,
                splits=splits,
                dividend_collection_id=dividend.collection_id,
                dividends=dividends,
            )
        except AdjustmentFactorError as exc:
            raise AdjustmentFactorBuildError(
                "resolved evidence violates the pinned adjustment policy"
            ) from exc
        publication = await self.publisher.publish(artifact)
        if publication.factor_set_id != artifact.factor_set_id:
            raise AdjustmentFactorBuildError("factor publisher returned a different identity")
        return AdjustmentFactorBuildResult(artifact=artifact, publication=publication)


async def _load_collection(
    session: AsyncSession,
    spec: AdjustmentFactorBuildSpec,
    action_type: Literal["split", "dividend"],
) -> _SelectedCollection:
    endpoint = SPLITS_ENDPOINT if action_type == "split" else DIVIDENDS_ENDPOINT
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
                and_(
                    CorporateActionCollectionAvailability.collection_id
                    == CorporateActionCollection.collection_id,
                    CorporateActionCollectionAvailability.collection_recorded_at
                    == CorporateActionCollection.recorded_at,
                ),
            )
            .where(
                CorporateActionCollection.source == CORPORATE_ACTION_SOURCE,
                CorporateActionCollection.symbol == spec.symbol,
                CorporateActionCollection.action_type == action_type,
                CorporateActionCollection.endpoint == endpoint,
                CorporateActionCollection.query_policy_hash == CORPORATE_ACTION_QUERY_POLICY_HASH,
                CorporateActionCollection.coverage_start == spec.coverage_start,
                CorporateActionCollection.coverage_end == spec.coverage_end,
                CorporateActionCollectionAvailability.available_at <= spec.cutoff,
            )
            .order_by(
                CorporateActionCollection.recorded_at.desc(),
                CorporateActionCollection.collection_id.desc(),
            )
            .limit(1)
        )
    ).one_or_none()
    if row is None:
        raise AdjustmentFactorBuildError(
            f"no complete {action_type} collection is visible for the exact cutoff scope"
        )
    recorded_at = _utc(row.recorded_at, f"{action_type} collection recorded_at")
    available_at = _utc(row.available_at, f"{action_type} collection available_at")
    if not recorded_at <= available_at <= spec.cutoff:
        raise AdjustmentFactorBuildError(
            f"stored {action_type} collection receipt is outside the cutoff"
        )
    members = (
        await session.execute(
            select(CorporateActionCollectionMember.ordinal, CorporateActionVersion)
            .join(
                CorporateActionVersion,
                CorporateActionVersion.action_version_id
                == CorporateActionCollectionMember.action_version_id,
            )
            .where(CorporateActionCollectionMember.collection_id == row.collection_id)
            .order_by(CorporateActionCollectionMember.ordinal)
        )
    ).all()
    if len(members) != row.event_count or any(
        ordinal != expected for expected, (ordinal, _) in enumerate(members)
    ):
        raise AdjustmentFactorBuildError(
            f"stored {action_type} collection membership is incomplete"
        )
    versions = tuple(version for _, version in members)
    if any(
        version.action_type != action_type
        or version.symbol != spec.symbol
        or version.source != CORPORATE_ACTION_SOURCE
        for version in versions
    ):
        raise AdjustmentFactorBuildError(
            f"stored {action_type} collection membership escaped its exact scope"
        )
    return _SelectedCollection(
        collection_id=row.collection_id,
        recorded_at=recorded_at,
        available_at=available_at,
        event_count=row.event_count,
        versions=versions,
    )


def _raw_inputs(
    rows: tuple[Any, ...],
    spec: AdjustmentFactorBuildSpec,
) -> tuple[RawCloseVersion, ...]:
    if not rows:
        raise AdjustmentFactorBuildError("no receipted raw bars are visible for the factor window")
    values: list[RawCloseVersion] = []
    for row in rows:
        if row.greatest_count != 1 or row.active_version_count != 1:
            raise AdjustmentFactorBuildError("raw bar version selection is ambiguous")
        observed_at = _utc(row.observed_at, "raw observed_at")
        if not spec.coverage_start <= observed_at.date() <= spec.coverage_end:
            raise AdjustmentFactorBuildError("raw bar escaped the exact factor coverage")
        close = float(row.close)
        values.append(
            RawCloseVersion(
                observation_date=observed_at.date(),
                observed_at=observed_at,
                timespan=row.timespan,
                multiplier=row.multiplier,
                source=row.source,
                adjustment_basis=row.adjustment_basis,
                version_recorded_at=_utc(row.version_recorded_at, "raw version_recorded_at"),
                available_at=_utc(row.available_at, "raw available_at"),
                close=Decimal(str(close)),
            )
        )
    return tuple(values)


def _split_inputs(
    selected: _SelectedCollection,
    spec: AdjustmentFactorBuildSpec,
) -> tuple[SplitActionVersion, ...]:
    result: list[SplitActionVersion] = []
    for row in selected.versions:
        if (
            row.split_from is None
            or row.split_to is None
            or row.adjustment_type is None
            or not spec.coverage_start <= row.effective_date <= spec.coverage_end
        ):
            raise AdjustmentFactorBuildError("split collection contains an incomplete projection")
        result.append(
            SplitActionVersion(
                provider_event_id=row.provider_event_id,
                version_id=row.action_version_id,
                effective_date=row.effective_date,
                split_from=row.split_from,
                split_to=row.split_to,
                adjustment_type=cast(SplitAdjustmentType, row.adjustment_type),
            )
        )
    return tuple(result)


def _dividend_inputs(
    selected: _SelectedCollection,
    spec: AdjustmentFactorBuildSpec,
) -> tuple[DividendActionVersion, ...]:
    result: list[DividendActionVersion] = []
    for row in selected.versions:
        if (
            row.cash_amount is None
            or row.currency is None
            or row.distribution_type is None
            or not spec.coverage_start <= row.effective_date <= spec.coverage_end
        ):
            raise AdjustmentFactorBuildError(
                "dividend collection contains an incomplete projection"
            )
        result.append(
            DividendActionVersion(
                provider_event_id=row.provider_event_id,
                version_id=row.action_version_id,
                ex_dividend_date=row.effective_date,
                cash_amount=row.cash_amount,
                currency=row.currency,
                distribution_type=cast(DividendDistributionType, row.distribution_type),
            )
        )
    return tuple(result)


def _validate_spec(spec: AdjustmentFactorBuildSpec) -> AdjustmentFactorBuildSpec:
    if not isinstance(spec, AdjustmentFactorBuildSpec):
        raise TypeError("spec must be an AdjustmentFactorBuildSpec")
    if (
        not isinstance(spec.symbol, str)
        or spec.symbol != spec.symbol.strip().upper()
        or not spec.symbol
        or len(spec.symbol) > 32
        or _SYMBOL_PATTERN.fullmatch(spec.symbol) is None
    ):
        raise AdjustmentFactorBuildError("factor symbol must be uppercase and canonical")
    if type(spec.coverage_start) is not date or type(spec.coverage_end) is not date:
        raise AdjustmentFactorBuildError("factor coverage bounds must be dates")
    if spec.coverage_start > spec.coverage_end:
        raise AdjustmentFactorBuildError("factor coverage is inverted")
    if not (_CALENDAR_START <= spec.coverage_start <= spec.coverage_end <= _CALENDAR_END):
        raise AdjustmentFactorBuildError("factor coverage is outside the pinned calendar")
    cutoff = _utc(spec.cutoff, "factor cutoff")
    return AdjustmentFactorBuildSpec(
        symbol=spec.symbol,
        coverage_start=spec.coverage_start,
        coverage_end=spec.coverage_end,
        cutoff=cutoff,
    )


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise AdjustmentFactorBuildError(f"{label} must be timezone-aware")
    try:
        return value.astimezone(UTC)
    except (OSError, OverflowError, ValueError) as exc:
        raise AdjustmentFactorBuildError(f"{label} cannot be normalized to UTC") from exc


__all__ = [
    "AdjustmentFactorBuildError",
    "AdjustmentFactorBuildResult",
    "AdjustmentFactorBuildSpec",
    "AdjustmentFactorBuilder",
    "build_factor_raw_inputs_statement",
]
