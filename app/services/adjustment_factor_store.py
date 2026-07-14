"""Two-phase persistence for immutable adjustment-factor sets."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, cast

from sqlalchemy import LargeBinary, bindparam, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.db.models.adjustment_factors import (
    AdjustmentFactorEntry,
    AdjustmentFactorSetAvailability,
    AdjustmentFactorSetRecord,
)
from app.services.adjustment_factors import AdjustmentFactorSet


class AdjustmentFactorStoreError(RuntimeError):
    """A factor set could not be durably published without ambiguity."""


class AdjustmentFactorStoreConflict(AdjustmentFactorStoreError):
    """Stored projections conflict with the requested content identity."""


class AdjustmentFactorStoreOutcomeUnknown(AdjustmentFactorStoreError):
    """A database failure left commit visibility genuinely unknown."""


@dataclass(frozen=True, slots=True)
class PublishedAdjustmentFactorSet:
    """Post-commit proof for one exact complete factor set."""

    factor_set_id: str
    factor_set_recorded_at: datetime
    available_at: datetime
    input_count: int
    max_input_available_at: datetime


_PUBLISH_CONTENT = text("SELECT public.publish_adjustment_factor_set(:payload)").bindparams(
    bindparam("payload", type_=LargeBinary())
)
_PUBLISH_RECEIPT = text(
    "SELECT factor_set_id, factor_set_recorded_at, available_at "
    "FROM public.publish_adjustment_factor_set_receipt(:factor_set_id)"
)
_DETERMINISTIC_PUBLISH_REJECTIONS = frozenset({"22007", "22023", "55000"})


class SqlAdjustmentFactorSetStore:
    """Publish canonical factor content, then its later DB receipt."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._maker = async_sessionmaker(engine, expire_on_commit=False)

    async def publish(self, artifact: AdjustmentFactorSet) -> PublishedAdjustmentFactorSet:
        if not isinstance(artifact, AdjustmentFactorSet):
            raise TypeError("artifact must be an AdjustmentFactorSet")

        try:
            async with self._maker.begin() as session:
                returned_id = (
                    await session.execute(
                        _PUBLISH_CONTENT,
                        {"payload": artifact.canonical_payload},
                    )
                ).scalar_one()
                if returned_id != artifact.factor_set_id:
                    raise AdjustmentFactorStoreConflict(
                        "database publisher returned a different factor-set identity"
                    )
        except IntegrityError as exc:
            raise AdjustmentFactorStoreConflict(
                "adjustment-factor content violates the database contract"
            ) from exc
        except DBAPIError as exc:
            if _sqlstate(exc) in _DETERMINISTIC_PUBLISH_REJECTIONS:
                raise AdjustmentFactorStoreConflict(
                    "adjustment-factor content was rejected by the database contract"
                ) from exc
            await self._reconcile_content_or_raise(artifact, exc)

        try:
            await self._require_content_match(artifact)
        except DBAPIError as exc:
            raise AdjustmentFactorStoreOutcomeUnknown(
                "adjustment-factor content visibility is unknown after publication"
            ) from exc

        try:
            async with self._maker.begin() as session:
                receipt = (
                    await session.execute(
                        _PUBLISH_RECEIPT,
                        {"factor_set_id": artifact.factor_set_id},
                    )
                ).one()
                if receipt.factor_set_id != artifact.factor_set_id:
                    raise AdjustmentFactorStoreConflict(
                        "database receipt publisher returned a different identity"
                    )
        except IntegrityError as exc:
            raise AdjustmentFactorStoreConflict(
                "adjustment-factor receipt violates the database contract"
            ) from exc
        except DBAPIError as exc:
            if _sqlstate(exc) in _DETERMINISTIC_PUBLISH_REJECTIONS:
                raise AdjustmentFactorStoreConflict(
                    "adjustment-factor receipt was rejected by the database contract"
                ) from exc
            reconciled = await self._reconcile_receipt(artifact)
            if reconciled is None:
                raise AdjustmentFactorStoreOutcomeUnknown(
                    "adjustment-factor receipt commit outcome is unknown"
                ) from exc
            return reconciled

        published = await self._reconcile_receipt(artifact)
        if published is None:
            raise AdjustmentFactorStoreOutcomeUnknown(
                "adjustment-factor receipt is not visible after commit"
            )
        return published

    async def get(self, factor_set_id: str) -> PublishedAdjustmentFactorSet | None:
        async with self._maker() as session:
            row = (
                await session.execute(
                    select(
                        AdjustmentFactorSetRecord.factor_set_id,
                        AdjustmentFactorSetRecord.recorded_at,
                        AdjustmentFactorSetRecord.input_count,
                        AdjustmentFactorSetRecord.max_input_available_at,
                        AdjustmentFactorSetAvailability.available_at,
                    )
                    .join(
                        AdjustmentFactorSetAvailability,
                        AdjustmentFactorSetAvailability.factor_set_id
                        == AdjustmentFactorSetRecord.factor_set_id,
                    )
                    .where(AdjustmentFactorSetRecord.factor_set_id == factor_set_id)
                )
            ).one_or_none()
            if row is None:
                return None
            return PublishedAdjustmentFactorSet(
                factor_set_id=row.factor_set_id,
                factor_set_recorded_at=_utc(row.recorded_at),
                available_at=_utc(row.available_at),
                input_count=row.input_count,
                max_input_available_at=_utc(row.max_input_available_at),
            )

    async def _reconcile_content_or_raise(
        self,
        artifact: AdjustmentFactorSet,
        original: DBAPIError,
    ) -> None:
        try:
            await self._require_content_match(artifact)
        except AdjustmentFactorStoreConflict as exc:
            if str(exc) == "adjustment-factor set is absent":
                raise AdjustmentFactorStoreOutcomeUnknown(
                    "adjustment-factor content commit outcome is unknown"
                ) from original
            raise exc from original
        except DBAPIError as exc:
            raise AdjustmentFactorStoreOutcomeUnknown(
                "adjustment-factor content commit outcome is unknown"
            ) from exc

    async def _reconcile_receipt(
        self,
        artifact: AdjustmentFactorSet,
    ) -> PublishedAdjustmentFactorSet | None:
        try:
            await self._require_content_match(artifact)
            published = await self.get(artifact.factor_set_id)
        except DBAPIError:
            return None
        if published is None:
            return None
        if published.input_count != len(artifact.raw_inputs):
            raise AdjustmentFactorStoreConflict(
                "published adjustment-factor input count does not match"
            )
        return published

    async def _require_content_match(self, artifact: AdjustmentFactorSet) -> None:
        async with self._maker() as session:
            await _require_factor_set_match(session, artifact)


async def _require_factor_set_match(
    session: AsyncSession,
    artifact: AdjustmentFactorSet,
) -> None:
    document = cast(dict[str, Any], json.loads(artifact.canonical_payload))
    raw_inputs = cast(list[dict[str, Any]], document["raw_inputs"])
    factors = cast(list[dict[str, Any]], document["factors"])
    row = (
        await session.execute(
            select(AdjustmentFactorSetRecord).where(
                AdjustmentFactorSetRecord.factor_set_id == artifact.factor_set_id
            )
        )
    ).scalar_one_or_none()
    if row is None:
        raise AdjustmentFactorStoreConflict("adjustment-factor set is absent")

    raw_available = tuple(_timestamp(value["available_at"]) for value in raw_inputs)
    expected_max_available = max(
        *raw_available,
        _utc(row.split_collection_available_at),
        _utc(row.dividend_collection_available_at),
    )
    if not (
        row.format == document["format"]
        and row.policy_version == artifact.policy_version
        and row.policy_hash == artifact.policy_hash
        and row.symbol == artifact.symbol
        and _utc(row.cutoff) == _utc(artifact.cutoff)
        and row.anchor_date == artifact.anchor_date
        and row.coverage_start == artifact.raw_inputs[0].observation_date
        and row.coverage_end == artifact.raw_inputs[-1].observation_date
        and row.input_count == len(artifact.raw_inputs)
        and _utc(row.max_input_available_at) == expected_max_available
        and row.split_collection_id == artifact.split_collection_id
        and row.dividend_collection_id == artifact.dividend_collection_id
        and _utc(row.split_collection_recorded_at)
        <= _utc(row.split_collection_available_at)
        <= _utc(artifact.cutoff)
        and _utc(row.dividend_collection_recorded_at)
        <= _utc(row.dividend_collection_available_at)
        <= _utc(artifact.cutoff)
        and row.canonical_payload == artifact.canonical_payload
    ):
        raise AdjustmentFactorStoreConflict(
            "stored adjustment-factor header does not match its content identity"
        )

    stored = (
        (
            await session.execute(
                select(AdjustmentFactorEntry)
                .where(AdjustmentFactorEntry.factor_set_id == artifact.factor_set_id)
                .order_by(AdjustmentFactorEntry.ordinal)
            )
        )
        .scalars()
        .all()
    )
    if len(stored) != len(raw_inputs) or len(stored) != len(factors):
        raise AdjustmentFactorStoreConflict("stored adjustment-factor entry count does not match")
    for ordinal, (entry, raw_input, factor) in enumerate(
        zip(stored, raw_inputs, factors, strict=True)
    ):
        if not (
            entry.ordinal == ordinal
            and entry.symbol == artifact.symbol
            and entry.observation_date == _date(raw_input["observation_date"])
            and _utc(entry.observed_at) == _timestamp(raw_input["observed_at"])
            and entry.timespan == raw_input["timespan"]
            and entry.multiplier == raw_input["multiplier"]
            and entry.source == raw_input["source"]
            and entry.adjustment_basis == raw_input["adjustment_basis"]
            and _utc(entry.version_recorded_at) == _timestamp(raw_input["version_recorded_at"])
            and _utc(entry.raw_available_at) == _timestamp(raw_input["available_at"])
            and entry.raw_close_decimal == raw_input["close_decimal"]
            and entry.raw_close_f64_be == bytes.fromhex(raw_input["close_f64_be"])
            and entry.price_factor_decimal == factor["price_factor_decimal"]
            and entry.price_factor_f64_be == bytes.fromhex(factor["price_factor_f64_be"])
            and entry.volume_factor_decimal == factor["volume_factor_decimal"]
            and entry.volume_factor_f64_be == bytes.fromhex(factor["volume_factor_f64_be"])
        ):
            raise AdjustmentFactorStoreConflict(
                "stored adjustment-factor entry does not match canonical bytes"
            )


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise AdjustmentFactorStoreConflict("canonical factor timestamp is malformed")
    try:
        return _utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError as exc:
        raise AdjustmentFactorStoreConflict("canonical factor timestamp is malformed") from exc


def _date(value: object) -> date:
    if not isinstance(value, str):
        raise AdjustmentFactorStoreConflict("canonical factor date is malformed")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise AdjustmentFactorStoreConflict("canonical factor date is malformed") from exc


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise AdjustmentFactorStoreConflict("database factor timestamp is naive")
    return value.astimezone(UTC)


def _sqlstate(exc: DBAPIError) -> str | None:
    for name in ("sqlstate", "pgcode"):
        value = getattr(exc.orig, name, None)
        if isinstance(value, str):
            return value
    return None


__all__ = [
    "AdjustmentFactorStoreConflict",
    "AdjustmentFactorStoreError",
    "AdjustmentFactorStoreOutcomeUnknown",
    "PublishedAdjustmentFactorSet",
    "SqlAdjustmentFactorSetStore",
]
