"""Postgres high-water anchors for the local vendor-acquisition ledger."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from app.db.models.vendor_acquisition import VendorAcquisitionCampaignAnchor as AnchorRow

_HASH_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_PUBLISH = text(
    "SELECT checkpoint_number, ledger_sha256, campaign_id, "
    "campaign_checkpoint_number, campaign_ledger_sha256, base_calls, "
    "authorized_calls, reserved_calls, recorded_at "
    "FROM public.publish_vendor_acquisition_campaign_anchor("
    ":checkpoint_number, :ledger_sha256, :campaign_id, "
    ":campaign_checkpoint_number, :campaign_ledger_sha256, :base_calls, "
    ":authorized_calls, :reserved_calls)"
)
_DETERMINISTIC_REJECTIONS = frozenset({"22023", "23502", "23505", "55000"})


class VendorAcquisitionCampaignStoreError(RuntimeError):
    """The database campaign anchor could not be proven durable."""


class VendorAcquisitionCampaignStoreConflict(VendorAcquisitionCampaignStoreError):
    """The requested checkpoint conflicts with the database high-water state."""


class VendorAcquisitionCampaignStoreOutcomeUnknown(VendorAcquisitionCampaignStoreError):
    """Commit visibility for a requested checkpoint cannot be determined."""


@dataclass(frozen=True, slots=True)
class VendorAcquisitionCampaignHighWater:
    """One exact append-only checkpoint shared by the file and database ledgers."""

    checkpoint_number: int
    ledger_sha256: str
    campaign_id: str
    campaign_checkpoint_number: int
    campaign_ledger_sha256: str
    base_calls: int
    authorized_calls: int
    reserved_calls: int
    recorded_at: datetime | None = None

    def __post_init__(self) -> None:
        if _HASH_PATTERN.fullmatch(self.campaign_id) is None:
            raise ValueError("campaign_id must be a sha256 digest")
        if _HASH_PATTERN.fullmatch(self.ledger_sha256) is None:
            raise ValueError("ledger_sha256 must be a sha256 digest")
        if _HASH_PATTERN.fullmatch(self.campaign_ledger_sha256) is None:
            raise ValueError("campaign_ledger_sha256 must be a sha256 digest")
        counters = (
            self.checkpoint_number,
            self.campaign_checkpoint_number,
            self.base_calls,
            self.authorized_calls,
            self.reserved_calls,
        )
        if any(isinstance(value, bool) or not isinstance(value, int) for value in counters):
            raise ValueError("campaign high-water counters must be integers")
        if (
            self.checkpoint_number < 1
            or not 1 <= self.campaign_checkpoint_number <= self.checkpoint_number
            or not 1 <= self.base_calls <= 259
            or not self.base_calls <= self.authorized_calls <= min(self.base_calls + 5, 264)
            or not 0 <= self.reserved_calls <= self.authorized_calls
        ):
            raise ValueError("campaign high-water counters are outside the supported bounds")
        if self.recorded_at is not None and (
            self.recorded_at.tzinfo is None or self.recorded_at.utcoffset() is None
        ):
            raise ValueError("campaign high-water recorded_at must be timezone-aware")

    def same_state(self, other: VendorAcquisitionCampaignHighWater | None) -> bool:
        return other is not None and (
            self.checkpoint_number,
            self.ledger_sha256,
            self.campaign_id,
            self.campaign_checkpoint_number,
            self.campaign_ledger_sha256,
            self.base_calls,
            self.authorized_calls,
            self.reserved_calls,
        ) == (
            other.checkpoint_number,
            other.ledger_sha256,
            other.campaign_id,
            other.campaign_checkpoint_number,
            other.campaign_ledger_sha256,
            other.base_calls,
            other.authorized_calls,
            other.reserved_calls,
        )


class SqlVendorAcquisitionCampaignStore:
    """Read and append campaign anchors through a least-privilege DB function."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._maker = async_sessionmaker(engine, expire_on_commit=False)

    async def latest(self) -> VendorAcquisitionCampaignHighWater | None:
        try:
            async with self._maker() as session:
                row = (
                    await session.execute(
                        select(AnchorRow).order_by(AnchorRow.checkpoint_number.desc()).limit(1)
                    )
                ).scalar_one_or_none()
        except DBAPIError as exc:
            raise VendorAcquisitionCampaignStoreOutcomeUnknown(
                "campaign checkpoint visibility is unknown"
            ) from exc
        if row is None:
            return None
        return _from_values(
            row.checkpoint_number,
            row.ledger_sha256,
            row.campaign_id,
            row.campaign_checkpoint_number,
            row.campaign_ledger_sha256,
            row.base_calls,
            row.authorized_calls,
            row.reserved_calls,
            row.recorded_at,
        )

    async def publish(
        self,
        requested: VendorAcquisitionCampaignHighWater,
    ) -> VendorAcquisitionCampaignHighWater:
        if not isinstance(requested, VendorAcquisitionCampaignHighWater):
            raise TypeError("requested must be a VendorAcquisitionCampaignHighWater")
        try:
            async with self._maker.begin() as session:
                row = (
                    await session.execute(
                        _PUBLISH,
                        {
                            "checkpoint_number": requested.checkpoint_number,
                            "ledger_sha256": requested.ledger_sha256,
                            "campaign_id": requested.campaign_id,
                            "campaign_checkpoint_number": requested.campaign_checkpoint_number,
                            "campaign_ledger_sha256": requested.campaign_ledger_sha256,
                            "base_calls": requested.base_calls,
                            "authorized_calls": requested.authorized_calls,
                            "reserved_calls": requested.reserved_calls,
                        },
                    )
                ).one()
            published = _from_values(*row)
        except DBAPIError as exc:
            try:
                visible = await self.latest()
            except VendorAcquisitionCampaignStoreOutcomeUnknown as reconcile_exc:
                raise VendorAcquisitionCampaignStoreOutcomeUnknown(
                    "campaign checkpoint visibility is unknown"
                ) from reconcile_exc
            if requested.same_state(visible):
                assert visible is not None
                return visible
            if _sqlstate(exc) in _DETERMINISTIC_REJECTIONS:
                raise VendorAcquisitionCampaignStoreConflict(
                    "campaign checkpoint conflicts with database high-water state"
                ) from exc
            raise VendorAcquisitionCampaignStoreOutcomeUnknown(
                "campaign checkpoint visibility is unknown"
            ) from exc
        if not requested.same_state(published):
            raise VendorAcquisitionCampaignStoreConflict(
                "database returned a different campaign checkpoint"
            )
        try:
            visible = await self.latest()
        except VendorAcquisitionCampaignStoreOutcomeUnknown as exc:
            raise VendorAcquisitionCampaignStoreOutcomeUnknown(
                "campaign checkpoint visibility is unknown"
            ) from exc
        if not requested.same_state(visible):
            raise VendorAcquisitionCampaignStoreOutcomeUnknown(
                "campaign checkpoint is not the visible database high-water state"
            )
        assert visible is not None
        return visible


def _from_values(
    checkpoint_number: int,
    ledger_sha256: str,
    campaign_id: str,
    campaign_checkpoint_number: int,
    campaign_ledger_sha256: str,
    base_calls: int,
    authorized_calls: int,
    reserved_calls: int,
    recorded_at: datetime,
) -> VendorAcquisitionCampaignHighWater:
    return VendorAcquisitionCampaignHighWater(
        checkpoint_number=checkpoint_number,
        ledger_sha256=ledger_sha256,
        campaign_id=campaign_id,
        campaign_checkpoint_number=campaign_checkpoint_number,
        campaign_ledger_sha256=campaign_ledger_sha256,
        base_calls=base_calls,
        authorized_calls=authorized_calls,
        reserved_calls=reserved_calls,
        recorded_at=recorded_at,
    )


def _sqlstate(exc: DBAPIError) -> str | None:
    original = exc.orig
    return getattr(original, "sqlstate", None) or getattr(original, "pgcode", None)


__all__ = [
    "SqlVendorAcquisitionCampaignStore",
    "VendorAcquisitionCampaignHighWater",
    "VendorAcquisitionCampaignStoreConflict",
    "VendorAcquisitionCampaignStoreError",
    "VendorAcquisitionCampaignStoreOutcomeUnknown",
]
