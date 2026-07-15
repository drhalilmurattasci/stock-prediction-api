"""Append-only database anchors for authorized vendor-acquisition campaigns."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    SmallInteger,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class VendorAcquisitionCampaignAnchor(Base):
    """One immutable high-water checkpoint for the external campaign ledger."""

    __tablename__ = "vendor_acquisition_campaign_anchors"
    __table_args__ = (
        CheckConstraint("schema_version = 1", name="schema_version_supported"),
        CheckConstraint(
            "campaign_id ~ '^sha256:[0-9a-f]{64}$'",
            name="campaign_id_format",
        ),
        CheckConstraint("ledger_sha256 ~ '^sha256:[0-9a-f]{64}$'", name="ledger_sha256_format"),
        CheckConstraint(
            "campaign_ledger_sha256 ~ '^sha256:[0-9a-f]{64}$'",
            name="campaign_ledger_sha256_format",
        ),
        CheckConstraint("checkpoint_number > 0", name="checkpoint_number_positive"),
        CheckConstraint(
            "campaign_checkpoint_number BETWEEN 1 AND checkpoint_number",
            name="campaign_checkpoint_number_bounded",
        ),
        CheckConstraint(
            "base_calls BETWEEN 1 AND 259",
            name="base_calls_bounded",
        ),
        CheckConstraint(
            "authorized_calls BETWEEN base_calls AND LEAST(base_calls + 5, 264)",
            name="authorized_calls_bounded",
        ),
        CheckConstraint(
            "reserved_calls BETWEEN 0 AND authorized_calls",
            name="reserved_calls_bounded",
        ),
        CheckConstraint("creator_xid > 0", name="creator_xid_positive"),
        UniqueConstraint(
            "ledger_sha256",
            name="uq_vendor_acquisition_campaign_anchors_ledger_sha256",
        ),
        UniqueConstraint(
            "campaign_id",
            "campaign_checkpoint_number",
            name="uq_vendor_acquisition_campaign_anchors_campaign_checkpoint",
        ),
        UniqueConstraint(
            "campaign_id",
            "campaign_ledger_sha256",
            name="uq_vendor_acquisition_campaign_anchors_campaign_ledger",
        ),
    )

    checkpoint_number: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    ledger_sha256: Mapped[str] = mapped_column(String(71), nullable=False)
    campaign_id: Mapped[str] = mapped_column(String(71), nullable=False)
    campaign_checkpoint_number: Mapped[int] = mapped_column(BigInteger, nullable=False)
    campaign_ledger_sha256: Mapped[str] = mapped_column(String(71), nullable=False)
    schema_version: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    base_calls: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    authorized_calls: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    reserved_calls: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("clock_timestamp()")
    )
    creator_xid: Mapped[int] = mapped_column(BigInteger, nullable=False)
