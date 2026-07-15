"""Add immutable vendor-acquisition ledger high-water anchors.

Revision ID: 0014_vendor_campaign_anchor
Revises: 0013_adjustment_factors
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0014_vendor_campaign_anchor"
down_revision: str | None = "0013_adjustment_factors"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "vendor_acquisition_campaign_anchors",
        sa.Column("checkpoint_number", sa.BigInteger(), nullable=False),
        sa.Column("ledger_sha256", sa.String(length=71), nullable=False),
        sa.Column("campaign_id", sa.String(length=71), nullable=False),
        sa.Column("campaign_checkpoint_number", sa.BigInteger(), nullable=False),
        sa.Column("campaign_ledger_sha256", sa.String(length=71), nullable=False),
        sa.Column("schema_version", sa.SmallInteger(), nullable=False),
        sa.Column("base_calls", sa.SmallInteger(), nullable=False),
        sa.Column("authorized_calls", sa.SmallInteger(), nullable=False),
        sa.Column("reserved_calls", sa.SmallInteger(), nullable=False),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.Column("creator_xid", sa.BigInteger(), nullable=False),
        sa.CheckConstraint("schema_version = 1", name="schema_version_supported"),
        sa.CheckConstraint("campaign_id ~ '^sha256:[0-9a-f]{64}$'", name="campaign_id_format"),
        sa.CheckConstraint("ledger_sha256 ~ '^sha256:[0-9a-f]{64}$'", name="ledger_sha256_format"),
        sa.CheckConstraint(
            "campaign_ledger_sha256 ~ '^sha256:[0-9a-f]{64}$'",
            name="campaign_ledger_sha256_format",
        ),
        sa.CheckConstraint("checkpoint_number > 0", name="checkpoint_number_positive"),
        sa.CheckConstraint(
            "campaign_checkpoint_number BETWEEN 1 AND checkpoint_number",
            name="campaign_checkpoint_number_bounded",
        ),
        sa.CheckConstraint("base_calls BETWEEN 1 AND 259", name="base_calls_bounded"),
        sa.CheckConstraint(
            "authorized_calls BETWEEN base_calls AND LEAST(base_calls + 5, 264)",
            name="authorized_calls_bounded",
        ),
        sa.CheckConstraint(
            "reserved_calls BETWEEN 0 AND authorized_calls",
            name="reserved_calls_bounded",
        ),
        sa.CheckConstraint("creator_xid > 0", name="creator_xid_positive"),
        sa.PrimaryKeyConstraint("checkpoint_number"),
        sa.UniqueConstraint(
            "ledger_sha256",
            name="uq_vendor_acquisition_campaign_anchors_ledger_sha256",
        ),
        sa.UniqueConstraint(
            "campaign_id",
            "campaign_checkpoint_number",
            name="uq_vendor_acquisition_campaign_anchors_campaign_checkpoint",
        ),
        sa.UniqueConstraint(
            "campaign_id",
            "campaign_ledger_sha256",
            name="uq_vendor_acquisition_campaign_anchors_campaign_ledger",
        ),
    )

    op.execute(
        r"""
        CREATE FUNCTION reject_vendor_acquisition_anchor_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        BEGIN
            RAISE EXCEPTION 'vendor-acquisition ledger anchors are append-only'
                USING ERRCODE = '55000';
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER vendor_acquisition_campaign_anchors_no_row_mutation "
        "BEFORE UPDATE OR DELETE ON vendor_acquisition_campaign_anchors "
        "FOR EACH ROW EXECUTE FUNCTION reject_vendor_acquisition_anchor_mutation()"
    )
    op.execute(
        "CREATE TRIGGER vendor_acquisition_campaign_anchors_no_truncate "
        "BEFORE TRUNCATE ON vendor_acquisition_campaign_anchors "
        "FOR EACH STATEMENT EXECUTE FUNCTION reject_vendor_acquisition_anchor_mutation()"
    )

    op.execute(
        r"""
        CREATE FUNCTION publish_vendor_acquisition_campaign_anchor(
            requested_checkpoint_number bigint,
            requested_ledger_sha256 text,
            requested_campaign_id text,
            requested_campaign_checkpoint_number bigint,
            requested_campaign_ledger_sha256 text,
            requested_base_calls integer,
            requested_authorized_calls integer,
            requested_reserved_calls integer
        )
        RETURNS TABLE(
            checkpoint_number bigint,
            ledger_sha256 text,
            campaign_id text,
            campaign_checkpoint_number bigint,
            campaign_ledger_sha256 text,
            base_calls integer,
            authorized_calls integer,
            reserved_calls integer,
            recorded_at timestamptz
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            prior_global public.vendor_acquisition_campaign_anchors%ROWTYPE;
            prior_campaign public.vendor_acquisition_campaign_anchors%ROWTYPE;
            has_global boolean;
            has_campaign boolean;
        BEGIN
            IF requested_checkpoint_number IS NULL
               OR requested_ledger_sha256 IS NULL
               OR requested_campaign_id IS NULL
               OR requested_campaign_checkpoint_number IS NULL
               OR requested_campaign_ledger_sha256 IS NULL
               OR requested_base_calls IS NULL
               OR requested_authorized_calls IS NULL
               OR requested_reserved_calls IS NULL
               OR requested_campaign_id !~ '^sha256:[0-9a-f]{64}$'
               OR requested_ledger_sha256 !~ '^sha256:[0-9a-f]{64}$'
               OR requested_campaign_ledger_sha256 !~ '^sha256:[0-9a-f]{64}$'
               OR requested_checkpoint_number < 1
               OR requested_campaign_checkpoint_number NOT BETWEEN 1
                   AND requested_checkpoint_number
               OR requested_base_calls NOT BETWEEN 1 AND 259
               OR requested_authorized_calls NOT BETWEEN requested_base_calls
                   AND LEAST(requested_base_calls + 5, 264)
               OR requested_reserved_calls NOT BETWEEN 0 AND requested_authorized_calls THEN
                RAISE EXCEPTION 'vendor-acquisition ledger anchor is invalid'
                    USING ERRCODE = '22023';
            END IF;

            PERFORM pg_catalog.pg_advisory_xact_lock(
                pg_catalog.hashtextextended('stockapi-vendor-acquisition-ledger-anchor', 0)
            );

            SELECT anchor.* INTO prior_global
            FROM public.vendor_acquisition_campaign_anchors AS anchor
            ORDER BY anchor.checkpoint_number DESC
            LIMIT 1;
            has_global := FOUND;

            IF has_global AND requested_checkpoint_number <= prior_global.checkpoint_number THEN
                IF requested_checkpoint_number = prior_global.checkpoint_number
                   AND requested_ledger_sha256 = prior_global.ledger_sha256
                   AND requested_campaign_id = prior_global.campaign_id
                   AND requested_campaign_checkpoint_number =
                       prior_global.campaign_checkpoint_number
                   AND requested_campaign_ledger_sha256 =
                       prior_global.campaign_ledger_sha256
                   AND requested_base_calls = prior_global.base_calls
                   AND requested_authorized_calls = prior_global.authorized_calls
                   AND requested_reserved_calls = prior_global.reserved_calls THEN
                    RETURN QUERY
                    SELECT prior_global.checkpoint_number,
                           prior_global.ledger_sha256::text,
                           prior_global.campaign_id::text,
                           prior_global.campaign_checkpoint_number,
                           prior_global.campaign_ledger_sha256::text,
                           prior_global.base_calls::integer,
                           prior_global.authorized_calls::integer,
                           prior_global.reserved_calls::integer,
                           prior_global.recorded_at;
                    RETURN;
                END IF;
                RAISE EXCEPTION 'historical campaign checkpoint cannot be replayed'
                    USING ERRCODE = '55000';
            END IF;

            IF (NOT has_global AND requested_checkpoint_number <> 1)
               OR (has_global AND (
                   requested_checkpoint_number <> prior_global.checkpoint_number + 1
                   OR requested_ledger_sha256 = prior_global.ledger_sha256
               )) THEN
                RAISE EXCEPTION 'ledger checkpoint does not extend the global high-water state'
                    USING ERRCODE = '55000';
            END IF;

            SELECT anchor.* INTO prior_campaign
            FROM public.vendor_acquisition_campaign_anchors AS anchor
            WHERE anchor.campaign_id = requested_campaign_id
            ORDER BY anchor.campaign_checkpoint_number DESC
            LIMIT 1;
            has_campaign := FOUND;

            IF NOT has_campaign THEN
                IF requested_campaign_checkpoint_number <> 1
                   OR requested_authorized_calls <> requested_base_calls
                   OR requested_reserved_calls <> 0 THEN
                    RAISE EXCEPTION 'initial campaign checkpoint is not canonical'
                        USING ERRCODE = '55000';
                END IF;
            ELSIF requested_campaign_checkpoint_number <>
                    prior_campaign.campaign_checkpoint_number + 1
               OR requested_campaign_ledger_sha256 = prior_campaign.campaign_ledger_sha256
               OR requested_base_calls <> prior_campaign.base_calls
               OR requested_authorized_calls < prior_campaign.authorized_calls
               OR requested_reserved_calls < prior_campaign.reserved_calls
               OR NOT (
                   (
                       requested_authorized_calls > prior_campaign.authorized_calls
                       AND requested_reserved_calls = prior_campaign.reserved_calls
                   )
                   OR (
                       requested_authorized_calls = prior_campaign.authorized_calls
                       AND requested_reserved_calls = prior_campaign.reserved_calls + 1
                   )
                   OR (
                       requested_authorized_calls = prior_campaign.authorized_calls
                       AND requested_reserved_calls = prior_campaign.reserved_calls
                   )
               ) THEN
                RAISE EXCEPTION 'campaign checkpoint does not extend its high-water state'
                    USING ERRCODE = '55000';
            END IF;

            INSERT INTO public.vendor_acquisition_campaign_anchors(
                checkpoint_number, ledger_sha256, campaign_id,
                campaign_checkpoint_number, campaign_ledger_sha256, schema_version,
                base_calls, authorized_calls, reserved_calls, creator_xid
            ) VALUES (
                requested_checkpoint_number, requested_ledger_sha256,
                requested_campaign_id, requested_campaign_checkpoint_number,
                requested_campaign_ledger_sha256, 1, requested_base_calls,
                requested_authorized_calls, requested_reserved_calls,
                pg_catalog.txid_current()
            );

            RETURN QUERY
            SELECT anchor.checkpoint_number, anchor.ledger_sha256::text,
                   anchor.campaign_id::text, anchor.campaign_checkpoint_number,
                   anchor.campaign_ledger_sha256::text, anchor.base_calls::integer,
                   anchor.authorized_calls::integer, anchor.reserved_calls::integer,
                   anchor.recorded_at
            FROM public.vendor_acquisition_campaign_anchors AS anchor
            WHERE anchor.checkpoint_number = requested_checkpoint_number;
        END;
        $$
        """
    )

    op.execute(
        "REVOKE ALL ON FUNCTION public.reject_vendor_acquisition_anchor_mutation() FROM PUBLIC"
    )
    op.execute(
        "REVOKE ALL ON FUNCTION public.publish_vendor_acquisition_campaign_anchor("
        "bigint,text,text,bigint,text,integer,integer,integer) FROM PUBLIC"
    )
    op.execute("REVOKE ALL ON TABLE public.vendor_acquisition_campaign_anchors FROM PUBLIC")
    op.execute("REVOKE ALL ON TABLE public.vendor_acquisition_campaign_anchors FROM stockapi_app")
    op.execute(
        "REVOKE ALL ON TABLE public.vendor_acquisition_campaign_anchors "
        "FROM stockapi_snapshot_builder"
    )
    op.execute("GRANT SELECT ON TABLE public.vendor_acquisition_campaign_anchors TO stockapi_app")
    op.execute(
        "GRANT EXECUTE ON FUNCTION public.publish_vendor_acquisition_campaign_anchor("
        "bigint,text,text,bigint,text,integer,integer,integer) TO stockapi_app"
    )


def downgrade() -> None:
    op.execute(
        r"""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM public.vendor_acquisition_campaign_anchors) THEN
                RAISE EXCEPTION 'cannot downgrade nonempty vendor-acquisition ledger anchors'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $$
        """
    )
    op.execute(
        "DROP FUNCTION IF EXISTS public.publish_vendor_acquisition_campaign_anchor("
        "bigint,text,text,bigint,text,integer,integer,integer)"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS vendor_acquisition_campaign_anchors_no_truncate "
        "ON vendor_acquisition_campaign_anchors"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS vendor_acquisition_campaign_anchors_no_row_mutation "
        "ON vendor_acquisition_campaign_anchors"
    )
    op.execute("DROP FUNCTION IF EXISTS public.reject_vendor_acquisition_anchor_mutation()")
    op.drop_table("vendor_acquisition_campaign_anchors")
