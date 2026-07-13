"""add post-commit bar-version availability receipts

Revision ID: 0008_bar_version_availability
Revises: 0007_snapshot_builder_privileges
Create Date: 2026-07-13

``bars.recorded_at`` is stamped while its write transaction is still open, so
it cannot prove commit visibility at a historical cutoff.  This append-only
ledger is finalized in a later transaction. PostgreSQL overwrites every
caller-supplied ``available_at`` and rejects receipts for a version created by
the same transaction, making its timestamp a conservative post-commit release
boundary. Existing committed versions become available no earlier than this
migration; no historical visibility time is invented.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0008_bar_version_availability"
down_revision: str | None = "0007_snapshot_builder_privileges"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # A full, DB-stamped transaction ID is the proof that a receipt is not
    # being created by the same top-level transaction as its bar version.
    # ``xmin`` is insufficient here: writes in a SAVEPOINT carry a subxid while
    # ``txid_current()`` returns the top-level xid. Existing rows receive the
    # reserved zero sentinel while this migration holds its ALTER TABLE locks;
    # they necessarily predate this transaction and are already committed.
    op.add_column(
        "bars",
        sa.Column(
            "version_creator_xid",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
    )
    op.add_column(
        "bars_revisions",
        sa.Column(
            "previous_creator_xid",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
    )
    op.add_column(
        "bars_revisions",
        sa.Column(
            "incoming_creator_xid",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
    )
    op.create_check_constraint(
        op.f("ck_bars_version_creator_xid_nonnegative"),
        "bars",
        "version_creator_xid >= 0",
    )
    op.create_check_constraint(
        op.f("ck_bars_revisions_creator_xids_nonnegative"),
        "bars_revisions",
        "previous_creator_xid >= 0 AND incoming_creator_xid >= 0",
    )
    op.alter_column("bars", "version_creator_xid", server_default=None)
    op.alter_column("bars_revisions", "previous_creator_xid", server_default=None)
    op.alter_column("bars_revisions", "incoming_creator_xid", server_default=None)

    # Replace the existing trigger function in-place so stamping, immutable
    # conflict keys, no-op replay behavior, and revision capture remain one
    # ordered operation. Caller-supplied creator IDs are always overwritten.
    op.execute(
        """
        CREATE OR REPLACE FUNCTION version_bar_write()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            recorded timestamptz;
            creator_xid bigint := txid_current();
        BEGIN
            IF TG_OP = 'INSERT' THEN
                NEW.recorded_at := clock_timestamp();
                NEW.version_creator_xid := txid_current();
                RETURN NEW;
            END IF;
            recorded := GREATEST(
                clock_timestamp(),
                OLD.recorded_at + interval '1 microsecond'
            );

            IF ROW(OLD.symbol, OLD.timespan, OLD.multiplier, OLD.ts,
                   OLD.source, OLD.adjustment_basis)
               IS DISTINCT FROM
               ROW(NEW.symbol, NEW.timespan, NEW.multiplier, NEW.ts,
                   NEW.source, NEW.adjustment_basis) THEN
                RAISE EXCEPTION 'bar conflict keys are immutable'
                    USING ERRCODE = '55000';
            END IF;

            IF ROW(OLD.open, OLD.high, OLD.low, OLD.close, OLD.volume,
                   OLD.vwap, OLD.trade_count)
               IS NOT DISTINCT FROM
               ROW(NEW.open, NEW.high, NEW.low, NEW.close, NEW.volume,
                   NEW.vwap, NEW.trade_count) THEN
                RETURN NULL;
            END IF;
            IF NEW.as_of <= OLD.as_of OR NEW.fetched_at <= OLD.fetched_at THEN
                RAISE EXCEPTION 'changed bars require newer fetched_at and as_of'
                    USING ERRCODE = '55000';
            END IF;

            NEW.recorded_at := recorded;
            NEW.version_creator_xid := creator_xid;
            INSERT INTO public.bars_revisions (
                symbol, timespan, multiplier, ts, source, adjustment_basis,
                previous_open, previous_high, previous_low, previous_close,
                previous_volume, previous_vwap, previous_trade_count,
                previous_fetched_at, previous_as_of,
                incoming_open, incoming_high, incoming_low, incoming_close,
                incoming_volume, incoming_vwap, incoming_trade_count,
                incoming_fetched_at, incoming_as_of,
                previous_recorded_at, incoming_recorded_at,
                previous_creator_xid, incoming_creator_xid, revised_at
            ) VALUES (
                OLD.symbol, OLD.timespan, OLD.multiplier, OLD.ts,
                OLD.source, OLD.adjustment_basis,
                OLD.open, OLD.high, OLD.low, OLD.close,
                OLD.volume, OLD.vwap, OLD.trade_count,
                OLD.fetched_at, OLD.as_of,
                NEW.open, NEW.high, NEW.low, NEW.close,
                NEW.volume, NEW.vwap, NEW.trade_count,
                NEW.fetched_at, NEW.as_of,
                OLD.recorded_at, recorded,
                OLD.version_creator_xid, creator_xid, recorded
            );
            RETURN NEW;
        END;
        $$
        """
    )

    op.create_table(
        "bar_version_availability",
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("timespan", sa.String(length=16), nullable=False),
        sa.Column("multiplier", sa.Integer(), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("adjustment_basis", sa.String(length=32), nullable=False),
        sa.Column("version_recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "multiplier >= 1",
            name=op.f("ck_bar_version_availability_availability_multiplier_positive"),
        ),
        sa.CheckConstraint(
            "available_at >= version_recorded_at",
            name=op.f("ck_bar_version_availability_availability_not_before_recording"),
        ),
        sa.PrimaryKeyConstraint(
            "symbol",
            "timespan",
            "multiplier",
            "ts",
            "source",
            "adjustment_basis",
            "version_recorded_at",
            name=op.f("pk_bar_version_availability"),
        ),
    )
    op.create_index(
        "ix_bar_version_availability_series_time",
        "bar_version_availability",
        [
            "symbol",
            "timespan",
            "multiplier",
            "source",
            "adjustment_basis",
            "ts",
            "available_at",
        ],
    )

    op.execute(
        r"""
        CREATE FUNCTION stamp_bar_version_availability()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            current_xid bigint := txid_current();
            version_exists boolean;
            version_is_uncommitted boolean;
        BEGIN
            SELECT EXISTS (
                SELECT 1
                FROM (
                    SELECT bar.recorded_at AS recorded_at,
                           bar.version_creator_xid AS creator_xid
                    FROM public.bars AS bar
                    WHERE bar.symbol = NEW.symbol
                      AND bar.timespan = NEW.timespan
                      AND bar.multiplier = NEW.multiplier
                      AND bar.ts = NEW.ts
                      AND bar.source = NEW.source
                      AND bar.adjustment_basis = NEW.adjustment_basis
                    UNION ALL
                    SELECT revision.previous_recorded_at,
                           revision.previous_creator_xid
                    FROM public.bars_revisions AS revision
                    WHERE revision.symbol = NEW.symbol
                      AND revision.timespan = NEW.timespan
                      AND revision.multiplier = NEW.multiplier
                      AND revision.ts = NEW.ts
                      AND revision.source = NEW.source
                      AND revision.adjustment_basis = NEW.adjustment_basis
                    UNION ALL
                    SELECT revision.incoming_recorded_at,
                           revision.incoming_creator_xid
                    FROM public.bars_revisions AS revision
                    WHERE revision.symbol = NEW.symbol
                      AND revision.timespan = NEW.timespan
                      AND revision.multiplier = NEW.multiplier
                      AND revision.ts = NEW.ts
                      AND revision.source = NEW.source
                      AND revision.adjustment_basis = NEW.adjustment_basis
                ) AS versions
                WHERE versions.recorded_at = NEW.version_recorded_at
            ), EXISTS (
                SELECT 1
                FROM (
                    SELECT bar.recorded_at AS recorded_at,
                           bar.version_creator_xid AS creator_xid
                    FROM public.bars AS bar
                    WHERE bar.symbol = NEW.symbol
                      AND bar.timespan = NEW.timespan
                      AND bar.multiplier = NEW.multiplier
                      AND bar.ts = NEW.ts
                      AND bar.source = NEW.source
                      AND bar.adjustment_basis = NEW.adjustment_basis
                    UNION ALL
                    SELECT revision.previous_recorded_at,
                           revision.previous_creator_xid
                    FROM public.bars_revisions AS revision
                    WHERE revision.symbol = NEW.symbol
                      AND revision.timespan = NEW.timespan
                      AND revision.multiplier = NEW.multiplier
                      AND revision.ts = NEW.ts
                      AND revision.source = NEW.source
                      AND revision.adjustment_basis = NEW.adjustment_basis
                    UNION ALL
                    SELECT revision.incoming_recorded_at,
                           revision.incoming_creator_xid
                    FROM public.bars_revisions AS revision
                    WHERE revision.symbol = NEW.symbol
                      AND revision.timespan = NEW.timespan
                      AND revision.multiplier = NEW.multiplier
                      AND revision.ts = NEW.ts
                      AND revision.source = NEW.source
                      AND revision.adjustment_basis = NEW.adjustment_basis
                ) AS versions
                WHERE versions.recorded_at = NEW.version_recorded_at
                  AND versions.creator_xid = current_xid
            )
            INTO version_exists, version_is_uncommitted;

            IF NOT version_exists THEN
                RAISE EXCEPTION 'availability receipt does not identify a stored bar version'
                    USING ERRCODE = '23503';
            END IF;
            IF version_is_uncommitted THEN
                RAISE EXCEPTION 'bar availability must be finalized after its write commits'
                    USING ERRCODE = '55000';
            END IF;
            NEW.available_at := clock_timestamp();
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute("REVOKE ALL ON FUNCTION stamp_bar_version_availability() FROM PUBLIC")
    op.execute(
        """
        CREATE TRIGGER bar_version_availability_stamp
        BEFORE INSERT ON bar_version_availability
        FOR EACH ROW EXECUTE FUNCTION stamp_bar_version_availability()
        """
    )
    op.execute(
        """
        CREATE TRIGGER bar_version_availability_no_row_mutation
        BEFORE UPDATE OR DELETE ON bar_version_availability
        FOR EACH ROW EXECUTE FUNCTION reject_bar_history_mutation()
        """
    )
    op.execute(
        """
        CREATE TRIGGER bar_version_availability_no_truncate
        BEFORE TRUNCATE ON bar_version_availability
        FOR EACH STATEMENT EXECUTE FUNCTION reject_bar_history_mutation()
        """
    )

    # The source rows predate this migration transaction, so the trigger proves
    # this is a post-commit, conservative backfill. UNION removes versions that
    # appear both as a revision's incoming value and as the current bar.
    op.execute(
        """
        INSERT INTO public.bar_version_availability (
            symbol, timespan, multiplier, ts, source, adjustment_basis,
            version_recorded_at
        )
        SELECT symbol, timespan, multiplier, ts, source, adjustment_basis,
               version_recorded_at
        FROM (
            SELECT symbol, timespan, multiplier, ts, source, adjustment_basis,
                   recorded_at AS version_recorded_at
            FROM public.bars
            UNION
            SELECT symbol, timespan, multiplier, ts, source, adjustment_basis,
                   previous_recorded_at AS version_recorded_at
            FROM public.bars_revisions
            WHERE previous_recorded_at IS NOT NULL
            UNION
            SELECT symbol, timespan, multiplier, ts, source, adjustment_basis,
                   incoming_recorded_at AS version_recorded_at
            FROM public.bars_revisions
            WHERE incoming_recorded_at IS NOT NULL
        ) AS committed_versions
        ON CONFLICT DO NOTHING
        """
    )

    op.execute("REVOKE ALL ON TABLE public.bar_version_availability FROM PUBLIC")
    op.execute("REVOKE ALL PRIVILEGES ON TABLE public.bar_version_availability FROM stockapi_app")
    op.execute(
        "REVOKE ALL PRIVILEGES ON TABLE public.bar_version_availability "
        "FROM stockapi_snapshot_builder"
    )
    op.execute("GRANT SELECT, INSERT ON TABLE public.bar_version_availability TO stockapi_app")
    op.execute("GRANT SELECT ON TABLE public.bar_version_availability TO stockapi_snapshot_builder")
    op.execute(
        """
        DO $$
        DECLARE
            app_role oid;
            builder_role oid;
        BEGIN
            SELECT oid INTO STRICT app_role
            FROM pg_roles WHERE rolname = 'stockapi_app';
            SELECT oid INTO STRICT builder_role
            FROM pg_roles WHERE rolname = 'stockapi_snapshot_builder';

            IF NOT has_table_privilege(
                app_role, 'public.bar_version_availability', 'SELECT'
            ) OR NOT has_table_privilege(
                app_role, 'public.bar_version_availability', 'INSERT'
            ) OR has_table_privilege(
                app_role, 'public.bar_version_availability', 'UPDATE'
            ) OR has_table_privilege(
                app_role, 'public.bar_version_availability', 'DELETE'
            ) OR has_table_privilege(
                app_role, 'public.bar_version_availability', 'TRUNCATE'
            ) OR has_table_privilege(
                app_role, 'public.bar_version_availability', 'REFERENCES'
            ) OR has_table_privilege(
                app_role, 'public.bar_version_availability', 'TRIGGER'
            ) OR has_table_privilege(
                app_role, 'public.bar_version_availability', 'MAINTAIN'
            ) OR has_any_column_privilege(
                app_role, 'public.bar_version_availability', 'UPDATE'
            ) OR has_any_column_privilege(
                app_role, 'public.bar_version_availability', 'REFERENCES'
            ) THEN
                RAISE EXCEPTION 'runtime availability-receipt privileges are not exact';
            END IF;
            IF NOT has_table_privilege(
                builder_role, 'public.bar_version_availability', 'SELECT'
            ) OR has_table_privilege(
                builder_role, 'public.bar_version_availability', 'INSERT'
            ) OR has_table_privilege(
                builder_role, 'public.bar_version_availability', 'UPDATE'
            ) OR has_table_privilege(
                builder_role, 'public.bar_version_availability', 'DELETE'
            ) OR has_table_privilege(
                builder_role, 'public.bar_version_availability', 'TRUNCATE'
            ) OR has_table_privilege(
                builder_role, 'public.bar_version_availability', 'REFERENCES'
            ) OR has_table_privilege(
                builder_role, 'public.bar_version_availability', 'TRIGGER'
            ) OR has_table_privilege(
                builder_role, 'public.bar_version_availability', 'MAINTAIN'
            ) OR has_any_column_privilege(
                builder_role, 'public.bar_version_availability', 'INSERT'
            ) OR has_any_column_privilege(
                builder_role, 'public.bar_version_availability', 'UPDATE'
            ) OR has_any_column_privilege(
                builder_role, 'public.bar_version_availability', 'REFERENCES'
            ) THEN
                RAISE EXCEPTION 'builder availability-receipt privileges are not exact';
            END IF;
            IF has_function_privilege(
                app_role, 'public.stamp_bar_version_availability()', 'EXECUTE'
            ) OR has_function_privilege(
                builder_role, 'public.stamp_bar_version_availability()', 'EXECUTE'
            ) THEN
                RAISE EXCEPTION 'availability trigger function is directly executable';
            END IF;
        END;
        $$
        """
    )


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER IF EXISTS bar_version_availability_no_truncate ON bar_version_availability"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS bar_version_availability_no_row_mutation "
        "ON bar_version_availability"
    )
    op.execute("DROP TRIGGER IF EXISTS bar_version_availability_stamp ON bar_version_availability")
    op.execute("DROP FUNCTION IF EXISTS stamp_bar_version_availability()")
    op.drop_index(
        "ix_bar_version_availability_series_time",
        table_name="bar_version_availability",
    )
    op.drop_table("bar_version_availability")

    # Restore the exact 0006 trigger body before removing the columns that the
    # 0008 body references. Leaving the newer PL/pgSQL body installed would
    # make every subsequent bar write fail after downgrade.
    op.execute(
        """
        CREATE OR REPLACE FUNCTION version_bar_write()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            recorded timestamptz;
        BEGIN
            IF TG_OP = 'INSERT' THEN
                NEW.recorded_at := clock_timestamp();
                RETURN NEW;
            END IF;
            recorded := GREATEST(
                clock_timestamp(),
                OLD.recorded_at + interval '1 microsecond'
            );

            IF ROW(OLD.symbol, OLD.timespan, OLD.multiplier, OLD.ts,
                   OLD.source, OLD.adjustment_basis)
               IS DISTINCT FROM
               ROW(NEW.symbol, NEW.timespan, NEW.multiplier, NEW.ts,
                   NEW.source, NEW.adjustment_basis) THEN
                RAISE EXCEPTION 'bar conflict keys are immutable'
                    USING ERRCODE = '55000';
            END IF;

            IF ROW(OLD.open, OLD.high, OLD.low, OLD.close, OLD.volume,
                   OLD.vwap, OLD.trade_count)
               IS NOT DISTINCT FROM
               ROW(NEW.open, NEW.high, NEW.low, NEW.close, NEW.volume,
                   NEW.vwap, NEW.trade_count) THEN
                RETURN NULL;
            END IF;
            IF NEW.as_of <= OLD.as_of OR NEW.fetched_at <= OLD.fetched_at THEN
                RAISE EXCEPTION 'changed bars require newer fetched_at and as_of'
                    USING ERRCODE = '55000';
            END IF;

            NEW.recorded_at := recorded;
            INSERT INTO public.bars_revisions (
                symbol, timespan, multiplier, ts, source, adjustment_basis,
                previous_open, previous_high, previous_low, previous_close,
                previous_volume, previous_vwap, previous_trade_count,
                previous_fetched_at, previous_as_of,
                incoming_open, incoming_high, incoming_low, incoming_close,
                incoming_volume, incoming_vwap, incoming_trade_count,
                incoming_fetched_at, incoming_as_of,
                previous_recorded_at, incoming_recorded_at, revised_at
            ) VALUES (
                OLD.symbol, OLD.timespan, OLD.multiplier, OLD.ts,
                OLD.source, OLD.adjustment_basis,
                OLD.open, OLD.high, OLD.low, OLD.close,
                OLD.volume, OLD.vwap, OLD.trade_count,
                OLD.fetched_at, OLD.as_of,
                NEW.open, NEW.high, NEW.low, NEW.close,
                NEW.volume, NEW.vwap, NEW.trade_count,
                NEW.fetched_at, NEW.as_of,
                OLD.recorded_at, recorded, recorded
            );
            RETURN NEW;
        END;
        $$
        """
    )
    op.drop_constraint(
        op.f("ck_bars_revisions_creator_xids_nonnegative"),
        "bars_revisions",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_bars_version_creator_xid_nonnegative"),
        "bars",
        type_="check",
    )
    op.drop_column("bars_revisions", "incoming_creator_xid")
    op.drop_column("bars_revisions", "previous_creator_xid")
    op.drop_column("bars", "version_creator_xid")
