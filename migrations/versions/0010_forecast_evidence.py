"""add immutable realized-outcome and precommitted cohort evidence

Revision ID: 0010_forecast_evidence
Revises: 0009_forecast_runs
Create Date: 2026-07-14

The schema records exact, content-addressed raw-close outcome evidence under an
explicit resolution policy and future-target cohort membership under an
explicit selection policy. Cohort commitment uses a second-transaction receipt:
an insert-time timestamp alone cannot prove that the creating transaction
committed before the first outcome became observable.

Version 1 outcomes are deliberately restricted to the ``polygon_open_close``
raw-close timestamp contract. Canonical cohort members are projected into an
immutable relational table and bound to exact scheduled forecast outputs before
a later transaction can seal the manifest.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0010_forecast_evidence"
down_revision: str | None = "0009_forecast_runs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    op.create_unique_constraint(
        "uq_bar_version_availability_exact_receipt",
        "bar_version_availability",
        [
            "symbol",
            "timespan",
            "multiplier",
            "ts",
            "source",
            "adjustment_basis",
            "version_recorded_at",
            "available_at",
        ],
    )

    op.create_table(
        "forecast_realized_outcomes",
        sa.Column("outcome_id", sa.String(length=71), nullable=False),
        sa.Column("schema_version", sa.SmallInteger(), nullable=False),
        sa.Column("outcome_resolution_policy_hash", sa.String(length=71), nullable=False),
        sa.Column("availability_rule_set_hash", sa.String(length=71), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("target", sa.String(length=32), nullable=False),
        sa.Column("series_basis", sa.String(length=32), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("target_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolution_cutoff", sa.DateTime(timezone=True), nullable=False),
        sa.Column("bar_timespan", sa.String(length=16), nullable=False),
        sa.Column("bar_multiplier", sa.Integer(), nullable=False),
        sa.Column("bar_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("bar_source", sa.String(length=64), nullable=False),
        sa.Column("bar_adjustment_basis", sa.String(length=32), nullable=False),
        sa.Column("bar_version_recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("bar_fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("bar_source_as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column("bar_available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("bar_field", sa.String(length=32), nullable=False),
        sa.Column("bar_value", sa.Float(), nullable=False),
        sa.Column("realized_value", sa.Float(), nullable=False),
        sa.Column(
            "sealed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.Column("canonical_evidence", sa.LargeBinary(), nullable=False),
        sa.CheckConstraint(
            "schema_version = 1",
            name=op.f("ck_forecast_realized_outcomes_schema_version_supported"),
        ),
        sa.CheckConstraint(
            "outcome_id ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_realized_outcomes_outcome_id_format"),
        ),
        sa.CheckConstraint(
            "outcome_resolution_policy_hash ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_realized_outcomes_resolution_policy_hash_format"),
        ),
        sa.CheckConstraint(
            "availability_rule_set_hash ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_realized_outcomes_availability_rule_set_hash_format"),
        ),
        sa.CheckConstraint(
            "symbol = upper(symbol)",
            name=op.f("ck_forecast_realized_outcomes_symbol_uppercase"),
        ),
        sa.CheckConstraint(
            "symbol ~ '^[A-Z0-9.\\-_:]+$'",
            name=op.f("ck_forecast_realized_outcomes_symbol_format"),
        ),
        sa.CheckConstraint(
            "target = 'close'",
            name=op.f("ck_forecast_realized_outcomes_target_supported"),
        ),
        sa.CheckConstraint(
            "series_basis = 'raw'",
            name=op.f("ck_forecast_realized_outcomes_series_basis_supported"),
        ),
        sa.CheckConstraint(
            "currency ~ '^[A-Z]{3}$'",
            name=op.f("ck_forecast_realized_outcomes_currency_format"),
        ),
        sa.CheckConstraint(
            "bar_timespan = 'day' AND bar_multiplier = 1 "
            "AND bar_source = 'polygon_open_close' "
            "AND bar_adjustment_basis = 'raw' AND bar_field = 'close'",
            name=op.f("ck_forecast_realized_outcomes_source_supported"),
        ),
        sa.CheckConstraint(
            "bar_value > '-Infinity'::float8 AND bar_value < 'Infinity'::float8 "
            "AND realized_value > '-Infinity'::float8 "
            "AND realized_value < 'Infinity'::float8",
            name=op.f("ck_forecast_realized_outcomes_values_finite"),
        ),
        sa.CheckConstraint(
            "bar_value >= 0 AND realized_value >= 0 AND bar_value = realized_value",
            name=op.f("ck_forecast_realized_outcomes_raw_close_value_matches"),
        ),
        sa.CheckConstraint(
            "bar_observed_at = target_time "
            "AND bar_observed_at <= bar_fetched_at "
            "AND bar_fetched_at <= bar_source_as_of "
            "AND bar_source_as_of <= bar_version_recorded_at "
            "AND bar_version_recorded_at <= bar_available_at "
            "AND bar_available_at <= resolution_cutoff "
            "AND resolution_cutoff <= sealed_at",
            name=op.f("ck_forecast_realized_outcomes_evidence_time_order"),
        ),
        sa.CheckConstraint(
            "octet_length(canonical_evidence) BETWEEN 1 AND 262144",
            name=op.f("ck_forecast_realized_outcomes_evidence_size_bounded"),
        ),
        sa.CheckConstraint(
            "outcome_id = 'sha256:' || encode(digest(canonical_evidence, 'sha256'), 'hex')",
            name=op.f("ck_forecast_realized_outcomes_outcome_id_matches_payload"),
        ),
        sa.ForeignKeyConstraint(
            [
                "symbol",
                "bar_timespan",
                "bar_multiplier",
                "bar_observed_at",
                "bar_source",
                "bar_adjustment_basis",
                "bar_version_recorded_at",
                "bar_available_at",
            ],
            [
                "bar_version_availability.symbol",
                "bar_version_availability.timespan",
                "bar_version_availability.multiplier",
                "bar_version_availability.ts",
                "bar_version_availability.source",
                "bar_version_availability.adjustment_basis",
                "bar_version_availability.version_recorded_at",
                "bar_version_availability.available_at",
            ],
            name=op.f("fk_forecast_realized_outcomes_exact_bar_receipt_bar_version_availability"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("outcome_id", name=op.f("pk_forecast_realized_outcomes")),
        sa.UniqueConstraint(
            "outcome_resolution_policy_hash",
            "availability_rule_set_hash",
            "symbol",
            "target",
            "series_basis",
            "target_time",
            name="uq_forecast_realized_outcomes_semantic_key",
        ),
    )
    op.create_index(
        "ix_forecast_realized_outcomes_target",
        "forecast_realized_outcomes",
        ["symbol", "target_time", "outcome_resolution_policy_hash"],
    )

    op.create_table(
        "forecast_outcome_cohort_manifests",
        sa.Column("cohort_id", sa.String(length=71), nullable=False),
        sa.Column("schema_version", sa.SmallInteger(), nullable=False),
        sa.Column("selection_policy_hash", sa.String(length=71), nullable=False),
        sa.Column("outcome_resolution_policy_hash", sa.String(length=71), nullable=False),
        sa.Column("availability_rule_set_hash", sa.String(length=71), nullable=False),
        sa.Column("purpose", sa.String(length=32), nullable=False),
        sa.Column("member_count", sa.Integer(), nullable=False),
        sa.Column("earliest_target_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("latest_target_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.Column("creator_xid", sa.BigInteger(), nullable=False),
        sa.Column("canonical_manifest", sa.LargeBinary(), nullable=False),
        sa.CheckConstraint(
            "schema_version = 1",
            name=op.f("ck_forecast_outcome_cohort_manifests_schema_version_supported"),
        ),
        sa.CheckConstraint(
            "cohort_id ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_outcome_cohort_manifests_cohort_id_format"),
        ),
        sa.CheckConstraint(
            "selection_policy_hash ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_outcome_cohort_manifests_selection_policy_hash_format"),
        ),
        sa.CheckConstraint(
            "outcome_resolution_policy_hash ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_outcome_cohort_manifests_outcome_resolution_policy_hash_format"),
        ),
        sa.CheckConstraint(
            "availability_rule_set_hash ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_outcome_cohort_manifests_availability_rule_set_hash_format"),
        ),
        sa.CheckConstraint(
            "purpose IN ('calibration_fit', 'heldout_evaluation')",
            name=op.f("ck_forecast_outcome_cohort_manifests_purpose_supported"),
        ),
        sa.CheckConstraint(
            "member_count BETWEEN 1 AND 10000",
            name=op.f("ck_forecast_outcome_cohort_manifests_member_count_bounded"),
        ),
        sa.CheckConstraint(
            "creator_xid > 0 AND recorded_at < earliest_target_time "
            "AND earliest_target_time <= latest_target_time",
            name=op.f("ck_forecast_outcome_cohort_manifests_time_order"),
        ),
        sa.CheckConstraint(
            "octet_length(canonical_manifest) BETWEEN 1 AND 4194304",
            name=op.f("ck_forecast_outcome_cohort_manifests_manifest_size_bounded"),
        ),
        sa.CheckConstraint(
            "cohort_id = 'sha256:' || encode(digest(canonical_manifest, 'sha256'), 'hex')",
            name=op.f("ck_forecast_outcome_cohort_manifests_cohort_id_matches_payload"),
        ),
        sa.PrimaryKeyConstraint("cohort_id", name=op.f("pk_forecast_outcome_cohort_manifests")),
    )
    op.create_index(
        "ix_forecast_outcome_cohorts_target_window",
        "forecast_outcome_cohort_manifests",
        ["earliest_target_time", "latest_target_time"],
    )
    op.create_table(
        "forecast_outcome_cohort_members",
        sa.Column("cohort_id", sa.String(length=71), nullable=False),
        sa.Column("forecast_id", sa.Uuid(), nullable=False),
        sa.Column("step", sa.SmallInteger(), nullable=False),
        sa.Column("target_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("opportunity_hash", sa.String(length=71), nullable=False),
        sa.Column("output_hash", sa.String(length=71), nullable=False),
        sa.CheckConstraint(
            "step BETWEEN 1 AND 252",
            name=op.f("ck_forecast_outcome_cohort_members_step_bounded"),
        ),
        sa.CheckConstraint(
            "opportunity_hash ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_outcome_cohort_members_opportunity_hash_format"),
        ),
        sa.CheckConstraint(
            "output_hash ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_outcome_cohort_members_output_hash_format"),
        ),
        sa.ForeignKeyConstraint(
            ["cohort_id"],
            ["forecast_outcome_cohort_manifests.cohort_id"],
            name=op.f(
                "fk_forecast_outcome_cohort_members_cohort_id_forecast_outcome_cohort_manifests"
            ),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["forecast_id"],
            ["forecast_runs.forecast_id"],
            name=op.f("fk_forecast_outcome_cohort_members_forecast_id_forecast_runs"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "cohort_id",
            "forecast_id",
            "step",
            name=op.f("pk_forecast_outcome_cohort_members"),
        ),
        sa.UniqueConstraint(
            "cohort_id",
            "opportunity_hash",
            "step",
            name="uq_forecast_outcome_cohort_members_opportunity_step",
        ),
    )
    op.create_index(
        "ix_forecast_outcome_cohort_members_target",
        "forecast_outcome_cohort_members",
        ["target_time", "cohort_id"],
    )
    op.create_table(
        "forecast_outcome_cohort_availability",
        sa.Column("cohort_id", sa.String(length=71), nullable=False),
        sa.Column("manifest_recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "sealed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.Column("sealer_xid", sa.BigInteger(), nullable=False),
        sa.CheckConstraint(
            "sealed_at >= manifest_recorded_at",
            name=op.f("ck_forecast_outcome_cohort_availability_not_before_recording"),
        ),
        sa.CheckConstraint(
            "sealer_xid > 0",
            name=op.f("ck_forecast_outcome_cohort_availability_sealer_xid_positive"),
        ),
        sa.ForeignKeyConstraint(
            ["cohort_id"],
            ["forecast_outcome_cohort_manifests.cohort_id"],
            name=op.f(
                "fk_forecast_outcome_cohort_availability_cohort_id_"
                "forecast_outcome_cohort_manifests"
            ),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("cohort_id", name=op.f("pk_forecast_outcome_cohort_availability")),
    )

    op.execute(
        r"""
        CREATE FUNCTION stamp_forecast_realized_outcome()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            document jsonb;
            payload jsonb;
            source_version jsonb;
            actual_close float8;
            actual_fetched_at timestamptz;
            actual_source_as_of timestamptz;
            actual_value_bits text;
        BEGIN
            IF NEW.canonical_evidence IS NULL
               OR octet_length(NEW.canonical_evidence) NOT BETWEEN 1 AND 262144 THEN
                RAISE EXCEPTION 'outcome evidence exceeds the storage bound'
                    USING ERRCODE = '22023';
            END IF;
            BEGIN
                document := convert_from(NEW.canonical_evidence, 'UTF8')::jsonb;
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'outcome evidence is not valid UTF-8 JSON'
                    USING ERRCODE = '22023';
            END;
            IF jsonb_typeof(document) IS DISTINCT FROM 'object'
               OR NOT (document ?& ARRAY['format', 'payload', 'schema_version'])
               OR EXISTS (
                   SELECT 1 FROM jsonb_object_keys(document) AS item(key)
                   WHERE key <> ALL (ARRAY['format', 'payload', 'schema_version'])
               )
               OR document->>'format' IS DISTINCT FROM 'forecast-realized-outcome-v1'
               OR jsonb_typeof(document->'schema_version') IS DISTINCT FROM 'number'
               OR document->>'schema_version' IS DISTINCT FROM '1' THEN
                RAISE EXCEPTION 'outcome evidence envelope is not supported'
                    USING ERRCODE = '22023';
            END IF;

            payload := document->'payload';
            IF jsonb_typeof(payload) IS DISTINCT FROM 'object'
               OR NOT (
                   payload ?& ARRAY[
                       'availability_rule_set_hash', 'currency',
                       'outcome_resolution_policy_hash', 'realized_value_f64',
                       'resolution_cutoff', 'series_basis', 'source_version',
                       'symbol', 'target', 'target_time'
                   ]
               )
               OR EXISTS (
                   SELECT 1 FROM jsonb_object_keys(payload) AS item(key)
                   WHERE key <> ALL (
                       ARRAY[
                           'availability_rule_set_hash', 'currency',
                           'outcome_resolution_policy_hash', 'realized_value_f64',
                           'resolution_cutoff', 'series_basis', 'source_version',
                           'symbol', 'target', 'target_time'
                       ]
                   )
               ) THEN
                RAISE EXCEPTION 'outcome evidence payload has invalid keys'
                    USING ERRCODE = '22023';
            END IF;

            source_version := payload->'source_version';
            IF jsonb_typeof(source_version) IS DISTINCT FROM 'object'
               OR NOT (
                   source_version ?& ARRAY[
                       'adjustment_basis', 'available_at', 'fetched_at', 'field',
                       'multiplier', 'observed_at', 'source', 'source_as_of',
                       'symbol', 'timespan', 'value_f64', 'version_recorded_at'
                   ]
               )
               OR EXISTS (
                   SELECT 1 FROM jsonb_object_keys(source_version) AS item(key)
                   WHERE key <> ALL (
                       ARRAY[
                           'adjustment_basis', 'available_at', 'fetched_at',
                           'field', 'multiplier', 'observed_at', 'source',
                           'source_as_of', 'symbol', 'timespan', 'value_f64',
                           'version_recorded_at'
                       ]
                   )
               )
               OR jsonb_typeof(source_version->'multiplier') IS DISTINCT FROM 'number'
               OR source_version->>'multiplier' IS DISTINCT FROM '1'
               OR source_version->>'value_f64' !~ '^[0-9a-f]{16}$'
               OR payload->>'realized_value_f64' !~ '^[0-9a-f]{16}$' THEN
                RAISE EXCEPTION 'outcome source-version evidence is malformed'
                    USING ERRCODE = '22023';
            END IF;

            BEGIN
                NEW.schema_version := (document->>'schema_version')::smallint;
                NEW.outcome_resolution_policy_hash :=
                    payload->>'outcome_resolution_policy_hash';
                NEW.availability_rule_set_hash :=
                    payload->>'availability_rule_set_hash';
                NEW.symbol := payload->>'symbol';
                NEW.target := payload->>'target';
                NEW.series_basis := payload->>'series_basis';
                NEW.currency := payload->>'currency';
                NEW.target_time := (payload->>'target_time')::timestamptz;
                NEW.resolution_cutoff :=
                    (payload->>'resolution_cutoff')::timestamptz;
                NEW.bar_timespan := source_version->>'timespan';
                NEW.bar_multiplier := (source_version->>'multiplier')::integer;
                NEW.bar_observed_at :=
                    (source_version->>'observed_at')::timestamptz;
                NEW.bar_source := source_version->>'source';
                NEW.bar_adjustment_basis := source_version->>'adjustment_basis';
                NEW.bar_version_recorded_at :=
                    (source_version->>'version_recorded_at')::timestamptz;
                NEW.bar_fetched_at :=
                    (source_version->>'fetched_at')::timestamptz;
                NEW.bar_source_as_of :=
                    (source_version->>'source_as_of')::timestamptz;
                NEW.bar_available_at :=
                    (source_version->>'available_at')::timestamptz;
                NEW.bar_field := source_version->>'field';
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'outcome evidence contains an invalid scalar'
                    USING ERRCODE = '22023';
            END;
            IF source_version->>'symbol' IS DISTINCT FROM NEW.symbol THEN
                RAISE EXCEPTION 'outcome source symbol does not match payload'
                    USING ERRCODE = '23000';
            END IF;

            BEGIN
                SELECT version.close, version.fetched_at, version.source_as_of
                INTO STRICT actual_close, actual_fetched_at, actual_source_as_of
                FROM (
                    SELECT bar.close, bar.fetched_at, bar.as_of AS source_as_of
                    FROM public.bars AS bar
                    WHERE bar.symbol = NEW.symbol
                      AND bar.timespan = NEW.bar_timespan
                      AND bar.multiplier = NEW.bar_multiplier
                      AND bar.ts = NEW.bar_observed_at
                      AND bar.source = NEW.bar_source
                      AND bar.adjustment_basis = NEW.bar_adjustment_basis
                      AND bar.recorded_at = NEW.bar_version_recorded_at
                    UNION
                    SELECT revision.previous_close,
                           revision.previous_fetched_at,
                           revision.previous_as_of
                    FROM public.bars_revisions AS revision
                    WHERE revision.symbol = NEW.symbol
                      AND revision.timespan = NEW.bar_timespan
                      AND revision.multiplier = NEW.bar_multiplier
                      AND revision.ts = NEW.bar_observed_at
                      AND revision.source = NEW.bar_source
                      AND revision.adjustment_basis = NEW.bar_adjustment_basis
                      AND revision.previous_recorded_at =
                          NEW.bar_version_recorded_at
                    UNION
                    SELECT revision.incoming_close,
                           revision.incoming_fetched_at,
                           revision.incoming_as_of
                    FROM public.bars_revisions AS revision
                    WHERE revision.symbol = NEW.symbol
                      AND revision.timespan = NEW.bar_timespan
                      AND revision.multiplier = NEW.bar_multiplier
                      AND revision.ts = NEW.bar_observed_at
                      AND revision.source = NEW.bar_source
                      AND revision.adjustment_basis = NEW.bar_adjustment_basis
                      AND revision.incoming_recorded_at =
                          NEW.bar_version_recorded_at
                ) AS version;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    RAISE EXCEPTION 'outcome does not identify a stored bar version'
                        USING ERRCODE = '23503';
                WHEN TOO_MANY_ROWS THEN
                    RAISE EXCEPTION 'stored bar-version evidence is inconsistent'
                        USING ERRCODE = '23000';
            END;

            actual_value_bits := encode(
                float8send(
                    CASE WHEN actual_close = 0 THEN 0::float8 ELSE actual_close END
                ),
                'hex'
            );
            IF NEW.bar_fetched_at IS DISTINCT FROM actual_fetched_at
               OR NEW.bar_source_as_of IS DISTINCT FROM actual_source_as_of
               OR source_version->>'value_f64' IS DISTINCT FROM actual_value_bits
               OR payload->>'realized_value_f64' IS DISTINCT FROM actual_value_bits THEN
                RAISE EXCEPTION 'outcome value does not match its exact bar version'
                    USING ERRCODE = '23000';
            END IF;

            PERFORM 1
            FROM public.bar_version_availability AS receipt
            WHERE receipt.symbol = NEW.symbol
              AND receipt.timespan = NEW.bar_timespan
              AND receipt.multiplier = NEW.bar_multiplier
              AND receipt.ts = NEW.bar_observed_at
              AND receipt.source = NEW.bar_source
              AND receipt.adjustment_basis = NEW.bar_adjustment_basis
              AND receipt.version_recorded_at = NEW.bar_version_recorded_at
              AND receipt.available_at = NEW.bar_available_at;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'outcome does not identify a stored availability receipt'
                    USING ERRCODE = '23503';
            END IF;

            NEW.bar_value :=
                CASE WHEN actual_close = 0 THEN 0::float8 ELSE actual_close END;
            NEW.realized_value := NEW.bar_value;
            NEW.sealed_at := clock_timestamp();
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        r"""
        CREATE FUNCTION stamp_forecast_outcome_cohort_manifest()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            document jsonb;
            members jsonb;
            member jsonb;
            member_target timestamptz;
            first_target timestamptz;
            last_target timestamptz;
        BEGIN
            IF NEW.canonical_manifest IS NULL
               OR octet_length(NEW.canonical_manifest) NOT BETWEEN 1 AND 4194304 THEN
                RAISE EXCEPTION 'cohort manifest exceeds the storage bound'
                    USING ERRCODE = '22023';
            END IF;
            BEGIN
                document := convert_from(NEW.canonical_manifest, 'UTF8')::jsonb;
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'cohort manifest is not valid UTF-8 JSON'
                    USING ERRCODE = '22023';
            END;
            IF jsonb_typeof(document) IS DISTINCT FROM 'object'
               OR NOT (
                   document ?& ARRAY[
                       'availability_rule_set_hash', 'format', 'members',
                       'outcome_resolution_policy_hash', 'purpose',
                       'schema_version', 'selection_policy_hash'
                   ]
               )
               OR EXISTS (
                   SELECT 1 FROM jsonb_object_keys(document) AS item(key)
                   WHERE key <> ALL (
                       ARRAY[
                           'availability_rule_set_hash', 'format', 'members',
                           'outcome_resolution_policy_hash', 'purpose',
                           'schema_version', 'selection_policy_hash'
                       ]
                   )
               )
               OR document->>'format' IS DISTINCT FROM 'forecast-outcome-cohort-v1'
               OR jsonb_typeof(document->'schema_version') IS DISTINCT FROM 'number'
               OR document->>'schema_version' IS DISTINCT FROM '1'
               OR jsonb_typeof(document->'members') IS DISTINCT FROM 'array' THEN
                RAISE EXCEPTION 'cohort manifest envelope is not supported'
                    USING ERRCODE = '22023';
            END IF;

            members := document->'members';
            IF jsonb_array_length(members) NOT BETWEEN 1 AND 10000 THEN
                RAISE EXCEPTION 'cohort member count is outside supported bounds'
                    USING ERRCODE = '22023';
            END IF;
            FOR member IN SELECT value FROM jsonb_array_elements(members) AS item(value)
            LOOP
                IF jsonb_typeof(member) IS DISTINCT FROM 'object'
                   OR NOT (
                       member ?& ARRAY[
                           'forecast_id', 'opportunity_hash', 'output_hash',
                           'step', 'target_time'
                       ]
                   )
                   OR EXISTS (
                       SELECT 1 FROM jsonb_object_keys(member) AS item(key)
                       WHERE key <> ALL (
                           ARRAY[
                               'forecast_id', 'opportunity_hash', 'output_hash',
                               'step', 'target_time'
                           ]
                       )
                   )
                   OR jsonb_typeof(member->'step') IS DISTINCT FROM 'number'
                   OR member->>'step' !~ '^[0-9]{1,3}$'
                   OR (member->>'step')::integer NOT BETWEEN 1 AND 252 THEN
                    RAISE EXCEPTION 'cohort member evidence is malformed'
                        USING ERRCODE = '22023';
                END IF;
                BEGIN
                    member_target := (member->>'target_time')::timestamptz;
                EXCEPTION WHEN OTHERS THEN
                    RAISE EXCEPTION 'cohort member target_time is invalid'
                        USING ERRCODE = '22023';
                END;
                first_target := LEAST(first_target, member_target);
                last_target := GREATEST(last_target, member_target);
            END LOOP;

            NEW.schema_version := (document->>'schema_version')::smallint;
            NEW.selection_policy_hash := document->>'selection_policy_hash';
            NEW.outcome_resolution_policy_hash :=
                document->>'outcome_resolution_policy_hash';
            NEW.availability_rule_set_hash :=
                document->>'availability_rule_set_hash';
            NEW.purpose := document->>'purpose';
            NEW.member_count := jsonb_array_length(members);
            NEW.earliest_target_time := first_target;
            NEW.latest_target_time := last_target;
            NEW.recorded_at := clock_timestamp();
            NEW.creator_xid := txid_current();
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        r"""
        CREATE FUNCTION validate_forecast_outcome_cohort_member()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            manifest_creator bigint;
            archived_output bytea;
            output_document jsonb;
            matching_steps integer;
        BEGIN
            SELECT creator_xid
            INTO STRICT manifest_creator
            FROM public.forecast_outcome_cohort_manifests
            WHERE cohort_id = NEW.cohort_id;
            IF manifest_creator <> txid_current() THEN
                RAISE EXCEPTION 'cohort membership is immutable after manifest commit'
                    USING ERRCODE = '55000';
            END IF;

            BEGIN
                SELECT canonical_output
                INTO STRICT archived_output
                FROM public.forecast_runs
                WHERE forecast_id = NEW.forecast_id
                  AND origin_kind = 'scheduled_evaluation'
                  AND opportunity_hash = NEW.opportunity_hash
                  AND output_hash = NEW.output_hash;
            EXCEPTION WHEN NO_DATA_FOUND THEN
                RAISE EXCEPTION 'cohort member is not an exact scheduled forecast run'
                    USING ERRCODE = '23503';
            END;

            BEGIN
                output_document := convert_from(archived_output, 'UTF8')::jsonb;
                SELECT count(*)
                INTO matching_steps
                FROM jsonb_array_elements(
                    output_document->'payload'->'forecasts'
                ) AS forecast_step(value)
                WHERE jsonb_typeof(value) = 'object'
                  AND (value->>'step')::integer = NEW.step
                  AND (value->>'target_time')::timestamptz = NEW.target_time;
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'scheduled forecast output cannot validate cohort member'
                    USING ERRCODE = '23000';
            END;
            IF matching_steps <> 1
               OR output_document->'payload'->'provenance'->>'forecast_id'
                  IS DISTINCT FROM NEW.forecast_id::text THEN
                RAISE EXCEPTION 'cohort member step does not match scheduled output'
                    USING ERRCODE = '23000';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        r"""
        CREATE FUNCTION materialize_forecast_outcome_cohort_members()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            document jsonb;
            member jsonb;
        BEGIN
            document := convert_from(NEW.canonical_manifest, 'UTF8')::jsonb;
            FOR member IN
                SELECT value
                FROM jsonb_array_elements(document->'members') AS item(value)
            LOOP
                BEGIN
                    INSERT INTO public.forecast_outcome_cohort_members (
                        cohort_id, forecast_id, step, target_time,
                        opportunity_hash, output_hash
                    ) VALUES (
                        NEW.cohort_id,
                        (member->>'forecast_id')::uuid,
                        (member->>'step')::smallint,
                        (member->>'target_time')::timestamptz,
                        member->>'opportunity_hash',
                        member->>'output_hash'
                    );
                EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
                    RAISE EXCEPTION 'cohort member contains an invalid scalar'
                        USING ERRCODE = '22023';
                END;
            END LOOP;
            RETURN NULL;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION stamp_forecast_outcome_cohort_availability()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            manifest_recorded timestamptz;
            manifest_creator bigint;
            first_target timestamptz;
            stamped timestamptz;
        BEGIN
            SELECT manifest.recorded_at, manifest.creator_xid,
                   min(member.target_time)
            INTO STRICT manifest_recorded, manifest_creator, first_target
            FROM public.forecast_outcome_cohort_manifests AS manifest
            JOIN public.forecast_outcome_cohort_members AS member
              ON member.cohort_id = manifest.cohort_id
            WHERE manifest.cohort_id = NEW.cohort_id
            GROUP BY manifest.recorded_at, manifest.creator_xid;

            IF manifest_creator = txid_current() THEN
                RAISE EXCEPTION 'cohort availability requires a later transaction'
                    USING ERRCODE = '55000';
            END IF;
            stamped := clock_timestamp();
            IF stamped >= first_target THEN
                RAISE EXCEPTION 'cohort was not committed before its first target'
                    USING ERRCODE = '55000';
            END IF;
            NEW.manifest_recorded_at := manifest_recorded;
            NEW.sealed_at := stamped;
            NEW.sealer_xid := txid_current();
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION reject_forecast_evidence_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        BEGIN
            RAISE EXCEPTION 'forecast evidence is insert-only'
                USING ERRCODE = '55000';
        END;
        $$
        """
    )

    op.execute(
        "CREATE TRIGGER forecast_realized_outcomes_stamp "
        "BEFORE INSERT ON forecast_realized_outcomes FOR EACH ROW "
        "EXECUTE FUNCTION stamp_forecast_realized_outcome()"
    )
    op.execute(
        "CREATE TRIGGER forecast_outcome_cohorts_stamp "
        "BEFORE INSERT ON forecast_outcome_cohort_manifests FOR EACH ROW "
        "EXECUTE FUNCTION stamp_forecast_outcome_cohort_manifest()"
    )
    op.execute(
        "CREATE TRIGGER forecast_outcome_cohort_members_validate "
        "BEFORE INSERT ON forecast_outcome_cohort_members FOR EACH ROW "
        "EXECUTE FUNCTION validate_forecast_outcome_cohort_member()"
    )
    op.execute(
        "CREATE TRIGGER forecast_outcome_cohorts_materialize_members "
        "AFTER INSERT ON forecast_outcome_cohort_manifests FOR EACH ROW "
        "EXECUTE FUNCTION materialize_forecast_outcome_cohort_members()"
    )
    op.execute(
        "CREATE TRIGGER forecast_outcome_cohort_availability_stamp "
        "BEFORE INSERT ON forecast_outcome_cohort_availability FOR EACH ROW "
        "EXECUTE FUNCTION stamp_forecast_outcome_cohort_availability()"
    )
    for table in (
        "forecast_realized_outcomes",
        "forecast_outcome_cohort_manifests",
        "forecast_outcome_cohort_members",
        "forecast_outcome_cohort_availability",
    ):
        op.execute(
            f"CREATE TRIGGER {table}_no_row_mutation BEFORE UPDATE OR DELETE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION reject_forecast_evidence_mutation()"
        )
        op.execute(
            f"CREATE TRIGGER {table}_no_truncate BEFORE TRUNCATE ON {table} "
            "FOR EACH STATEMENT EXECUTE FUNCTION reject_forecast_evidence_mutation()"
        )

    tables = (
        "forecast_realized_outcomes",
        "forecast_outcome_cohort_manifests",
        "forecast_outcome_cohort_members",
        "forecast_outcome_cohort_availability",
    )
    for table in tables:
        op.execute(f"REVOKE ALL ON TABLE public.{table} FROM PUBLIC")
        op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM stockapi_app")
        op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM stockapi_snapshot_builder")
        privileges = "SELECT" if table == "forecast_outcome_cohort_members" else "SELECT, INSERT"
        op.execute(f"GRANT {privileges} ON TABLE public.{table} TO stockapi_app")
    op.execute(
        "REVOKE ALL ON FUNCTION public.stamp_forecast_realized_outcome(), "
        "public.stamp_forecast_outcome_cohort_manifest(), "
        "public.validate_forecast_outcome_cohort_member(), "
        "public.materialize_forecast_outcome_cohort_members(), "
        "public.stamp_forecast_outcome_cohort_availability(), "
        "public.reject_forecast_evidence_mutation() "
        "FROM PUBLIC, stockapi_app, stockapi_snapshot_builder"
    )
    op.execute(
        """
        DO $$
        DECLARE
            app_role oid;
            builder_role oid;
            relation_name text;
            function_name text;
            app_may_insert boolean;
        BEGIN
            SELECT oid INTO STRICT app_role FROM pg_roles WHERE rolname = 'stockapi_app';
            SELECT oid INTO STRICT builder_role
            FROM pg_roles WHERE rolname = 'stockapi_snapshot_builder';

            FOREACH relation_name IN ARRAY ARRAY[
                'forecast_realized_outcomes',
                'forecast_outcome_cohort_manifests',
                'forecast_outcome_cohort_members',
                'forecast_outcome_cohort_availability'
            ] LOOP
                app_may_insert :=
                    relation_name <> 'forecast_outcome_cohort_members';
                IF NOT has_table_privilege(app_role, 'public.' || relation_name, 'SELECT')
                   OR has_table_privilege(
                       app_role, 'public.' || relation_name, 'INSERT'
                   ) IS DISTINCT FROM app_may_insert
                   OR has_table_privilege(app_role, 'public.' || relation_name, 'UPDATE')
                   OR has_table_privilege(app_role, 'public.' || relation_name, 'DELETE')
                   OR has_table_privilege(app_role, 'public.' || relation_name, 'TRUNCATE')
                   OR has_table_privilege(app_role, 'public.' || relation_name, 'REFERENCES')
                   OR has_table_privilege(app_role, 'public.' || relation_name, 'TRIGGER')
                   OR has_table_privilege(app_role, 'public.' || relation_name, 'MAINTAIN')
                   OR has_any_column_privilege(
                       app_role, 'public.' || relation_name, 'INSERT'
                   ) IS DISTINCT FROM app_may_insert
                   OR has_any_column_privilege(
                       app_role, 'public.' || relation_name, 'UPDATE'
                   )
                   OR has_any_column_privilege(
                       app_role, 'public.' || relation_name, 'REFERENCES'
                   ) THEN
                    RAISE EXCEPTION 'runtime forecast-evidence privileges are not exact';
                END IF;
                IF has_table_privilege(builder_role, 'public.' || relation_name, 'SELECT')
                   OR has_table_privilege(builder_role, 'public.' || relation_name, 'INSERT')
                   OR has_table_privilege(builder_role, 'public.' || relation_name, 'UPDATE')
                   OR has_table_privilege(builder_role, 'public.' || relation_name, 'DELETE')
                   OR has_table_privilege(builder_role, 'public.' || relation_name, 'TRUNCATE')
                   OR has_table_privilege(builder_role, 'public.' || relation_name, 'REFERENCES')
                   OR has_table_privilege(builder_role, 'public.' || relation_name, 'TRIGGER')
                   OR has_table_privilege(builder_role, 'public.' || relation_name, 'MAINTAIN')
                   OR has_any_column_privilege(
                       builder_role, 'public.' || relation_name, 'SELECT'
                   )
                   OR has_any_column_privilege(
                       builder_role, 'public.' || relation_name, 'INSERT'
                   )
                   OR has_any_column_privilege(
                       builder_role, 'public.' || relation_name, 'UPDATE'
                   )
                   OR has_any_column_privilege(
                       builder_role, 'public.' || relation_name, 'REFERENCES'
                   ) THEN
                    RAISE EXCEPTION 'snapshot builder forecast-evidence privileges are not empty';
                END IF;
            END LOOP;

            FOREACH function_name IN ARRAY ARRAY[
                'stamp_forecast_realized_outcome()',
                'stamp_forecast_outcome_cohort_manifest()',
                'validate_forecast_outcome_cohort_member()',
                'materialize_forecast_outcome_cohort_members()',
                'stamp_forecast_outcome_cohort_availability()',
                'reject_forecast_evidence_mutation()'
            ] LOOP
                IF has_function_privilege(app_role, 'public.' || function_name, 'EXECUTE')
                   OR has_function_privilege(
                       builder_role, 'public.' || function_name, 'EXECUTE'
                   ) THEN
                    RAISE EXCEPTION 'forecast-evidence trigger function is executable';
                END IF;
            END LOOP;
        END;
        $$
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        DECLARE relation_name text;
        BEGIN
            FOREACH relation_name IN ARRAY ARRAY[
                'forecast_realized_outcomes',
                'forecast_outcome_cohort_manifests',
                'forecast_outcome_cohort_members',
                'forecast_outcome_cohort_availability'
            ] LOOP
                IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stockapi_app') THEN
                    EXECUTE format(
                        'REVOKE ALL PRIVILEGES ON TABLE public.%I FROM stockapi_app',
                        relation_name
                    );
                END IF;
                IF EXISTS (
                    SELECT 1 FROM pg_roles WHERE rolname = 'stockapi_snapshot_builder'
                ) THEN
                    EXECUTE format(
                        'REVOKE ALL PRIVILEGES ON TABLE public.%I '
                        'FROM stockapi_snapshot_builder', relation_name
                    );
                END IF;
            END LOOP;
        END;
        $$
        """
    )
    for table in (
        "forecast_realized_outcomes",
        "forecast_outcome_cohort_manifests",
        "forecast_outcome_cohort_members",
        "forecast_outcome_cohort_availability",
    ):
        op.execute(f"REVOKE ALL ON TABLE public.{table} FROM PUBLIC")

    for table in (
        "forecast_realized_outcomes",
        "forecast_outcome_cohort_manifests",
        "forecast_outcome_cohort_members",
        "forecast_outcome_cohort_availability",
    ):
        op.execute(f"DROP TRIGGER IF EXISTS {table}_no_truncate ON {table}")
        op.execute(f"DROP TRIGGER IF EXISTS {table}_no_row_mutation ON {table}")
    op.execute(
        "DROP TRIGGER IF EXISTS forecast_outcome_cohort_availability_stamp "
        "ON forecast_outcome_cohort_availability"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS forecast_outcome_cohorts_materialize_members "
        "ON forecast_outcome_cohort_manifests"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS forecast_outcome_cohort_members_validate "
        "ON forecast_outcome_cohort_members"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS forecast_outcome_cohorts_stamp ON forecast_outcome_cohort_manifests"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS forecast_realized_outcomes_stamp ON forecast_realized_outcomes"
    )
    op.execute("DROP FUNCTION IF EXISTS reject_forecast_evidence_mutation()")
    op.execute("DROP FUNCTION IF EXISTS stamp_forecast_outcome_cohort_availability()")
    op.execute("DROP FUNCTION IF EXISTS materialize_forecast_outcome_cohort_members()")
    op.execute("DROP FUNCTION IF EXISTS validate_forecast_outcome_cohort_member()")
    op.execute("DROP FUNCTION IF EXISTS stamp_forecast_outcome_cohort_manifest()")
    op.execute("DROP FUNCTION IF EXISTS stamp_forecast_realized_outcome()")
    op.drop_table("forecast_outcome_cohort_availability")
    op.drop_index(
        "ix_forecast_outcome_cohort_members_target",
        table_name="forecast_outcome_cohort_members",
    )
    op.drop_table("forecast_outcome_cohort_members")
    op.drop_index(
        "ix_forecast_outcome_cohorts_target_window",
        table_name="forecast_outcome_cohort_manifests",
    )
    op.drop_table("forecast_outcome_cohort_manifests")
    op.drop_index(
        "ix_forecast_realized_outcomes_target",
        table_name="forecast_realized_outcomes",
    )
    op.drop_table("forecast_realized_outcomes")
    op.drop_constraint(
        "uq_bar_version_availability_exact_receipt",
        "bar_version_availability",
        type_="unique",
    )
