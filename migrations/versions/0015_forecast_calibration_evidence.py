"""add immutable fitted calibration and descriptive held-out evidence

Revision ID: 0015_calibration_evidence
Revises: 0014_vendor_campaign_anchor
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0015_calibration_evidence"
down_revision: str | None = "0014_vendor_campaign_anchor"
branch_labels: str | None = None
depends_on: str | None = None


_TABLES = (
    "forecast_fitted_calibration_sets",
    "forecast_heldout_coverage_releases",
    "forecast_heldout_coverage_release_buckets",
    "forecast_heldout_coverage_release_availability",
)


def upgrade() -> None:
    op.create_table(
        "forecast_fitted_calibration_sets",
        sa.Column("calibration_set_version", sa.String(length=71), nullable=False),
        sa.Column("schema_version", sa.SmallInteger(), nullable=False),
        sa.Column("model_version", sa.String(length=128), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("target", sa.String(length=32), nullable=False),
        sa.Column("series_basis", sa.String(length=32), nullable=False),
        sa.Column("horizon_unit", sa.String(length=32), nullable=False),
        sa.Column("currency", sa.String(length=16), nullable=False),
        sa.Column("source_calibration_set_version", sa.String(length=128), nullable=False),
        sa.Column("source_calibration_method", sa.String(length=32), nullable=False),
        sa.Column("forecast_resolution_policy_hash", sa.String(length=71), nullable=False),
        sa.Column("forecast_availability_rule_set_hash", sa.String(length=71), nullable=False),
        sa.Column("fit_evidence_digest", sa.String(length=71), nullable=False),
        sa.Column("method", sa.String(length=32), nullable=False),
        sa.Column("window_start", sa.Date(), nullable=False),
        sa.Column("window_end", sa.Date(), nullable=False),
        sa.Column("sample_count", sa.Integer(), nullable=False),
        sa.Column("cohort_id", sa.String(length=71), nullable=False),
        sa.Column("selection_policy_hash", sa.String(length=71), nullable=False),
        sa.Column("outcome_resolution_policy_hash", sa.String(length=71), nullable=False),
        sa.Column("outcome_availability_rule_set_hash", sa.String(length=71), nullable=False),
        sa.Column("interval_policy_version", sa.String(length=64), nullable=False),
        sa.Column("window_date_policy_version", sa.String(length=64), nullable=False),
        sa.Column("bucket_count", sa.Integer(), nullable=False),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.Column("creator_xid", sa.BigInteger(), nullable=False),
        sa.Column("canonical_set", sa.LargeBinary(), nullable=False),
        sa.CheckConstraint(
            "schema_version = 2",
            name=op.f("ck_forecast_fitted_calibration_sets_schema_version_supported"),
        ),
        sa.CheckConstraint(
            "calibration_set_version ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_fitted_calibration_sets_calibration_set_version_format"),
        ),
        sa.CheckConstraint(
            "forecast_resolution_policy_hash ~ '^sha256:[0-9a-f]{64}$' "
            "AND forecast_availability_rule_set_hash ~ '^sha256:[0-9a-f]{64}$' "
            "AND fit_evidence_digest ~ '^sha256:[0-9a-f]{64}$' "
            "AND cohort_id ~ '^sha256:[0-9a-f]{64}$' "
            "AND selection_policy_hash ~ '^sha256:[0-9a-f]{64}$' "
            "AND outcome_resolution_policy_hash ~ '^sha256:[0-9a-f]{64}$' "
            "AND outcome_availability_rule_set_hash ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_fitted_calibration_sets_hashes_format"),
        ),
        sa.CheckConstraint(
            "symbol = upper(symbol)",
            name=op.f("ck_forecast_fitted_calibration_sets_symbol_uppercase"),
        ),
        sa.CheckConstraint(
            "symbol ~ '^[A-Z0-9.\\-_:]+$'",
            name=op.f("ck_forecast_fitted_calibration_sets_symbol_format"),
        ),
        sa.CheckConstraint(
            "target = 'close' AND series_basis = 'raw' "
            "AND horizon_unit = 'trading_day' AND currency = 'USD'",
            name=op.f("ck_forecast_fitted_calibration_sets_semantic_scope_supported"),
        ),
        sa.CheckConstraint(
            "method IN ('empirical_residual', 'conformal_quantile_regression')",
            name=op.f("ck_forecast_fitted_calibration_sets_method_supported"),
        ),
        sa.CheckConstraint(
            "source_calibration_method = 'none' "
            "AND source_calibration_set_version = 'uncalibrated:' || model_version",
            name=op.f("ck_forecast_fitted_calibration_sets_source_uncalibrated"),
        ),
        sa.CheckConstraint(
            "interval_policy_version = 'central-equal-tailed-v1' "
            "AND window_date_policy_version = 'utc-target-date-v1'",
            name=op.f("ck_forecast_fitted_calibration_sets_policies_supported"),
        ),
        sa.CheckConstraint(
            "window_start <= window_end",
            name=op.f("ck_forecast_fitted_calibration_sets_window_order"),
        ),
        sa.CheckConstraint(
            "sample_count BETWEEN 1 AND 10000",
            name=op.f("ck_forecast_fitted_calibration_sets_sample_count_bounded"),
        ),
        sa.CheckConstraint(
            "bucket_count BETWEEN 1 AND 10000",
            name=op.f("ck_forecast_fitted_calibration_sets_bucket_count_bounded"),
        ),
        sa.CheckConstraint(
            "creator_xid > 0",
            name=op.f("ck_forecast_fitted_calibration_sets_creator_xid_positive"),
        ),
        sa.CheckConstraint(
            "octet_length(canonical_set) BETWEEN 1 AND 4194304",
            name=op.f("ck_forecast_fitted_calibration_sets_canonical_set_size_bounded"),
        ),
        sa.CheckConstraint(
            "calibration_set_version = 'sha256:' || encode(digest(canonical_set, 'sha256'), 'hex')",
            name=op.f(
                "ck_forecast_fitted_calibration_sets_calibration_set_version_matches_payload"
            ),
        ),
        sa.ForeignKeyConstraint(
            ["cohort_id"],
            ["forecast_outcome_cohort_availability.cohort_id"],
            name=op.f(
                "fk_forecast_fitted_calibration_sets_cohort_id_forecast_outcome_cohort_availability"
            ),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["outcome_resolution_policy_hash", "outcome_availability_rule_set_hash"],
            [
                "forecast_outcome_resolution_policies.policy_hash",
                "forecast_outcome_resolution_policies.availability_rule_set_hash",
            ],
            name="fk_fitted_calibration_sets_registered_outcome_policy",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "cohort_id",
            "method",
            name="uq_fitted_calibration_sets_cohort_method",
        ),
        sa.PrimaryKeyConstraint(
            "calibration_set_version",
            name=op.f("pk_forecast_fitted_calibration_sets"),
        ),
    )
    op.create_index(
        "ix_forecast_fitted_calibration_sets_cohort_id",
        "forecast_fitted_calibration_sets",
        ["cohort_id"],
    )

    op.create_table(
        "forecast_heldout_coverage_releases",
        sa.Column("release_id", sa.String(length=71), nullable=False),
        sa.Column("schema_version", sa.SmallInteger(), nullable=False),
        sa.Column("evidence_scope", sa.String(length=32), nullable=False),
        sa.Column("fitted_calibration_set_version", sa.String(length=71), nullable=False),
        sa.Column("method", sa.String(length=32), nullable=False),
        sa.Column("model_version", sa.String(length=128), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("target", sa.String(length=32), nullable=False),
        sa.Column("series_basis", sa.String(length=32), nullable=False),
        sa.Column("horizon_unit", sa.String(length=32), nullable=False),
        sa.Column("currency", sa.String(length=16), nullable=False),
        sa.Column("fit_cohort_id", sa.String(length=71), nullable=False),
        sa.Column("fit_selection_policy_hash", sa.String(length=71), nullable=False),
        sa.Column("heldout_cohort_id", sa.String(length=71), nullable=False),
        sa.Column("heldout_selection_policy_hash", sa.String(length=71), nullable=False),
        sa.Column("outcome_resolution_policy_hash", sa.String(length=71), nullable=False),
        sa.Column("outcome_availability_rule_set_hash", sa.String(length=71), nullable=False),
        sa.Column("forecast_resolution_policy_hash", sa.String(length=71), nullable=False),
        sa.Column("forecast_availability_rule_set_hash", sa.String(length=71), nullable=False),
        sa.Column("fit_evidence_digest", sa.String(length=71), nullable=False),
        sa.Column("heldout_evidence_digest", sa.String(length=71), nullable=False),
        sa.Column("heldout_window_start", sa.Date(), nullable=False),
        sa.Column("heldout_window_end", sa.Date(), nullable=False),
        sa.Column("heldout_sample_count", sa.Integer(), nullable=False),
        sa.Column("confidence_level_f64_be", sa.LargeBinary(), nullable=False),
        sa.Column("interval_policy_version", sa.String(length=64), nullable=False),
        sa.Column("window_date_policy_version", sa.String(length=64), nullable=False),
        sa.Column("estimator_policy_version", sa.String(length=64), nullable=False),
        sa.Column("bucket_count", sa.Integer(), nullable=False),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.Column("creator_xid", sa.BigInteger(), nullable=False),
        sa.Column("canonical_release", sa.LargeBinary(), nullable=False),
        sa.CheckConstraint(
            "schema_version = 1",
            name=op.f("ck_forecast_heldout_coverage_releases_schema_version_supported"),
        ),
        sa.CheckConstraint(
            "evidence_scope = 'descriptive-only'",
            name=op.f("ck_forecast_heldout_coverage_releases_scope_descriptive_only"),
        ),
        sa.CheckConstraint(
            "release_id ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_heldout_coverage_releases_release_id_format"),
        ),
        sa.CheckConstraint(
            "fitted_calibration_set_version ~ '^sha256:[0-9a-f]{64}$' "
            "AND fit_cohort_id ~ '^sha256:[0-9a-f]{64}$' "
            "AND fit_selection_policy_hash ~ '^sha256:[0-9a-f]{64}$' "
            "AND heldout_cohort_id ~ '^sha256:[0-9a-f]{64}$' "
            "AND heldout_selection_policy_hash ~ '^sha256:[0-9a-f]{64}$' "
            "AND outcome_resolution_policy_hash ~ '^sha256:[0-9a-f]{64}$' "
            "AND outcome_availability_rule_set_hash ~ '^sha256:[0-9a-f]{64}$' "
            "AND forecast_resolution_policy_hash ~ '^sha256:[0-9a-f]{64}$' "
            "AND forecast_availability_rule_set_hash ~ '^sha256:[0-9a-f]{64}$' "
            "AND fit_evidence_digest ~ '^sha256:[0-9a-f]{64}$' "
            "AND heldout_evidence_digest ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_heldout_coverage_releases_hashes_format"),
        ),
        sa.CheckConstraint(
            "symbol = upper(symbol)",
            name=op.f("ck_forecast_heldout_coverage_releases_symbol_uppercase"),
        ),
        sa.CheckConstraint(
            "symbol ~ '^[A-Z0-9.\\-_:]+$'",
            name=op.f("ck_forecast_heldout_coverage_releases_symbol_format"),
        ),
        sa.CheckConstraint(
            "target = 'close' AND series_basis = 'raw' "
            "AND horizon_unit = 'trading_day' AND currency = 'USD'",
            name=op.f("ck_forecast_heldout_coverage_releases_semantic_scope_supported"),
        ),
        sa.CheckConstraint(
            "method IN ('empirical_residual', 'conformal_quantile_regression')",
            name=op.f("ck_forecast_heldout_coverage_releases_method_supported"),
        ),
        sa.CheckConstraint(
            "interval_policy_version = 'central-equal-tailed-v1' "
            "AND window_date_policy_version = 'utc-target-date-v1' "
            "AND estimator_policy_version = 'wilson-score-two-sided-v1'",
            name=op.f("ck_forecast_heldout_coverage_releases_policies_supported"),
        ),
        sa.CheckConstraint(
            "fit_cohort_id <> heldout_cohort_id",
            name=op.f("ck_forecast_heldout_coverage_releases_cohorts_distinct"),
        ),
        sa.CheckConstraint(
            "heldout_window_start <= heldout_window_end",
            name=op.f("ck_forecast_heldout_coverage_releases_heldout_window_order"),
        ),
        sa.CheckConstraint(
            "heldout_sample_count BETWEEN 1 AND 10000",
            name=op.f("ck_forecast_heldout_coverage_releases_heldout_sample_count_bounded"),
        ),
        sa.CheckConstraint(
            "bucket_count BETWEEN 1 AND 10000",
            name=op.f("ck_forecast_heldout_coverage_releases_bucket_count_bounded"),
        ),
        sa.CheckConstraint(
            "octet_length(confidence_level_f64_be) = 8",
            name=op.f("ck_forecast_heldout_coverage_releases_confidence_level_f64_size"),
        ),
        sa.CheckConstraint(
            "creator_xid > 0",
            name=op.f("ck_forecast_heldout_coverage_releases_creator_xid_positive"),
        ),
        sa.CheckConstraint(
            "octet_length(canonical_release) BETWEEN 1 AND 4194304",
            name=op.f("ck_forecast_heldout_coverage_releases_canonical_release_size_bounded"),
        ),
        sa.CheckConstraint(
            "release_id = 'sha256:' || encode(digest(canonical_release, 'sha256'), 'hex')",
            name=op.f("ck_forecast_heldout_coverage_releases_release_id_matches_payload"),
        ),
        sa.ForeignKeyConstraint(
            ["fitted_calibration_set_version"],
            ["forecast_fitted_calibration_sets.calibration_set_version"],
            name=op.f(
                "fk_forecast_heldout_coverage_releases_fitted_calibration_set_version_"
                "forecast_fitted_calibration_sets"
            ),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["fit_cohort_id"],
            ["forecast_outcome_cohort_availability.cohort_id"],
            name="fk_heldout_coverage_releases_fit_cohort",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["heldout_cohort_id"],
            ["forecast_outcome_cohort_availability.cohort_id"],
            name="fk_heldout_coverage_releases_heldout_cohort",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["outcome_resolution_policy_hash", "outcome_availability_rule_set_hash"],
            [
                "forecast_outcome_resolution_policies.policy_hash",
                "forecast_outcome_resolution_policies.availability_rule_set_hash",
            ],
            name="fk_heldout_coverage_releases_registered_outcome_policy",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("release_id", name=op.f("pk_forecast_heldout_coverage_releases")),
        sa.UniqueConstraint(
            "heldout_cohort_id",
            "fitted_calibration_set_version",
            "confidence_level_f64_be",
            "estimator_policy_version",
            name="uq_heldout_coverage_releases_exact_analysis",
        ),
    )
    op.create_index(
        "ix_forecast_heldout_coverage_releases_fitted_set",
        "forecast_heldout_coverage_releases",
        ["fitted_calibration_set_version"],
    )
    op.create_index(
        "ix_forecast_heldout_coverage_releases_heldout_cohort",
        "forecast_heldout_coverage_releases",
        ["heldout_cohort_id"],
    )

    op.create_table(
        "forecast_heldout_coverage_release_buckets",
        sa.Column("release_id", sa.String(length=71), nullable=False),
        sa.Column("horizon", sa.SmallInteger(), nullable=False),
        sa.Column("coverage_millis", sa.SmallInteger(), nullable=False),
        sa.Column("covered_count", sa.Integer(), nullable=False),
        sa.Column("sample_count", sa.Integer(), nullable=False),
        sa.Column("empirical_coverage_f64_be", sa.LargeBinary(), nullable=False),
        sa.Column("confidence_low_f64_be", sa.LargeBinary(), nullable=False),
        sa.Column("confidence_high_f64_be", sa.LargeBinary(), nullable=False),
        sa.CheckConstraint(
            "horizon BETWEEN 1 AND 252",
            name=op.f("ck_forecast_heldout_coverage_release_buckets_horizon_bounded"),
        ),
        sa.CheckConstraint(
            "coverage_millis BETWEEN 1 AND 999",
            name=op.f("ck_forecast_heldout_coverage_release_buckets_coverage_millis_bounded"),
        ),
        sa.CheckConstraint(
            "covered_count BETWEEN 0 AND sample_count",
            name=op.f("ck_forecast_heldout_coverage_release_buckets_covered_count_bounded"),
        ),
        sa.CheckConstraint(
            "sample_count BETWEEN 1 AND 10000",
            name=op.f("ck_forecast_heldout_coverage_release_buckets_sample_count_bounded"),
        ),
        sa.CheckConstraint(
            "octet_length(empirical_coverage_f64_be) = 8 "
            "AND octet_length(confidence_low_f64_be) = 8 "
            "AND octet_length(confidence_high_f64_be) = 8",
            name=op.f("ck_forecast_heldout_coverage_release_buckets_f64_values_size"),
        ),
        sa.ForeignKeyConstraint(
            ["release_id"],
            ["forecast_heldout_coverage_releases.release_id"],
            name=op.f(
                "fk_forecast_heldout_coverage_release_buckets_release_id_"
                "forecast_heldout_coverage_releases"
            ),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "release_id",
            "horizon",
            "coverage_millis",
            name=op.f("pk_forecast_heldout_coverage_release_buckets"),
        ),
    )

    op.create_table(
        "forecast_heldout_coverage_release_availability",
        sa.Column("release_id", sa.String(length=71), nullable=False),
        sa.Column("release_recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.Column("sealer_xid", sa.BigInteger(), nullable=False),
        sa.CheckConstraint(
            "available_at >= release_recorded_at",
            name=op.f("ck_forecast_heldout_coverage_release_availability_not_before_recording"),
        ),
        sa.CheckConstraint(
            "sealer_xid > 0",
            name=op.f("ck_forecast_heldout_coverage_release_availability_sealer_xid_positive"),
        ),
        sa.ForeignKeyConstraint(
            ["release_id"],
            ["forecast_heldout_coverage_releases.release_id"],
            name=op.f(
                "fk_forecast_heldout_coverage_release_availability_release_id_"
                "forecast_heldout_coverage_releases"
            ),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "release_id",
            name=op.f("pk_forecast_heldout_coverage_release_availability"),
        ),
    )

    op.create_index(
        "ix_forecast_realized_outcome_publications_cohort_member",
        "forecast_realized_outcome_publications",
        ["cohort_id", "forecast_id", "step", "outcome_id"],
    )

    _create_functions_triggers_and_acls()


def _create_functions_triggers_and_acls() -> None:
    """Install the trusted write boundary after every referenced table exists."""

    op.execute(
        r"""
        CREATE FUNCTION reject_forecast_calibration_evidence_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        BEGIN
            RAISE EXCEPTION 'forecast calibration evidence is append-only'
                USING ERRCODE = '55000';
        END;
        $$
        """
    )

    op.execute(
        r"""
        CREATE FUNCTION canonical_forecast_calibration_json(value jsonb)
        RETURNS text
        LANGUAGE sql
        IMMUTABLE STRICT PARALLEL SAFE
        SET search_path = pg_catalog, public
        AS $$
            SELECT CASE jsonb_typeof(value)
                WHEN 'object' THEN (
                    SELECT '{' || COALESCE(
                        string_agg(
                            to_json(key)::text || ':' ||
                            public.canonical_forecast_calibration_json(member),
                            ',' ORDER BY key COLLATE "C"
                        ),
                        ''
                    ) || '}'
                    FROM jsonb_each(value) AS item(key, member)
                )
                WHEN 'array' THEN (
                    SELECT '[' || COALESCE(
                        string_agg(
                            public.canonical_forecast_calibration_json(member),
                            ',' ORDER BY ordinal
                        ),
                        ''
                    ) || ']'
                    FROM jsonb_array_elements(value)
                         WITH ORDINALITY AS item(member, ordinal)
                )
                ELSE value::text
            END
        $$
        """
    )

    op.execute(
        r"""
        CREATE FUNCTION publish_fitted_calibration_set(p_canonical_set bytea)
        RETURNS varchar
        LANGUAGE plpgsql VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            document jsonb;
            bucket_document jsonb;
            candidate_version varchar(71);
            stored_bytes bytea;
            cohort_member_count integer;
            outcome_count integer;
            expected_bucket_sample_count integer;
            first_target_date date;
            last_target_date date;
        BEGIN
            IF p_canonical_set IS NULL
               OR octet_length(p_canonical_set) NOT BETWEEN 1 AND 4194304 THEN
                RAISE EXCEPTION 'fitted calibration set exceeds its storage bound'
                    USING ERRCODE = '22023';
            END IF;
            BEGIN
                document := convert_from(p_canonical_set, 'UTF8')::jsonb;
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'fitted calibration set is not valid UTF-8 JSON'
                    USING ERRCODE = '22023';
            END;
            IF jsonb_typeof(document) IS DISTINCT FROM 'object'
               OR document->>'format' IS DISTINCT FROM 'forecast-calibration-set-v2'
               OR document->>'schema_version' IS DISTINCT FROM '2'
               OR NOT (document ?& ARRAY[
                    'buckets', 'cohort_id', 'currency', 'fit_evidence_digest',
                    'forecast_availability_rule_set_hash',
                    'forecast_resolution_policy_hash', 'format', 'horizon_unit',
                    'interval_policy_version', 'method', 'model_version',
                    'outcome_availability_rule_set_hash',
                    'outcome_resolution_policy_hash', 'sample_count',
                    'schema_version', 'selection_policy_hash', 'series_basis',
                    'source_calibration_method', 'source_calibration_set_version',
                    'symbol', 'target', 'window_date_policy_version',
                    'window_end', 'window_start'
               ])
               OR EXISTS (
                    SELECT 1 FROM jsonb_object_keys(document) AS item(key)
                    WHERE key <> ALL (ARRAY[
                        'buckets', 'cohort_id', 'currency', 'fit_evidence_digest',
                        'forecast_availability_rule_set_hash',
                        'forecast_resolution_policy_hash', 'format', 'horizon_unit',
                        'interval_policy_version', 'method', 'model_version',
                        'outcome_availability_rule_set_hash',
                        'outcome_resolution_policy_hash', 'sample_count',
                        'schema_version', 'selection_policy_hash', 'series_basis',
                        'source_calibration_method', 'source_calibration_set_version',
                        'symbol', 'target', 'window_date_policy_version',
                        'window_end', 'window_start'
                    ])
               )
               OR EXISTS (
                    SELECT 1
                    FROM unnest(ARRAY[
                        'cohort_id', 'currency', 'fit_evidence_digest',
                        'forecast_availability_rule_set_hash',
                        'forecast_resolution_policy_hash', 'format', 'horizon_unit',
                        'interval_policy_version', 'method', 'model_version',
                        'outcome_availability_rule_set_hash',
                        'outcome_resolution_policy_hash', 'selection_policy_hash',
                        'series_basis', 'source_calibration_method',
                        'source_calibration_set_version', 'symbol', 'target',
                        'window_date_policy_version', 'window_end', 'window_start'
                    ]) AS required(key)
                    WHERE jsonb_typeof(document->key) IS DISTINCT FROM 'string'
               )
               OR jsonb_typeof(document->'schema_version') IS DISTINCT FROM 'number'
               OR jsonb_typeof(document->'sample_count') IS DISTINCT FROM 'number'
               OR document->>'sample_count' !~ '^[1-9][0-9]*$'
               OR document->>'window_start' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
               OR document->>'window_end' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
               OR jsonb_typeof(document->'buckets') IS DISTINCT FROM 'array'
               OR jsonb_array_length(document->'buckets') NOT BETWEEN 1 AND 10000
               OR EXISTS (
                    SELECT 1 FROM jsonb_array_elements(document->'buckets') AS item(bucket)
                    WHERE jsonb_typeof(bucket) IS DISTINCT FROM 'object'
                       OR NOT (bucket ?& ARRAY[
                            'correction_policy_version', 'coverage_millis',
                            'fit_sample_count', 'horizon',
                            'quantile_selection_policy_version', 'rank', 'value_f64_be'
                       ])
                       OR EXISTS (
                            SELECT 1 FROM jsonb_object_keys(bucket) AS member(key)
                            WHERE key <> ALL (ARRAY[
                                'correction_policy_version', 'coverage_millis',
                                'fit_sample_count', 'horizon',
                                'quantile_selection_policy_version', 'rank', 'value_f64_be'
                            ])
                       )
                       OR EXISTS (
                            SELECT 1
                            FROM unnest(ARRAY[
                                'correction_policy_version',
                                'quantile_selection_policy_version', 'value_f64_be'
                            ]) AS required(key)
                            WHERE jsonb_typeof(bucket->key) IS DISTINCT FROM 'string'
                       )
                       OR EXISTS (
                            SELECT 1
                             FROM unnest(ARRAY[
                                 'coverage_millis', 'fit_sample_count', 'horizon', 'rank'
                             ]) AS required(key)
                             WHERE jsonb_typeof(bucket->key) IS DISTINCT FROM 'number'
                        )
                       OR EXISTS (
                            SELECT 1
                            FROM unnest(ARRAY[
                                'coverage_millis', 'fit_sample_count', 'horizon', 'rank'
                            ]) AS required(key)
                            WHERE bucket->>key !~ '^[1-9][0-9]*$'
                       )
               ) THEN
                RAISE EXCEPTION 'fitted calibration set envelope is unsupported'
                    USING ERRCODE = '22023';
            END IF;
            IF convert_to(
                   public.canonical_forecast_calibration_json(document), 'UTF8'
               ) IS DISTINCT FROM p_canonical_set THEN
                RAISE EXCEPTION 'fitted calibration set bytes are not canonical'
                    USING ERRCODE = '22023';
            END IF;
            IF document->>'method' NOT IN (
                   'empirical_residual', 'conformal_quantile_regression'
               ) THEN
                RAISE EXCEPTION 'fitted calibration method is unsupported'
                    USING ERRCODE = '22023';
            END IF;
            BEGIN
                FOR bucket_document IN
                    SELECT value FROM jsonb_array_elements(document->'buckets')
                LOOP
                    IF bucket_document->>'value_f64_be' !~ '^[0-9a-f]{16}$'
                       OR bucket_document->>'value_f64_be' ~ '^(7ff|fff)'
                       OR bucket_document->>'value_f64_be' = '8000000000000000'
                       OR (
                           document->>'method' = 'empirical_residual'
                           AND bucket_document->>'value_f64_be' !~ '^[0-7]'
                       )
                       OR (bucket_document->>'horizon')::integer NOT BETWEEN 1 AND 252
                       OR (bucket_document->>'coverage_millis')::integer NOT BETWEEN 1 AND 999
                       OR (bucket_document->>'fit_sample_count')::integer NOT BETWEEN 1
                          AND (document->>'sample_count')::integer
                       OR (bucket_document->>'rank')::integer NOT BETWEEN 1
                          AND (bucket_document->>'fit_sample_count')::integer
                       OR (bucket_document->>'rank')::integer IS DISTINCT FROM ceil(
                           (
                               ((bucket_document->>'fit_sample_count')::integer + 1)
                               * (bucket_document->>'coverage_millis')::integer
                           )::numeric / 1000
                       )::integer
                       OR bucket_document->>'quantile_selection_policy_version'
                          IS DISTINCT FROM 'finite-sample-nearest-rank-v1'
                       OR (
                           document->>'method' = 'empirical_residual'
                           AND bucket_document->>'correction_policy_version'
                               IS DISTINCT FROM 'absolute-residual-v1'
                       )
                       OR (
                           document->>'method' = 'conformal_quantile_regression'
                           AND bucket_document->>'correction_policy_version'
                               IS DISTINCT FROM 'signed-cqr-v1'
                       ) THEN
                        RAISE EXCEPTION 'fitted calibration bucket is invalid'
                            USING ERRCODE = '22023';
                    END IF;
                END LOOP;
                IF (
                    SELECT count(*)
                    FROM jsonb_array_elements(document->'buckets') AS item(value)
                ) IS DISTINCT FROM (
                    SELECT count(DISTINCT (
                        (value->>'horizon')::integer,
                        (value->>'coverage_millis')::integer
                    ))
                    FROM jsonb_array_elements(document->'buckets') AS item(value)
                ) THEN
                    RAISE EXCEPTION 'fitted calibration buckets contain a duplicate'
                        USING ERRCODE = '22023';
                END IF;
                IF EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(document->'buckets') WITH ORDINALITY
                         AS current_bucket(value, position)
                    JOIN jsonb_array_elements(document->'buckets') WITH ORDINALITY
                         AS prior_bucket(value, position)
                      ON prior_bucket.position + 1 = current_bucket.position
                    WHERE (
                        (prior_bucket.value->>'horizon')::integer,
                        (prior_bucket.value->>'coverage_millis')::integer
                    ) >= (
                        (current_bucket.value->>'horizon')::integer,
                        (current_bucket.value->>'coverage_millis')::integer
                    )
                ) THEN
                    RAISE EXCEPTION 'fitted calibration buckets are not strictly ordered'
                        USING ERRCODE = '22023';
                END IF;
            EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
                RAISE EXCEPTION 'fitted calibration bucket scalars are invalid'
                    USING ERRCODE = '22023';
            END;
            BEGIN
                PERFORM (document->>'window_start')::date;
                PERFORM (document->>'window_end')::date;
                PERFORM (document->>'sample_count')::integer;
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'fitted calibration set scalar fields are invalid'
                    USING ERRCODE = '22023';
            END;

            SELECT manifest.member_count,
                   min((member.target_time AT TIME ZONE 'UTC')::date),
                   max((member.target_time AT TIME ZONE 'UTC')::date),
                   count(publication.outcome_id)
            INTO cohort_member_count, first_target_date, last_target_date, outcome_count
            FROM public.forecast_outcome_cohort_manifests AS manifest
            JOIN public.forecast_outcome_cohort_availability AS seal
              ON seal.cohort_id = manifest.cohort_id
             AND seal.manifest_recorded_at = manifest.recorded_at
            JOIN public.forecast_outcome_cohort_members AS member
              ON member.cohort_id = manifest.cohort_id
            LEFT JOIN public.forecast_realized_outcome_publications AS publication
              ON publication.cohort_id = member.cohort_id
             AND publication.forecast_id = member.forecast_id
             AND publication.step = member.step
            WHERE manifest.cohort_id = document->>'cohort_id'
              AND manifest.purpose = 'calibration_fit'
              AND manifest.selection_policy_hash = document->>'selection_policy_hash'
              AND manifest.outcome_resolution_policy_hash =
                  document->>'outcome_resolution_policy_hash'
              AND manifest.availability_rule_set_hash =
                  document->>'outcome_availability_rule_set_hash'
            GROUP BY manifest.member_count;
            IF NOT FOUND
               OR cohort_member_count IS DISTINCT FROM (document->>'sample_count')::integer
               OR outcome_count IS DISTINCT FROM cohort_member_count
               OR first_target_date IS DISTINCT FROM (document->>'window_start')::date
               OR last_target_date IS DISTINCT FROM (document->>'window_end')::date THEN
                RAISE EXCEPTION
                    'fitted calibration set lacks one exact complete calibration-fit cohort'
                    USING ERRCODE = '23503';
            END IF;
            IF EXISTS (
                   SELECT 1
                   FROM public.forecast_outcome_cohort_members AS member
                   WHERE member.cohort_id = document->>'cohort_id'
                     AND NOT EXISTS (
                         SELECT 1
                         FROM jsonb_array_elements(document->'buckets') AS bucket(value)
                         WHERE (bucket.value->>'horizon')::smallint = member.step
                     )
               ) OR EXISTS (
                   SELECT 1
                   FROM jsonb_array_elements(document->'buckets') AS bucket(value)
                   WHERE NOT EXISTS (
                       SELECT 1
                       FROM public.forecast_outcome_cohort_members AS member
                       WHERE member.cohort_id = document->>'cohort_id'
                         AND member.step = (bucket.value->>'horizon')::smallint
                   )
               ) THEN
                RAISE EXCEPTION 'fitted bucket horizons differ from cohort horizons'
                    USING ERRCODE = '23000';
            END IF;
            FOR bucket_document IN
                SELECT value FROM jsonb_array_elements(document->'buckets')
            LOOP
                SELECT count(*) INTO expected_bucket_sample_count
                FROM public.forecast_outcome_cohort_members AS member
                WHERE member.cohort_id = document->>'cohort_id'
                  AND member.step = (bucket_document->>'horizon')::smallint;
                IF expected_bucket_sample_count IS DISTINCT FROM
                   (bucket_document->>'fit_sample_count')::integer THEN
                    RAISE EXCEPTION 'fitted bucket sample count differs from cohort'
                        USING ERRCODE = '23000';
                END IF;
            END LOOP;

            IF EXISTS (
                SELECT 1
                FROM public.forecast_outcome_cohort_members AS member
                JOIN public.forecast_runs AS run ON run.forecast_id = member.forecast_id
                WHERE member.cohort_id = document->>'cohort_id'
                  AND (
                      run.origin_kind <> 'scheduled_evaluation'
                      OR run.symbol <> document->>'symbol'
                      OR run.target <> document->>'target'
                      OR run.series_basis <> document->>'series_basis'
                      OR run.horizon_unit <> document->>'horizon_unit'
                      OR run.model_version <> document->>'model_version'
                      OR run.calibration_method <> document->>'source_calibration_method'
                      OR run.calibration_set_version <>
                          document->>'source_calibration_set_version'
                      OR run.resolution_policy_hash <>
                          document->>'forecast_resolution_policy_hash'
                      OR run.availability_rule_set_hash <>
                          document->>'forecast_availability_rule_set_hash'
                  )
            ) THEN
                RAISE EXCEPTION 'fitted calibration set scope differs from its forecast cohort'
                    USING ERRCODE = '23000';
            END IF;

            candidate_version := 'sha256:'
                || encode(digest(p_canonical_set, 'sha256'), 'hex');
            INSERT INTO public.forecast_fitted_calibration_sets(
                calibration_set_version, schema_version, model_version, symbol,
                target, series_basis, horizon_unit, currency,
                source_calibration_set_version, source_calibration_method,
                forecast_resolution_policy_hash, forecast_availability_rule_set_hash,
                fit_evidence_digest, method, window_start, window_end, sample_count,
                cohort_id, selection_policy_hash, outcome_resolution_policy_hash,
                outcome_availability_rule_set_hash, interval_policy_version,
                window_date_policy_version, bucket_count, recorded_at, creator_xid,
                canonical_set
            ) VALUES (
                candidate_version, 2, document->>'model_version', document->>'symbol',
                document->>'target', document->>'series_basis',
                document->>'horizon_unit', document->>'currency',
                document->>'source_calibration_set_version',
                document->>'source_calibration_method',
                document->>'forecast_resolution_policy_hash',
                document->>'forecast_availability_rule_set_hash',
                document->>'fit_evidence_digest', document->>'method',
                (document->>'window_start')::date, (document->>'window_end')::date,
                (document->>'sample_count')::integer, document->>'cohort_id',
                document->>'selection_policy_hash',
                document->>'outcome_resolution_policy_hash',
                document->>'outcome_availability_rule_set_hash',
                document->>'interval_policy_version',
                document->>'window_date_policy_version',
                jsonb_array_length(document->'buckets'), clock_timestamp(),
                txid_current(), p_canonical_set
            ) ON CONFLICT (calibration_set_version) DO NOTHING
            RETURNING canonical_set INTO stored_bytes;
            IF NOT FOUND THEN
                SELECT stored.canonical_set INTO STRICT stored_bytes
                FROM public.forecast_fitted_calibration_sets AS stored
                WHERE stored.calibration_set_version = candidate_version;
            END IF;
            IF stored_bytes IS DISTINCT FROM p_canonical_set THEN
                RAISE EXCEPTION 'fitted calibration set identity is occupied by other bytes'
                    USING ERRCODE = '23000';
            END IF;
            RETURN candidate_version;
        END;
        $$
        """
    )

    op.execute(
        r"""
        CREATE FUNCTION publish_forecast_heldout_coverage_release(
            p_canonical_evidence bytea
        ) RETURNS varchar
        LANGUAGE plpgsql VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            document jsonb;
            bucket_document jsonb;
            fitted_document jsonb;
            candidate_id varchar(71);
            stored_bytes bytea;
            fitted public.forecast_fitted_calibration_sets%ROWTYPE;
            heldout_count integer;
            outcome_count integer;
            first_target_date date;
            last_target_date date;
            expected_bucket_sample_count integer;
            inserted_new boolean := false;
        BEGIN
            IF p_canonical_evidence IS NULL
               OR octet_length(p_canonical_evidence) NOT BETWEEN 1 AND 4194304 THEN
                RAISE EXCEPTION 'held-out coverage release exceeds its storage bound'
                    USING ERRCODE = '22023';
            END IF;
            BEGIN
                document := convert_from(p_canonical_evidence, 'UTF8')::jsonb;
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'held-out coverage release is not valid UTF-8 JSON'
                    USING ERRCODE = '22023';
            END;
            IF jsonb_typeof(document) IS DISTINCT FROM 'object'
               OR document->>'format' IS DISTINCT FROM
                  'forecast-heldout-coverage-release-v1'
               OR document->>'schema_version' IS DISTINCT FROM '1'
               OR document->>'evidence_scope' IS DISTINCT FROM 'descriptive-only'
               OR NOT (document ?& ARRAY[
                    'buckets', 'confidence_level_f64_be', 'currency',
                    'estimator_policy_version', 'evidence_scope', 'fit_cohort_id',
                    'fit_evidence_digest', 'fit_selection_policy_hash',
                    'fitted_calibration_set_version',
                    'forecast_availability_rule_set_hash',
                    'forecast_resolution_policy_hash', 'format',
                    'heldout_cohort_id', 'heldout_evidence_digest',
                    'heldout_sample_count', 'heldout_selection_policy_hash',
                    'heldout_window_end', 'heldout_window_start', 'horizon_unit',
                    'interval_policy_version', 'method', 'model_version',
                    'outcome_availability_rule_set_hash',
                    'outcome_resolution_policy_hash', 'schema_version',
                    'series_basis', 'symbol', 'target', 'window_date_policy_version'
               ])
               OR EXISTS (
                    SELECT 1 FROM jsonb_object_keys(document) AS item(key)
                    WHERE key <> ALL (ARRAY[
                        'buckets', 'confidence_level_f64_be', 'currency',
                        'estimator_policy_version', 'evidence_scope', 'fit_cohort_id',
                        'fit_evidence_digest', 'fit_selection_policy_hash',
                        'fitted_calibration_set_version',
                        'forecast_availability_rule_set_hash',
                        'forecast_resolution_policy_hash', 'format',
                        'heldout_cohort_id', 'heldout_evidence_digest',
                        'heldout_sample_count', 'heldout_selection_policy_hash',
                        'heldout_window_end', 'heldout_window_start', 'horizon_unit',
                        'interval_policy_version', 'method', 'model_version',
                        'outcome_availability_rule_set_hash',
                        'outcome_resolution_policy_hash', 'schema_version',
                        'series_basis', 'symbol', 'target',
                        'window_date_policy_version'
                    ])
               )
               OR EXISTS (
                    SELECT 1
                    FROM unnest(ARRAY[
                        'confidence_level_f64_be', 'currency',
                        'estimator_policy_version', 'evidence_scope', 'fit_cohort_id',
                        'fit_evidence_digest', 'fit_selection_policy_hash',
                        'fitted_calibration_set_version',
                        'forecast_availability_rule_set_hash',
                        'forecast_resolution_policy_hash', 'format',
                        'heldout_cohort_id', 'heldout_evidence_digest',
                        'heldout_selection_policy_hash', 'heldout_window_end',
                        'heldout_window_start', 'horizon_unit',
                        'interval_policy_version', 'method', 'model_version',
                        'outcome_availability_rule_set_hash',
                        'outcome_resolution_policy_hash', 'series_basis', 'symbol',
                        'target', 'window_date_policy_version'
                    ]) AS required(key)
                    WHERE jsonb_typeof(document->key) IS DISTINCT FROM 'string'
               )
               OR jsonb_typeof(document->'schema_version') IS DISTINCT FROM 'number'
               OR jsonb_typeof(document->'heldout_sample_count')
                  IS DISTINCT FROM 'number'
               OR document->>'heldout_sample_count' !~ '^[1-9][0-9]*$'
               OR document->>'heldout_window_start' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
               OR document->>'heldout_window_end' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
               OR document->>'confidence_level_f64_be' !~ '^[0-9a-f]{16}$'
               OR jsonb_typeof(document->'buckets') IS DISTINCT FROM 'array'
               OR jsonb_array_length(document->'buckets') NOT BETWEEN 1 AND 10000
               OR EXISTS (
                    SELECT 1 FROM jsonb_array_elements(document->'buckets') AS item(bucket)
                    WHERE jsonb_typeof(bucket) IS DISTINCT FROM 'object'
                       OR NOT (bucket ?& ARRAY[
                            'confidence_high_f64_be', 'confidence_low_f64_be',
                            'coverage_millis', 'covered_count',
                            'empirical_coverage_f64_be', 'horizon', 'sample_count'
                       ])
                       OR EXISTS (
                            SELECT 1 FROM jsonb_object_keys(bucket) AS member(key)
                            WHERE key <> ALL (ARRAY[
                                'confidence_high_f64_be', 'confidence_low_f64_be',
                                'coverage_millis', 'covered_count',
                                'empirical_coverage_f64_be', 'horizon', 'sample_count'
                            ])
                       )
                       OR EXISTS (
                            SELECT 1
                            FROM unnest(ARRAY[
                                'confidence_high_f64_be', 'confidence_low_f64_be',
                                'empirical_coverage_f64_be'
                            ]) AS required(key)
                            WHERE jsonb_typeof(bucket->key) IS DISTINCT FROM 'string'
                       )
                       OR EXISTS (
                            SELECT 1
                             FROM unnest(ARRAY[
                                 'coverage_millis', 'covered_count', 'horizon',
                                 'sample_count'
                             ]) AS required(key)
                             WHERE jsonb_typeof(bucket->key) IS DISTINCT FROM 'number'
                        )
                       OR bucket->>'coverage_millis' !~ '^[1-9][0-9]*$'
                       OR bucket->>'covered_count' !~ '^(0|[1-9][0-9]*)$'
                       OR bucket->>'horizon' !~ '^[1-9][0-9]*$'
                       OR bucket->>'sample_count' !~ '^[1-9][0-9]*$'
               ) THEN
                RAISE EXCEPTION 'held-out coverage release envelope is unsupported'
                    USING ERRCODE = '22023';
            END IF;
            IF convert_to(
                   public.canonical_forecast_calibration_json(document), 'UTF8'
               ) IS DISTINCT FROM p_canonical_evidence THEN
                RAISE EXCEPTION 'held-out coverage release bytes are not canonical'
                    USING ERRCODE = '22023';
            END IF;
            IF (document->>'confidence_level_f64_be') COLLATE "C"
                  <= '0000000000000000'
               OR (document->>'confidence_level_f64_be') COLLATE "C"
                  >= '3ff0000000000000' THEN
                RAISE EXCEPTION 'held-out confidence level must be strictly between zero and one'
                    USING ERRCODE = '22023';
            END IF;
            BEGIN
                FOR bucket_document IN
                    SELECT value FROM jsonb_array_elements(document->'buckets')
                LOOP
                    IF bucket_document->>'empirical_coverage_f64_be' !~ '^[0-9a-f]{16}$'
                       OR bucket_document->>'confidence_low_f64_be' !~ '^[0-9a-f]{16}$'
                       OR bucket_document->>'confidence_high_f64_be' !~ '^[0-9a-f]{16}$'
                       OR (bucket_document->>'empirical_coverage_f64_be') COLLATE "C"
                          > '3ff0000000000000'
                       OR (bucket_document->>'confidence_low_f64_be') COLLATE "C"
                          > '3ff0000000000000'
                       OR (bucket_document->>'confidence_high_f64_be') COLLATE "C"
                          > '3ff0000000000000'
                       OR (bucket_document->>'confidence_low_f64_be') COLLATE "C" >
                          (bucket_document->>'empirical_coverage_f64_be') COLLATE "C"
                       OR (bucket_document->>'empirical_coverage_f64_be') COLLATE "C" >
                          (bucket_document->>'confidence_high_f64_be') COLLATE "C"
                       OR (bucket_document->>'horizon')::integer NOT BETWEEN 1 AND 252
                       OR (bucket_document->>'coverage_millis')::integer NOT BETWEEN 1 AND 999
                       OR (bucket_document->>'sample_count')::integer NOT BETWEEN 1 AND 10000
                       OR (bucket_document->>'covered_count')::integer NOT BETWEEN 0
                          AND (bucket_document->>'sample_count')::integer THEN
                        RAISE EXCEPTION 'held-out coverage bucket is invalid'
                            USING ERRCODE = '22023';
                    END IF;
                END LOOP;
                IF (
                    SELECT count(*)
                    FROM jsonb_array_elements(document->'buckets') AS item(value)
                ) IS DISTINCT FROM (
                    SELECT count(DISTINCT (
                        (value->>'horizon')::integer,
                        (value->>'coverage_millis')::integer
                    ))
                    FROM jsonb_array_elements(document->'buckets') AS item(value)
                ) THEN
                    RAISE EXCEPTION 'held-out coverage buckets contain a duplicate'
                        USING ERRCODE = '22023';
                END IF;
                IF EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(document->'buckets') WITH ORDINALITY
                         AS current_bucket(value, position)
                    JOIN jsonb_array_elements(document->'buckets') WITH ORDINALITY
                         AS prior_bucket(value, position)
                      ON prior_bucket.position + 1 = current_bucket.position
                    WHERE (
                        (prior_bucket.value->>'horizon')::integer,
                        (prior_bucket.value->>'coverage_millis')::integer
                    ) >= (
                        (current_bucket.value->>'horizon')::integer,
                        (current_bucket.value->>'coverage_millis')::integer
                    )
                ) THEN
                    RAISE EXCEPTION 'held-out coverage buckets are not strictly ordered'
                        USING ERRCODE = '22023';
                END IF;
            EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
                RAISE EXCEPTION 'held-out coverage bucket scalars are invalid'
                    USING ERRCODE = '22023';
            END;
            BEGIN
                PERFORM (document->>'heldout_window_start')::date;
                PERFORM (document->>'heldout_window_end')::date;
                PERFORM (document->>'heldout_sample_count')::integer;
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'held-out coverage release scalar fields are invalid'
                    USING ERRCODE = '22023';
            END;

            SELECT stored.* INTO STRICT fitted
            FROM public.forecast_fitted_calibration_sets AS stored
            WHERE stored.calibration_set_version =
                  document->>'fitted_calibration_set_version';
            fitted_document := convert_from(fitted.canonical_set, 'UTF8')::jsonb;
            IF fitted.method IS DISTINCT FROM document->>'method'
               OR fitted.model_version IS DISTINCT FROM document->>'model_version'
               OR fitted.symbol IS DISTINCT FROM document->>'symbol'
               OR fitted.target IS DISTINCT FROM document->>'target'
               OR fitted.series_basis IS DISTINCT FROM document->>'series_basis'
               OR fitted.horizon_unit IS DISTINCT FROM document->>'horizon_unit'
               OR fitted.currency IS DISTINCT FROM document->>'currency'
               OR fitted.cohort_id IS DISTINCT FROM document->>'fit_cohort_id'
               OR fitted.selection_policy_hash IS DISTINCT FROM
                  document->>'fit_selection_policy_hash'
               OR fitted.fit_evidence_digest IS DISTINCT FROM
                  document->>'fit_evidence_digest'
               OR fitted.outcome_resolution_policy_hash IS DISTINCT FROM
                  document->>'outcome_resolution_policy_hash'
               OR fitted.outcome_availability_rule_set_hash IS DISTINCT FROM
                  document->>'outcome_availability_rule_set_hash'
               OR fitted.forecast_resolution_policy_hash IS DISTINCT FROM
                  document->>'forecast_resolution_policy_hash'
               OR fitted.forecast_availability_rule_set_hash IS DISTINCT FROM
                  document->>'forecast_availability_rule_set_hash'
               OR fitted.interval_policy_version IS DISTINCT FROM
                  document->>'interval_policy_version'
               OR fitted.window_date_policy_version IS DISTINCT FROM
                  document->>'window_date_policy_version' THEN
                RAISE EXCEPTION 'held-out release differs from its fitted calibration set'
                    USING ERRCODE = '23000';
            END IF;
            IF jsonb_array_length(document->'buckets') IS DISTINCT FROM
                  jsonb_array_length(fitted_document->'buckets')
               OR EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(document->'buckets') AS release_bucket(value)
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(fitted_document->'buckets')
                             AS fitted_bucket(value)
                        WHERE fitted_bucket.value->>'horizon'
                              = release_bucket.value->>'horizon'
                          AND fitted_bucket.value->>'coverage_millis'
                              = release_bucket.value->>'coverage_millis'
                    )
               ) THEN
                RAISE EXCEPTION 'held-out release bucket grid differs from fitted set'
                    USING ERRCODE = '23000';
            END IF;

            SELECT manifest.member_count,
                   min((member.target_time AT TIME ZONE 'UTC')::date),
                   max((member.target_time AT TIME ZONE 'UTC')::date),
                   count(publication.outcome_id)
            INTO heldout_count, first_target_date, last_target_date, outcome_count
            FROM public.forecast_outcome_cohort_manifests AS manifest
            JOIN public.forecast_outcome_cohort_availability AS seal
              ON seal.cohort_id = manifest.cohort_id
             AND seal.manifest_recorded_at = manifest.recorded_at
            JOIN public.forecast_outcome_cohort_members AS member
              ON member.cohort_id = manifest.cohort_id
            LEFT JOIN public.forecast_realized_outcome_publications AS publication
              ON publication.cohort_id = member.cohort_id
             AND publication.forecast_id = member.forecast_id
             AND publication.step = member.step
            WHERE manifest.cohort_id = document->>'heldout_cohort_id'
              AND manifest.purpose = 'heldout_evaluation'
              AND manifest.selection_policy_hash =
                  document->>'heldout_selection_policy_hash'
              AND manifest.outcome_resolution_policy_hash =
                  document->>'outcome_resolution_policy_hash'
              AND manifest.availability_rule_set_hash =
                  document->>'outcome_availability_rule_set_hash'
            GROUP BY manifest.member_count;
            IF NOT FOUND
               OR heldout_count IS DISTINCT FROM
                  (document->>'heldout_sample_count')::integer
               OR outcome_count IS DISTINCT FROM heldout_count
               OR first_target_date IS DISTINCT FROM
                  (document->>'heldout_window_start')::date
               OR last_target_date IS DISTINCT FROM
                  (document->>'heldout_window_end')::date THEN
                RAISE EXCEPTION
                    'held-out release lacks one exact complete held-out cohort'
                    USING ERRCODE = '23503';
            END IF;
            IF EXISTS (
                   SELECT 1
                   FROM public.forecast_outcome_cohort_members AS member
                   WHERE member.cohort_id = document->>'heldout_cohort_id'
                     AND NOT EXISTS (
                         SELECT 1
                         FROM jsonb_array_elements(document->'buckets') AS bucket(value)
                         WHERE (bucket.value->>'horizon')::smallint = member.step
                     )
               ) OR EXISTS (
                   SELECT 1
                   FROM jsonb_array_elements(document->'buckets') AS bucket(value)
                   WHERE NOT EXISTS (
                       SELECT 1
                       FROM public.forecast_outcome_cohort_members AS member
                       WHERE member.cohort_id = document->>'heldout_cohort_id'
                         AND member.step = (bucket.value->>'horizon')::smallint
                   )
               ) OR heldout_count IS DISTINCT FROM (
                   SELECT count(*)
                   FROM public.forecast_outcome_cohort_members AS member
                   WHERE member.cohort_id = document->>'heldout_cohort_id'
                     AND member.step IN (
                         SELECT DISTINCT (bucket.value->>'horizon')::smallint
                         FROM jsonb_array_elements(document->'buckets') AS bucket(value)
                     )
               ) THEN
                RAISE EXCEPTION 'held-out bucket horizons differ from cohort horizons'
                    USING ERRCODE = '23000';
            END IF;
            FOR bucket_document IN
                SELECT value FROM jsonb_array_elements(document->'buckets')
            LOOP
                SELECT count(*) INTO expected_bucket_sample_count
                FROM public.forecast_outcome_cohort_members AS member
                WHERE member.cohort_id = document->>'heldout_cohort_id'
                  AND member.step = (bucket_document->>'horizon')::smallint;
                IF expected_bucket_sample_count IS DISTINCT FROM
                   (bucket_document->>'sample_count')::integer THEN
                    RAISE EXCEPTION 'held-out bucket sample count differs from cohort'
                        USING ERRCODE = '23000';
                END IF;
            END LOOP;

            IF EXISTS (
                SELECT 1
                FROM public.forecast_outcome_cohort_members AS member
                JOIN public.forecast_runs AS run ON run.forecast_id = member.forecast_id
                WHERE member.cohort_id = document->>'heldout_cohort_id'
                  AND (
                      run.origin_kind <> 'scheduled_evaluation'
                      OR run.symbol <> document->>'symbol'
                      OR run.target <> document->>'target'
                      OR run.series_basis <> document->>'series_basis'
                      OR run.horizon_unit <> document->>'horizon_unit'
                      OR run.model_version <> document->>'model_version'
                      OR run.calibration_method <> fitted.source_calibration_method
                      OR run.calibration_set_version <>
                          fitted.source_calibration_set_version
                      OR run.resolution_policy_hash <>
                          document->>'forecast_resolution_policy_hash'
                      OR run.availability_rule_set_hash <>
                          document->>'forecast_availability_rule_set_hash'
                  )
            ) THEN
                RAISE EXCEPTION 'held-out release scope differs from its forecast cohort'
                    USING ERRCODE = '23000';
            END IF;

            IF document->>'fit_cohort_id' = document->>'heldout_cohort_id'
               OR EXISTS (
                    SELECT 1
                    FROM public.forecast_outcome_cohort_members AS fit_member
                    JOIN public.forecast_outcome_cohort_members AS heldout_member
                      ON heldout_member.cohort_id = document->>'heldout_cohort_id'
                     AND (
                         heldout_member.forecast_id = fit_member.forecast_id
                         OR heldout_member.opportunity_hash = fit_member.opportunity_hash
                         OR heldout_member.target_time = fit_member.target_time
                     )
                    WHERE fit_member.cohort_id = document->>'fit_cohort_id'
               )
               OR EXISTS (
                    SELECT 1
                    FROM public.forecast_realized_outcome_publications AS fit_publication
                    JOIN public.forecast_realized_outcome_publications AS heldout_publication
                      ON heldout_publication.cohort_id = document->>'heldout_cohort_id'
                     AND heldout_publication.outcome_id = fit_publication.outcome_id
                    WHERE fit_publication.cohort_id = document->>'fit_cohort_id'
               ) THEN
                RAISE EXCEPTION 'fit and held-out release evidence overlap'
                    USING ERRCODE = '23000';
            END IF;

            candidate_id := 'sha256:'
                || encode(digest(p_canonical_evidence, 'sha256'), 'hex');
            INSERT INTO public.forecast_heldout_coverage_releases(
                release_id, schema_version, evidence_scope,
                fitted_calibration_set_version, method, model_version, symbol,
                target, series_basis, horizon_unit, currency, fit_cohort_id,
                fit_selection_policy_hash, heldout_cohort_id,
                heldout_selection_policy_hash, outcome_resolution_policy_hash,
                outcome_availability_rule_set_hash, forecast_resolution_policy_hash,
                forecast_availability_rule_set_hash, fit_evidence_digest,
                heldout_evidence_digest, heldout_window_start, heldout_window_end,
                heldout_sample_count, confidence_level_f64_be,
                interval_policy_version, window_date_policy_version,
                estimator_policy_version, bucket_count, recorded_at, creator_xid,
                canonical_release
            ) VALUES (
                candidate_id, 1, 'descriptive-only',
                document->>'fitted_calibration_set_version', document->>'method',
                document->>'model_version', document->>'symbol', document->>'target',
                document->>'series_basis', document->>'horizon_unit',
                document->>'currency', document->>'fit_cohort_id',
                document->>'fit_selection_policy_hash', document->>'heldout_cohort_id',
                document->>'heldout_selection_policy_hash',
                document->>'outcome_resolution_policy_hash',
                document->>'outcome_availability_rule_set_hash',
                document->>'forecast_resolution_policy_hash',
                document->>'forecast_availability_rule_set_hash',
                document->>'fit_evidence_digest', document->>'heldout_evidence_digest',
                (document->>'heldout_window_start')::date,
                (document->>'heldout_window_end')::date,
                (document->>'heldout_sample_count')::integer,
                decode(document->>'confidence_level_f64_be', 'hex'),
                document->>'interval_policy_version',
                document->>'window_date_policy_version',
                document->>'estimator_policy_version',
                jsonb_array_length(document->'buckets'), clock_timestamp(),
                txid_current(), p_canonical_evidence
            ) ON CONFLICT (release_id) DO NOTHING
            RETURNING canonical_release INTO stored_bytes;
            inserted_new := FOUND;
            IF NOT inserted_new THEN
                SELECT stored.canonical_release INTO STRICT stored_bytes
                FROM public.forecast_heldout_coverage_releases AS stored
                WHERE stored.release_id = candidate_id;
            END IF;
            IF stored_bytes IS DISTINCT FROM p_canonical_evidence THEN
                RAISE EXCEPTION 'held-out release identity is occupied by other bytes'
                    USING ERRCODE = '23000';
            END IF;

            IF inserted_new THEN
                FOR bucket_document IN
                    SELECT value FROM jsonb_array_elements(document->'buckets')
                LOOP
                    IF jsonb_typeof(bucket_document) IS DISTINCT FROM 'object'
                       OR bucket_document->>'empirical_coverage_f64_be' !~ '^[0-9a-f]{16}$'
                       OR bucket_document->>'confidence_low_f64_be' !~ '^[0-9a-f]{16}$'
                       OR bucket_document->>'confidence_high_f64_be' !~ '^[0-9a-f]{16}$' THEN
                        RAISE EXCEPTION 'held-out coverage bucket is malformed'
                            USING ERRCODE = '22023';
                    END IF;
                    BEGIN
                        INSERT INTO public.forecast_heldout_coverage_release_buckets(
                            release_id, horizon, coverage_millis, covered_count,
                            sample_count, empirical_coverage_f64_be,
                            confidence_low_f64_be, confidence_high_f64_be
                        ) VALUES (
                            candidate_id, (bucket_document->>'horizon')::smallint,
                            (bucket_document->>'coverage_millis')::smallint,
                            (bucket_document->>'covered_count')::integer,
                            (bucket_document->>'sample_count')::integer,
                            decode(bucket_document->>'empirical_coverage_f64_be', 'hex'),
                            decode(bucket_document->>'confidence_low_f64_be', 'hex'),
                            decode(bucket_document->>'confidence_high_f64_be', 'hex')
                        );
                    EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
                        RAISE EXCEPTION 'held-out coverage bucket scalars are invalid'
                            USING ERRCODE = '22023';
                    END;
                END LOOP;
            ELSE
                IF (SELECT count(*)
                    FROM public.forecast_heldout_coverage_release_buckets AS stored_bucket
                    WHERE stored_bucket.release_id = candidate_id)
                   IS DISTINCT FROM jsonb_array_length(document->'buckets') THEN
                    RAISE EXCEPTION 'held-out release bucket projection is incomplete'
                        USING ERRCODE = '23000';
                END IF;
            END IF;
            FOR bucket_document IN
                SELECT value FROM jsonb_array_elements(document->'buckets')
            LOOP
                PERFORM 1
                FROM public.forecast_heldout_coverage_release_buckets AS stored_bucket
                WHERE stored_bucket.release_id = candidate_id
                  AND stored_bucket.horizon = (bucket_document->>'horizon')::smallint
                  AND stored_bucket.coverage_millis =
                      (bucket_document->>'coverage_millis')::smallint
                  AND stored_bucket.covered_count =
                      (bucket_document->>'covered_count')::integer
                  AND stored_bucket.sample_count =
                      (bucket_document->>'sample_count')::integer
                  AND stored_bucket.empirical_coverage_f64_be =
                      decode(bucket_document->>'empirical_coverage_f64_be', 'hex')
                  AND stored_bucket.confidence_low_f64_be =
                      decode(bucket_document->>'confidence_low_f64_be', 'hex')
                  AND stored_bucket.confidence_high_f64_be =
                      decode(bucket_document->>'confidence_high_f64_be', 'hex');
                IF NOT FOUND THEN
                    RAISE EXCEPTION 'held-out release bucket projection differs from bytes'
                        USING ERRCODE = '23000';
                END IF;
            END LOOP;
            RETURN candidate_id;
        EXCEPTION WHEN NO_DATA_FOUND THEN
            RAISE EXCEPTION 'referenced fitted calibration evidence does not exist'
                USING ERRCODE = '23503';
        END;
        $$
        """
    )

    op.execute(
        r"""
        CREATE FUNCTION stamp_forecast_heldout_coverage_release_availability()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE release_row public.forecast_heldout_coverage_releases%ROWTYPE;
        BEGIN
            SELECT stored.* INTO STRICT release_row
            FROM public.forecast_heldout_coverage_releases AS stored
            WHERE stored.release_id = NEW.release_id;
            IF release_row.creator_xid = txid_current() THEN
                RAISE EXCEPTION 'held-out release availability requires a later transaction'
                    USING ERRCODE = '55000';
            END IF;
            NEW.release_recorded_at := release_row.recorded_at;
            NEW.available_at := clock_timestamp();
            NEW.sealer_xid := txid_current();
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER forecast_heldout_coverage_release_availability_stamp "
        "BEFORE INSERT ON forecast_heldout_coverage_release_availability FOR EACH ROW "
        "EXECUTE FUNCTION stamp_forecast_heldout_coverage_release_availability()"
    )

    op.execute(
        r"""
        CREATE FUNCTION publish_forecast_heldout_coverage_release_receipt(
            p_release_id varchar
        ) RETURNS TABLE(
            release_id text,
            release_recorded_at timestamptz,
            available_at timestamptz
        )
        LANGUAGE plpgsql VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        BEGIN
            IF p_release_id IS NULL
               OR p_release_id !~ '^sha256:[0-9a-f]{64}$' THEN
                RAISE EXCEPTION 'held-out release identity is invalid'
                    USING ERRCODE = '22023';
            END IF;
            INSERT INTO public.forecast_heldout_coverage_release_availability(
                release_id
            ) VALUES (p_release_id)
            ON CONFLICT ON CONSTRAINT
                pk_forecast_heldout_coverage_release_availability DO NOTHING
            ;
            RETURN QUERY
            SELECT receipt.release_id::text, receipt.release_recorded_at,
                   receipt.available_at
            FROM public.forecast_heldout_coverage_release_availability AS receipt
            WHERE receipt.release_id = p_release_id;
        EXCEPTION WHEN NO_DATA_FOUND THEN
            RAISE EXCEPTION 'held-out release does not exist'
                USING ERRCODE = '23503';
        END;
        $$
        """
    )

    for table in _TABLES:
        op.execute(
            f"CREATE TRIGGER {table}_no_row_mutation BEFORE UPDATE OR DELETE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION reject_forecast_calibration_evidence_mutation()"
        )
        op.execute(
            f"CREATE TRIGGER {table}_no_truncate BEFORE TRUNCATE ON {table} "
            "FOR EACH STATEMENT EXECUTE FUNCTION "
            "reject_forecast_calibration_evidence_mutation()"
        )

    for table in _TABLES:
        op.execute(f"REVOKE ALL ON TABLE public.{table} FROM PUBLIC")
        op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM stockapi_app")
        op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM stockapi_snapshot_builder")
        op.execute(f"GRANT SELECT ON TABLE public.{table} TO stockapi_app")

    op.execute(
        "REVOKE ALL ON FUNCTION "
        "public.reject_forecast_calibration_evidence_mutation(), "
        "public.canonical_forecast_calibration_json(jsonb), "
        "public.stamp_forecast_heldout_coverage_release_availability(), "
        "public.publish_fitted_calibration_set(bytea), "
        "public.publish_forecast_heldout_coverage_release(bytea), "
        "public.publish_forecast_heldout_coverage_release_receipt(varchar) "
        "FROM PUBLIC, stockapi_app, stockapi_snapshot_builder"
    )
    op.execute(
        "GRANT EXECUTE ON FUNCTION public.publish_fitted_calibration_set(bytea), "
        "public.publish_forecast_heldout_coverage_release(bytea), "
        "public.publish_forecast_heldout_coverage_release_receipt(varchar) "
        "TO stockapi_app"
    )

    _audit_privileges()


def _audit_privileges() -> None:
    op.execute(
        r"""
        DO $$
        DECLARE
            app_role oid;
            builder_role oid;
            relation_name text;
            function_name text;
        BEGIN
            SELECT oid INTO STRICT app_role FROM pg_roles WHERE rolname = 'stockapi_app';
            SELECT oid INTO STRICT builder_role
            FROM pg_roles WHERE rolname = 'stockapi_snapshot_builder';
            FOREACH relation_name IN ARRAY ARRAY[
                'forecast_fitted_calibration_sets',
                'forecast_heldout_coverage_releases',
                'forecast_heldout_coverage_release_buckets',
                'forecast_heldout_coverage_release_availability'
            ] LOOP
                IF NOT has_table_privilege(app_role, 'public.' || relation_name, 'SELECT')
                   OR has_table_privilege(app_role, 'public.' || relation_name, 'INSERT')
                   OR has_table_privilege(app_role, 'public.' || relation_name, 'UPDATE')
                   OR has_table_privilege(app_role, 'public.' || relation_name, 'DELETE')
                   OR has_table_privilege(app_role, 'public.' || relation_name, 'TRUNCATE')
                   OR has_table_privilege(app_role, 'public.' || relation_name, 'REFERENCES')
                   OR has_table_privilege(app_role, 'public.' || relation_name, 'TRIGGER')
                   OR has_table_privilege(app_role, 'public.' || relation_name, 'MAINTAIN')
                   OR has_any_column_privilege(app_role, 'public.' || relation_name, 'INSERT')
                   OR has_any_column_privilege(app_role, 'public.' || relation_name, 'UPDATE')
                   OR has_any_column_privilege(app_role, 'public.' || relation_name, 'REFERENCES')
                THEN
                    RAISE EXCEPTION 'runtime calibration-evidence privileges are not exact';
                END IF;
                IF has_table_privilege(builder_role, 'public.' || relation_name, 'SELECT')
                   OR has_table_privilege(builder_role, 'public.' || relation_name, 'INSERT')
                   OR has_table_privilege(builder_role, 'public.' || relation_name, 'UPDATE')
                   OR has_table_privilege(builder_role, 'public.' || relation_name, 'DELETE')
                   OR has_table_privilege(builder_role, 'public.' || relation_name, 'TRUNCATE')
                   OR has_table_privilege(builder_role, 'public.' || relation_name, 'REFERENCES')
                   OR has_table_privilege(builder_role, 'public.' || relation_name, 'TRIGGER')
                   OR has_table_privilege(builder_role, 'public.' || relation_name, 'MAINTAIN')
                   OR has_any_column_privilege(builder_role, 'public.' || relation_name, 'SELECT')
                   OR has_any_column_privilege(builder_role, 'public.' || relation_name, 'INSERT')
                   OR has_any_column_privilege(builder_role, 'public.' || relation_name, 'UPDATE')
                   OR has_any_column_privilege(
                       builder_role, 'public.' || relation_name, 'REFERENCES'
                   )
                THEN
                    RAISE EXCEPTION
                        'snapshot builder calibration-evidence privileges are not empty';
                END IF;
            END LOOP;

            FOREACH function_name IN ARRAY ARRAY[
                'reject_forecast_calibration_evidence_mutation()',
                'canonical_forecast_calibration_json(jsonb)',
                'stamp_forecast_heldout_coverage_release_availability()'
            ] LOOP
                IF has_function_privilege(app_role, 'public.' || function_name, 'EXECUTE')
                   OR has_function_privilege(builder_role, 'public.' || function_name, 'EXECUTE')
                THEN
                    RAISE EXCEPTION 'calibration-evidence trigger function is executable';
                END IF;
            END LOOP;
            FOREACH function_name IN ARRAY ARRAY[
                'publish_fitted_calibration_set(bytea)',
                'publish_forecast_heldout_coverage_release(bytea)',
                'publish_forecast_heldout_coverage_release_receipt(varchar)'
            ] LOOP
                IF NOT has_function_privilege(app_role, 'public.' || function_name, 'EXECUTE')
                   OR has_function_privilege(builder_role, 'public.' || function_name, 'EXECUTE')
                THEN
                    RAISE EXCEPTION 'calibration-evidence publisher privileges are not exact';
                END IF;
            END LOOP;
        END;
        $$
        """
    )


def downgrade() -> None:
    op.execute(
        r"""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM public.forecast_fitted_calibration_sets)
               OR EXISTS (SELECT 1 FROM public.forecast_heldout_coverage_releases)
               OR EXISTS (SELECT 1 FROM public.forecast_heldout_coverage_release_buckets)
               OR EXISTS (
                    SELECT 1 FROM public.forecast_heldout_coverage_release_availability
               ) THEN
                RAISE EXCEPTION 'cannot downgrade nonempty forecast calibration evidence'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $$
        """
    )
    op.execute(
        "REVOKE ALL ON FUNCTION public.publish_fitted_calibration_set(bytea), "
        "public.publish_forecast_heldout_coverage_release(bytea), "
        "public.publish_forecast_heldout_coverage_release_receipt(varchar) "
        "FROM PUBLIC, stockapi_app, stockapi_snapshot_builder"
    )
    op.execute(
        "DROP FUNCTION IF EXISTS public.publish_forecast_heldout_coverage_release_receipt(varchar)"
    )
    op.execute("DROP FUNCTION IF EXISTS public.publish_forecast_heldout_coverage_release(bytea)")
    op.execute("DROP FUNCTION IF EXISTS public.publish_fitted_calibration_set(bytea)")
    for table in reversed(_TABLES):
        op.execute(f"DROP TRIGGER IF EXISTS {table}_no_truncate ON {table}")
        op.execute(f"DROP TRIGGER IF EXISTS {table}_no_row_mutation ON {table}")
    op.execute(
        "DROP TRIGGER IF EXISTS forecast_heldout_coverage_release_availability_stamp "
        "ON forecast_heldout_coverage_release_availability"
    )
    op.execute(
        "DROP FUNCTION IF EXISTS public.stamp_forecast_heldout_coverage_release_availability()"
    )
    op.execute("DROP FUNCTION IF EXISTS public.reject_forecast_calibration_evidence_mutation()")
    op.execute("DROP FUNCTION IF EXISTS public.canonical_forecast_calibration_json(jsonb)")
    op.drop_index(
        "ix_forecast_realized_outcome_publications_cohort_member",
        table_name="forecast_realized_outcome_publications",
    )
    op.drop_table("forecast_heldout_coverage_release_availability")
    op.drop_table("forecast_heldout_coverage_release_buckets")
    op.drop_index(
        "ix_forecast_heldout_coverage_releases_heldout_cohort",
        table_name="forecast_heldout_coverage_releases",
    )
    op.drop_index(
        "ix_forecast_heldout_coverage_releases_fitted_set",
        table_name="forecast_heldout_coverage_releases",
    )
    op.drop_table("forecast_heldout_coverage_releases")
    op.drop_index(
        "ix_forecast_fitted_calibration_sets_cohort_id",
        table_name="forecast_fitted_calibration_sets",
    )
    op.drop_table("forecast_fitted_calibration_sets")
