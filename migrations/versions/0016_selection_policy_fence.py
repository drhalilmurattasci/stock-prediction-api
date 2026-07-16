"""register prospective selection policy and fence cohort purpose

Revision ID: 0016_selection_policy_fence
Revises: 0015_calibration_evidence

The selection-policy document was previously represented on cohort manifests
only by an opaque SHA-256 value.  This migration installs an immutable policy
registry, binds every future cohort to one registered policy/outcome epoch, and
projects the policy and purpose onto cohort members.  Declarative uniqueness
and exclusion constraints reject both exact member reuse and reuse of one
forecast opportunity across fit and held-out purposes.

The ratified minimum seal lead is stored and validated as policy evidence, but
is deliberately not enforced here.  The future prospective operator owns that
authoritative-clock seam; enforcing four hours in this migration would also
make the synthetic live database gate impossible to exercise honestly.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0016_selection_policy_fence"
down_revision: str | None = "0015_calibration_evidence"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    _assert_pre_policy_cohorts_empty()
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    op.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")

    _create_selection_policy_registry()
    _create_selection_policy_functions()
    _create_selection_policy_triggers()
    _bind_cohort_manifests()
    _bind_cohort_members()
    _install_materializer(scoped=True)
    _create_manifest_validator()
    _install_acls_and_audit()


def _assert_pre_policy_cohorts_empty() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM public.forecast_outcome_cohort_manifests)
               OR EXISTS (SELECT 1 FROM public.forecast_outcome_cohort_members) THEN
                RAISE EXCEPTION
                    'selection policy migration requires empty cohort evidence tables'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $$
        """
    )


def _create_selection_policy_registry() -> None:
    op.create_table(
        "forecast_selection_policies",
        sa.Column("policy_hash", sa.String(length=71), nullable=False),
        sa.Column("schema_version", sa.SmallInteger(), nullable=False),
        sa.Column("forecast_resolution_policy_hash", sa.String(length=71), nullable=False),
        sa.Column(
            "forecast_availability_rule_set_hash",
            sa.String(length=71),
            nullable=False,
        ),
        sa.Column("outcome_resolution_policy_hash", sa.String(length=71), nullable=False),
        sa.Column(
            "outcome_availability_rule_set_hash",
            sa.String(length=71),
            nullable=False,
        ),
        sa.Column("resolution_lag_seconds", sa.Integer(), nullable=False),
        sa.Column("fit_window_start", sa.Date(), nullable=False),
        sa.Column("fit_window_end", sa.Date(), nullable=False),
        sa.Column("heldout_window_start", sa.Date(), nullable=False),
        sa.Column("heldout_window_end", sa.Date(), nullable=False),
        sa.Column("minimum_fit_member_count", sa.Integer(), nullable=False),
        sa.Column("minimum_heldout_member_count", sa.Integer(), nullable=False),
        sa.Column("minimum_seal_lead_seconds", sa.Integer(), nullable=False),
        sa.Column("selected_steps", sa.ARRAY(sa.SmallInteger()), nullable=False),
        sa.Column("canonical_policy", sa.LargeBinary(), nullable=False),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.Column("creator_xid", sa.BigInteger(), nullable=False),
        sa.CheckConstraint(
            "policy_hash ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_selection_policies_policy_hash_format"),
        ),
        sa.CheckConstraint(
            "forecast_resolution_policy_hash ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_selection_policies_forecast_resolution_policy_hash_format"),
        ),
        sa.CheckConstraint(
            "forecast_availability_rule_set_hash ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_selection_policies_forecast_availability_rule_set_hash_format"),
        ),
        sa.CheckConstraint(
            "outcome_resolution_policy_hash ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_selection_policies_outcome_resolution_policy_hash_format"),
        ),
        sa.CheckConstraint(
            "outcome_availability_rule_set_hash ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_selection_policies_outcome_availability_rule_set_hash_format"),
        ),
        sa.CheckConstraint(
            "schema_version = 1",
            name=op.f("ck_forecast_selection_policies_schema_version_supported"),
        ),
        sa.CheckConstraint(
            "resolution_lag_seconds BETWEEN 1 AND 31622400",
            name=op.f("ck_forecast_selection_policies_resolution_lag_bounded"),
        ),
        sa.CheckConstraint(
            "fit_window_start <= fit_window_end "
            "AND fit_window_end < heldout_window_start "
            "AND heldout_window_start <= heldout_window_end",
            name=op.f("ck_forecast_selection_policies_window_order"),
        ),
        sa.CheckConstraint(
            "minimum_fit_member_count BETWEEN 1 AND 1000000 "
            "AND minimum_heldout_member_count BETWEEN 1 AND 1000000",
            name=op.f("ck_forecast_selection_policies_minimum_member_counts_bounded"),
        ),
        sa.CheckConstraint(
            "minimum_seal_lead_seconds BETWEEN 14400 AND 31622400",
            name=op.f("ck_forecast_selection_policies_minimum_seal_lead_bounded"),
        ),
        sa.CheckConstraint(
            "cardinality(selected_steps) BETWEEN 1 AND 252 "
            "AND array_position(selected_steps, NULL) IS NULL",
            name=op.f("ck_forecast_selection_policies_selected_steps_bounded"),
        ),
        sa.CheckConstraint(
            "octet_length(canonical_policy) BETWEEN 1 AND 262144",
            name=op.f("ck_forecast_selection_policies_canonical_policy_size_bounded"),
        ),
        sa.CheckConstraint(
            "policy_hash = 'sha256:' || encode(digest(canonical_policy, 'sha256'), 'hex')",
            name=op.f("ck_forecast_selection_policies_policy_hash_matches_payload"),
        ),
        sa.CheckConstraint(
            "creator_xid > 0",
            name=op.f("ck_forecast_selection_policies_creator_xid_positive"),
        ),
        sa.ForeignKeyConstraint(
            ["outcome_resolution_policy_hash", "outcome_availability_rule_set_hash"],
            [
                "forecast_outcome_resolution_policies.policy_hash",
                "forecast_outcome_resolution_policies.availability_rule_set_hash",
            ],
            name="fk_forecast_selection_policies_registered_outcome_policy",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "policy_hash",
            name="pk_forecast_selection_policies",
        ),
        sa.UniqueConstraint(
            "policy_hash",
            "outcome_resolution_policy_hash",
            "outcome_availability_rule_set_hash",
            name="uq_forecast_selection_policies_outcome_epoch",
        ),
    )


def _create_selection_policy_functions() -> None:
    op.execute(
        r"""
        CREATE FUNCTION canonical_forecast_selection_json(p_value jsonb)
        RETURNS text
        LANGUAGE sql
        IMMUTABLE STRICT PARALLEL SAFE
        SET search_path = pg_catalog, public
        AS $$
            SELECT CASE jsonb_typeof(p_value)
                WHEN 'object' THEN (
                    SELECT '{' || COALESCE(
                        string_agg(
                            to_json(object_key)::text || ':' ||
                            public.canonical_forecast_selection_json(object_value),
                            ',' ORDER BY object_key COLLATE "C"
                        ),
                        ''
                    ) || '}'
                    FROM jsonb_each(p_value) AS entry(object_key, object_value)
                )
                WHEN 'array' THEN (
                    SELECT '[' || COALESCE(
                        string_agg(
                            public.canonical_forecast_selection_json(array_value),
                            ',' ORDER BY ordinal
                        ),
                        ''
                    ) || ']'
                    FROM jsonb_array_elements(p_value)
                         WITH ORDINALITY AS entry(array_value, ordinal)
                )
                ELSE p_value::text
            END
        $$
        """
    )

    op.execute(
        r"""
        CREATE FUNCTION stamp_forecast_selection_policy()
        RETURNS trigger
        LANGUAGE plpgsql VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            policy_document jsonb;
            study_document jsonb;
            windows_document jsonb;
            fit_document jsonb;
            heldout_document jsonb;
            epoch_document jsonb;
            selected_step_values smallint[];
            resolution_lag_value integer;
            minimum_fit_value integer;
            minimum_heldout_value integer;
            minimum_seal_lead_value integer;
            horizon_value integer;
            fit_start_value date;
            fit_end_value date;
            heldout_start_value date;
            heldout_end_value date;
        BEGIN
            IF NEW.canonical_policy IS NULL
               OR octet_length(NEW.canonical_policy) NOT BETWEEN 1 AND 262144 THEN
                RAISE EXCEPTION 'selection policy exceeds the storage bound'
                    USING ERRCODE = '22023';
            END IF;
            BEGIN
                policy_document := convert_from(NEW.canonical_policy, 'UTF8')::jsonb;
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'selection policy is not valid UTF-8 JSON'
                    USING ERRCODE = '22023';
            END;
            IF jsonb_typeof(policy_document) IS DISTINCT FROM 'object'
               OR NOT (
                   policy_document ?& ARRAY[
                       'format', 'minimum_seal_lead_seconds', 'policy_epoch',
                       'schema_version', 'study', 'windows'
                   ]
               )
               OR EXISTS (
                   SELECT 1
                   FROM jsonb_object_keys(policy_document) AS item(object_key)
                   WHERE object_key <> ALL (
                       ARRAY[
                           'format', 'minimum_seal_lead_seconds', 'policy_epoch',
                           'schema_version', 'study', 'windows'
                       ]
                   )
               )
               OR policy_document->>'format' IS DISTINCT FROM
                  'forecast-prospective-selection-policy-v1'
               OR jsonb_typeof(policy_document->'schema_version') IS DISTINCT FROM 'number'
               OR policy_document->>'schema_version' IS DISTINCT FROM '1'
               OR jsonb_typeof(policy_document->'minimum_seal_lead_seconds')
                  IS DISTINCT FROM 'number'
               OR policy_document->>'minimum_seal_lead_seconds' !~ '^[0-9]{1,8}$'
               OR jsonb_typeof(policy_document->'study') IS DISTINCT FROM 'object'
               OR jsonb_typeof(policy_document->'windows') IS DISTINCT FROM 'object'
               OR jsonb_typeof(policy_document->'policy_epoch') IS DISTINCT FROM 'object' THEN
                RAISE EXCEPTION 'selection policy envelope is not supported'
                    USING ERRCODE = '22023';
            END IF;
            IF convert_to(
                   public.canonical_forecast_selection_json(policy_document), 'UTF8'
               ) IS DISTINCT FROM NEW.canonical_policy THEN
                RAISE EXCEPTION 'selection policy bytes are not canonical'
                    USING ERRCODE = '22023';
            END IF;

            study_document := policy_document->'study';
            IF NOT (
                   study_document ?& ARRAY[
                       'cadence', 'currency', 'horizon', 'horizon_unit',
                       'interval_coverages_millis', 'model_selector', 'model_version',
                       'selected_steps', 'selection_rule', 'series_basis',
                       'snapshot_binding', 'symbols', 'target'
                   ]
               )
               OR EXISTS (
                   SELECT 1
                   FROM jsonb_object_keys(study_document) AS item(object_key)
                   WHERE object_key <> ALL (
                       ARRAY[
                           'cadence', 'currency', 'horizon', 'horizon_unit',
                           'interval_coverages_millis', 'model_selector', 'model_version',
                           'selected_steps', 'selection_rule', 'series_basis',
                           'snapshot_binding', 'symbols', 'target'
                       ]
                   )
               )
               OR study_document->>'target' IS DISTINCT FROM 'close'
               OR study_document->>'series_basis' IS DISTINCT FROM 'raw'
               OR study_document->>'horizon_unit' IS DISTINCT FROM 'trading_day'
               OR study_document->>'currency' IS DISTINCT FROM 'USD'
               OR study_document->>'cadence' IS DISTINCT FROM 'xnys_session_daily'
               OR study_document->>'snapshot_binding' IS DISTINCT FROM
                  'explicit_snapshot_id'
               OR study_document->>'selection_rule' IS DISTINCT FROM
                  'complete_selected_step_bundle_within_one_utc_target_window'
               OR study_document->>'model_selector' IS NULL
               OR study_document->>'model_selector' NOT IN (
                   'baseline_naive', 'baseline_drift'
               )
               OR study_document->>'model_version' IS NULL
               OR study_document->>'model_version' !~
                  '^[A-Za-z0-9][A-Za-z0-9._@-]{0,127}$'
               OR (
                   study_document->>'model_selector' = 'baseline_naive'
                   AND study_document->>'model_version' !~ '^baseline-naive@'
               )
               OR (
                   study_document->>'model_selector' = 'baseline_drift'
                   AND study_document->>'model_version' !~ '^baseline-drift@'
               )
               OR jsonb_typeof(study_document->'horizon') IS DISTINCT FROM 'number'
               OR study_document->>'horizon' !~ '^[0-9]{1,3}$'
               OR jsonb_typeof(study_document->'selected_steps') IS DISTINCT FROM 'array'
               OR jsonb_typeof(study_document->'interval_coverages_millis')
                  IS DISTINCT FROM 'array'
               OR jsonb_typeof(study_document->'symbols') IS DISTINCT FROM 'array' THEN
                RAISE EXCEPTION 'selection policy study is not supported'
                    USING ERRCODE = '22023';
            END IF;
            IF jsonb_array_length(study_document->'selected_steps') NOT BETWEEN 1 AND 252
               OR jsonb_array_length(study_document->'interval_coverages_millis')
                  NOT BETWEEN 1 AND 9
               OR jsonb_array_length(study_document->'symbols') NOT BETWEEN 1 AND 10000 THEN
                RAISE EXCEPTION 'selection policy study arrays are outside supported bounds'
                    USING ERRCODE = '22023';
            END IF;
            IF EXISTS (
                   SELECT 1
                   FROM jsonb_array_elements(study_document->'selected_steps')
                        AS item(step_document)
                   WHERE jsonb_typeof(step_document) IS DISTINCT FROM 'number'
                      OR step_document #>> '{}' !~ '^[0-9]{1,3}$'
               )
               OR EXISTS (
                   SELECT 1
                   FROM jsonb_array_elements(study_document->'interval_coverages_millis')
                        AS item(coverage_document)
                   WHERE jsonb_typeof(coverage_document) IS DISTINCT FROM 'number'
                      OR coverage_document #>> '{}' !~ '^[0-9]{1,3}$'
               )
               OR EXISTS (
                   SELECT 1
                   FROM jsonb_array_elements(study_document->'symbols')
                        AS item(symbol_document)
                   WHERE jsonb_typeof(symbol_document) IS DISTINCT FROM 'string'
                      OR symbol_document #>> '{}' !~ '^[A-Z0-9.\-_:]+$'
                      OR length(symbol_document #>> '{}') > 32
                      OR symbol_document #>> '{}' <> upper(symbol_document #>> '{}')
               ) THEN
                RAISE EXCEPTION 'selection policy study arrays contain invalid values'
                    USING ERRCODE = '22023';
            END IF;

            BEGIN
                horizon_value := (study_document->>'horizon')::integer;
                minimum_seal_lead_value :=
                    (policy_document->>'minimum_seal_lead_seconds')::integer;
                SELECT array_agg(
                           (step_document #>> '{}')::smallint ORDER BY ordinal
                       )
                INTO selected_step_values
                FROM jsonb_array_elements(study_document->'selected_steps')
                     WITH ORDINALITY AS item(step_document, ordinal);
            EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
                RAISE EXCEPTION 'selection policy integer values are invalid'
                    USING ERRCODE = '22023';
            END;
            IF horizon_value NOT BETWEEN 1 AND 252
               OR minimum_seal_lead_value NOT BETWEEN 14400 AND 31622400
               OR EXISTS (
                   SELECT 1
                   FROM (
                       SELECT step_value,
                              lag(step_value) OVER (ORDER BY ordinal) AS prior_value
                       FROM unnest(selected_step_values)
                            WITH ORDINALITY AS item(step_value, ordinal)
                   ) AS ordered_steps
                   WHERE step_value NOT BETWEEN 1 AND horizon_value
                      OR (prior_value IS NOT NULL AND step_value <= prior_value)
               )
               OR EXISTS (
                   SELECT 1
                   FROM (
                       SELECT (coverage_document #>> '{}')::smallint AS coverage_value,
                              ordinal,
                              lag((coverage_document #>> '{}')::smallint)
                                  OVER (ORDER BY ordinal) AS prior_value
                       FROM jsonb_array_elements(
                           study_document->'interval_coverages_millis'
                       ) WITH ORDINALITY AS item(coverage_document, ordinal)
                   ) AS ordered_coverages
                   WHERE coverage_value NOT BETWEEN 1 AND 999
                      OR (prior_value IS NOT NULL AND coverage_value <= prior_value)
               )
               OR EXISTS (
                   SELECT 1
                   FROM (
                       SELECT symbol_document #>> '{}' AS symbol_value,
                              ordinal,
                              lag(symbol_document #>> '{}') OVER (ORDER BY ordinal)
                                  AS prior_value
                       FROM jsonb_array_elements(study_document->'symbols')
                            WITH ORDINALITY AS item(symbol_document, ordinal)
                   ) AS ordered_symbols
                   WHERE prior_value IS NOT NULL
                     AND (symbol_value COLLATE "C") <= (prior_value COLLATE "C")
               ) THEN
                RAISE EXCEPTION 'selection policy study ordering or bounds are invalid'
                    USING ERRCODE = '22023';
            END IF;

            windows_document := policy_document->'windows';
            IF NOT (
                   windows_document ?& ARRAY[
                       'fit', 'heldout', 'membership_aggregation_rule',
                       'membership_counting_unit', 'window_date_policy_version'
                   ]
               )
               OR EXISTS (
                   SELECT 1
                   FROM jsonb_object_keys(windows_document) AS item(object_key)
                   WHERE object_key <> ALL (
                       ARRAY[
                           'fit', 'heldout', 'membership_aggregation_rule',
                           'membership_counting_unit', 'window_date_policy_version'
                       ]
                   )
               )
               OR windows_document->>'membership_aggregation_rule' IS DISTINCT FROM
                  'distinct_opportunity_steps_across_sealed_cohorts_by_policy_purpose_window'
               OR windows_document->>'membership_counting_unit' IS DISTINCT FROM
                  'forecast_opportunity_step'
               OR windows_document->>'window_date_policy_version' IS DISTINCT FROM
                  'utc-target-date-v1'
               OR jsonb_typeof(windows_document->'fit') IS DISTINCT FROM 'object'
               OR jsonb_typeof(windows_document->'heldout') IS DISTINCT FROM 'object' THEN
                RAISE EXCEPTION 'selection policy windows are not supported'
                    USING ERRCODE = '22023';
            END IF;
            fit_document := windows_document->'fit';
            heldout_document := windows_document->'heldout';
            IF NOT (fit_document ?& ARRAY['end', 'minimum_member_count', 'start'])
               OR EXISTS (
                   SELECT 1 FROM jsonb_object_keys(fit_document) AS item(object_key)
                   WHERE object_key <> ALL (ARRAY['end', 'minimum_member_count', 'start'])
               )
               OR NOT (heldout_document ?& ARRAY['end', 'minimum_member_count', 'start'])
               OR EXISTS (
                   SELECT 1 FROM jsonb_object_keys(heldout_document) AS item(object_key)
                   WHERE object_key <> ALL (ARRAY['end', 'minimum_member_count', 'start'])
               )
               OR jsonb_typeof(fit_document->'minimum_member_count')
                  IS DISTINCT FROM 'number'
               OR fit_document->>'minimum_member_count' !~ '^[0-9]{1,7}$'
               OR jsonb_typeof(heldout_document->'minimum_member_count')
                  IS DISTINCT FROM 'number'
               OR heldout_document->>'minimum_member_count' !~ '^[0-9]{1,7}$'
               OR fit_document->>'start' IS NULL
               OR fit_document->>'start' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
               OR fit_document->>'end' IS NULL
               OR fit_document->>'end' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
               OR heldout_document->>'start' IS NULL
               OR heldout_document->>'start' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
               OR heldout_document->>'end' IS NULL
               OR heldout_document->>'end' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                RAISE EXCEPTION 'selection policy window values are malformed'
                    USING ERRCODE = '22023';
            END IF;
            BEGIN
                fit_start_value := (fit_document->>'start')::date;
                fit_end_value := (fit_document->>'end')::date;
                heldout_start_value := (heldout_document->>'start')::date;
                heldout_end_value := (heldout_document->>'end')::date;
                minimum_fit_value := (fit_document->>'minimum_member_count')::integer;
                minimum_heldout_value :=
                    (heldout_document->>'minimum_member_count')::integer;
            EXCEPTION
                WHEN invalid_text_representation
                   OR invalid_datetime_format
                   OR datetime_field_overflow
                   OR numeric_value_out_of_range THEN
                    RAISE EXCEPTION 'selection policy window values are invalid'
                        USING ERRCODE = '22023';
            END;
            IF fit_start_value::text IS DISTINCT FROM fit_document->>'start'
               OR fit_end_value::text IS DISTINCT FROM fit_document->>'end'
               OR heldout_start_value::text IS DISTINCT FROM heldout_document->>'start'
               OR heldout_end_value::text IS DISTINCT FROM heldout_document->>'end'
               OR fit_start_value > fit_end_value
               OR fit_end_value >= heldout_start_value
               OR heldout_start_value > heldout_end_value
               OR minimum_fit_value NOT BETWEEN 1 AND 1000000
               OR minimum_heldout_value NOT BETWEEN 1 AND 1000000 THEN
                RAISE EXCEPTION 'selection policy windows are invalid or overlap'
                    USING ERRCODE = '22023';
            END IF;

            epoch_document := policy_document->'policy_epoch';
            IF NOT (
                   epoch_document ?& ARRAY[
                       'forecast_availability_rule_set_hash',
                       'forecast_resolution_policy_hash',
                       'outcome_availability_rule_set_hash',
                       'outcome_resolution_policy_hash', 'resolution_lag_seconds'
                   ]
               )
               OR EXISTS (
                   SELECT 1
                   FROM jsonb_object_keys(epoch_document) AS item(object_key)
                   WHERE object_key <> ALL (
                       ARRAY[
                           'forecast_availability_rule_set_hash',
                           'forecast_resolution_policy_hash',
                           'outcome_availability_rule_set_hash',
                           'outcome_resolution_policy_hash', 'resolution_lag_seconds'
                       ]
                   )
               )
               OR epoch_document->>'forecast_availability_rule_set_hash' IS NULL
               OR epoch_document->>'forecast_availability_rule_set_hash'
                  !~ '^sha256:[0-9a-f]{64}$'
               OR epoch_document->>'forecast_resolution_policy_hash' IS NULL
               OR epoch_document->>'forecast_resolution_policy_hash'
                  !~ '^sha256:[0-9a-f]{64}$'
               OR epoch_document->>'outcome_availability_rule_set_hash' IS NULL
               OR epoch_document->>'outcome_availability_rule_set_hash'
                  !~ '^sha256:[0-9a-f]{64}$'
               OR epoch_document->>'outcome_resolution_policy_hash' IS NULL
               OR epoch_document->>'outcome_resolution_policy_hash'
                  !~ '^sha256:[0-9a-f]{64}$'
               OR jsonb_typeof(epoch_document->'resolution_lag_seconds')
                  IS DISTINCT FROM 'number'
               OR epoch_document->>'resolution_lag_seconds' !~ '^[0-9]{1,8}$' THEN
                RAISE EXCEPTION 'selection policy epoch is malformed'
                    USING ERRCODE = '22023';
            END IF;
            BEGIN
                resolution_lag_value :=
                    (epoch_document->>'resolution_lag_seconds')::integer;
            EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
                RAISE EXCEPTION 'selection policy epoch lag is invalid'
                    USING ERRCODE = '22023';
            END;
            IF resolution_lag_value NOT BETWEEN 1 AND 31622400 THEN
                RAISE EXCEPTION 'selection policy epoch lag is outside supported bounds'
                    USING ERRCODE = '22023';
            END IF;
            PERFORM 1
            FROM public.forecast_outcome_resolution_policies AS registered_outcome
            WHERE registered_outcome.policy_hash =
                      epoch_document->>'outcome_resolution_policy_hash'
              AND registered_outcome.availability_rule_set_hash =
                      epoch_document->>'outcome_availability_rule_set_hash'
              AND registered_outcome.resolution_lag_seconds = resolution_lag_value;
            IF NOT FOUND THEN
                RAISE EXCEPTION
                    'selection policy does not match a registered outcome-policy epoch'
                    USING ERRCODE = '23503';
            END IF;

            NEW.policy_hash := 'sha256:'
                || encode(digest(NEW.canonical_policy, 'sha256'), 'hex');
            NEW.schema_version := 1;
            NEW.forecast_resolution_policy_hash :=
                epoch_document->>'forecast_resolution_policy_hash';
            NEW.forecast_availability_rule_set_hash :=
                epoch_document->>'forecast_availability_rule_set_hash';
            NEW.outcome_resolution_policy_hash :=
                epoch_document->>'outcome_resolution_policy_hash';
            NEW.outcome_availability_rule_set_hash :=
                epoch_document->>'outcome_availability_rule_set_hash';
            NEW.resolution_lag_seconds := resolution_lag_value;
            NEW.fit_window_start := fit_start_value;
            NEW.fit_window_end := fit_end_value;
            NEW.heldout_window_start := heldout_start_value;
            NEW.heldout_window_end := heldout_end_value;
            NEW.minimum_fit_member_count := minimum_fit_value;
            NEW.minimum_heldout_member_count := minimum_heldout_value;
            NEW.minimum_seal_lead_seconds := minimum_seal_lead_value;
            NEW.selected_steps := selected_step_values;
            NEW.recorded_at := clock_timestamp();
            NEW.creator_xid := txid_current();
            RETURN NEW;
        END;
        $$
        """
    )

    op.execute(
        r"""
        CREATE FUNCTION register_forecast_selection_policy(p_canonical_policy bytea)
        RETURNS varchar
        LANGUAGE plpgsql VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            registered_policy_hash varchar(71);
        BEGIN
            INSERT INTO public.forecast_selection_policies (
                policy_hash, canonical_policy
            ) VALUES (
                'sha256:' || repeat('0', 64), p_canonical_policy
            )
            RETURNING policy_hash INTO registered_policy_hash;
            RETURN registered_policy_hash;
        END;
        $$
        """
    )


def _create_selection_policy_triggers() -> None:
    op.execute(
        "CREATE TRIGGER forecast_selection_policies_stamp "
        "BEFORE INSERT ON forecast_selection_policies FOR EACH ROW "
        "EXECUTE FUNCTION stamp_forecast_selection_policy()"
    )
    op.execute(
        "CREATE TRIGGER forecast_selection_policies_no_row_mutation "
        "BEFORE UPDATE OR DELETE ON forecast_selection_policies FOR EACH ROW "
        "EXECUTE FUNCTION reject_forecast_evidence_mutation()"
    )
    op.execute(
        "CREATE TRIGGER forecast_selection_policies_no_truncate "
        "BEFORE TRUNCATE ON forecast_selection_policies FOR EACH STATEMENT "
        "EXECUTE FUNCTION reject_forecast_evidence_mutation()"
    )


def _bind_cohort_manifests() -> None:
    op.create_unique_constraint(
        "uq_forecast_outcome_cohort_manifests_selection_scope",
        "forecast_outcome_cohort_manifests",
        ["cohort_id", "selection_policy_hash", "purpose"],
    )
    op.create_foreign_key(
        "fk_cohort_manifests_registered_selection_policy",
        "forecast_outcome_cohort_manifests",
        "forecast_selection_policies",
        [
            "selection_policy_hash",
            "outcome_resolution_policy_hash",
            "availability_rule_set_hash",
        ],
        [
            "policy_hash",
            "outcome_resolution_policy_hash",
            "outcome_availability_rule_set_hash",
        ],
        ondelete="RESTRICT",
    )


def _bind_cohort_members() -> None:
    op.add_column(
        "forecast_outcome_cohort_members",
        sa.Column("selection_policy_hash", sa.String(length=71), nullable=False),
    )
    op.add_column(
        "forecast_outcome_cohort_members",
        sa.Column("purpose", sa.String(length=32), nullable=False),
    )
    op.create_check_constraint(
        op.f("ck_forecast_outcome_cohort_members_selection_policy_hash_format"),
        "forecast_outcome_cohort_members",
        "selection_policy_hash ~ '^sha256:[0-9a-f]{64}$'",
    )
    op.create_check_constraint(
        op.f("ck_forecast_outcome_cohort_members_purpose_supported"),
        "forecast_outcome_cohort_members",
        "purpose IN ('calibration_fit', 'heldout_evaluation')",
    )
    op.create_foreign_key(
        "fk_forecast_outcome_cohort_members_manifest_selection_scope",
        "forecast_outcome_cohort_members",
        "forecast_outcome_cohort_manifests",
        ["cohort_id", "selection_policy_hash", "purpose"],
        ["cohort_id", "selection_policy_hash", "purpose"],
        ondelete="RESTRICT",
    )
    op.create_unique_constraint(
        "uq_forecast_outcome_cohort_members_policy_opportunity_step",
        "forecast_outcome_cohort_members",
        ["selection_policy_hash", "opportunity_hash", "step"],
    )
    op.execute(
        "ALTER TABLE public.forecast_outcome_cohort_members "
        "ADD CONSTRAINT ex_forecast_outcome_cohort_members_cross_purpose "
        "EXCLUDE USING gist ("
        "selection_policy_hash WITH =, opportunity_hash WITH =, purpose WITH <>"
        ")"
    )


def _install_materializer(*, scoped: bool) -> None:
    scope_columns = ", selection_policy_hash, purpose" if scoped else ""
    scope_values = ", NEW.selection_policy_hash, NEW.purpose" if scoped else ""
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION materialize_forecast_outcome_cohort_members()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            manifest_document jsonb;
            member_document jsonb;
        BEGIN
            manifest_document := convert_from(NEW.canonical_manifest, 'UTF8')::jsonb;
            FOR member_document IN
                SELECT member_item.value
                FROM jsonb_array_elements(manifest_document->'members')
                     AS member_item(value)
            LOOP
                BEGIN
                    INSERT INTO public.forecast_outcome_cohort_members (
                        cohort_id, forecast_id, step, target_time,
                        opportunity_hash, output_hash{scope_columns}
                    ) VALUES (
                        NEW.cohort_id,
                        (member_document->>'forecast_id')::uuid,
                        (member_document->>'step')::smallint,
                        (member_document->>'target_time')::timestamptz,
                        member_document->>'opportunity_hash',
                        member_document->>'output_hash'{scope_values}
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


def _create_manifest_validator() -> None:
    op.execute(
        r"""
        CREATE FUNCTION validate_forecast_cohort_selection_policy()
        RETURNS trigger
        LANGUAGE plpgsql VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            registered_policy public.forecast_selection_policies%ROWTYPE;
            manifest_document jsonb;
            policy_document jsonb;
            study_document jsonb;
        BEGIN
            BEGIN
                SELECT selection_registry.*
                INTO STRICT registered_policy
                FROM public.forecast_selection_policies AS selection_registry
                WHERE selection_registry.policy_hash = NEW.selection_policy_hash;
            EXCEPTION WHEN NO_DATA_FOUND THEN
                RAISE EXCEPTION 'cohort selection policy is not registered'
                    USING ERRCODE = '23503';
            END;
            IF NEW.outcome_resolution_policy_hash IS DISTINCT FROM
                   registered_policy.outcome_resolution_policy_hash
               OR NEW.availability_rule_set_hash IS DISTINCT FROM
                   registered_policy.outcome_availability_rule_set_hash THEN
                RAISE EXCEPTION 'cohort outcome epoch differs from its selection policy'
                    USING ERRCODE = '23000';
            END IF;

            manifest_document := convert_from(NEW.canonical_manifest, 'UTF8')::jsonb;
            policy_document := convert_from(registered_policy.canonical_policy, 'UTF8')::jsonb;
            study_document := policy_document->'study';
            IF EXISTS (
                SELECT 1
                FROM jsonb_array_elements(manifest_document->'members')
                     AS member_item(member_document)
                WHERE CASE NEW.purpose
                    WHEN 'calibration_fit' THEN
                        (
                            timezone(
                                'UTC',
                                (member_document->>'target_time')::timestamptz
                            )::date
                        ) NOT BETWEEN registered_policy.fit_window_start
                                      AND registered_policy.fit_window_end
                    WHEN 'heldout_evaluation' THEN
                        (
                            timezone(
                                'UTC',
                                (member_document->>'target_time')::timestamptz
                            )::date
                        ) NOT BETWEEN registered_policy.heldout_window_start
                                      AND registered_policy.heldout_window_end
                    ELSE true
                END
            ) THEN
                RAISE EXCEPTION 'cohort target lies outside its policy purpose window'
                    USING ERRCODE = '23000';
            END IF;

            IF EXISTS (
                SELECT 1
                FROM (
                    SELECT member_document->>'opportunity_hash' AS opportunity_identity,
                           array_agg(
                               (member_document->>'step')::smallint
                               ORDER BY (member_document->>'step')::smallint
                           ) AS opportunity_steps
                    FROM jsonb_array_elements(manifest_document->'members')
                         AS member_item(member_document)
                    GROUP BY member_document->>'opportunity_hash'
                ) AS opportunity_bundle
                WHERE opportunity_bundle.opportunity_steps IS DISTINCT FROM
                      registered_policy.selected_steps
            ) THEN
                RAISE EXCEPTION
                    'each cohort opportunity must contain the exact selected-step bundle'
                    USING ERRCODE = '23000';
            END IF;

            IF EXISTS (
                SELECT 1
                FROM jsonb_array_elements(manifest_document->'members')
                     AS member_item(member_document)
                LEFT JOIN public.forecast_runs AS archived_run
                  ON archived_run.forecast_id =
                     (member_document->>'forecast_id')::uuid
                LEFT JOIN public.forecast_input_snapshots AS archived_snapshot
                  ON archived_snapshot.snapshot_id = archived_run.snapshot_id
                LEFT JOIN LATERAL (
                    SELECT
                        convert_from(
                            archived_run.canonical_request, 'UTF8'
                        )::jsonb AS request_document,
                        convert_from(
                            archived_run.canonical_output, 'UTF8'
                        )::jsonb AS output_document,
                        convert_from(
                            archived_snapshot.canonical_payload, 'UTF8'
                        )::jsonb AS snapshot_document
                ) AS run_evidence ON true
                WHERE archived_run.forecast_id IS NULL
                   OR archived_snapshot.snapshot_id IS NULL
                   OR archived_run.origin_kind IS DISTINCT FROM 'scheduled_evaluation'
                   OR archived_run.opportunity_hash IS DISTINCT FROM
                      member_document->>'opportunity_hash'
                   OR archived_run.output_hash IS DISTINCT FROM
                      member_document->>'output_hash'
                   OR NOT (study_document->'symbols' ? archived_run.symbol)
                   OR archived_run.target IS DISTINCT FROM study_document->>'target'
                   OR archived_run.series_basis IS DISTINCT FROM
                      study_document->>'series_basis'
                   OR archived_run.horizon_unit IS DISTINCT FROM
                      study_document->>'horizon_unit'
                   OR archived_run.horizon::text IS DISTINCT FROM
                      study_document->>'horizon'
                   OR archived_run.model_version IS DISTINCT FROM
                      study_document->>'model_version'
                   OR archived_run.resolution_policy_hash IS DISTINCT FROM
                      registered_policy.forecast_resolution_policy_hash
                   OR archived_run.availability_rule_set_hash IS DISTINCT FROM
                      registered_policy.forecast_availability_rule_set_hash
                   OR run_evidence.request_document->>'format' IS DISTINCT FROM
                      'forecast-run-request-v1'
                   OR run_evidence.request_document->>'schema_version' IS DISTINCT FROM '1'
                   OR run_evidence.request_document#>>'{payload,symbol}' IS DISTINCT FROM
                      archived_run.symbol
                   OR run_evidence.request_document#>>'{payload,target}' IS DISTINCT FROM
                      study_document->>'target'
                   OR run_evidence.request_document#>>'{payload,horizon_unit}' IS DISTINCT FROM
                      study_document->>'horizon_unit'
                   OR run_evidence.request_document#>>'{payload,horizon}' IS DISTINCT FROM
                      study_document->>'horizon'
                   OR run_evidence.request_document#>>'{payload,model}' IS DISTINCT FROM
                      study_document->>'model_selector'
                   OR run_evidence.request_document#>'{payload,as_of}' IS DISTINCT FROM
                      'null'::jsonb
                   OR run_evidence.request_document#>>'{payload,snapshot_id}' IS DISTINCT FROM
                      archived_run.snapshot_id
                   OR ARRAY(
                          SELECT coverage_value::numeric
                          FROM jsonb_array_elements_text(
                              run_evidence.request_document
                                  #> '{payload,interval_coverages}'
                          ) WITH ORDINALITY AS coverage_item(coverage_value, ordinal)
                          ORDER BY ordinal
                      ) IS DISTINCT FROM ARRAY(
                          SELECT coverage_millis::numeric / 1000
                          FROM jsonb_array_elements_text(
                              study_document->'interval_coverages_millis'
                          ) WITH ORDINALITY AS coverage_item(coverage_millis, ordinal)
                          ORDER BY ordinal
                      )
                   OR run_evidence.output_document->>'format' IS DISTINCT FROM
                      'forecast-run-output-v1'
                   OR run_evidence.output_document->>'schema_version' IS DISTINCT FROM '1'
                   OR run_evidence.output_document#>>'{payload,symbol}' IS DISTINCT FROM
                      archived_run.symbol
                   OR run_evidence.output_document#>>'{payload,target}' IS DISTINCT FROM
                      study_document->>'target'
                   OR run_evidence.output_document#>>'{payload,horizon_unit}' IS DISTINCT FROM
                      study_document->>'horizon_unit'
                   OR run_evidence.output_document#>>'{payload,horizon}' IS DISTINCT FROM
                      study_document->>'horizon'
                   OR run_evidence.output_document#>>'{payload,currency}' IS DISTINCT FROM
                      study_document->>'currency'
                   OR run_evidence.output_document
                          #>>'{payload,provenance,model_version}' IS DISTINCT FROM
                      study_document->>'model_version'
                   OR run_evidence.output_document
                          #>>'{payload,provenance,series_basis}' IS DISTINCT FROM
                      study_document->>'series_basis'
                   OR run_evidence.output_document
                          #>>'{payload,provenance,snapshot_id}' IS DISTINCT FROM
                      archived_run.snapshot_id
                   OR archived_snapshot.symbol IS DISTINCT FROM archived_run.symbol
                   OR archived_snapshot.target IS DISTINCT FROM study_document->>'target'
                   OR archived_snapshot.horizon_unit IS DISTINCT FROM
                      study_document->>'horizon_unit'
                   OR archived_snapshot.series_basis IS DISTINCT FROM
                      study_document->>'series_basis'
                   OR archived_snapshot.currency IS DISTINCT FROM
                      study_document->>'currency'
                   OR archived_snapshot.resolution_policy_hash IS DISTINCT FROM
                      registered_policy.forecast_resolution_policy_hash
                   OR archived_snapshot.availability_status IS DISTINCT FROM 'passed'
                   OR archived_snapshot.availability_rule_set_hash IS DISTINCT FROM
                      registered_policy.forecast_availability_rule_set_hash
                   OR archived_snapshot.target_time_count < archived_run.horizon
                   OR run_evidence.snapshot_document->>'format' IS DISTINCT FROM
                      'forecast-input-snapshot-v1'
                   OR run_evidence.snapshot_document->>'schema_version' IS DISTINCT FROM '1'
                   OR run_evidence.snapshot_document->>'symbol' IS DISTINCT FROM
                      archived_run.symbol
                   OR run_evidence.snapshot_document->>'target' IS DISTINCT FROM
                      study_document->>'target'
                   OR run_evidence.snapshot_document->>'horizon_unit' IS DISTINCT FROM
                      study_document->>'horizon_unit'
                   OR run_evidence.snapshot_document->>'series_basis' IS DISTINCT FROM
                      study_document->>'series_basis'
                   OR run_evidence.snapshot_document->>'currency' IS DISTINCT FROM
                      study_document->>'currency'
                   OR run_evidence.snapshot_document->>'resolution_policy_hash'
                      IS DISTINCT FROM registered_policy.forecast_resolution_policy_hash
                   OR run_evidence.snapshot_document#>>'{availability,status}'
                      IS DISTINCT FROM 'passed'
                   OR run_evidence.snapshot_document#>>'{availability,rule_set_hash}'
                      IS DISTINCT FROM registered_policy.forecast_availability_rule_set_hash
                   OR (
                          run_evidence.snapshot_document->'target_times'
                              ->>((member_document->>'step')::integer - 1)
                      )::timestamptz IS DISTINCT FROM
                      (member_document->>'target_time')::timestamptz
                   OR EXISTS (
                          SELECT 1
                          FROM jsonb_array_elements(
                              run_evidence.output_document#>'{payload,forecasts}'
                          ) AS forecast_item(forecast_document)
                          WHERE ARRAY(
                                    SELECT (interval_document->>'coverage')::numeric
                                    FROM jsonb_array_elements(
                                        forecast_document->'intervals'
                                    ) AS interval_item(interval_document)
                                    ORDER BY (interval_document->>'coverage')::numeric
                                ) IS DISTINCT FROM ARRAY(
                                    SELECT coverage_millis::numeric / 1000
                                    FROM jsonb_array_elements_text(
                                        study_document->'interval_coverages_millis'
                                    ) AS policy_coverage(coverage_millis)
                                    ORDER BY coverage_millis::integer
                                )
                      )
            ) THEN
                RAISE EXCEPTION
                    'scheduled run does not match its registered selection policy'
                    USING ERRCODE = '23000';
            END IF;
            RETURN NEW;
        EXCEPTION
            WHEN invalid_text_representation OR numeric_value_out_of_range THEN
                RAISE EXCEPTION 'cohort selection-policy membership is malformed'
                    USING ERRCODE = '22023';
        END;
        $$
        """
    )
    # PostgreSQL runs same-event triggers alphabetically.  This validator must
    # run after forecast_outcome_cohorts_stamp derives NEW's relational fields.
    op.execute(
        "CREATE TRIGGER forecast_outcome_cohorts_validate_selection "
        "BEFORE INSERT ON forecast_outcome_cohort_manifests FOR EACH ROW "
        "EXECUTE FUNCTION validate_forecast_cohort_selection_policy()"
    )


def _install_acls_and_audit() -> None:
    op.execute("REVOKE ALL ON TABLE public.forecast_selection_policies FROM PUBLIC")
    op.execute(
        "REVOKE ALL PRIVILEGES ON TABLE public.forecast_selection_policies "
        "FROM stockapi_app, stockapi_snapshot_builder"
    )
    op.execute("GRANT SELECT ON TABLE public.forecast_selection_policies TO stockapi_app")
    op.execute(
        "REVOKE ALL ON FUNCTION "
        "public.canonical_forecast_selection_json(jsonb), "
        "public.stamp_forecast_selection_policy(), "
        "public.register_forecast_selection_policy(bytea), "
        "public.validate_forecast_cohort_selection_policy() "
        "FROM PUBLIC, stockapi_app, stockapi_snapshot_builder"
    )
    op.execute(
        "GRANT EXECUTE ON FUNCTION public.register_forecast_selection_policy(bytea) TO stockapi_app"
    )
    op.execute(
        r"""
        DO $$
        DECLARE
            app_role oid;
            builder_role oid;
            function_identity text;
        BEGIN
            SELECT role_row.oid INTO STRICT app_role
            FROM pg_catalog.pg_roles AS role_row
            WHERE role_row.rolname = 'stockapi_app';
            SELECT role_row.oid INTO STRICT builder_role
            FROM pg_catalog.pg_roles AS role_row
            WHERE role_row.rolname = 'stockapi_snapshot_builder';

            IF NOT has_table_privilege(
                       app_role, 'public.forecast_selection_policies', 'SELECT'
                   )
               OR has_table_privilege(
                       app_role, 'public.forecast_selection_policies', 'INSERT'
                   )
               OR has_table_privilege(
                       app_role, 'public.forecast_selection_policies', 'UPDATE'
                   )
               OR has_table_privilege(
                       app_role, 'public.forecast_selection_policies', 'DELETE'
                   )
               OR has_table_privilege(
                       app_role, 'public.forecast_selection_policies', 'TRUNCATE'
                   )
               OR has_table_privilege(
                       app_role, 'public.forecast_selection_policies', 'REFERENCES'
                   )
               OR has_table_privilege(
                       app_role, 'public.forecast_selection_policies', 'TRIGGER'
                   )
               OR has_table_privilege(
                       app_role, 'public.forecast_selection_policies', 'MAINTAIN'
                   )
               OR has_any_column_privilege(
                       app_role, 'public.forecast_selection_policies', 'INSERT'
                   )
               OR has_any_column_privilege(
                       app_role, 'public.forecast_selection_policies', 'UPDATE'
                   )
               OR has_any_column_privilege(
                       app_role, 'public.forecast_selection_policies', 'REFERENCES'
                   )
               OR NOT has_function_privilege(
                       app_role,
                       'public.register_forecast_selection_policy(bytea)',
                       'EXECUTE'
                   )
               OR has_any_column_privilege(
                       app_role, 'public.forecast_outcome_cohort_members', 'INSERT'
                   )
               OR has_any_column_privilege(
                       app_role, 'public.forecast_outcome_cohort_members', 'UPDATE'
                   )
               OR has_any_column_privilege(
                       app_role, 'public.forecast_outcome_cohort_members', 'REFERENCES'
                   ) THEN
                RAISE EXCEPTION 'runtime selection-policy privileges are not exact';
            END IF;

            IF has_table_privilege(
                       builder_role, 'public.forecast_selection_policies', 'SELECT'
                   )
               OR has_table_privilege(
                       builder_role, 'public.forecast_selection_policies', 'INSERT'
                   )
               OR has_table_privilege(
                       builder_role, 'public.forecast_selection_policies', 'UPDATE'
                   )
               OR has_table_privilege(
                       builder_role, 'public.forecast_selection_policies', 'DELETE'
                   )
               OR has_table_privilege(
                       builder_role, 'public.forecast_selection_policies', 'TRUNCATE'
                   )
               OR has_table_privilege(
                       builder_role, 'public.forecast_selection_policies', 'REFERENCES'
                   )
               OR has_table_privilege(
                       builder_role, 'public.forecast_selection_policies', 'TRIGGER'
                   )
               OR has_table_privilege(
                       builder_role, 'public.forecast_selection_policies', 'MAINTAIN'
                   )
               OR has_any_column_privilege(
                       builder_role, 'public.forecast_selection_policies', 'SELECT'
                   )
               OR has_any_column_privilege(
                       builder_role, 'public.forecast_selection_policies', 'INSERT'
                   )
               OR has_any_column_privilege(
                       builder_role, 'public.forecast_selection_policies', 'UPDATE'
                   )
               OR has_any_column_privilege(
                       builder_role, 'public.forecast_selection_policies', 'REFERENCES'
                   )
               OR has_any_column_privilege(
                       builder_role, 'public.forecast_outcome_cohort_members', 'SELECT'
                   )
               OR has_any_column_privilege(
                       builder_role, 'public.forecast_outcome_cohort_members', 'INSERT'
                   )
               OR has_any_column_privilege(
                       builder_role, 'public.forecast_outcome_cohort_members', 'UPDATE'
                   )
               OR has_any_column_privilege(
                       builder_role, 'public.forecast_outcome_cohort_members', 'REFERENCES'
                   ) THEN
                RAISE EXCEPTION 'snapshot builder selection-policy privileges are not empty';
            END IF;

            FOREACH function_identity IN ARRAY ARRAY[
                'canonical_forecast_selection_json(jsonb)',
                'stamp_forecast_selection_policy()',
                'validate_forecast_cohort_selection_policy()'
            ] LOOP
                IF has_function_privilege(
                       app_role, 'public.' || function_identity, 'EXECUTE'
                   )
                   OR has_function_privilege(
                       builder_role, 'public.' || function_identity, 'EXECUTE'
                   ) THEN
                    RAISE EXCEPTION 'selection-policy internal function is executable';
                END IF;
            END LOOP;
            IF has_function_privilege(
                   builder_role,
                   'public.register_forecast_selection_policy(bytea)',
                   'EXECUTE'
               ) THEN
                RAISE EXCEPTION 'snapshot builder selection-policy register is executable';
            END IF;
        END;
        $$
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM public.forecast_selection_policies)
               OR EXISTS (SELECT 1 FROM public.forecast_outcome_cohort_manifests)
               OR EXISTS (SELECT 1 FROM public.forecast_outcome_cohort_members) THEN
                RAISE EXCEPTION 'cannot downgrade nonempty selection-policy evidence'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $$
        """
    )
    op.execute(
        "REVOKE ALL ON FUNCTION public.register_forecast_selection_policy(bytea) "
        "FROM PUBLIC, stockapi_app, stockapi_snapshot_builder"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS forecast_outcome_cohorts_validate_selection "
        "ON forecast_outcome_cohort_manifests"
    )
    op.execute("DROP FUNCTION IF EXISTS public.validate_forecast_cohort_selection_policy()")
    op.execute("DROP FUNCTION IF EXISTS public.register_forecast_selection_policy(bytea)")

    # Restore the 0010 projection before removing the columns referenced by the
    # 0016 definition, so the downgraded 0015 schema remains executable.
    _install_materializer(scoped=False)

    op.execute(
        "ALTER TABLE public.forecast_outcome_cohort_members "
        "DROP CONSTRAINT IF EXISTS ex_forecast_outcome_cohort_members_cross_purpose"
    )
    op.drop_constraint(
        "uq_forecast_outcome_cohort_members_policy_opportunity_step",
        "forecast_outcome_cohort_members",
        type_="unique",
    )
    op.drop_constraint(
        "fk_forecast_outcome_cohort_members_manifest_selection_scope",
        "forecast_outcome_cohort_members",
        type_="foreignkey",
    )
    op.drop_constraint(
        op.f("ck_forecast_outcome_cohort_members_purpose_supported"),
        "forecast_outcome_cohort_members",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_forecast_outcome_cohort_members_selection_policy_hash_format"),
        "forecast_outcome_cohort_members",
        type_="check",
    )
    op.drop_column("forecast_outcome_cohort_members", "purpose")
    op.drop_column("forecast_outcome_cohort_members", "selection_policy_hash")

    op.drop_constraint(
        "fk_cohort_manifests_registered_selection_policy",
        "forecast_outcome_cohort_manifests",
        type_="foreignkey",
    )
    op.drop_constraint(
        "uq_forecast_outcome_cohort_manifests_selection_scope",
        "forecast_outcome_cohort_manifests",
        type_="unique",
    )

    op.execute(
        "DROP TRIGGER IF EXISTS forecast_selection_policies_no_truncate "
        "ON forecast_selection_policies"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS forecast_selection_policies_no_row_mutation "
        "ON forecast_selection_policies"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS forecast_selection_policies_stamp ON forecast_selection_policies"
    )
    op.execute("DROP FUNCTION IF EXISTS public.stamp_forecast_selection_policy()")
    op.execute("DROP FUNCTION IF EXISTS public.canonical_forecast_selection_json(jsonb)")
    op.drop_table("forecast_selection_policies")
