"""enforce realized-outcome policy and publication at the database boundary

Revision ID: 0011_outcome_policy_fence
Revises: 0010_forecast_evidence
Create Date: 2026-07-14

This migration closes two evidence races left by the initial outcome schema:
availability receipts now take a database-defined per-series fence before they
are timestamped, and runtime outcome writes must pass through a policy- and
cohort-validating publication function.  No historical policy is inferred;
the migration deliberately requires the still-unused evidence tables to be
empty before installing the immutable policy registry.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0011_outcome_policy_fence"
down_revision: str | None = "0010_forecast_evidence"
branch_labels = None
depends_on = None

_AVAILABILITY_RULE_SET_HASH = (
    "sha256:cfd2d129386375b8663f71f5752b70630cf8dbde21cc18596985de41a58ca705"
)


def upgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM public.forecast_realized_outcomes)
               OR EXISTS (SELECT 1 FROM public.forecast_outcome_cohort_manifests) THEN
                RAISE EXCEPTION
                    'outcome policy migration requires empty pre-policy evidence tables'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $$
        """
    )

    op.create_table(
        "forecast_outcome_resolution_policies",
        sa.Column("policy_hash", sa.String(length=71), nullable=False),
        sa.Column("availability_rule_set_hash", sa.String(length=71), nullable=False),
        sa.Column("schema_version", sa.SmallInteger(), nullable=False),
        sa.Column("resolution_lag_seconds", sa.Integer(), nullable=False),
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
            name=op.f("ck_forecast_outcome_resolution_policies_policy_hash_format"),
        ),
        sa.CheckConstraint(
            "availability_rule_set_hash ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_forecast_outcome_resolution_policies_availability_rule_set_hash_format"),
        ),
        sa.CheckConstraint(
            "schema_version = 1",
            name=op.f("ck_forecast_outcome_resolution_policies_schema_version_supported"),
        ),
        sa.CheckConstraint(
            "resolution_lag_seconds BETWEEN 1 AND 31622400",
            name=op.f("ck_forecast_outcome_resolution_policies_resolution_lag_bounded"),
        ),
        sa.CheckConstraint(
            "creator_xid > 0",
            name=op.f("ck_forecast_outcome_resolution_policies_creator_xid_positive"),
        ),
        sa.CheckConstraint(
            "octet_length(canonical_policy) BETWEEN 1 AND 262144",
            name=op.f("ck_forecast_outcome_resolution_policies_canonical_policy_size_bounded"),
        ),
        sa.CheckConstraint(
            "policy_hash = 'sha256:' || encode(digest(canonical_policy, 'sha256'), 'hex')",
            name=op.f("ck_forecast_outcome_resolution_policies_policy_hash_matches_payload"),
        ),
        sa.PrimaryKeyConstraint(
            "policy_hash",
            name=op.f("pk_forecast_outcome_resolution_policies"),
        ),
        sa.UniqueConstraint(
            "policy_hash",
            "availability_rule_set_hash",
            name="uq_forecast_outcome_resolution_policies_policy_rules",
        ),
    )

    # The length-framed digest is reproduced byte-for-byte by
    # ingestion.locks.bar_series_lock_id.  PostgreSQL's bit(64)->bigint cast
    # interprets the first digest word as the same signed two's-complement key.
    op.execute(
        """
        CREATE FUNCTION forecast_bar_series_fence_id(
            symbol text, source text, timespan text
        ) RETURNS bigint
        LANGUAGE plpgsql
        IMMUTABLE STRICT PARALLEL SAFE
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            material bytea := convert_to('stockapi.bar-series-fence.v1', 'UTF8');
            part bytea;
        BEGIN
            IF symbol = '' OR source = '' OR timespan = '' THEN
                RAISE EXCEPTION 'bar-series fence identity contains an empty part'
                    USING ERRCODE = '22023';
            END IF;
            FOREACH part IN ARRAY ARRAY[
                convert_to(source, 'UTF8'),
                convert_to(timespan, 'UTF8'),
                convert_to(symbol, 'UTF8')
            ] LOOP
                material := material || int4send(octet_length(part)) || part;
            END LOOP;
            RETURN ('x' || encode(substring(digest(material, 'sha256') FROM 1 FOR 8), 'hex'))
                ::bit(64)::bigint;
        END;
        $$
        """
    )

    # This trigger runs alphabetically before the existing stamp trigger.  As
    # a VOLATILE PL/pgSQL function, its post-wait statements use fresh
    # READ-COMMITTED snapshots; the timestamp is therefore never published
    # outside the same fence consumed by outcome resolution.
    op.execute(
        """
        CREATE FUNCTION fence_bar_version_availability()
        RETURNS trigger
        LANGUAGE plpgsql VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        BEGIN
            PERFORM set_config('lock_timeout', '30s', true);
            PERFORM pg_advisory_xact_lock(
                public.forecast_bar_series_fence_id(
                    NEW.symbol, NEW.source, NEW.timespan
                )
            );
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER bar_version_availability_fence "
        "BEFORE INSERT ON bar_version_availability FOR EACH ROW "
        "EXECUTE FUNCTION fence_bar_version_availability()"
    )

    op.execute(
        f"""
        CREATE FUNCTION stamp_forecast_outcome_resolution_policy()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            document jsonb;
            lag_seconds integer;
            expected_policy bytea;
        BEGIN
            IF NEW.canonical_policy IS NULL
               OR octet_length(NEW.canonical_policy) NOT BETWEEN 1 AND 262144 THEN
                RAISE EXCEPTION 'outcome policy exceeds the storage bound'
                    USING ERRCODE = '22023';
            END IF;
            BEGIN
                document := convert_from(NEW.canonical_policy, 'UTF8')::jsonb;
                lag_seconds :=
                    (document->'cutoff'->>'resolution_lag_seconds')::integer;
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'outcome policy is not valid UTF-8 JSON'
                    USING ERRCODE = '22023';
            END;
            IF lag_seconds NOT BETWEEN 1 AND 31622400 THEN
                RAISE EXCEPTION 'outcome policy lag is outside the supported bound'
                    USING ERRCODE = '22023';
            END IF;

            expected_policy := convert_to(
                '{{"availability_rule_set_hash":"{_AVAILABILITY_RULE_SET_HASH}",'
                || '"calendar":{{"engine":"exchange_calendars",'
                || '"engine_version":"4.13.2","name":"XNYS",'
                || '"pandas_version":"3.0.3","schedule_end":"2100-12-31",'
                || '"schedule_start":"1990-01-01",'
                || '"target_timestamp":"regular_session_close_utc",'
                || '"tzdata_version":"2026.2"}},'
                || '"currency":{{"resolver":"fixed_us_equity_v1","value":"USD"}},'
                || '"cutoff":{{"formula":"target_time_utc+resolution_lag_seconds",'
                || '"maturity_clock":"postgresql_clock_timestamp",'
                || '"resolution_lag_seconds":' || lag_seconds::text || '}},'
                || '"format":"forecast-outcome-resolution-policy-v1",'
                || '"schema_version":' || '1,'
                || '"source":{{"adjustment_basis":"raw","field":"close",'
                || '"multiplier":' || '1,'
                || '"provider_endpoint":"/v1/open-close/{{ticker}}/{{date}}?adjusted=false",'
                || '"provider_semantics":"regular_session_close; preMarket and '
                || 'afterHours are separate fields",'
                || '"source":"polygon_open_close","timespan":"day"}},'
                || '"target":{{"name":"close","series_basis":"raw",'
                || '"transform":"identity"}}}}',
                'UTF8'
            );
            IF NEW.canonical_policy IS DISTINCT FROM expected_policy THEN
                RAISE EXCEPTION 'outcome policy bytes are not the exact supported canonical form'
                    USING ERRCODE = '22023';
            END IF;

            NEW.policy_hash := 'sha256:'
                || encode(digest(NEW.canonical_policy, 'sha256'), 'hex');
            NEW.availability_rule_set_hash := '{_AVAILABILITY_RULE_SET_HASH}';
            NEW.schema_version := 1;
            NEW.resolution_lag_seconds := lag_seconds;
            NEW.recorded_at := clock_timestamp();
            NEW.creator_xid := txid_current();
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER forecast_outcome_resolution_policies_stamp "
        "BEFORE INSERT ON forecast_outcome_resolution_policies FOR EACH ROW "
        "EXECUTE FUNCTION stamp_forecast_outcome_resolution_policy()"
    )
    op.execute(
        "CREATE TRIGGER forecast_outcome_resolution_policies_no_row_mutation "
        "BEFORE UPDATE OR DELETE ON forecast_outcome_resolution_policies FOR EACH ROW "
        "EXECUTE FUNCTION reject_forecast_evidence_mutation()"
    )
    op.execute(
        "CREATE TRIGGER forecast_outcome_resolution_policies_no_truncate "
        "BEFORE TRUNCATE ON forecast_outcome_resolution_policies FOR EACH STATEMENT "
        "EXECUTE FUNCTION reject_forecast_evidence_mutation()"
    )
    op.execute(
        """
        CREATE FUNCTION register_forecast_outcome_resolution_policy(
            p_canonical_policy bytea
        ) RETURNS varchar
        LANGUAGE plpgsql VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE registered_hash varchar(71);
        BEGIN
            INSERT INTO public.forecast_outcome_resolution_policies (
                policy_hash, canonical_policy
            ) VALUES ('sha256:' || repeat('0', 64), p_canonical_policy)
            RETURNING policy_hash INTO registered_hash;
            RETURN registered_hash;
        END;
        $$
        """
    )

    op.create_foreign_key(
        "fk_forecast_realized_outcomes_registered_policy",
        "forecast_realized_outcomes",
        "forecast_outcome_resolution_policies",
        ["outcome_resolution_policy_hash", "availability_rule_set_hash"],
        ["policy_hash", "availability_rule_set_hash"],
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "fk_forecast_outcome_cohort_manifests_registered_policy",
        "forecast_outcome_cohort_manifests",
        "forecast_outcome_resolution_policies",
        ["outcome_resolution_policy_hash", "availability_rule_set_hash"],
        ["policy_hash", "availability_rule_set_hash"],
        ondelete="RESTRICT",
    )
    op.create_check_constraint(
        op.f("ck_forecast_realized_outcomes_currency_usd"),
        "forecast_realized_outcomes",
        "currency = 'USD'",
    )

    op.create_table(
        "forecast_realized_outcome_publications",
        sa.Column("outcome_id", sa.String(length=71), nullable=False),
        sa.Column("cohort_id", sa.String(length=71), nullable=False),
        sa.Column("forecast_id", sa.Uuid(), nullable=False),
        sa.Column("step", sa.SmallInteger(), nullable=False),
        sa.Column(
            "published_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.Column("publisher_xid", sa.BigInteger(), nullable=False),
        sa.CheckConstraint(
            "step BETWEEN 1 AND 252",
            name=op.f("ck_forecast_realized_outcome_publications_step_bounded"),
        ),
        sa.CheckConstraint(
            "publisher_xid > 0",
            name=op.f("ck_forecast_realized_outcome_publications_publisher_xid_positive"),
        ),
        sa.ForeignKeyConstraint(
            ["outcome_id"],
            ["forecast_realized_outcomes.outcome_id"],
            name=op.f(
                "fk_forecast_realized_outcome_publications_outcome_id_forecast_realized_outcomes"
            ),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["cohort_id", "forecast_id", "step"],
            [
                "forecast_outcome_cohort_members.cohort_id",
                "forecast_outcome_cohort_members.forecast_id",
                "forecast_outcome_cohort_members.step",
            ],
            name=op.f(
                "fk_forecast_realized_outcome_publications_cohort_member_"
                "forecast_outcome_cohort_members"
            ),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "outcome_id",
            "cohort_id",
            "forecast_id",
            "step",
            name=op.f("pk_forecast_realized_outcome_publications"),
        ),
    )
    op.execute(
        "CREATE TRIGGER forecast_realized_outcome_publications_no_row_mutation "
        "BEFORE UPDATE OR DELETE ON forecast_realized_outcome_publications FOR EACH ROW "
        "EXECUTE FUNCTION reject_forecast_evidence_mutation()"
    )
    op.execute(
        "CREATE TRIGGER forecast_realized_outcome_publications_no_truncate "
        "BEFORE TRUNCATE ON forecast_realized_outcome_publications FOR EACH STATEMENT "
        "EXECUTE FUNCTION reject_forecast_evidence_mutation()"
    )

    # The existing stamp trigger parses canonical evidence and validates its
    # exact bar-version fields.  This alphabetically later trigger enforces the
    # registered policy and unique cutoff-visible maximum.  It requires the
    # publisher to already own the lane fence; the public publisher below owns
    # the correctness path by taking it before issuing this INSERT statement.
    op.execute(
        """
        CREATE FUNCTION validate_forecast_realized_outcome_policy()
        RETURNS trigger
        LANGUAGE plpgsql VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            lag_seconds integer;
            lane_lock bigint;
            lock_is_held boolean;
            maximum_count integer;
            exact_count integer;
        BEGIN
            IF current_setting('transaction_isolation') <> 'read committed' THEN
                RAISE EXCEPTION 'outcome publication requires READ COMMITTED isolation'
                    USING ERRCODE = '55000';
            END IF;
            lane_lock := public.forecast_bar_series_fence_id(
                NEW.symbol, NEW.bar_source, NEW.bar_timespan
            );
            SELECT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_locks AS held
                WHERE held.locktype = 'advisory'
                  AND held.pid = pg_backend_pid()
                  AND held.database = (
                      SELECT oid FROM pg_catalog.pg_database
                      WHERE datname = current_database()
                  )
                  AND held.objsubid = 1
                  AND held.mode = 'ExclusiveLock'
                  AND held.granted
                  AND held.classid = ((lane_lock >> 32) & 4294967295)::oid
                  AND held.objid = (lane_lock & 4294967295)::oid
            ) INTO lock_is_held;
            IF NOT lock_is_held THEN
                RAISE EXCEPTION 'outcome publication does not hold the bar-series fence'
                    USING ERRCODE = '55000';
            END IF;

            BEGIN
                SELECT policy.resolution_lag_seconds
                INTO STRICT lag_seconds
                FROM public.forecast_outcome_resolution_policies AS policy
                WHERE policy.policy_hash = NEW.outcome_resolution_policy_hash
                  AND policy.availability_rule_set_hash = NEW.availability_rule_set_hash;
            EXCEPTION WHEN NO_DATA_FOUND THEN
                RAISE EXCEPTION 'outcome policy is not registered'
                    USING ERRCODE = '23503';
            END;
            IF NEW.currency IS DISTINCT FROM 'USD'
               OR NEW.resolution_cutoff IS DISTINCT FROM
                  NEW.target_time + make_interval(secs => lag_seconds)
               OR clock_timestamp() < NEW.resolution_cutoff THEN
                RAISE EXCEPTION 'outcome does not satisfy its registered cutoff policy'
                    USING ERRCODE = '23514';
            END IF;

            WITH versions AS (
                SELECT bar.recorded_at AS version_recorded_at
                FROM public.bars AS bar
                WHERE bar.symbol = NEW.symbol
                  AND bar.timespan = NEW.bar_timespan
                  AND bar.multiplier = NEW.bar_multiplier
                  AND bar.ts = NEW.bar_observed_at
                  AND bar.source = NEW.bar_source
                  AND bar.adjustment_basis = NEW.bar_adjustment_basis
                UNION
                SELECT revision.previous_recorded_at
                FROM public.bars_revisions AS revision
                WHERE revision.symbol = NEW.symbol
                  AND revision.timespan = NEW.bar_timespan
                  AND revision.multiplier = NEW.bar_multiplier
                  AND revision.ts = NEW.bar_observed_at
                  AND revision.source = NEW.bar_source
                  AND revision.adjustment_basis = NEW.bar_adjustment_basis
                  AND revision.previous_recorded_at IS NOT NULL
                UNION
                SELECT revision.incoming_recorded_at
                FROM public.bars_revisions AS revision
                WHERE revision.symbol = NEW.symbol
                  AND revision.timespan = NEW.bar_timespan
                  AND revision.multiplier = NEW.bar_multiplier
                  AND revision.ts = NEW.bar_observed_at
                  AND revision.source = NEW.bar_source
                  AND revision.adjustment_basis = NEW.bar_adjustment_basis
                  AND revision.incoming_recorded_at IS NOT NULL
            ), eligible AS (
                SELECT versions.version_recorded_at, receipt.available_at
                FROM versions
                JOIN public.bar_version_availability AS receipt
                  ON receipt.symbol = NEW.symbol
                 AND receipt.timespan = NEW.bar_timespan
                 AND receipt.multiplier = NEW.bar_multiplier
                 AND receipt.ts = NEW.bar_observed_at
                 AND receipt.source = NEW.bar_source
                 AND receipt.adjustment_basis = NEW.bar_adjustment_basis
                 AND receipt.version_recorded_at = versions.version_recorded_at
                WHERE receipt.available_at <= NEW.resolution_cutoff
            ), maximum AS (
                SELECT * FROM eligible
                WHERE version_recorded_at = (
                    SELECT max(version_recorded_at) FROM eligible
                )
            )
            SELECT count(*), count(*) FILTER (
                WHERE version_recorded_at = NEW.bar_version_recorded_at
                  AND available_at = NEW.bar_available_at
            )
            INTO maximum_count, exact_count
            FROM maximum;
            IF maximum_count <> 1 OR exact_count <> 1 THEN
                RAISE EXCEPTION
                    'outcome source is not the unique newest cutoff-visible version'
                    USING ERRCODE = '23000';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER forecast_realized_outcomes_validate_policy "
        "BEFORE INSERT ON forecast_realized_outcomes FOR EACH ROW "
        "EXECUTE FUNCTION validate_forecast_realized_outcome_policy()"
    )

    op.execute(
        """
        CREATE FUNCTION publish_forecast_realized_outcome(
            p_cohort_id varchar,
            p_forecast_id uuid,
            p_forecast_step smallint,
            p_outcome_id varchar,
            p_canonical_evidence bytea
        ) RETURNS varchar
        LANGUAGE plpgsql VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            evidence_document jsonb;
            evidence_payload jsonb;
            source_version jsonb;
            expected_evidence bytea;
            outcome_row public.forecast_realized_outcomes%ROWTYPE;
            output_document jsonb;
            snapshot_document jsonb;
            archived_snapshot_id varchar(71);
            archived_feature_set_hash varchar(71);
            provenance_count integer;
        BEGIN
            IF p_cohort_id IS NULL
               OR p_cohort_id !~ '^sha256:[0-9a-f]{64}$'
               OR p_forecast_id IS NULL
               OR p_forecast_step IS NULL
               OR p_forecast_step NOT BETWEEN 1 AND 252
               OR p_outcome_id IS NULL
               OR p_outcome_id !~ '^sha256:[0-9a-f]{64}$'
               OR p_canonical_evidence IS NULL
               OR octet_length(p_canonical_evidence) NOT BETWEEN 1 AND 262144 THEN
                RAISE EXCEPTION 'outcome publication input is invalid or exceeds its bound'
                    USING ERRCODE = '22023';
            END IF;
            IF current_setting('transaction_isolation') <> 'read committed' THEN
                RAISE EXCEPTION 'outcome publication requires READ COMMITTED isolation'
                    USING ERRCODE = '55000';
            END IF;
            BEGIN
                evidence_document := convert_from(p_canonical_evidence, 'UTF8')::jsonb;
                evidence_payload := evidence_document->'payload';
                source_version := evidence_payload->'source_version';
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'outcome evidence is not valid UTF-8 JSON'
                    USING ERRCODE = '22023';
            END;

            IF evidence_payload->>'resolution_cutoff'
                  !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$'
               OR evidence_payload->>'target_time'
                  !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$'
               OR source_version->>'available_at'
                  !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$'
               OR source_version->>'fetched_at'
                  !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$'
               OR source_version->>'observed_at'
                  !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$'
               OR source_version->>'source_as_of'
                  !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$'
               OR source_version->>'version_recorded_at'
                  !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$' THEN
                RAISE EXCEPTION 'outcome evidence timestamps are not canonical UTC values'
                    USING ERRCODE = '22023';
            END IF;
            expected_evidence := convert_to(
                '{"format":"forecast-realized-outcome-v1","payload":'
                || '{"availability_rule_set_hash":"'
                || (evidence_payload->>'availability_rule_set_hash')
                || '","currency":"' || (evidence_payload->>'currency')
                || '","outcome_resolution_policy_hash":"'
                || (evidence_payload->>'outcome_resolution_policy_hash')
                || '","realized_value_f64":"'
                || (evidence_payload->>'realized_value_f64')
                || '","resolution_cutoff":"'
                || (evidence_payload->>'resolution_cutoff')
                || '","series_basis":"' || (evidence_payload->>'series_basis')
                || '","source_version":{"adjustment_basis":"'
                || (source_version->>'adjustment_basis')
                || '","available_at":"' || (source_version->>'available_at')
                || '","fetched_at":"' || (source_version->>'fetched_at')
                || '","field":"' || (source_version->>'field')
                || '","multiplier":' || (source_version->>'multiplier')
                || ',"observed_at":"' || (source_version->>'observed_at')
                || '","source":"' || (source_version->>'source')
                || '","source_as_of":"' || (source_version->>'source_as_of')
                || '","symbol":"' || (source_version->>'symbol')
                || '","timespan":"' || (source_version->>'timespan')
                || '","value_f64":"' || (source_version->>'value_f64')
                || '","version_recorded_at":"'
                || (source_version->>'version_recorded_at')
                || '"},"symbol":"' || (evidence_payload->>'symbol')
                || '","target":"' || (evidence_payload->>'target')
                || '","target_time":"' || (evidence_payload->>'target_time')
                || '"},"schema_version":' || '1}',
                'UTF8'
            );
            IF p_canonical_evidence IS DISTINCT FROM expected_evidence THEN
                RAISE EXCEPTION 'outcome evidence bytes are not the exact canonical form'
                    USING ERRCODE = '22023';
            END IF;

            PERFORM set_config('lock_timeout', '30s', true);
            PERFORM pg_advisory_xact_lock(
                public.forecast_bar_series_fence_id(
                    evidence_payload->>'symbol',
                    evidence_payload->'source_version'->>'source',
                    evidence_payload->'source_version'->>'timespan'
                )
            );

            INSERT INTO public.forecast_realized_outcomes (
                outcome_id, canonical_evidence
            ) VALUES (p_outcome_id, p_canonical_evidence)
            ON CONFLICT (outcome_id) DO NOTHING
            RETURNING * INTO outcome_row;
            IF NOT FOUND THEN
                SELECT stored.* INTO STRICT outcome_row
                FROM public.forecast_realized_outcomes AS stored
                WHERE stored.outcome_id = p_outcome_id;
                IF outcome_row.canonical_evidence IS DISTINCT FROM p_canonical_evidence THEN
                    RAISE EXCEPTION 'outcome content identity is occupied by other evidence'
                        USING ERRCODE = '23000';
                END IF;
            END IF;

            BEGIN
                SELECT convert_from(run.canonical_output, 'UTF8')::jsonb,
                       convert_from(snapshot.canonical_payload, 'UTF8')::jsonb,
                       run.snapshot_id,
                       run.feature_set_hash,
                       count(*) OVER ()
                INTO STRICT output_document, snapshot_document,
                            archived_snapshot_id, archived_feature_set_hash,
                            provenance_count
                FROM public.forecast_outcome_cohort_manifests AS manifest
                JOIN public.forecast_outcome_cohort_availability AS seal
                  ON seal.cohort_id = manifest.cohort_id
                 AND seal.manifest_recorded_at = manifest.recorded_at
                JOIN public.forecast_outcome_cohort_members AS member
                  ON member.cohort_id = manifest.cohort_id
                JOIN public.forecast_runs AS run
                  ON run.forecast_id = member.forecast_id
                 AND run.origin_kind = 'scheduled_evaluation'
                 AND run.opportunity_hash = member.opportunity_hash
                 AND run.output_hash = member.output_hash
                JOIN public.forecast_input_snapshots AS snapshot
                  ON snapshot.snapshot_id = run.snapshot_id
                WHERE manifest.cohort_id = p_cohort_id
                  AND member.forecast_id = p_forecast_id
                  AND member.step = p_forecast_step
                  AND member.target_time = outcome_row.target_time
                  AND seal.sealed_at < member.target_time
                  AND manifest.outcome_resolution_policy_hash =
                      outcome_row.outcome_resolution_policy_hash
                  AND manifest.availability_rule_set_hash =
                      outcome_row.availability_rule_set_hash
                  AND run.symbol = outcome_row.symbol
                  AND run.target = outcome_row.target
                  AND run.series_basis = outcome_row.series_basis
                  AND snapshot.symbol = outcome_row.symbol
                  AND snapshot.target = outcome_row.target
                  AND snapshot.series_basis = outcome_row.series_basis
                  AND snapshot.currency = outcome_row.currency;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    RAISE EXCEPTION
                        'outcome publication is not backed by an exact sealed cohort member'
                        USING ERRCODE = '23503';
                WHEN TOO_MANY_ROWS THEN
                    RAISE EXCEPTION 'outcome publication provenance is ambiguous'
                        USING ERRCODE = '23000';
                WHEN OTHERS THEN
                    RAISE EXCEPTION 'outcome publication provenance is invalid'
                        USING ERRCODE = '23000';
            END;

            IF provenance_count <> 1
               OR output_document->>'format'
                  IS DISTINCT FROM 'forecast-run-output-v1'
               OR output_document->>'schema_version' IS DISTINCT FROM '1'
               OR output_document->'payload'->>'symbol' IS DISTINCT FROM outcome_row.symbol
               OR output_document->'payload'->>'target' IS DISTINCT FROM outcome_row.target
               OR output_document->'payload'->>'currency' IS DISTINCT FROM outcome_row.currency
               OR output_document->'payload'->'provenance'->>'series_basis'
                  IS DISTINCT FROM outcome_row.series_basis
               OR output_document->'payload'->'provenance'->>'forecast_id'
                  IS DISTINCT FROM p_forecast_id::text
               OR output_document->'payload'->'provenance'->>'snapshot_id'
                  IS DISTINCT FROM archived_snapshot_id
               OR output_document->'payload'->'provenance'->>'feature_set_hash'
                  IS DISTINCT FROM archived_feature_set_hash
               OR jsonb_typeof(output_document->'payload'->'forecasts')
                  IS DISTINCT FROM 'array'
               OR 1 <> (
                    SELECT count(*)
                    FROM jsonb_array_elements(
                        output_document->'payload'->'forecasts'
                    ) AS item(value)
                    WHERE (value->>'step')::smallint = p_forecast_step
                      AND (value->>'target_time')::timestamptz = outcome_row.target_time
               )
               OR snapshot_document->>'format'
                  IS DISTINCT FROM 'forecast-input-snapshot-v1'
               OR snapshot_document->>'symbol' IS DISTINCT FROM outcome_row.symbol
               OR snapshot_document->>'target' IS DISTINCT FROM outcome_row.target
               OR snapshot_document->>'series_basis'
                  IS DISTINCT FROM outcome_row.series_basis
               OR snapshot_document->>'currency' IS DISTINCT FROM outcome_row.currency
               OR jsonb_typeof(snapshot_document->'target_times') IS DISTINCT FROM 'array'
               OR jsonb_array_length(snapshot_document->'target_times') < p_forecast_step
               OR (snapshot_document->'target_times'->>(p_forecast_step - 1))::timestamptz
                  IS DISTINCT FROM outcome_row.target_time THEN
                RAISE EXCEPTION
                    'outcome publication does not match its forecast and snapshot provenance'
                    USING ERRCODE = '23000';
            END IF;

            INSERT INTO public.forecast_realized_outcome_publications (
                outcome_id, cohort_id, forecast_id, step,
                published_at, publisher_xid
            ) VALUES (
                outcome_row.outcome_id, p_cohort_id, p_forecast_id, p_forecast_step,
                clock_timestamp(), txid_current()
            ) ON CONFLICT (outcome_id, cohort_id, forecast_id, step) DO NOTHING;
            RETURN outcome_row.outcome_id;
        END;
        $$
        """
    )

    for table in (
        "forecast_outcome_resolution_policies",
        "forecast_realized_outcome_publications",
    ):
        op.execute(f"REVOKE ALL ON TABLE public.{table} FROM PUBLIC")
        op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM stockapi_app")
        op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM stockapi_snapshot_builder")
        op.execute(f"GRANT SELECT ON TABLE public.{table} TO stockapi_app")

    op.execute("REVOKE INSERT ON TABLE public.forecast_realized_outcomes FROM stockapi_app")
    # Table-level REVOKE does not remove a pre-existing column grant.  Since
    # the inherited stamp trigger derives every other field, INSERT on just
    # outcome_id/canonical_evidence would bypass the provenance publisher.
    op.execute(
        """
        DO $$
        DECLARE column_name text;
        BEGIN
            FOR column_name IN
                SELECT attname
                FROM pg_catalog.pg_attribute
                WHERE attrelid = 'public.forecast_realized_outcomes'::regclass
                  AND attnum > 0 AND NOT attisdropped
            LOOP
                EXECUTE format(
                    'REVOKE INSERT (%I) ON TABLE public.forecast_realized_outcomes '
                    'FROM PUBLIC, stockapi_app, stockapi_snapshot_builder',
                    column_name
                );
                EXECUTE format(
                    'REVOKE UPDATE (%I) ON TABLE public.forecast_realized_outcomes '
                    'FROM PUBLIC, stockapi_app, stockapi_snapshot_builder',
                    column_name
                );
                EXECUTE format(
                    'REVOKE REFERENCES (%I) ON TABLE public.forecast_realized_outcomes '
                    'FROM PUBLIC, stockapi_app, stockapi_snapshot_builder',
                    column_name
                );
            END LOOP;
        END;
        $$
        """
    )
    op.execute(
        "REVOKE ALL ON FUNCTION public.forecast_bar_series_fence_id(text, text, text), "
        "public.fence_bar_version_availability(), "
        "public.stamp_forecast_outcome_resolution_policy(), "
        "public.validate_forecast_realized_outcome_policy(), "
        "public.register_forecast_outcome_resolution_policy(bytea), "
        "public.publish_forecast_realized_outcome(varchar, uuid, smallint, varchar, bytea) "
        "FROM PUBLIC, stockapi_app, stockapi_snapshot_builder"
    )
    op.execute(
        "GRANT EXECUTE ON FUNCTION "
        "public.register_forecast_outcome_resolution_policy(bytea), "
        "public.publish_forecast_realized_outcome(varchar, uuid, smallint, varchar, bytea) "
        "TO stockapi_app"
    )

    op.execute(
        """
        DO $$
        DECLARE app_role oid; builder_role oid;
        BEGIN
            SELECT oid INTO STRICT app_role FROM pg_roles WHERE rolname = 'stockapi_app';
            SELECT oid INTO STRICT builder_role
            FROM pg_roles WHERE rolname = 'stockapi_snapshot_builder';
            IF NOT has_table_privilege(
                    app_role, 'public.forecast_realized_outcomes', 'SELECT'
               ) OR has_table_privilege(
                    app_role, 'public.forecast_realized_outcomes', 'INSERT'
               ) OR has_any_column_privilege(
                    app_role, 'public.forecast_realized_outcomes', 'INSERT'
               ) OR has_any_column_privilege(
                    app_role, 'public.forecast_realized_outcomes', 'UPDATE'
               ) OR has_any_column_privilege(
                    app_role, 'public.forecast_realized_outcomes', 'REFERENCES'
               ) OR NOT has_table_privilege(
                    app_role, 'public.forecast_outcome_resolution_policies', 'SELECT'
               ) OR has_table_privilege(
                    app_role, 'public.forecast_outcome_resolution_policies', 'INSERT'
               ) OR has_any_column_privilege(
                    app_role, 'public.forecast_outcome_resolution_policies', 'INSERT'
               ) OR has_any_column_privilege(
                    app_role, 'public.forecast_outcome_resolution_policies', 'UPDATE'
               ) OR has_any_column_privilege(
                    app_role, 'public.forecast_outcome_resolution_policies', 'REFERENCES'
               ) OR NOT has_table_privilege(
                    app_role, 'public.forecast_realized_outcome_publications', 'SELECT'
               ) OR has_table_privilege(
                    app_role, 'public.forecast_realized_outcome_publications', 'INSERT'
               ) OR has_any_column_privilege(
                    app_role, 'public.forecast_realized_outcome_publications', 'INSERT'
               ) OR has_any_column_privilege(
                    app_role, 'public.forecast_realized_outcome_publications', 'UPDATE'
               ) OR has_any_column_privilege(
                    app_role, 'public.forecast_realized_outcome_publications', 'REFERENCES'
               ) OR NOT has_function_privilege(
                    app_role,
                    'public.register_forecast_outcome_resolution_policy(bytea)',
                    'EXECUTE'
               ) OR NOT has_function_privilege(
                    app_role,
                    'public.publish_forecast_realized_outcome(varchar,uuid,smallint,varchar,bytea)',
                    'EXECUTE'
               ) THEN
                RAISE EXCEPTION 'runtime outcome-policy privileges are not exact';
            END IF;
            IF has_table_privilege(
                    builder_role, 'public.forecast_outcome_resolution_policies', 'SELECT'
               ) OR has_any_column_privilege(
                    builder_role, 'public.forecast_outcome_resolution_policies', 'INSERT'
               ) OR has_any_column_privilege(
                    builder_role, 'public.forecast_outcome_resolution_policies', 'UPDATE'
               ) OR has_any_column_privilege(
                    builder_role, 'public.forecast_outcome_resolution_policies', 'REFERENCES'
               ) OR has_table_privilege(
                    builder_role, 'public.forecast_realized_outcome_publications', 'SELECT'
               ) OR has_any_column_privilege(
                    builder_role, 'public.forecast_realized_outcome_publications', 'INSERT'
               ) OR has_any_column_privilege(
                    builder_role, 'public.forecast_realized_outcome_publications', 'UPDATE'
               ) OR has_any_column_privilege(
                    builder_role, 'public.forecast_realized_outcome_publications', 'REFERENCES'
               ) OR has_function_privilege(
                    builder_role,
                    'public.register_forecast_outcome_resolution_policy(bytea)',
                    'EXECUTE'
               ) OR has_function_privilege(
                    builder_role,
                    'public.publish_forecast_realized_outcome(varchar,uuid,smallint,varchar,bytea)',
                    'EXECUTE'
               ) THEN
                RAISE EXCEPTION 'snapshot builder outcome-policy privileges are not empty';
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
            IF EXISTS (SELECT 1 FROM public.forecast_realized_outcomes)
               OR EXISTS (SELECT 1 FROM public.forecast_outcome_cohort_manifests)
               OR EXISTS (SELECT 1 FROM public.forecast_outcome_resolution_policies) THEN
                RAISE EXCEPTION 'cannot downgrade nonempty outcome-policy evidence'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $$
        """
    )
    op.execute(
        "REVOKE ALL ON FUNCTION "
        "public.publish_forecast_realized_outcome(varchar, uuid, smallint, varchar, bytea), "
        "public.register_forecast_outcome_resolution_policy(bytea) "
        "FROM PUBLIC, stockapi_app, stockapi_snapshot_builder"
    )
    op.execute(
        "DROP FUNCTION IF EXISTS "
        "publish_forecast_realized_outcome(varchar, uuid, smallint, varchar, bytea)"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS forecast_realized_outcomes_validate_policy "
        "ON forecast_realized_outcomes"
    )
    op.execute("DROP FUNCTION IF EXISTS validate_forecast_realized_outcome_policy()")
    op.execute(
        "DROP TRIGGER IF EXISTS forecast_realized_outcome_publications_no_truncate "
        "ON forecast_realized_outcome_publications"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS forecast_realized_outcome_publications_no_row_mutation "
        "ON forecast_realized_outcome_publications"
    )
    op.drop_table("forecast_realized_outcome_publications")
    op.drop_constraint(
        op.f("ck_forecast_realized_outcomes_currency_usd"),
        "forecast_realized_outcomes",
        type_="check",
    )
    op.drop_constraint(
        "fk_forecast_outcome_cohort_manifests_registered_policy",
        "forecast_outcome_cohort_manifests",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_forecast_realized_outcomes_registered_policy",
        "forecast_realized_outcomes",
        type_="foreignkey",
    )
    op.execute("DROP FUNCTION IF EXISTS register_forecast_outcome_resolution_policy(bytea)")
    op.execute(
        "DROP TRIGGER IF EXISTS forecast_outcome_resolution_policies_no_truncate "
        "ON forecast_outcome_resolution_policies"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS forecast_outcome_resolution_policies_no_row_mutation "
        "ON forecast_outcome_resolution_policies"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS forecast_outcome_resolution_policies_stamp "
        "ON forecast_outcome_resolution_policies"
    )
    op.execute("DROP FUNCTION IF EXISTS stamp_forecast_outcome_resolution_policy()")
    op.drop_table("forecast_outcome_resolution_policies")
    op.execute("DROP TRIGGER IF EXISTS bar_version_availability_fence ON bar_version_availability")
    op.execute("DROP FUNCTION IF EXISTS fence_bar_version_availability()")
    op.execute("DROP FUNCTION IF EXISTS forecast_bar_series_fence_id(text, text, text)")
    op.execute("GRANT INSERT ON TABLE public.forecast_realized_outcomes TO stockapi_app")
