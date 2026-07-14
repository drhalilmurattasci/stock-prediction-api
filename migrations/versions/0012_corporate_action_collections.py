"""add immutable, complete corporate-action collections

Revision ID: 0012_corporate_actions
Revises: 0011_outcome_policy_fence
Create Date: 2026-07-14

Corporate actions are stored as immutable content versions and complete,
bounded query manifests.  A manifest can honestly prove an empty result and a
later complete manifest can represent a provider correction or withdrawal
without mutating prior evidence.  Availability is stamped only in a second
transaction after relational membership has been checked.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0012_corporate_actions"
down_revision: str | None = "0011_outcome_policy_fence"
branch_labels = None
depends_on = None

_QUERY_POLICY_HASH = "sha256:a9784b0ed486b1cdce14596b546f83e7da4035209b05427564f1e529a96d5127"


def upgrade() -> None:
    op.create_table(
        "corporate_action_versions",
        sa.Column("action_version_id", sa.String(length=71), nullable=False),
        sa.Column("schema_version", sa.SmallInteger(), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("action_type", sa.String(length=16), nullable=False),
        sa.Column("provider_event_id", sa.String(length=128), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("effective_date", sa.Date(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("split_from", sa.Numeric(38, 18), nullable=True),
        sa.Column("split_to", sa.Numeric(38, 18), nullable=True),
        sa.Column("adjustment_type", sa.String(length=32), nullable=True),
        sa.Column("cash_amount", sa.Numeric(38, 18), nullable=True),
        sa.Column("split_adjusted_cash_amount", sa.Numeric(38, 18), nullable=True),
        sa.Column("currency", sa.String(length=3), nullable=True),
        sa.Column("declaration_date", sa.Date(), nullable=True),
        sa.Column("record_date", sa.Date(), nullable=True),
        sa.Column("pay_date", sa.Date(), nullable=True),
        sa.Column("frequency", sa.Integer(), nullable=True),
        sa.Column("distribution_type", sa.String(length=32), nullable=True),
        sa.Column("historical_adjustment_factor", sa.Numeric(38, 18), nullable=True),
        sa.Column("canonical_event", sa.LargeBinary(), nullable=False),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.Column("creator_xid", sa.BigInteger(), nullable=False),
        sa.CheckConstraint(
            "schema_version = 1",
            name=op.f("ck_corporate_action_versions_schema_version_supported"),
        ),
        sa.CheckConstraint(
            "action_version_id ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_corporate_action_versions_action_version_id_format"),
        ),
        sa.CheckConstraint(
            "symbol = upper(symbol)",
            name=op.f("ck_corporate_action_versions_symbol_uppercase"),
        ),
        sa.CheckConstraint(
            "symbol ~ '^[A-Z0-9.\\-_:]+$'",
            name=op.f("ck_corporate_action_versions_symbol_format"),
        ),
        sa.CheckConstraint(
            "action_type IN ('split', 'dividend')",
            name=op.f("ck_corporate_action_versions_action_type_supported"),
        ),
        sa.CheckConstraint(
            "source = 'polygon'",
            name=op.f("ck_corporate_action_versions_source_supported"),
        ),
        sa.CheckConstraint(
            "provider_event_id ~ '^[A-Za-z0-9._:\\-]+$'",
            name=op.f("ck_corporate_action_versions_provider_event_id_format"),
        ),
        sa.CheckConstraint(
            "status = 'active'",
            name=op.f("ck_corporate_action_versions_status_supported"),
        ),
        sa.CheckConstraint(
            "(action_type = 'split' AND split_from IS NOT NULL "
            "AND split_to IS NOT NULL AND split_from > 0 AND split_to > 0 "
            "AND adjustment_type IS NOT NULL AND adjustment_type IN "
            "('forward_split', 'reverse_split', 'stock_dividend') "
            "AND historical_adjustment_factor IS NOT NULL "
            "AND historical_adjustment_factor > 0 "
            "AND cash_amount IS NULL AND currency IS NULL "
            "AND declaration_date IS NULL AND record_date IS NULL "
            "AND pay_date IS NULL AND frequency IS NULL "
            "AND distribution_type IS NULL "
            "AND split_adjusted_cash_amount IS NULL) OR "
            "(action_type = 'dividend' AND split_from IS NULL "
            "AND split_to IS NULL AND cash_amount IS NOT NULL "
            "AND cash_amount > 0 AND split_adjusted_cash_amount IS NOT NULL "
            "AND split_adjusted_cash_amount > 0 "
            "AND currency IS NOT NULL AND currency ~ '^[A-Z]{3}$' "
            "AND distribution_type IS NOT NULL AND distribution_type IN "
            "('recurring', 'special', 'supplemental', 'irregular', 'unknown') "
            "AND historical_adjustment_factor IS NOT NULL "
            "AND historical_adjustment_factor > 0 AND adjustment_type IS NULL)",
            name=op.f("ck_corporate_action_versions_action_shape"),
        ),
        sa.CheckConstraint(
            "historical_adjustment_factor IS NULL OR "
            "(historical_adjustment_factor > 0 "
            "AND historical_adjustment_factor < 'Infinity'::numeric)",
            name=op.f("ck_corporate_action_versions_historical_factor_positive"),
        ),
        sa.CheckConstraint(
            "split_adjusted_cash_amount IS NULL OR "
            "(split_adjusted_cash_amount > 0 "
            "AND split_adjusted_cash_amount < 'Infinity'::numeric)",
            name=op.f("ck_corporate_action_versions_split_adjusted_cash_positive"),
        ),
        sa.CheckConstraint(
            "split_from IS NULL OR (split_from > 0 AND split_from < 'Infinity'::numeric)",
            name=op.f("ck_corporate_action_versions_split_from_finite_positive"),
        ),
        sa.CheckConstraint(
            "split_to IS NULL OR (split_to > 0 AND split_to < 'Infinity'::numeric)",
            name=op.f("ck_corporate_action_versions_split_to_finite_positive"),
        ),
        sa.CheckConstraint(
            "cash_amount IS NULL OR (cash_amount > 0 AND cash_amount < 'Infinity'::numeric)",
            name=op.f("ck_corporate_action_versions_cash_amount_finite_positive"),
        ),
        sa.CheckConstraint(
            "frequency IS NULL OR frequency >= 0",
            name=op.f("ck_corporate_action_versions_frequency_nonnegative"),
        ),
        sa.CheckConstraint(
            "octet_length(canonical_event) BETWEEN 1 AND 65536",
            name=op.f("ck_corporate_action_versions_canonical_event_size_bounded"),
        ),
        sa.CheckConstraint(
            "action_version_id = 'sha256:' || encode(digest(canonical_event, 'sha256'), 'hex')",
            name=op.f("ck_corporate_action_versions_action_version_id_matches_payload"),
        ),
        sa.CheckConstraint(
            "creator_xid > 0",
            name=op.f("ck_corporate_action_versions_creator_xid_positive"),
        ),
        sa.PrimaryKeyConstraint(
            "action_version_id",
            name=op.f("pk_corporate_action_versions"),
        ),
        sa.UniqueConstraint(
            "source",
            "action_type",
            "provider_event_id",
            "action_version_id",
            name="uq_corporate_action_versions_source_event_version",
        ),
    )
    op.create_index(
        "ix_corporate_action_versions_series_date",
        "corporate_action_versions",
        ["source", "symbol", "action_type", "effective_date"],
    )

    op.create_table(
        "corporate_action_collections",
        sa.Column("collection_id", sa.String(length=71), nullable=False),
        sa.Column("schema_version", sa.SmallInteger(), nullable=False),
        sa.Column("query_policy_hash", sa.String(length=71), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("endpoint", sa.String(length=128), nullable=False),
        sa.Column("action_type", sa.String(length=16), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("coverage_start", sa.Date(), nullable=False),
        sa.Column("coverage_end", sa.Date(), nullable=False),
        sa.Column("page_limit", sa.Integer(), nullable=False),
        sa.Column("page_count", sa.SmallInteger(), nullable=False),
        sa.Column("event_count", sa.Integer(), nullable=False),
        sa.Column("pagination_exhausted", sa.Boolean(), nullable=False),
        sa.Column("provider_request_id", sa.String(length=128), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("canonical_manifest", sa.LargeBinary(), nullable=False),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.Column("creator_xid", sa.BigInteger(), nullable=False),
        sa.CheckConstraint(
            "schema_version = 1",
            name=op.f("ck_corporate_action_collections_schema_version_supported"),
        ),
        sa.CheckConstraint(
            "collection_id ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_corporate_action_collections_collection_id_format"),
        ),
        sa.CheckConstraint(
            "query_policy_hash ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_corporate_action_collections_query_policy_hash_format"),
        ),
        sa.CheckConstraint(
            "action_type IN ('split', 'dividend')",
            name=op.f("ck_corporate_action_collections_action_type_supported"),
        ),
        sa.CheckConstraint(
            "symbol = upper(symbol)",
            name=op.f("ck_corporate_action_collections_symbol_uppercase"),
        ),
        sa.CheckConstraint(
            "coverage_start <= coverage_end",
            name=op.f("ck_corporate_action_collections_coverage_order"),
        ),
        sa.CheckConstraint(
            "page_limit = 5000",
            name=op.f("ck_corporate_action_collections_page_limit_supported"),
        ),
        sa.CheckConstraint(
            "page_count = 1",
            name=op.f("ck_corporate_action_collections_page_count_supported"),
        ),
        sa.CheckConstraint(
            "event_count BETWEEN 0 AND 5000",
            name=op.f("ck_corporate_action_collections_event_count_bounded"),
        ),
        sa.CheckConstraint(
            "pagination_exhausted",
            name=op.f("ck_corporate_action_collections_pagination_must_be_exhausted"),
        ),
        sa.CheckConstraint(
            "octet_length(canonical_manifest) BETWEEN 1 AND 1048576",
            name=op.f("ck_corporate_action_collections_canonical_manifest_size_bounded"),
        ),
        sa.CheckConstraint(
            "collection_id = 'sha256:' || encode(digest(canonical_manifest, 'sha256'), 'hex')",
            name=op.f("ck_corporate_action_collections_collection_id_matches_payload"),
        ),
        sa.CheckConstraint(
            "creator_xid > 0",
            name=op.f("ck_corporate_action_collections_creator_xid_positive"),
        ),
        sa.PrimaryKeyConstraint(
            "collection_id",
            name=op.f("pk_corporate_action_collections"),
        ),
        sa.UniqueConstraint(
            "collection_id",
            "recorded_at",
            name="uq_corporate_action_collections_exact_recording",
        ),
    )
    op.create_index(
        "ix_corporate_action_collections_scope",
        "corporate_action_collections",
        [
            "source",
            "symbol",
            "action_type",
            "coverage_start",
            "coverage_end",
            "recorded_at",
            "collection_id",
        ],
    )

    op.create_table(
        "corporate_action_collection_members",
        sa.Column("collection_id", sa.String(length=71), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("action_version_id", sa.String(length=71), nullable=False),
        sa.Column("creator_xid", sa.BigInteger(), nullable=False),
        sa.CheckConstraint(
            "ordinal >= 0",
            name=op.f("ck_corporate_action_collection_members_ordinal_nonnegative"),
        ),
        sa.CheckConstraint(
            "creator_xid > 0",
            name=op.f("ck_corporate_action_collection_members_creator_xid_positive"),
        ),
        sa.ForeignKeyConstraint(
            ("collection_id",),
            ("corporate_action_collections.collection_id",),
            name="fk_corporate_action_collection_members_collection",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ("action_version_id",),
            ("corporate_action_versions.action_version_id",),
            name="fk_corporate_action_collection_members_action_version",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "collection_id",
            "ordinal",
            name=op.f("pk_corporate_action_collection_members"),
        ),
        sa.UniqueConstraint(
            "collection_id",
            "action_version_id",
            name="uq_corporate_action_collection_members_version",
        ),
    )

    op.create_table(
        "corporate_action_collection_availability",
        sa.Column("collection_id", sa.String(length=71), nullable=False),
        sa.Column("collection_recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "available_at >= collection_recorded_at",
            name=op.f("ck_corporate_action_collection_availability_available_after_collection"),
        ),
        sa.ForeignKeyConstraint(
            ("collection_id", "collection_recorded_at"),
            (
                "corporate_action_collections.collection_id",
                "corporate_action_collections.recorded_at",
            ),
            name="fk_corporate_action_collection_availability_exact_collection",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "collection_id",
            name=op.f("pk_corporate_action_collection_availability"),
        ),
        sa.UniqueConstraint(
            "collection_id",
            "collection_recorded_at",
            "available_at",
            name="uq_corporate_action_collection_availability_exact_receipt",
        ),
    )

    op.execute(
        r"""
        CREATE FUNCTION stamp_corporate_action_evidence()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        BEGIN
            NEW.recorded_at := clock_timestamp();
            NEW.creator_xid := txid_current();
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER corporate_action_versions_stamp "
        "BEFORE INSERT ON corporate_action_versions FOR EACH ROW "
        "EXECUTE FUNCTION stamp_corporate_action_evidence()"
    )
    op.execute(
        "CREATE TRIGGER corporate_action_collections_stamp "
        "BEFORE INSERT ON corporate_action_collections FOR EACH ROW "
        "EXECUTE FUNCTION stamp_corporate_action_evidence()"
    )

    op.execute(
        r"""
        CREATE FUNCTION corporate_action_series_fence_id(
            source text, symbol text, action_type text
        ) RETURNS bigint
        LANGUAGE plpgsql
        IMMUTABLE STRICT PARALLEL SAFE
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            material bytea := convert_to(
                'stockapi.corporate-action-series-fence.v1', 'UTF8'
            );
            part bytea;
        BEGIN
            IF source = '' OR symbol = '' OR action_type = '' THEN
                RAISE EXCEPTION 'corporate-action fence contains an empty part'
                    USING ERRCODE = '22023';
            END IF;
            FOREACH part IN ARRAY ARRAY[
                convert_to(source, 'UTF8'),
                convert_to(symbol, 'UTF8'),
                convert_to(action_type, 'UTF8')
            ] LOOP
                material := material || int4send(octet_length(part)) || part;
            END LOOP;
            RETURN ('x' || encode(substring(digest(material, 'sha256') FROM 1 FOR 8), 'hex'))
                ::bit(64)::bigint;
        END;
        $$
        """
    )

    op.execute(
        r"""
        CREATE FUNCTION stamp_corporate_action_collection_member()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            collection_creator bigint;
        BEGIN
            SELECT creator_xid INTO STRICT collection_creator
            FROM public.corporate_action_collections
            WHERE collection_id = NEW.collection_id;
            IF collection_creator <> txid_current() THEN
                RAISE EXCEPTION
                    'corporate-action members must be inserted with their collection'
                    USING ERRCODE = '55000';
            END IF;
            IF EXISTS (
                SELECT 1 FROM public.corporate_action_collection_availability
                WHERE collection_id = NEW.collection_id
            ) THEN
                RAISE EXCEPTION 'a receipted corporate-action collection is frozen'
                    USING ERRCODE = '55000';
            END IF;
            NEW.creator_xid := txid_current();
            RETURN NEW;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RAISE EXCEPTION 'corporate-action collection does not exist'
                    USING ERRCODE = '23503';
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER corporate_action_collection_members_stamp "
        "BEFORE INSERT ON corporate_action_collection_members FOR EACH ROW "
        "EXECUTE FUNCTION stamp_corporate_action_collection_member()"
    )

    op.execute(
        r"""
        CREATE FUNCTION reject_corporate_action_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        BEGIN
            RAISE EXCEPTION 'corporate-action evidence is append-only'
                USING ERRCODE = '55000';
        END;
        $$
        """
    )

    for table in (
        "corporate_action_versions",
        "corporate_action_collections",
        "corporate_action_collection_members",
        "corporate_action_collection_availability",
    ):
        op.execute(
            f"CREATE TRIGGER {table}_no_row_mutation BEFORE UPDATE OR DELETE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION reject_corporate_action_mutation()"
        )
        op.execute(
            f"CREATE TRIGGER {table}_no_truncate BEFORE TRUNCATE ON {table} "
            "FOR EACH STATEMENT EXECUTE FUNCTION reject_corporate_action_mutation()"
        )

    op.execute(
        r"""
        CREATE FUNCTION canonical_corporate_action_json(value jsonb)
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
                            public.canonical_corporate_action_json(member),
                            ',' ORDER BY key COLLATE "C"
                        ),
                        ''
                    ) || '}'
                    FROM jsonb_each(value) AS item(key, member)
                )
                WHEN 'array' THEN (
                    SELECT '[' || COALESCE(
                        string_agg(
                            public.canonical_corporate_action_json(member),
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
        CREATE FUNCTION parse_corporate_action_date(value text)
        RETURNS date
        LANGUAGE plpgsql
        IMMUTABLE STRICT PARALLEL SAFE
        SET search_path = pg_catalog, public
        AS $$
        DECLARE parsed date;
        BEGIN
            IF value !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                RAISE EXCEPTION 'corporate-action date is not canonical'
                    USING ERRCODE = '22007';
            END IF;
            parsed := value::date;
            IF to_char(parsed, 'YYYY-MM-DD') <> value THEN
                RAISE EXCEPTION 'corporate-action date does not round trip'
                    USING ERRCODE = '22007';
            END IF;
            RETURN parsed;
        END;
        $$
        """
    )
    op.execute(
        r"""
        CREATE FUNCTION parse_corporate_action_timestamp(value text)
        RETURNS timestamptz
        LANGUAGE plpgsql
        IMMUTABLE STRICT PARALLEL SAFE
        SET search_path = pg_catalog, public
        AS $$
        DECLARE parsed timestamptz;
        BEGIN
            IF value !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:'
                         '[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$' THEN
                RAISE EXCEPTION 'corporate-action timestamp is not canonical'
                    USING ERRCODE = '22007';
            END IF;
            parsed := value::timestamptz;
            IF to_char(
                parsed AT TIME ZONE 'UTC',
                'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
            ) <> value THEN
                RAISE EXCEPTION 'corporate-action timestamp does not round trip'
                    USING ERRCODE = '22007';
            END IF;
            RETURN parsed;
        END;
        $$
        """
    )
    op.execute(
        r"""
        CREATE FUNCTION parse_corporate_action_decimal(value text)
        RETURNS numeric
        LANGUAGE plpgsql
        IMMUTABLE STRICT PARALLEL SAFE
        SET search_path = pg_catalog, public
        AS $$
        DECLARE parsed numeric;
        BEGIN
            IF length(value) NOT BETWEEN 1 AND 39
               OR value !~ '^(0|[1-9][0-9]{0,19})(\.[0-9]{0,17}[1-9])?$' THEN
                RAISE EXCEPTION 'corporate-action decimal is not canonical'
                    USING ERRCODE = '22023';
            END IF;
            parsed := value::numeric;
            IF parsed <= 0 OR trim_scale(parsed)::text <> value THEN
                RAISE EXCEPTION 'corporate-action decimal must be canonical and positive'
                    USING ERRCODE = '22023';
            END IF;
            RETURN parsed;
        END;
        $$
        """
    )

    op.execute(
        f"""
        CREATE FUNCTION publish_corporate_action_collection(
            manifest_bytes bytea,
            event_bytes bytea[]
        ) RETURNS text
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            manifest jsonb;
            event jsonb;
            event_payload bytea;
            collection_identity text;
            event_identity text;
            source_value text;
            symbol_value text;
            action_type_value text;
            endpoint_value text;
            request_identity text;
            coverage_start_value date;
            coverage_end_value date;
            fetched_value timestamptz;
            effective_value date;
            provider_event_identity text;
            event_count_value integer;
            inserted_count integer;
            index_value integer;
            seen_provider_ids text[] := ARRAY[]::text[];
            split_from_text text;
            split_to_text text;
            historical_factor_text text;
            cash_text text;
            split_cash_text text;
            frequency_value integer;
        BEGIN
            IF manifest_bytes IS NULL
               OR octet_length(manifest_bytes) NOT BETWEEN 1 AND 1048576
               OR event_bytes IS NULL
               OR cardinality(event_bytes) > 5000 THEN
                RAISE EXCEPTION 'corporate-action publication exceeds its bounds'
                    USING ERRCODE = '22023';
            END IF;
            BEGIN
                manifest := convert_from(manifest_bytes, 'UTF8')::jsonb;
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'corporate-action manifest is not valid UTF-8 JSON'
                    USING ERRCODE = '22023';
            END;
            IF jsonb_typeof(manifest) <> 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(manifest)) <> 14
               OR NOT (manifest ?& ARRAY[
                   'action_type', 'coverage', 'endpoint', 'event_count',
                   'event_version_ids', 'format', 'page', 'provider_origin',
                   'provider_request_id', 'query_policy_hash',
                   'response_completed_at', 'schema_version', 'source', 'symbol'
               ])
               OR jsonb_typeof(manifest->'coverage') <> 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(manifest->'coverage')) <> 2
               OR NOT ((manifest->'coverage') ?& ARRAY['start', 'end'])
               OR jsonb_typeof(manifest->'page') <> 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(manifest->'page')) <> 3
               OR NOT ((manifest->'page') ?& ARRAY[
                   'count', 'limit', 'pagination_exhausted'
               ])
               OR jsonb_typeof(manifest->'event_version_ids') <> 'array' THEN
                RAISE EXCEPTION 'corporate-action manifest shape is not supported'
                    USING ERRCODE = '22023';
            END IF;
            IF jsonb_typeof(manifest->'action_type') <> 'string'
               OR jsonb_typeof(manifest->'endpoint') <> 'string'
               OR jsonb_typeof(manifest->'format') <> 'string'
               OR jsonb_typeof(manifest->'provider_origin') <> 'string'
               OR jsonb_typeof(manifest->'provider_request_id') <> 'string'
               OR jsonb_typeof(manifest->'query_policy_hash') <> 'string'
               OR jsonb_typeof(manifest->'response_completed_at') <> 'string'
               OR jsonb_typeof(manifest->'source') <> 'string'
               OR jsonb_typeof(manifest->'symbol') <> 'string'
               OR jsonb_typeof(manifest->'schema_version') <> 'number'
               OR (manifest->'schema_version')::text <> '1'
               OR jsonb_typeof(manifest->'event_count') <> 'number'
               OR (manifest->'event_count')::text !~ '^(0|[1-9][0-9]{{0,3}})$'
               OR jsonb_typeof(manifest->'coverage'->'start') <> 'string'
               OR jsonb_typeof(manifest->'coverage'->'end') <> 'string'
               OR jsonb_typeof(manifest->'page'->'count') <> 'number'
               OR (manifest->'page'->'count')::text <> '1'
               OR jsonb_typeof(manifest->'page'->'limit') <> 'number'
               OR (manifest->'page'->'limit')::text <> '5000'
               OR jsonb_typeof(
                   manifest->'page'->'pagination_exhausted'
               ) <> 'boolean'
               OR manifest->'page'->'pagination_exhausted' <> 'true'::jsonb
               OR EXISTS (
                   SELECT 1
                   FROM jsonb_array_elements(
                       manifest->'event_version_ids'
                   ) AS version_id(value)
                   WHERE jsonb_typeof(version_id.value) <> 'string'
                      OR version_id.value#>>'{{}}' !~ '^sha256:[0-9a-f]{{64}}$'
               ) THEN
                RAISE EXCEPTION 'corporate-action manifest scalar types are invalid'
                    USING ERRCODE = '22023';
            END IF;
            IF convert_to(
                public.canonical_corporate_action_json(manifest), 'UTF8'
            ) <> manifest_bytes THEN
                RAISE EXCEPTION 'corporate-action manifest is not canonical JSON'
                    USING ERRCODE = '22023';
            END IF;

            source_value := manifest->>'source';
            symbol_value := manifest->>'symbol';
            action_type_value := manifest->>'action_type';
            endpoint_value := manifest->>'endpoint';
            request_identity := manifest->>'provider_request_id';
            coverage_start_value := public.parse_corporate_action_date(
                manifest->'coverage'->>'start'
            );
            coverage_end_value := public.parse_corporate_action_date(
                manifest->'coverage'->>'end'
            );
            fetched_value := public.parse_corporate_action_timestamp(
                manifest->>'response_completed_at'
            );
            event_count_value := (manifest->>'event_count')::integer;
            collection_identity := 'sha256:' || encode(digest(manifest_bytes, 'sha256'), 'hex');

            IF manifest->>'format' <> 'corporate-action-complete-collection-v1'
               OR (manifest->>'schema_version')::integer <> 1
               OR manifest->>'query_policy_hash' <> '{_QUERY_POLICY_HASH}'
               OR manifest->>'provider_origin' <> 'https://api.massive.com'
               OR source_value <> 'polygon'
               OR action_type_value NOT IN ('split', 'dividend')
               OR endpoint_value <> (CASE action_type_value
                    WHEN 'split' THEN '/stocks/v1/splits'
                    ELSE '/stocks/v1/dividends'
                  END)
               OR symbol_value IS NULL
               OR symbol_value <> upper(symbol_value)
               OR symbol_value !~ '^[A-Z0-9.\\-_:]+$'
               OR length(symbol_value) NOT BETWEEN 1 AND 32
               OR request_identity IS NULL
               OR length(request_identity) NOT BETWEEN 1 AND 128
               OR request_identity !~ '^[A-Za-z0-9._:\-]+$'
               OR coverage_start_value > coverage_end_value
               OR (manifest->'page'->>'count')::integer <> 1
               OR (manifest->'page'->>'limit')::integer <> 5000
               OR (manifest->'page'->>'pagination_exhausted')::boolean IS NOT TRUE
               OR event_count_value NOT BETWEEN 0 AND 5000
               OR jsonb_array_length(manifest->'event_version_ids') <> event_count_value
               OR cardinality(event_bytes) <> event_count_value THEN
                RAISE EXCEPTION 'corporate-action manifest violates the query policy'
                    USING ERRCODE = '22023';
            END IF;

            INSERT INTO public.corporate_action_collections (
                collection_id, schema_version, query_policy_hash, source,
                endpoint, action_type, symbol, coverage_start, coverage_end,
                page_limit, page_count, event_count, pagination_exhausted,
                provider_request_id, fetched_at, canonical_manifest, creator_xid
            ) VALUES (
                collection_identity, 1, '{_QUERY_POLICY_HASH}', source_value,
                endpoint_value, action_type_value, symbol_value,
                coverage_start_value, coverage_end_value, 5000, 1,
                event_count_value, true, request_identity, fetched_value,
                manifest_bytes, 1
            ) ON CONFLICT (collection_id) DO NOTHING;
            GET DIAGNOSTICS inserted_count = ROW_COUNT;

            IF NOT EXISTS (
                SELECT 1 FROM public.corporate_action_collections
                WHERE collection_id = collection_identity
                  AND schema_version = 1
                  AND query_policy_hash = '{_QUERY_POLICY_HASH}'
                  AND source = source_value
                  AND endpoint = endpoint_value
                  AND action_type = action_type_value
                  AND symbol = symbol_value
                  AND coverage_start = coverage_start_value
                  AND coverage_end = coverage_end_value
                  AND page_limit = 5000
                  AND page_count = 1
                  AND event_count = event_count_value
                  AND pagination_exhausted
                  AND provider_request_id = request_identity
                  AND fetched_at = fetched_value
                  AND canonical_manifest = manifest_bytes
            ) THEN
                RAISE EXCEPTION 'corporate-action collection identity collision'
                    USING ERRCODE = '55000';
            END IF;

            IF event_count_value > 0 THEN
                FOR index_value IN 1..event_count_value LOOP
                    event_payload := event_bytes[index_value];
                    IF event_payload IS NULL
                       OR octet_length(event_payload) NOT BETWEEN 1 AND 65536 THEN
                        RAISE EXCEPTION 'corporate-action event exceeds its bound'
                            USING ERRCODE = '22023';
                    END IF;
                    BEGIN
                        event := convert_from(event_payload, 'UTF8')::jsonb;
                    EXCEPTION WHEN OTHERS THEN
                        RAISE EXCEPTION 'corporate-action event is not valid UTF-8 JSON'
                            USING ERRCODE = '22023';
                    END;
                    event_identity :=
                        'sha256:' || encode(digest(event_payload, 'sha256'), 'hex');
                    IF manifest->'event_version_ids'->>(index_value - 1) <> event_identity
                       OR jsonb_typeof(event) <> 'object' THEN
                        RAISE EXCEPTION 'corporate-action event identity is not in the manifest'
                            USING ERRCODE = '22023';
                    END IF;

                    IF NOT (event ?& ARRAY[
                           'action_type', 'effective_date',
                           'historical_adjustment_factor', 'provider_event_id',
                           'schema_version', 'source', 'status', 'symbol'
                       ])
                       OR jsonb_typeof(event->'action_type') <> 'string'
                       OR jsonb_typeof(event->'effective_date') <> 'string'
                       OR jsonb_typeof(
                           event->'historical_adjustment_factor'
                       ) <> 'string'
                       OR jsonb_typeof(event->'provider_event_id') <> 'string'
                       OR jsonb_typeof(event->'schema_version') <> 'number'
                       OR (event->'schema_version')::text <> '1'
                       OR jsonb_typeof(event->'source') <> 'string'
                       OR jsonb_typeof(event->'status') <> 'string'
                       OR jsonb_typeof(event->'symbol') <> 'string' THEN
                        RAISE EXCEPTION 'corporate-action event scalar types are invalid'
                            USING ERRCODE = '22023';
                    END IF;

                    provider_event_identity := event->>'provider_event_id';
                    effective_value := public.parse_corporate_action_date(
                        event->>'effective_date'
                    );
                    historical_factor_text := event->>'historical_adjustment_factor';
                    IF provider_event_identity IS NULL
                       OR length(provider_event_identity) NOT BETWEEN 1 AND 128
                       OR provider_event_identity !~ '^[A-Za-z0-9._:\-]+$'
                       OR provider_event_identity = ANY(seen_provider_ids)
                       OR event->>'action_type' <> action_type_value
                       OR event->>'source' <> source_value
                       OR event->>'symbol' <> symbol_value
                       OR event->>'status' <> 'active'
                       OR effective_value < coverage_start_value
                       OR effective_value > coverage_end_value
                       OR historical_factor_text IS NULL THEN
                        RAISE EXCEPTION 'corporate-action event violates its scope'
                            USING ERRCODE = '22023';
                    END IF;
                    PERFORM public.parse_corporate_action_decimal(
                        historical_factor_text
                    );
                    IF convert_to(
                        public.canonical_corporate_action_json(event), 'UTF8'
                    ) <> event_payload THEN
                        RAISE EXCEPTION 'corporate-action event is not canonical JSON'
                            USING ERRCODE = '22023';
                    END IF;
                    seen_provider_ids := array_append(
                        seen_provider_ids, provider_event_identity
                    );

                    IF action_type_value = 'split' THEN
                        IF (SELECT count(*) FROM jsonb_object_keys(event)) <> 11
                           OR NOT (event ?& ARRAY[
                               'action_type', 'adjustment_type', 'effective_date',
                               'historical_adjustment_factor', 'provider_event_id',
                               'schema_version', 'source', 'split_from', 'split_to',
                               'status', 'symbol'
                           ])
                           OR jsonb_typeof(event->'adjustment_type') <> 'string'
                           OR jsonb_typeof(event->'split_from') <> 'string'
                           OR jsonb_typeof(event->'split_to') <> 'string'
                           OR event->>'adjustment_type'
                              NOT IN ('forward_split', 'reverse_split', 'stock_dividend') THEN
                            RAISE EXCEPTION 'corporate-action split shape is not supported'
                                USING ERRCODE = '22023';
                        END IF;
                        split_from_text := event->>'split_from';
                        split_to_text := event->>'split_to';
                        IF split_from_text IS NULL OR split_to_text IS NULL
                           THEN
                            RAISE EXCEPTION 'corporate-action split numerics are invalid'
                                USING ERRCODE = '22023';
                        END IF;
                        PERFORM public.parse_corporate_action_decimal(split_from_text);
                        PERFORM public.parse_corporate_action_decimal(split_to_text);
                        INSERT INTO public.corporate_action_versions (
                            action_version_id, schema_version, source, action_type,
                            provider_event_id, symbol, effective_date, status,
                            split_from, split_to, adjustment_type,
                            historical_adjustment_factor, canonical_event, creator_xid
                        ) VALUES (
                            event_identity, 1, source_value, 'split',
                            provider_event_identity, symbol_value, effective_value,
                            'active', public.parse_corporate_action_decimal(
                                split_from_text
                            ), public.parse_corporate_action_decimal(split_to_text),
                            event->>'adjustment_type',
                            public.parse_corporate_action_decimal(
                                historical_factor_text
                            ), event_payload, 1
                        ) ON CONFLICT (action_version_id) DO NOTHING;
                        IF NOT EXISTS (
                            SELECT 1 FROM public.corporate_action_versions
                            WHERE action_version_id = event_identity
                              AND schema_version = 1
                              AND source = source_value
                              AND action_type = 'split'
                              AND provider_event_id = provider_event_identity
                              AND symbol = symbol_value
                              AND effective_date = effective_value
                              AND status = 'active'
                              AND split_from = public.parse_corporate_action_decimal(
                                  split_from_text
                              )
                              AND split_to = public.parse_corporate_action_decimal(
                                  split_to_text
                              )
                              AND adjustment_type = event->>'adjustment_type'
                              AND cash_amount IS NULL
                              AND split_adjusted_cash_amount IS NULL
                              AND currency IS NULL
                              AND declaration_date IS NULL
                              AND record_date IS NULL
                              AND pay_date IS NULL
                              AND frequency IS NULL
                              AND distribution_type IS NULL
                              AND historical_adjustment_factor
                                  = public.parse_corporate_action_decimal(
                                      historical_factor_text
                                  )
                              AND canonical_event = event_payload
                        ) THEN
                            RAISE EXCEPTION
                                'corporate-action split projection conflicts with content'
                                USING ERRCODE = '55000';
                        END IF;
                    ELSE
                        IF (SELECT count(*) FROM jsonb_object_keys(event)) <> 16
                           OR NOT (event ?& ARRAY[
                               'action_type', 'cash_amount', 'currency',
                               'declaration_date', 'distribution_type',
                               'effective_date', 'frequency',
                               'historical_adjustment_factor', 'pay_date',
                               'provider_event_id', 'record_date', 'schema_version',
                               'source', 'split_adjusted_cash_amount', 'status',
                               'symbol'
                           ])
                           OR jsonb_typeof(event->'cash_amount') <> 'string'
                           OR jsonb_typeof(event->'currency') <> 'string'
                           OR jsonb_typeof(event->'distribution_type') <> 'string'
                           OR jsonb_typeof(
                               event->'split_adjusted_cash_amount'
                           ) <> 'string'
                           OR jsonb_typeof(event->'declaration_date')
                              NOT IN ('null', 'string')
                           OR jsonb_typeof(event->'record_date')
                              NOT IN ('null', 'string')
                           OR jsonb_typeof(event->'pay_date')
                              NOT IN ('null', 'string')
                           OR jsonb_typeof(event->'frequency')
                              NOT IN ('null', 'number')
                           OR event->>'distribution_type' NOT IN (
                               'recurring', 'special', 'supplemental',
                               'irregular', 'unknown'
                           ) THEN
                            RAISE EXCEPTION 'corporate-action dividend shape is not supported'
                                USING ERRCODE = '22023';
                        END IF;
                        cash_text := event->>'cash_amount';
                        split_cash_text := event->>'split_adjusted_cash_amount';
                        IF cash_text IS NULL OR split_cash_text IS NULL
                           OR event->>'currency' IS NULL
                           OR event->>'currency' !~ '^[A-Z]{{3}}$' THEN
                            RAISE EXCEPTION 'corporate-action dividend numerics are invalid'
                                USING ERRCODE = '22023';
                        END IF;
                        PERFORM public.parse_corporate_action_decimal(cash_text);
                        PERFORM public.parse_corporate_action_decimal(split_cash_text);
                        IF event->'declaration_date' <> 'null'::jsonb THEN
                            PERFORM public.parse_corporate_action_date(
                                event->>'declaration_date'
                            );
                        END IF;
                        IF event->'record_date' <> 'null'::jsonb THEN
                            PERFORM public.parse_corporate_action_date(
                                event->>'record_date'
                            );
                        END IF;
                        IF event->'pay_date' <> 'null'::jsonb THEN
                            PERFORM public.parse_corporate_action_date(
                                event->>'pay_date'
                            );
                        END IF;
                        frequency_value := CASE
                            WHEN event->'frequency' = 'null'::jsonb THEN NULL
                            ELSE (event->>'frequency')::integer
                        END;
                        IF frequency_value IS NOT NULL AND (
                            (event->'frequency')::text !~ '^(0|[1-9][0-9]*)$'
                            OR frequency_value < 0
                        ) THEN
                            RAISE EXCEPTION 'corporate-action dividend frequency is invalid'
                                USING ERRCODE = '22023';
                        END IF;
                        INSERT INTO public.corporate_action_versions (
                            action_version_id, schema_version, source, action_type,
                            provider_event_id, symbol, effective_date, status,
                            cash_amount, split_adjusted_cash_amount, currency,
                            declaration_date, record_date, pay_date, frequency,
                            distribution_type, historical_adjustment_factor,
                            canonical_event, creator_xid
                        ) VALUES (
                            event_identity, 1, source_value, 'dividend',
                            provider_event_identity, symbol_value, effective_value,
                            'active', public.parse_corporate_action_decimal(cash_text),
                            public.parse_corporate_action_decimal(split_cash_text),
                            event->>'currency',
                            CASE WHEN event->'declaration_date' = 'null'::jsonb
                                THEN NULL ELSE public.parse_corporate_action_date(
                                    event->>'declaration_date'
                                ) END,
                            CASE WHEN event->'record_date' = 'null'::jsonb
                                THEN NULL ELSE public.parse_corporate_action_date(
                                    event->>'record_date'
                                ) END,
                            CASE WHEN event->'pay_date' = 'null'::jsonb
                                THEN NULL ELSE public.parse_corporate_action_date(
                                    event->>'pay_date'
                                ) END,
                            frequency_value, event->>'distribution_type',
                            public.parse_corporate_action_decimal(
                                historical_factor_text
                            ), event_payload, 1
                        ) ON CONFLICT (action_version_id) DO NOTHING;
                        IF NOT EXISTS (
                            SELECT 1 FROM public.corporate_action_versions
                            WHERE action_version_id = event_identity
                              AND schema_version = 1
                              AND source = source_value
                              AND action_type = 'dividend'
                              AND provider_event_id = provider_event_identity
                              AND symbol = symbol_value
                              AND effective_date = effective_value
                              AND status = 'active'
                              AND split_from IS NULL
                              AND split_to IS NULL
                              AND adjustment_type IS NULL
                              AND cash_amount = public.parse_corporate_action_decimal(
                                  cash_text
                              )
                              AND split_adjusted_cash_amount
                                  = public.parse_corporate_action_decimal(
                                      split_cash_text
                                  )
                              AND currency = event->>'currency'
                              AND declaration_date IS NOT DISTINCT FROM
                                  CASE WHEN event->'declaration_date' = 'null'::jsonb
                                      THEN NULL ELSE public.parse_corporate_action_date(
                                          event->>'declaration_date'
                                      ) END
                              AND record_date IS NOT DISTINCT FROM
                                  CASE WHEN event->'record_date' = 'null'::jsonb
                                      THEN NULL ELSE public.parse_corporate_action_date(
                                          event->>'record_date'
                                      ) END
                              AND pay_date IS NOT DISTINCT FROM
                                  CASE WHEN event->'pay_date' = 'null'::jsonb
                                      THEN NULL ELSE public.parse_corporate_action_date(
                                          event->>'pay_date'
                                      ) END
                              AND frequency IS NOT DISTINCT FROM frequency_value
                              AND distribution_type = event->>'distribution_type'
                              AND historical_adjustment_factor
                                  = public.parse_corporate_action_decimal(
                                      historical_factor_text
                                  )
                              AND canonical_event = event_payload
                        ) THEN
                            RAISE EXCEPTION
                                'corporate-action dividend projection conflicts with content'
                                USING ERRCODE = '55000';
                        END IF;
                    END IF;

                    IF NOT EXISTS (
                        SELECT 1 FROM public.corporate_action_versions
                        WHERE action_version_id = event_identity
                          AND canonical_event = event_payload
                    ) THEN
                        RAISE EXCEPTION 'corporate-action version identity collision'
                            USING ERRCODE = '55000';
                    END IF;

                    IF inserted_count = 1 THEN
                        INSERT INTO public.corporate_action_collection_members (
                            collection_id, ordinal, action_version_id, creator_xid
                        ) VALUES (
                            collection_identity, index_value - 1, event_identity, 1
                        );
                    END IF;
                END LOOP;
            END IF;

            IF (
                SELECT count(*) FROM public.corporate_action_collection_members
                WHERE collection_id = collection_identity
            ) <> event_count_value THEN
                RAISE EXCEPTION 'corporate-action manifest membership count conflicts'
                    USING ERRCODE = '55000';
            END IF;
            IF EXISTS (
                SELECT 1
                FROM jsonb_array_elements_text(
                    manifest->'event_version_ids'
                ) WITH ORDINALITY AS expected(version_id, ordinal)
                LEFT JOIN public.corporate_action_collection_members AS member
                  ON member.collection_id = collection_identity
                 AND member.ordinal = expected.ordinal - 1
                 AND member.action_version_id = expected.version_id
                WHERE member.collection_id IS NULL
            ) THEN
                RAISE EXCEPTION 'corporate-action manifest membership conflicts'
                    USING ERRCODE = '55000';
            END IF;
            RETURN collection_identity;
        EXCEPTION
            WHEN invalid_text_representation OR datetime_field_overflow
                 OR numeric_value_out_of_range THEN
                RAISE EXCEPTION 'corporate-action evidence contains an invalid scalar'
                    USING ERRCODE = '22023';
        END;
        $$
        """
    )

    op.execute(
        r"""
        CREATE FUNCTION stamp_corporate_action_collection_availability()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            collection_row public.corporate_action_collections%ROWTYPE;
            member_count integer;
            logical_event_count integer;
            invalid_member_count integer;
        BEGIN
            SELECT * INTO STRICT collection_row
            FROM public.corporate_action_collections
            WHERE collection_id = NEW.collection_id
              AND recorded_at = NEW.collection_recorded_at;

            IF collection_row.creator_xid = txid_current() THEN
                RAISE EXCEPTION
                    'corporate-action collection availability requires a later transaction'
                    USING ERRCODE = '55000';
            END IF;

            PERFORM set_config('lock_timeout', '30s', true);
            PERFORM pg_advisory_xact_lock(
                public.corporate_action_series_fence_id(
                    collection_row.source,
                    collection_row.symbol,
                    collection_row.action_type
                )
            );

            SELECT count(*), count(DISTINCT version.provider_event_id),
                   count(*) FILTER (
                       WHERE version.source <> collection_row.source
                          OR version.symbol <> collection_row.symbol
                          OR version.action_type <> collection_row.action_type
                          OR version.effective_date < collection_row.coverage_start
                          OR version.effective_date > collection_row.coverage_end
                          OR member.creator_xid = txid_current()
                          OR version.creator_xid = txid_current()
                   )
            INTO member_count, logical_event_count, invalid_member_count
            FROM public.corporate_action_collection_members AS member
            JOIN public.corporate_action_versions AS version
              ON version.action_version_id = member.action_version_id
            WHERE member.collection_id = NEW.collection_id;

            IF member_count <> collection_row.event_count
               OR logical_event_count <> collection_row.event_count
               OR invalid_member_count <> 0 THEN
                RAISE EXCEPTION
                    'corporate-action collection members do not match the complete manifest'
                    USING ERRCODE = '55000';
            END IF;

            NEW.available_at := clock_timestamp();
            RETURN NEW;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RAISE EXCEPTION 'corporate-action collection does not exist'
                    USING ERRCODE = '23503';
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER corporate_action_collection_availability_stamp "
        "BEFORE INSERT ON corporate_action_collection_availability FOR EACH ROW "
        "EXECUTE FUNCTION stamp_corporate_action_collection_availability()"
    )

    op.execute(
        r"""
        CREATE FUNCTION publish_corporate_action_collection_receipt(
            requested_collection_id text
        ) RETURNS TABLE (
            collection_id text,
            collection_recorded_at timestamptz,
            available_at timestamptz
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            recorded timestamptz;
        BEGIN
            IF requested_collection_id !~ '^sha256:[0-9a-f]{64}$' THEN
                RAISE EXCEPTION 'corporate-action collection id is malformed'
                    USING ERRCODE = '22023';
            END IF;
            SELECT value.recorded_at INTO STRICT recorded
            FROM public.corporate_action_collections AS value
            WHERE value.collection_id = requested_collection_id;

            INSERT INTO public.corporate_action_collection_availability (
                collection_id, collection_recorded_at
            ) VALUES (requested_collection_id, recorded)
            ON CONFLICT ON CONSTRAINT pk_corporate_action_collection_availability
            DO NOTHING;

            RETURN QUERY
            SELECT receipt.collection_id::text,
                   receipt.collection_recorded_at,
                   receipt.available_at
            FROM public.corporate_action_collection_availability AS receipt
            WHERE receipt.collection_id = requested_collection_id;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RAISE EXCEPTION 'corporate-action collection does not exist'
                    USING ERRCODE = '23503';
        END;
        $$
        """
    )

    for function in (
        "stamp_corporate_action_evidence()",
        "corporate_action_series_fence_id(text,text,text)",
        "stamp_corporate_action_collection_member()",
        "reject_corporate_action_mutation()",
        "canonical_corporate_action_json(jsonb)",
        "parse_corporate_action_date(text)",
        "parse_corporate_action_timestamp(text)",
        "parse_corporate_action_decimal(text)",
        "publish_corporate_action_collection(bytea,bytea[])",
        "stamp_corporate_action_collection_availability()",
        "publish_corporate_action_collection_receipt(text)",
    ):
        op.execute(f"REVOKE ALL ON FUNCTION public.{function} FROM PUBLIC")

    tables = ", ".join(
        (
            "public.corporate_action_versions",
            "public.corporate_action_collections",
            "public.corporate_action_collection_members",
            "public.corporate_action_collection_availability",
        )
    )
    op.execute(f"REVOKE ALL ON TABLE {tables} FROM PUBLIC")
    op.execute(f"REVOKE ALL ON TABLE {tables} FROM stockapi_app")
    op.execute(f"REVOKE ALL ON TABLE {tables} FROM stockapi_snapshot_builder")
    op.execute(f"GRANT SELECT ON TABLE {tables} TO stockapi_app")
    op.execute(f"GRANT SELECT ON TABLE {tables} TO stockapi_snapshot_builder")
    op.execute(
        "GRANT EXECUTE ON FUNCTION "
        "public.publish_corporate_action_collection(bytea,bytea[]) TO stockapi_app"
    )
    op.execute(
        "GRANT EXECUTE ON FUNCTION "
        "public.publish_corporate_action_collection_receipt(text) TO stockapi_app"
    )
    op.execute(
        "GRANT EXECUTE ON FUNCTION "
        "public.corporate_action_series_fence_id(text,text,text) "
        "TO stockapi_snapshot_builder"
    )


def downgrade() -> None:
    op.execute(
        r"""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM public.corporate_action_versions)
               OR EXISTS (SELECT 1 FROM public.corporate_action_collections)
               OR EXISTS (
                   SELECT 1 FROM public.corporate_action_collection_members
               )
               OR EXISTS (
                   SELECT 1 FROM public.corporate_action_collection_availability
               ) THEN
                RAISE EXCEPTION 'cannot downgrade nonempty corporate-action evidence'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $$
        """
    )
    for table in (
        "corporate_action_collection_availability",
        "corporate_action_collection_members",
        "corporate_action_collections",
        "corporate_action_versions",
    ):
        op.execute(f"DROP TRIGGER IF EXISTS {table}_no_truncate ON {table}")
        op.execute(f"DROP TRIGGER IF EXISTS {table}_no_row_mutation ON {table}")
    op.execute(
        "DROP TRIGGER IF EXISTS corporate_action_collection_availability_stamp "
        "ON corporate_action_collection_availability"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS corporate_action_collection_members_stamp "
        "ON corporate_action_collection_members"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS corporate_action_collections_stamp ON corporate_action_collections"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS corporate_action_versions_stamp ON corporate_action_versions"
    )
    op.execute("DROP FUNCTION IF EXISTS publish_corporate_action_collection_receipt(text)")
    op.execute("DROP FUNCTION IF EXISTS stamp_corporate_action_collection_availability()")
    op.execute("DROP FUNCTION IF EXISTS publish_corporate_action_collection(bytea,bytea[])")
    op.execute("DROP FUNCTION IF EXISTS parse_corporate_action_decimal(text)")
    op.execute("DROP FUNCTION IF EXISTS parse_corporate_action_timestamp(text)")
    op.execute("DROP FUNCTION IF EXISTS parse_corporate_action_date(text)")
    op.execute("DROP FUNCTION IF EXISTS canonical_corporate_action_json(jsonb)")
    op.execute("DROP FUNCTION IF EXISTS reject_corporate_action_mutation()")
    op.execute("DROP FUNCTION IF EXISTS stamp_corporate_action_collection_member()")
    op.execute("DROP FUNCTION IF EXISTS corporate_action_series_fence_id(text,text,text)")
    op.execute("DROP FUNCTION IF EXISTS stamp_corporate_action_evidence()")
    op.drop_table("corporate_action_collection_availability")
    op.drop_table("corporate_action_collection_members")
    op.drop_index(
        "ix_corporate_action_collections_scope",
        table_name="corporate_action_collections",
    )
    op.drop_table("corporate_action_collections")
    op.drop_index(
        "ix_corporate_action_versions_series_date",
        table_name="corporate_action_versions",
    )
    op.drop_table("corporate_action_versions")
