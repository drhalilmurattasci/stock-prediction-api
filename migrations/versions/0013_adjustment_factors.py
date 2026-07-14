"""add immutable, point-in-time adjustment-factor sets

Revision ID: 0013_adjustment_factors
Revises: 0012_corporate_actions
Create Date: 2026-07-14

Factor sets are published from their exact canonical bytes by a narrow
SECURITY DEFINER boundary.  Relational entries bind the exact post-commit bar
and corporate-action receipts used by the pure factor kernel.  A second
transaction publishes availability only after membership is frozen.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0013_adjustment_factors"
down_revision: str | None = "0012_corporate_actions"
branch_labels = None
depends_on = None

_FACTOR_FORMAT = "stockapi-adjustment-factor-set-v1"
_POLICY_VERSION = "split-dividend-gross-total-return-v1"
_POLICY_HASH = "sha256:f825ca4aa36725fb98a2697dd339b07275397711b0caaf488e9c87d70afd2b37"
_ACTION_QUERY_POLICY_HASH = (
    "sha256:a9784b0ed486b1cdce14596b546f83e7da4035209b05427564f1e529a96d5127"
)


def upgrade() -> None:
    op.create_table(
        "adjustment_factor_sets",
        sa.Column("factor_set_id", sa.String(length=71), nullable=False),
        sa.Column("format", sa.String(length=64), nullable=False),
        sa.Column("policy_version", sa.String(length=64), nullable=False),
        sa.Column("policy_hash", sa.String(length=71), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("cutoff", sa.DateTime(timezone=True), nullable=False),
        sa.Column("anchor_date", sa.Date(), nullable=False),
        sa.Column("coverage_start", sa.Date(), nullable=False),
        sa.Column("coverage_end", sa.Date(), nullable=False),
        sa.Column("input_count", sa.Integer(), nullable=False),
        sa.Column("max_input_available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("split_collection_id", sa.String(length=71), nullable=False),
        sa.Column(
            "split_collection_recorded_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "split_collection_available_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column("dividend_collection_id", sa.String(length=71), nullable=False),
        sa.Column(
            "dividend_collection_recorded_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "dividend_collection_available_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column("canonical_payload", sa.LargeBinary(), nullable=False),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.Column("creator_xid", sa.BigInteger(), nullable=False),
        sa.CheckConstraint(
            "factor_set_id ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_adjustment_factor_sets_factor_set_id_format"),
        ),
        sa.CheckConstraint(
            "policy_hash ~ '^sha256:[0-9a-f]{64}$'",
            name=op.f("ck_adjustment_factor_sets_policy_hash_format"),
        ),
        sa.CheckConstraint(
            "symbol = upper(symbol)",
            name=op.f("ck_adjustment_factor_sets_symbol_uppercase"),
        ),
        sa.CheckConstraint(
            "symbol ~ '^[A-Z0-9.\\-_:]+$'",
            name=op.f("ck_adjustment_factor_sets_symbol_format"),
        ),
        sa.CheckConstraint(
            "split_collection_id ~ '^sha256:[0-9a-f]{64}$' "
            "AND dividend_collection_id ~ '^sha256:[0-9a-f]{64}$' "
            "AND split_collection_id <> dividend_collection_id",
            name=op.f("ck_adjustment_factor_sets_collection_ids_valid"),
        ),
        sa.CheckConstraint(
            "coverage_start <= anchor_date AND anchor_date = coverage_end",
            name=op.f("ck_adjustment_factor_sets_coverage_order"),
        ),
        sa.CheckConstraint(
            "input_count BETWEEN 1 AND 5000",
            name=op.f("ck_adjustment_factor_sets_input_count_bounded"),
        ),
        sa.CheckConstraint(
            "split_collection_recorded_at <= split_collection_available_at "
            "AND dividend_collection_recorded_at <= dividend_collection_available_at "
            "AND split_collection_available_at <= cutoff "
            "AND dividend_collection_available_at <= cutoff "
            "AND max_input_available_at <= cutoff",
            name=op.f("ck_adjustment_factor_sets_input_availability_cutoff"),
        ),
        sa.CheckConstraint(
            "octet_length(canonical_payload) BETWEEN 1 AND 4194304",
            name=op.f("ck_adjustment_factor_sets_canonical_payload_size_bounded"),
        ),
        sa.CheckConstraint(
            "factor_set_id = 'sha256:' || encode(digest(canonical_payload, 'sha256'), 'hex')",
            name=op.f("ck_adjustment_factor_sets_factor_set_id_matches_payload"),
        ),
        sa.CheckConstraint(
            "creator_xid > 0",
            name=op.f("ck_adjustment_factor_sets_creator_xid_positive"),
        ),
        sa.ForeignKeyConstraint(
            (
                "split_collection_id",
                "split_collection_recorded_at",
                "split_collection_available_at",
            ),
            (
                "corporate_action_collection_availability.collection_id",
                "corporate_action_collection_availability.collection_recorded_at",
                "corporate_action_collection_availability.available_at",
            ),
            name="fk_adjustment_factor_sets_exact_split_collection_receipt",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            (
                "dividend_collection_id",
                "dividend_collection_recorded_at",
                "dividend_collection_available_at",
            ),
            (
                "corporate_action_collection_availability.collection_id",
                "corporate_action_collection_availability.collection_recorded_at",
                "corporate_action_collection_availability.available_at",
            ),
            name="fk_adjustment_factor_sets_exact_dividend_collection_receipt",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "factor_set_id",
            name=op.f("pk_adjustment_factor_sets"),
        ),
        sa.UniqueConstraint(
            "factor_set_id",
            "recorded_at",
            name="uq_adjustment_factor_sets_exact_recording",
        ),
    )
    op.create_index(
        "ix_adjustment_factor_sets_resolve",
        "adjustment_factor_sets",
        ["symbol", "cutoff", "anchor_date", "factor_set_id"],
    )

    op.create_table(
        "adjustment_factor_entries",
        sa.Column("factor_set_id", sa.String(length=71), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("observation_date", sa.Date(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("timespan", sa.String(length=16), nullable=False),
        sa.Column("multiplier", sa.SmallInteger(), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("adjustment_basis", sa.String(length=32), nullable=False),
        sa.Column("version_recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_close_decimal", sa.String(length=400), nullable=False),
        sa.Column("raw_close_f64_be", sa.LargeBinary(length=8), nullable=False),
        sa.Column("price_factor_decimal", sa.String(length=400), nullable=False),
        sa.Column("price_factor_f64_be", sa.LargeBinary(length=8), nullable=False),
        sa.Column("volume_factor_decimal", sa.String(length=400), nullable=False),
        sa.Column("volume_factor_f64_be", sa.LargeBinary(length=8), nullable=False),
        sa.Column("creator_xid", sa.BigInteger(), nullable=False),
        sa.CheckConstraint(
            "ordinal >= 0",
            name=op.f("ck_adjustment_factor_entries_ordinal_nonnegative"),
        ),
        sa.CheckConstraint(
            "symbol = upper(symbol)",
            name=op.f("ck_adjustment_factor_entries_symbol_uppercase"),
        ),
        sa.CheckConstraint(
            "timespan = 'day' AND multiplier = 1 "
            "AND source = 'polygon_open_close' AND adjustment_basis = 'raw'",
            name=op.f("ck_adjustment_factor_entries_raw_source_supported"),
        ),
        sa.CheckConstraint(
            "(observed_at AT TIME ZONE 'UTC')::date = observation_date "
            "AND observed_at <= version_recorded_at "
            "AND version_recorded_at <= raw_available_at",
            name=op.f("ck_adjustment_factor_entries_raw_receipt_time_order"),
        ),
        sa.CheckConstraint(
            "raw_close_decimal ~ '^(0|[1-9][0-9]*)(\\.[0-9]*[1-9])?$' "
            "AND raw_close_decimal::numeric > 0",
            name=op.f("ck_adjustment_factor_entries_raw_close_decimal_positive"),
        ),
        sa.CheckConstraint(
            "price_factor_decimal ~ '^(0|[1-9][0-9]*)(\\.[0-9]*[1-9])?$' "
            "AND price_factor_decimal::numeric > 0 "
            "AND volume_factor_decimal ~ '^(0|[1-9][0-9]*)(\\.[0-9]*[1-9])?$' "
            "AND volume_factor_decimal::numeric > 0",
            name=op.f("ck_adjustment_factor_entries_factor_decimals_positive"),
        ),
        sa.CheckConstraint(
            "octet_length(raw_close_f64_be) = 8 "
            "AND octet_length(price_factor_f64_be) = 8 "
            "AND octet_length(volume_factor_f64_be) = 8",
            name=op.f("ck_adjustment_factor_entries_binary64_width"),
        ),
        sa.CheckConstraint(
            "creator_xid > 0",
            name=op.f("ck_adjustment_factor_entries_creator_xid_positive"),
        ),
        sa.ForeignKeyConstraint(
            ("factor_set_id",),
            ("adjustment_factor_sets.factor_set_id",),
            name="fk_adjustment_factor_entries_factor_set",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            (
                "symbol",
                "timespan",
                "multiplier",
                "observed_at",
                "source",
                "adjustment_basis",
                "version_recorded_at",
                "raw_available_at",
            ),
            (
                "bar_version_availability.symbol",
                "bar_version_availability.timespan",
                "bar_version_availability.multiplier",
                "bar_version_availability.ts",
                "bar_version_availability.source",
                "bar_version_availability.adjustment_basis",
                "bar_version_availability.version_recorded_at",
                "bar_version_availability.available_at",
            ),
            name="fk_adjustment_factor_entries_exact_bar_receipt",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "factor_set_id",
            "ordinal",
            name=op.f("pk_adjustment_factor_entries"),
        ),
        sa.UniqueConstraint(
            "factor_set_id",
            "observation_date",
            name="uq_adjustment_factor_entries_observation",
        ),
    )

    op.create_table(
        "adjustment_factor_set_availability",
        sa.Column("factor_set_id", sa.String(length=71), nullable=False),
        sa.Column("factor_set_recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "available_at >= factor_set_recorded_at",
            name=op.f("ck_adjustment_factor_set_availability_available_after_factor_set"),
        ),
        sa.ForeignKeyConstraint(
            ("factor_set_id", "factor_set_recorded_at"),
            (
                "adjustment_factor_sets.factor_set_id",
                "adjustment_factor_sets.recorded_at",
            ),
            name="fk_adjustment_factor_set_availability_exact_factor_set",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "factor_set_id",
            name=op.f("pk_adjustment_factor_set_availability"),
        ),
        sa.UniqueConstraint(
            "factor_set_id",
            "factor_set_recorded_at",
            "available_at",
            name="uq_adjustment_factor_set_availability_exact_receipt",
        ),
    )

    _create_support_functions()
    _create_publishers()
    _apply_privileges()


def _create_support_functions() -> None:
    op.execute(
        r"""
        CREATE FUNCTION canonical_adjustment_factor_json(value jsonb)
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
                            public.canonical_adjustment_factor_json(member),
                            ',' ORDER BY key COLLATE "C"
                        ),
                        ''
                    ) || '}'
                    FROM jsonb_each(value) AS item(key, member)
                )
                WHEN 'array' THEN (
                    SELECT '[' || COALESCE(
                        string_agg(
                            public.canonical_adjustment_factor_json(member),
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
        CREATE FUNCTION adjustment_decimal34(value numeric)
        RETURNS numeric
        LANGUAGE plpgsql
        IMMUTABLE STRICT PARALLEL SAFE
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            rendered text;
            integer_part text;
            fractional text;
            significant text;
            coefficient_text text;
            result_text text;
            exponent_value integer;
            decimal_places integer;
            first_nonzero integer;
            next_digit integer;
            coefficient numeric;
        BEGIN
            rendered := value::text;
            IF rendered IN ('NaN', 'Infinity', '-Infinity') OR value <= 0 THEN
                RAISE EXCEPTION 'adjustment decimal must be positive'
                    USING ERRCODE = '22023';
            END IF;
            rendered := trim_scale(value)::text;
            IF value >= 1 THEN
                integer_part := split_part(rendered, '.', 1);
                fractional := split_part(rendered, '.', 2);
                exponent_value := length(integer_part) - 1;
                significant := integer_part || fractional;
            ELSE
                fractional := split_part(rendered, '.', 2);
                first_nonzero := 1;
                WHILE first_nonzero <= length(fractional)
                      AND substring(fractional FROM first_nonzero FOR 1) = '0' LOOP
                    first_nonzero := first_nonzero + 1;
                END LOOP;
                IF first_nonzero > length(fractional) THEN
                    RAISE EXCEPTION 'adjustment decimal has no significant digit'
                        USING ERRCODE = '22023';
                END IF;
                exponent_value := -first_nonzero;
                significant := substring(fractional FROM first_nonzero);
            END IF;

            IF length(significant) > 34 THEN
                coefficient_text := substring(significant FROM 1 FOR 34);
                coefficient := coefficient_text::numeric;
                next_digit := substring(significant FROM 35 FOR 1)::integer;
                IF next_digit > 5
                   OR (
                       next_digit = 5
                       AND (
                           substring(significant FROM 36) ~ '[1-9]'
                           OR mod(
                               substring(coefficient_text FROM 34 FOR 1)::integer,
                               2
                           ) = 1
                       )
                   ) THEN
                    coefficient := coefficient + 1;
                END IF;
                IF coefficient = power(10::numeric, 34) THEN
                    coefficient := power(10::numeric, 33);
                    exponent_value := exponent_value + 1;
                END IF;
            ELSE
                coefficient := significant::numeric;
            END IF;

            decimal_places := 33 - exponent_value;
            IF decimal_places NOT BETWEEN -400 AND 700 THEN
                RAISE EXCEPTION 'adjustment decimal exponent is unsupported'
                    USING ERRCODE = '22003';
            END IF;

            IF length(significant) <= 34 THEN
                RETURN value;
            END IF;

            coefficient_text := coefficient::text;
            IF decimal_places > 0 THEN
                IF length(coefficient_text) > decimal_places THEN
                    result_text :=
                        substring(
                            coefficient_text
                            FROM 1 FOR length(coefficient_text) - decimal_places
                        ) || '.' ||
                        substring(
                            coefficient_text
                            FROM length(coefficient_text) - decimal_places + 1
                        );
                ELSE
                    result_text := '0.' ||
                        repeat('0', decimal_places - length(coefficient_text)) ||
                        coefficient_text;
                END IF;
            ELSIF decimal_places < 0 THEN
                result_text := coefficient_text || repeat('0', -decimal_places);
            ELSE
                result_text := coefficient_text;
            END IF;
            RETURN result_text::numeric;
        END;
        $$
        """
    )
    op.execute(
        r"""
        CREATE FUNCTION adjustment_divide34(numerator numeric, denominator numeric)
        RETURNS numeric
        LANGUAGE plpgsql
        IMMUTABLE STRICT PARALLEL SAFE
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            numerator_rendered text;
            denominator_rendered text;
            fractional text;
            coefficient_text text;
            result_text text;
            first_nonzero integer;
            numerator_exponent integer;
            denominator_exponent integer;
            quotient_exponent integer;
            decimal_places integer;
            scaled_numerator numeric;
            scaled_denominator numeric;
            coefficient numeric;
            remainder numeric;
        BEGIN
            numerator_rendered := numerator::text;
            denominator_rendered := denominator::text;
            IF numerator_rendered IN ('NaN', 'Infinity', '-Infinity')
               OR denominator_rendered IN ('NaN', 'Infinity', '-Infinity')
               OR numerator <= 0
               OR denominator <= 0 THEN
                RAISE EXCEPTION 'adjustment division requires positive operands'
                    USING ERRCODE = '22023';
            END IF;

            numerator_rendered := trim_scale(numerator)::text;
            IF numerator >= 1 THEN
                numerator_exponent :=
                    length(split_part(numerator_rendered, '.', 1)) - 1;
            ELSE
                fractional := split_part(numerator_rendered, '.', 2);
                first_nonzero := 1;
                WHILE first_nonzero <= length(fractional)
                      AND substring(fractional FROM first_nonzero FOR 1) = '0' LOOP
                    first_nonzero := first_nonzero + 1;
                END LOOP;
                numerator_exponent := -first_nonzero;
            END IF;

            denominator_rendered := trim_scale(denominator)::text;
            IF denominator >= 1 THEN
                denominator_exponent :=
                    length(split_part(denominator_rendered, '.', 1)) - 1;
            ELSE
                fractional := split_part(denominator_rendered, '.', 2);
                first_nonzero := 1;
                WHILE first_nonzero <= length(fractional)
                      AND substring(fractional FROM first_nonzero FOR 1) = '0' LOOP
                    first_nonzero := first_nonzero + 1;
                END LOOP;
                denominator_exponent := -first_nonzero;
            END IF;

            quotient_exponent := numerator_exponent - denominator_exponent;
            IF quotient_exponent >= 0 THEN
                IF numerator < denominator * power(10::numeric, quotient_exponent) THEN
                    quotient_exponent := quotient_exponent - 1;
                END IF;
            ELSIF numerator * power(10::numeric, -quotient_exponent) < denominator THEN
                quotient_exponent := quotient_exponent - 1;
            END IF;

            decimal_places := 33 - quotient_exponent;
            IF decimal_places NOT BETWEEN -400 AND 700 THEN
                RAISE EXCEPTION 'adjustment decimal exponent is unsupported'
                    USING ERRCODE = '22003';
            END IF;
            IF decimal_places >= 0 THEN
                scaled_numerator :=
                    numerator * power(10::numeric, decimal_places);
                scaled_denominator := denominator;
            ELSE
                scaled_numerator := numerator;
                scaled_denominator :=
                    denominator * power(10::numeric, -decimal_places);
            END IF;

            coefficient := div(scaled_numerator, scaled_denominator);
            remainder := mod(scaled_numerator, scaled_denominator);
            IF remainder * 2 > scaled_denominator
               OR (
                   remainder * 2 = scaled_denominator
                   AND mod(coefficient, 2) = 1
               ) THEN
                coefficient := coefficient + 1;
            END IF;
            IF coefficient = power(10::numeric, 34) THEN
                coefficient := power(10::numeric, 33);
                quotient_exponent := quotient_exponent + 1;
                decimal_places := decimal_places - 1;
            END IF;

            coefficient_text := coefficient::text;
            IF decimal_places > 0 THEN
                IF length(coefficient_text) > decimal_places THEN
                    result_text :=
                        substring(
                            coefficient_text
                            FROM 1 FOR length(coefficient_text) - decimal_places
                        ) || '.' ||
                        substring(
                            coefficient_text
                            FROM length(coefficient_text) - decimal_places + 1
                        );
                ELSE
                    result_text := '0.' ||
                        repeat('0', decimal_places - length(coefficient_text)) ||
                        coefficient_text;
                END IF;
            ELSIF decimal_places < 0 THEN
                result_text := coefficient_text || repeat('0', -decimal_places);
            ELSE
                result_text := coefficient_text;
            END IF;
            RETURN result_text::numeric;
        END;
        $$
        """
    )
    op.execute(
        r"""
        CREATE FUNCTION adjustment_decimal_text(value numeric)
        RETURNS text
        LANGUAGE sql
        IMMUTABLE STRICT PARALLEL SAFE
        SET search_path = pg_catalog, public
        AS $$
            SELECT trim_scale(value)::text
        $$
        """
    )
    op.execute(
        r"""
        CREATE FUNCTION parse_adjustment_timestamp(value text)
        RETURNS timestamptz
        LANGUAGE plpgsql
        IMMUTABLE STRICT PARALLEL SAFE
        SET search_path = pg_catalog, public
        AS $$
        DECLARE parsed timestamptz;
        BEGIN
            IF value !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:'
                         '[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$' THEN
                RAISE EXCEPTION 'adjustment timestamp is not canonical'
                    USING ERRCODE = '22007';
            END IF;
            parsed := value::timestamptz;
            IF to_char(
                parsed AT TIME ZONE 'UTC',
                'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
            ) <> value THEN
                RAISE EXCEPTION 'adjustment timestamp does not round trip'
                    USING ERRCODE = '22007';
            END IF;
            RETURN parsed;
        END;
        $$
        """
    )
    op.execute(
        r"""
        CREATE FUNCTION parse_adjustment_date(value text)
        RETURNS date
        LANGUAGE plpgsql
        IMMUTABLE STRICT PARALLEL SAFE
        SET search_path = pg_catalog, public
        AS $$
        DECLARE parsed date;
        BEGIN
            IF value !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                RAISE EXCEPTION 'adjustment date is not canonical'
                    USING ERRCODE = '22007';
            END IF;
            parsed := value::date;
            IF to_char(parsed, 'YYYY-MM-DD') <> value THEN
                RAISE EXCEPTION 'adjustment date does not round trip'
                    USING ERRCODE = '22007';
            END IF;
            RETURN parsed;
        END;
        $$
        """
    )
    op.execute(
        r"""
        CREATE FUNCTION parse_adjustment_decimal(value text)
        RETURNS numeric
        LANGUAGE plpgsql
        IMMUTABLE STRICT PARALLEL SAFE
        SET search_path = pg_catalog, public
        AS $$
        DECLARE parsed numeric;
        BEGIN
            IF length(value) NOT BETWEEN 1 AND 400
               OR value !~ '^(0|[1-9][0-9]*)(\.[0-9]*[1-9])?$' THEN
                RAISE EXCEPTION 'adjustment decimal is not canonical'
                    USING ERRCODE = '22023';
            END IF;
            parsed := value::numeric;
            IF parsed <= 0
               OR public.adjustment_decimal_text(parsed) <> value THEN
                RAISE EXCEPTION 'adjustment decimal must be canonical and positive'
                    USING ERRCODE = '22023';
            END IF;
            RETURN parsed;
        END;
        $$
        """
    )
    op.execute(
        r"""
        CREATE FUNCTION adjustment_factor_series_fence_id(symbol text)
        RETURNS bigint
        LANGUAGE plpgsql
        IMMUTABLE STRICT PARALLEL SAFE
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            material bytea := convert_to(
                'stockapi.adjustment-factor-series-fence.v1', 'UTF8'
            );
            symbol_bytes bytea;
        BEGIN
            IF symbol = '' THEN
                RAISE EXCEPTION 'adjustment-factor fence symbol is empty'
                    USING ERRCODE = '22023';
            END IF;
            symbol_bytes := convert_to(symbol, 'UTF8');
            material := material || int4send(octet_length(symbol_bytes)) || symbol_bytes;
            RETURN ('x' || encode(
                substring(digest(material, 'sha256') FROM 1 FOR 8), 'hex'
            ))::bit(64)::bigint;
        END;
        $$
        """
    )
    op.execute(
        r"""
        CREATE FUNCTION stamp_adjustment_factor_set()
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
        "CREATE TRIGGER adjustment_factor_sets_stamp "
        "BEFORE INSERT ON adjustment_factor_sets FOR EACH ROW "
        "EXECUTE FUNCTION stamp_adjustment_factor_set()"
    )
    op.execute(
        r"""
        CREATE FUNCTION stamp_adjustment_factor_entry()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            header_creator bigint;
        BEGIN
            SELECT creator_xid INTO STRICT header_creator
            FROM public.adjustment_factor_sets
            WHERE factor_set_id = NEW.factor_set_id;
            IF header_creator <> txid_current() THEN
                RAISE EXCEPTION 'factor entries must be inserted with their header'
                    USING ERRCODE = '55000';
            END IF;
            IF EXISTS (
                SELECT 1 FROM public.adjustment_factor_set_availability
                WHERE factor_set_id = NEW.factor_set_id
            ) THEN
                RAISE EXCEPTION 'a receipted factor set is frozen'
                    USING ERRCODE = '55000';
            END IF;
            NEW.creator_xid := txid_current();
            RETURN NEW;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RAISE EXCEPTION 'factor-set header does not exist'
                    USING ERRCODE = '23503';
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER adjustment_factor_entries_stamp "
        "BEFORE INSERT ON adjustment_factor_entries FOR EACH ROW "
        "EXECUTE FUNCTION stamp_adjustment_factor_entry()"
    )
    op.execute(
        r"""
        CREATE FUNCTION reject_adjustment_factor_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        BEGIN
            RAISE EXCEPTION 'adjustment-factor evidence is append-only'
                USING ERRCODE = '55000';
        END;
        $$
        """
    )
    for table in (
        "adjustment_factor_sets",
        "adjustment_factor_entries",
        "adjustment_factor_set_availability",
    ):
        op.execute(
            f"CREATE TRIGGER {table}_no_row_mutation BEFORE UPDATE OR DELETE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION reject_adjustment_factor_mutation()"
        )
        op.execute(
            f"CREATE TRIGGER {table}_no_truncate BEFORE TRUNCATE ON {table} "
            "FOR EACH STATEMENT EXECUTE FUNCTION reject_adjustment_factor_mutation()"
        )


def _create_publishers() -> None:
    op.execute(
        f"""
        CREATE FUNCTION publish_adjustment_factor_set(payload_bytes bytea)
        RETURNS text
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            payload jsonb;
            factor_identity text;
            symbol_value text;
            cutoff_value timestamptz;
            anchor_value date;
            coverage_start_value date;
            coverage_end_value date;
            raw_count integer;
            factor_count integer;
            split_count integer;
            dividend_count integer;
            split_collection_identity text;
            dividend_collection_identity text;
            split_receipt record;
            dividend_receipt record;
            lock_key bigint;
            index_value integer;
            raw_row jsonb;
            factor_row jsonb;
            split_row jsonb;
            dividend_row jsonb;
            observation_value date;
            observed_value timestamptz;
            recorded_value timestamptz;
            available_value timestamptz;
            previous_observation date;
            previous_observed timestamptz;
            raw_close_text text;
            raw_close_value numeric;
            raw_close_bits text;
            price_factor_text text;
            volume_factor_text text;
            price_factor_bits text;
            volume_factor_bits text;
            max_evidence_available timestamptz;
            exact_version_count integer;
            latest_version_count integer;
            complete_raw_count integer;
            unmatched_raw_count integer;
            stored_close float8;
            latest_recorded timestamptz;
            latest_available timestamptz;
            action_version record;
            split_from_value numeric;
            split_to_value numeric;
            dividend_cash_value numeric;
            expected_price numeric := 1;
            expected_volume numeric := 1;
            expected_denominator numeric;
            inserted_count integer;
        BEGIN
            IF payload_bytes IS NULL
               OR octet_length(payload_bytes) NOT BETWEEN 1 AND 4194304 THEN
                RAISE EXCEPTION 'adjustment-factor payload exceeds its bound'
                    USING ERRCODE = '22023';
            END IF;
            BEGIN
                payload := convert_from(payload_bytes, 'UTF8')::jsonb;
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'adjustment-factor payload is not UTF-8 JSON'
                    USING ERRCODE = '22023';
            END;
            IF jsonb_typeof(payload) <> 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(payload)) <> 9
               OR NOT (payload ?& ARRAY[
                   'actions', 'anchor_date', 'cutoff', 'factors', 'format',
                   'policy_hash', 'policy_version', 'raw_inputs', 'symbol'
               ])
               OR convert_to(
                   public.canonical_adjustment_factor_json(payload), 'UTF8'
               ) <> payload_bytes THEN
                RAISE EXCEPTION 'adjustment-factor root is not exact canonical JSON'
                    USING ERRCODE = '22023';
            END IF;
            IF jsonb_typeof(payload->'format') <> 'string'
               OR jsonb_typeof(payload->'policy_hash') <> 'string'
               OR jsonb_typeof(payload->'policy_version') <> 'string'
               OR jsonb_typeof(payload->'symbol') <> 'string'
               OR jsonb_typeof(payload->'cutoff') <> 'string'
               OR jsonb_typeof(payload->'anchor_date') <> 'string'
               OR jsonb_typeof(payload->'actions') <> 'object'
               OR jsonb_typeof(payload->'raw_inputs') <> 'array'
               OR jsonb_typeof(payload->'factors') <> 'array'
               OR payload->>'format' <> '{_FACTOR_FORMAT}'
               OR payload->>'policy_version' <> '{_POLICY_VERSION}'
               OR payload->>'policy_hash' <> '{_POLICY_HASH}' THEN
                RAISE EXCEPTION 'adjustment-factor policy or root types are unsupported'
                    USING ERRCODE = '22023';
            END IF;

            symbol_value := payload->>'symbol';
            IF symbol_value IS NULL
               OR symbol_value <> upper(symbol_value)
               OR symbol_value !~ '^[A-Z0-9.\\-_:]+$'
               OR length(symbol_value) NOT BETWEEN 1 AND 32 THEN
                RAISE EXCEPTION 'adjustment-factor symbol is not canonical'
                    USING ERRCODE = '22023';
            END IF;
            cutoff_value := public.parse_adjustment_timestamp(payload->>'cutoff');
            anchor_value := public.parse_adjustment_date(payload->>'anchor_date');
            raw_count := jsonb_array_length(payload->'raw_inputs');
            factor_count := jsonb_array_length(payload->'factors');
            IF raw_count NOT BETWEEN 1 AND 5000 OR factor_count <> raw_count THEN
                RAISE EXCEPTION 'adjustment-factor input/factor cardinality is invalid'
                    USING ERRCODE = '22023';
            END IF;
            coverage_start_value := public.parse_adjustment_date(
                payload->'raw_inputs'->0->>'observation_date'
            );
            coverage_end_value := public.parse_adjustment_date(
                payload->'raw_inputs'->(raw_count - 1)->>'observation_date'
            );
            IF anchor_value <> coverage_end_value THEN
                RAISE EXCEPTION 'adjustment-factor anchor does not equal coverage end'
                    USING ERRCODE = '22023';
            END IF;

            IF (SELECT count(*) FROM jsonb_object_keys(payload->'actions')) <> 2
               OR NOT (payload->'actions' ?& ARRAY['dividends', 'splits'])
               OR jsonb_typeof(payload->'actions'->'splits') <> 'object'
               OR jsonb_typeof(payload->'actions'->'dividends') <> 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(
                   payload->'actions'->'splits'
               )) <> 2
               OR (SELECT count(*) FROM jsonb_object_keys(
                   payload->'actions'->'dividends'
               )) <> 2
               OR NOT (payload->'actions'->'splits' ?& ARRAY['collection_id', 'versions'])
               OR NOT (
                   payload->'actions'->'dividends' ?& ARRAY['collection_id', 'versions']
               )
               OR jsonb_typeof(
                   payload->'actions'->'splits'->'collection_id'
               ) <> 'string'
               OR jsonb_typeof(
                   payload->'actions'->'dividends'->'collection_id'
               ) <> 'string'
               OR jsonb_typeof(payload->'actions'->'splits'->'versions') <> 'array'
               OR jsonb_typeof(payload->'actions'->'dividends'->'versions') <> 'array' THEN
                RAISE EXCEPTION 'adjustment-factor action collections are malformed'
                    USING ERRCODE = '22023';
            END IF;
            split_collection_identity :=
                payload->'actions'->'splits'->>'collection_id';
            dividend_collection_identity :=
                payload->'actions'->'dividends'->>'collection_id';
            IF split_collection_identity !~ '^sha256:[0-9a-f]{{64}}$'
               OR dividend_collection_identity !~ '^sha256:[0-9a-f]{{64}}$'
               OR split_collection_identity = dividend_collection_identity THEN
                RAISE EXCEPTION 'adjustment-factor collection identities are invalid'
                    USING ERRCODE = '22023';
            END IF;
            split_count := jsonb_array_length(
                payload->'actions'->'splits'->'versions'
            );
            dividend_count := jsonb_array_length(
                payload->'actions'->'dividends'->'versions'
            );
            IF split_count > 5000 OR dividend_count > 5000 THEN
                RAISE EXCEPTION 'adjustment-factor action cardinality is invalid'
                    USING ERRCODE = '22023';
            END IF;

            PERFORM set_config('lock_timeout', '30s', true);
            FOR lock_key IN
                SELECT DISTINCT value
                FROM unnest(ARRAY[
                    public.forecast_bar_series_fence_id(
                        symbol_value, 'polygon_open_close', 'day'
                    ),
                    public.corporate_action_series_fence_id(
                        'polygon', symbol_value, 'split'
                    ),
                    public.corporate_action_series_fence_id(
                        'polygon', symbol_value, 'dividend'
                    )
                ]) AS locks(value)
                ORDER BY value
            LOOP
                PERFORM pg_advisory_xact_lock(lock_key);
            END LOOP;
            IF cutoff_value > clock_timestamp() THEN
                RAISE EXCEPTION 'adjustment-factor cutoff is in the future'
                    USING ERRCODE = '22023';
            END IF;

            SELECT candidate.* INTO split_receipt
            FROM (
                SELECT collection.collection_id,
                       collection.recorded_at AS collection_recorded_at,
                       receipt.available_at,
                       collection.event_count,
                       row_number() OVER (
                           ORDER BY collection.recorded_at DESC,
                                    collection.collection_id DESC
                       ) AS candidate_rank
                FROM public.corporate_action_collections AS collection
                JOIN public.corporate_action_collection_availability AS receipt
                  ON receipt.collection_id = collection.collection_id
                 AND receipt.collection_recorded_at = collection.recorded_at
                WHERE collection.source = 'polygon'
                  AND collection.symbol = symbol_value
                  AND collection.action_type = 'split'
                  AND collection.endpoint = '/stocks/v1/splits'
                  AND collection.query_policy_hash = '{_ACTION_QUERY_POLICY_HASH}'
                  AND collection.coverage_start = coverage_start_value
                  AND collection.coverage_end = coverage_end_value
                  AND receipt.available_at <= cutoff_value
            ) AS candidate
            WHERE candidate.candidate_rank = 1;
            IF NOT FOUND OR split_receipt.collection_id <> split_collection_identity THEN
                RAISE EXCEPTION 'split collection is not newest for the exact cutoff scope'
                    USING ERRCODE = '55000';
            END IF;

            SELECT candidate.* INTO dividend_receipt
            FROM (
                SELECT collection.collection_id,
                       collection.recorded_at AS collection_recorded_at,
                       receipt.available_at,
                       collection.event_count,
                       row_number() OVER (
                           ORDER BY collection.recorded_at DESC,
                                    collection.collection_id DESC
                       ) AS candidate_rank
                FROM public.corporate_action_collections AS collection
                JOIN public.corporate_action_collection_availability AS receipt
                  ON receipt.collection_id = collection.collection_id
                 AND receipt.collection_recorded_at = collection.recorded_at
                WHERE collection.source = 'polygon'
                  AND collection.symbol = symbol_value
                  AND collection.action_type = 'dividend'
                  AND collection.endpoint = '/stocks/v1/dividends'
                  AND collection.query_policy_hash = '{_ACTION_QUERY_POLICY_HASH}'
                  AND collection.coverage_start = coverage_start_value
                  AND collection.coverage_end = coverage_end_value
                  AND receipt.available_at <= cutoff_value
            ) AS candidate
            WHERE candidate.candidate_rank = 1;
            IF NOT FOUND OR dividend_receipt.collection_id <> dividend_collection_identity THEN
                RAISE EXCEPTION 'dividend collection is not newest for the exact cutoff scope'
                    USING ERRCODE = '55000';
            END IF;
            IF split_count <> split_receipt.event_count
               OR dividend_count <> dividend_receipt.event_count THEN
                RAISE EXCEPTION 'factor action lists are not the complete collections'
                    USING ERRCODE = '55000';
            END IF;

            max_evidence_available := GREATEST(
                split_receipt.available_at,
                dividend_receipt.available_at
            );

            FOR index_value IN 0..(raw_count - 1) LOOP
                raw_row := payload->'raw_inputs'->index_value;
                factor_row := payload->'factors'->index_value;
                IF jsonb_typeof(raw_row) <> 'object'
                   OR (SELECT count(*) FROM jsonb_object_keys(raw_row)) <> 10
                   OR NOT (raw_row ?& ARRAY[
                       'adjustment_basis', 'available_at', 'close_decimal',
                       'close_f64_be', 'multiplier', 'observation_date',
                       'observed_at', 'source', 'timespan', 'version_recorded_at'
                   ])
                   OR jsonb_typeof(factor_row) <> 'object'
                   OR (SELECT count(*) FROM jsonb_object_keys(factor_row)) <> 5
                   OR NOT (factor_row ?& ARRAY[
                       'price_factor_decimal', 'price_factor_f64_be',
                       'raw_input_ordinal', 'volume_factor_decimal',
                       'volume_factor_f64_be'
                   ]) THEN
                    RAISE EXCEPTION 'factor raw/factor row shape is not exact'
                        USING ERRCODE = '22023';
                END IF;
                IF jsonb_typeof(raw_row->'adjustment_basis') <> 'string'
                   OR jsonb_typeof(raw_row->'available_at') <> 'string'
                   OR jsonb_typeof(raw_row->'close_decimal') <> 'string'
                   OR jsonb_typeof(raw_row->'close_f64_be') <> 'string'
                   OR jsonb_typeof(raw_row->'multiplier') <> 'number'
                   OR jsonb_typeof(raw_row->'observation_date') <> 'string'
                   OR jsonb_typeof(raw_row->'observed_at') <> 'string'
                   OR jsonb_typeof(raw_row->'source') <> 'string'
                   OR jsonb_typeof(raw_row->'timespan') <> 'string'
                   OR jsonb_typeof(raw_row->'version_recorded_at') <> 'string'
                   OR jsonb_typeof(factor_row->'price_factor_decimal') <> 'string'
                   OR jsonb_typeof(factor_row->'price_factor_f64_be') <> 'string'
                   OR jsonb_typeof(factor_row->'raw_input_ordinal') <> 'number'
                   OR jsonb_typeof(factor_row->'volume_factor_decimal') <> 'string'
                   OR jsonb_typeof(factor_row->'volume_factor_f64_be') <> 'string' THEN
                    RAISE EXCEPTION 'factor raw/factor scalar types are invalid'
                        USING ERRCODE = '22023';
                END IF;
                IF raw_row->>'timespan' <> 'day'
                   OR raw_row->>'multiplier' <> '1'
                   OR raw_row->>'source' <> 'polygon_open_close'
                   OR raw_row->>'adjustment_basis' <> 'raw'
                   OR factor_row->>'raw_input_ordinal' <> index_value::text THEN
                    RAISE EXCEPTION 'factor raw source or ordinal is unsupported'
                        USING ERRCODE = '22023';
                END IF;

                observation_value := public.parse_adjustment_date(
                    raw_row->>'observation_date'
                );
                observed_value := public.parse_adjustment_timestamp(
                    raw_row->>'observed_at'
                );
                recorded_value := public.parse_adjustment_timestamp(
                    raw_row->>'version_recorded_at'
                );
                available_value := public.parse_adjustment_timestamp(
                    raw_row->>'available_at'
                );
                IF (observed_value AT TIME ZONE 'UTC')::date <> observation_value
                   OR NOT (
                       observed_value <= recorded_value
                       AND recorded_value <= available_value
                       AND available_value <= cutoff_value
                   ) THEN
                    RAISE EXCEPTION 'factor raw receipt time order is invalid'
                        USING ERRCODE = '22023';
                END IF;
                IF index_value > 0 AND (
                    observation_value <= previous_observation
                    OR observed_value <= previous_observed
                ) THEN
                    RAISE EXCEPTION 'factor raw receipts are not strictly chronological'
                        USING ERRCODE = '22023';
                END IF;
                previous_observation := observation_value;
                previous_observed := observed_value;

                raw_close_text := raw_row->>'close_decimal';
                raw_close_value := public.parse_adjustment_decimal(raw_close_text);
                raw_close_bits := raw_row->>'close_f64_be';
                price_factor_text := factor_row->>'price_factor_decimal';
                volume_factor_text := factor_row->>'volume_factor_decimal';
                price_factor_bits := factor_row->>'price_factor_f64_be';
                volume_factor_bits := factor_row->>'volume_factor_f64_be';
                PERFORM public.parse_adjustment_decimal(price_factor_text);
                PERFORM public.parse_adjustment_decimal(volume_factor_text);
                IF raw_close_bits !~ '^[0-9a-f]{{16}}$'
                   OR price_factor_bits !~ '^[0-9a-f]{{16}}$'
                   OR volume_factor_bits !~ '^[0-9a-f]{{16}}$'
                   OR float8send(raw_close_value::float8) <> decode(raw_close_bits, 'hex')
                   OR float8send(price_factor_text::numeric::float8)
                      <> decode(price_factor_bits, 'hex')
                   OR float8send(volume_factor_text::numeric::float8)
                      <> decode(volume_factor_bits, 'hex') THEN
                    RAISE EXCEPTION 'factor decimal/binary64 publication disagrees'
                        USING ERRCODE = '22023';
                END IF;

                SELECT count(*), min(version.close)
                INTO exact_version_count, stored_close
                FROM (
                    SELECT bar.symbol, bar.timespan, bar.multiplier,
                           bar.ts AS observed_at, bar.source,
                           bar.adjustment_basis, bar.recorded_at, bar.close
                    FROM public.bars AS bar
                    UNION
                    SELECT revision.symbol, revision.timespan, revision.multiplier,
                           revision.ts, revision.source, revision.adjustment_basis,
                           revision.previous_recorded_at, revision.previous_close
                    FROM public.bars_revisions AS revision
                    WHERE revision.previous_recorded_at IS NOT NULL
                    UNION
                    SELECT revision.symbol, revision.timespan, revision.multiplier,
                           revision.ts, revision.source, revision.adjustment_basis,
                           revision.incoming_recorded_at, revision.incoming_close
                    FROM public.bars_revisions AS revision
                    WHERE revision.incoming_recorded_at IS NOT NULL
                ) AS version
                JOIN public.bar_version_availability AS receipt
                  ON receipt.symbol = version.symbol
                 AND receipt.timespan = version.timespan
                 AND receipt.multiplier = version.multiplier
                 AND receipt.ts = version.observed_at
                 AND receipt.source = version.source
                 AND receipt.adjustment_basis = version.adjustment_basis
                 AND receipt.version_recorded_at = version.recorded_at
                WHERE version.symbol = symbol_value
                  AND version.timespan = 'day'
                  AND version.multiplier = 1
                  AND version.observed_at = observed_value
                  AND version.source = 'polygon_open_close'
                  AND version.adjustment_basis = 'raw'
                  AND version.recorded_at = recorded_value
                  AND receipt.available_at = available_value;
                IF exact_version_count <> 1
                   OR float8send(stored_close) <> decode(raw_close_bits, 'hex')
                   OR public.adjustment_decimal_text((stored_close::text)::numeric)
                      <> raw_close_text THEN
                    RAISE EXCEPTION 'factor raw row does not match its exact stored bar receipt'
                        USING ERRCODE = '55000';
                END IF;

                SELECT candidate.version_recorded_at,
                       candidate.available_at,
                       candidate.greatest_count
                INTO latest_recorded, latest_available, latest_version_count
                FROM (
                    SELECT receipt.version_recorded_at,
                           receipt.available_at,
                           count(*) OVER (
                               PARTITION BY receipt.version_recorded_at
                           ) AS greatest_count,
                           row_number() OVER (
                               ORDER BY receipt.version_recorded_at DESC,
                                        receipt.available_at DESC
                           ) AS candidate_rank
                    FROM public.bar_version_availability AS receipt
                    WHERE receipt.symbol = symbol_value
                      AND receipt.timespan = 'day'
                      AND receipt.multiplier = 1
                      AND receipt.ts = observed_value
                      AND receipt.source = 'polygon_open_close'
                      AND receipt.adjustment_basis = 'raw'
                      AND receipt.available_at <= cutoff_value
                ) AS candidate
                WHERE candidate.candidate_rank = 1;
                IF NOT FOUND
                   OR latest_version_count <> 1
                   OR latest_recorded <> recorded_value
                   OR latest_available <> available_value THEN
                    RAISE EXCEPTION 'factor raw row is not the unique newest cutoff version'
                        USING ERRCODE = '55000';
                END IF;
                max_evidence_available := GREATEST(
                    max_evidence_available,
                    available_value
                );
            END LOOP;

            WITH ranked_visible AS (
                SELECT receipt.ts,
                       receipt.version_recorded_at,
                       receipt.available_at,
                       count(*) OVER (
                           PARTITION BY receipt.ts, receipt.version_recorded_at
                       ) AS greatest_count,
                       row_number() OVER (
                           PARTITION BY receipt.ts
                           ORDER BY receipt.version_recorded_at DESC,
                                    receipt.available_at DESC
                       ) AS candidate_rank
                FROM public.bar_version_availability AS receipt
                WHERE receipt.symbol = symbol_value
                  AND receipt.timespan = 'day'
                  AND receipt.multiplier = 1
                  AND receipt.source = 'polygon_open_close'
                  AND receipt.adjustment_basis = 'raw'
                  AND (receipt.ts AT TIME ZONE 'UTC')::date
                      BETWEEN coverage_start_value AND coverage_end_value
                  AND receipt.available_at <= cutoff_value
            ), newest_visible AS (
                SELECT * FROM ranked_visible WHERE candidate_rank = 1
            )
            SELECT count(*), count(*) FILTER (
                WHERE newest.greatest_count <> 1
                   OR NOT EXISTS (
                       SELECT 1
                       FROM jsonb_array_elements(payload->'raw_inputs') AS raw(value)
                       WHERE public.parse_adjustment_timestamp(
                                 raw.value->>'observed_at'
                             ) = newest.ts
                         AND public.parse_adjustment_timestamp(
                                 raw.value->>'version_recorded_at'
                             ) = newest.version_recorded_at
                         AND public.parse_adjustment_timestamp(
                                 raw.value->>'available_at'
                             ) = newest.available_at
                   )
            )
            INTO complete_raw_count, unmatched_raw_count
            FROM newest_visible AS newest;
            IF complete_raw_count <> raw_count OR unmatched_raw_count <> 0 THEN
                RAISE EXCEPTION 'factor raw inputs omit newest cutoff-visible stored receipts'
                    USING ERRCODE = '55000';
            END IF;

            IF split_count > 0 THEN
                FOR index_value IN 0..(split_count - 1) LOOP
                    split_row := payload->'actions'->'splits'->'versions'->index_value;
                    IF jsonb_typeof(split_row) <> 'object'
                       OR (SELECT count(*) FROM jsonb_object_keys(split_row)) <> 6
                       OR NOT (split_row ?& ARRAY[
                           'adjustment_type', 'effective_date', 'provider_event_id',
                           'split_from', 'split_to', 'version_id'
                       ])
                       OR jsonb_typeof(split_row->'adjustment_type') <> 'string'
                       OR jsonb_typeof(split_row->'effective_date') <> 'string'
                       OR jsonb_typeof(split_row->'provider_event_id') <> 'string'
                       OR jsonb_typeof(split_row->'split_from') <> 'string'
                       OR jsonb_typeof(split_row->'split_to') <> 'string'
                       OR jsonb_typeof(split_row->'version_id') <> 'string' THEN
                        RAISE EXCEPTION 'factor split row shape is not exact'
                            USING ERRCODE = '22023';
                    END IF;
                    SELECT version.* INTO action_version
                    FROM public.corporate_action_collection_members AS member
                    JOIN public.corporate_action_versions AS version
                      ON version.action_version_id = member.action_version_id
                    WHERE member.collection_id = split_collection_identity
                      AND member.ordinal = index_value
                      AND member.action_version_id = split_row->>'version_id';
                    IF NOT FOUND
                       OR action_version.action_type IS DISTINCT FROM 'split'
                       OR action_version.source IS DISTINCT FROM 'polygon'
                       OR action_version.symbol IS DISTINCT FROM symbol_value
                       OR action_version.provider_event_id
                          IS DISTINCT FROM split_row->>'provider_event_id'
                       OR action_version.adjustment_type
                          IS DISTINCT FROM split_row->>'adjustment_type'
                       OR split_row->>'adjustment_type'
                          NOT IN ('forward_split', 'reverse_split')
                       OR action_version.effective_date
                          IS DISTINCT FROM public.parse_adjustment_date(
                              split_row->>'effective_date'
                          ) THEN
                        RAISE EXCEPTION 'factor split row does not match collection membership'
                            USING ERRCODE = '55000';
                    END IF;
                    split_from_value := public.parse_adjustment_decimal(
                        split_row->>'split_from'
                    );
                    split_to_value := public.parse_adjustment_decimal(
                        split_row->>'split_to'
                    );
                    IF split_from_value IS DISTINCT FROM
                          public.adjustment_decimal34(action_version.split_from)
                       OR split_to_value IS DISTINCT FROM
                          public.adjustment_decimal34(action_version.split_to)
                       OR NOT EXISTS (
                           SELECT 1 FROM jsonb_array_elements(
                               payload->'raw_inputs'
                           ) AS raw(value)
                           WHERE raw.value->>'observation_date'
                                 = split_row->>'effective_date'
                       ) THEN
                        RAISE EXCEPTION 'factor split values or session are invalid'
                            USING ERRCODE = '55000';
                    END IF;
                END LOOP;
            END IF;

            IF dividend_count > 0 THEN
                FOR index_value IN 0..(dividend_count - 1) LOOP
                    dividend_row :=
                        payload->'actions'->'dividends'->'versions'->index_value;
                    IF jsonb_typeof(dividend_row) <> 'object'
                       OR (SELECT count(*) FROM jsonb_object_keys(dividend_row)) <> 6
                       OR NOT (dividend_row ?& ARRAY[
                           'cash_amount', 'currency', 'distribution_type',
                           'ex_dividend_date', 'provider_event_id', 'version_id'
                       ])
                       OR jsonb_typeof(dividend_row->'cash_amount') <> 'string'
                       OR jsonb_typeof(dividend_row->'currency') <> 'string'
                       OR jsonb_typeof(dividend_row->'distribution_type') <> 'string'
                       OR jsonb_typeof(dividend_row->'ex_dividend_date') <> 'string'
                       OR jsonb_typeof(dividend_row->'provider_event_id') <> 'string'
                       OR jsonb_typeof(dividend_row->'version_id') <> 'string' THEN
                        RAISE EXCEPTION 'factor dividend row shape is not exact'
                            USING ERRCODE = '22023';
                    END IF;
                    SELECT version.* INTO action_version
                    FROM public.corporate_action_collection_members AS member
                    JOIN public.corporate_action_versions AS version
                      ON version.action_version_id = member.action_version_id
                    WHERE member.collection_id = dividend_collection_identity
                      AND member.ordinal = index_value
                      AND member.action_version_id = dividend_row->>'version_id';
                    IF NOT FOUND
                       OR action_version.action_type IS DISTINCT FROM 'dividend'
                       OR action_version.source IS DISTINCT FROM 'polygon'
                       OR action_version.symbol IS DISTINCT FROM symbol_value
                       OR action_version.provider_event_id
                          IS DISTINCT FROM dividend_row->>'provider_event_id'
                       OR action_version.currency IS DISTINCT FROM 'USD'
                       OR dividend_row->>'currency' <> 'USD'
                       OR action_version.distribution_type IS DISTINCT FROM 'recurring'
                       OR dividend_row->>'distribution_type' <> 'recurring'
                       OR action_version.effective_date
                          IS DISTINCT FROM public.parse_adjustment_date(
                              dividend_row->>'ex_dividend_date'
                          ) THEN
                        RAISE EXCEPTION 'factor dividend row does not match collection membership'
                            USING ERRCODE = '55000';
                    END IF;
                    dividend_cash_value := public.parse_adjustment_decimal(
                        dividend_row->>'cash_amount'
                    );
                    IF dividend_cash_value IS DISTINCT FROM
                          public.adjustment_decimal34(action_version.cash_amount)
                       OR NOT EXISTS (
                           SELECT 1 FROM jsonb_array_elements(
                               payload->'raw_inputs'
                           ) AS raw(value)
                           WHERE raw.value->>'observation_date'
                                 = dividend_row->>'ex_dividend_date'
                       ) THEN
                        RAISE EXCEPTION 'factor dividend values or session are invalid'
                            USING ERRCODE = '55000';
                    END IF;
                END LOOP;
            END IF;

            IF EXISTS (
                SELECT action_date
                FROM (
                    SELECT value->>'effective_date' AS action_date
                    FROM jsonb_array_elements(
                        payload->'actions'->'splits'->'versions'
                    ) AS split(value)
                    UNION ALL
                    SELECT value->>'ex_dividend_date'
                    FROM jsonb_array_elements(
                        payload->'actions'->'dividends'->'versions'
                    ) AS dividend(value)
                ) AS actions
                GROUP BY action_date
                HAVING count(*) > 1
            ) THEN
                RAISE EXCEPTION 'factor policy permits at most one action per session'
                    USING ERRCODE = '22023';
            END IF;
            IF EXISTS (
                SELECT provider_event_id
                FROM (
                    SELECT value->>'provider_event_id' AS provider_event_id
                    FROM jsonb_array_elements(
                        payload->'actions'->'splits'->'versions'
                    ) AS split(value)
                    UNION ALL
                    SELECT value->>'provider_event_id'
                    FROM jsonb_array_elements(
                        payload->'actions'->'dividends'->'versions'
                    ) AS dividend(value)
                ) AS actions
                GROUP BY provider_event_id
                HAVING count(*) > 1
            ) OR EXISTS (
                SELECT version_id
                FROM (
                    SELECT value->>'version_id' AS version_id
                    FROM jsonb_array_elements(
                        payload->'actions'->'splits'->'versions'
                    ) AS split(value)
                    UNION ALL
                    SELECT value->>'version_id'
                    FROM jsonb_array_elements(
                        payload->'actions'->'dividends'->'versions'
                    ) AS dividend(value)
                ) AS actions
                GROUP BY version_id
                HAVING count(*) > 1
            ) THEN
                RAISE EXCEPTION 'factor policy requires globally unique action identities'
                    USING ERRCODE = '22023';
            END IF;

            expected_price := 1;
            expected_volume := 1;
            FOR index_value IN REVERSE (raw_count - 1)..0 LOOP
                raw_row := payload->'raw_inputs'->index_value;
                factor_row := payload->'factors'->index_value;
                raw_close_value := public.parse_adjustment_decimal(
                    raw_row->>'close_decimal'
                );
                price_factor_text := factor_row->>'price_factor_decimal';
                volume_factor_text := factor_row->>'volume_factor_decimal';
                IF price_factor_text
                      <> public.adjustment_decimal_text(expected_price)
                   OR volume_factor_text
                      <> public.adjustment_decimal_text(expected_volume)
                   OR float8send(expected_price::float8)
                      <> decode(factor_row->>'price_factor_f64_be', 'hex')
                   OR float8send(expected_volume::float8)
                      <> decode(factor_row->>'volume_factor_f64_be', 'hex') THEN
                    RAISE EXCEPTION
                        'factor values do not satisfy the pinned kernel at index %: '
                        'expected price %/% but received %/%; '
                        'expected volume %/% but received %/%',
                        index_value,
                        public.adjustment_decimal_text(expected_price),
                        encode(float8send(expected_price::float8), 'hex'),
                        price_factor_text,
                        factor_row->>'price_factor_f64_be',
                        public.adjustment_decimal_text(expected_volume),
                        encode(float8send(expected_volume::float8), 'hex'),
                        volume_factor_text,
                        factor_row->>'volume_factor_f64_be'
                        USING ERRCODE = '55000';
                END IF;

                SELECT value INTO split_row
                FROM jsonb_array_elements(
                    payload->'actions'->'splits'->'versions'
                ) AS split(value)
                WHERE value->>'effective_date' = raw_row->>'observation_date';
                SELECT value INTO dividend_row
                FROM jsonb_array_elements(
                    payload->'actions'->'dividends'->'versions'
                ) AS dividend(value)
                WHERE value->>'ex_dividend_date' = raw_row->>'observation_date';
                IF split_row IS NOT NULL THEN
                    split_from_value := public.parse_adjustment_decimal(
                        split_row->>'split_from'
                    );
                    split_to_value := public.parse_adjustment_decimal(
                        split_row->>'split_to'
                    );
                    expected_price := public.adjustment_divide34(
                        public.adjustment_decimal34(
                            expected_price * split_from_value
                        ),
                        split_to_value
                    );
                    expected_volume := public.adjustment_divide34(
                        public.adjustment_decimal34(
                            expected_volume * split_to_value
                        ),
                        split_from_value
                    );
                ELSIF dividend_row IS NOT NULL THEN
                    dividend_cash_value := public.parse_adjustment_decimal(
                        dividend_row->>'cash_amount'
                    );
                    expected_denominator := public.adjustment_decimal34(
                        raw_close_value + dividend_cash_value
                    );
                    expected_price := public.adjustment_divide34(
                        public.adjustment_decimal34(
                            expected_price * raw_close_value
                        ),
                        expected_denominator
                    );
                END IF;
                split_row := NULL;
                dividend_row := NULL;
            END LOOP;

            factor_identity := 'sha256:' || encode(
                digest(payload_bytes, 'sha256'), 'hex'
            );
            INSERT INTO public.adjustment_factor_sets (
                factor_set_id, format, policy_version, policy_hash, symbol,
                cutoff, anchor_date, coverage_start, coverage_end, input_count,
                max_input_available_at,
                split_collection_id, split_collection_recorded_at,
                split_collection_available_at,
                dividend_collection_id, dividend_collection_recorded_at,
                dividend_collection_available_at,
                canonical_payload, creator_xid
            ) VALUES (
                factor_identity, '{_FACTOR_FORMAT}', '{_POLICY_VERSION}',
                '{_POLICY_HASH}', symbol_value, cutoff_value, anchor_value,
                coverage_start_value, coverage_end_value, raw_count,
                max_evidence_available,
                split_collection_identity, split_receipt.collection_recorded_at,
                split_receipt.available_at,
                dividend_collection_identity, dividend_receipt.collection_recorded_at,
                dividend_receipt.available_at,
                payload_bytes, 1
            ) ON CONFLICT (factor_set_id) DO NOTHING;
            GET DIAGNOSTICS inserted_count = ROW_COUNT;

            IF NOT EXISTS (
                SELECT 1 FROM public.adjustment_factor_sets AS header
                WHERE header.factor_set_id = factor_identity
                  AND header.format = '{_FACTOR_FORMAT}'
                  AND header.policy_version = '{_POLICY_VERSION}'
                  AND header.policy_hash = '{_POLICY_HASH}'
                  AND header.symbol = symbol_value
                  AND header.cutoff = cutoff_value
                  AND header.anchor_date = anchor_value
                  AND header.coverage_start = coverage_start_value
                  AND header.coverage_end = coverage_end_value
                  AND header.input_count = raw_count
                  AND header.max_input_available_at = max_evidence_available
                  AND header.split_collection_id = split_collection_identity
                  AND header.split_collection_recorded_at
                      = split_receipt.collection_recorded_at
                  AND header.split_collection_available_at = split_receipt.available_at
                  AND header.dividend_collection_id = dividend_collection_identity
                  AND header.dividend_collection_recorded_at
                      = dividend_receipt.collection_recorded_at
                  AND header.dividend_collection_available_at
                      = dividend_receipt.available_at
                  AND header.canonical_payload = payload_bytes
            ) THEN
                RAISE EXCEPTION 'factor-set content identity collision'
                    USING ERRCODE = '55000';
            END IF;

            IF inserted_count = 1 THEN
                FOR index_value IN 0..(raw_count - 1) LOOP
                    raw_row := payload->'raw_inputs'->index_value;
                    factor_row := payload->'factors'->index_value;
                    INSERT INTO public.adjustment_factor_entries (
                        factor_set_id, ordinal, symbol, observation_date,
                        observed_at, timespan, multiplier, source,
                        adjustment_basis, version_recorded_at, raw_available_at,
                        raw_close_decimal, raw_close_f64_be,
                        price_factor_decimal, price_factor_f64_be,
                        volume_factor_decimal, volume_factor_f64_be, creator_xid
                    ) VALUES (
                        factor_identity, index_value, symbol_value,
                        public.parse_adjustment_date(raw_row->>'observation_date'),
                        public.parse_adjustment_timestamp(raw_row->>'observed_at'),
                        'day', 1, 'polygon_open_close', 'raw',
                        public.parse_adjustment_timestamp(
                            raw_row->>'version_recorded_at'
                        ),
                        public.parse_adjustment_timestamp(raw_row->>'available_at'),
                        raw_row->>'close_decimal',
                        decode(raw_row->>'close_f64_be', 'hex'),
                        factor_row->>'price_factor_decimal',
                        decode(factor_row->>'price_factor_f64_be', 'hex'),
                        factor_row->>'volume_factor_decimal',
                        decode(factor_row->>'volume_factor_f64_be', 'hex'), 1
                    );
                END LOOP;
            END IF;

            IF (
                SELECT count(*) FROM public.adjustment_factor_entries
                WHERE factor_set_id = factor_identity
            ) <> raw_count THEN
                RAISE EXCEPTION 'factor-set entry count conflicts with canonical bytes'
                    USING ERRCODE = '55000';
            END IF;
            FOR index_value IN 0..(raw_count - 1) LOOP
                raw_row := payload->'raw_inputs'->index_value;
                factor_row := payload->'factors'->index_value;
                IF NOT EXISTS (
                    SELECT 1 FROM public.adjustment_factor_entries AS entry
                    WHERE entry.factor_set_id = factor_identity
                      AND entry.ordinal = index_value
                      AND entry.symbol = symbol_value
                      AND entry.observation_date = public.parse_adjustment_date(
                          raw_row->>'observation_date'
                      )
                      AND entry.observed_at = public.parse_adjustment_timestamp(
                          raw_row->>'observed_at'
                      )
                      AND entry.timespan = 'day'
                      AND entry.multiplier = 1
                      AND entry.source = 'polygon_open_close'
                      AND entry.adjustment_basis = 'raw'
                      AND entry.version_recorded_at = public.parse_adjustment_timestamp(
                          raw_row->>'version_recorded_at'
                      )
                      AND entry.raw_available_at = public.parse_adjustment_timestamp(
                          raw_row->>'available_at'
                      )
                      AND entry.raw_close_decimal = raw_row->>'close_decimal'
                      AND entry.raw_close_f64_be = decode(
                          raw_row->>'close_f64_be', 'hex'
                      )
                      AND entry.price_factor_decimal
                          = factor_row->>'price_factor_decimal'
                      AND entry.price_factor_f64_be = decode(
                          factor_row->>'price_factor_f64_be', 'hex'
                      )
                      AND entry.volume_factor_decimal
                          = factor_row->>'volume_factor_decimal'
                      AND entry.volume_factor_f64_be = decode(
                          factor_row->>'volume_factor_f64_be', 'hex'
                      )
                ) THEN
                    RAISE EXCEPTION 'factor-set entry projection conflicts with canonical bytes'
                        USING ERRCODE = '55000';
                END IF;
            END LOOP;
            RETURN factor_identity;
        EXCEPTION
            WHEN invalid_text_representation OR datetime_field_overflow
                 OR numeric_value_out_of_range OR division_by_zero THEN
                RAISE EXCEPTION 'adjustment-factor payload contains an invalid scalar'
                    USING ERRCODE = '22023';
        END;
        $$
        """
    )

    op.execute(
        r"""
        CREATE FUNCTION stamp_adjustment_factor_set_availability()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE
            header public.adjustment_factor_sets%ROWTYPE;
            entry_count integer;
            invalid_count integer;
        BEGIN
            SELECT * INTO STRICT header
            FROM public.adjustment_factor_sets
            WHERE factor_set_id = NEW.factor_set_id
              AND recorded_at = NEW.factor_set_recorded_at;
            IF header.creator_xid = txid_current() THEN
                RAISE EXCEPTION 'factor-set availability requires a later transaction'
                    USING ERRCODE = '55000';
            END IF;
            PERFORM set_config('lock_timeout', '30s', true);
            PERFORM pg_advisory_xact_lock(
                public.adjustment_factor_series_fence_id(header.symbol)
            );
            SELECT count(*), count(*) FILTER (
                WHERE entry.creator_xid = txid_current()
                   OR entry.symbol <> header.symbol
                   OR entry.raw_available_at > header.cutoff
            )
            INTO entry_count, invalid_count
            FROM public.adjustment_factor_entries AS entry
            WHERE entry.factor_set_id = NEW.factor_set_id;
            IF entry_count <> header.input_count OR invalid_count <> 0 THEN
                RAISE EXCEPTION 'factor-set entries are incomplete or uncommitted'
                    USING ERRCODE = '55000';
            END IF;
            NEW.available_at := clock_timestamp();
            RETURN NEW;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RAISE EXCEPTION 'factor-set header does not exist'
                    USING ERRCODE = '23503';
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER adjustment_factor_set_availability_stamp "
        "BEFORE INSERT ON adjustment_factor_set_availability FOR EACH ROW "
        "EXECUTE FUNCTION stamp_adjustment_factor_set_availability()"
    )
    op.execute(
        r"""
        CREATE FUNCTION publish_adjustment_factor_set_receipt(
            requested_factor_set_id text
        ) RETURNS TABLE (
            factor_set_id text,
            factor_set_recorded_at timestamptz,
            available_at timestamptz
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE recorded timestamptz;
        BEGIN
            IF requested_factor_set_id !~ '^sha256:[0-9a-f]{64}$' THEN
                RAISE EXCEPTION 'factor-set id is malformed'
                    USING ERRCODE = '22023';
            END IF;
            SELECT header.recorded_at INTO STRICT recorded
            FROM public.adjustment_factor_sets AS header
            WHERE header.factor_set_id = requested_factor_set_id;
            INSERT INTO public.adjustment_factor_set_availability (
                factor_set_id, factor_set_recorded_at
            ) VALUES (requested_factor_set_id, recorded)
            ON CONFLICT ON CONSTRAINT pk_adjustment_factor_set_availability
            DO NOTHING;
            RETURN QUERY
            SELECT receipt.factor_set_id::text,
                   receipt.factor_set_recorded_at,
                   receipt.available_at
            FROM public.adjustment_factor_set_availability AS receipt
            WHERE receipt.factor_set_id = requested_factor_set_id;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RAISE EXCEPTION 'factor-set header does not exist'
                    USING ERRCODE = '23503';
        END;
        $$
        """
    )


def _apply_privileges() -> None:
    functions = (
        "canonical_adjustment_factor_json(jsonb)",
        "adjustment_decimal34(numeric)",
        "adjustment_divide34(numeric,numeric)",
        "adjustment_decimal_text(numeric)",
        "parse_adjustment_timestamp(text)",
        "parse_adjustment_date(text)",
        "parse_adjustment_decimal(text)",
        "adjustment_factor_series_fence_id(text)",
        "stamp_adjustment_factor_set()",
        "stamp_adjustment_factor_entry()",
        "reject_adjustment_factor_mutation()",
        "publish_adjustment_factor_set(bytea)",
        "stamp_adjustment_factor_set_availability()",
        "publish_adjustment_factor_set_receipt(text)",
    )
    for function in functions:
        op.execute(f"REVOKE ALL ON FUNCTION public.{function} FROM PUBLIC")

    tables = ", ".join(
        (
            "public.adjustment_factor_sets",
            "public.adjustment_factor_entries",
            "public.adjustment_factor_set_availability",
        )
    )
    op.execute(f"REVOKE ALL ON TABLE {tables} FROM PUBLIC")
    op.execute(f"REVOKE ALL ON TABLE {tables} FROM stockapi_app")
    op.execute(f"REVOKE ALL ON TABLE {tables} FROM stockapi_snapshot_builder")
    op.execute(f"GRANT SELECT ON TABLE {tables} TO stockapi_app")
    op.execute(f"GRANT SELECT ON TABLE {tables} TO stockapi_snapshot_builder")
    op.execute(
        "GRANT EXECUTE ON FUNCTION public.publish_adjustment_factor_set(bytea) "
        "TO stockapi_snapshot_builder"
    )
    op.execute(
        "GRANT EXECUTE ON FUNCTION public.publish_adjustment_factor_set_receipt(text) "
        "TO stockapi_snapshot_builder"
    )
    op.execute(
        "GRANT EXECUTE ON FUNCTION public.adjustment_factor_series_fence_id(text) "
        "TO stockapi_snapshot_builder"
    )


def downgrade() -> None:
    op.execute(
        r"""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM public.adjustment_factor_sets)
               OR EXISTS (SELECT 1 FROM public.adjustment_factor_entries)
               OR EXISTS (
                   SELECT 1 FROM public.adjustment_factor_set_availability
               ) THEN
                RAISE EXCEPTION 'cannot downgrade nonempty adjustment-factor evidence'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $$
        """
    )
    for table in (
        "adjustment_factor_set_availability",
        "adjustment_factor_entries",
        "adjustment_factor_sets",
    ):
        op.execute(f"DROP TRIGGER IF EXISTS {table}_no_truncate ON {table}")
        op.execute(f"DROP TRIGGER IF EXISTS {table}_no_row_mutation ON {table}")
    op.execute(
        "DROP TRIGGER IF EXISTS adjustment_factor_set_availability_stamp "
        "ON adjustment_factor_set_availability"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS adjustment_factor_entries_stamp ON adjustment_factor_entries"
    )
    op.execute("DROP TRIGGER IF EXISTS adjustment_factor_sets_stamp ON adjustment_factor_sets")
    for function in (
        "publish_adjustment_factor_set_receipt(text)",
        "stamp_adjustment_factor_set_availability()",
        "publish_adjustment_factor_set(bytea)",
        "reject_adjustment_factor_mutation()",
        "stamp_adjustment_factor_entry()",
        "stamp_adjustment_factor_set()",
        "adjustment_factor_series_fence_id(text)",
        "parse_adjustment_decimal(text)",
        "parse_adjustment_date(text)",
        "parse_adjustment_timestamp(text)",
        "adjustment_decimal_text(numeric)",
        "adjustment_divide34(numeric,numeric)",
        "adjustment_decimal34(numeric)",
        "canonical_adjustment_factor_json(jsonb)",
    ):
        op.execute(f"DROP FUNCTION IF EXISTS public.{function}")
    op.drop_table("adjustment_factor_set_availability")
    op.drop_table("adjustment_factor_entries")
    op.drop_index(
        "ix_adjustment_factor_sets_resolve",
        table_name="adjustment_factor_sets",
    )
    op.drop_table("adjustment_factor_sets")
