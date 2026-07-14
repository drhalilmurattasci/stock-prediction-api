"""Static schema proofs for immutable adjustment-factor evidence."""

from __future__ import annotations

import ast
from pathlib import Path

from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from app.db.models.adjustment_factors import (
    AdjustmentFactorEntry,
    AdjustmentFactorSetAvailability,
    AdjustmentFactorSetRecord,
)
from app.services.adjustment_factors import ADJUSTMENT_FACTOR_POLICY_HASH
from app.services.corporate_actions import CORPORATE_ACTION_QUERY_POLICY_HASH

MIGRATION = Path(__file__).parents[2] / "migrations" / "versions" / "0013_adjustment_factors.py"


def _ddl(model: type[object]) -> str:
    table = model.__table__  # type: ignore[attr-defined]
    return str(CreateTable(table).compile(dialect=postgresql.dialect()))


def test_models_bind_canonical_header_exact_receipts_and_factor_entries() -> None:
    header = _ddl(AdjustmentFactorSetRecord)
    entry = _ddl(AdjustmentFactorEntry)
    receipt = _ddl(AdjustmentFactorSetAvailability)

    assert "digest(canonical_payload, 'sha256')" in header
    assert "max_input_available_at" in header
    assert "exact_split_collection_receipt" in header
    assert "exact_dividend_collection_receipt" in header
    assert "corporate_action_collection_availability" in header
    assert "exact_bar_receipt" in entry
    assert "bar_version_availability" in entry
    assert "raw_close_f64_be BYTEA" in entry
    assert "price_factor_f64_be BYTEA" in entry
    assert "volume_factor_f64_be BYTEA" in entry
    assert "AT TIME ZONE 'UTC'" in entry
    assert "REFERENCES adjustment_factor_sets (factor_set_id, recorded_at)" in receipt


def test_migration_is_linear_and_recursively_validates_byte_derived_projections() -> None:
    migration = MIGRATION.read_text(encoding="utf-8")

    assert 'revision: str = "0013_adjustment_factors"' in migration
    assert len("0013_adjustment_factors") <= 32
    assert 'down_revision: str | None = "0012_corporate_actions"' in migration
    assert ADJUSTMENT_FACTOR_POLICY_HASH in migration
    assert CORPORATE_ACTION_QUERY_POLICY_HASH in migration
    assert "CREATE FUNCTION canonical_adjustment_factor_json(value jsonb)" in migration
    assert 'ORDER BY key COLLATE "C"' in migration
    assert "CREATE FUNCTION publish_adjustment_factor_set(payload_bytes bytea)" in migration
    assert "count(*) FROM jsonb_object_keys(payload)" in migration
    assert "count(*) FROM jsonb_object_keys(raw_row)" in migration
    assert "count(*) FROM jsonb_object_keys(factor_row)" in migration
    assert "count(*) FROM jsonb_object_keys(split_row)" in migration
    assert "count(*) FROM jsonb_object_keys(dividend_row)" in migration
    assert "digest(payload_bytes, 'sha256')" in migration
    assert "float8send(stored_close)" in migration
    assert "stored adjustment-factor entry" not in migration
    assert "entry projection conflicts with canonical bytes" in migration


def test_publisher_fences_and_rejects_stale_bars_or_collections() -> None:
    migration = MIGRATION.read_text(encoding="utf-8")

    assert "forecast_bar_series_fence_id" in migration
    assert migration.count("corporate_action_series_fence_id") >= 2
    assert "SELECT DISTINCT value" in migration
    assert "ORDER BY value" in migration
    assert "pg_advisory_xact_lock(lock_key)" in migration
    assert "cutoff_value > clock_timestamp()" in migration
    assert "ORDER BY receipt.version_recorded_at DESC" in migration
    assert "unique newest cutoff version" in migration
    assert "WITH ranked_visible AS" in migration
    assert "PARTITION BY receipt.ts" in migration
    assert "BETWEEN coverage_start_value AND coverage_end_value" in migration
    assert "factor raw inputs omit newest cutoff-visible stored receipts" in migration
    assert migration.count("ORDER BY collection.recorded_at DESC") == 2
    assert migration.count("collection.collection_id DESC") == 2
    assert "ORDER BY collection.fetched_at DESC" not in migration
    # Both availability tie-breaks belong to bar-version selection; collection
    # availability is eligibility only under the corporate-action query policy.
    assert migration.count("receipt.available_at DESC") == 2
    # Two collection selectors plus per-row and coverage-wide bar selectors.
    assert migration.count("receipt.available_at <= cutoff_value") == 4
    assert "split collection is not newest for the exact cutoff scope" in migration
    assert "dividend collection is not newest for the exact cutoff scope" in migration
    assert "collection.coverage_start = coverage_start_value" in migration
    assert "collection.coverage_end = coverage_end_value" in migration


def test_publisher_recomputes_kernel_and_maximum_evidence_availability() -> None:
    migration = MIGRATION.read_text(encoding="utf-8")

    assert "CREATE FUNCTION adjustment_decimal34(value numeric)" in migration
    assert "substring(significant FROM 36) ~ '[1-9]'" in migration
    assert "CREATE FUNCTION adjustment_divide34" in migration
    assert "coefficient := div(scaled_numerator, scaled_denominator)" in migration
    assert "remainder * 2 = scaled_denominator" in migration
    assert "AND mod(coefficient, 2) = 1" in migration
    assert "numeric(1000,650)" not in migration
    assert "expected_price * split_from_value" in migration
    assert "expected_volume * split_to_value" in migration
    assert "raw_close_value + dividend_cash_value" in migration
    assert migration.count("IS DISTINCT FROM") >= 13
    assert "factor values do not satisfy the pinned kernel" in migration
    assert "factor policy requires globally unique action identities" in migration
    assert "max_evidence_available := GREATEST(" in migration
    assert "split_receipt.available_at" in migration
    assert "dividend_receipt.available_at" in migration
    assert "max_evidence_available,\n                    available_value" in migration


def test_tables_are_frozen_and_only_builder_can_publish() -> None:
    migration = MIGRATION.read_text(encoding="utf-8")
    string_literals = {
        node.value
        for node in ast.walk(ast.parse(migration))
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }

    for table in (
        "adjustment_factor_sets",
        "adjustment_factor_entries",
        "adjustment_factor_set_availability",
    ):
        assert table in migration
    assert "BEFORE UPDATE OR DELETE" in migration
    assert "BEFORE TRUNCATE" in migration
    assert "factor entries must be inserted with their header" in migration
    assert "factor-set availability requires a later transaction" in migration
    assert (
        "GRANT EXECUTE ON FUNCTION public.publish_adjustment_factor_set(bytea) "
        "TO stockapi_snapshot_builder"
    ) in string_literals
    assert (
        "GRANT EXECUTE ON FUNCTION public.publish_adjustment_factor_set_receipt(text) "
        "TO stockapi_snapshot_builder"
    ) in string_literals
    assert not any(
        "publish_adjustment_factor_set(bytea)" in literal and "TO stockapi_app" in literal
        for literal in string_literals
    )
    assert not any(
        "publish_adjustment_factor_set_receipt(text)" in literal and "TO stockapi_app" in literal
        for literal in string_literals
    )
    assert "GRANT SELECT ON TABLE" in migration
    assert "GRANT SELECT, INSERT ON TABLE" not in migration
    assert "cannot downgrade nonempty adjustment-factor evidence" in migration
