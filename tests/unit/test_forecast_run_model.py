"""ORM and migration shape for immutable forecast-run persistence."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import DateTime, LargeBinary

from app.db.base import Base
from app.db.models import ForecastRun

MIGRATION = Path("migrations/versions/0009_forecast_runs.py")


def test_forecast_run_registers_the_complete_audit_record() -> None:
    table = ForecastRun.__table__

    assert Base.metadata.tables["forecast_runs"] is table
    assert tuple(column.name for column in table.primary_key) == ("forecast_id",)
    assert tuple(table.c) == tuple(
        table.c[name]
        for name in (
            "forecast_id",
            "schema_version",
            "origin_kind",
            "idempotency_token_digest",
            "request_hash",
            "opportunity_hash",
            "output_hash",
            "snapshot_id",
            "resolution_policy_hash",
            "availability_rule_set_hash",
            "symbol",
            "target",
            "horizon",
            "horizon_unit",
            "series_basis",
            "as_of",
            "max_available_at",
            "model_version",
            "feature_set_hash",
            "code_version",
            "calibration_set_version",
            "calibration_method",
            "generated_at",
            "recorded_at",
            "canonical_request",
            "canonical_output",
        )
    )
    assert isinstance(table.c.canonical_request.type, LargeBinary)
    assert isinstance(table.c.canonical_output.type, LargeBinary)
    assert table.c.idempotency_token_digest.type.length == 76
    assert table.c.idempotency_token_digest.nullable is True

    for column_name in ("as_of", "max_available_at", "generated_at", "recorded_at"):
        column_type = table.c[column_name].type
        assert isinstance(column_type, DateTime)
        assert column_type.timezone is True

    foreign_keys = tuple(table.c.snapshot_id.foreign_keys)
    assert len(foreign_keys) == 1
    assert foreign_keys[0].target_fullname == "forecast_input_snapshots.snapshot_id"
    assert foreign_keys[0].ondelete == "RESTRICT"


def test_forecast_run_metadata_pins_hashes_time_order_and_deduplication() -> None:
    table = ForecastRun.__table__
    constraints = {str(constraint.name): constraint for constraint in table.constraints}

    assert {
        "ck_forecast_runs_request_hash_matches_payload",
        "ck_forecast_runs_output_hash_matches_payload",
        "ck_forecast_runs_idempotency_token_digest_format",
        "ck_forecast_runs_time_order",
        "ck_forecast_runs_target_series_basis",
        "uq_forecast_runs_idempotency_token_digest",
        "fk_forecast_runs_snapshot_id_forecast_input_snapshots",
    } <= constraints.keys()
    assert "digest(canonical_request, 'sha256')" in str(
        constraints["ck_forecast_runs_request_hash_matches_payload"].sqltext
    )
    assert "digest(canonical_output, 'sha256')" in str(
        constraints["ck_forecast_runs_output_hash_matches_payload"].sqltext
    )
    assert "hmac-sha256:" in str(
        constraints["ck_forecast_runs_idempotency_token_digest_format"].sqltext
    )
    assert "max_available_at <= as_of" in str(constraints["ck_forecast_runs_time_order"].sqltext)

    indexes = {index.name: index for index in table.indexes}
    assert tuple(indexes["ix_forecast_runs_opportunity_hash"].columns.keys()) == (
        "opportunity_hash",
    )
    scheduled = indexes["uq_forecast_runs_scheduled_opportunity"]
    assert scheduled.unique is True
    assert tuple(scheduled.columns.keys()) == ("opportunity_hash",)
    assert str(scheduled.dialect_options["postgresql"]["where"]) == (
        "origin_kind = 'scheduled_evaluation'"
    )


def test_forecast_run_migration_enforces_insert_only_bytes_and_exact_roles() -> None:
    migration = MIGRATION.read_text(encoding="utf-8")
    upgrade, downgrade = migration.split("def downgrade() -> None:", maxsplit=1)

    assert 'revision: str = "0009_forecast_runs"' in upgrade
    assert 'down_revision: str | None = "0008_bar_version_availability"' in upgrade
    assert "CREATE EXTENSION IF NOT EXISTS pgcrypto" in upgrade
    assert "digest(canonical_request, 'sha256')" in upgrade
    assert "digest(canonical_output, 'sha256')" in upgrade
    assert "^hmac-sha256:[0-9a-f]{64}$" in upgrade
    assert "NEW.recorded_at := clock_timestamp()" in upgrade
    assert "BEFORE UPDATE OR DELETE ON forecast_runs" in upgrade
    assert "BEFORE TRUNCATE ON forecast_runs" in upgrade
    assert "GRANT SELECT, INSERT ON TABLE public.forecast_runs TO stockapi_app" in upgrade
    assert "FROM stockapi_snapshot_builder" in upgrade
    assert "REVOKE ALL ON TABLE public.forecast_runs FROM PUBLIC" in upgrade
    assert "'MAINTAIN'" in upgrade
    assert "has_any_column_privilege" in upgrade
    assert "forecast-run trigger functions are directly executable" in upgrade

    assert "DROP TRIGGER IF EXISTS forecast_runs_no_truncate" in downgrade
    assert "DROP TRIGGER IF EXISTS forecast_runs_no_row_mutation" in downgrade
    assert "DROP TRIGGER IF EXISTS forecast_runs_stamp_recorded_at" in downgrade
    assert "DROP FUNCTION IF EXISTS reject_forecast_run_mutation()" in downgrade
    assert "DROP FUNCTION IF EXISTS stamp_forecast_run_recorded_at()" in downgrade
    assert 'op.drop_table("forecast_runs")' in downgrade
    assert "DROP EXTENSION" not in downgrade
