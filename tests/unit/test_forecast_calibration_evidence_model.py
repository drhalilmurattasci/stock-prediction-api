"""ORM and migration shape for immutable calibration evidence."""

from __future__ import annotations

from dataclasses import fields
from pathlib import Path

from sqlalchemy import Date, DateTime, LargeBinary

from app.db.base import Base
from app.db.models import (
    ForecastFittedCalibrationSet,
    ForecastHeldoutCoverageRelease,
    ForecastHeldoutCoverageReleaseAvailability,
    ForecastHeldoutCoverageReleaseBucket,
)
from app.services.forecast_calibration_release_store import (
    FittedCalibrationSetRecord,
    HeldoutCoverageReleaseAvailability,
    HeldoutCoverageReleaseBucketRecord,
    HeldoutCoverageReleaseRecord,
)

MIGRATION = Path("migrations/versions/0015_forecast_calibration_evidence.py")


def _constraints(table: object) -> dict[str, object]:
    return {str(item.name): item for item in table.constraints}  # type: ignore[attr-defined]


def test_fitted_calibration_model_is_content_addressed_and_sealed_cohort_bound() -> None:
    table = ForecastFittedCalibrationSet.__table__
    assert Base.metadata.tables[table.name] is table
    assert tuple(column.name for column in table.primary_key) == ("calibration_set_version",)
    assert isinstance(table.c.canonical_set.type, LargeBinary)
    assert isinstance(table.c.window_start.type, Date)
    assert isinstance(table.c.window_end.type, Date)
    assert isinstance(table.c.recorded_at.type, DateTime)
    assert table.c.recorded_at.type.timezone is True

    constraints = _constraints(table)
    assert {
        "ck_forecast_fitted_calibration_sets_calibration_set_version_matches_payload",
        "ck_forecast_fitted_calibration_sets_semantic_scope_supported",
        "ck_forecast_fitted_calibration_sets_source_uncalibrated",
        "ck_forecast_fitted_calibration_sets_policies_supported",
        "fk_fitted_calibration_sets_registered_outcome_policy",
        "uq_fitted_calibration_sets_cohort_method",
    } <= constraints.keys()
    cohort_key = next(
        key
        for key in table.foreign_key_constraints
        if tuple(column.name for column in key.columns) == ("cohort_id",)
    )
    assert next(iter(cohort_key.elements)).target_fullname == (
        "forecast_outcome_cohort_availability.cohort_id"
    )
    assert cohort_key.ondelete == "RESTRICT"
    assert {field.name for field in fields(FittedCalibrationSetRecord)} == set(table.c.keys())


def test_heldout_release_is_descriptive_content_addressed_and_fully_projected() -> None:
    release = ForecastHeldoutCoverageRelease.__table__
    buckets = ForecastHeldoutCoverageReleaseBucket.__table__
    receipt = ForecastHeldoutCoverageReleaseAvailability.__table__
    for table in (release, buckets, receipt):
        assert Base.metadata.tables[table.name] is table

    assert tuple(column.name for column in release.primary_key) == ("release_id",)
    assert isinstance(release.c.canonical_release.type, LargeBinary)
    assert isinstance(release.c.confidence_level_f64_be.type, LargeBinary)
    constraints = _constraints(release)
    assert {
        "ck_forecast_heldout_coverage_releases_scope_descriptive_only",
        "ck_forecast_heldout_coverage_releases_release_id_matches_payload",
        "ck_forecast_heldout_coverage_releases_cohorts_distinct",
        "ck_forecast_heldout_coverage_releases_confidence_level_f64_size",
        "uq_heldout_coverage_releases_exact_analysis",
        "fk_heldout_coverage_releases_registered_outcome_policy",
    } <= constraints.keys()
    assert {foreign_key.target_fullname for foreign_key in release.foreign_keys} == {
        "forecast_fitted_calibration_sets.calibration_set_version",
        "forecast_outcome_cohort_availability.cohort_id",
        "forecast_outcome_resolution_policies.policy_hash",
        "forecast_outcome_resolution_policies.availability_rule_set_hash",
    }
    assert {
        constraint.name
        for constraint in release.foreign_key_constraints
        if tuple(column.name for column in constraint.columns)
        in {("fit_cohort_id",), ("heldout_cohort_id",)}
    } == {
        "fk_heldout_coverage_releases_fit_cohort",
        "fk_heldout_coverage_releases_heldout_cohort",
    }

    assert tuple(column.name for column in buckets.primary_key) == (
        "release_id",
        "horizon",
        "coverage_millis",
    )
    for name in (
        "empirical_coverage_f64_be",
        "confidence_low_f64_be",
        "confidence_high_f64_be",
    ):
        assert isinstance(buckets.c[name].type, LargeBinary)
    assert "ck_forecast_heldout_coverage_release_buckets_f64_values_size" in _constraints(buckets)

    assert tuple(column.name for column in receipt.primary_key) == ("release_id",)
    assert next(iter(receipt.c.release_id.foreign_keys)).ondelete == "RESTRICT"
    for name in ("release_recorded_at", "available_at"):
        assert isinstance(receipt.c[name].type, DateTime)
        assert receipt.c[name].type.timezone is True
    assert {field.name for field in fields(HeldoutCoverageReleaseRecord)} == set(release.c.keys())
    assert {field.name for field in fields(HeldoutCoverageReleaseBucketRecord)} == set(
        buckets.c.keys()
    )
    assert {field.name for field in fields(HeldoutCoverageReleaseAvailability)} == set(
        receipt.c.keys()
    )


def test_migration_uses_definer_publishers_exact_acls_and_second_transaction_receipt() -> None:
    migration = MIGRATION.read_text(encoding="utf-8")
    upgrade, downgrade = migration.split("def downgrade() -> None:", maxsplit=1)

    assert 'revision: str = "0015_calibration_evidence"' in upgrade
    assert len("0015_calibration_evidence") <= 32
    assert 'down_revision: str | None = "0014_vendor_campaign_anchor"' in upgrade
    for table in (
        "forecast_fitted_calibration_sets",
        "forecast_heldout_coverage_releases",
        "forecast_heldout_coverage_release_buckets",
        "forecast_heldout_coverage_release_availability",
    ):
        assert f'"{table}"' in upgrade
    assert "evidence_scope = 'descriptive-only'" in upgrade
    assert "CREATE FUNCTION canonical_forecast_calibration_json(value jsonb)" in upgrade
    assert "fitted calibration set bytes are not canonical" in upgrade
    assert "held-out coverage release bytes are not canonical" in upgrade
    assert "jsonb_typeof(document->key) IS DISTINCT FROM 'string'" in upgrade
    assert "jsonb_typeof(document->'schema_version') IS DISTINCT FROM 'number'" in upgrade
    assert "jsonb_typeof(document->'heldout_sample_count')" in upgrade
    assert "jsonb_typeof(bucket->key) IS DISTINCT FROM 'string'" in upgrade
    assert "jsonb_typeof(bucket->key) IS DISTINCT FROM 'number'" in upgrade
    assert "document->>'sample_count' !~ '^[1-9][0-9]*$'" in upgrade
    assert "document->>'heldout_sample_count' !~ '^[1-9][0-9]*$'" in upgrade
    assert "bucket->>'covered_count' !~ '^(0|[1-9][0-9]*)$'" in upgrade
    assert "document->>'window_start' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'" in upgrade
    assert "document->>'heldout_window_start' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'" in upgrade
    assert "fitted calibration buckets are not strictly ordered" in upgrade
    assert "held-out coverage buckets are not strictly ordered" in upgrade
    assert upgrade.count("bucket_document jsonb;") == 2
    assert "\n            bucket jsonb;" not in upgrade
    assert "value_f64_be" in upgrade
    assert "held-out release bucket projection differs from bytes" in upgrade
    assert "uq_fitted_calibration_sets_cohort_method" in upgrade
    assert "ix_forecast_realized_outcome_publications_cohort_member" in upgrade
    assert "held-out release scope differs from its forecast cohort" in upgrade
    assert "run.calibration_method <> fitted.source_calibration_method" in upgrade
    assert "run.calibration_set_version <>" in upgrade
    assert "CREATE FUNCTION publish_fitted_calibration_set(p_canonical_set bytea)" in upgrade
    assert "CREATE FUNCTION publish_forecast_heldout_coverage_release(" in upgrade
    assert "CREATE FUNCTION publish_forecast_heldout_coverage_release_receipt(" in upgrade
    assert "RETURNS TABLE(" in upgrade
    assert "release_id text" in upgrade
    assert "SECURITY DEFINER" in upgrade
    assert "held-out release availability requires a later transaction" in upgrade
    assert (
        "ON CONFLICT ON CONSTRAINT\n"
        "                pk_forecast_heldout_coverage_release_availability DO NOTHING"
    ) in upgrade
    assert "BEFORE UPDATE OR DELETE" in upgrade
    assert "BEFORE TRUNCATE" in upgrade
    assert "GRANT SELECT ON TABLE" in upgrade
    assert "has_any_column_privilege" in upgrade
    assert "runtime calibration-evidence privileges are not exact" in upgrade
    assert "snapshot builder calibration-evidence privileges are not empty" in upgrade
    assert "calibration-evidence publisher privileges are not exact" in upgrade

    assert "cannot downgrade nonempty forecast calibration evidence" in downgrade
    assert "publish_forecast_heldout_coverage_release_receipt(varchar)" in downgrade
    assert "ix_forecast_realized_outcome_publications_cohort_member" in downgrade
    assert 'op.drop_table("forecast_heldout_coverage_release_availability")' in downgrade
    assert 'op.drop_table("forecast_heldout_coverage_release_buckets")' in downgrade
    assert 'op.drop_table("forecast_heldout_coverage_releases")' in downgrade
    assert 'op.drop_table("forecast_fitted_calibration_sets")' in downgrade
