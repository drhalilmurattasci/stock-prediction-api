"""ORM and migration shape for immutable outcome/cohort evidence."""

from __future__ import annotations

from dataclasses import fields
from pathlib import Path

from sqlalchemy import DateTime, LargeBinary

from app.db.base import Base
from app.db.models import (
    BarVersionAvailability,
    ForecastOutcomeCohortAvailability,
    ForecastOutcomeCohortManifest,
    ForecastOutcomeCohortMember,
    ForecastOutcomeResolutionPolicyRegistration,
    ForecastRealizedOutcome,
    ForecastRealizedOutcomePublication,
)
from app.services.forecast_cohorts import ForecastCohortRecord, ForecastCohortSeal
from app.services.forecast_outcome_store import ForecastOutcomePublicationRecord
from app.services.forecast_outcomes import RealizedOutcomeRecord

MIGRATION = Path("migrations/versions/0010_forecast_evidence.py")
POLICY_FENCE_MIGRATION = Path("migrations/versions/0011_outcome_policy_fence.py")


def test_outcome_model_binds_exact_receipt_and_content_hash() -> None:
    table = ForecastRealizedOutcome.__table__
    assert Base.metadata.tables["forecast_realized_outcomes"] is table
    assert tuple(column.name for column in table.primary_key) == ("outcome_id",)
    assert isinstance(table.c.canonical_evidence.type, LargeBinary)
    for name in (
        "target_time",
        "resolution_cutoff",
        "bar_observed_at",
        "bar_version_recorded_at",
        "bar_fetched_at",
        "bar_source_as_of",
        "bar_available_at",
        "sealed_at",
    ):
        assert isinstance(table.c[name].type, DateTime)
        assert table.c[name].type.timezone is True

    constraints = {str(item.name): item for item in table.constraints}
    assert {
        "ck_forecast_realized_outcomes_outcome_id_matches_payload",
        "ck_forecast_realized_outcomes_evidence_time_order",
        "ck_forecast_realized_outcomes_values_finite",
        "uq_forecast_realized_outcomes_semantic_key",
        "fk_forecast_realized_outcomes_exact_bar_receipt_bar_version_availability",
    } <= constraints.keys()
    foreign_key = constraints[
        "fk_forecast_realized_outcomes_exact_bar_receipt_bar_version_availability"
    ]
    semantic_key = constraints["uq_forecast_realized_outcomes_semantic_key"]
    assert tuple(column.name for column in semantic_key.columns) == (
        "outcome_resolution_policy_hash",
        "availability_rule_set_hash",
        "symbol",
        "target",
        "series_basis",
        "target_time",
    )
    assert tuple(element.parent.name for element in foreign_key.elements) == (
        "symbol",
        "bar_timespan",
        "bar_multiplier",
        "bar_observed_at",
        "bar_source",
        "bar_adjustment_basis",
        "bar_version_recorded_at",
        "bar_available_at",
    )
    assert all(
        element.target_fullname.startswith("bar_version_availability.")
        for element in foreign_key.elements
    )
    assert foreign_key.ondelete == "RESTRICT"

    receipt_constraints = {
        str(item.name): item for item in BarVersionAvailability.__table__.constraints
    }
    assert "uq_bar_version_availability_exact_receipt" in receipt_constraints
    assert {field.name for field in fields(RealizedOutcomeRecord)} == set(table.c.keys())


def test_cohort_model_separates_manifest_from_commit_availability() -> None:
    manifest = ForecastOutcomeCohortManifest.__table__
    members = ForecastOutcomeCohortMember.__table__
    availability = ForecastOutcomeCohortAvailability.__table__
    assert Base.metadata.tables[manifest.name] is manifest
    assert Base.metadata.tables[availability.name] is availability
    assert Base.metadata.tables[members.name] is members
    assert isinstance(manifest.c.canonical_manifest.type, LargeBinary)
    assert tuple(column.name for column in availability.primary_key) == ("cohort_id",)
    assert next(iter(availability.c.cohort_id.foreign_keys)).ondelete == "RESTRICT"
    assert manifest.c.purpose.type.length == 32
    for table, names in (
        (manifest, ("earliest_target_time", "latest_target_time", "recorded_at")),
        (availability, ("manifest_recorded_at", "sealed_at")),
    ):
        for name in names:
            assert isinstance(table.c[name].type, DateTime)
            assert table.c[name].type.timezone is True
    assert availability.c.sealer_xid.nullable is False
    assert {field.name for field in fields(ForecastCohortRecord)} == set(manifest.c.keys())
    assert {field.name for field in fields(ForecastCohortSeal)} == set(availability.c.keys())
    assert tuple(column.name for column in members.primary_key) == (
        "cohort_id",
        "forecast_id",
        "step",
    )
    member_foreign_keys = {key.target_fullname for key in members.foreign_keys}
    assert member_foreign_keys == {
        "forecast_outcome_cohort_manifests.cohort_id",
        "forecast_runs.forecast_id",
    }
    member_constraints = {str(item.name) for item in members.constraints}
    assert "uq_forecast_outcome_cohort_members_opportunity_step" in member_constraints


def test_policy_registry_and_publication_models_bind_exact_provenance() -> None:
    policies = ForecastOutcomeResolutionPolicyRegistration.__table__
    outcomes = ForecastRealizedOutcome.__table__
    manifests = ForecastOutcomeCohortManifest.__table__
    publications = ForecastRealizedOutcomePublication.__table__

    assert Base.metadata.tables[policies.name] is policies
    assert tuple(column.name for column in policies.primary_key) == ("policy_hash",)
    policy_constraints = {str(item.name): item for item in policies.constraints}
    assert {
        "ck_forecast_outcome_resolution_policies_resolution_lag_bounded",
        "ck_forecast_outcome_resolution_policies_canonical_policy_size_bounded",
        "ck_forecast_outcome_resolution_policies_policy_hash_matches_payload",
        "uq_forecast_outcome_resolution_policies_policy_rules",
    } <= policy_constraints.keys()

    for table in (outcomes, manifests):
        foreign_keys = {
            str(item.name): item for item in table.constraints if getattr(item, "elements", None)
        }
        policy_key = next(
            item for name, item in foreign_keys.items() if name.endswith("registered_policy")
        )
        assert tuple(element.parent.name for element in policy_key.elements) == (
            "outcome_resolution_policy_hash",
            "availability_rule_set_hash",
        )
        assert policy_key.ondelete == "RESTRICT"

    assert "ck_forecast_realized_outcomes_currency_usd" in {
        str(item.name) for item in outcomes.constraints
    }
    assert tuple(column.name for column in publications.primary_key) == (
        "outcome_id",
        "cohort_id",
        "forecast_id",
        "step",
    )
    publication_foreign_keys = {
        str(item.name): item for item in publications.constraints if getattr(item, "elements", None)
    }
    cohort_member_key = next(
        item for name, item in publication_foreign_keys.items() if "cohort_member" in name
    )
    assert tuple(element.parent.name for element in cohort_member_key.elements) == (
        "cohort_id",
        "forecast_id",
        "step",
    )
    assert cohort_member_key.ondelete == "RESTRICT"
    assert {field.name for field in fields(ForecastOutcomePublicationRecord)} == set(
        publications.c.keys()
    )


def test_migration_uses_second_transaction_precommit_proof_and_exact_acls() -> None:
    migration = MIGRATION.read_text(encoding="utf-8")
    upgrade, downgrade = migration.split("def downgrade() -> None:", maxsplit=1)

    assert 'revision: str = "0010_forecast_evidence"' in upgrade
    assert 'down_revision: str | None = "0009_forecast_runs"' in upgrade
    assert "uq_bar_version_availability_exact_receipt" in upgrade
    assert "digest(canonical_evidence, 'sha256')" in upgrade
    assert "digest(canonical_manifest, 'sha256')" in upgrade
    assert "manifest_creator = txid_current()" in upgrade
    assert "stamped >= first_target" in upgrade
    assert "cohort availability requires a later transaction" in upgrade
    assert "cohort was not committed before its first target" in upgrade
    assert "SELECT version.close, version.fetched_at, version.source_as_of" in upgrade
    assert "outcome value does not match its exact bar version" in upgrade
    assert "CREATE FUNCTION materialize_forecast_outcome_cohort_members()" in upgrade
    assert "CREATE FUNCTION validate_forecast_outcome_cohort_member()" in upgrade
    assert "origin_kind = 'scheduled_evaluation'" in upgrade
    assert "min(member.target_time)" in upgrade
    assert "NEW.sealer_xid := txid_current()" in upgrade
    assert "BEFORE UPDATE OR DELETE" in upgrade
    assert "BEFORE TRUNCATE" in upgrade
    assert 'else "SELECT, INSERT"' in upgrade
    assert 'table == "forecast_outcome_cohort_members"' in upgrade
    assert "runtime forecast-evidence privileges are not exact" in upgrade
    assert "snapshot builder forecast-evidence privileges are not empty" in upgrade
    assert "forecast-evidence trigger function is executable" in upgrade

    assert "DROP FUNCTION IF EXISTS reject_forecast_evidence_mutation()" in downgrade
    assert 'op.drop_table("forecast_outcome_cohort_availability")' in downgrade
    assert 'op.drop_table("forecast_outcome_cohort_members")' in downgrade
    assert 'op.drop_table("forecast_outcome_cohort_manifests")' in downgrade
    assert 'op.drop_table("forecast_realized_outcomes")' in downgrade
    assert "uq_bar_version_availability_exact_receipt" in downgrade
    assert "DROP EXTENSION" not in downgrade


def test_policy_fence_migration_is_fail_closed_and_source_bound() -> None:
    migration = POLICY_FENCE_MIGRATION.read_text(encoding="utf-8")
    upgrade, downgrade = migration.split("def downgrade() -> None:", maxsplit=1)

    assert 'revision: str = "0011_outcome_policy_fence"' in upgrade
    assert 'down_revision: str | None = "0010_forecast_evidence"' in upgrade
    assert "outcome policy migration requires empty pre-policy evidence tables" in upgrade
    assert "CREATE FUNCTION forecast_bar_series_fence_id(" in upgrade
    assert "CREATE FUNCTION fence_bar_version_availability()" in upgrade
    assert "pg_advisory_xact_lock(" in upgrade
    assert "CREATE FUNCTION register_forecast_outcome_resolution_policy(" in upgrade
    assert "p_canonical_policy bytea" in upgrade
    assert "CREATE FUNCTION publish_forecast_realized_outcome(" in upgrade
    assert "p_cohort_id varchar" in upgrade
    assert "octet_length(p_canonical_evidence) NOT BETWEEN 1 AND 262144" in upgrade
    assert "outcome evidence bytes are not the exact canonical form" in upgrade
    assert "transaction_isolation') <> 'read committed'" in upgrade
    assert "forecast-run-output-v1" in upgrade
    assert "IS DISTINCT FROM archived_snapshot_id" in upgrade
    assert "ON CONFLICT (outcome_id) DO NOTHING" in upgrade
    assert "ON CONFLICT (outcome_id, cohort_id, forecast_id, step) DO NOTHING" in upgrade
    assert "has_any_column_privilege(" in upgrade
    assert "REVOKE INSERT (%I) ON TABLE public.forecast_realized_outcomes" in upgrade
    assert "builder_role" in upgrade
    assert "register_forecast_outcome_resolution_policy(bytea)" in upgrade

    assert "cannot downgrade nonempty outcome-policy evidence" in downgrade
    assert 'op.drop_table("forecast_realized_outcome_publications")' in downgrade
    assert 'op.drop_table("forecast_outcome_resolution_policies")' in downgrade
    assert "DROP FUNCTION IF EXISTS forecast_bar_series_fence_id" in downgrade
