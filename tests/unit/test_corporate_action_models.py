"""Static schema proofs for immutable corporate-action collections."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from app.db.models.corporate_actions import (
    CorporateActionCollection,
    CorporateActionCollectionAvailability,
    CorporateActionCollectionMember,
    CorporateActionVersion,
)
from app.services.corporate_actions import CORPORATE_ACTION_QUERY_POLICY_HASH

MIGRATION = (
    Path(__file__).parents[2] / "migrations" / "versions" / "0012_corporate_action_collections.py"
)


def _ddl(model: type[object]) -> str:
    table = model.__table__  # type: ignore[attr-defined]
    return str(CreateTable(table).compile(dialect=postgresql.dialect()))


def test_models_pin_content_hashes_complete_membership_and_exact_receipt() -> None:
    version = _ddl(CorporateActionVersion)
    collection = _ddl(CorporateActionCollection)
    member = _ddl(CorporateActionCollectionMember)
    receipt = _ddl(CorporateActionCollectionAvailability)

    assert "digest(canonical_event, 'sha256')" in version
    assert "NUMERIC(38, 18)" in version
    assert "'Infinity'::numeric" in version
    assert "digest(canonical_manifest, 'sha256')" in collection
    assert "pagination_exhausted" in collection
    assert "creator_xid" in member
    assert "REFERENCES corporate_action_versions (action_version_id)" in member
    assert "REFERENCES corporate_action_collections (collection_id, recorded_at)" in receipt
    assert "UNIQUE (collection_id, collection_recorded_at, available_at)" in receipt


def test_migration_uses_validated_publishers_and_freezes_complete_sets() -> None:
    migration = MIGRATION.read_text(encoding="utf-8")

    assert 'revision: str = "0012_corporate_actions"' in migration
    assert 'down_revision: str | None = "0011_outcome_policy_fence"' in migration
    assert CORPORATE_ACTION_QUERY_POLICY_HASH in migration
    assert "CREATE FUNCTION publish_corporate_action_collection(" in migration
    assert "CREATE FUNCTION publish_corporate_action_collection_receipt(" in migration
    assert "CREATE FUNCTION canonical_corporate_action_json(" in migration
    assert 'ORDER BY key COLLATE "C"' in migration
    assert "corporate_action_collection_members_stamp" in migration
    assert "members must be inserted with their collection" in migration
    assert "collection availability requires a later transaction" in migration
    assert "pg_advisory_xact_lock" in migration
    assert "corporate_action_series_fence_id" in migration
    assert "GRANT SELECT, INSERT ON TABLE" not in migration
    assert "publish_corporate_action_collection(bytea,bytea[]) TO stockapi_app" in migration
    assert "TO stockapi_snapshot_builder" in migration


def test_every_evidence_table_rejects_row_mutation_and_truncate() -> None:
    migration = MIGRATION.read_text(encoding="utf-8")

    for table in (
        "corporate_action_versions",
        "corporate_action_collections",
        "corporate_action_collection_members",
        "corporate_action_collection_availability",
    ):
        assert table in migration
    assert "BEFORE UPDATE OR DELETE" in migration
    assert "BEFORE TRUNCATE" in migration
