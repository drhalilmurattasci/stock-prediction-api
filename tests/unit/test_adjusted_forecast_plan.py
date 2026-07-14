"""Deterministic read-only adjusted forecast plan tests."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Literal, cast

import exchange_calendars as xcals
import pandas as pd
import pytest

import scripts.adjusted_forecast_plan as plan_module
from app.config import Settings
from app.services.adjusted_forecast_snapshot_builder import (
    ADJUSTED_AVAILABILITY_RULE_SET_HASH,
    ADJUSTED_RESOLUTION_POLICY_HASH,
)
from app.services.adjustment_factor_builder import AdjustmentFactorBuildSpec
from app.services.adjustment_factors import (
    ADJUSTMENT_FACTOR_POLICY_HASH,
    ADJUSTMENT_FACTOR_POLICY_VERSION,
    ADJUSTMENT_FACTOR_SET_FORMAT,
    AdjustmentFactorSet,
    RawCloseVersion,
    build_adjustment_factor_set,
)
from app.services.corporate_action_store import (
    CorporateActionCollectionEvidence,
    CorporateActionScopeCoverage,
)
from app.services.corporate_actions import (
    CORPORATE_ACTION_QUERY_POLICY_HASH,
    CORPORATE_ACTION_SOURCE,
    DIVIDENDS_ENDPOINT,
    SPLITS_ENDPOINT,
)
from scripts.vendor_acquisition import AcquisitionPlan, ActionScopeState
from scripts.vendor_backfill import (
    REQUIRED_SESSIONS,
    BackfillPlan,
    BackfillRefused,
    _expected_session_dates,
)

END = date(2026, 7, 14)
TOOL_REVISION = "a" * 40
ACQUISITION_PLAN_ID = "sha256:" + "1" * 64
SPLIT_COLLECTION_ID = "sha256:" + "2" * 64
DIVIDEND_COLLECTION_ID = "sha256:" + "3" * 64
RAW_MAX_AVAILABLE_AT = datetime(2026, 7, 14, 20, 2, tzinfo=UTC)
SPLIT_RECORDED_AT = datetime(2026, 7, 14, 20, 2, 30, tzinfo=UTC)
SPLIT_AVAILABLE_AT = datetime(2026, 7, 14, 20, 3, tzinfo=UTC)
DIVIDEND_RECORDED_AT = datetime(2026, 7, 14, 20, 3, 30, tzinfo=UTC)
DIVIDEND_AVAILABLE_AT = datetime(2026, 7, 14, 20, 4, tzinfo=UTC)
FACTOR_CUTOFF = DIVIDEND_AVAILABLE_AT
DATABASE_NOW = datetime(2026, 7, 14, 21, tzinfo=UTC)
FINAL_DATABASE_NOW = DATABASE_NOW + timedelta(minutes=1)


def _settings(**updates: object) -> Settings:
    values: dict[str, object] = {
        "app_env": "local",
        "api_v1_prefix": "/v1",
        "database_url": (
            "postgresql+asyncpg://stockapi_app:secret-canary@127.0.0.1:5432/stockapi_test"
        ),
        "api_keys": "local-adjusted-demo-key",
        "jwt_secret": "adjusted-demo-binding-secret-32-characters",
        "forecast_adjusted_close_resolution_policy_hash": (ADJUSTED_RESOLUTION_POLICY_HASH),
        "forecast_adjusted_close_trusted_availability_rule_set_hash": (
            ADJUSTED_AVAILABILITY_RULE_SET_HASH
        ),
        "polygon_api_key": "vendor-secret-canary",
    }
    values.update(updates)
    return Settings(**values)  # type: ignore[arg-type]


def _collection(
    action_type: Literal["split", "dividend"],
    collection_id: str,
    recorded_at: datetime,
    available_at: datetime | None,
) -> CorporateActionCollectionEvidence:
    del action_type
    return CorporateActionCollectionEvidence(
        collection_id=collection_id,
        collection_recorded_at=recorded_at,
        fetched_at=recorded_at - timedelta(minutes=1),
        event_count=0,
        available_at=available_at,
    )


def _coverage(
    action_type: Literal["split", "dividend"],
    collections: tuple[CorporateActionCollectionEvidence, ...],
) -> CorporateActionScopeCoverage:
    dates = _expected_session_dates(END)
    endpoint = SPLITS_ENDPOINT if action_type == "split" else DIVIDENDS_ENDPOINT
    return CorporateActionScopeCoverage(
        action_type=action_type,
        endpoint=endpoint,
        source=CORPORATE_ACTION_SOURCE,
        symbol="MSFT",
        coverage_start=dates[0],
        coverage_end=dates[-1],
        query_policy_hash=CORPORATE_ACTION_QUERY_POLICY_HASH,
        collections=collections,
    )


def _acquisition_plan(
    *,
    split_collections: tuple[CorporateActionCollectionEvidence, ...] | None = None,
    dividend_collections: tuple[CorporateActionCollectionEvidence, ...] | None = None,
    complete_count: int = REQUIRED_SESSIONS,
) -> AcquisitionPlan:
    dates = _expected_session_dates(END)
    complete = dates[:complete_count]
    missing = dates[complete_count:]
    price = BackfillPlan(
        end_session=END,
        tool_revision=TOOL_REVISION,
        expected_dates=dates,
        complete_dates=complete,
        repairable_dates=(),
        missing_dates=missing,
        version_ids=tuple((value, f"sha256:{index:064x}") for index, value in enumerate(complete)),
        ambiguous_dates=(),
        plan_id="sha256:" + "4" * 64,
    )
    split_values = split_collections or (
        _collection(
            "split",
            SPLIT_COLLECTION_ID,
            SPLIT_RECORDED_AT,
            SPLIT_AVAILABLE_AT,
        ),
    )
    dividend_values = dividend_collections or (
        _collection(
            "dividend",
            DIVIDEND_COLLECTION_ID,
            DIVIDEND_RECORDED_AT,
            DIVIDEND_AVAILABLE_AT,
        ),
    )
    return AcquisitionPlan(
        price_plan=price,
        split_state=ActionScopeState(
            "split",
            SPLITS_ENDPOINT,
            _coverage("split", split_values),
        ),
        dividend_state=ActionScopeState(
            "dividend",
            DIVIDENDS_ENDPOINT,
            _coverage("dividend", dividend_values),
        ),
        calls=(),
        calls_sha256="sha256:" + "5" * 64,
        ambiguous_call_ids=(),
        plan_id=ACQUISITION_PLAN_ID,
    )


def _artifact(
    *,
    split_collection_id: str = SPLIT_COLLECTION_ID,
    dividend_collection_id: str = DIVIDEND_COLLECTION_ID,
) -> AdjustmentFactorSet:
    dates = _expected_session_dates(END)
    calendar = xcals.get_calendar("XNYS")
    rows: list[RawCloseVersion] = []
    for ordinal, session_date in enumerate(dates):
        observed_at = (
            calendar.session_close(pd.Timestamp(session_date)).to_pydatetime().astimezone(UTC)
        )
        if session_date == END:
            available_at = RAW_MAX_AVAILABLE_AT
        else:
            available_at = observed_at + timedelta(minutes=2)
        rows.append(
            RawCloseVersion(
                observation_date=session_date,
                observed_at=observed_at,
                timespan="day",
                multiplier=1,
                source="polygon_open_close",
                adjustment_basis="raw",
                version_recorded_at=available_at - timedelta(minutes=1),
                available_at=available_at,
                close=Decimal("100") + Decimal(ordinal) / Decimal("10"),
            )
        )
    return build_adjustment_factor_set(
        symbol="MSFT",
        cutoff=FACTOR_CUTOFF,
        raw_closes=tuple(rows),
        split_collection_id=split_collection_id,
        splits=(),
        dividend_collection_id=dividend_collection_id,
        dividends=(),
    )


class _FakeStore:
    def __init__(
        self,
        *,
        artifact: AdjustmentFactorSet | None = None,
        raw_state: plan_module.RawReceiptState | None = None,
        prior: plan_module.PriorFactorState | None = None,
        database_times: tuple[datetime, datetime] = (DATABASE_NOW, FINAL_DATABASE_NOW),
    ) -> None:
        self.artifact = artifact or _artifact()
        self.raw_state = raw_state or plan_module.RawReceiptState(
            REQUIRED_SESSIONS,
            RAW_MAX_AVAILABLE_AT,
        )
        self.prior = prior or plan_module.PriorFactorState(False, None, None, ())
        self.database_times = list(database_times)
        self.prepared_specs: list[AdjustmentFactorBuildSpec] = []

    async def database_now(self) -> datetime:
        return self.database_times.pop(0)

    async def raw_receipts(
        self,
        session_dates: tuple[date, ...],
    ) -> plan_module.RawReceiptState:
        assert session_dates == _expected_session_dates(END)
        return self.raw_state

    async def prepare_factor(
        self,
        spec: AdjustmentFactorBuildSpec,
    ) -> AdjustmentFactorSet:
        self.prepared_specs.append(spec)
        return self.artifact

    async def prior_factor_state(self, **kwargs: object) -> plan_module.PriorFactorState:
        assert kwargs["factor_cutoff"] == FACTOR_CUTOFF
        assert kwargs["expected_factor_set_id"] == self.artifact.factor_set_id
        return self.prior


def _store_factory(store: _FakeStore):
    @asynccontextmanager
    async def factory(settings: Settings):
        assert settings.polygon_api_key is None
        yield store

    return factory


def _acquisition_planner(plan: AcquisitionPlan):
    async def planner(**kwargs: object) -> AcquisitionPlan:
        settings = cast(Settings, kwargs["settings"])
        assert settings.polygon_api_key is None
        assert cast(Callable[[], datetime], kwargs["clock"])() == DATABASE_NOW
        assert cast(Callable[[], str], kwargs["revision_fn"])() == TOOL_REVISION
        return plan

    return planner


@pytest.mark.asyncio
async def test_ready_plan_binds_exact_receipts_factor_identity_auth_and_post_request() -> None:
    acquisition = _acquisition_plan()
    artifact = _artifact()
    store = _FakeStore(artifact=artifact)

    plan = await plan_module.plan_adjusted_forecast_seal(
        end_session=END,
        settings=_settings(),
        store_factory=_store_factory(store),
        acquisition_planner=_acquisition_planner(acquisition),
        revision_fn=lambda: TOOL_REVISION,
    )

    assert plan.ready is True
    assert plan.blockers == ()
    assert plan.factor_cutoff == FACTOR_CUTOFF
    assert plan.expected_factor_set_id == artifact.factor_set_id
    assert plan.acquisition_plan_id == ACQUISITION_PLAN_ID
    spec = store.prepared_specs[0]
    assert spec.cutoff == FACTOR_CUTOFF
    assert spec.coverage_start == _expected_session_dates(END)[0]
    public = plan.public_result()
    assert public["status"] == "ready"
    assert public["adjustment_factor_set_format"] == ADJUSTMENT_FACTOR_SET_FORMAT
    assert public["adjustment_factor_policy_hash"] == ADJUSTMENT_FACTOR_POLICY_HASH
    assert public["adjustment_factor_policy_version"] == ADJUSTMENT_FACTOR_POLICY_VERSION
    assert public["split_collection_receipt"] == {
        "action_type": "split",
        "collection_id": SPLIT_COLLECTION_ID,
        "collection_recorded_at": SPLIT_RECORDED_AT.isoformat(),
        "available_at": SPLIT_AVAILABLE_AT.isoformat(),
        "event_count": 0,
    }
    request = cast(dict[str, object], public["request"])
    assert request["method"] == "POST"
    assert request["path"] == "/v1/forecast"
    assert request["target"] == "adjusted_close"
    assert cast(dict[str, object], request["body"])["target"] == "adjusted_close"
    assert request["idempotency_key_derivation_version"] == (
        plan_module.IDEMPOTENCY_KEY_DERIVATION_VERSION
    )
    assert plan.idempotency_key == (
        "stockapi-adjusted-demo-" + plan.plan_id.removeprefix("sha256:")
    )
    assert "local-adjusted-demo-key" not in str(public)
    assert "secret-canary" not in str(public)


@pytest.mark.asyncio
async def test_plan_id_is_stable_across_clock_and_exact_factor_recovery_changes() -> None:
    acquisition = _acquisition_plan()
    artifact = _artifact()
    first = await plan_module.plan_adjusted_forecast_seal(
        end_session=END,
        settings=_settings(),
        store_factory=_store_factory(_FakeStore(artifact=artifact)),
        acquisition_planner=_acquisition_planner(acquisition),
        revision_fn=lambda: TOOL_REVISION,
    )
    recovered_store = _FakeStore(
        artifact=artifact,
        prior=plan_module.PriorFactorState(
            True,
            FINAL_DATABASE_NOW,
            FINAL_DATABASE_NOW + timedelta(seconds=1),
            (),
        ),
        database_times=(
            DATABASE_NOW + timedelta(minutes=5),
            FINAL_DATABASE_NOW + timedelta(minutes=5),
        ),
    )

    async def recovered_acquisition(**kwargs: object) -> AcquisitionPlan:
        del kwargs
        return acquisition

    recovered = await plan_module.plan_adjusted_forecast_seal(
        end_session=END,
        settings=_settings(),
        store_factory=_store_factory(recovered_store),
        acquisition_planner=recovered_acquisition,
        revision_fn=lambda: TOOL_REVISION,
    )

    assert recovered.expected_factor_exists is True
    assert recovered.expected_factor_available_at is not None
    assert recovered.plan_id == first.plan_id
    assert recovered.expected_factor_set_id == first.expected_factor_set_id
    assert recovered.factor_cutoff == first.factor_cutoff


def test_corrected_action_collections_select_newest_recording_then_id() -> None:
    older = _collection(
        "split",
        "sha256:" + "6" * 64,
        SPLIT_RECORDED_AT - timedelta(minutes=1),
        SPLIT_AVAILABLE_AT - timedelta(minutes=1),
    )
    same_time_lower_id = _collection(
        "split",
        "sha256:" + "7" * 64,
        SPLIT_RECORDED_AT,
        SPLIT_AVAILABLE_AT,
    )
    newest = _collection(
        "split",
        "sha256:" + "8" * 64,
        SPLIT_RECORDED_AT,
        SPLIT_AVAILABLE_AT + timedelta(seconds=1),
    )

    selected = plan_module._one_action_binding(
        (newest, older, same_time_lower_id),
        "split",
    )

    assert selected is not None
    assert selected.collection_id == newest.collection_id


@pytest.mark.asyncio
async def test_incomplete_or_incompatible_evidence_blocks_without_inventing_cutoff() -> None:
    acquisition = _acquisition_plan(complete_count=257)
    store = _FakeStore(
        raw_state=plan_module.RawReceiptState(257, RAW_MAX_AVAILABLE_AT),
    )

    blocked = await plan_module.plan_adjusted_forecast_seal(
        end_session=END,
        settings=_settings(),
        store_factory=_store_factory(store),
        acquisition_planner=_acquisition_planner(acquisition),
        revision_fn=lambda: TOOL_REVISION,
    )

    assert blocked.ready is False
    assert blocked.factor_cutoff is None
    assert blocked.expected_factor_set_id is None
    assert store.prepared_specs == []
    assert any("258" in blocker for blocker in blocked.blockers)

    incompatible_id = "sha256:" + "9" * 64
    incompatible_store = _FakeStore(
        prior=plan_module.PriorFactorState(
            False,
            None,
            None,
            (incompatible_id,),
        )
    )
    incompatible = await plan_module.plan_adjusted_forecast_seal(
        end_session=END,
        settings=_settings(),
        store_factory=_store_factory(incompatible_store),
        acquisition_planner=_acquisition_planner(_acquisition_plan()),
        revision_fn=lambda: TOOL_REVISION,
    )
    assert incompatible.ready is False
    assert incompatible.incompatible_factor_set_ids == (incompatible_id,)
    assert any("incompatible" in blocker for blocker in incompatible.blockers)


@pytest.mark.asyncio
async def test_factor_cutoff_is_max_selected_receipt_not_database_clock() -> None:
    artifact = _artifact()
    store = _FakeStore(
        artifact=artifact,
        database_times=(
            DATABASE_NOW + timedelta(hours=1),
            FINAL_DATABASE_NOW + timedelta(hours=1),
        ),
    )

    async def planner(**kwargs: object) -> AcquisitionPlan:
        del kwargs
        return _acquisition_plan()

    result = await plan_module.plan_adjusted_forecast_seal(
        end_session=END,
        settings=_settings(),
        store_factory=_store_factory(store),
        acquisition_planner=planner,
        revision_fn=lambda: TOOL_REVISION,
    )

    assert result.factor_cutoff == max(
        RAW_MAX_AVAILABLE_AT,
        SPLIT_AVAILABLE_AT,
        DIVIDEND_AVAILABLE_AT,
    )
    assert result.factor_cutoff != result.database_now


def test_safe_settings_require_runtime_local_db_v1_and_strip_vendor_secrets() -> None:
    safe = plan_module._safe_settings(_settings())
    assert safe.polygon_api_key is None
    assert safe.database_url == _settings().database_url

    for candidate in (
        _settings(app_env="production"),
        _settings(api_v1_prefix="/api"),
        _settings(
            database_url=(
                "postgresql+asyncpg://stockapi_snapshot_builder:x@127.0.0.1:5432/stockapi_test"
            )
        ),
        _settings(database_url=("postgresql+asyncpg://stockapi_app:x@remote:5432/stockapi_test")),
    ):
        with pytest.raises(plan_module.AdjustedForecastPlanRefused):
            plan_module._safe_settings(candidate)


def test_plan_source_is_read_only_and_has_no_vendor_provider_path() -> None:
    source = Path(plan_module.__file__).read_text(encoding="utf-8")
    assert "insert(" not in source
    assert "update(" not in source
    assert "delete(" not in source
    assert "PolygonOpenCloseProvider" not in source
    assert "provider.get_" not in source
    assert "AdjustmentFactorBuilder(self._maker).prepare(spec)" in source


def test_existing_factor_status_is_excluded_from_canonical_plan_identity() -> None:
    acquisition = _acquisition_plan()
    artifact = _artifact()
    base = plan_module._build_plan(
        settings=_settings(),
        acquisition_plan=acquisition,
        database_now=FINAL_DATABASE_NOW,
        raw_state=plan_module.RawReceiptState(REQUIRED_SESSIONS, RAW_MAX_AVAILABLE_AT),
        split=plan_module._one_action_binding(
            acquisition.split_state.coverage.collections,
            "split",
        ),
        dividend=plan_module._one_action_binding(
            acquisition.dividend_state.coverage.collections,
            "dividend",
        ),
        factor_cutoff=FACTOR_CUTOFF,
        artifact=artifact,
        prior=plan_module.PriorFactorState(False, None, None, ()),
    )
    recovered = plan_module._build_plan(
        settings=_settings(),
        acquisition_plan=acquisition,
        database_now=FINAL_DATABASE_NOW + timedelta(minutes=1),
        raw_state=plan_module.RawReceiptState(REQUIRED_SESSIONS, RAW_MAX_AVAILABLE_AT),
        split=base.split_collection_receipt,
        dividend=base.dividend_collection_receipt,
        factor_cutoff=FACTOR_CUTOFF,
        artifact=artifact,
        prior=plan_module.PriorFactorState(
            True,
            FINAL_DATABASE_NOW,
            FINAL_DATABASE_NOW + timedelta(seconds=1),
            (),
        ),
    )

    assert base.plan_id == recovered.plan_id
    assert base.expected_factor_exists is False
    assert recovered.expected_factor_exists is True


def test_code_policy_constants_are_bound_to_the_plan_contract() -> None:
    request = plan_module._public_request()
    assert request["target"] == "adjusted_close"
    assert request["idempotency_key_input"] == "plan_id_hex"
    assert ADJUSTMENT_FACTOR_SET_FORMAT == "stockapi-adjustment-factor-set-v1"
    assert ADJUSTMENT_FACTOR_POLICY_HASH.startswith("sha256:")
    assert ADJUSTED_RESOLUTION_POLICY_HASH.startswith("sha256:")
    assert ADJUSTED_AVAILABILITY_RULE_SET_HASH.startswith("sha256:")


@pytest.mark.asyncio
async def test_backfill_failures_are_classified_at_the_plan_boundary() -> None:
    def revision_refused() -> str:
        raise BackfillRefused("revision-secret-canary")

    with pytest.raises(plan_module.AdjustedForecastPlanRefused, match="revision-secret-canary"):
        await plan_module.plan_adjusted_forecast_seal(
            end_session=END,
            settings=_settings(),
            revision_fn=revision_refused,
        )

    async def acquisition_refused(**kwargs: object) -> AcquisitionPlan:
        del kwargs
        raise BackfillRefused("acquisition-secret-canary")

    with pytest.raises(
        plan_module.AdjustedForecastPlanRefused,
        match="acquisition-secret-canary",
    ):
        await plan_module.plan_adjusted_forecast_seal(
            end_session=END,
            settings=_settings(),
            store_factory=_store_factory(_FakeStore()),
            acquisition_planner=acquisition_refused,
            revision_fn=lambda: TOOL_REVISION,
        )
