"""Fail-closed tests for the isolated adjusted snapshot sealing command."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import ingestion.tasks.seal_adjusted_forecast_snapshot as seal
from app.config import Settings
from app.services.adjusted_forecast_snapshot_builder import (
    ADJUSTED_AVAILABILITY_RULE_SET_HASH,
    ADJUSTED_RESOLUTION_POLICY_HASH,
    DEFAULT_ADJUSTED_SNAPSHOT_BUILD_POLICY,
    AdjustedSnapshotBuildSpec,
)
from app.services.adjustment_factor_builder import AdjustmentFactorBuildSpec
from app.services.forecast_snapshot_builder import SnapshotBuildResult

END = date(2026, 7, 13)
FACTOR_CUTOFF = datetime(2026, 7, 13, 21, 0, tzinfo=UTC)
PREFLIGHT_NOW = FACTOR_CUTOFF + timedelta(seconds=30)
FACTOR_RECORDED_AT = FACTOR_CUTOFF + timedelta(minutes=1)
FACTOR_AVAILABLE_AT = FACTOR_CUTOFF + timedelta(minutes=2)
DATABASE_NOW = FACTOR_CUTOFF + timedelta(minutes=3)
SNAPSHOT_CHECKED_AT = DATABASE_NOW + timedelta(seconds=1)
REVISION = "a" * 40
FACTOR_SET_ID = "sha256:" + "b" * 64
SNAPSHOT_ID = "sha256:" + "c" * 64


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "app_env": "local",
        "database_url": (
            "postgresql+asyncpg://stockapi_snapshot_builder:secret-canary@"
            "timescaledb:5432/stockapi_test"
        ),
        "forecast_adjusted_close_resolution_policy_hash": (ADJUSTED_RESOLUTION_POLICY_HASH),
        "forecast_adjusted_close_trusted_availability_rule_set_hash": (
            ADJUSTED_AVAILABILITY_RULE_SET_HASH
        ),
        # An ambient key must neither be consumed nor appear in output.  This
        # command has no provider construction or vendor-call path.
        "polygon_api_key": "vendor-secret-canary",
    }
    values.update(overrides)
    return Settings(**values)  # type: ignore[arg-type]


def _factor_result(*, available_at: datetime = FACTOR_AVAILABLE_AT) -> SimpleNamespace:
    dates = seal._coverage_dates(END)
    artifact = SimpleNamespace(
        symbol="MSFT",
        cutoff=FACTOR_CUTOFF,
        anchor_date=END,
        raw_inputs=tuple(SimpleNamespace(observation_date=value) for value in dates),
        factor_set_id=FACTOR_SET_ID,
    )
    publication = SimpleNamespace(
        factor_set_id=FACTOR_SET_ID,
        factor_set_recorded_at=FACTOR_RECORDED_AT,
        available_at=available_at,
        input_count=seal.REQUIRED_SESSIONS,
    )
    return SimpleNamespace(artifact=artifact, publication=publication)


def _snapshot_result(*, created: bool) -> SnapshotBuildResult:
    return SnapshotBuildResult(
        snapshot_id=SNAPSHOT_ID,
        as_of=FACTOR_AVAILABLE_AT,
        availability_checked_at=SNAPSHOT_CHECKED_AT,
        observation_count=seal.REQUIRED_SESSIONS,
        target_time_count=DEFAULT_ADJUSTED_SNAPSHOT_BUILD_POLICY.target_time_count,
        created=created,
    )


class _FakeEngine:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def dispose(self) -> None:
        self.events.append("dispose")


class _SealHarness:
    def __init__(
        self,
        *,
        preflight_now: datetime = PREFLIGHT_NOW,
        post_publication_now: datetime = DATABASE_NOW,
        factor_result: SimpleNamespace | None = None,
        snapshot_created: bool = True,
    ) -> None:
        self.events: list[str] = []
        self.database_clocks = [preflight_now, post_publication_now]
        self.factor_result = factor_result or _factor_result()
        self.snapshot_result = _snapshot_result(created=snapshot_created)
        self.factor_specs: list[AdjustmentFactorBuildSpec] = []
        self.snapshot_specs: list[AdjustedSnapshotBuildSpec] = []
        self.lineage_checks: list[dict[str, object]] = []
        self.maker = object()
        self.engine = _FakeEngine(self.events)

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        harness = self

        def fake_build_engine(settings: Settings) -> _FakeEngine:
            assert settings is not None
            harness.events.append("engine")
            return harness.engine

        def fake_build_sessionmaker(engine: _FakeEngine) -> object:
            assert engine is harness.engine
            harness.events.append("maker")
            return harness.maker

        class FakeStore:
            def __init__(self, engine: _FakeEngine) -> None:
                assert engine is harness.engine
                harness.events.append("factor_store")

        class FakeFactorBuilder:
            def __init__(self, maker: object, publisher: object) -> None:
                assert maker is harness.maker
                assert isinstance(publisher, FakeStore)
                harness.events.append("factor_builder")

            async def prepare(self, spec: AdjustmentFactorBuildSpec) -> SimpleNamespace:
                harness.factor_specs.append(spec)
                harness.events.append("factor_prepare")
                return harness.factor_result.artifact

            async def publish(self, artifact: object) -> SimpleNamespace:
                assert artifact is harness.factor_result.artifact
                harness.events.append("factor_publish")
                return harness.factor_result

        async def fake_database_clock(maker: object) -> datetime:
            assert maker is harness.maker
            harness.events.append("database_clock")
            return harness.database_clocks.pop(0)

        class FakeAdjustedBuilder:
            def __init__(self, maker: object) -> None:
                assert maker is harness.maker
                harness.events.append("snapshot_builder")

            async def build(self, spec: AdjustedSnapshotBuildSpec) -> SnapshotBuildResult:
                harness.snapshot_specs.append(spec)
                harness.events.append("snapshot_build")
                return harness.snapshot_result

        async def fake_lineage(maker: object, **kwargs: object) -> None:
            assert maker is harness.maker
            harness.events.append("lineage")
            harness.lineage_checks.append(kwargs)

        monkeypatch.setattr(seal, "build_engine", fake_build_engine)
        monkeypatch.setattr(seal, "build_sessionmaker", fake_build_sessionmaker)
        monkeypatch.setattr(seal, "SqlAdjustmentFactorSetStore", FakeStore)
        monkeypatch.setattr(seal, "AdjustmentFactorBuilder", FakeFactorBuilder)
        monkeypatch.setattr(seal, "database_snapshot_cutoff", fake_database_clock)
        monkeypatch.setattr(seal, "AdjustedForecastSnapshotBuilder", FakeAdjustedBuilder)
        monkeypatch.setattr(seal, "_require_exact_factor_lineage", fake_lineage)


def test_builder_settings_require_exact_local_role_and_adjusted_policy_pins() -> None:
    settings = _settings()
    assert seal._safe_settings(settings) is settings

    bad_settings = (
        _settings(app_env="production"),
        _settings(
            database_url=("postgresql+asyncpg://stockapi_app:x@timescaledb:5432/stockapi_test")
        ),
        _settings(
            database_url=(
                "postgresql+asyncpg://stockapi_snapshot_builder:x@localhost:5432/stockapi_test"
            )
        ),
        _settings(forecast_adjusted_close_resolution_policy_hash=None),
        _settings(forecast_adjusted_close_trusted_availability_rule_set_hash=None),
    )
    for candidate in bad_settings:
        with pytest.raises(seal.OneShotAdjustedSealRefused):
            seal._safe_settings(candidate)


def test_reviewed_revision_attestation_must_exist_and_match(tmp_path: Path) -> None:
    revision_file = tmp_path / "revision"
    revision_file.write_text(REVISION + "\n", encoding="ascii")
    seal._attest_build_revision(REVISION, revision_file)

    with pytest.raises(seal.OneShotAdjustedSealRefused, match="differs"):
        seal._attest_build_revision("b" * 40, revision_file)
    with pytest.raises(seal.OneShotAdjustedSealRefused, match="no trusted revision"):
        seal._attest_build_revision(REVISION, tmp_path / "missing")
    with pytest.raises(seal.OneShotAdjustedSealRefused, match="reviewed Git commit"):
        seal._attest_build_revision("not-a-revision", revision_file)


def test_scope_is_exact_258_session_msft_window_and_has_no_automation_path() -> None:
    dates = seal._coverage_dates(END)
    assert len(dates) == seal.REQUIRED_SESSIONS
    assert dates[-1] == END
    assert dates == tuple(sorted(set(dates)))

    source = Path(seal.__file__).read_text(encoding="utf-8")
    assert "@celery_app.task" not in source
    assert "send_task(" not in source
    assert "from data_sources" not in source
    assert "Polygon" not in source


@pytest.mark.asyncio
async def test_seal_binds_factor_cutoff_receipt_as_of_and_exact_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _SealHarness()
    harness.install(monkeypatch)

    result = await seal.seal_once(
        factor_cutoff=FACTOR_CUTOFF,
        expected_factor_set_id=FACTOR_SET_ID,
        end_session=END,
        authorization=seal.AUTHORIZATION_SENTINEL,
        settings=_settings(),
    )

    factor_spec = harness.factor_specs[0]
    assert factor_spec.symbol == "MSFT"
    assert factor_spec.coverage_start == seal._coverage_dates(END)[0]
    assert factor_spec.coverage_end == END
    assert factor_spec.cutoff == FACTOR_CUTOFF
    snapshot_spec = harness.snapshot_specs[0]
    assert snapshot_spec.symbol == "MSFT"
    assert snapshot_spec.target == "adjusted_close"
    assert snapshot_spec.horizon_unit == "trading_day"
    assert snapshot_spec.as_of == FACTOR_AVAILABLE_AT
    assert harness.lineage_checks == [
        {
            "snapshot_id": SNAPSHOT_ID,
            "snapshot_as_of": FACTOR_AVAILABLE_AT,
            "factor_set_id": FACTOR_SET_ID,
            "factor_available_at": FACTOR_AVAILABLE_AT,
        }
    ]
    assert harness.events == [
        "engine",
        "maker",
        "database_clock",
        "factor_store",
        "factor_builder",
        "factor_prepare",
        "factor_publish",
        "database_clock",
        "snapshot_builder",
        "snapshot_build",
        "lineage",
        "dispose",
    ]
    assert result["factor_cutoff"] == "2026-07-13T21:00:00.000000Z"
    assert result["snapshot_as_of"] == "2026-07-13T21:02:00.000000Z"
    assert result["factor_set_id"] == FACTOR_SET_ID
    assert result["factor_set_recorded_at"] == "2026-07-13T21:01:00.000000Z"
    assert result["snapshot_id"] == SNAPSHOT_ID
    assert result["snapshot_status"] == "created"
    assert "secret-canary" not in json.dumps(result)
    assert "vendor-secret-canary" not in json.dumps(result)


@pytest.mark.asyncio
async def test_retry_replays_exact_factor_and_snapshot_identities(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _SealHarness(snapshot_created=True)
    first.install(monkeypatch)
    created = await seal.seal_once(
        factor_cutoff=FACTOR_CUTOFF,
        expected_factor_set_id=FACTOR_SET_ID,
        end_session=END,
        authorization=seal.AUTHORIZATION_SENTINEL,
        settings=_settings(),
    )

    retry = _SealHarness(snapshot_created=False)
    retry.install(monkeypatch)
    replayed = await seal.seal_once(
        factor_cutoff=FACTOR_CUTOFF,
        expected_factor_set_id=FACTOR_SET_ID,
        end_session=END,
        authorization=seal.AUTHORIZATION_SENTINEL,
        settings=_settings(),
    )

    assert created["factor_set_id"] == replayed["factor_set_id"] == FACTOR_SET_ID
    assert created["snapshot_id"] == replayed["snapshot_id"] == SNAPSHOT_ID
    assert created["snapshot_as_of"] == replayed["snapshot_as_of"]
    assert created["snapshot_status"] == "created"
    assert replayed["snapshot_status"] == "replayed"
    assert first.factor_specs[0].cutoff == retry.factor_specs[0].cutoff
    assert first.snapshot_specs[0].as_of == retry.snapshot_specs[0].as_of


@pytest.mark.asyncio
async def test_prepared_factor_must_match_plan_before_any_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factor_result = _factor_result()
    factor_result.artifact.factor_set_id = "sha256:" + "d" * 64
    harness = _SealHarness(factor_result=factor_result)
    harness.install(monkeypatch)

    with pytest.raises(seal.OneShotAdjustedSealRefused, match="differs from the read-only plan"):
        await seal.seal_once(
            factor_cutoff=FACTOR_CUTOFF,
            expected_factor_set_id=FACTOR_SET_ID,
            end_session=END,
            authorization=seal.AUTHORIZATION_SENTINEL,
            settings=_settings(),
        )

    assert "factor_prepare" in harness.events
    assert "factor_publish" not in harness.events
    assert "snapshot_build" not in harness.events
    assert harness.events[-1] == "dispose"


@pytest.mark.asyncio
async def test_persisted_snapshot_must_name_exact_factor_and_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical = b"canonical-adjusted-snapshot"
    factor_source = SimpleNamespace(
        name="stockapi_adjustment_factors",
        snapshot_id=FACTOR_SET_ID,
        max_available_at=FACTOR_AVAILABLE_AT,
        fields=("adjusted_close", "price_factor_f64"),
    )
    payload = SimpleNamespace(
        data_sources=(factor_source,),
        resolution_policy_hash=ADJUSTED_RESOLUTION_POLICY_HASH,
        symbol="MSFT",
        target="adjusted_close",
        horizon_unit="trading_day",
        series_basis=DEFAULT_ADJUSTED_SNAPSHOT_BUILD_POLICY.series_basis,
        as_of=FACTOR_AVAILABLE_AT,
        availability=SimpleNamespace(rule_set_hash=ADJUSTED_AVAILABILITY_RULE_SET_HASH),
    )

    class Result:
        def scalar_one_or_none(self) -> SimpleNamespace:
            return SimpleNamespace(canonical_payload=canonical)

    class Session:
        async def __aenter__(self) -> Session:
            return self

        async def __aexit__(self, *args: object) -> None:
            del args

        async def execute(self, statement: object) -> Result:
            assert statement is not None
            return Result()

    class Maker:
        def __call__(self) -> Session:
            return Session()

    monkeypatch.setattr(seal, "parse_snapshot_payload", lambda value: payload)
    monkeypatch.setattr(seal, "canonical_snapshot_payload", lambda value: canonical)
    monkeypatch.setattr(seal, "snapshot_id_for_payload", lambda value: SNAPSHOT_ID)
    maker = cast(async_sessionmaker[AsyncSession], Maker())

    await seal._require_exact_factor_lineage(
        maker,
        snapshot_id=SNAPSHOT_ID,
        snapshot_as_of=FACTOR_AVAILABLE_AT,
        factor_set_id=FACTOR_SET_ID,
        factor_available_at=FACTOR_AVAILABLE_AT,
    )
    factor_source.snapshot_id = "sha256:" + "d" * 64
    with pytest.raises(seal.OneShotAdjustedSealRefused, match="exact published factor"):
        await seal._require_exact_factor_lineage(
            maker,
            snapshot_id=SNAPSHOT_ID,
            snapshot_as_of=FACTOR_AVAILABLE_AT,
            factor_set_id=FACTOR_SET_ID,
            factor_available_at=FACTOR_AVAILABLE_AT,
        )


def test_session_freshness_and_rollover_are_checked_at_both_boundaries() -> None:
    seal._require_current_session(FACTOR_CUTOFF, END, phase="planned factor cutoff")

    with pytest.raises(seal.OneShotAdjustedSealRefused, match="newer XNYS session"):
        seal._require_current_session(
            datetime(2026, 7, 14, 21, tzinfo=UTC),
            END,
            phase="post-publication visibility check",
        )
    with pytest.raises(seal.OneShotAdjustedSealRefused, match="too little time"):
        seal._require_current_session(
            datetime(2026, 7, 14, 19, 55, 1, tzinfo=UTC),
            END,
            phase="post-publication visibility check",
        )


@pytest.mark.asyncio
async def test_later_database_clock_and_publication_visibility_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    not_later = _SealHarness(
        preflight_now=DATABASE_NOW,
        post_publication_now=DATABASE_NOW,
    )
    not_later.install(monkeypatch)
    with pytest.raises(seal.OneShotAdjustedSealRefused, match="did not advance"):
        await seal.seal_once(
            factor_cutoff=FACTOR_CUTOFF,
            expected_factor_set_id=FACTOR_SET_ID,
            end_session=END,
            authorization=seal.AUTHORIZATION_SENTINEL,
            settings=_settings(),
        )
    assert "snapshot_build" not in not_later.events
    assert not_later.events[-1] == "dispose"

    late_receipt = _SealHarness(
        factor_result=_factor_result(available_at=DATABASE_NOW + timedelta(seconds=1))
    )
    late_receipt.install(monkeypatch)
    with pytest.raises(seal.OneShotAdjustedSealRefused, match="not visible"):
        await seal.seal_once(
            factor_cutoff=FACTOR_CUTOFF,
            expected_factor_set_id=FACTOR_SET_ID,
            end_session=END,
            authorization=seal.AUTHORIZATION_SENTINEL,
            settings=_settings(),
        )
    assert "snapshot_build" not in late_receipt.events
    assert late_receipt.events[-1] == "dispose"


@pytest.mark.asyncio
async def test_stale_end_or_future_cutoff_refuses_before_factor_publication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stale = _SealHarness(
        preflight_now=datetime(2026, 7, 14, 21, tzinfo=UTC),
        post_publication_now=datetime(2026, 7, 14, 21, 1, tzinfo=UTC),
    )
    stale.install(monkeypatch)
    with pytest.raises(seal.OneShotAdjustedSealRefused, match="newer XNYS session"):
        await seal.seal_once(
            factor_cutoff=FACTOR_CUTOFF,
            expected_factor_set_id=FACTOR_SET_ID,
            end_session=END,
            authorization=seal.AUTHORIZATION_SENTINEL,
            settings=_settings(),
        )
    assert "factor_builder" not in stale.events
    assert "factor_prepare" not in stale.events
    assert "factor_publish" not in stale.events
    assert stale.events[-1] == "dispose"

    future = _SealHarness(
        preflight_now=FACTOR_CUTOFF - timedelta(microseconds=1),
        post_publication_now=DATABASE_NOW,
    )
    future.install(monkeypatch)
    with pytest.raises(seal.OneShotAdjustedSealRefused, match="later than the preflight"):
        await seal.seal_once(
            factor_cutoff=FACTOR_CUTOFF,
            expected_factor_set_id=FACTOR_SET_ID,
            end_session=END,
            authorization=seal.AUTHORIZATION_SENTINEL,
            settings=_settings(),
        )
    assert "factor_builder" not in future.events
    assert "factor_prepare" not in future.events
    assert "factor_publish" not in future.events
    assert future.events[-1] == "dispose"


@pytest.mark.asyncio
async def test_wrong_authorization_refuses_before_database_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    def forbidden_engine(settings: Settings) -> object:
        nonlocal called
        del settings
        called = True
        raise AssertionError("database construction must not happen")

    monkeypatch.setattr(seal, "build_engine", forbidden_engine)
    with pytest.raises(seal.OneShotAdjustedSealRefused, match="authorization"):
        await seal.seal_once(
            factor_cutoff=FACTOR_CUTOFF,
            expected_factor_set_id=FACTOR_SET_ID,
            end_session=END,
            authorization="not-authorized",
            settings=_settings(),
        )
    assert called is False


def test_cli_emits_one_sanitized_json_line_and_hides_library_output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(seal, "_attest_build_revision", lambda revision: None)

    async def successful(**kwargs: object) -> dict[str, object]:
        assert kwargs["factor_cutoff"] == FACTOR_CUTOFF
        assert kwargs["expected_factor_set_id"] == FACTOR_SET_ID
        print("vendor-secret-canary")
        return {"status": "ok", "snapshot_id": SNAPSHOT_ID}

    monkeypatch.setattr(seal, "seal_once", successful)
    exit_code = seal.main(
        [
            "--end",
            END.isoformat(),
            "--factor-cutoff",
            FACTOR_CUTOFF.isoformat(),
            "--expected-factor-set-id",
            FACTOR_SET_ID,
            "--tool-revision",
            REVISION,
            "--authorization",
            seal.AUTHORIZATION_SENTINEL,
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert json.loads(captured.out) == {"snapshot_id": SNAPSHOT_ID, "status": "ok"}
    assert captured.err == ""
    assert "secret-canary" not in captured.out


def test_cli_never_echoes_unexpected_exception_text(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(seal, "_attest_build_revision", lambda revision: None)

    async def explode(**kwargs: object) -> dict[str, object]:
        del kwargs
        raise RuntimeError("postgresql://builder:secret-canary@timescaledb")

    monkeypatch.setattr(seal, "seal_once", explode)
    exit_code = seal.main(
        [
            "--end",
            END.isoformat(),
            "--factor-cutoff",
            FACTOR_CUTOFF.isoformat(),
            "--expected-factor-set-id",
            FACTOR_SET_ID,
            "--tool-revision",
            REVISION,
            "--authorization",
            seal.AUTHORIZATION_SENTINEL,
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "RuntimeError" in captured.err
    assert "secret-canary" not in captured.err
    assert captured.out == ""


def test_snapshot_lineage_verifier_is_not_optional_in_source() -> None:
    source = Path(seal.__file__).read_text(encoding="utf-8")
    assert "await _require_exact_factor_lineage(" in source
    assert 'source.name == "stockapi_adjustment_factors"' in source
    assert "factor_sources[0].snapshot_id != factor_set_id" in source
    assert "factor_sources[0].max_available_at != factor_available_at" in source


def test_helpers_reject_naive_factor_cutoff_and_non_session_end() -> None:
    with pytest.raises(seal.OneShotAdjustedSealRefused, match="timezone-aware"):
        seal._aware(datetime(2026, 7, 13, 21), "factor_cutoff")
    with pytest.raises(seal.OneShotAdjustedSealRefused, match="trading session"):
        seal._coverage_dates(date(2026, 7, 12))


def test_module_exports_only_an_explicit_one_shot_contract() -> None:
    assert seal.SYMBOL == "MSFT"
    assert seal.AUTHORIZATION_SENTINEL == "stockapi-msft-adjusted-seal-only"
    assert seal.REQUIRED_SESSIONS == 258
    assert not hasattr(seal, "celery_app")
    assert not hasattr(seal, "provider")
