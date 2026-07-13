"""Fail-closed tests for the local MSFT snapshot seal-and-serve operator."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

import scripts.forecast_demo as demo
from app.config import Settings
from app.services.forecast_snapshot_builder import (
    DEFAULT_AVAILABILITY_RULE_SET_HASH,
    DEFAULT_RESOLUTION_POLICY_HASH,
    DEFAULT_SNAPSHOT_BUILD_POLICY,
)
from ingestion.tasks.seal_forecast_demo_snapshot import (
    OneShotSealRefused,
    _attest_build_revision,
)
from ingestion.tasks.seal_forecast_demo_snapshot import (
    _safe_settings as safe_builder_settings,
)
from scripts.vendor_backfill import BackfillPlan, _expected_session_dates

END = date(2026, 7, 10)
DATABASE_NOW = datetime(2026, 7, 13, 12, tzinfo=UTC)
REVISION = "a" * 40
ATTESTATION = demo.RuntimeImageAttestation(
    tool_revision=REVISION,
    api_image_id="sha256:" + "1" * 64,
    builder_image_id="sha256:" + "2" * 64,
    api_container_id="3" * 64,
)


def _runtime_attestor(tool_revision: str) -> demo.RuntimeImageAttestation:
    assert tool_revision == REVISION
    return ATTESTATION


def _runtime_revalidator(attestation: demo.RuntimeImageAttestation) -> None:
    assert attestation == ATTESTATION


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "app_env": "local",
        "database_url": (
            "postgresql+asyncpg://stockapi_app:test-secret@localhost:5432/stockapi_test"
        ),
        "celery_broker_url": "redis://localhost:6380/0",
        "celery_result_backend": "redis://localhost:6380/1",
        "api_keys": "local-demo-key",
        "jwt_secret": "test-jwt-binding-secret-32-characters-long",
        "forecast_resolution_policy_hash": DEFAULT_RESOLUTION_POLICY_HASH,
        "forecast_trusted_availability_rule_set_hash": (DEFAULT_AVAILABILITY_RULE_SET_HASH),
    }
    values.update(overrides)
    return Settings(**values)  # type: ignore[arg-type]


def _backfill_plan(*, complete: bool = True, plan_id: str | None = None) -> BackfillPlan:
    expected = _expected_session_dates(END)
    complete_dates = expected if complete else expected[:-1]
    missing = () if complete else (expected[-1],)
    versions = tuple((value, f"sha256:{index:064x}") for index, value in enumerate(complete_dates))
    return BackfillPlan(
        end_session=END,
        tool_revision=REVISION,
        expected_dates=expected,
        complete_dates=complete_dates,
        repairable_dates=(),
        missing_dates=missing,
        version_ids=versions,
        ambiguous_dates=(),
        plan_id=plan_id or "sha256:" + "b" * 64,
    )


def _state(*, count: int = 258, cutoff: datetime | None = DATABASE_NOW):
    return demo.ForecastDemoDatabaseState(
        database_now=DATABASE_NOW,
        exact_receipt_rows=count,
        stable_cutoff=cutoff,
    )


def _ready_plan() -> demo.ForecastDemoPlan:
    return demo._build_demo_plan(
        settings=_settings(),
        backfill_plan=_backfill_plan(),
        state=_state(),
        final_database_now=DATABASE_NOW,
    )


def _task_result(*, status: str = "created") -> dict[str, object]:
    return {
        "status": "ok",
        "as_of": DATABASE_NOW.isoformat(),
        "resolution_policy_hash": DEFAULT_RESOLUTION_POLICY_HASH,
        "availability_rule_set_hash": DEFAULT_AVAILABILITY_RULE_SET_HASH,
        "created": int(status == "created"),
        "replayed": int(status == "replayed"),
        "deferred": 0,
        "failed": 0,
        "per_symbol": [
            {
                "symbol": "MSFT",
                "status": status,
                "snapshot_id": "sha256:" + "c" * 64,
                "observations": 258,
                "target_times": DEFAULT_SNAPSHOT_BUILD_POLICY.target_time_count,
            }
        ],
    }


def test_ready_plan_binds_exact_backfill_configuration_and_request() -> None:
    plan = _ready_plan()

    assert plan.ready is True
    public = plan.public_result()
    assert public["status"] == "ready"
    assert public["complete_sessions"] == 258
    assert public["exact_receipt_rows"] == 258
    assert public["stable_cutoff"] == DATABASE_NOW.isoformat()
    assert public["request"] == {
        "method": "GET",
        "origin": "http://127.0.0.1:8000",
        "path": "/v1/forecast/MSFT",
        "horizon": 5,
        "horizon_unit": "trading_day",
        "target": "close",
        "model": "baseline_naive",
        "coverage": 0.8,
        "authentication": "X-API-Key",
    }
    assert "local-demo-key" not in str(public)


@pytest.mark.parametrize(
    ("settings", "backfill", "state", "blocker"),
    [
        (_settings(api_keys=""), _backfill_plan(), _state(), "API_KEYS"),
        (
            _settings(jwt_secret="change_me_random_64_chars"),
            _backfill_plan(),
            _state(),
            "JWT_SECRET",
        ),
        (
            _settings(forecast_resolution_policy_hash="sha256:" + "0" * 64),
            _backfill_plan(),
            _state(),
            "resolution-policy",
        ),
        (_settings(), _backfill_plan(complete=False), _state(count=257), "backfill"),
        (_settings(), _backfill_plan(), _state(cutoff=None), "cutoff"),
    ],
)
def test_plan_blocks_incomplete_auth_policy_or_database_state(
    settings: Settings,
    backfill: BackfillPlan,
    state: demo.ForecastDemoDatabaseState,
    blocker: str,
) -> None:
    plan = demo._build_demo_plan(
        settings=settings,
        backfill_plan=backfill,
        state=state,
        final_database_now=DATABASE_NOW,
    )

    assert plan.ready is False
    assert any(blocker in value for value in plan.blockers)


def test_missing_api_key_does_not_misreport_an_already_valid_jwt_secret() -> None:
    plan = demo._build_demo_plan(
        settings=_settings(api_keys=""),
        backfill_plan=_backfill_plan(),
        state=_state(),
        final_database_now=DATABASE_NOW,
    )

    assert any("API_KEYS" in blocker for blocker in plan.blockers)
    assert not any("JWT_SECRET" in blocker for blocker in plan.blockers)


def test_plan_id_changes_with_cutoff_backfill_or_pin_state() -> None:
    baseline = _ready_plan().plan_id
    changed_cutoff = demo._build_demo_plan(
        settings=_settings(),
        backfill_plan=_backfill_plan(),
        state=_state(cutoff=DATABASE_NOW.replace(microsecond=1)),
        final_database_now=DATABASE_NOW.replace(microsecond=1),
    ).plan_id
    changed_backfill = demo._build_demo_plan(
        settings=_settings(),
        backfill_plan=_backfill_plan(plan_id="sha256:" + "d" * 64),
        state=_state(),
        final_database_now=DATABASE_NOW,
    ).plan_id
    changed_pin = demo._build_demo_plan(
        settings=_settings(forecast_resolution_policy_hash="sha256:" + "0" * 64),
        backfill_plan=_backfill_plan(),
        state=_state(),
        final_database_now=DATABASE_NOW,
    ).plan_id
    changed_api_key = demo._build_demo_plan(
        settings=_settings(api_keys="rotated-local-demo-key"),
        backfill_plan=_backfill_plan(),
        state=_state(),
        final_database_now=DATABASE_NOW,
    ).plan_id

    assert len({baseline, changed_cutoff, changed_backfill, changed_pin, changed_api_key}) == 5


@pytest.mark.parametrize(
    "overrides",
    [
        {"app_env": "production"},
        {"database_url": "postgresql+asyncpg://stockapi_app:x@remote:5432/stockapi_test"},
        {"celery_broker_url": "redis://localhost:6379/0"},
        {"celery_result_backend": "redis://localhost:6380/0"},
        {"api_v1_prefix": "/api"},
        {"api_keys": "bad key"},
    ],
)
def test_host_settings_are_hard_bound_to_local_nonsecret_contract(
    overrides: dict[str, object],
) -> None:
    with pytest.raises((demo.ForecastDemoRefused, RuntimeError)):
        demo._safe_demo_settings(_settings(**overrides))


def test_minimal_environment_never_loads_vendor_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in demo._VENDOR_SECRET_VARIABLES:
        monkeypatch.setenv(name, f"ambient-{name.lower()}")
    environment = demo.ForecastDemoEnvironment(
        database_url=_settings().database_url,
        api_keys="local-demo-key",
        jwt_secret="test-jwt-binding-secret-32-characters-long",
        forecast_resolution_policy_hash=DEFAULT_RESOLUTION_POLICY_HASH,
        forecast_trusted_availability_rule_set_hash=DEFAULT_AVAILABILITY_RULE_SET_HASH,
        polygon_api_key="must-be-ignored",
    )

    runtime = environment.runtime_settings()
    assert runtime.polygon_api_key is None
    assert runtime.fmp_api_key is None
    assert runtime.finnhub_api_key is None
    assert runtime.nasdaq_data_link_api_key is None
    assert runtime.alpaca_api_key is None
    assert runtime.alpaca_api_secret is None
    assert runtime.databento_api_key is None
    assert runtime.api_keys == "local-demo-key"


@pytest.mark.asyncio
async def test_loopback_http_disables_ambient_proxy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://proxy.invalid:9999")
    captured: dict[str, object] = {}

    class FakeClient:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc_info: object) -> None:
            return None

        async def get(self, *args: object, **kwargs: object):
            del args, kwargs
            return httpx.Response(200, content=b"ok")

    monkeypatch.setattr(demo.httpx, "AsyncClient", FakeClient)
    result = await demo._default_http_get("/healthz", (), None)

    assert result.status_code == 200
    assert captured["trust_env"] is False


def test_docker_subprocess_environment_scrubs_scope_wideners_and_vendor_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("COMPOSE_FILE", "outside.yml")
    monkeypatch.setenv("COMPOSE_ENV_FILES", "outside.env")
    monkeypatch.setenv("COMPOSE_DISABLE_ENV_FILE", "1")
    monkeypatch.setenv("DOCKER_CONTEXT", "remote")
    monkeypatch.setenv("DOCKER_HOST", "tcp://remote:2375")
    monkeypatch.setenv("STOCKAPI_API_IMAGE", "remote-image")
    monkeypatch.setenv("STOCKAPI_SNAPSHOT_BUILDER_IMAGE", "remote-builder")
    monkeypatch.setenv("POLYGON_API_KEY", "vendor-secret")
    monkeypatch.setenv("FMP_API_KEY", "other-secret")

    environment = demo._sanitized_subprocess_environment()

    assert "COMPOSE_FILE" not in environment
    assert "COMPOSE_ENV_FILES" not in environment
    assert "COMPOSE_DISABLE_ENV_FILE" not in environment
    assert "DOCKER_CONTEXT" not in environment
    assert "DOCKER_HOST" not in environment
    assert "STOCKAPI_API_IMAGE" not in environment
    assert "STOCKAPI_SNAPSHOT_BUILDER_IMAGE" not in environment
    assert "POLYGON_API_KEY" not in environment
    assert "FMP_API_KEY" not in environment


def test_execute_image_attestation_is_mandatory_and_binds_immutable_images(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        demo._ATTESTED_REVISION_ENV,
        demo._ATTESTED_API_IMAGE_ENV,
        demo._ATTESTED_BUILDER_IMAGE_ENV,
        demo._ATTESTED_API_CONTAINER_ENV,
    ):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(demo.ForecastDemoRefused, match="immutable image attestation"):
        demo._attest_runtime_images(REVISION)

    monkeypatch.setenv(demo._ATTESTED_REVISION_ENV, REVISION)
    monkeypatch.setenv(demo._ATTESTED_API_IMAGE_ENV, ATTESTATION.api_image_id)
    monkeypatch.setenv(demo._ATTESTED_BUILDER_IMAGE_ENV, ATTESTATION.builder_image_id)
    monkeypatch.setenv(demo._ATTESTED_API_CONTAINER_ENV, ATTESTATION.api_container_id)
    monkeypatch.setattr(demo, "_validate_local_docker", lambda environment: None)
    monkeypatch.setattr(
        demo,
        "_api_container_facts",
        lambda environment: (ATTESTATION.api_container_id, ATTESTATION.api_image_id),
    )
    monkeypatch.setattr(demo, "_image_revision", lambda image_id, environment: REVISION)

    assert demo._attest_runtime_images(REVISION) == ATTESTATION


def test_api_revalidation_rejects_container_replacement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        demo,
        "_api_container_facts",
        lambda environment: ("4" * 64, ATTESTATION.api_image_id),
    )
    monkeypatch.setattr(demo, "_image_revision", lambda image_id, environment: REVISION)

    with pytest.raises(demo.ForecastDemoRefused, match="changed"):
        demo._revalidate_api_container(ATTESTATION)


@pytest.mark.parametrize(
    "facts",
    [
        f"{'3' * 64}|{ATTESTATION.api_image_id}|true|other-project|api|0",
        f"{'3' * 64}|{ATTESTATION.api_image_id}|true|stock-api|worker|0",
        f"{'3' * 64}|{ATTESTATION.api_image_id}|true|stock-api|api|1",
    ],
)
def test_api_container_scope_rejects_wrong_project_service_or_mount(
    monkeypatch: pytest.MonkeyPatch,
    facts: str,
) -> None:
    monkeypatch.setattr(
        demo,
        "_run_docker",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=facts + "\n"),
    )

    with pytest.raises(demo.ForecastDemoRefused, match="escaped its fixed scope"):
        demo._api_container_facts({})


def test_one_shot_cleanup_removes_only_the_inspected_immutable_container_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan_id = "sha256:" + "a" * 64
    container_id = "b" * 64
    calls: list[tuple[str, ...]] = []

    def run_docker(arguments: object, **kwargs: object):
        del kwargs
        call = tuple(arguments)  # type: ignore[arg-type]
        calls.append(call)
        if call[0] == "inspect":
            return SimpleNamespace(returncode=0, stdout=f"{container_id}|{plan_id}\n")
        return SimpleNamespace(returncode=0, stdout="")

    monkeypatch.setattr(demo, "_run_docker", run_docker)
    demo._cleanup_one_shot_container("mutable-name", plan_id, {})

    assert calls[-1] == ("rm", "--force", container_id)
    assert "mutable-name" not in calls[-1]


@pytest.mark.asyncio
async def test_one_shot_builder_runs_the_attested_immutable_image_without_pull(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[tuple[str, ...], dict[str, str]]] = []

    def run_docker(arguments: object, *, environment: dict[str, str], timeout: int = 30):
        del timeout
        call = tuple(arguments)  # type: ignore[arg-type]
        calls.append((call, dict(environment)))
        if call[0] == "inspect":
            return SimpleNamespace(returncode=1, stdout="")
        return SimpleNamespace(
            returncode=0,
            stdout=demo.json.dumps(_task_result(), separators=(",", ":")) + "\n",
        )

    monkeypatch.setattr(demo, "_run_docker", run_docker)
    monkeypatch.setattr(demo, "_validate_local_docker", lambda environment: None)
    monkeypatch.setattr(demo, "_image_revision", lambda image_id, environment: REVISION)
    monkeypatch.setattr(demo, "_cleanup_one_shot_container", lambda *args: None)

    result = await demo._seal_snapshot_once(DATABASE_NOW, END, _ready_plan().plan_id, ATTESTATION)

    assert result["status"] == "ok"
    compose_call, environment = next(item for item in calls if "compose" in item[0])
    assert compose_call[compose_call.index("run") : compose_call.index("run") + 3] == (
        "run",
        "--pull",
        "never",
    )
    assert environment[demo._API_IMAGE_OVERRIDE_ENV] == ATTESTATION.api_image_id
    assert environment[demo._BUILDER_IMAGE_OVERRIDE_ENV] == ATTESTATION.builder_image_id


def test_plan_blocks_inside_the_bounded_session_rollover_margin() -> None:
    near_close = datetime(2026, 7, 13, 19, 55, tzinfo=UTC)
    plan = demo._build_demo_plan(
        settings=_settings(),
        backfill_plan=_backfill_plan(),
        state=_state(cutoff=DATABASE_NOW),
        final_database_now=near_close,
    )

    assert plan.ready is False
    assert "too little time" in plan.blockers[-1]


def test_task_result_accepts_created_or_replayed_and_rejects_malformed() -> None:
    for status in ("created", "replayed"):
        snapshot_id, returned_status, observations = demo._validated_task_result(
            _task_result(status=status), DATABASE_NOW
        )
        assert snapshot_id == "sha256:" + "c" * 64
        assert returned_status == status
        assert observations == 258

    malformed = _task_result()
    malformed["created"] = True
    with pytest.raises(demo.ForecastDemoRefused, match="created count"):
        demo._validated_task_result(malformed, DATABASE_NOW)

    for created, replayed, entry_status in (
        (2, -1, "replayed"),
        (-1, 2, "created"),
        (1, 0, "replayed"),
        (0, 1, "created"),
    ):
        contradictory = _task_result(status=entry_status)
        contradictory["created"] = created
        contradictory["replayed"] = replayed
        with pytest.raises(demo.ForecastDemoRefused, match="does not match the plan"):
            demo._validated_task_result(contradictory, DATABASE_NOW)


def _served_fixture(
    evidence: demo.ValidatedSnapshotEvidence,
    *,
    source_snapshot_id: str | None = None,
    first_target_time: datetime | None = None,
):
    quantile_paths = dict(evidence.expected_quantiles)
    steps = []
    for index, target_time in enumerate(evidence.target_times):
        if index == 0 and first_target_time is not None:
            target_time = first_target_time
        steps.append(
            SimpleNamespace(
                target_time=target_time,
                point=evidence.expected_points[index],
                quantiles=[
                    SimpleNamespace(level=level, value=quantile_paths[level][index])
                    for level in (0.1, 0.5, 0.9)
                ],
                intervals=[
                    SimpleNamespace(
                        coverage=0.8,
                        lower_quantile=0.1,
                        upper_quantile=0.9,
                        lower=quantile_paths[0.1][index],
                        upper=quantile_paths[0.9][index],
                    )
                ],
            )
        )
    return SimpleNamespace(
        symbol="MSFT",
        target="close",
        horizon=5,
        horizon_unit="trading_day",
        as_of=DATABASE_NOW,
        currency="USD",
        forecasts=steps,
        provenance=SimpleNamespace(
            snapshot_id="sha256:" + "c" * 64,
            feature_set_hash="sha256:" + "c" * 64,
            model_version="baseline-naive@1",
            series_basis="raw",
            max_available_at=evidence.max_available_at,
            lookahead_check=SimpleNamespace(
                status="passed",
                violations=[],
                max_feature_available_at=evidence.max_available_at,
            ),
            data_sources=[
                SimpleNamespace(
                    name="polygon_open_close",
                    snapshot_id=source_snapshot_id or evidence.source_snapshot_id,
                    fields=["close"],
                    max_available_at=evidence.source_max_available_at,
                )
            ],
        ),
        calibration=SimpleNamespace(
            calibration_set_version="uncalibrated:baseline-naive@1",
            method="none",
            sample_count=0,
            window_start=None,
            window_end=None,
            by_interval=[],
        ),
        disclaimer=demo.DISCLAIMER,
    )


def test_response_must_equal_sealed_schedule_lineage_and_baseline_values() -> None:
    target_times = tuple(DATABASE_NOW.replace(day=14 + index) for index in range(5))
    evidence = demo.ValidatedSnapshotEvidence(
        target_times=target_times,
        source_snapshot_id="sha256:" + "d" * 64,
        source_max_available_at=DATABASE_NOW,
        max_available_at=DATABASE_NOW,
        expected_points=(100.0,) * 5,
        expected_quantiles=(
            (0.1, (90.0,) * 5),
            (0.5, (100.0,) * 5),
            (0.9, (110.0,) * 5),
        ),
    )
    snapshot_id = "sha256:" + "c" * 64
    demo._validate_forecast_response(
        _served_fixture(evidence),
        snapshot_id=snapshot_id,
        cutoff=DATABASE_NOW,
        evidence=evidence,
    )

    with pytest.raises(demo.ForecastDemoRefused, match="lineage"):
        demo._validate_forecast_response(
            _served_fixture(evidence, source_snapshot_id="sha256:" + "e" * 64),
            snapshot_id=snapshot_id,
            cutoff=DATABASE_NOW,
            evidence=evidence,
        )
    with pytest.raises(demo.ForecastDemoRefused, match="forecast path"):
        demo._validate_forecast_response(
            _served_fixture(
                evidence,
                first_target_time=target_times[0].replace(hour=13),
            ),
            snapshot_id=snapshot_id,
            cutoff=DATABASE_NOW,
            evidence=evidence,
        )


def test_builder_role_is_exact_and_never_accepts_runtime_or_remote_database() -> None:
    builder = _settings(
        database_url=(
            "postgresql+asyncpg://stockapi_snapshot_builder:x@timescaledb:5432/stockapi_test"
        )
    )
    assert safe_builder_settings(builder) is builder

    for url in (
        "postgresql+asyncpg://stockapi_app:x@timescaledb:5432/stockapi_test",
        "postgresql+asyncpg://stockapi_snapshot_builder:x@remote:5432/stockapi_test",
    ):
        with pytest.raises(OneShotSealRefused):
            safe_builder_settings(_settings(database_url=url))


def test_one_shot_builder_revision_file_must_match_reviewed_commit(tmp_path: Path) -> None:
    revision_file = tmp_path / "revision"
    revision_file.write_text(REVISION + "\n", encoding="ascii")
    _attest_build_revision(REVISION, revision_file)

    with pytest.raises(OneShotSealRefused, match="differs"):
        _attest_build_revision("b" * 40, revision_file)
    with pytest.raises(OneShotSealRefused, match="no trusted revision"):
        _attest_build_revision(REVISION, tmp_path / "missing")


class FakeStore:
    def __init__(self) -> None:
        self.snapshot_reads = 0

    async def database_state(self, session_dates: tuple[date, ...]):
        del session_dates
        return _state()

    async def database_now(self) -> datetime:
        return DATABASE_NOW

    async def get_snapshot(self, snapshot_id: str):
        self.snapshot_reads += 1
        if snapshot_id == "sha256:" + "c" * 64:
            return object()
        return None


def _store_factory(store: FakeStore):
    @asynccontextmanager
    async def factory(settings: Settings) -> AsyncIterator[FakeStore]:
        del settings
        yield store

    return factory


@asynccontextmanager
async def _no_lock(settings: Settings) -> AsyncIterator[None]:
    del settings
    yield


@pytest.mark.asyncio
async def test_execute_orders_auth_404_one_shot_seal_and_authenticated_200(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _ready_plan()
    store = FakeStore()
    events: list[str] = []

    async def fake_plan(**kwargs: object) -> demo.ForecastDemoPlan:
        del kwargs
        return plan

    async def http_get(
        path: str,
        params: object,
        api_key: str | None,
    ) -> demo.HttpResult:
        del params
        if path == "/healthz":
            events.append("health")
            return demo.HttpResult(200, b"")
        if api_key is None:
            events.append("unauthenticated")
            return demo.HttpResult(401, b"", "X-API-Key")
        if api_key != "local-demo-key":
            events.append("wrong-key")
            return demo.HttpResult(401, b"", "X-API-Key")
        if "seal" not in events:
            events.append("missing")
            return demo.HttpResult(404, b"")
        events.append("served")
        return demo.HttpResult(200, b"fixture")

    async def seal(
        cutoff: datetime,
        end_session: date,
        plan_id: str,
        attestation: demo.RuntimeImageAttestation,
    ) -> dict[str, object]:
        assert cutoff == DATABASE_NOW
        assert end_session == END
        assert plan_id == plan.plan_id
        assert attestation == ATTESTATION
        events.append("seal")
        return _task_result()

    response = SimpleNamespace(
        provenance=SimpleNamespace(
            model_version="baseline-naive@1",
            lookahead_check=SimpleNamespace(status="passed"),
        ),
        calibration=SimpleNamespace(method="none"),
        forecasts=[object()] * 5,
    )
    monkeypatch.setattr(demo, "plan_forecast_demo", fake_plan)
    monkeypatch.setattr(demo, "_validate_snapshot_record", lambda *args, **kwargs: None)
    monkeypatch.setattr(demo, "_parse_forecast_response", lambda content: response)
    monkeypatch.setattr(demo, "_validate_forecast_response", lambda *args, **kwargs: None)

    result = await demo.execute_forecast_demo(
        end_session=END,
        plan_id=plan.plan_id,
        authorization=demo.AUTHORIZATION_SENTINEL,
        settings=_settings(),
        store_factory=_store_factory(store),
        http_get=http_get,
        snapshot_sealer=seal,
        runtime_attestor=_runtime_attestor,
        runtime_revalidator=_runtime_revalidator,
        lock_fn=_no_lock,
    )

    assert events == [
        "health",
        "unauthenticated",
        "wrong-key",
        "missing",
        "seal",
        "served",
    ]
    assert result["authenticated_http_status"] == 200
    assert result["snapshot_status"] == "created"
    assert "local-demo-key" not in str(result)


@pytest.mark.asyncio
async def test_execute_refuses_missing_image_attestation_before_http_or_seal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _ready_plan()

    async def fake_plan(**kwargs: object) -> demo.ForecastDemoPlan:
        del kwargs
        return plan

    async def forbidden_http(*args: object, **kwargs: object) -> demo.HttpResult:
        del args, kwargs
        pytest.fail("HTTP must not run without image attestation")

    async def forbidden_seal(*args: object, **kwargs: object) -> dict[str, object]:
        del args, kwargs
        pytest.fail("seal must not run without image attestation")

    def refuse_attestation(tool_revision: str) -> demo.RuntimeImageAttestation:
        assert tool_revision == REVISION
        raise demo.ForecastDemoRefused("missing attestation")

    monkeypatch.setattr(demo, "plan_forecast_demo", fake_plan)
    with pytest.raises(demo.ForecastDemoRefused, match="missing attestation"):
        await demo.execute_forecast_demo(
            end_session=END,
            plan_id=plan.plan_id,
            authorization=demo.AUTHORIZATION_SENTINEL,
            settings=_settings(),
            store_factory=_store_factory(FakeStore()),
            http_get=forbidden_http,
            snapshot_sealer=forbidden_seal,
            runtime_attestor=refuse_attestation,
            runtime_revalidator=_runtime_revalidator,
            lock_fn=_no_lock,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure_mode", "expected_type", "expected_http_status"),
    [
        ("timeout", "ReadTimeout", None),
        ("http_500", "ForecastDemoRefused", 500),
    ],
)
async def test_post_seal_failures_return_sanitized_recovery_receipt(
    monkeypatch: pytest.MonkeyPatch,
    failure_mode: str,
    expected_type: str,
    expected_http_status: int | None,
) -> None:
    plan = _ready_plan()
    store = FakeStore()
    sealed = False

    async def fake_plan(**kwargs: object) -> demo.ForecastDemoPlan:
        del kwargs
        return plan

    async def http_get(
        path: str,
        params: object,
        api_key: str | None,
    ) -> demo.HttpResult:
        del params
        if path == "/healthz":
            return demo.HttpResult(200, b"")
        if api_key is None or api_key != "local-demo-key":
            return demo.HttpResult(401, b"", "X-API-Key")
        if not sealed:
            return demo.HttpResult(404, b"")
        if failure_mode == "timeout":
            raise httpx.ReadTimeout("secret-canary")
        return demo.HttpResult(500, b"secret-canary")

    async def seal(
        cutoff: datetime,
        end_session: date,
        plan_id: str,
        attestation: demo.RuntimeImageAttestation,
    ) -> dict[str, object]:
        nonlocal sealed
        assert cutoff == DATABASE_NOW
        assert end_session == END
        assert plan_id == plan.plan_id
        assert attestation == ATTESTATION
        sealed = True
        return _task_result()

    monkeypatch.setattr(demo, "plan_forecast_demo", fake_plan)
    monkeypatch.setattr(demo, "_validate_snapshot_record", lambda *args, **kwargs: None)

    result = await demo.execute_forecast_demo(
        end_session=END,
        plan_id=plan.plan_id,
        authorization=demo.AUTHORIZATION_SENTINEL,
        settings=_settings(),
        store_factory=_store_factory(store),
        http_get=http_get,
        snapshot_sealer=seal,
        runtime_attestor=_runtime_attestor,
        runtime_revalidator=_runtime_revalidator,
        lock_fn=_no_lock,
    )

    assert result["status"] == "sealed_proof_failed"
    assert result["snapshot_id"] == "sha256:" + "c" * 64
    assert result["snapshot_status"] == "created"
    assert result["proof_phase"] == "authenticated_forecast_request"
    assert result["failure_type"] == expected_type
    assert result["http_status"] == expected_http_status
    assert "secret-canary" not in str(result)


def test_cli_never_echoes_unexpected_exception_text(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def explode(**kwargs: object):
        del kwargs
        raise RuntimeError("secret-canary")

    monkeypatch.setattr(demo, "execute_forecast_demo", explode)
    exit_code = demo.main(
        [
            "execute",
            "--end",
            END.isoformat(),
            "--plan-id",
            "sha256:" + "a" * 64,
            "--authorization",
            demo.AUTHORIZATION_SENTINEL,
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "RuntimeError" in captured.err
    assert "secret-canary" not in captured.err


def test_cli_refuses_ambient_vendor_secret_before_planning(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    called = False

    async def plan(**kwargs: object):
        nonlocal called
        del kwargs
        called = True
        return _ready_plan()

    monkeypatch.setenv("POLYGON_API_KEY", "secret-canary")
    monkeypatch.setattr(demo, "plan_forecast_demo", plan)
    exit_code = demo.main(["plan", "--end", END.isoformat()])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert called is False
    assert "secret-canary" not in captured.err


def test_cli_surfaces_recoverable_post_seal_session_rollover(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    snapshot_id = "sha256:" + "f" * 64

    async def advanced(**kwargs: object):
        del kwargs
        return {
            "status": "sealed_session_advanced",
            "snapshot_id": snapshot_id,
            "session_currency_at_completion": "advanced_after_seal",
        }

    monkeypatch.setattr(demo, "execute_forecast_demo", advanced)
    exit_code = demo.main(
        [
            "execute",
            "--end",
            END.isoformat(),
            "--plan-id",
            "sha256:" + "a" * 64,
            "--authorization",
            demo.AUTHORIZATION_SENTINEL,
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 3
    assert snapshot_id in captured.err
    assert captured.out == ""


def test_wrapper_uses_one_shot_builder_and_never_starts_vendor_workers() -> None:
    root = Path(__file__).resolve().parents[2]
    wrapper = (root / "run-forecast-demo.ps1").read_text(encoding="utf-8")
    controller = (root / "scripts/forecast_demo.py").read_text(encoding="utf-8")
    dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
    live_wrapper = (root / "run-live-gate.ps1").read_text(encoding="utf-8")
    live_gate = (root / "tests/integration/test_bars_live_gate.py").read_text(encoding="utf-8")

    assert "git worktree add --detach" in wrapper
    assert "docker @dockerArgs build" in wrapper
    assert "--tag stock-api-api" in wrapper
    assert "--tag stock-api-snapshot-builder" in wrapper
    assert "compose @composeArgs --profile app images -q" not in wrapper
    assert "STOCKAPI_BUILD_REVISION=$($reviewedPlan.tool_revision)" in wrapper
    assert "STOCKAPI_FORECAST_DEMO_API_IMAGE_ID" in wrapper
    assert "STOCKAPI_FORECAST_DEMO_BUILDER_IMAGE_ID" in wrapper
    assert "org.opencontainers.image.revision" in dockerfile
    assert "/app/.stockapi-build-revision" in dockerfile
    assert r"Labels \"org.opencontainers.image.revision\"" in wrapper
    assert r"Labels \"com.docker.compose.project\"" in wrapper
    assert '"ALEMBIC_CONFIG"' in live_wrapper
    assert 'environment.pop("ALEMBIC_CONFIG", None)' in live_gate
    assert 'str(REPO_ROOT / "alembic.ini")' in live_gate
    assert '"GIT_DIR"' in wrapper
    assert '"GIT_WORK_TREE"' in wrapper
    assert '"GIT_OBJECT_DIRECTORY"' in wrapper
    assert "-d --no-deps --force-recreate --no-build --pull never api" in wrapper
    assert '"run",\n        "--pull",\n        "never",\n        "--rm"' in controller
    assert 'Remove-Item "Env:$name"' in wrapper
    assert 'Set-Item "Env:$name" -Value $previous' in wrapper
    assert "snapshot_celery_app.send_task" not in controller
    assert 'Where-Object { $_ -in @("worker", "beat", "snapshot-builder") }' in wrapper
    assert "trust_env=False" in controller
    assert '"--env-file",\n        str(env_file)' in controller
    assert '["docker", "--context", "desktop-linux", *arguments]' in controller
    for pinned_wrapper in (wrapper, live_wrapper):
        assert '"--env-file", $envFile' in pinned_wrapper
        assert "@dockerArgs compose @composeArgs" in pinned_wrapper
        assert '"COMPOSE_ENV_FILES"' in pinned_wrapper
        assert '"COMPOSE_DISABLE_ENV_FILE"' in pinned_wrapper
    for name in (
        "run-forecast-demo.ps1",
        "run-live-gate.ps1",
        "run-vendor-smoke.ps1",
        "run-vendor-backfill.ps1",
    ):
        operator = (root / name).read_text(encoding="utf-8")
        assert "Global\\StockApiMutatingOperator" in operator
        assert '"--env-file", $envFile' in operator
        assert "@dockerArgs compose @composeArgs" in operator
        assert '"COMPOSE_ENV_FILES"' in operator
        assert '"COMPOSE_DISABLE_ENV_FILE"' in operator
