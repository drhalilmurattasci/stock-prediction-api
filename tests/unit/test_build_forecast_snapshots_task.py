"""Dedicated snapshot-builder batch orchestration and fail-closed config."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.config import Settings
from app.services.adjusted_forecast_snapshot_builder import (
    ADJUSTED_AVAILABILITY_RULE_SET_HASH,
    ADJUSTED_RESOLUTION_POLICY_HASH,
)
from app.services.forecast_snapshot_builder import (
    DEFAULT_AVAILABILITY_RULE_SET_HASH,
    DEFAULT_RESOLUTION_POLICY_HASH,
    SnapshotBuildError,
    SnapshotBuildMisconfigured,
    SnapshotBuildResult,
    SnapshotInputUnavailable,
)
from ingestion.tasks import build_forecast_snapshots as task_module

CUTOFF = datetime(2026, 7, 13, 17, tzinfo=UTC)


def _settings(**overrides: object) -> Settings:
    fields: dict[str, object] = {
        "app_env": "test",
        "forecast_resolution_policy_hash": DEFAULT_RESOLUTION_POLICY_HASH,
        "forecast_trusted_availability_rule_set_hash": (DEFAULT_AVAILABILITY_RULE_SET_HASH),
    }
    fields.update(overrides)
    return Settings(**fields)  # type: ignore[arg-type]


async def test_batch_sorts_deduplicates_and_reports_created_replayed_deferred(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    class FakeBuilder:
        def __init__(self, sessionmaker: object, policy: object) -> None:
            del sessionmaker, policy

        async def build(self, spec: object) -> SnapshotBuildResult:
            symbol = spec.symbol  # type: ignore[attr-defined]
            calls.append(symbol)
            if symbol == "MSFT":
                raise SnapshotInputUnavailable("history is still short")
            return SnapshotBuildResult(
                snapshot_id="sha256:" + ("a" if symbol == "AAPL" else "b") * 64,
                as_of=CUTOFF,
                availability_checked_at=CUTOFF,
                observation_count=512,
                target_time_count=252,
                created=symbol == "AAPL",
            )

    monkeypatch.setattr(task_module, "ForecastSnapshotBuilder", FakeBuilder)
    result = await task_module.build_forecast_snapshots_async(
        symbols=["spy", "AAPL", "aapl", "MSFT"],
        as_of=CUTOFF,
        settings=_settings(),
        sessionmaker=object(),  # type: ignore[arg-type]
    )

    assert calls == ["AAPL", "MSFT", "SPY"]
    assert result["status"] == "degraded"
    assert result["created"] == 1
    assert result["replayed"] == 1
    assert result["deferred"] == 1
    assert result["failed"] == 0
    assert result["as_of"] == CUTOFF.isoformat()
    assert [row["status"] for row in result["per_symbol"]] == [
        "created",
        "deferred",
        "replayed",
    ]


async def test_batch_refuses_unset_or_wrong_policy_hash_before_database_use() -> None:
    for settings in (
        Settings(app_env="test"),
        _settings(forecast_resolution_policy_hash="sha256:" + "0" * 64),
        _settings(forecast_trusted_availability_rule_set_hash="sha256:" + "0" * 64),
    ):
        with pytest.raises(SnapshotBuildMisconfigured):
            await task_module.build_forecast_snapshots_async(
                symbols=["AAPL"],
                as_of=CUTOFF,
                settings=settings,
                sessionmaker=object(),  # type: ignore[arg-type]
            )


@pytest.mark.parametrize("symbols", [[], ["TSLA"], [" "]])
async def test_batch_rejects_empty_or_unpinned_symbol_universe(symbols: list[str]) -> None:
    with pytest.raises(ValueError):
        await task_module.build_forecast_snapshots_async(
            symbols=symbols,
            as_of=CUTOFF,
            settings=_settings(),
            sessionmaker=object(),  # type: ignore[arg-type]
        )


def test_task_cutoff_parser_requires_an_aware_iso_timestamp() -> None:
    assert task_module._parse_as_of("2026-07-13T17:00:00Z") == CUTOFF
    with pytest.raises(ValueError, match="timezone-aware"):
        task_module._parse_as_of("2026-07-13T23:00:00")


def test_operator_command_prints_the_exact_hashes(capsys: pytest.CaptureFixture[str]) -> None:
    assert task_module._main(["--print-policy-hashes"]) == 0
    output = capsys.readouterr().out
    assert f"FORECAST_RESOLUTION_POLICY_HASH={DEFAULT_RESOLUTION_POLICY_HASH}" in output
    assert (
        "FORECAST_TRUSTED_AVAILABILITY_RULE_SET_HASH="
        f"{DEFAULT_AVAILABILITY_RULE_SET_HASH}" in output
    )
    assert (
        "FORECAST_ADJUSTED_CLOSE_RESOLUTION_POLICY_HASH="
        f"{ADJUSTED_RESOLUTION_POLICY_HASH}" in output
    )
    assert (
        "FORECAST_ADJUSTED_CLOSE_TRUSTED_AVAILABILITY_RULE_SET_HASH="
        f"{ADJUSTED_AVAILABILITY_RULE_SET_HASH}" in output
    )


def test_retry_canonicalizes_a_positional_symbol_call_to_kwargs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    async def fail_batch(**kwargs: object) -> dict[str, object]:
        del kwargs
        raise task_module.SnapshotBatchTransientError(CUTOFF, RuntimeError("retry me"))

    class RetryRaised(Exception):
        pass

    def fake_retry(*args: object, **kwargs: object) -> None:
        del args
        captured.update(kwargs)
        raise RetryRaised

    monkeypatch.setattr(task_module, "_run_owned_snapshot_batch", fail_batch)
    monkeypatch.setattr(
        task_module,
        "get_settings",
        lambda: _settings(automation_enabled=True),
    )
    monkeypatch.setattr(task_module.build_forecast_snapshots, "retry", fake_retry)

    with pytest.raises(RetryRaised):
        task_module.build_forecast_snapshots.run(["AAPL"])

    assert captured["args"] == ()
    assert captured["kwargs"] == {
        "symbols": ["AAPL"],
        "as_of": CUTOFF.isoformat(),
    }


async def test_owned_batch_does_not_retry_a_mixed_deterministic_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    disposed = False

    class FakeEngine:
        async def dispose(self) -> None:
            nonlocal disposed
            disposed = True

    async def mixed_result(**kwargs: object) -> dict[str, object]:
        del kwargs
        return {"failed": 1, "deferred": 1}

    engine = FakeEngine()
    monkeypatch.setattr(task_module, "build_engine", lambda settings: engine)
    monkeypatch.setattr(task_module, "build_sessionmaker", lambda value: object())
    monkeypatch.setattr(task_module, "build_forecast_snapshots_async", mixed_result)

    with pytest.raises(SnapshotBuildError, match="failed deterministic trust checks"):
        await task_module._run_owned_snapshot_batch(
            symbols=["AAPL", "MSFT"],
            as_of=CUTOFF,
            settings=_settings(),
        )
    assert disposed is True
