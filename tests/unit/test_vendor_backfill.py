"""Fail-closed tests for the separately authorized MSFT backfill lane."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy.dialects import postgresql

import scripts.vendor_backfill as backfill
from app.config import Settings
from data_sources.base import OHLCVBar
from data_sources.guards import AsyncPacingCostRateGuard
from ingestion.locks import vendor_operation_lock_id
from scripts.vendor_backfill import (
    AUTHORIZATION_SENTINEL,
    BACKFILL_MAX_CALLS_PER_WINDOW,
    BACKFILL_RATE_WINDOW_SECONDS,
    BACKFILL_SOURCE,
    BACKFILL_SYMBOL,
    AttemptLedger,
    BackfillExecutionFailed,
    BackfillRefused,
    ExistingCoverage,
    _expected_session_dates,
    execute_backfill,
    plan_backfill,
    repair_backfill,
)
from scripts.vendor_smoke import SMOKE_LOCK_ID

END = date(2026, 7, 10)
NOW = datetime(2026, 7, 13, 16, tzinfo=UTC)
TEST_REVISION = "a" * 40
SMALL_WINDOW = (date(2026, 7, 8), date(2026, 7, 9), END)


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "app_env": "local",
        "database_url": (
            "postgresql+asyncpg://stockapi_app:test-secret@localhost:5432/stockapi_test"
        ),
        "polygon_api_key": "test-vendor-key",
        # Deliberately unsafe values: the operator lane must ignore these and
        # hard-bind its reviewed free-tier pacing contract.
        "polygon_max_calls_per_window": 258,
        "polygon_rate_window_seconds": 1,
    }
    values.update(overrides)
    return Settings(**values)  # type: ignore[arg-type]


def _version_id(session_date: date, state: str = "v1") -> str:
    return f"sha256:{session_date.strftime('%Y%m%d')}{state.encode().hex():0<48}"[:71]


def _coverage(
    *,
    complete: tuple[date, ...] = (),
    repairable: tuple[date, ...] = (),
    state: str = "v1",
) -> ExistingCoverage:
    versions = tuple(
        (value, _version_id(value, state)) for value in sorted((*complete, *repairable))
    )
    return ExistingCoverage(
        complete_dates=tuple(sorted(complete)),
        repairable_dates=tuple(sorted(repairable)),
        version_ids=versions,
    )


def _test_plan(
    end_session: date,
    session_dates: tuple[date, ...],
    coverage: ExistingCoverage,
) -> backfill.BackfillPlan:
    return backfill._build_plan(
        end_session,
        session_dates,
        coverage,
        tool_revision=TEST_REVISION,
    )


def _revision() -> str:
    return TEST_REVISION


class FakeStore:
    def __init__(
        self,
        *,
        complete: tuple[date, ...] = (),
        repairable: tuple[date, ...] = (),
        fail_receipt_for: date | None = None,
    ) -> None:
        self.complete = set(complete)
        self.repairable = set(repairable)
        self.fail_receipt_for = fail_receipt_for
        self.persisted: list[date] = []
        self.repaired: list[date] = []

    async def coverage(self, session_dates: tuple[date, ...]) -> ExistingCoverage:
        selected = set(session_dates)
        return _coverage(
            complete=tuple(sorted(self.complete.intersection(selected))),
            repairable=tuple(sorted(self.repairable.intersection(selected))),
        )

    async def repair_receipts(self, session_dates: tuple[date, ...]) -> int:
        repaired = 0
        for value in session_dates:
            if value in self.repairable:
                self.repairable.remove(value)
                self.complete.add(value)
                self.repaired.append(value)
                repaired += 1
        return repaired

    async def persist(self, bar: OHLCVBar) -> None:
        session_date = bar.timestamp.astimezone(UTC).date()
        self.persisted.append(session_date)
        if session_date == self.fail_receipt_for:
            self.repairable.add(session_date)
            raise RuntimeError("synthetic receipt failure")
        self.complete.add(session_date)


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


class FakeProvider:
    name = BACKFILL_SOURCE

    def __init__(
        self,
        guard: AsyncPacingCostRateGuard,
        *,
        fail_on: date | None = None,
    ) -> None:
        self.guard = guard
        self.fail_on = fail_on
        self.calls: list[date] = []
        self.closed = False

    async def __aenter__(self) -> FakeProvider:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self.closed = True

    async def get_daily_bars(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjusted: bool = False,
    ) -> list[OHLCVBar]:
        assert symbol == BACKFILL_SYMBOL
        assert start == end
        assert adjusted is False
        await self.guard.acquire(BACKFILL_SOURCE)
        self.calls.append(start)
        if start == self.fail_on:
            raise RuntimeError("synthetic transport failure")
        close = backfill._session_close(start)
        return [
            OHLCVBar(
                symbol=BACKFILL_SYMBOL,
                timestamp=close,
                timespan="day",
                multiplier=1,
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=1_000.0,
                source=BACKFILL_SOURCE,
                adjustment_basis="raw",
                fetched_at=close + timedelta(minutes=1),
            )
        ]


def _provider_factory(captured: list[FakeProvider], *, fail_on: date | None = None):
    def factory(
        settings: Settings,
        guard: AsyncPacingCostRateGuard,
    ) -> FakeProvider:
        del settings
        provider = FakeProvider(guard, fail_on=fail_on)
        captured.append(provider)
        return provider

    return factory


def test_exact_window_has_258_sessions_and_expected_bounds() -> None:
    sessions = _expected_session_dates(END)
    assert len(sessions) == 258
    assert sessions[0] == date(2025, 7, 1)
    assert sessions[-1] == END


def test_smoke_backfill_and_ingestion_share_one_vendor_wide_lock() -> None:
    expected = vendor_operation_lock_id()
    assert expected == backfill.BACKFILL_LOCK_ID
    assert expected == SMOKE_LOCK_ID


def test_receipt_repair_statement_is_exact_current_versions_only() -> None:
    closes = tuple(backfill._session_close(value) for value in SMALL_WINDOW[:2])
    sql = str(
        backfill._exact_receipt_insert_statement(closes).compile(dialect=postgresql.dialect())
    )

    assert "INSERT INTO bar_version_availability" in sql
    assert "FROM bars" in sql
    assert "bars.ts IN" in sql
    assert "bars_revisions" not in sql


def test_latest_smoke_anchor_reduces_backfill_to_257_calls() -> None:
    sessions = _expected_session_dates(END)
    plan = _test_plan(END, sessions, _coverage(complete=(END,)))
    result = plan.public_result()

    assert plan.smoke_anchor_present is True
    assert plan.required_outbound_attempts == 257
    assert END not in plan.missing_dates
    assert result["status"] == "ready"
    assert result["required_sessions"] == 258


def test_plan_id_binds_current_version_identity_and_pacing() -> None:
    first = _test_plan(END, SMALL_WINDOW, _coverage(complete=(END,), state="v1"))
    restated = _test_plan(END, SMALL_WINDOW, _coverage(complete=(END,), state="v2"))
    changed_tool = backfill._build_plan(
        END,
        SMALL_WINDOW,
        _coverage(complete=(END,), state="v1"),
        tool_revision="b" * 40,
    )

    assert first.plan_id != restated.plan_id
    assert first.plan_id != changed_tool.plan_id
    assert first.public_result()["tool_revision"] == TEST_REVISION
    assert first.public_result()["max_calls_per_window"] == 5
    assert first.public_result()["rate_window_seconds"] == 60.0


def test_attempt_ledger_is_single_use_and_marks_terminal_outcomes(tmp_path: Path) -> None:
    ledger = AttemptLedger(tmp_path / "attempts.jsonl", clock=lambda: NOW)
    plan = _test_plan(END, SMALL_WINDOW, _coverage(complete=(END,)))
    authorization_id = "msft-20260710-a"

    ledger.begin_authorization(
        authorization_id=authorization_id,
        plan=plan,
        max_calls=2,
    )
    ledger.reserve_attempt(
        authorization_id=authorization_id,
        plan_id=plan.plan_id,
        session_date=SMALL_WINDOW[0],
    )
    assert ledger.unresolved_dates() == (SMALL_WINDOW[0],)
    ledger.finish_attempt(
        authorization_id=authorization_id,
        plan_id=plan.plan_id,
        session_date=SMALL_WINDOW[0],
        status="failed",
        failure_type="RuntimeError",
    )

    assert ledger.unresolved_dates() == ()
    assert ledger.attempt_count(authorization_id) == 1
    with pytest.raises(BackfillRefused, match="already consumed"):
        ledger.begin_authorization(
            authorization_id=authorization_id,
            plan=plan,
            max_calls=2,
        )
    records = (tmp_path / "attempts.jsonl").read_text(encoding="utf-8")
    assert "test-vendor-key" not in records
    assert '"record_type":"outcome"' in records


def test_malformed_outcome_cannot_clear_an_open_reservation(tmp_path: Path) -> None:
    path = tmp_path / "attempts.jsonl"
    ledger = AttemptLedger(path, clock=lambda: NOW)
    plan = _test_plan(END, SMALL_WINDOW, _coverage(complete=(END,)))
    authorization_id = "msft-20260710-corrupt"
    ledger.begin_authorization(
        authorization_id=authorization_id,
        plan=plan,
        max_calls=2,
    )
    ledger.reserve_attempt(
        authorization_id=authorization_id,
        plan_id=plan.plan_id,
        session_date=SMALL_WINDOW[0],
    )
    malformed = {
        "record_type": "outcome",
        "authorization_id": authorization_id,
        "plan_id": plan.plan_id,
        "session_date": SMALL_WINDOW[0].isoformat(),
        "status": "checkpointed",
        "failure_type": "forged-detail",
        "recorded_at": NOW.isoformat(),
    }
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(malformed, sort_keys=True, separators=(",", ":")) + "\n")

    with pytest.raises(BackfillRefused, match="unreadable; stop for forensics"):
        ledger.unresolved_dates()


def test_unhashable_ledger_record_type_is_a_structured_refusal(tmp_path: Path) -> None:
    path = tmp_path / "attempts.jsonl"
    path.write_text('{"record_type":[]}\n', encoding="utf-8")

    with pytest.raises(BackfillRefused, match="unreadable; stop for forensics"):
        AttemptLedger(path).unresolved_dates()


async def test_success_calls_only_missing_sessions_and_checkpoints_each(
    tmp_path: Path,
) -> None:
    store = FakeStore(complete=(END,))
    initial = _test_plan(END, SMALL_WINDOW, await store.coverage(SMALL_WINDOW))
    providers: list[FakeProvider] = []

    result = await execute_backfill(
        end_session=END,
        plan_id=initial.plan_id,
        max_calls=2,
        authorization=AUTHORIZATION_SENTINEL,
        authorization_id="msft-20260710-success",
        settings=_settings(),
        clock=lambda: NOW,
        store_factory=_store_factory(store),  # type: ignore[arg-type]
        provider_factory=_provider_factory(providers),
        lock_fn=_no_lock,
        sessions_fn=lambda _end: SMALL_WINDOW,
        revision_fn=_revision,
        ledger_path=tmp_path / "attempts.jsonl",
    )

    assert providers[0].calls == list(SMALL_WINDOW[:-1])
    assert store.persisted == list(SMALL_WINDOW[:-1])
    assert store.complete == set(SMALL_WINDOW)
    assert result["attempts_reserved"] == 2
    assert result["attempts_spent"] == 2
    assert result["remaining_sessions"] == 0
    assert providers[0].guard.max_calls == BACKFILL_MAX_CALLS_PER_WINDOW
    assert providers[0].guard.window == BACKFILL_RATE_WINDOW_SECONDS


async def test_late_failure_preserves_checkpoints_and_fresh_plan_resumes_only_missing(
    tmp_path: Path,
) -> None:
    store = FakeStore(complete=(END,))
    first_plan = _test_plan(END, SMALL_WINDOW, await store.coverage(SMALL_WINDOW))
    failed_providers: list[FakeProvider] = []
    with pytest.raises(BackfillExecutionFailed) as failure:
        await execute_backfill(
            end_session=END,
            plan_id=first_plan.plan_id,
            max_calls=2,
            authorization=AUTHORIZATION_SENTINEL,
            authorization_id="msft-20260710-first",
            settings=_settings(),
            clock=lambda: NOW,
            store_factory=_store_factory(store),  # type: ignore[arg-type]
            provider_factory=_provider_factory(
                failed_providers,
                fail_on=SMALL_WINDOW[1],
            ),
            lock_fn=_no_lock,
            sessions_fn=lambda _end: SMALL_WINDOW,
            revision_fn=_revision,
            ledger_path=tmp_path / "attempts.jsonl",
        )

    assert failed_providers[0].calls == list(SMALL_WINDOW[:-1])
    assert store.complete == {SMALL_WINDOW[0], END}
    assert failure.value.result["attempts_spent"] == 2
    assert failure.value.result["sessions_persisted"] == 1
    assert AttemptLedger(tmp_path / "attempts.jsonl").unresolved_dates() == ()

    second_plan = _test_plan(END, SMALL_WINDOW, await store.coverage(SMALL_WINDOW))
    resumed_providers: list[FakeProvider] = []
    result = await execute_backfill(
        end_session=END,
        plan_id=second_plan.plan_id,
        max_calls=1,
        authorization=AUTHORIZATION_SENTINEL,
        authorization_id="msft-20260710-second",
        settings=_settings(),
        clock=lambda: NOW,
        store_factory=_store_factory(store),  # type: ignore[arg-type]
        provider_factory=_provider_factory(resumed_providers),
        lock_fn=_no_lock,
        sessions_fn=lambda _end: SMALL_WINDOW,
        revision_fn=_revision,
        ledger_path=tmp_path / "attempts.jsonl",
    )

    assert resumed_providers[0].calls == [SMALL_WINDOW[1]]
    assert result["attempts_spent"] == 1
    assert store.complete == set(SMALL_WINDOW)


async def test_session_scope_expiry_after_pacing_refuses_before_http(tmp_path: Path) -> None:
    store = FakeStore(complete=(END,))
    plan = _test_plan(END, SMALL_WINDOW, await store.coverage(SMALL_WINDOW))
    providers: list[FakeProvider] = []
    clock_calls = 0

    def expiring_clock() -> datetime:
        nonlocal clock_calls
        clock_calls += 1
        if clock_calls <= 2:
            return NOW
        return datetime(2026, 7, 13, 21, tzinfo=UTC)

    with pytest.raises(BackfillExecutionFailed) as failure:
        await execute_backfill(
            end_session=END,
            plan_id=plan.plan_id,
            max_calls=2,
            authorization=AUTHORIZATION_SENTINEL,
            authorization_id="msft-20260710-expired",
            settings=_settings(),
            clock=expiring_clock,
            store_factory=_store_factory(store),  # type: ignore[arg-type]
            provider_factory=_provider_factory(providers),
            lock_fn=_no_lock,
            sessions_fn=lambda _end: SMALL_WINDOW,
            revision_fn=_revision,
            ledger_path=tmp_path / "attempts.jsonl",
        )

    assert providers[0].calls == []
    assert failure.value.result["attempts_reserved"] == 1
    assert failure.value.result["attempts_spent"] == 0
    assert failure.value.result["failure_type"] == "BackfillRefused"
    assert AttemptLedger(tmp_path / "attempts.jsonl").unresolved_dates() == ()


async def test_unresolved_crash_reservation_blocks_a_duplicate_call(tmp_path: Path) -> None:
    store = FakeStore(complete=(END,))
    initial = _test_plan(END, SMALL_WINDOW, await store.coverage(SMALL_WINDOW))
    ledger_path = tmp_path / "attempts.jsonl"
    ledger = AttemptLedger(ledger_path, clock=lambda: NOW)
    ledger.begin_authorization(
        authorization_id="msft-20260710-crashed",
        plan=initial,
        max_calls=2,
    )
    ledger.reserve_attempt(
        authorization_id="msft-20260710-crashed",
        plan_id=initial.plan_id,
        session_date=SMALL_WINDOW[0],
    )

    plan = await plan_backfill(
        end_session=END,
        settings=_settings(polygon_api_key=None),
        clock=lambda: NOW,
        store_factory=_store_factory(store),  # type: ignore[arg-type]
        sessions_fn=lambda _end: SMALL_WINDOW,
        revision_fn=_revision,
        ledger_path=ledger_path,
    )
    providers: list[FakeProvider] = []
    assert plan.ambiguous_dates == (SMALL_WINDOW[0],)
    assert plan.public_result()["status"] == "blocked"

    with pytest.raises(BackfillRefused, match="unresolved prior attempt"):
        await execute_backfill(
            end_session=END,
            plan_id=plan.plan_id,
            max_calls=2,
            authorization=AUTHORIZATION_SENTINEL,
            authorization_id="msft-20260710-new",
            settings=_settings(),
            clock=lambda: NOW,
            store_factory=_store_factory(store),  # type: ignore[arg-type]
            provider_factory=_provider_factory(providers),
            lock_fn=_no_lock,
            sessions_fn=lambda _end: SMALL_WINDOW,
            revision_fn=_revision,
            ledger_path=ledger_path,
        )
    assert providers == []


async def test_receipt_only_crash_has_a_zero_vendor_repair_path(tmp_path: Path) -> None:
    store = FakeStore(complete=(END,), repairable=SMALL_WINDOW[:-1])
    plan = _test_plan(END, SMALL_WINDOW, await store.coverage(SMALL_WINDOW))

    result = await repair_backfill(
        end_session=END,
        plan_id=plan.plan_id,
        settings=_settings(polygon_api_key=None),
        clock=lambda: NOW,
        store_factory=_store_factory(store),  # type: ignore[arg-type]
        lock_fn=_no_lock,
        sessions_fn=lambda _end: SMALL_WINDOW,
        revision_fn=_revision,
        ledger_path=tmp_path / "attempts.jsonl",
    )

    assert result["outbound_attempts"] == 0
    assert result["sessions_repaired"] == 2
    assert store.complete == set(SMALL_WINDOW)
    assert store.repaired == list(SMALL_WINDOW[:-1])


async def test_persist_receipt_crash_can_be_repaired_without_another_vendor_call(
    tmp_path: Path,
) -> None:
    store = FakeStore(complete=(END,), fail_receipt_for=SMALL_WINDOW[0])
    plan = _test_plan(END, SMALL_WINDOW, await store.coverage(SMALL_WINDOW))
    providers: list[FakeProvider] = []
    ledger_path = tmp_path / "attempts.jsonl"

    with pytest.raises(BackfillExecutionFailed):
        await execute_backfill(
            end_session=END,
            plan_id=plan.plan_id,
            max_calls=2,
            authorization=AUTHORIZATION_SENTINEL,
            authorization_id="msft-20260710-receipt-crash",
            settings=_settings(),
            clock=lambda: NOW,
            store_factory=_store_factory(store),  # type: ignore[arg-type]
            provider_factory=_provider_factory(providers),
            lock_fn=_no_lock,
            sessions_fn=lambda _end: SMALL_WINDOW,
            revision_fn=_revision,
            ledger_path=ledger_path,
        )

    assert providers[0].calls == [SMALL_WINDOW[0]]
    assert SMALL_WINDOW[0] in store.repairable
    repair_plan = _test_plan(END, SMALL_WINDOW, await store.coverage(SMALL_WINDOW))
    result = await repair_backfill(
        end_session=END,
        plan_id=repair_plan.plan_id,
        settings=_settings(polygon_api_key=None),
        clock=lambda: NOW,
        store_factory=_store_factory(store),  # type: ignore[arg-type]
        lock_fn=_no_lock,
        sessions_fn=lambda _end: SMALL_WINDOW,
        revision_fn=_revision,
        ledger_path=ledger_path,
    )

    assert result["outbound_attempts"] == 0
    assert store.repaired == [SMALL_WINDOW[0]]
    assert SMALL_WINDOW[1] not in store.complete


async def test_budget_or_anchor_mismatch_refuses_before_ledger_or_provider(
    tmp_path: Path,
) -> None:
    store = FakeStore(complete=(END,))
    plan = _test_plan(END, SMALL_WINDOW, await store.coverage(SMALL_WINDOW))
    providers: list[FakeProvider] = []
    with pytest.raises(BackfillRefused, match="max_calls must equal"):
        await execute_backfill(
            end_session=END,
            plan_id=plan.plan_id,
            max_calls=1,
            authorization=AUTHORIZATION_SENTINEL,
            authorization_id="msft-20260710-wrong-budget",
            settings=_settings(),
            clock=lambda: NOW,
            store_factory=_store_factory(store),  # type: ignore[arg-type]
            provider_factory=_provider_factory(providers),
            lock_fn=_no_lock,
            sessions_fn=lambda _end: SMALL_WINDOW,
            revision_fn=_revision,
            ledger_path=tmp_path / "attempts.jsonl",
        )
    assert providers == []
    assert not (tmp_path / "attempts.jsonl").exists()

    no_anchor = FakeStore()
    blocked = _test_plan(END, SMALL_WINDOW, await no_anchor.coverage(SMALL_WINDOW))
    with pytest.raises(BackfillRefused, match="smoke bar and receipt"):
        await execute_backfill(
            end_session=END,
            plan_id=blocked.plan_id,
            max_calls=3,
            authorization=AUTHORIZATION_SENTINEL,
            authorization_id="msft-20260710-no-anchor",
            settings=_settings(),
            clock=lambda: NOW,
            store_factory=_store_factory(no_anchor),  # type: ignore[arg-type]
            provider_factory=_provider_factory(providers),
            lock_fn=_no_lock,
            sessions_fn=lambda _end: SMALL_WINDOW,
            revision_fn=_revision,
            ledger_path=tmp_path / "other-attempts.jsonl",
        )
    assert providers == []


def test_main_never_renders_a_secret_bearing_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake_key = "FAKE_BACKFILL_KEY_MUST_NOT_RENDER"

    async def fail(**kwargs: object) -> dict[str, object]:
        del kwargs
        raise RuntimeError(f"Authorization: Bearer {fake_key}")

    monkeypatch.setattr(backfill, "execute_backfill", fail)
    result = backfill.main(
        [
            "execute",
            "--end",
            END.isoformat(),
            "--plan-id",
            "sha256:" + "a" * 64,
            "--max-calls",
            "1",
            "--authorization",
            AUTHORIZATION_SENTINEL,
            "--authorization-id",
            "msft-20260710-secret",
        ]
    )
    captured = capsys.readouterr()
    rendered = captured.out + captured.err

    assert result == 1
    assert fake_key not in rendered
    assert "Authorization" not in rendered


def test_clean_revision_scrubs_git_routing_and_proves_repository_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in backfill._GIT_ROUTING_ENVIRONMENT:
        monkeypatch.setenv(name, "outside-repository")

    def run(arguments: list[str], **kwargs: object) -> SimpleNamespace:
        environment = kwargs["env"]
        assert isinstance(environment, dict)
        assert not (backfill._GIT_ROUTING_ENVIRONMENT & environment.keys())
        if "--show-toplevel" in arguments:
            stdout = str(backfill.REPO_ROOT)
        elif "status" in arguments:
            stdout = ""
        else:
            stdout = TEST_REVISION
        return SimpleNamespace(returncode=0, stdout=stdout, stderr="")

    monkeypatch.setattr(backfill.subprocess, "run", run)

    assert backfill._clean_git_revision() == TEST_REVISION


def test_wrapper_parses_and_never_accepts_the_key_on_argv() -> None:
    wrapper = (backfill.REPO_ROOT / "run-vendor-backfill.ps1").read_text(encoding="utf-8")
    assert "POLYGON_API_KEY" not in wrapper
    assert 'ValidateSet("plan", "repair", "execute")' in wrapper
    assert "stockapi-msft-backfill-only" in wrapper
    assert "(?:py|python" in wrapper
    assert "exit $commandExitCode" in wrapper
    assert "vendor backfill command failed" not in wrapper
