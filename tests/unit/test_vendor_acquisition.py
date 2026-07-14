"""Fail-closed tests for the typed action-plus-price acquisition lane."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

import scripts.vendor_acquisition as acquisition
from app.config import Settings
from app.services.corporate_action_store import (
    CorporateActionCollectionEvidence,
    CorporateActionScopeCoverage,
    PublishedCorporateActionCollection,
)
from app.services.corporate_actions import (
    CORPORATE_ACTION_QUERY_POLICY_HASH,
    CORPORATE_ACTION_SOURCE,
    DIVIDENDS_ENDPOINT,
    SPLITS_ENDPOINT,
    CorporateActionCollectionRecord,
)
from data_sources.base import CostBudgetExceeded, DividendPage, OHLCVBar, SplitPage
from data_sources.polygon_open_close import open_close_endpoint_identity
from scripts.vendor_acquisition import (
    AUTHORIZATION_SENTINEL,
    AcquisitionExecutionFailed,
    AcquisitionLedger,
    AcquisitionRefused,
    PlanBoundGlobalGuard,
    execute_acquisition,
    plan_acquisition,
    repair_acquisition,
)
from scripts.vendor_backfill import (
    BACKFILL_SOURCE,
    BACKFILL_SYMBOL,
    ExistingCoverage,
    _session_close,
)

END = date(2026, 7, 13)
NOW = datetime(2026, 7, 14, 16, tzinfo=UTC)
TEST_REVISION = "a" * 40
SMALL_WINDOW = (date(2026, 7, 9), date(2026, 7, 10), END)
ACTION_FETCHED_AT = datetime(2026, 7, 14, 15, tzinfo=UTC)


def _hash(label: str) -> str:
    import hashlib

    return "sha256:" + hashlib.sha256(label.encode()).hexdigest()


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "app_env": "local",
        "database_url": (
            "postgresql+asyncpg://stockapi_app:test-secret@localhost:5432/stockapi_test"
        ),
        "polygon_api_key": "test-vendor-key",
    }
    values.update(overrides)
    return Settings(**values)  # type: ignore[arg-type]


def _revision() -> str:
    return TEST_REVISION


def _version_id(session_date: date) -> str:
    return _hash(f"price:{session_date.isoformat()}")


def _price_coverage(
    dates: tuple[date, ...],
    *,
    complete: set[date],
    repairable: set[date],
) -> ExistingCoverage:
    selected = set(dates)
    complete_dates = tuple(sorted(selected.intersection(complete)))
    repairable_dates = tuple(sorted(selected.intersection(repairable)))
    return ExistingCoverage(
        complete_dates=complete_dates,
        repairable_dates=repairable_dates,
        version_ids=tuple(
            (value, _version_id(value)) for value in sorted((*complete_dates, *repairable_dates))
        ),
    )


def _evidence(
    label: str,
    *,
    available: bool,
    fetched_at: datetime = ACTION_FETCHED_AT,
) -> CorporateActionCollectionEvidence:
    recorded_at = fetched_at + timedelta(seconds=1)
    return CorporateActionCollectionEvidence(
        collection_id=_hash(label),
        collection_recorded_at=recorded_at,
        fetched_at=fetched_at,
        event_count=0,
        available_at=recorded_at + timedelta(seconds=1) if available else None,
    )


class FakeStore:
    def __init__(
        self,
        *,
        complete_prices: tuple[date, ...] = (),
        repairable_prices: tuple[date, ...] = (),
        splits: tuple[CorporateActionCollectionEvidence, ...] = (),
        dividends: tuple[CorporateActionCollectionEvidence, ...] = (),
    ) -> None:
        self.complete_prices = set(complete_prices)
        self.repairable_prices = set(repairable_prices)
        self.actions: dict[str, list[CorporateActionCollectionEvidence]] = {
            "split": list(splits),
            "dividend": list(dividends),
        }
        self.persisted_prices: list[date] = []
        self.persisted_actions: list[str] = []
        self.repaired: list[str] = []

    async def price_coverage(self, dates: tuple[date, ...]) -> ExistingCoverage:
        return _price_coverage(
            dates,
            complete=self.complete_prices,
            repairable=self.repairable_prices,
        )

    async def repair_price_receipts(self, dates: tuple[date, ...]) -> int:
        repaired = 0
        for value in dates:
            if value in self.repairable_prices:
                self.repairable_prices.remove(value)
                self.complete_prices.add(value)
                self.repaired.append(f"price:{value.isoformat()}")
                repaired += 1
        return repaired

    async def persist_price(self, bar: OHLCVBar) -> None:
        session_date = bar.timestamp.astimezone(UTC).date()
        self.persisted_prices.append(session_date)
        self.complete_prices.add(session_date)

    async def action_coverage(
        self,
        action_type: acquisition.ActionType,
        start: date,
        end: date,
    ) -> CorporateActionScopeCoverage:
        endpoint = SPLITS_ENDPOINT if action_type == "split" else DIVIDENDS_ENDPOINT
        return CorporateActionScopeCoverage(
            action_type=action_type,
            endpoint=endpoint,
            source=CORPORATE_ACTION_SOURCE,
            symbol=BACKFILL_SYMBOL,
            coverage_start=start,
            coverage_end=end,
            query_policy_hash=CORPORATE_ACTION_QUERY_POLICY_HASH,
            collections=tuple(self.actions[action_type]),
        )

    async def repair_action_receipt(
        self,
        collection_id: str,
    ) -> PublishedCorporateActionCollection:
        for action_type, values in self.actions.items():
            for index, value in enumerate(values):
                if value.collection_id != collection_id:
                    continue
                available_at = value.collection_recorded_at + timedelta(seconds=1)
                values[index] = CorporateActionCollectionEvidence(
                    collection_id=value.collection_id,
                    collection_recorded_at=value.collection_recorded_at,
                    fetched_at=value.fetched_at,
                    event_count=value.event_count,
                    available_at=available_at,
                )
                self.repaired.append(f"{action_type}:{collection_id}")
                return PublishedCorporateActionCollection(
                    collection_id=collection_id,
                    collection_recorded_at=value.collection_recorded_at,
                    available_at=available_at,
                    event_count=value.event_count,
                )
        raise RuntimeError("unknown synthetic collection")

    async def persist_action(
        self,
        record: CorporateActionCollectionRecord,
    ) -> PublishedCorporateActionCollection:
        recorded_at = record.fetched_at + timedelta(seconds=1)
        available_at = recorded_at + timedelta(seconds=1)
        evidence = CorporateActionCollectionEvidence(
            collection_id=record.collection_id,
            collection_recorded_at=recorded_at,
            fetched_at=record.fetched_at,
            event_count=record.event_count,
            available_at=available_at,
        )
        self.actions[record.action_type].append(evidence)
        self.persisted_actions.append(record.action_type)
        return PublishedCorporateActionCollection(
            collection_id=record.collection_id,
            collection_recorded_at=recorded_at,
            available_at=available_at,
            event_count=record.event_count,
        )


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
        guard: PlanBoundGlobalGuard,
        calls: list[str],
        *,
        fail_kind: str | None = None,
    ) -> None:
        self.guard = guard
        self.calls = calls
        self.fail_kind = fail_kind

    async def __aenter__(self) -> FakeProvider:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None

    async def get_splits(self, symbol: str, *, start: date, end: date) -> SplitPage:
        assert symbol == BACKFILL_SYMBOL
        await self.guard.acquire("polygon", endpoint=SPLITS_ENDPOINT)
        self.calls.append("split_page")
        if self.fail_kind == "split_page":
            raise RuntimeError("synthetic split failure")
        return SplitPage(
            provider_request_id="split-request",
            provider_origin="https://api.massive.com",
            endpoint=SPLITS_ENDPOINT,
            symbol=symbol,
            start=start,
            end=end,
            source=CORPORATE_ACTION_SOURCE,
            fetched_at=ACTION_FETCHED_AT,
            results=(),
        )

    async def get_dividends(self, symbol: str, *, start: date, end: date) -> DividendPage:
        assert symbol == BACKFILL_SYMBOL
        await self.guard.acquire("polygon", endpoint=DIVIDENDS_ENDPOINT)
        self.calls.append("dividend_page")
        if self.fail_kind == "dividend_page":
            raise RuntimeError("synthetic dividend failure")
        return DividendPage(
            provider_request_id="dividend-request",
            provider_origin="https://api.massive.com",
            endpoint=DIVIDENDS_ENDPOINT,
            symbol=symbol,
            start=start,
            end=end,
            source=CORPORATE_ACTION_SOURCE,
            fetched_at=ACTION_FETCHED_AT,
            results=(),
        )

    async def get_daily_bars(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjusted: bool = False,
    ) -> list[OHLCVBar]:
        assert symbol == BACKFILL_SYMBOL and start == end and adjusted is False
        await self.guard.acquire(
            BACKFILL_SOURCE,
            endpoint=open_close_endpoint_identity(symbol, start),
        )
        self.calls.append(f"open_close:{start.isoformat()}")
        if self.fail_kind == f"open_close:{start.isoformat()}":
            raise RuntimeError("synthetic price failure")
        close = _session_close(start)
        return [
            OHLCVBar(
                symbol=symbol,
                timestamp=close,
                timespan="day",
                multiplier=1,
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=1_000.0,
                adjustment_basis="raw",
                source=BACKFILL_SOURCE,
                fetched_at=close + timedelta(minutes=1),
            )
        ]


def _provider_factory(
    calls: list[str],
    *,
    fail_kind: str | None = None,
):
    def factory(settings: Settings, guard: PlanBoundGlobalGuard) -> FakeProvider:
        del settings
        return FakeProvider(guard, calls, fail_kind=fail_kind)

    return factory


async def _plan(
    store: FakeStore,
    tmp_path: Path,
    *,
    sessions: tuple[date, ...] = SMALL_WINDOW,
):
    return await plan_acquisition(
        end_session=END,
        settings=_settings(polygon_api_key=None),
        clock=lambda: NOW,
        store_factory=_store_factory(store),  # type: ignore[arg-type]
        sessions_fn=lambda _end: sessions,
        revision_fn=_revision,
        ledger_path=tmp_path / "acquisition.jsonl",
        legacy_ledger_path=tmp_path / "legacy.jsonl",
    )


async def _execute(
    plan,
    store: FakeStore,
    tmp_path: Path,
    calls: list[str],
    *,
    authorization_id: str,
    fail_kind: str | None = None,
):
    allocation = plan.allocation
    return await execute_acquisition(
        end_session=END,
        plan_id=plan.plan_id,
        max_calls=plan.required_outbound_attempts,
        split_calls=allocation["split_page"],
        dividend_calls=allocation["dividend_page"],
        open_close_calls=allocation["open_close"],
        authorization=AUTHORIZATION_SENTINEL,
        authorization_id=authorization_id,
        settings=_settings(),
        clock=lambda: NOW,
        store_factory=_store_factory(store),  # type: ignore[arg-type]
        provider_factory=_provider_factory(calls, fail_kind=fail_kind),
        lock_fn=_no_lock,
        sessions_fn=lambda _end: SMALL_WINDOW,
        revision_fn=_revision,
        ledger_path=tmp_path / "acquisition.jsonl",
        legacy_ledger_path=tmp_path / "legacy.jsonl",
    )


async def test_full_plan_after_smoke_is_two_actions_plus_257_prices(tmp_path: Path) -> None:
    store = FakeStore(complete_prices=(END,))
    plan = await _plan(
        store,
        tmp_path,
        sessions=acquisition._expected_session_dates(END),
    )

    assert plan.required_outbound_attempts == 259
    assert plan.allocation == {"split_page": 1, "dividend_page": 1, "open_close": 257}
    assert [value.kind for value in plan.calls[:2]] == ["split_page", "dividend_page"]
    assert all(value.kind == "open_close" for value in plan.calls[2:])
    assert plan.public_result()["status"] == "ready"


async def test_outer_plan_binds_action_receipts_policy_tool_and_call_digest(tmp_path: Path) -> None:
    first_store = FakeStore(complete_prices=(END,))
    first = await _plan(first_store, tmp_path)
    second_store = FakeStore(
        complete_prices=(END,),
        splits=(_evidence("split-complete", available=True),),
    )
    second = await _plan(second_store, tmp_path)

    assert first.plan_id != second.plan_id
    assert first.calls_sha256 != second.calls_sha256
    assert second.allocation["split_page"] == 0
    assert second.public_result()["corporate_action_query_policy_hash"] == (
        CORPORATE_ACTION_QUERY_POLICY_HASH
    )


async def test_global_guard_has_one_budget_across_provider_names() -> None:
    guard = PlanBoundGlobalGuard(total_budget=2, end_session=END, clock=lambda: NOW)
    split = acquisition._make_call("split_page", SPLITS_ENDPOINT, SMALL_WINDOW[0], END)
    dividend = acquisition._make_call("dividend_page", DIVIDENDS_ENDPOINT, SMALL_WINDOW[0], END)
    price = acquisition._make_call(
        "open_close",
        open_close_endpoint_identity(BACKFILL_SYMBOL, SMALL_WINDOW[0]),
        SMALL_WINDOW[0],
        SMALL_WINDOW[0],
    )

    guard.arm(split)
    await guard.acquire("polygon", endpoint=SPLITS_ENDPOINT)
    assert guard.finish(split) is True
    guard.arm(dividend)
    await guard.acquire(BACKFILL_SOURCE, endpoint=DIVIDENDS_ENDPOINT)
    assert guard.finish(dividend) is True
    guard.arm(price)
    with pytest.raises(CostBudgetExceeded):
        await guard.acquire(BACKFILL_SOURCE, endpoint=price.endpoint)
    assert guard.finish(price) is False
    assert guard.spent == 2


async def test_guard_refuses_wrong_endpoint_and_second_acquisition() -> None:
    guard = PlanBoundGlobalGuard(total_budget=1, end_session=END, clock=lambda: NOW)
    call = acquisition._make_call("split_page", SPLITS_ENDPOINT, SMALL_WINDOW[0], END)
    guard.arm(call)
    with pytest.raises(AcquisitionRefused, match="armed acquisition call"):
        await guard.acquire("polygon", endpoint=DIVIDENDS_ENDPOINT)
    await guard.acquire("polygon", endpoint=SPLITS_ENDPOINT)
    with pytest.raises(AcquisitionRefused, match="armed acquisition call"):
        await guard.acquire("polygon", endpoint=SPLITS_ENDPOINT)
    assert guard.finish(call) is True


async def test_success_runs_actions_first_and_checkpoints_every_receipt(tmp_path: Path) -> None:
    store = FakeStore(complete_prices=(END,))
    plan = await _plan(store, tmp_path)
    calls: list[str] = []

    result = await _execute(
        plan,
        store,
        tmp_path,
        calls,
        authorization_id="msft-20260713-success",
    )

    assert calls == [
        "split_page",
        "dividend_page",
        f"open_close:{SMALL_WINDOW[0].isoformat()}",
        f"open_close:{SMALL_WINDOW[1].isoformat()}",
    ]
    assert store.persisted_actions == ["split", "dividend"]
    assert store.complete_prices == set(SMALL_WINDOW)
    assert result["attempts_reserved"] == 4
    assert result["attempts_spent"] == 4
    assert result["checkpointed"] == {
        "split_page": 1,
        "dividend_page": 1,
        "open_close": 2,
    }


async def test_action_failure_stops_before_prices_and_fresh_plan_skips_checkpoint(
    tmp_path: Path,
) -> None:
    store = FakeStore(complete_prices=(END,))
    first_plan = await _plan(store, tmp_path)
    failed_calls: list[str] = []
    with pytest.raises(AcquisitionExecutionFailed) as failed:
        await _execute(
            first_plan,
            store,
            tmp_path,
            failed_calls,
            authorization_id="msft-20260713-first",
            fail_kind="dividend_page",
        )

    assert failed_calls == ["split_page", "dividend_page"]
    assert failed.value.result["attempts_spent"] == 2
    second_plan = await _plan(store, tmp_path)
    assert second_plan.allocation == {"split_page": 0, "dividend_page": 1, "open_close": 2}
    resumed_calls: list[str] = []
    result = await _execute(
        second_plan,
        store,
        tmp_path,
        resumed_calls,
        authorization_id="msft-20260713-second",
    )
    assert resumed_calls[0] == "dividend_page"
    assert "split_page" not in resumed_calls
    assert result["attempts_spent"] == 3


async def test_empty_action_page_is_durable_complete_evidence(tmp_path: Path) -> None:
    store = FakeStore(complete_prices=SMALL_WINDOW)
    plan = await _plan(store, tmp_path)
    calls: list[str] = []
    await _execute(
        plan,
        store,
        tmp_path,
        calls,
        authorization_id="msft-20260713-empty",
    )
    final = await _plan(store, tmp_path)
    assert final.public_result()["status"] == "complete"
    assert all(value.event_count == 0 for value in final.split_state.coverage.complete)
    assert all(value.event_count == 0 for value in final.dividend_state.coverage.complete)


async def test_action_content_without_receipt_repairs_with_zero_calls(tmp_path: Path) -> None:
    store = FakeStore(
        complete_prices=SMALL_WINDOW,
        splits=(_evidence("split-repair", available=False),),
        dividends=(_evidence("dividend-complete", available=True),),
    )
    plan = await _plan(store, tmp_path)
    assert plan.required_outbound_attempts == 0
    assert plan.receipt_repairs_required == 1

    result = await repair_acquisition(
        end_session=END,
        plan_id=plan.plan_id,
        settings=_settings(polygon_api_key=None),
        clock=lambda: NOW,
        store_factory=_store_factory(store),  # type: ignore[arg-type]
        lock_fn=_no_lock,
        sessions_fn=lambda _end: SMALL_WINDOW,
        revision_fn=_revision,
        ledger_path=tmp_path / "acquisition.jsonl",
        legacy_ledger_path=tmp_path / "legacy.jsonl",
    )
    assert result["outbound_attempts"] == 0
    assert result["receipts_repaired"] == 1
    assert (await _plan(store, tmp_path)).public_result()["status"] == "complete"


async def test_unresolved_typed_reservation_blocks_duplicate_call(tmp_path: Path) -> None:
    store = FakeStore(complete_prices=(END,))
    original = await _plan(store, tmp_path)
    ledger = AcquisitionLedger(tmp_path / "acquisition.jsonl", clock=lambda: NOW)
    ledger.begin_authorization(
        authorization_id="msft-20260713-crashed",
        plan=original,
        max_calls=original.required_outbound_attempts,
    )
    ledger.reserve_call(
        authorization_id="msft-20260713-crashed",
        plan_id=original.plan_id,
        call=original.calls[0],
    )

    blocked = await _plan(store, tmp_path)
    assert blocked.ambiguous_call_ids == (original.calls[0].call_id,)
    assert blocked.public_result()["status"] == "blocked"
    with pytest.raises(AcquisitionRefused, match="unresolved prior attempt"):
        await _execute(
            blocked,
            store,
            tmp_path,
            [],
            authorization_id="msft-20260713-new",
        )


async def test_wrong_typed_allocation_refuses_before_ledger_or_provider(tmp_path: Path) -> None:
    store = FakeStore(complete_prices=(END,))
    plan = await _plan(store, tmp_path)
    calls: list[str] = []
    with pytest.raises(AcquisitionRefused, match="typed allocation must exactly"):
        await execute_acquisition(
            end_session=END,
            plan_id=plan.plan_id,
            max_calls=plan.required_outbound_attempts,
            split_calls=0,
            dividend_calls=1,
            open_close_calls=plan.required_outbound_attempts - 1,
            authorization=AUTHORIZATION_SENTINEL,
            authorization_id="msft-20260713-wrong",
            settings=_settings(),
            clock=lambda: NOW,
            store_factory=_store_factory(store),  # type: ignore[arg-type]
            provider_factory=_provider_factory(calls),
            lock_fn=_no_lock,
            sessions_fn=lambda _end: SMALL_WINDOW,
            revision_fn=_revision,
            ledger_path=tmp_path / "acquisition.jsonl",
            legacy_ledger_path=tmp_path / "legacy.jsonl",
        )
    assert calls == []
    assert not (tmp_path / "acquisition.jsonl").exists()


@pytest.mark.parametrize(
    ("kind", "endpoint"),
    [
        ("split_page", SPLITS_ENDPOINT),
        ("dividend_page", DIVIDENDS_ENDPOINT),
        (
            "open_close",
            open_close_endpoint_identity(BACKFILL_SYMBOL, SMALL_WINDOW[0]),
        ),
    ],
)
async def test_session_rollover_refuses_each_call_kind_before_spend(kind, endpoint) -> None:
    call = acquisition._make_call(
        kind,
        endpoint,
        SMALL_WINDOW[0],
        SMALL_WINDOW[0] if kind == "open_close" else END,
    )
    guard = PlanBoundGlobalGuard(
        total_budget=1,
        end_session=END,
        clock=lambda: datetime(2026, 7, 14, 21, tzinfo=UTC),
    )
    guard.arm(call)
    with pytest.raises(AcquisitionRefused, match="latest completed XNYS session"):
        await guard.acquire("polygon", endpoint=endpoint)
    assert guard.finish(call) is False
    assert guard.spent == 0


def test_typed_ledger_tampering_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "acquisition.jsonl"
    path.write_text('{"record_type":"attempt","call_kind":"open_close"}\n', encoding="utf-8")
    with pytest.raises(AcquisitionRefused, match="unreadable; stop for forensics"):
        AcquisitionLedger(path).unresolved_call_ids()


def test_wrapper_defaults_to_plan_and_old_wrapper_rejects_expanded_sentinel() -> None:
    wrapper = (acquisition.REPO_ROOT / "run-vendor-acquisition.ps1").read_text(encoding="utf-8")
    old_wrapper = (acquisition.REPO_ROOT / "run-vendor-backfill.ps1").read_text(encoding="utf-8")
    assert '[string]$Mode = "plan"' in wrapper
    assert "stockapi-msft-acquisition-only" in wrapper
    assert "SplitCalls + $DividendCalls + $OpenCloseCalls" in wrapper
    assert "POLYGON_API_KEY" not in wrapper
    assert "stockapi-msft-acquisition-only" not in old_wrapper


def test_main_never_renders_secret_bearing_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret = "FAKE_ACQUISITION_KEY_MUST_NOT_RENDER"

    async def fail(**kwargs: object) -> dict[str, object]:
        del kwargs
        raise RuntimeError(f"Authorization: Bearer {secret}")

    monkeypatch.setattr(acquisition, "execute_acquisition", fail)
    result = acquisition.main(
        [
            "execute",
            "--end",
            END.isoformat(),
            "--plan-id",
            _hash("plan"),
            "--max-calls",
            "1",
            "--split-calls",
            "1",
            "--dividend-calls",
            "0",
            "--open-close-calls",
            "0",
            "--authorization",
            AUTHORIZATION_SENTINEL,
            "--authorization-id",
            "msft-20260713-secret",
        ]
    )
    captured = capsys.readouterr()
    rendered = captured.out + captured.err
    assert result == 1
    assert secret not in rendered
    assert "Authorization" not in rendered


def test_ledger_records_typed_calls_without_key(tmp_path: Path) -> None:
    price_plan = acquisition._build_plan(
        END,
        SMALL_WINDOW,
        _price_coverage(SMALL_WINDOW, complete={END}, repairable=set()),
        tool_revision=TEST_REVISION,
    )
    split = CorporateActionScopeCoverage(
        action_type="split",
        endpoint=SPLITS_ENDPOINT,
        source=CORPORATE_ACTION_SOURCE,
        symbol=BACKFILL_SYMBOL,
        coverage_start=SMALL_WINDOW[0],
        coverage_end=END,
        query_policy_hash=CORPORATE_ACTION_QUERY_POLICY_HASH,
        collections=(),
    )
    dividend = CorporateActionScopeCoverage(
        action_type="dividend",
        endpoint=DIVIDENDS_ENDPOINT,
        source=CORPORATE_ACTION_SOURCE,
        symbol=BACKFILL_SYMBOL,
        coverage_start=SMALL_WINDOW[0],
        coverage_end=END,
        query_policy_hash=CORPORATE_ACTION_QUERY_POLICY_HASH,
        collections=(),
    )
    plan = acquisition._build_acquisition_plan(price_plan, split, dividend)
    ledger = AcquisitionLedger(tmp_path / "acquisition.jsonl", clock=lambda: NOW)
    ledger.begin_authorization(
        authorization_id="msft-20260713-ledger",
        plan=plan,
        max_calls=plan.required_outbound_attempts,
    )
    with pytest.raises(AcquisitionRefused, match="action-first order"):
        ledger.reserve_call(
            authorization_id="msft-20260713-ledger",
            plan_id=plan.plan_id,
            call=plan.calls[1],
        )
    ledger.reserve_call(
        authorization_id="msft-20260713-ledger",
        plan_id=plan.plan_id,
        call=plan.calls[0],
    )
    ledger.finish_call(
        authorization_id="msft-20260713-ledger",
        plan_id=plan.plan_id,
        call=plan.calls[0],
        status="checkpointed",
        evidence_id=_hash("evidence"),
    )
    records = [
        json.loads(value)
        for value in (tmp_path / "acquisition.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert records[1]["call_kind"] == "split_page"
    assert records[1]["endpoint"] == SPLITS_ENDPOINT
    assert "test-vendor-key" not in json.dumps(records)
