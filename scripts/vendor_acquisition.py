"""Fail-closed MSFT price and corporate-action acquisition operator.

Planning is read-only and needs no vendor credential. Execution is separately
authorized, binds one clean tool revision and database state, and admits only an
ordered split page, dividend page, then exact missing daily-close calls. Every
outbound attempt consumes one typed, fsynced reservation before HTTP admission.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sys
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal, Protocol, cast

from app.config import Settings, get_settings
from app.core.logging import configure_logging
from app.db.session import build_engine
from app.services.corporate_action_store import (
    CorporateActionScopeCoverage,
    PublishedCorporateActionCollection,
    SqlCorporateActionCollectionStore,
)
from app.services.corporate_actions import (
    CORPORATE_ACTION_QUERY_POLICY_HASH,
    CORPORATE_ACTION_SOURCE,
    DIVIDENDS_ENDPOINT,
    SPLITS_ENDPOINT,
    CorporateActionCollectionRecord,
    build_dividend_collection,
    build_split_collection,
)
from data_sources.base import CostRateGuard, DividendPage, OHLCVBar, SplitPage
from data_sources.guards import AsyncPacingCostRateGuard
from data_sources.polygon_open_close import (
    PolygonOpenCloseProvider,
    open_close_endpoint_identity,
)
from scripts.vendor_backfill import (
    BACKFILL_MAX_CALLS_PER_WINDOW,
    BACKFILL_RATE_WINDOW_SECONDS,
    BACKFILL_SOURCE,
    BACKFILL_SYMBOL,
    REQUIRED_SESSIONS,
    AttemptLedger,
    BackfillPlan,
    BackfillRefused,
    ExistingCoverage,
    SqlBackfillStore,
    _build_plan,
    _clean_git_revision,
    _exclusive_backfill,
    _expected_session_dates,
    _safe_settings,
    _validate_current_end,
    _validate_one_session_bar,
)
from scripts.vendor_backfill import (
    DEFAULT_LEDGER_PATH as LEGACY_LEDGER_PATH,
)

AUTHORIZATION_SENTINEL = "stockapi-msft-acquisition-only"
ACQUISITION_SCHEMA_VERSION = 1
MAX_AUTHORIZED_CALLS = REQUIRED_SESSIONS - 1 + 2
GLOBAL_BUDGET_KEY = "polygon_authorized_acquisition"
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LEDGER_PATH = REPO_ROOT / "data" / "vendor_acquisition_attempts.jsonl"
_AUTHORIZATION_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{2,63}$")
_HASH_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_GIT_REVISION_PATTERN = re.compile(r"^[0-9a-f]{40,64}$")
_FAILURE_TYPE_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]{0,127}$")

CallKind = Literal["split_page", "dividend_page", "open_close"]
ActionType = Literal["split", "dividend"]
Clock = Callable[[], datetime]
RevisionFn = Callable[[], str]
SessionsFn = Callable[[date], tuple[date, ...]]


class AcquisitionRefused(BackfillRefused):
    """The requested operation escaped the reviewed acquisition contract."""


class AcquisitionExecutionFailed(RuntimeError):
    """A fail-fast execution stopped after preserving durable checkpoints."""

    def __init__(self, result: dict[str, object]) -> None:
        super().__init__("acquisition execution failed")
        self.result = result


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _content_id(value: object) -> str:
    return "sha256:" + hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _aware_iso(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise AcquisitionRefused("ledger clock must be timezone-aware")
    return value.astimezone(UTC).isoformat()


@dataclass(frozen=True, slots=True)
class AcquisitionCall:
    """One immutable outbound-attempt identity."""

    call_id: str
    kind: CallKind
    endpoint: str
    symbol: str
    start: date
    end: date

    def __post_init__(self) -> None:
        if self.symbol != BACKFILL_SYMBOL or self.start > self.end:
            raise AcquisitionRefused("acquisition call scope is not canonical")
        expected = _call_document(
            kind=self.kind,
            endpoint=self.endpoint,
            symbol=self.symbol,
            start=self.start,
            end=self.end,
        )
        if self.call_id != _content_id(expected):
            raise AcquisitionRefused("acquisition call identity does not match its scope")
        if self.kind == "split_page" and self.endpoint != SPLITS_ENDPOINT:
            raise AcquisitionRefused("split call endpoint is not canonical")
        if self.kind == "dividend_page" and self.endpoint != DIVIDENDS_ENDPOINT:
            raise AcquisitionRefused("dividend call endpoint is not canonical")
        if self.kind == "open_close" and (
            self.start != self.end
            or self.endpoint != open_close_endpoint_identity(self.symbol, self.start)
        ):
            raise AcquisitionRefused("open-close call endpoint is not canonical")

    def ledger_document(self) -> dict[str, object]:
        return {
            **_call_document(
                kind=self.kind,
                endpoint=self.endpoint,
                symbol=self.symbol,
                start=self.start,
                end=self.end,
            ),
            "call_id": self.call_id,
        }


def _call_document(
    *,
    kind: CallKind,
    endpoint: str,
    symbol: str,
    start: date,
    end: date,
) -> dict[str, object]:
    return {
        "format": "stockapi-vendor-acquisition-call-v1",
        "kind": kind,
        "endpoint": endpoint,
        "symbol": symbol,
        "start": start.isoformat(),
        "end": end.isoformat(),
    }


def _make_call(
    kind: CallKind,
    endpoint: str,
    start: date,
    end: date,
) -> AcquisitionCall:
    document = _call_document(
        kind=kind,
        endpoint=endpoint,
        symbol=BACKFILL_SYMBOL,
        start=start,
        end=end,
    )
    return AcquisitionCall(
        call_id=_content_id(document),
        kind=kind,
        endpoint=endpoint,
        symbol=BACKFILL_SYMBOL,
        start=start,
        end=end,
    )


def _call_from_document(value: object) -> AcquisitionCall:
    if not isinstance(value, dict) or set(value) != {
        "format",
        "call_id",
        "kind",
        "endpoint",
        "symbol",
        "start",
        "end",
    }:
        raise ValueError("typed call schema mismatch")
    if value["format"] != "stockapi-vendor-acquisition-call-v1":
        raise ValueError("typed call format is unsupported")
    try:
        kind = cast(CallKind, value["kind"])
        if kind not in {"split_page", "dividend_page", "open_close"}:
            raise ValueError("typed call kind is unsupported")
        call = AcquisitionCall(
            call_id=cast(str, value["call_id"]),
            kind=kind,
            endpoint=cast(str, value["endpoint"]),
            symbol=cast(str, value["symbol"]),
            start=date.fromisoformat(cast(str, value["start"])),
            end=date.fromisoformat(cast(str, value["end"])),
        )
    except (AcquisitionRefused, TypeError, ValueError) as exc:
        raise ValueError("typed call is invalid") from exc
    return call


@dataclass(frozen=True, slots=True)
class ActionScopeState:
    """Exact immutable collection state for one action query."""

    action_type: ActionType
    endpoint: str
    coverage: CorporateActionScopeCoverage

    def __post_init__(self) -> None:
        if not (
            self.coverage.action_type == self.action_type
            and self.coverage.endpoint == self.endpoint
            and self.coverage.source == CORPORATE_ACTION_SOURCE
            and self.coverage.symbol == BACKFILL_SYMBOL
            and self.coverage.query_policy_hash == CORPORATE_ACTION_QUERY_POLICY_HASH
        ):
            raise AcquisitionRefused("corporate-action coverage escaped the pinned query policy")

    @property
    def complete(self) -> bool:
        return bool(self.coverage.complete)

    @property
    def repairable_ids(self) -> tuple[str, ...]:
        return tuple(value.collection_id for value in self.coverage.repairable)

    @property
    def ambiguous(self) -> bool:
        return len(self.repairable_ids) > 1

    @property
    def requires_call(self) -> bool:
        return not self.complete and not self.repairable_ids

    @property
    def requires_repair(self) -> bool:
        return bool(self.repairable_ids)

    def canonical_document(self) -> dict[str, object]:
        return {
            "action_type": self.action_type,
            "endpoint": self.endpoint,
            "source": self.coverage.source,
            "symbol": self.coverage.symbol,
            "coverage_start": self.coverage.coverage_start.isoformat(),
            "coverage_end": self.coverage.coverage_end.isoformat(),
            "query_policy_hash": self.coverage.query_policy_hash,
            "collections": [
                {
                    "collection_id": value.collection_id,
                    "collection_recorded_at": _aware_iso(value.collection_recorded_at),
                    "fetched_at": _aware_iso(value.fetched_at),
                    "event_count": value.event_count,
                    "available_at": (
                        _aware_iso(value.available_at) if value.available_at is not None else None
                    ),
                }
                for value in self.coverage.collections
            ],
        }


@dataclass(frozen=True, slots=True)
class AcquisitionPlan:
    """Content-addressed authorization scope around the stable price plan."""

    price_plan: BackfillPlan
    split_state: ActionScopeState
    dividend_state: ActionScopeState
    calls: tuple[AcquisitionCall, ...]
    calls_sha256: str
    ambiguous_call_ids: tuple[str, ...]
    plan_id: str

    @property
    def tool_revision(self) -> str:
        return self.price_plan.tool_revision

    @property
    def end_session(self) -> date:
        return self.price_plan.end_session

    @property
    def smoke_anchor_present(self) -> bool:
        return self.price_plan.smoke_anchor_present

    @property
    def allocation(self) -> dict[str, int]:
        return {
            "split_page": sum(value.kind == "split_page" for value in self.calls),
            "dividend_page": sum(value.kind == "dividend_page" for value in self.calls),
            "open_close": sum(value.kind == "open_close" for value in self.calls),
        }

    @property
    def required_outbound_attempts(self) -> int:
        return len(self.calls)

    @property
    def receipt_repairs_required(self) -> int:
        return (
            len(self.price_plan.repairable_dates)
            + len(self.split_state.repairable_ids)
            + len(self.dividend_state.repairable_ids)
        )

    @property
    def blocked(self) -> bool:
        return (
            not self.smoke_anchor_present
            or self.split_state.ambiguous
            or self.dividend_state.ambiguous
            or bool(self.ambiguous_call_ids)
            or self.required_outbound_attempts > MAX_AUTHORIZED_CALLS
        )

    def public_result(self) -> dict[str, object]:
        if self.blocked:
            status = "blocked"
        elif self.calls or self.receipt_repairs_required:
            status = "ready"
        else:
            status = "complete"
        return {
            "status": status,
            "plan_id": self.plan_id,
            "tool_revision": self.tool_revision,
            "authorization": AUTHORIZATION_SENTINEL,
            "symbol": BACKFILL_SYMBOL,
            "window_start": self.price_plan.expected_dates[0].isoformat(),
            "window_end": self.end_session.isoformat(),
            "calls_sha256": self.calls_sha256,
            "required_outbound_attempts": self.required_outbound_attempts,
            "call_allocation": self.allocation,
            "receipt_repairs_required": self.receipt_repairs_required,
            "ambiguous_prior_attempts": len(self.ambiguous_call_ids),
            "smoke_anchor_present": self.smoke_anchor_present,
            "corporate_action_query_policy_hash": CORPORATE_ACTION_QUERY_POLICY_HASH,
            "max_calls_per_window": BACKFILL_MAX_CALLS_PER_WINDOW,
            "rate_window_seconds": BACKFILL_RATE_WINDOW_SECONDS,
            "hard_max_authorized_calls": MAX_AUTHORIZED_CALLS,
        }


def _validate_call_order(calls: tuple[AcquisitionCall, ...]) -> None:
    ranks = {"split_page": 0, "dividend_page": 1, "open_close": 2}
    if [ranks[value.kind] for value in calls] != sorted(ranks[value.kind] for value in calls):
        raise AcquisitionRefused("acquisition calls are not action-first")
    if sum(value.kind == "split_page" for value in calls) > 1:
        raise AcquisitionRefused("acquisition plan contains multiple split pages")
    if sum(value.kind == "dividend_page" for value in calls) > 1:
        raise AcquisitionRefused("acquisition plan contains multiple dividend pages")
    price_dates = [value.start for value in calls if value.kind == "open_close"]
    if price_dates != sorted(set(price_dates)):
        raise AcquisitionRefused("open-close calls are not unique and ordered")


def _build_acquisition_plan(
    price_plan: BackfillPlan,
    split_coverage: CorporateActionScopeCoverage,
    dividend_coverage: CorporateActionScopeCoverage,
    *,
    unresolved_call_ids: tuple[str, ...] = (),
    legacy_unresolved_dates: tuple[date, ...] = (),
) -> AcquisitionPlan:
    window_start = price_plan.expected_dates[0]
    window_end = price_plan.end_session
    split_state = ActionScopeState("split", SPLITS_ENDPOINT, split_coverage)
    dividend_state = ActionScopeState("dividend", DIVIDENDS_ENDPOINT, dividend_coverage)
    if any(
        state.coverage.coverage_start != window_start or state.coverage.coverage_end != window_end
        for state in (split_state, dividend_state)
    ):
        raise AcquisitionRefused("corporate-action coverage escaped the price window")
    calls: list[AcquisitionCall] = []
    if split_state.requires_call:
        calls.append(_make_call("split_page", SPLITS_ENDPOINT, window_start, window_end))
    if dividend_state.requires_call:
        calls.append(_make_call("dividend_page", DIVIDENDS_ENDPOINT, window_start, window_end))
    calls.extend(
        _make_call(
            "open_close",
            open_close_endpoint_identity(BACKFILL_SYMBOL, session_date),
            session_date,
            session_date,
        )
        for session_date in price_plan.missing_dates
    )
    ordered = tuple(calls)
    _validate_call_order(ordered)
    call_ids = {value.call_id for value in ordered}
    legacy_ids = {
        _make_call(
            "open_close",
            open_close_endpoint_identity(BACKFILL_SYMBOL, session_date),
            session_date,
            session_date,
        ).call_id
        for session_date in legacy_unresolved_dates
        if session_date in price_plan.missing_dates
    }
    ambiguous = tuple(sorted(call_ids.intersection((*unresolved_call_ids, *legacy_ids))))
    calls_document = [value.ledger_document() for value in ordered]
    calls_sha256 = _content_id(calls_document)
    canonical = {
        "format": "stockapi-vendor-acquisition-plan-v1",
        "schema_version": ACQUISITION_SCHEMA_VERSION,
        "authorization": AUTHORIZATION_SENTINEL,
        "tool_revision": price_plan.tool_revision,
        "symbol": BACKFILL_SYMBOL,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "price_plan_id": price_plan.plan_id,
        "split_state": split_state.canonical_document(),
        "dividend_state": dividend_state.canonical_document(),
        "calls": calls_document,
        "calls_sha256": calls_sha256,
        "ambiguous_call_ids": list(ambiguous),
        "max_calls_per_window": BACKFILL_MAX_CALLS_PER_WINDOW,
        "rate_window_seconds": BACKFILL_RATE_WINDOW_SECONDS,
        "hard_max_authorized_calls": MAX_AUTHORIZED_CALLS,
    }
    return AcquisitionPlan(
        price_plan=price_plan,
        split_state=split_state,
        dividend_state=dividend_state,
        calls=ordered,
        calls_sha256=calls_sha256,
        ambiguous_call_ids=ambiguous,
        plan_id=_content_id(canonical),
    )


class AcquisitionStore(Protocol):
    async def price_coverage(self, session_dates: tuple[date, ...]) -> ExistingCoverage: ...

    async def repair_price_receipts(self, session_dates: tuple[date, ...]) -> int: ...

    async def persist_price(self, bar: OHLCVBar) -> None: ...

    async def action_coverage(
        self,
        action_type: ActionType,
        start: date,
        end: date,
    ) -> CorporateActionScopeCoverage: ...

    async def repair_action_receipt(
        self,
        collection_id: str,
    ) -> PublishedCorporateActionCollection: ...

    async def persist_action(
        self,
        record: CorporateActionCollectionRecord,
    ) -> PublishedCorporateActionCollection: ...


class AcquisitionProvider(Protocol):
    name: str

    async def __aenter__(self) -> AcquisitionProvider: ...

    async def __aexit__(self, *exc_info: object) -> None: ...

    async def get_splits(self, symbol: str, *, start: date, end: date) -> SplitPage: ...

    async def get_dividends(self, symbol: str, *, start: date, end: date) -> DividendPage: ...

    async def get_daily_bars(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjusted: bool = False,
    ) -> list[OHLCVBar]: ...


class SqlAcquisitionStore:
    """Compose the proven price store with the collection publisher."""

    def __init__(self, settings: Settings) -> None:
        self._prices = SqlBackfillStore(settings)
        self._action_engine = build_engine(settings)
        self._actions = SqlCorporateActionCollectionStore(self._action_engine)

    async def __aenter__(self) -> SqlAcquisitionStore:
        await self._prices.__aenter__()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        try:
            await self._prices.__aexit__(*exc_info)
        finally:
            await self._action_engine.dispose()

    async def price_coverage(self, session_dates: tuple[date, ...]) -> ExistingCoverage:
        return await self._prices.coverage(session_dates)

    async def repair_price_receipts(self, session_dates: tuple[date, ...]) -> int:
        return await self._prices.repair_receipts(session_dates)

    async def persist_price(self, bar: OHLCVBar) -> None:
        await self._prices.persist(bar)

    async def action_coverage(
        self,
        action_type: ActionType,
        start: date,
        end: date,
    ) -> CorporateActionScopeCoverage:
        endpoint = SPLITS_ENDPOINT if action_type == "split" else DIVIDENDS_ENDPOINT
        return await self._actions.scope_coverage(
            action_type=action_type,
            endpoint=endpoint,
            source=CORPORATE_ACTION_SOURCE,
            symbol=BACKFILL_SYMBOL,
            coverage_start=start,
            coverage_end=end,
            query_policy_hash=CORPORATE_ACTION_QUERY_POLICY_HASH,
        )

    async def repair_action_receipt(
        self,
        collection_id: str,
    ) -> PublishedCorporateActionCollection:
        return await self._actions.publish_receipt(collection_id)

    async def persist_action(
        self,
        record: CorporateActionCollectionRecord,
    ) -> PublishedCorporateActionCollection:
        return await self._actions.publish(record)


StoreFactory = Callable[[Settings], AbstractAsyncContextManager[AcquisitionStore]]
LockFn = Callable[[Settings], AbstractAsyncContextManager[None]]


@asynccontextmanager
async def _sql_store(settings: Settings) -> AsyncIterator[AcquisitionStore]:
    async with SqlAcquisitionStore(settings) as store:
        yield store


class PlanBoundGlobalGuard(CostRateGuard):
    """Admit one armed typed call into one global vendor budget."""

    def __init__(
        self,
        *,
        total_budget: int,
        end_session: date,
        clock: Clock,
    ) -> None:
        self._armed: AcquisitionCall | None = None
        self._acquired = False

        def admission_check() -> None:
            try:
                _validate_current_end(end_session, clock())
            except BackfillRefused as exc:
                raise AcquisitionRefused(str(exc)) from None

        self._pacer = AsyncPacingCostRateGuard(
            max_calls_per_window=BACKFILL_MAX_CALLS_PER_WINDOW,
            window_seconds=BACKFILL_RATE_WINDOW_SECONDS,
            total_budget=total_budget,
            admission_check=admission_check,
        )

    def arm(self, call: AcquisitionCall) -> None:
        if self._armed is not None:
            raise AcquisitionRefused("vendor guard is already armed")
        self._armed = call
        self._acquired = False

    def finish(self, call: AcquisitionCall) -> bool:
        if self._armed != call:
            raise AcquisitionRefused("vendor guard completion does not match its call")
        acquired = self._acquired
        self._armed = None
        self._acquired = False
        return acquired

    async def acquire(
        self,
        vendor: str,
        *,
        cost: int = 1,
        endpoint: str | None = None,
    ) -> None:
        call = self._armed
        if (
            call is None
            or self._acquired
            or vendor not in {"polygon", BACKFILL_SOURCE}
            or cost != 1
            or endpoint != call.endpoint
        ):
            raise AcquisitionRefused("vendor attempt does not match the armed acquisition call")
        await self._pacer.acquire(GLOBAL_BUDGET_KEY, cost=1, endpoint=endpoint)
        self._acquired = True

    @property
    def spent(self) -> int:
        return self._pacer.snapshot(GLOBAL_BUDGET_KEY)["spent"]


ProviderFactory = Callable[[Settings, PlanBoundGlobalGuard], AcquisitionProvider]


def _default_provider(
    settings: Settings,
    guard: PlanBoundGlobalGuard,
) -> AcquisitionProvider:
    key = settings.polygon_api_key or ""
    if not key:
        raise AcquisitionRefused("POLYGON_API_KEY must be non-empty in .env")
    return PolygonOpenCloseProvider(key, guard=guard, max_attempts=1)


class AcquisitionLedger:
    """Strict append-only authorization and typed-attempt ledger."""

    def __init__(self, path: Path = DEFAULT_LEDGER_PATH, *, clock: Clock = _utcnow) -> None:
        self.path = path
        self.clock = clock

    def begin_authorization(
        self,
        *,
        authorization_id: str,
        plan: AcquisitionPlan,
        max_calls: int,
    ) -> None:
        _validate_authorization_id(authorization_id)
        if (
            max_calls != plan.required_outbound_attempts
            or not 1 <= max_calls <= MAX_AUTHORIZED_CALLS
        ):
            raise AcquisitionRefused("authorization max_calls must equal the exact typed call set")
        records = self._records()
        if any(record.get("authorization_id") == authorization_id for record in records):
            raise AcquisitionRefused("authorization_id is already consumed; obtain a fresh grant")
        self._append(
            {
                "record_type": "authorization",
                "schema_version": ACQUISITION_SCHEMA_VERSION,
                "authorization": AUTHORIZATION_SENTINEL,
                "authorization_id": authorization_id,
                "plan_id": plan.plan_id,
                "tool_revision": plan.tool_revision,
                "symbol": BACKFILL_SYMBOL,
                "window_start": plan.price_plan.expected_dates[0].isoformat(),
                "window_end": plan.end_session.isoformat(),
                "calls_sha256": plan.calls_sha256,
                "calls": [value.ledger_document() for value in plan.calls],
                "allocation": plan.allocation,
                "max_calls": max_calls,
                "recorded_at": _aware_iso(self.clock()),
            }
        )

    def reserve_call(
        self,
        *,
        authorization_id: str,
        plan_id: str,
        call: AcquisitionCall,
    ) -> int:
        records = self._records()
        header = _one_header(records, authorization_id, plan_id)
        ordered_calls = [
            _call_from_document(value) for value in cast(list[object], header["calls"])
        ]
        allowed = {value.call_id: value.ledger_document() for value in ordered_calls}
        if allowed.get(call.call_id) != call.ledger_document():
            raise AcquisitionRefused("typed call is outside the authorization ledger")
        attempts = _authorization_attempts(records, authorization_id, plan_id)
        if any(record["call_id"] == call.call_id for record in attempts):
            raise AcquisitionRefused("this authorization already reserved the typed call")
        positions = {value.call_id: index for index, value in enumerate(ordered_calls)}
        attempted_ids = {cast(str, record["call_id"]) for record in attempts}
        earlier_action_ids = {
            value.call_id
            for value in ordered_calls[: positions[call.call_id]]
            if value.kind in {"split_page", "dividend_page"}
        }
        if not earlier_action_ids.issubset(attempted_ids):
            raise AcquisitionRefused("typed call reservation violates action-first order")
        if attempts and positions[call.call_id] <= max(
            positions[cast(str, record["call_id"])] for record in attempts
        ):
            raise AcquisitionRefused("typed call reservation violates action-first order")
        max_calls = cast(int, header["max_calls"])
        if len(attempts) >= max_calls:
            raise AcquisitionRefused("authorization global call budget is exhausted")
        attempt_number = len(attempts) + 1
        self._append(
            {
                "record_type": "attempt",
                "schema_version": ACQUISITION_SCHEMA_VERSION,
                "authorization_id": authorization_id,
                "plan_id": plan_id,
                "attempt_number": attempt_number,
                "call_id": call.call_id,
                "call_kind": call.kind,
                "endpoint": call.endpoint,
                "symbol": call.symbol,
                "start": call.start.isoformat(),
                "end": call.end.isoformat(),
                "reserved_at": _aware_iso(self.clock()),
            }
        )
        return attempt_number

    def finish_call(
        self,
        *,
        authorization_id: str,
        plan_id: str,
        call: AcquisitionCall,
        status: Literal["checkpointed", "failed"],
        evidence_id: str | None = None,
        failure_type: str | None = None,
    ) -> None:
        if status == "checkpointed":
            if (
                evidence_id is None
                or not _HASH_PATTERN.fullmatch(evidence_id)
                or failure_type is not None
            ):
                raise AcquisitionRefused("checkpointed outcome evidence is invalid")
        elif (
            status != "failed"
            or evidence_id is not None
            or failure_type is None
            or _FAILURE_TYPE_PATTERN.fullmatch(failure_type) is None
        ):
            raise AcquisitionRefused("failed outcome detail is invalid")
        records = self._records()
        _one_header(records, authorization_id, plan_id)
        attempts = [
            record
            for record in _authorization_attempts(records, authorization_id, plan_id)
            if record["call_id"] == call.call_id
        ]
        outcomes = [
            record
            for record in records
            if record.get("record_type") == "outcome"
            and record.get("authorization_id") == authorization_id
            and record.get("plan_id") == plan_id
            and record.get("call_id") == call.call_id
        ]
        if len(attempts) != 1 or outcomes:
            raise AcquisitionRefused("outcome does not identify one open typed reservation")
        self._append(
            {
                "record_type": "outcome",
                "schema_version": ACQUISITION_SCHEMA_VERSION,
                "authorization_id": authorization_id,
                "plan_id": plan_id,
                "call_id": call.call_id,
                "call_kind": call.kind,
                "status": status,
                "evidence_id": evidence_id,
                "failure_type": failure_type,
                "recorded_at": _aware_iso(self.clock()),
            }
        )

    def attempt_count(self, authorization_id: str) -> int:
        return sum(
            record.get("record_type") == "attempt"
            and record.get("authorization_id") == authorization_id
            for record in self._records()
        )

    def unresolved_call_ids(self) -> tuple[str, ...]:
        records = self._records()
        attempts = {
            (str(record["authorization_id"]), str(record["plan_id"]), str(record["call_id"]))
            for record in records
            if record["record_type"] == "attempt"
        }
        outcomes = {
            (str(record["authorization_id"]), str(record["plan_id"]), str(record["call_id"]))
            for record in records
            if record["record_type"] == "outcome"
        }
        return tuple(sorted(call_id for _, _, call_id in attempts.difference(outcomes)))

    def _records(self) -> list[dict[str, object]]:
        if not self.path.exists():
            return []
        try:
            records: list[dict[str, object]] = []
            for line_number, line in enumerate(
                self.path.read_text(encoding="utf-8").splitlines(), start=1
            ):
                if not line:
                    raise ValueError(f"blank line {line_number}")
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise ValueError(f"non-object line {line_number}")
                records.append(value)
            self._validate_records(records)
            return records
        except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
            raise AcquisitionRefused(
                "acquisition ledger is unreadable; stop for forensics"
            ) from exc

    @staticmethod
    def _validate_records(records: list[dict[str, object]]) -> None:
        authorization_schema = {
            "record_type",
            "schema_version",
            "authorization",
            "authorization_id",
            "plan_id",
            "tool_revision",
            "symbol",
            "window_start",
            "window_end",
            "calls_sha256",
            "calls",
            "allocation",
            "max_calls",
            "recorded_at",
        }
        attempt_schema = {
            "record_type",
            "schema_version",
            "authorization_id",
            "plan_id",
            "attempt_number",
            "call_id",
            "call_kind",
            "endpoint",
            "symbol",
            "start",
            "end",
            "reserved_at",
        }
        outcome_schema = {
            "record_type",
            "schema_version",
            "authorization_id",
            "plan_id",
            "call_id",
            "call_kind",
            "status",
            "evidence_id",
            "failure_type",
            "recorded_at",
        }
        headers: dict[str, dict[str, object]] = {}
        attempts: dict[tuple[str, str], dict[str, object]] = {}
        outcomes: set[tuple[str, str]] = set()
        attempt_counts: dict[str, int] = {}
        last_call_positions: dict[str, int] = {}
        for record in records:
            record_type = record.get("record_type")
            expected_schema = {
                "authorization": authorization_schema,
                "attempt": attempt_schema,
                "outcome": outcome_schema,
            }.get(cast(str, record_type))
            if expected_schema is None or set(record) != expected_schema:
                raise ValueError("ledger record schema mismatch")
            if record["schema_version"] != ACQUISITION_SCHEMA_VERSION:
                raise ValueError("ledger schema version mismatch")
            authorization_id = record["authorization_id"]
            plan_id = record["plan_id"]
            if (
                not isinstance(authorization_id, str)
                or _AUTHORIZATION_ID_PATTERN.fullmatch(authorization_id) is None
                or not isinstance(plan_id, str)
                or _HASH_PATTERN.fullmatch(plan_id) is None
            ):
                raise ValueError("ledger identity is invalid")
            if record_type == "authorization":
                if authorization_id in headers:
                    raise ValueError("duplicate ledger authorization")
                calls_value = record["calls"]
                if not isinstance(calls_value, list):
                    raise ValueError("authorization calls must be an array")
                calls = tuple(_call_from_document(value) for value in calls_value)
                _validate_call_order(calls)
                allocation = record["allocation"]
                expected_allocation = {
                    "split_page": sum(value.kind == "split_page" for value in calls),
                    "dividend_page": sum(value.kind == "dividend_page" for value in calls),
                    "open_close": sum(value.kind == "open_close" for value in calls),
                }
                max_calls = record["max_calls"]
                if (
                    record["authorization"] != AUTHORIZATION_SENTINEL
                    or record["symbol"] != BACKFILL_SYMBOL
                    or not isinstance(record["tool_revision"], str)
                    or _GIT_REVISION_PATTERN.fullmatch(record["tool_revision"]) is None
                    or allocation != expected_allocation
                    or isinstance(max_calls, bool)
                    or not isinstance(max_calls, int)
                    or max_calls != len(calls)
                    or not 1 <= max_calls <= MAX_AUTHORIZED_CALLS
                    or record["calls_sha256"]
                    != _content_id([value.ledger_document() for value in calls])
                ):
                    raise ValueError("authorization header is invalid")
                start = _parse_ledger_date(record["window_start"])
                end = _parse_ledger_date(record["window_end"])
                if start > end:
                    raise ValueError("authorization window is inverted")
                _parse_ledger_timestamp(record["recorded_at"])
                headers[authorization_id] = record
                attempt_counts[authorization_id] = 0
                last_call_positions[authorization_id] = -1
                continue

            header = headers.get(authorization_id)
            if header is None or header["plan_id"] != plan_id:
                raise ValueError("ledger child precedes or mismatches authorization")
            call_id = record["call_id"]
            call_kind = record["call_kind"]
            if not isinstance(call_id, str) or not isinstance(call_kind, str):
                raise ValueError("ledger call identity is invalid")
            call_map = {
                value.call_id: value
                for value in (
                    _call_from_document(item) for item in cast(list[object], header["calls"])
                )
            }
            call_positions = {value.call_id: index for index, value in enumerate(call_map.values())}
            ordered_header_calls = list(call_map.values())
            call = call_map.get(call_id)
            if call is None or call.kind != call_kind:
                raise ValueError("ledger call escaped authorization")
            key = (authorization_id, call_id)
            if record_type == "attempt":
                attempt_number = record["attempt_number"]
                expected_number = attempt_counts[authorization_id] + 1
                earlier_action_ids = {
                    value.call_id
                    for value in ordered_header_calls[: call_positions[call_id]]
                    if value.kind in {"split_page", "dividend_page"}
                }
                attempted_ids = {
                    attempt_call_id
                    for attempt_authorization_id, attempt_call_id in attempts
                    if attempt_authorization_id == authorization_id
                }
                if (
                    key in attempts
                    or isinstance(attempt_number, bool)
                    or not isinstance(attempt_number, int)
                    or attempt_number != expected_number
                    or record["endpoint"] != call.endpoint
                    or record["symbol"] != call.symbol
                    or record["start"] != call.start.isoformat()
                    or record["end"] != call.end.isoformat()
                    or call_positions[call_id] <= last_call_positions[authorization_id]
                    or not earlier_action_ids.issubset(attempted_ids)
                ):
                    raise ValueError("ledger attempt is invalid")
                _parse_ledger_timestamp(record["reserved_at"])
                attempts[key] = record
                attempt_counts[authorization_id] = expected_number
                last_call_positions[authorization_id] = call_positions[call_id]
                continue
            status = record["status"]
            evidence_id = record["evidence_id"]
            failure_type = record["failure_type"]
            if key not in attempts or key in outcomes or status not in {"checkpointed", "failed"}:
                raise ValueError("ledger outcome is invalid")
            if status == "checkpointed":
                if (
                    not isinstance(evidence_id, str)
                    or _HASH_PATTERN.fullmatch(evidence_id) is None
                    or failure_type is not None
                ):
                    raise ValueError("checkpointed outcome evidence is invalid")
            elif (
                evidence_id is not None
                or not isinstance(failure_type, str)
                or _FAILURE_TYPE_PATTERN.fullmatch(failure_type) is None
            ):
                raise ValueError("failed outcome detail is invalid")
            _parse_ledger_timestamp(record["recorded_at"])
            outcomes.add(key)

    def _append(self, record: dict[str, object]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = _canonical_bytes(record) + b"\n"
        descriptor = os.open(self.path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
        try:
            offset = 0
            while offset < len(payload):
                offset += os.write(descriptor, payload[offset:])
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _one_header(
    records: list[dict[str, object]],
    authorization_id: str,
    plan_id: str,
) -> dict[str, object]:
    headers = [
        record
        for record in records
        if record.get("record_type") == "authorization"
        and record.get("authorization_id") == authorization_id
        and record.get("plan_id") == plan_id
    ]
    if len(headers) != 1:
        raise AcquisitionRefused("authorization ledger header is missing or ambiguous")
    return headers[0]


def _authorization_attempts(
    records: list[dict[str, object]],
    authorization_id: str,
    plan_id: str,
) -> list[dict[str, object]]:
    return [
        record
        for record in records
        if record.get("record_type") == "attempt"
        and record.get("authorization_id") == authorization_id
        and record.get("plan_id") == plan_id
    ]


def _parse_ledger_date(value: object) -> date:
    if not isinstance(value, str):
        raise ValueError("ledger date must be text")
    return date.fromisoformat(value)


def _parse_ledger_timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("ledger timestamp must be text")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("ledger timestamp must be timezone-aware")
    return parsed


def _validate_authorization_id(value: str) -> None:
    if _AUTHORIZATION_ID_PATTERN.fullmatch(value) is None:
        raise AcquisitionRefused("authorization_id must be 3-64 lowercase safe characters")


async def _current_plan(
    *,
    store: AcquisitionStore,
    end_session: date,
    expected_dates: tuple[date, ...],
    tool_revision: str,
    ledger: AcquisitionLedger,
    legacy_ledger_path: Path,
) -> AcquisitionPlan:
    window_start = expected_dates[0]
    price_plan = _build_plan(
        end_session,
        expected_dates,
        await store.price_coverage(expected_dates),
        tool_revision=tool_revision,
    )
    split_coverage, dividend_coverage = await asyncio.gather(
        store.action_coverage("split", window_start, end_session),
        store.action_coverage("dividend", window_start, end_session),
    )
    return _build_acquisition_plan(
        price_plan,
        split_coverage,
        dividend_coverage,
        unresolved_call_ids=ledger.unresolved_call_ids(),
        legacy_unresolved_dates=AttemptLedger(legacy_ledger_path).unresolved_dates(),
    )


async def plan_acquisition(
    *,
    end_session: date,
    settings: Settings | None = None,
    clock: Clock = _utcnow,
    store_factory: StoreFactory = _sql_store,
    sessions_fn: SessionsFn = _expected_session_dates,
    revision_fn: RevisionFn = _clean_git_revision,
    ledger_path: Path = DEFAULT_LEDGER_PATH,
    legacy_ledger_path: Path = LEGACY_LEDGER_PATH,
) -> AcquisitionPlan:
    safe_settings = _safe_settings(settings or get_settings(), require_key=False)
    tool_revision = revision_fn()
    _validate_current_end(end_session, clock())
    expected_dates = sessions_fn(end_session)
    ledger = AcquisitionLedger(ledger_path, clock=clock)
    async with store_factory(safe_settings) as store:
        return await _current_plan(
            store=store,
            end_session=end_session,
            expected_dates=expected_dates,
            tool_revision=tool_revision,
            ledger=ledger,
            legacy_ledger_path=legacy_ledger_path,
        )


async def _repair_plan_receipts(
    store: AcquisitionStore,
    plan: AcquisitionPlan,
) -> int:
    repaired = await store.repair_price_receipts(plan.price_plan.repairable_dates)
    for state in (plan.split_state, plan.dividend_state):
        if state.ambiguous:
            raise AcquisitionRefused("multiple unreceipted action collections require forensics")
        for collection_id in state.repairable_ids:
            await store.repair_action_receipt(collection_id)
            repaired += 1
    return repaired


async def repair_acquisition(
    *,
    end_session: date,
    plan_id: str,
    settings: Settings | None = None,
    clock: Clock = _utcnow,
    store_factory: StoreFactory = _sql_store,
    lock_fn: LockFn = _exclusive_backfill,
    sessions_fn: SessionsFn = _expected_session_dates,
    revision_fn: RevisionFn = _clean_git_revision,
    ledger_path: Path = DEFAULT_LEDGER_PATH,
    legacy_ledger_path: Path = LEGACY_LEDGER_PATH,
) -> dict[str, object]:
    if _HASH_PATTERN.fullmatch(plan_id) is None:
        raise AcquisitionRefused("plan_id must be a sha256 digest from the plan command")
    safe_settings = _safe_settings(settings or get_settings(), require_key=False)
    tool_revision = revision_fn()
    _validate_current_end(end_session, clock())
    expected_dates = sessions_fn(end_session)
    ledger = AcquisitionLedger(ledger_path, clock=clock)
    async with lock_fn(safe_settings), store_factory(safe_settings) as store:
        plan = await _current_plan(
            store=store,
            end_session=end_session,
            expected_dates=expected_dates,
            tool_revision=tool_revision,
            ledger=ledger,
            legacy_ledger_path=legacy_ledger_path,
        )
        if plan.plan_id != plan_id:
            raise AcquisitionRefused("database state no longer matches the repair plan_id")
        if plan.split_state.ambiguous or plan.dividend_state.ambiguous:
            raise AcquisitionRefused("action receipt state is ambiguous; stop for forensics")
        if not plan.receipt_repairs_required:
            raise AcquisitionRefused("no receipt-only repairs are required")
        repaired = await _repair_plan_receipts(store, plan)
        final_plan = await _current_plan(
            store=store,
            end_session=end_session,
            expected_dates=expected_dates,
            tool_revision=tool_revision,
            ledger=ledger,
            legacy_ledger_path=legacy_ledger_path,
        )
        if final_plan.receipt_repairs_required:
            raise AcquisitionRefused("receipt-only repair postflight is incomplete")
    return {
        "status": "ok",
        "original_plan_id": plan.plan_id,
        "new_plan_id": final_plan.plan_id,
        "symbol": BACKFILL_SYMBOL,
        "receipts_repaired": repaired,
        "remaining_outbound_attempts": final_plan.required_outbound_attempts,
        "outbound_attempts": 0,
    }


async def _existing_call_evidence(
    store: AcquisitionStore,
    call: AcquisitionCall,
) -> str | None:
    if call.kind == "open_close":
        price_coverage = await store.price_coverage((call.start,))
        versions = dict(price_coverage.version_ids)
        if call.start in price_coverage.complete_dates:
            return versions[call.start]
        if call.start in price_coverage.repairable_dates:
            await store.repair_price_receipts((call.start,))
            repaired = await store.price_coverage((call.start,))
            if call.start not in repaired.complete_dates:
                raise AcquisitionRefused("price receipt repair did not complete")
            return dict(repaired.version_ids)[call.start]
        return None
    action_type: ActionType = "split" if call.kind == "split_page" else "dividend"
    action_coverage = await store.action_coverage(action_type, call.start, call.end)
    if len(action_coverage.repairable) > 1:
        raise AcquisitionRefused("multiple unreceipted action collections require forensics")
    if action_coverage.repairable:
        await store.repair_action_receipt(action_coverage.repairable[0].collection_id)
        action_coverage = await store.action_coverage(action_type, call.start, call.end)
    if action_coverage.newest_complete is not None:
        return action_coverage.newest_complete.collection_id
    return None


async def _perform_call(
    store: AcquisitionStore,
    provider: AcquisitionProvider,
    call: AcquisitionCall,
) -> str:
    if call.kind == "split_page":
        split_page = await provider.get_splits(BACKFILL_SYMBOL, start=call.start, end=call.end)
        record = build_split_collection(split_page)
        published = await store.persist_action(record)
        if published.collection_id != record.collection_id:
            raise AcquisitionRefused("split collection checkpoint identity changed")
        evidence = await _existing_call_evidence(store, call)
        if evidence != record.collection_id:
            raise AcquisitionRefused("split collection lacks its exact post-commit receipt")
        return record.collection_id
    if call.kind == "dividend_page":
        dividend_page = await provider.get_dividends(
            BACKFILL_SYMBOL, start=call.start, end=call.end
        )
        record = build_dividend_collection(dividend_page)
        published = await store.persist_action(record)
        if published.collection_id != record.collection_id:
            raise AcquisitionRefused("dividend collection checkpoint identity changed")
        evidence = await _existing_call_evidence(store, call)
        if evidence != record.collection_id:
            raise AcquisitionRefused("dividend collection lacks its exact post-commit receipt")
        return record.collection_id
    bars = await provider.get_daily_bars(
        BACKFILL_SYMBOL,
        call.start,
        call.end,
        adjusted=False,
    )
    bar = _validate_one_session_bar(call.start, bars)
    await store.persist_price(bar)
    evidence = await _existing_call_evidence(store, call)
    if evidence is None:
        raise AcquisitionRefused("price checkpoint lacks its exact post-commit receipt")
    return evidence


def _allocation_matches(
    plan: AcquisitionPlan,
    *,
    max_calls: int,
    split_calls: int,
    dividend_calls: int,
    open_close_calls: int,
) -> bool:
    return (
        plan.allocation
        == {
            "split_page": split_calls,
            "dividend_page": dividend_calls,
            "open_close": open_close_calls,
        }
        and max_calls == split_calls + dividend_calls + open_close_calls
        and max_calls == plan.required_outbound_attempts
    )


async def execute_acquisition(
    *,
    end_session: date,
    plan_id: str,
    max_calls: int,
    split_calls: int,
    dividend_calls: int,
    open_close_calls: int,
    authorization: str,
    authorization_id: str,
    settings: Settings | None = None,
    clock: Clock = _utcnow,
    store_factory: StoreFactory = _sql_store,
    provider_factory: ProviderFactory = _default_provider,
    lock_fn: LockFn = _exclusive_backfill,
    sessions_fn: SessionsFn = _expected_session_dates,
    revision_fn: RevisionFn = _clean_git_revision,
    ledger_path: Path = DEFAULT_LEDGER_PATH,
    legacy_ledger_path: Path = LEGACY_LEDGER_PATH,
) -> dict[str, object]:
    if authorization != AUTHORIZATION_SENTINEL:
        raise AcquisitionRefused(f"authorization must be exactly {AUTHORIZATION_SENTINEL}")
    _validate_authorization_id(authorization_id)
    if _HASH_PATTERN.fullmatch(plan_id) is None:
        raise AcquisitionRefused("plan_id must be a sha256 digest from the plan command")
    if (
        not 1 <= max_calls <= MAX_AUTHORIZED_CALLS
        or split_calls not in {0, 1}
        or dividend_calls not in {0, 1}
        or not 0 <= open_close_calls <= REQUIRED_SESSIONS - 1
    ):
        raise AcquisitionRefused("typed call allocation is outside the acquisition contract")
    safe_settings = _safe_settings(settings or get_settings(), require_key=True)
    tool_revision = revision_fn()
    _validate_current_end(end_session, clock())
    expected_dates = sessions_fn(end_session)
    ledger = AcquisitionLedger(ledger_path, clock=clock)

    async with lock_fn(safe_settings), store_factory(safe_settings) as store:
        plan = await _current_plan(
            store=store,
            end_session=end_session,
            expected_dates=expected_dates,
            tool_revision=tool_revision,
            ledger=ledger,
            legacy_ledger_path=legacy_ledger_path,
        )
        if plan.plan_id != plan_id:
            raise AcquisitionRefused("database state no longer matches the authorized plan_id")
        if not plan.smoke_anchor_present:
            raise AcquisitionRefused("latest-session smoke bar and receipt must exist first")
        if plan.split_state.ambiguous or plan.dividend_state.ambiguous:
            raise AcquisitionRefused("action receipt state is ambiguous; stop for forensics")
        if plan.ambiguous_call_ids:
            raise AcquisitionRefused("an unresolved prior attempt overlaps required work")
        if not plan.calls:
            raise AcquisitionRefused("no outbound vendor calls are required")
        if not _allocation_matches(
            plan,
            max_calls=max_calls,
            split_calls=split_calls,
            dividend_calls=dividend_calls,
            open_close_calls=open_close_calls,
        ):
            raise AcquisitionRefused("typed allocation must exactly equal the acquisition plan")

        ledger.begin_authorization(
            authorization_id=authorization_id,
            plan=plan,
            max_calls=max_calls,
        )
        repaired = await _repair_plan_receipts(store, plan)
        guard = PlanBoundGlobalGuard(
            total_budget=max_calls,
            end_session=end_session,
            clock=clock,
        )
        checkpointed = {"split_page": 0, "dividend_page": 0, "open_close": 0}
        try:
            provider = provider_factory(safe_settings, guard)
            if provider.name != BACKFILL_SOURCE:
                raise AcquisitionRefused("provider source is outside the acquisition contract")
            async with provider:
                for call in plan.calls:
                    existing = await _existing_call_evidence(store, call)
                    if existing is not None:
                        if call.kind in {"split_page", "dividend_page"}:
                            raise AcquisitionRefused(
                                "planned action state changed before its call; obtain a fresh plan"
                            )
                        continue
                    ledger.reserve_call(
                        authorization_id=authorization_id,
                        plan_id=plan.plan_id,
                        call=call,
                    )
                    guard.arm(call)
                    caught: Exception | None = None
                    evidence_id: str | None = None
                    try:
                        evidence_id = await _perform_call(store, provider, call)
                    except Exception as exc:  # noqa: BLE001 - type only reaches output/ledger.
                        caught = exc
                    admitted = guard.finish(call)
                    if caught is None and not admitted:
                        caught = AcquisitionRefused(
                            "provider returned without one guarded HTTP attempt"
                        )
                    if caught is not None:
                        ledger.finish_call(
                            authorization_id=authorization_id,
                            plan_id=plan.plan_id,
                            call=call,
                            status="failed",
                            failure_type=type(caught).__name__,
                        )
                        raise caught
                    if evidence_id is None:
                        raise AcquisitionRefused("checkpoint evidence identity is absent")
                    ledger.finish_call(
                        authorization_id=authorization_id,
                        plan_id=plan.plan_id,
                        call=call,
                        status="checkpointed",
                        evidence_id=evidence_id,
                    )
                    checkpointed[call.kind] += 1
        except Exception as exc:
            final = await _safe_remaining_plan(
                store=store,
                plan=plan,
                expected_dates=expected_dates,
                ledger=ledger,
                legacy_ledger_path=legacy_ledger_path,
            )
            raise AcquisitionExecutionFailed(
                _failure_result(
                    plan=plan,
                    authorization_id=authorization_id,
                    max_calls=max_calls,
                    ledger=ledger,
                    guard=guard,
                    checkpointed=checkpointed,
                    remaining=(
                        final.required_outbound_attempts
                        if final is not None
                        else plan.required_outbound_attempts
                    ),
                    failure_type=type(exc).__name__,
                )
            ) from None

        _validate_current_end(end_session, clock())
        final_plan = await _current_plan(
            store=store,
            end_session=end_session,
            expected_dates=expected_dates,
            tool_revision=tool_revision,
            ledger=ledger,
            legacy_ledger_path=legacy_ledger_path,
        )
        if (
            final_plan.calls
            or final_plan.receipt_repairs_required
            or final_plan.ambiguous_call_ids
            or final_plan.split_state.ambiguous
            or final_plan.dividend_state.ambiguous
        ):
            raise AcquisitionExecutionFailed(
                _failure_result(
                    plan=plan,
                    authorization_id=authorization_id,
                    max_calls=max_calls,
                    ledger=ledger,
                    guard=guard,
                    checkpointed=checkpointed,
                    remaining=final_plan.required_outbound_attempts,
                    failure_type="PostflightIncomplete",
                )
            )
        attempts_reserved = ledger.attempt_count(authorization_id)
        if guard.spent != attempts_reserved:
            raise AcquisitionExecutionFailed(
                _failure_result(
                    plan=plan,
                    authorization_id=authorization_id,
                    max_calls=max_calls,
                    ledger=ledger,
                    guard=guard,
                    checkpointed=checkpointed,
                    remaining=0,
                    failure_type="AttemptAccountingMismatch",
                )
            )
        return {
            "status": "ok",
            "plan_id": plan.plan_id,
            "tool_revision": plan.tool_revision,
            "authorization_id": authorization_id,
            "symbol": BACKFILL_SYMBOL,
            "window_start": plan.price_plan.expected_dates[0].isoformat(),
            "window_end": plan.end_session.isoformat(),
            "calls_sha256": plan.calls_sha256,
            "authorized_max_calls": max_calls,
            "authorized_allocation": plan.allocation,
            "attempts_reserved": attempts_reserved,
            "attempts_spent": guard.spent,
            "checkpointed": checkpointed,
            "receipt_repairs": repaired,
            "remaining_calls": 0,
        }


async def _safe_remaining_plan(
    *,
    store: AcquisitionStore,
    plan: AcquisitionPlan,
    expected_dates: tuple[date, ...],
    ledger: AcquisitionLedger,
    legacy_ledger_path: Path,
) -> AcquisitionPlan | None:
    try:
        return await _current_plan(
            store=store,
            end_session=plan.end_session,
            expected_dates=expected_dates,
            tool_revision=plan.tool_revision,
            ledger=ledger,
            legacy_ledger_path=legacy_ledger_path,
        )
    except Exception:
        return None


def _failure_result(
    *,
    plan: AcquisitionPlan,
    authorization_id: str,
    max_calls: int,
    ledger: AcquisitionLedger,
    guard: PlanBoundGlobalGuard,
    checkpointed: dict[str, int],
    remaining: int,
    failure_type: str,
) -> dict[str, object]:
    return {
        "status": "failed",
        "plan_id": plan.plan_id,
        "tool_revision": plan.tool_revision,
        "authorization_id": authorization_id,
        "symbol": BACKFILL_SYMBOL,
        "authorized_max_calls": max_calls,
        "authorized_allocation": plan.allocation,
        "attempts_reserved": ledger.attempt_count(authorization_id),
        "attempts_spent": guard.spent,
        "checkpointed": checkpointed,
        "remaining_calls": remaining,
        "failure_type": failure_type,
    }


def _iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError("date must be YYYY-MM-DD") from None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan = subparsers.add_parser("plan", help="read-only exact typed-call plan")
    plan.add_argument("--end", required=True, type=_iso_date)
    repair = subparsers.add_parser("repair", help="repair receipts without vendor calls")
    repair.add_argument("--end", required=True, type=_iso_date)
    repair.add_argument("--plan-id", required=True)
    execute = subparsers.add_parser("execute", help="run one separately authorized plan")
    execute.add_argument("--end", required=True, type=_iso_date)
    execute.add_argument("--plan-id", required=True)
    execute.add_argument("--max-calls", required=True, type=int)
    execute.add_argument("--split-calls", required=True, type=int)
    execute.add_argument("--dividend-calls", required=True, type=int)
    execute.add_argument("--open-close-calls", required=True, type=int)
    execute.add_argument("--authorization", required=True)
    execute.add_argument("--authorization-id", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    configure_logging("INFO", json_logs=False, exception_details=False)
    try:
        if args.command == "plan":
            result: dict[str, object] = asyncio.run(
                plan_acquisition(end_session=args.end)
            ).public_result()
        elif args.command == "repair":
            result = asyncio.run(repair_acquisition(end_session=args.end, plan_id=args.plan_id))
        else:
            result = asyncio.run(
                execute_acquisition(
                    end_session=args.end,
                    plan_id=args.plan_id,
                    max_calls=args.max_calls,
                    split_calls=args.split_calls,
                    dividend_calls=args.dividend_calls,
                    open_close_calls=args.open_close_calls,
                    authorization=args.authorization,
                    authorization_id=args.authorization_id,
                )
            )
    except AcquisitionExecutionFailed as exc:
        print(_canonical_bytes(exc.result).decode("ascii"), file=sys.stderr)
        return 1
    except BackfillRefused as exc:
        print(f"vendor acquisition refused: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001 - never render possibly sensitive exception text.
        print(f"vendor acquisition failed: {type(exc).__name__}", file=sys.stderr)
        return 1
    print(_canonical_bytes(result).decode("ascii"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
