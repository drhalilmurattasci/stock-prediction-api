"""Unit proofs for vendor-acquisition high-water reconciliation."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import DBAPIError

from app.services.vendor_acquisition_campaign_store import (
    SqlVendorAcquisitionCampaignStore,
    VendorAcquisitionCampaignHighWater,
    VendorAcquisitionCampaignStoreConflict,
    VendorAcquisitionCampaignStoreOutcomeUnknown,
)


def _hash(character: str) -> str:
    return "sha256:" + character * 64


def _checkpoint(number: int = 1) -> VendorAcquisitionCampaignHighWater:
    return VendorAcquisitionCampaignHighWater(
        checkpoint_number=number,
        ledger_sha256=_hash("a"),
        campaign_id=_hash("b"),
        campaign_checkpoint_number=number,
        campaign_ledger_sha256=_hash("c"),
        base_calls=4,
        authorized_calls=4,
        reserved_calls=0,
    )


class _Result:
    def __init__(self, value: object) -> None:
        self.value = value

    def scalar_one_or_none(self) -> object:
        return self.value

    def one(self) -> object:
        return self.value


class _Session:
    def __init__(self, outcome: object) -> None:
        self.outcome = outcome

    async def execute(self, statement: object, params: object | None = None) -> _Result:
        del statement, params
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return _Result(self.outcome)


class _Context:
    def __init__(self, session: _Session) -> None:
        self.session = session

    async def __aenter__(self) -> _Session:
        return self.session

    async def __aexit__(self, *exc_info: object) -> None:
        return None


class _Maker:
    def __init__(self, outcomes: list[object]) -> None:
        self.outcomes: Iterator[object] = iter(outcomes)

    def begin(self) -> _Context:
        return _Context(_Session(next(self.outcomes)))

    def __call__(self) -> _Context:
        return _Context(_Session(next(self.outcomes)))


class _DriverRejection(RuntimeError):
    def __init__(self, sqlstate: str | None = None) -> None:
        super().__init__(sqlstate or "connection lost")
        self.sqlstate = sqlstate


def _row(value: VendorAcquisitionCampaignHighWater) -> tuple[object, ...]:
    return (
        value.checkpoint_number,
        value.ledger_sha256,
        value.campaign_id,
        value.campaign_checkpoint_number,
        value.campaign_ledger_sha256,
        value.base_calls,
        value.authorized_calls,
        value.reserved_calls,
        None,
    )


def test_global_checkpoint_has_no_arbitrary_ten_thousand_record_expiry() -> None:
    assert _checkpoint(10_001).checkpoint_number == 10_001


async def test_unknown_commit_replays_exact_visible_checkpoint() -> None:
    requested = _checkpoint()
    failure = DBAPIError("publish", {}, _DriverRejection())
    store = object.__new__(SqlVendorAcquisitionCampaignStore)
    cast(Any, store)._maker = _Maker([failure])
    store.latest = AsyncMock(return_value=requested)  # type: ignore[method-assign]

    assert await store.publish(requested) == requested


async def test_deterministic_rejection_with_different_visibility_is_conflict() -> None:
    requested = _checkpoint()
    failure = DBAPIError("publish", {}, _DriverRejection("55000"))
    store = object.__new__(SqlVendorAcquisitionCampaignStore)
    cast(Any, store)._maker = _Maker([failure])
    store.latest = AsyncMock(return_value=None)  # type: ignore[method-assign]

    with pytest.raises(VendorAcquisitionCampaignStoreConflict, match="conflicts"):
        await store.publish(requested)


async def test_post_publish_visibility_failure_is_outcome_unknown() -> None:
    requested = _checkpoint()
    store = object.__new__(SqlVendorAcquisitionCampaignStore)
    cast(Any, store)._maker = _Maker([_row(requested)])
    store.latest = AsyncMock(  # type: ignore[method-assign]
        side_effect=VendorAcquisitionCampaignStoreOutcomeUnknown("read failed")
    )

    with pytest.raises(VendorAcquisitionCampaignStoreOutcomeUnknown, match="visibility"):
        await store.publish(requested)


async def test_latest_database_failure_is_outcome_unknown() -> None:
    failure = DBAPIError("read", {}, _DriverRejection())
    store = object.__new__(SqlVendorAcquisitionCampaignStore)
    cast(Any, store)._maker = _Maker([failure])

    with pytest.raises(VendorAcquisitionCampaignStoreOutcomeUnknown, match="visibility"):
        await store.latest()
