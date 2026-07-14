"""Canonical complete-collection tests for corporate-action evidence."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.services.corporate_actions import (
    CORPORATE_ACTION_QUERY_POLICY_DOCUMENT,
    CORPORATE_ACTION_QUERY_POLICY_HASH,
    CorporateActionValidationError,
    build_dividend_collection,
    build_split_collection,
)
from data_sources.base import Dividend, DividendPage, Split, SplitPage

FETCHED = datetime(2026, 7, 14, 18, 0, tzinfo=UTC)
START = date(2025, 7, 2)
END = date(2026, 7, 13)


def test_policy_pins_pit_collection_eligibility_and_response_time_ordering() -> None:
    assert CORPORATE_ACTION_QUERY_POLICY_DOCUMENT["version_selection"] == {
        "eligibility": "post_commit_receipt_available_at_lte_consumer_cutoff",
        "newest_order": [
            "collection_recorded_at_desc",
            "collection_id_desc",
        ],
    }


def _split(**changes: object) -> Split:
    values: dict[str, object] = {
        "provider_event_id": "split-1",
        "symbol": "MSFT",
        "execution_date": date(2026, 1, 15),
        "split_from": Decimal("1"),
        "split_to": Decimal("2"),
        "adjustment_type": "forward_split",
        "historical_adjustment_factor": Decimal("0.5"),
        "source": "polygon",
        "fetched_at": FETCHED,
    }
    values.update(changes)
    return Split(**values)  # type: ignore[arg-type]


def _dividend(**changes: object) -> Dividend:
    values: dict[str, object] = {
        "provider_event_id": "dividend-1",
        "symbol": "MSFT",
        "ex_dividend_date": date(2026, 5, 21),
        "cash_amount": Decimal("0.91"),
        "split_adjusted_cash_amount": Decimal("0.91"),
        "historical_adjustment_factor": Decimal("0.9981"),
        "currency": "USD",
        "pay_date": date(2026, 6, 11),
        "record_date": date(2026, 5, 21),
        "declaration_date": date(2026, 3, 10),
        "frequency": 4,
        "distribution_type": "recurring",
        "source": "polygon",
        "fetched_at": FETCHED,
    }
    values.update(changes)
    return Dividend(**values)  # type: ignore[arg-type]


def _split_page(*results: Split, **changes: object) -> SplitPage:
    values: dict[str, object] = {
        "provider_request_id": "request-splits",
        "provider_origin": "https://api.massive.com",
        "endpoint": "/stocks/v1/splits",
        "symbol": "MSFT",
        "start": START,
        "end": END,
        "source": "polygon",
        "fetched_at": FETCHED,
        "results": results,
    }
    values.update(changes)
    return SplitPage(**values)  # type: ignore[arg-type]


def _dividend_page(*results: Dividend, **changes: object) -> DividendPage:
    values: dict[str, object] = {
        "provider_request_id": "request-dividends",
        "provider_origin": "https://api.massive.com",
        "endpoint": "/stocks/v1/dividends",
        "symbol": "MSFT",
        "start": START,
        "end": END,
        "source": "polygon",
        "fetched_at": FETCHED,
        "results": results,
    }
    values.update(changes)
    return DividendPage(**values)  # type: ignore[arg-type]


def test_split_collection_is_sorted_content_addressed_and_scope_bound() -> None:
    later = _split(provider_event_id="split-z", execution_date=date(2026, 6, 1))
    earlier = _split(provider_event_id="split-a", execution_date=date(2025, 9, 1))

    record = build_split_collection(_split_page(later, earlier))

    assert record.query_policy_hash == CORPORATE_ACTION_QUERY_POLICY_HASH
    assert record.event_count == 2
    assert [member.provider_event_id for member in record.members] == ["split-a", "split-z"]
    manifest = json.loads(record.canonical_manifest)
    assert manifest["event_version_ids"] == [member.action_version_id for member in record.members]
    assert manifest["page"] == {
        "count": 1,
        "limit": 5000,
        "pagination_exhausted": True,
    }


def test_empty_page_is_complete_evidence_with_request_and_completion_identity() -> None:
    first = build_split_collection(_split_page())
    replay = build_split_collection(_split_page())
    later_request = build_split_collection(_split_page(provider_request_id="request-splits-2"))

    assert first == replay
    assert first.event_count == 0
    assert first.members == ()
    assert later_request.collection_id != first.collection_id


def test_equivalent_decimal_representations_have_one_event_identity() -> None:
    first = build_dividend_collection(
        _dividend_page(
            _dividend(
                cash_amount=Decimal("0.9100"),
                split_adjusted_cash_amount=Decimal("0.910"),
            )
        )
    )
    second = build_dividend_collection(
        _dividend_page(
            _dividend(
                cash_amount=Decimal("0.91"),
                split_adjusted_cash_amount=Decimal("0.91"),
            )
        )
    )

    assert first.members[0].action_version_id == second.members[0].action_version_id
    payload = json.loads(first.members[0].canonical_event)
    assert payload["cash_amount"] == "0.91"
    assert payload["split_adjusted_cash_amount"] == "0.91"


def test_full_numeric_38_18_value_is_not_rounded_by_ambient_decimal_context() -> None:
    exact = Decimal("12345678901234567890.123456789012345678")
    record = build_dividend_collection(
        _dividend_page(
            _dividend(
                cash_amount=exact,
                split_adjusted_cash_amount=exact,
            )
        )
    )

    payload = json.loads(record.members[0].canonical_event)
    assert payload["cash_amount"] == str(exact)
    assert payload["split_adjusted_cash_amount"] == str(exact)


@pytest.mark.parametrize(
    "page",
    [
        _split_page(provider_origin="https://api.polygon.io"),
        _split_page(endpoint="/v3/reference/splits"),
        _split_page(source="other"),
    ],
)
def test_collection_refuses_unpinned_origin_endpoint_or_source(page: SplitPage) -> None:
    with pytest.raises(CorporateActionValidationError):
        build_split_collection(page)


@pytest.mark.parametrize(
    "dividend",
    [
        _dividend(currency=None),
        _dividend(currency="US"),
        _dividend(cash_amount=Decimal("0.1234567890123456789")),
        _dividend(cash_amount=Decimal("1E+20")),
    ],
)
def test_canonicalization_refuses_values_the_database_cannot_represent_exactly(
    dividend: Dividend,
) -> None:
    with pytest.raises(CorporateActionValidationError):
        build_dividend_collection(_dividend_page(dividend))


@pytest.mark.parametrize("field", ["cash_amount", "split_adjusted_cash_amount"])
def test_dividend_dto_refuses_zero_cash_before_persistence(field: str) -> None:
    with pytest.raises(ValidationError):
        _dividend(**{field: Decimal("0")})


def test_logical_provider_event_cannot_appear_twice_in_one_collection() -> None:
    page = _split_page(
        _split(provider_event_id="same", execution_date=date(2025, 9, 1)),
        _split(provider_event_id="same", execution_date=date(2026, 1, 2)),
    )

    with pytest.raises(CorporateActionValidationError, match="logical provider event"):
        build_split_collection(page)


@pytest.mark.parametrize(
    "page",
    [
        _split_page(provider_request_id="bad, request"),
        _split_page(_split(provider_event_id="bad, event")),
    ],
)
def test_provider_identifiers_must_preserve_canonical_json_bytes(page: SplitPage) -> None:
    with pytest.raises(CorporateActionValidationError, match="provider identifier"):
        build_split_collection(page)
