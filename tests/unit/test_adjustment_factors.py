"""Golden and fail-closed tests for the pure v1 adjustment artifact."""

from __future__ import annotations

import hashlib
import json
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, date, datetime, time, timedelta, timezone
from decimal import Decimal

import pytest

from app.services.adjustment_factors import (
    ADJUSTMENT_FACTOR_POLICY_HASH,
    ADJUSTMENT_FACTOR_POLICY_VERSION,
    ADJUSTMENT_FACTOR_SET_FORMAT,
    AdjustmentFactorError,
    DividendActionVersion,
    RawCloseVersion,
    SplitActionVersion,
    adjustment_factor_policy_document,
    build_adjustment_factor_set,
)

DATES = (
    date(2026, 7, 6),
    date(2026, 7, 7),
    date(2026, 7, 8),
    date(2026, 7, 9),
    date(2026, 7, 10),
)
CUTOFF = datetime(2026, 7, 10, 21, tzinfo=UTC)
SPLIT_COLLECTION_ID = "sha256:" + "1" * 64
DIVIDEND_COLLECTION_ID = "sha256:" + "2" * 64
SPLIT_VERSION_ID = "sha256:" + "3" * 64
DIVIDEND_VERSION_ID = "sha256:" + "4" * 64


def _content_id(digit: str) -> str:
    return "sha256:" + digit * 64


def _raw(
    *,
    dates: tuple[date, ...] = DATES,
    closes: tuple[str, ...] = ("100", "102", "100", "104", "52"),
) -> tuple[RawCloseVersion, ...]:
    rows: list[RawCloseVersion] = []
    for observation_date, close in zip(dates, closes, strict=True):
        observed_at = datetime.combine(observation_date, time(20), tzinfo=UTC)
        rows.append(
            RawCloseVersion(
                observation_date=observation_date,
                observed_at=observed_at,
                timespan="day",
                multiplier=1,
                source="polygon_open_close",
                adjustment_basis="raw",
                version_recorded_at=observed_at + timedelta(minutes=5),
                available_at=observed_at + timedelta(minutes=6),
                close=Decimal(close),
            )
        )
    return tuple(rows)


def _changed_raw(index: int = 0, **changes: object) -> tuple[RawCloseVersion, ...]:
    rows = list(_raw())
    rows[index] = replace(rows[index], **changes)  # type: ignore[arg-type]
    return tuple(rows)


def _changed_raw_field(field: str, value: object) -> tuple[RawCloseVersion, ...]:
    rows = list(_raw())
    rows[0] = replace(rows[0], **{field: value})  # type: ignore[arg-type]
    return tuple(rows)


def _split(
    *,
    event_id: str = "split-event",
    version_id: str = SPLIT_VERSION_ID,
    effective_date: date = DATES[-1],
    split_from: str = "1",
    split_to: str = "2",
    adjustment_type: str = "forward_split",
) -> SplitActionVersion:
    return SplitActionVersion(
        provider_event_id=event_id,
        version_id=version_id,
        effective_date=effective_date,
        split_from=Decimal(split_from),
        split_to=Decimal(split_to),
        adjustment_type=adjustment_type,  # type: ignore[arg-type]
    )


def _dividend(
    *,
    event_id: str = "dividend-event",
    version_id: str = DIVIDEND_VERSION_ID,
    ex_date: date = DATES[2],
    cash: str = "2",
    currency: str | None = "USD",
    distribution_type: str = "recurring",
) -> DividendActionVersion:
    return DividendActionVersion(
        provider_event_id=event_id,
        version_id=version_id,
        ex_dividend_date=ex_date,
        cash_amount=Decimal(cash),
        currency=currency,
        distribution_type=distribution_type,  # type: ignore[arg-type]
    )


def _build(**overrides: object):
    values: dict[str, object] = {
        "symbol": "MSFT",
        "cutoff": CUTOFF,
        "raw_closes": _raw(),
        "split_collection_id": SPLIT_COLLECTION_ID,
        "splits": (_split(),),
        "dividend_collection_id": DIVIDEND_COLLECTION_ID,
        "dividends": (_dividend(),),
    }
    values.update(overrides)
    return build_adjustment_factor_set(**values)  # type: ignore[arg-type]


def test_raw_input_count_is_bounded_before_calendar_work() -> None:
    with pytest.raises(AdjustmentFactorError, match="5000-row factor-set limit"):
        _build(raw_closes=_raw() * 1_001, splits=(), dividends=())


def test_golden_split_and_gross_total_return_factors() -> None:
    artifact = _build()

    assert ADJUSTMENT_FACTOR_POLICY_HASH == (
        "sha256:f825ca4aa36725fb98a2697dd339b07275397711b0caaf488e9c87d70afd2b37"
    )
    assert artifact.factor_set_id == (
        "sha256:fd9349584930f5ff6918f92e9f7073004a3400a4b5105f11bc7ad968a0182f28"
    )
    assert artifact.policy_version == ADJUSTMENT_FACTOR_POLICY_VERSION
    assert artifact.policy_hash == ADJUSTMENT_FACTOR_POLICY_HASH
    assert artifact.anchor_date == DATES[-1]
    assert artifact.raw_inputs == _raw()
    assert artifact.action_version_ids == (SPLIT_VERSION_ID, DIVIDEND_VERSION_ID)
    assert [row.raw_input_ordinal for row in artifact.factors] == list(range(5))
    assert [row.price_factor_decimal for row in artifact.factors] == [
        "0.4901960784313725490196078431372549",
        "0.4901960784313725490196078431372549",
        "0.5",
        "0.5",
        "1",
    ]
    assert [row.volume_factor_decimal for row in artifact.factors] == [
        "2",
        "2",
        "2",
        "2",
        "1",
    ]
    assert [row.price_factor_f64_be for row in artifact.factors] == [
        "3fdf5f5f5f5f5f5f",
        "3fdf5f5f5f5f5f5f",
        "3fe0000000000000",
        "3fe0000000000000",
        "3ff0000000000000",
    ]
    assert [row.volume_factor_f64_be for row in artifact.factors] == [
        "4000000000000000",
        "4000000000000000",
        "4000000000000000",
        "4000000000000000",
        "3ff0000000000000",
    ]

    # Dividend session adjusted return equals exact gross total return:
    # (100 + 2) / 102 - 1 = 0.  The later split factor is common to both.
    adjusted_prior = Decimal("102") * Decimal(artifact.factors[1].price_factor_decimal)
    adjusted_post = Decimal("100") * Decimal(artifact.factors[2].price_factor_decimal)
    assert adjusted_post / adjusted_prior - 1 == 0
    # Split session similarly removes the mechanical 2:1 price discontinuity.
    assert Decimal("104") * Decimal(artifact.factors[3].price_factor_decimal) == Decimal(
        "52"
    ) * Decimal(artifact.factors[4].price_factor_decimal)


def test_payload_is_canonical_content_addressed_and_binds_every_input_identity() -> None:
    artifact = _build()
    document = json.loads(artifact.canonical_payload)

    assert (
        artifact.factor_set_id == "sha256:" + hashlib.sha256(artifact.canonical_payload).hexdigest()
    )
    assert artifact.canonical_payload == json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    assert document["format"] == ADJUSTMENT_FACTOR_SET_FORMAT
    assert document["policy_version"] == ADJUSTMENT_FACTOR_POLICY_VERSION
    assert document["policy_hash"] == ADJUSTMENT_FACTOR_POLICY_HASH
    assert document["symbol"] == "MSFT"
    assert document["cutoff"] == "2026-07-10T21:00:00.000000Z"
    assert document["anchor_date"] == "2026-07-10"
    assert document["actions"]["splits"]["collection_id"] == SPLIT_COLLECTION_ID
    assert document["actions"]["dividends"]["collection_id"] == DIVIDEND_COLLECTION_ID
    assert [row["observation_date"] for row in document["raw_inputs"]] == [
        value.isoformat() for value in DATES
    ]
    assert [row["observed_at"] for row in document["raw_inputs"]] == [
        f"{value.isoformat()}T20:00:00.000000Z" for value in DATES
    ]
    assert all(row["timespan"] == "day" for row in document["raw_inputs"])
    assert all(row["multiplier"] == 1 for row in document["raw_inputs"])
    assert all(row["source"] == "polygon_open_close" for row in document["raw_inputs"])
    assert all(row["adjustment_basis"] == "raw" for row in document["raw_inputs"])
    assert [row["close_decimal"] for row in document["raw_inputs"]] == [
        "100",
        "102",
        "100",
        "104",
        "52",
    ]
    assert [row["close_f64_be"] for row in document["raw_inputs"]] == [
        "4059000000000000",
        "4059800000000000",
        "4059000000000000",
        "405a000000000000",
        "404a000000000000",
    ]
    assert [row["raw_input_ordinal"] for row in document["factors"]] == list(range(5))
    assert set(document["factors"][0]) == {
        "price_factor_decimal",
        "price_factor_f64_be",
        "raw_input_ordinal",
        "volume_factor_decimal",
        "volume_factor_f64_be",
    }
    assert document["actions"]["splits"]["versions"][0]["version_id"] == SPLIT_VERSION_ID
    assert document["actions"]["dividends"]["versions"][0]["version_id"] == (DIVIDEND_VERSION_ID)


def test_policy_hash_is_reproducible_and_document_is_detached() -> None:
    first = adjustment_factor_policy_document()
    expected = (
        "sha256:"
        + hashlib.sha256(
            json.dumps(
                first,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
    )
    assert expected == ADJUSTMENT_FACTOR_POLICY_HASH
    assert first["dividend"]["allowed_distribution_types"] == ["recurring"]  # type: ignore[index]
    assert first["split"]["allowed_adjustment_types"] == [  # type: ignore[index]
        "forward_split",
        "reverse_split",
    ]

    first["version"] = "tampered"
    assert adjustment_factor_policy_document()["version"] == (ADJUSTMENT_FACTOR_POLICY_VERSION)


def test_input_order_does_not_change_payload_or_factor_set_id() -> None:
    splits = (
        _split(
            event_id="later-split",
            version_id=_content_id("5"),
            effective_date=DATES[-1],
        ),
        _split(
            event_id="earlier-split",
            version_id=_content_id("6"),
            effective_date=DATES[1],
            split_from="2",
            split_to="1",
            adjustment_type="reverse_split",
        ),
    )
    first = _build(raw_closes=_raw(), splits=splits, dividends=())
    second = _build(
        raw_closes=tuple(reversed(_raw())),
        splits=tuple(reversed(splits)),
        dividends=(),
    )

    assert first == second
    assert first.canonical_payload == second.canonical_payload


@pytest.mark.parametrize(
    ("override", "value"),
    [
        ("split_collection_id", _content_id("7")),
        ("dividend_collection_id", _content_id("8")),
        ("cutoff", CUTOFF.replace(microsecond=1)),
        (
            "raw_closes",
            _changed_raw(
                available_at=_raw()[0].available_at + timedelta(microseconds=1),
            ),
        ),
        ("splits", (_split(version_id=_content_id("9")),)),
        ("dividends", (_dividend(version_id=_content_id("a")),)),
    ],
)
def test_every_version_collection_and_cutoff_identity_changes_artifact(
    override: str,
    value: object,
) -> None:
    baseline = _build()
    changed = _build(**{override: value})

    assert changed.factor_set_id != baseline.factor_set_id
    assert changed.canonical_payload != baseline.canonical_payload


def test_action_on_earliest_session_has_no_effect_under_strict_date_rule() -> None:
    artifact = _build(splits=(), dividends=(_dividend(ex_date=DATES[0]),))

    assert all(row.price_factor_decimal == "1" for row in artifact.factors)
    assert all(row.volume_factor_decimal == "1" for row in artifact.factors)


def test_empty_complete_action_collections_publish_unit_factors() -> None:
    artifact = _build(splits=(), dividends=())

    assert all(row.price_factor_decimal == "1" for row in artifact.factors)
    assert all(row.volume_factor_decimal == "1" for row in artifact.factors)
    document = json.loads(artifact.canonical_payload)
    assert document["actions"]["splits"]["versions"] == []
    assert document["actions"]["dividends"]["versions"] == []


@pytest.mark.parametrize(
    ("split_from", "expected"),
    [
        (
            "1.2345678901234567890123456789012345",
            "1.234567890123456789012345678901234",
        ),
        (
            "1.2345678901234567890123456789012355",
            "1.234567890123456789012345678901236",
        ),
    ],
)
def test_decimal34_uses_half_even_rounding(split_from: str, expected: str) -> None:
    artifact = _build(
        splits=(_split(split_from=split_from, split_to="1"),),
        dividends=(),
    )

    assert artifact.factors[-2].price_factor_decimal == expected


def test_artifact_and_rows_are_frozen() -> None:
    artifact = _build()

    with pytest.raises(FrozenInstanceError):
        artifact.symbol = "AAPL"
    with pytest.raises(FrozenInstanceError):
        artifact.factors[0].price_factor_decimal = "1"


@pytest.mark.parametrize(
    "raw_closes",
    [
        (),
        (_raw()[0], _raw()[0]),
        _raw(dates=(DATES[0], DATES[1], DATES[3], DATES[4]), closes=("1", "1", "1", "1")),
        _raw(
            dates=(date(2026, 7, 10), date(2026, 7, 11)),
            closes=("1", "1"),
        ),
    ],
)
def test_raw_series_must_be_nonempty_unique_and_contiguous_xnys(
    raw_closes: tuple[RawCloseVersion, ...],
) -> None:
    with pytest.raises(AdjustmentFactorError):
        _build(raw_closes=raw_closes, splits=(), dividends=())


@pytest.mark.parametrize("close", ["0", "-1", "NaN", "Infinity", "-Infinity"])
def test_raw_closes_must_be_positive_finite_decimals(close: str) -> None:
    raw = list(_raw())
    raw[0] = replace(raw[0], close=Decimal(close))

    with pytest.raises(AdjustmentFactorError):
        _build(raw_closes=tuple(raw))


def test_raw_close_binary_float_is_rejected_before_canonicalization() -> None:
    raw = list(_raw())
    raw[0] = replace(raw[0], close=100.0)  # type: ignore[arg-type]

    with pytest.raises(AdjustmentFactorError, match="Decimal"):
        _build(raw_closes=tuple(raw))


def test_cutoff_must_be_aware_and_not_before_anchor_close() -> None:
    with pytest.raises(AdjustmentFactorError, match="timezone-aware"):
        _build(cutoff=datetime(2026, 7, 10, 21))
    with pytest.raises(AdjustmentFactorError, match="cutoff"):
        _build(cutoff=datetime(2026, 7, 10, 19, 59, tzinfo=UTC))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("timespan", "hour"),
        ("multiplier", 2),
        ("multiplier", True),
        ("source", "polygon"),
        ("adjustment_basis", "split_adjusted"),
    ],
)
def test_raw_receipts_require_the_exact_v1_source_contract(field: str, value: object) -> None:
    with pytest.raises(AdjustmentFactorError):
        _build(
            raw_closes=_changed_raw_field(field, value),
            splits=(),
            dividends=(),
        )


@pytest.mark.parametrize("field", ["observed_at", "version_recorded_at", "available_at"])
def test_raw_receipt_timestamps_must_be_timezone_aware(field: str) -> None:
    aware = getattr(_raw()[0], field)
    with pytest.raises(AdjustmentFactorError, match="timezone-aware"):
        _build(
            raw_closes=_changed_raw(**{field: aware.replace(tzinfo=None)}),
            splits=(),
            dividends=(),
        )


def test_raw_receipt_timestamps_normalize_to_utc_without_changing_identity() -> None:
    offset = timezone(timedelta(hours=3))
    rows = list(_raw())
    rows[0] = replace(
        rows[0],
        observed_at=rows[0].observed_at.astimezone(offset),
        version_recorded_at=rows[0].version_recorded_at.astimezone(offset),
        available_at=rows[0].available_at.astimezone(offset),
    )

    assert _build(raw_closes=tuple(rows)) == _build()


@pytest.mark.parametrize(
    "raw_closes",
    [
        _changed_raw(
            observed_at=datetime(2026, 7, 5, 20, tzinfo=UTC),
        ),
        _changed_raw(
            observed_at=datetime(2026, 7, 6, 19, 59, tzinfo=UTC),
        ),
        _changed_raw(
            version_recorded_at=_raw()[0].observed_at - timedelta(microseconds=1),
        ),
        _changed_raw(
            available_at=_raw()[0].version_recorded_at - timedelta(microseconds=1),
        ),
        _changed_raw(
            index=-1,
            available_at=CUTOFF + timedelta(microseconds=1),
        ),
    ],
)
def test_raw_receipt_dates_session_close_and_availability_order_are_exact(
    raw_closes: tuple[RawCloseVersion, ...],
) -> None:
    with pytest.raises(AdjustmentFactorError):
        _build(raw_closes=raw_closes, splits=(), dividends=())


def test_raw_close_decimal_is_canonicalized_from_and_binds_exact_binary64() -> None:
    artifact = _build(
        raw_closes=_changed_raw(close=Decimal("0.100000000000000004")),
        splits=(),
        dividends=(),
    )
    document = json.loads(artifact.canonical_payload)

    assert artifact.raw_inputs[0].close == Decimal("0.1")
    assert document["raw_inputs"][0]["close_decimal"] == "0.1"
    assert document["raw_inputs"][0]["close_f64_be"] == "3fb999999999999a"


@pytest.mark.parametrize(
    "split",
    [
        _split(split_from="0"),
        _split(split_from="-1"),
        _split(split_to="0"),
        _split(split_to="NaN"),
        _split(split_to="Infinity"),
        _split(adjustment_type="stock_dividend"),
        _split(adjustment_type="unknown"),
    ],
)
def test_splits_require_positive_finite_ratios_and_allowlisted_types(
    split: SplitActionVersion,
) -> None:
    with pytest.raises(AdjustmentFactorError):
        _build(splits=(split,))


@pytest.mark.parametrize(
    "dividend",
    [
        _dividend(cash="0"),
        _dividend(cash="-1"),
        _dividend(cash="NaN"),
        _dividend(cash="Infinity"),
        _dividend(currency="EUR"),
        _dividend(currency=None),
        _dividend(distribution_type="special"),
        _dividend(distribution_type="supplemental"),
        _dividend(distribution_type="irregular"),
        _dividend(distribution_type="unknown"),
    ],
)
def test_dividends_require_positive_usd_recurring_cash(
    dividend: DividendActionVersion,
) -> None:
    with pytest.raises(AdjustmentFactorError):
        _build(dividends=(dividend,))


@pytest.mark.parametrize(
    ("splits", "dividends"),
    [
        (
            (_split(event_id="same", version_id=_content_id("5"), effective_date=DATES[1]),),
            (_dividend(event_id="same", version_id=_content_id("6"), ex_date=DATES[2]),),
        ),
        (
            (_split(event_id="split-a", version_id=_content_id("5"), effective_date=DATES[1]),),
            (_dividend(event_id="div-a", version_id=_content_id("5"), ex_date=DATES[2]),),
        ),
        (
            (
                _split(
                    event_id="split-a",
                    version_id=_content_id("5"),
                    effective_date=DATES[2],
                ),
            ),
            (_dividend(event_id="div-a", version_id=_content_id("6"), ex_date=DATES[2]),),
        ),
        (
            (
                _split(
                    event_id="split-a",
                    version_id=_content_id("5"),
                    effective_date=DATES[2],
                ),
                _split(
                    event_id="split-b",
                    version_id=_content_id("6"),
                    effective_date=DATES[2],
                ),
            ),
            (),
        ),
        (
            (),
            (
                _dividend(event_id="div-a", version_id=_content_id("5"), ex_date=DATES[2]),
                _dividend(event_id="div-b", version_id=_content_id("6"), ex_date=DATES[2]),
            ),
        ),
    ],
)
def test_action_ids_versions_and_session_cardinality_are_unambiguous(
    splits: tuple[SplitActionVersion, ...],
    dividends: tuple[DividendActionVersion, ...],
) -> None:
    with pytest.raises(AdjustmentFactorError):
        _build(splits=splits, dividends=dividends)


@pytest.mark.parametrize(
    ("splits", "dividends"),
    [
        ((_split(effective_date=date(2026, 7, 3)),), ()),
        ((_split(effective_date=date(2026, 7, 13)),), ()),
        ((), (_dividend(ex_date=date(2026, 7, 3)),)),
        ((), (_dividend(ex_date=date(2026, 7, 13)),)),
        # Weekend lies inside a Friday-to-Monday contiguous XNYS raw span but
        # is not itself an exact raw session.
        (
            (),
            (_dividend(ex_date=date(2026, 7, 11)),),
        ),
    ],
)
def test_actions_must_land_on_an_exact_raw_session_inside_the_span(
    splits: tuple[SplitActionVersion, ...],
    dividends: tuple[DividendActionVersion, ...],
) -> None:
    kwargs: dict[str, object] = {"splits": splits, "dividends": dividends}
    if dividends and dividends[0].ex_dividend_date == date(2026, 7, 11):
        kwargs["raw_closes"] = _raw(
            dates=(date(2026, 7, 10), date(2026, 7, 13)),
            closes=("100", "101"),
        )
        kwargs["cutoff"] = datetime(2026, 7, 13, 21, tzinfo=UTC)
    with pytest.raises(AdjustmentFactorError):
        _build(**kwargs)


def test_unknown_action_dates_fail_closed() -> None:
    split = _split()
    dividend = _dividend()
    object.__setattr__(split, "effective_date", None)
    object.__setattr__(dividend, "ex_dividend_date", None)

    with pytest.raises(AdjustmentFactorError, match="must be known"):
        _build(splits=(split,), dividends=())
    with pytest.raises(AdjustmentFactorError, match="must be known"):
        _build(splits=(), dividends=(dividend,))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("symbol", "msft"),
        ("symbol", " MSFT"),
        ("split_collection_id", ""),
        ("split_collection_id", " split-collection"),
        ("split_collection_id", "sha256:" + "A" * 64),
        ("dividend_collection_id", ""),
        ("dividend_collection_id", SPLIT_COLLECTION_ID),
    ],
)
def test_root_and_collection_identities_are_exact(field: str, value: str) -> None:
    with pytest.raises(AdjustmentFactorError):
        _build(**{field: value})


@pytest.mark.parametrize(
    ("splits", "dividends"),
    [
        ((_split(version_id="split-version"),), ()),
        ((), (_dividend(version_id="sha256:" + "G" * 64),)),
    ],
)
def test_action_version_ids_are_sha256_content_addresses(
    splits: tuple[SplitActionVersion, ...],
    dividends: tuple[DividendActionVersion, ...],
) -> None:
    with pytest.raises(AdjustmentFactorError, match="content address"):
        _build(splits=splits, dividends=dividends)


def test_factor_that_cannot_be_published_as_positive_binary64_is_rejected() -> None:
    enormous = _split(
        effective_date=DATES[-1],
        split_from="1E+9999",
        split_to="1",
    )

    with pytest.raises(AdjustmentFactorError, match="binary64"):
        _build(splits=(enormous,), dividends=())
