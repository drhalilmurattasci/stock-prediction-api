"""Resolution and connection-lifetime proofs for the factor builder."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest

from app.services.adjustment_factor_builder import (
    AdjustmentFactorBuilder,
    AdjustmentFactorBuildError,
    AdjustmentFactorBuildSpec,
    _raw_inputs,
    build_factor_raw_inputs_statement,
)
from app.services.adjustment_factor_store import PublishedAdjustmentFactorSet


def _hash(character: str) -> str:
    return "sha256:" + character * 64


def _spec() -> AdjustmentFactorBuildSpec:
    return AdjustmentFactorBuildSpec(
        symbol="MSFT",
        coverage_start=date(2026, 7, 10),
        coverage_end=date(2026, 7, 13),
        cutoff=datetime(2026, 7, 14, 12, tzinfo=UTC),
    )


def _raw_row(day: date, hour: int = 20) -> SimpleNamespace:
    observed = datetime(day.year, day.month, day.day, hour, tzinfo=UTC)
    return SimpleNamespace(
        symbol="MSFT",
        timespan="day",
        multiplier=1,
        observed_at=observed,
        source="polygon_open_close",
        adjustment_basis="raw",
        version_recorded_at=observed.replace(hour=21),
        available_at=observed.replace(hour=22),
        close=100.5,
        greatest_count=1,
        active_version_count=1,
    )


def test_raw_statement_selects_newest_exact_receipts_at_cutoff() -> None:
    sql = str(build_factor_raw_inputs_statement(_spec()))

    assert "bar_version_availability" in sql
    assert "version_recorded_at DESC" in sql
    assert "available_at DESC" in sql
    assert "version_rank =" in sql
    assert "available_at <=" in sql
    assert "ORDER BY factor_visible_bar_versions.observed_at" in sql
    assert "LIMIT" not in sql


def test_raw_projection_rejects_ambiguous_receipt() -> None:
    row = _raw_row(date(2026, 7, 10))
    row.greatest_count = 2

    with pytest.raises(AdjustmentFactorBuildError, match="ambiguous"):
        _raw_inputs((row,), _spec())


@pytest.mark.parametrize(
    "spec",
    [
        AdjustmentFactorBuildSpec(
            symbol="msft",
            coverage_start=date(2026, 7, 10),
            coverage_end=date(2026, 7, 13),
            cutoff=datetime(2026, 7, 14, 12, tzinfo=UTC),
        ),
        AdjustmentFactorBuildSpec(
            symbol="MSFT",
            coverage_start=date(2026, 7, 13),
            coverage_end=date(2026, 7, 10),
            cutoff=datetime(2026, 7, 14, 12, tzinfo=UTC),
        ),
    ],
)
def test_spec_is_explicit_and_canonical(spec: AdjustmentFactorBuildSpec) -> None:
    with pytest.raises(AdjustmentFactorBuildError):
        build_factor_raw_inputs_statement(spec)


class _Result:
    def __init__(self, value: object) -> None:
        self.value = value

    def scalar_one(self) -> object:
        return self.value

    def one_or_none(self) -> object:
        return self.value

    def all(self) -> list[object]:
        return cast(list[object], self.value)

    def mappings(self) -> list[object]:
        return cast(list[object], self.value)


class _Session:
    def __init__(self, outcomes: list[object]) -> None:
        self.outcomes: Iterator[object] = iter(outcomes)
        self.closed = False

    async def execute(self, statement: object) -> _Result:
        del statement
        return _Result(next(self.outcomes))


class _SessionContext:
    def __init__(self, session: _Session) -> None:
        self.session = session

    async def __aenter__(self) -> _Session:
        return self.session

    async def __aexit__(self, *exc_info: object) -> None:
        self.session.closed = True


class _Maker:
    def __init__(self, session: _Session) -> None:
        self.session = session

    def __call__(self) -> _SessionContext:
        return _SessionContext(self.session)


class _Publisher:
    def __init__(self, session: _Session) -> None:
        self.session = session
        self.published = False

    async def publish(self, artifact):
        assert self.session.closed is True
        self.published = True
        return PublishedAdjustmentFactorSet(
            factor_set_id=artifact.factor_set_id,
            factor_set_recorded_at=datetime(2026, 7, 14, 13, tzinfo=UTC),
            available_at=datetime(2026, 7, 14, 13, 1, tzinfo=UTC),
            input_count=len(artifact.raw_inputs),
            max_input_available_at=max(row.available_at for row in artifact.raw_inputs),
        )


@pytest.mark.asyncio
async def test_builder_releases_resolver_session_before_calculation_publish() -> None:
    split_header = SimpleNamespace(
        collection_id=_hash("1"),
        recorded_at=datetime(2026, 7, 14, 10, tzinfo=UTC),
        available_at=datetime(2026, 7, 14, 10, 1, tzinfo=UTC),
        event_count=0,
    )
    dividend_header = SimpleNamespace(
        collection_id=_hash("2"),
        recorded_at=datetime(2026, 7, 14, 10, 2, tzinfo=UTC),
        available_at=datetime(2026, 7, 14, 10, 3, tzinfo=UTC),
        event_count=0,
    )
    session = _Session(
        [
            datetime(2026, 7, 14, 12, 1, tzinfo=UTC),
            split_header,
            [],
            dividend_header,
            [],
            [_raw_row(date(2026, 7, 10)), _raw_row(date(2026, 7, 13))],
        ]
    )
    publisher = _Publisher(session)
    builder = AdjustmentFactorBuilder(
        sessionmaker=cast(Any, _Maker(session)),
        publisher=publisher,
    )

    result = await builder.build(_spec())

    assert result.publication.factor_set_id == result.artifact.factor_set_id
    assert result.artifact.anchor_date == date(2026, 7, 13)
    assert publisher.published is True
