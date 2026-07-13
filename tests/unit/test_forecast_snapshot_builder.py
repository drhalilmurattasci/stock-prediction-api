"""Point-in-time snapshot-builder policy, calendar, and availability gates."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, time, timedelta

import exchange_calendars
import pytest
from sqlalchemy.dialects import postgresql

from app.services.forecast_snapshot_builder import (
    AVAILABILITY_RULE_SET_DOCUMENT,
    DEFAULT_AVAILABILITY_RULE_SET_HASH,
    DEFAULT_RESOLUTION_POLICY_HASH,
    DEFAULT_SNAPSHOT_BUILD_POLICY,
    RESOLUTION_POLICY_DOCUMENT,
    ForecastSnapshotBuildPolicy,
    PointInTimeBar,
    SnapshotAvailabilityError,
    SnapshotBuildError,
    SnapshotBuildMisconfigured,
    SnapshotBuildSpec,
    SnapshotInputUnavailable,
    assemble_verified_snapshot_payload,
    build_point_in_time_bars_statement,
    scheduled_snapshot_cutoff,
)

POLICY = DEFAULT_SNAPSHOT_BUILD_POLICY


def _spec(cutoff: datetime) -> SnapshotBuildSpec:
    return SnapshotBuildSpec(
        symbol="AAPL",
        target="close",
        horizon_unit="trading_day",
        as_of=cutoff,
    )


def _complete_rows(
    *,
    latest_session: str = "2026-11-25",
    count: int = 258,
    recorded_delay: timedelta = timedelta(hours=1),
) -> tuple[tuple[PointInTimeBar, ...], datetime]:
    calendar = exchange_calendars.get_calendar("XNYS", start="1990-01-01", end="2100-12-31")
    labels = calendar.sessions_window(latest_session, -count)
    rows = []
    for index, label in enumerate(labels):
        observed_at = label.to_pydatetime().replace(tzinfo=UTC)
        session_close = calendar.session_close(label).to_pydatetime()
        recorded_at = session_close + recorded_delay
        available_at = recorded_at + timedelta(minutes=1)
        rows.append(
            PointInTimeBar(
                symbol="AAPL",
                timespan="day",
                multiplier=1,
                source="polygon_open_close",
                adjustment_basis="raw",
                observed_at=observed_at,
                close=100.0 + index / 10,
                fetched_at=recorded_at - timedelta(minutes=2),
                source_as_of=recorded_at - timedelta(minutes=1),
                recorded_at=recorded_at,
                available_at=available_at,
            )
        )
    cutoff = datetime.combine(
        labels[-1].date() + timedelta(days=1),
        time(17, tzinfo=UTC),
    )
    return tuple(rows), cutoff


def test_policy_and_rule_set_hashes_are_golden_and_content_derived() -> None:
    assert DEFAULT_RESOLUTION_POLICY_HASH == (
        "sha256:da8a173eb7086f99bf18ee537ea18666efb3c396eca63bb28894624eef2fcfc5"
    )
    assert DEFAULT_AVAILABILITY_RULE_SET_HASH == (
        "sha256:5015dbf402284cd0269a26ad51f0ddd111741b5e7e25f59676e7c1e020653841"
    )
    assert RESOLUTION_POLICY_DOCUMENT["availability_rule_set_hash"] == (
        DEFAULT_AVAILABILITY_RULE_SET_HASH
    )
    assert "newest_completed_session_present" in AVAILABILITY_RULE_SET_DOCUMENT["rules"]
    assert (
        replace(POLICY, observation_limit=511).resolution_policy_hash
        != DEFAULT_RESOLUTION_POLICY_HASH
    )


def test_policy_requires_exact_operator_pins() -> None:
    POLICY.validate_configured_hashes(
        DEFAULT_RESOLUTION_POLICY_HASH,
        DEFAULT_AVAILABILITY_RULE_SET_HASH,
    )
    with pytest.raises(SnapshotBuildMisconfigured, match="resolution-policy"):
        POLICY.validate_configured_hashes(None, DEFAULT_AVAILABILITY_RULE_SET_HASH)
    with pytest.raises(SnapshotBuildMisconfigured, match="availability"):
        POLICY.validate_configured_hashes(DEFAULT_RESOLUTION_POLICY_HASH, "sha256:" + "0" * 64)


@pytest.mark.parametrize(
    "spec",
    [
        SnapshotBuildSpec("TSLA", "close", "trading_day", datetime(2026, 7, 1, tzinfo=UTC)),
        SnapshotBuildSpec(  # type: ignore[arg-type]
            "AAPL", "adjusted_close", "trading_day", datetime(2026, 7, 1, tzinfo=UTC)
        ),
        SnapshotBuildSpec(  # type: ignore[arg-type]
            "AAPL", "close", "calendar_day", datetime(2026, 7, 1, tzinfo=UTC)
        ),
    ],
)
def test_policy_refuses_unversioned_symbol_target_and_horizon_semantics(
    spec: SnapshotBuildSpec,
) -> None:
    with pytest.raises(SnapshotBuildError):
        POLICY.validate_spec(spec)


def test_point_in_time_sql_selects_newest_exact_finalized_version() -> None:
    _, cutoff = _complete_rows()
    statement = build_point_in_time_bars_statement(_spec(cutoff))
    sql = str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert sql.count(" UNION SELECT ") == 2
    assert "JOIN bar_version_availability ON" in sql
    assert "bar_version_availability.available_at <=" in sql
    assert "bars_revisions.previous_recorded_at IS NOT NULL" in sql
    assert "bars_revisions.incoming_recorded_at IS NOT NULL" in sql
    assert "row_number() OVER (PARTITION BY stored_bar_versions.observed_at " in sql
    assert "ORDER BY stored_bar_versions.recorded_at DESC)" in sql
    assert "finalized_bar_versions.version_rank = 1" in sql
    assert "bars.source = 'polygon_open_close'" in sql
    assert "bars.adjustment_basis = 'raw'" in sql
    assert "LIMIT 512" in sql


def test_verified_payload_has_252_real_session_closes_and_source_manifest() -> None:
    rows, cutoff = _complete_rows(latest_session="2026-11-25")
    checked_at = cutoff + timedelta(minutes=1)
    payload = assemble_verified_snapshot_payload(rows, _spec(cutoff), checked_at=checked_at)

    assert len(payload.observations) == 258
    assert len(payload.target_times) == 252
    # Thanksgiving is closed; the next session is the 13:00 ET early close.
    assert payload.target_times[0] == datetime(2026, 11, 27, 18, tzinfo=UTC)
    assert payload.availability.status == "passed"
    assert payload.availability.rule_set_hash == DEFAULT_AVAILABILITY_RULE_SET_HASH
    assert payload.availability.checked_at == checked_at
    assert payload.data_sources[0].snapshot_id.startswith("sha256:")
    assert len(payload.data_sources[0].snapshot_id) == 71
    assert payload.data_sources[0].max_available_at == rows[-1].available_at


def test_source_manifest_changes_when_one_selected_version_changes() -> None:
    rows, cutoff = _complete_rows()
    first = assemble_verified_snapshot_payload(
        rows, _spec(cutoff), checked_at=cutoff + timedelta(minutes=1)
    )
    changed = (*rows[:-1], replace(rows[-1], close=rows[-1].close + 1.0))
    second = assemble_verified_snapshot_payload(
        changed, _spec(cutoff), checked_at=cutoff + timedelta(minutes=1)
    )
    assert first.data_sources[0].snapshot_id != second.data_sources[0].snapshot_id


def test_missing_or_stale_session_fails_closed() -> None:
    rows, cutoff = _complete_rows()
    with pytest.raises(SnapshotInputUnavailable, match="contiguous"):
        assemble_verified_snapshot_payload(
            rows[1:-1],
            _spec(cutoff),
            checked_at=cutoff + timedelta(minutes=1),
            policy=replace(POLICY, minimum_observations=len(rows) - 2),
        )
    with pytest.raises(SnapshotInputUnavailable, match="contiguous"):
        assemble_verified_snapshot_payload(
            rows[:-1],
            _spec(cutoff),
            checked_at=cutoff + timedelta(minutes=1),
            policy=replace(POLICY, minimum_observations=len(rows) - 1),
        )


def test_old_gap_retains_newest_contiguous_suffix_when_minimum_is_met() -> None:
    rows, cutoff = _complete_rows(count=400)
    with_old_gap = (*rows[:100], *rows[101:])

    payload = assemble_verified_snapshot_payload(
        with_old_gap,
        _spec(cutoff),
        checked_at=cutoff + timedelta(minutes=1),
    )

    assert len(payload.observations) == 299
    assert payload.observations[0].observed_at == rows[101].observed_at
    assert payload.observations[-1].observed_at == rows[-1].observed_at


def test_short_history_unfinished_bar_and_duplicate_version_fail_closed() -> None:
    rows, cutoff = _complete_rows()
    with pytest.raises(SnapshotInputUnavailable, match="at least 258"):
        assemble_verified_snapshot_payload(
            rows[:257], _spec(cutoff), checked_at=cutoff + timedelta(minutes=1)
        )

    unfinished = (*rows[:-1], replace(rows[-1], recorded_at=rows[-1].observed_at))
    with pytest.raises(SnapshotAvailabilityError, match="availability timestamps"):
        assemble_verified_snapshot_payload(
            unfinished, _spec(cutoff), checked_at=cutoff + timedelta(minutes=1)
        )

    duplicate = (*rows[:-1], replace(rows[-1], active_version_count=2))
    with pytest.raises(SnapshotAvailabilityError, match="multiple active versions"):
        assemble_verified_snapshot_payload(
            duplicate, _spec(cutoff), checked_at=cutoff + timedelta(minutes=1)
        )


def test_checked_at_and_bar_cutoff_cannot_be_from_the_future() -> None:
    rows, cutoff = _complete_rows()
    with pytest.raises(SnapshotAvailabilityError, match="check predates"):
        assemble_verified_snapshot_payload(
            rows, _spec(cutoff), checked_at=cutoff - timedelta(microseconds=1)
        )
    future_bar = (*rows[:-1], replace(rows[-1], available_at=cutoff + timedelta(seconds=1)))
    with pytest.raises(SnapshotAvailabilityError, match="timestamps violate"):
        assemble_verified_snapshot_payload(
            future_bar, _spec(cutoff), checked_at=cutoff + timedelta(minutes=1)
        )


def test_policy_window_covers_contract_maximum_baseline_history() -> None:
    policy = ForecastSnapshotBuildPolicy()
    assert policy.observation_limit == 512
    assert policy.minimum_observations == 258
    assert policy.target_time_count == 252


def test_daily_cutoff_is_stable_across_midnight_redelivery() -> None:
    before = scheduled_snapshot_cutoff(datetime(2026, 7, 13, 17, 1, tzinfo=UTC))
    after_midnight = scheduled_snapshot_cutoff(datetime(2026, 7, 14, 5, tzinfo=UTC))
    next_slot = scheduled_snapshot_cutoff(datetime(2026, 7, 14, 17, tzinfo=UTC))
    assert before == after_midnight == datetime(2026, 7, 13, 17, tzinfo=UTC)
    assert next_slot == datetime(2026, 7, 14, 17, tzinfo=UTC)


def test_policy_document_matches_ad_hoc_and_scheduled_cutoff_behavior() -> None:
    cutoff_policy = RESOLUTION_POLICY_DOCUMENT["cutoff"]
    assert isinstance(cutoff_policy, dict)
    assert cutoff_policy["scheduled_default"] == "most_recent_daily_17:00:00Z"
    assert POLICY.validate_spec(
        _spec(datetime(2026, 7, 13, 17, 0, 1, tzinfo=UTC))
    ).as_of == datetime(2026, 7, 13, 17, 0, 1, tzinfo=UTC)
