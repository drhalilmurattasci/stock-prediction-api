"""Vendor-neutral exchange-calendar helpers shared by operators and ingestion."""

from __future__ import annotations

from datetime import UTC, date, datetime

import exchange_calendars as xcals
import pandas as pd


def latest_completed_xnys_session(value: datetime) -> date:
    """Resolve the most recent XNYS session whose official close has passed."""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("clock must return a timezone-aware datetime")
    now = value.astimezone(UTC)
    calendar = xcals.get_calendar("XNYS")
    label = calendar.date_to_session(pd.Timestamp(now.date()), direction="previous")
    if calendar.session_close(label).to_pydatetime() > now:
        label = calendar.previous_session(label)
    return label.date()


__all__ = ["latest_completed_xnys_session"]
