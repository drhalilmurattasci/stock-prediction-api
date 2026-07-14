"""Massive/Polygon regular-session daily-close adapter.

The custom aggregate endpoint used by ``PolygonProvider`` can include extended
hours. Forecast inputs therefore use the official per-session open/close
endpoint, whose ``close`` and ``afterHours`` fields are distinct. Only the
regular-session OHLCV fields are normalized here.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from functools import lru_cache
from typing import Any

import exchange_calendars as xcals
import pandas as pd

from data_sources.base import OHLCVBar, ProviderError
from data_sources.polygon import DEFAULT_BASE_URL, PolygonProvider

SOURCE_NAME = "polygon_open_close"
EXCHANGE_CALENDAR = "XNYS"


class OpenClosePayloadError(ProviderError):
    """The response cannot prove a completed regular-session bar."""


@lru_cache(maxsize=1)
def _xnys_calendar() -> Any:
    return xcals.get_calendar(EXCHANGE_CALENDAR)


def _as_utc(value: datetime, label: str) -> datetime:
    if value.tzinfo is None:
        raise OpenClosePayloadError(f"{label} must be timezone-aware")
    return value.astimezone(UTC)


def open_close_endpoint_identity(symbol: str, session_date: date) -> str:
    """Return the guard identity for one exact unadjusted session request."""

    return f"/v1/open-close/{symbol}/{session_date.isoformat()}?adjusted=false"


class PolygonOpenCloseProvider(PolygonProvider):
    """Fetch raw regular-session daily OHLCV from ``/v1/open-close``."""

    name = SOURCE_NAME

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = DEFAULT_BASE_URL,
        **kwargs: Any,
    ) -> None:
        super().__init__(api_key, base_url=base_url, **kwargs)

    async def get_daily_bars(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjusted: bool = False,
    ) -> list[OHLCVBar]:
        """Return one raw bar for every XNYS session in the inclusive range."""

        if adjusted:
            raise ValueError("regular-session forecast closes must be unadjusted")
        if start > end:
            raise ValueError("start must be on or before end")

        sym = symbol.strip().upper()
        if not sym:
            raise ValueError("symbol must not be empty")

        calendar = _xnys_calendar()
        sessions = calendar.sessions_in_range(pd.Timestamp(start), pd.Timestamp(end))
        bars: list[OHLCVBar] = []
        for session in sessions:
            session_date = session.date()
            path = f"/v1/open-close/{sym}/{session_date.isoformat()}"
            payload = await self._get(
                path,
                {"adjusted": "false"},
                endpoint=open_close_endpoint_identity(sym, session_date),
            )
            # The clock is deliberately sampled only after the complete response
            # has been received and decoded by ``_get``.
            fetched_at = _as_utc(self._clock(), "fetched_at")
            bars.append(
                self._to_open_close_bar(
                    sym,
                    session,
                    payload,
                    fetched_at=fetched_at,
                    calendar=calendar,
                )
            )
        return bars

    def _to_open_close_bar(
        self,
        symbol: str,
        session: pd.Timestamp,
        payload: dict[str, Any],
        *,
        fetched_at: datetime,
        calendar: Any,
    ) -> OHLCVBar:
        session_date = session.date()
        if payload.get("status") != "OK":
            raise OpenClosePayloadError("daily open/close response status is not OK")
        if payload.get("symbol") != symbol:
            raise OpenClosePayloadError("daily open/close response symbol does not match request")
        if payload.get("from") != session_date.isoformat():
            raise OpenClosePayloadError("daily open/close response date does not match request")

        session_close = _as_utc(
            calendar.session_close(session).to_pydatetime(),
            "session close",
        )
        if fetched_at < session_close:
            raise OpenClosePayloadError(
                "daily open/close response arrived before the session closed"
            )

        required = ("open", "high", "low", "close", "volume")
        missing = [field for field in required if payload.get(field) is None]
        if missing:
            raise OpenClosePayloadError(
                f"daily open/close response is missing fields: {', '.join(missing)}"
            )

        return OHLCVBar(
            symbol=symbol,
            timestamp=session_close,
            timespan="day",
            multiplier=1,
            open=payload["open"],
            high=payload["high"],
            low=payload["low"],
            close=payload["close"],
            volume=payload["volume"],
            vwap=None,
            trade_count=None,
            adjustment_basis="raw",
            source=self.name,
            fetched_at=fetched_at,
        )
