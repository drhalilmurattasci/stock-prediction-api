"""Pinned, point-in-time resolution of one realized raw-close outcome.

This module is deliberately inert: it defines no API route, task, Beat entry,
or vendor call.  A caller must supply the exact target and the deterministic
cutoff covered by an explicit policy.  Once the PostgreSQL clock reaches that
cutoff, the resolver selects one exact post-commit-visible bar version or
fails closed.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from importlib.metadata import version as package_version
from typing import Any, Protocol, runtime_checkable

import exchange_calendars
import pandas as pd
from fastapi import status
from sqlalchemy import Select, and_, func, select, text, union
from sqlalchemy.exc import DBAPIError, SQLAlchemyError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.exceptions import AppError
from app.db.models.bars import Bar, BarRevision, BarVersionAvailability
from app.services.forecast_outcomes import BarVersionEvidence
from ingestion.locks import acquire_advisory_xact_lock, bar_series_lock_id

_CALENDAR_ENGINE_VERSION = "4.13.2"
_CALENDAR_TZDATA_VERSION = package_version("tzdata")
_PANDAS_VERSION = package_version("pandas")
_CALENDAR_START = "1990-01-01"
_CALENDAR_END = "2100-12-31"
_HASH_PREFIX = "sha256:"
_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9.\-_:]+$")
_MAX_RESOLUTION_LAG_SECONDS = 366 * 24 * 60 * 60


class OutcomeResolutionPolicyError(ValueError):
    """An outcome-resolution request is outside the pinned v1 contract."""


class OutcomeResolutionPolicyMismatch(OutcomeResolutionPolicyError):
    """Caller-supplied evidence does not match its deterministic policy."""


class OutcomeResolutionMisconfigured(OutcomeResolutionPolicyError):
    """The installed resolver cannot reproduce its hashed policy."""


def _canonical_document(document: Mapping[str, object]) -> bytes:
    return json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _hash_document(document: Mapping[str, object]) -> str:
    return _HASH_PREFIX + hashlib.sha256(_canonical_document(document)).hexdigest()


_OUTCOME_AVAILABILITY_RULE_SET_DOCUMENT: dict[str, object] = {
    "format": "forecast-outcome-availability-rules-v1",
    "schema_version": 1,
    "rules": [
        "exact_polygon_open_close_raw_day_one_source_key",
        "post_commit_receipt_for_exact_bar_version",
        "receipt_available_at_not_after_resolution_cutoff",
        "maximum_version_recorded_at_among_cutoff_visible_versions",
        "distinct_candidates_tied_at_maximum_are_rejected",
        "observed_at_is_exact_xnys_regular_session_close",
        "observed_at<=fetched_at<=source_as_of<=version_recorded_at"
        "<=post_commit_available_at<=resolution_cutoff",
        "finite_nonnegative_raw_close",
        "database_clock_not_before_resolution_cutoff",
        "transaction_isolation_is_read_committed",
        "receipt_writers_take_database_enforced_series_fence",
        "outcome_publication_requires_preheld_series_fence",
    ],
    "version_visibility": {
        "deduplication": "sql_union_exact_candidate_bytes",
        "reconstruction_lanes": [
            "bars.current",
            "bars_revisions.previous",
            "bars_revisions.incoming",
        ],
        "receipt_order": "irrelevant_to_version_selection",
        "selection": (
            "unique maximum version_recorded_at whose exact receipt.available_at<=resolution_cutoff"
        ),
        "same_maximum_distinct_candidates": "reject",
        "same_transaction_receipt": "rejected_by_database",
        "legacy_or_unfinalized_version": "not_visible",
        "transaction_isolation": "read_committed",
        "writer_exclusion": {
            "identity": "sha256_length_framed_first_signed_bigint_v1",
            "namespace": "stockapi.bar-series-fence.v1",
            "outcome_publication": "preheld_transaction_advisory_lock",
            "receipt_publication": "database_trigger_transaction_advisory_lock",
        },
    },
}
_OUTCOME_AVAILABILITY_RULE_SET_CANONICAL = _canonical_document(
    _OUTCOME_AVAILABILITY_RULE_SET_DOCUMENT
)
OUTCOME_AVAILABILITY_RULE_SET_HASH = (
    _HASH_PREFIX + hashlib.sha256(_OUTCOME_AVAILABILITY_RULE_SET_CANONICAL).hexdigest()
)


def outcome_availability_rule_set_document() -> dict[str, object]:
    """Return a detached copy of the exact availability policy evidence."""

    value = json.loads(_OUTCOME_AVAILABILITY_RULE_SET_CANONICAL.decode("utf-8"))
    if not isinstance(value, dict):  # pragma: no cover - frozen module invariant
        raise OutcomeResolutionMisconfigured("availability policy is not an object")
    return value


@dataclass(frozen=True)
class OutcomeResolutionRequest:
    """Normalized identity and deterministic cutoff for one v1 outcome."""

    symbol: str
    target_time: datetime
    resolution_cutoff: datetime


@dataclass(frozen=True)
class ForecastOutcomeResolutionPolicy:
    """Every choice that can change realized-outcome truth selection.

    ``resolution_lag_seconds`` intentionally has no default.  The outcome
    table's semantic key excludes ``resolution_cutoff``; binding one exact lag
    into the policy identity therefore guarantees one cutoff per target under
    that policy.
    """

    resolution_lag_seconds: int

    def __post_init__(self) -> None:
        if (
            type(self.resolution_lag_seconds) is not int
            or self.resolution_lag_seconds <= 0
            or self.resolution_lag_seconds > _MAX_RESOLUTION_LAG_SECONDS
        ):
            raise OutcomeResolutionMisconfigured(
                "resolution_lag_seconds must be a positive bounded integer"
            )

    @property
    def availability_rule_set_hash(self) -> str:
        return OUTCOME_AVAILABILITY_RULE_SET_HASH

    @property
    def outcome_resolution_policy_hash(self) -> str:
        return _hash_document(self.outcome_resolution_policy_document)

    @property
    def canonical_policy(self) -> bytes:
        """Canonical bytes registered at the database policy boundary."""

        return _canonical_document(self.outcome_resolution_policy_document)

    @property
    def outcome_resolution_policy_document(self) -> dict[str, object]:
        return {
            "format": "forecast-outcome-resolution-policy-v1",
            "schema_version": 1,
            "availability_rule_set_hash": self.availability_rule_set_hash,
            "calendar": {
                "engine": "exchange_calendars",
                "engine_version": _CALENDAR_ENGINE_VERSION,
                "name": "XNYS",
                "pandas_version": _PANDAS_VERSION,
                "schedule_start": _CALENDAR_START,
                "schedule_end": _CALENDAR_END,
                "target_timestamp": "regular_session_close_utc",
                "tzdata_version": _CALENDAR_TZDATA_VERSION,
            },
            "currency": {
                "resolver": "fixed_us_equity_v1",
                "value": "USD",
            },
            "cutoff": {
                "formula": "target_time_utc+resolution_lag_seconds",
                "resolution_lag_seconds": self.resolution_lag_seconds,
                "maturity_clock": "postgresql_clock_timestamp",
            },
            "source": {
                "adjustment_basis": "raw",
                "field": "close",
                "multiplier": 1,
                "provider_endpoint": "/v1/open-close/{ticker}/{date}?adjusted=false",
                "provider_semantics": (
                    "regular_session_close; preMarket and afterHours are separate fields"
                ),
                "source": "polygon_open_close",
                "timespan": "day",
            },
            "target": {"name": "close", "series_basis": "raw", "transform": "identity"},
        }

    def cutoff_for(self, target_time: datetime) -> datetime:
        target = _utc(target_time, "target_time")
        try:
            return target + timedelta(seconds=self.resolution_lag_seconds)
        except OverflowError as exc:
            raise OutcomeResolutionPolicyError(
                "target_time cannot represent the policy cutoff"
            ) from exc

    def validate_request(
        self,
        *,
        symbol: str,
        target_time: datetime,
        resolution_cutoff: datetime,
    ) -> OutcomeResolutionRequest:
        normalized_symbol = _canonical_symbol(symbol)
        target = _utc(target_time, "target_time")
        cutoff = _utc(resolution_cutoff, "resolution_cutoff")
        _validate_xnys_close(target)
        if cutoff != self.cutoff_for(target):
            raise OutcomeResolutionPolicyMismatch(
                "resolution_cutoff does not equal the policy-derived cutoff"
            )
        return OutcomeResolutionRequest(
            symbol=normalized_symbol,
            target_time=target,
            resolution_cutoff=cutoff,
        )

    def validate_hashes(
        self,
        *,
        outcome_resolution_policy_hash: str,
        availability_rule_set_hash: str,
    ) -> None:
        if outcome_resolution_policy_hash != self.outcome_resolution_policy_hash:
            raise OutcomeResolutionPolicyMismatch(
                "outcome resolution policy hash does not match the resolver"
            )
        if availability_rule_set_hash != self.availability_rule_set_hash:
            raise OutcomeResolutionPolicyMismatch(
                "outcome availability rule-set hash does not match the resolver"
            )


def build_exact_bar_version_statement(
    request: OutcomeResolutionRequest,
) -> Select[Any]:
    """Return every distinct candidate tied at the newest visible version.

    Exact duplicate evidence is collapsed by ``UNION``.  Distinct evidence at
    the same greatest ``version_recorded_at`` remains visible to the caller,
    which must reject it rather than letting row order choose truth.
    """

    current_filters = (
        Bar.symbol == request.symbol,
        Bar.timespan == "day",
        Bar.multiplier == 1,
        Bar.ts == request.target_time,
        Bar.source == "polygon_open_close",
        Bar.adjustment_basis == "raw",
    )
    revision_filters = (
        BarRevision.symbol == request.symbol,
        BarRevision.timespan == "day",
        BarRevision.multiplier == 1,
        BarRevision.ts == request.target_time,
        BarRevision.source == "polygon_open_close",
        BarRevision.adjustment_basis == "raw",
    )
    current = select(
        Bar.symbol.label("symbol"),
        Bar.timespan.label("timespan"),
        Bar.multiplier.label("multiplier"),
        Bar.ts.label("observed_at"),
        Bar.source.label("source"),
        Bar.adjustment_basis.label("adjustment_basis"),
        Bar.fetched_at.label("fetched_at"),
        Bar.as_of.label("source_as_of"),
        Bar.recorded_at.label("version_recorded_at"),
        Bar.close.label("value"),
    ).where(*current_filters)
    previous = select(
        BarRevision.symbol,
        BarRevision.timespan,
        BarRevision.multiplier,
        BarRevision.ts.label("observed_at"),
        BarRevision.source,
        BarRevision.adjustment_basis,
        BarRevision.previous_fetched_at.label("fetched_at"),
        BarRevision.previous_as_of.label("source_as_of"),
        BarRevision.previous_recorded_at.label("version_recorded_at"),
        BarRevision.previous_close.label("value"),
    ).where(*revision_filters, BarRevision.previous_recorded_at.is_not(None))
    incoming = select(
        BarRevision.symbol,
        BarRevision.timespan,
        BarRevision.multiplier,
        BarRevision.ts.label("observed_at"),
        BarRevision.source,
        BarRevision.adjustment_basis,
        BarRevision.incoming_fetched_at.label("fetched_at"),
        BarRevision.incoming_as_of.label("source_as_of"),
        BarRevision.incoming_recorded_at.label("version_recorded_at"),
        BarRevision.incoming_close.label("value"),
    ).where(*revision_filters, BarRevision.incoming_recorded_at.is_not(None))
    versions = union(current, previous, incoming).subquery("outcome_bar_versions")
    receipt = BarVersionAvailability
    ranked = (
        select(
            *versions.c,
            receipt.available_at.label("available_at"),
            func.rank().over(order_by=versions.c.version_recorded_at.desc()).label("version_rank"),
        )
        .join(
            receipt,
            and_(
                receipt.symbol == versions.c.symbol,
                receipt.timespan == versions.c.timespan,
                receipt.multiplier == versions.c.multiplier,
                receipt.ts == versions.c.observed_at,
                receipt.source == versions.c.source,
                receipt.adjustment_basis == versions.c.adjustment_basis,
                receipt.version_recorded_at == versions.c.version_recorded_at,
            ),
        )
        .where(receipt.available_at <= request.resolution_cutoff)
        .subquery("ranked_outcome_bar_versions")
    )
    return (
        select(
            ranked.c.symbol,
            ranked.c.timespan,
            ranked.c.multiplier,
            ranked.c.observed_at,
            ranked.c.source,
            ranked.c.adjustment_basis,
            ranked.c.fetched_at,
            ranked.c.source_as_of,
            ranked.c.version_recorded_at,
            ranked.c.available_at,
            ranked.c.value,
        )
        .where(ranked.c.version_rank == 1)
        .order_by(
            ranked.c.version_recorded_at.desc(),
            ranked.c.fetched_at.desc(),
            ranked.c.source_as_of.desc(),
            ranked.c.value.desc(),
        )
        .limit(2)
    )


@runtime_checkable
class OutcomeBarVersionResolver(Protocol):
    @property
    def outcome_resolution_policy_hash(self) -> str: ...

    @property
    def availability_rule_set_hash(self) -> str: ...

    async def resolve(
        self,
        *,
        symbol: str,
        target_time: datetime,
        resolution_cutoff: datetime,
    ) -> BarVersionEvidence: ...


@dataclass(frozen=True)
class SqlOutcomeBarVersionResolver:
    """Resolve one immutable bar-version claim after its policy cutoff."""

    sessionmaker: async_sessionmaker[AsyncSession]
    policy: ForecastOutcomeResolutionPolicy

    @property
    def outcome_resolution_policy_hash(self) -> str:
        return self.policy.outcome_resolution_policy_hash

    @property
    def availability_rule_set_hash(self) -> str:
        return self.policy.availability_rule_set_hash

    async def resolve(
        self,
        *,
        symbol: str,
        target_time: datetime,
        resolution_cutoff: datetime,
    ) -> BarVersionEvidence:
        try:
            request = self.policy.validate_request(
                symbol=symbol,
                target_time=target_time,
                resolution_cutoff=resolution_cutoff,
            )
        except OutcomeResolutionMisconfigured as exc:
            raise _configuration_invalid() from exc
        except OutcomeResolutionPolicyMismatch as exc:
            raise _policy_mismatch() from exc
        except OutcomeResolutionPolicyError as exc:
            raise _invalid_request() from exc

        try:
            async with self.sessionmaker() as session, session.begin():
                # Pin the snapshot contract before the transaction's first
                # query.  READ COMMITTED gives the post-wait selection query a
                # fresh statement snapshot after an in-flight receipt commits.
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
                # Receipt publication is a second ingestion transaction under
                # this same lane lock.  Waiting for it before reading the DB
                # clock prevents an in-flight receipt stamped before cutoff
                # from appearing only after resolution declared the set closed.
                await acquire_advisory_xact_lock(
                    session,
                    bar_series_lock_id(request.symbol, "polygon_open_close", "day"),
                )
                database_now = _utc(
                    (await session.execute(select(func.clock_timestamp()))).scalar_one(),
                    "database clock",
                )
                if database_now < request.resolution_cutoff:
                    raise _not_ready()
                result = await session.execute(build_exact_bar_version_statement(request))
                rows = tuple(result.mappings().all())
        except AppError:
            raise
        except OutcomeResolutionPolicyError as exc:
            raise _corrupt() from exc
        except SQLAlchemyTimeoutError as exc:
            raise _unavailable() from exc
        except DBAPIError as exc:
            raise _database_error(exc) from exc
        except SQLAlchemyError as exc:
            raise _configuration_invalid() from exc

        if not rows:
            raise _source_unavailable()
        if len(rows) != 1:
            raise _ambiguous()
        try:
            return _evidence_from_row(dict(rows[0]), request)
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise _corrupt() from exc


def _evidence_from_row(
    row: Mapping[str, object],
    request: OutcomeResolutionRequest,
) -> BarVersionEvidence:
    symbol = row["symbol"]
    timespan = row["timespan"]
    multiplier = row["multiplier"]
    source = row["source"]
    adjustment_basis = row["adjustment_basis"]
    observed_at = _utc(row["observed_at"], "bar observed_at")
    fetched_at = _utc(row["fetched_at"], "bar fetched_at")
    source_as_of = _utc(row["source_as_of"], "bar source_as_of")
    version_recorded_at = _utc(row["version_recorded_at"], "bar version_recorded_at")
    available_at = _utc(row["available_at"], "bar available_at")
    value = _finite_nonnegative(row["value"], "bar close")
    if (
        symbol != request.symbol
        or timespan != "day"
        or type(multiplier) is not int
        or multiplier != 1
        or source != "polygon_open_close"
        or adjustment_basis != "raw"
        or observed_at != request.target_time
        or not (
            observed_at
            <= fetched_at
            <= source_as_of
            <= version_recorded_at
            <= available_at
            <= request.resolution_cutoff
        )
    ):
        raise ValueError("resolved evidence violates the pinned availability rules")
    return BarVersionEvidence(
        symbol=request.symbol,
        timespan="day",
        multiplier=1,
        observed_at=request.target_time,
        source="polygon_open_close",
        adjustment_basis="raw",
        fetched_at=fetched_at,
        source_as_of=source_as_of,
        version_recorded_at=version_recorded_at,
        available_at=available_at,
        field="close",
        value=value,
    )


@lru_cache(maxsize=1)
def _xnys_calendar() -> Any:
    if exchange_calendars.__version__ != _CALENDAR_ENGINE_VERSION:
        raise OutcomeResolutionMisconfigured(
            "exchange_calendars version differs from the hashed outcome policy"
        )
    try:
        return exchange_calendars.get_calendar(
            "XNYS",
            start=_CALENDAR_START,
            end=_CALENDAR_END,
        )
    except (KeyError, ValueError) as exc:
        raise OutcomeResolutionMisconfigured(
            "the pinned outcome exchange calendar is unavailable"
        ) from exc


def _validate_xnys_close(target_time: datetime) -> None:
    calendar = _xnys_calendar()
    label = pd.Timestamp(target_time.date())
    try:
        if not calendar.is_session(label):
            raise OutcomeResolutionPolicyError("target_time date is not an XNYS session")
        session_close = _utc(
            calendar.session_close(label).to_pydatetime(),
            "XNYS session close",
        )
    except OutcomeResolutionPolicyError:
        raise
    except (KeyError, ValueError, OverflowError) as exc:
        raise OutcomeResolutionPolicyError(
            "target_time is outside the pinned XNYS schedule"
        ) from exc
    if target_time != session_close:
        raise OutcomeResolutionPolicyError(
            "target_time must equal the exact XNYS regular session close"
        )


def _canonical_symbol(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > 32
        or value != value.upper()
        or _SYMBOL_PATTERN.fullmatch(value) is None
    ):
        raise OutcomeResolutionPolicyError("symbol must be uppercase and canonical")
    return value


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise OutcomeResolutionPolicyError(f"{label} must be timezone-aware")
    try:
        if value.utcoffset() is None:
            raise OutcomeResolutionPolicyError(f"{label} must be timezone-aware")
        return value.astimezone(UTC)
    except (ValueError, OverflowError, OSError) as exc:
        raise OutcomeResolutionPolicyError(f"{label} cannot be normalized to UTC") from exc


def _finite_nonnegative(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{label} must be a real number")
    number = float(value)
    if not math.isfinite(number) or number < 0.0:
        raise ValueError(f"{label} must be finite and nonnegative")
    return 0.0 if number == 0.0 else number


def _db_error_attribute(exc: BaseException, name: str) -> str | None:
    pending: list[object] = [exc, getattr(exc, "orig", None)]
    seen: set[int] = set()
    while pending:
        candidate = pending.pop(0)
        if candidate is None or id(candidate) in seen:
            continue
        seen.add(id(candidate))
        value = getattr(candidate, name, None)
        if isinstance(value, str) and value:
            return value
        pending.extend(
            (
                getattr(candidate, "__cause__", None),
                getattr(candidate, "__context__", None),
            )
        )
    return None


def _database_error(exc: DBAPIError) -> AppError:
    sqlstate = _db_error_attribute(exc, "sqlstate")
    if sqlstate is not None and sqlstate.startswith(("0A", "22", "28", "3D", "3F", "42")):
        return _configuration_invalid()
    return _unavailable()


def _invalid_request() -> AppError:
    return AppError(
        "The realized-outcome resolution request is invalid.",
        code="forecast_outcome_resolution_invalid",
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        details={"retryable": False},
    )


def _policy_mismatch() -> AppError:
    return AppError(
        "The realized-outcome cutoff does not match the pinned policy.",
        code="forecast_outcome_resolution_policy_mismatch",
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        details={"retryable": False},
    )


def _not_ready() -> AppError:
    return AppError(
        "The realized-outcome resolution cutoff has not matured.",
        code="forecast_outcome_resolution_not_ready",
        status_code=status.HTTP_409_CONFLICT,
        details={"retryable": True},
    )


def _source_unavailable() -> AppError:
    return AppError(
        "No finalized source version was available at the outcome cutoff.",
        code="forecast_outcome_source_unavailable",
        status_code=status.HTTP_409_CONFLICT,
        details={"retryable": False},
    )


def _ambiguous() -> AppError:
    return AppError(
        "Realized-outcome source evidence has an ambiguous newest version.",
        code="forecast_outcome_source_ambiguous",
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        details={"retryable": False},
    )


def _corrupt() -> AppError:
    return AppError(
        "Realized-outcome source evidence failed validation.",
        code="forecast_outcome_source_corrupt",
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        details={"retryable": False},
    )


def _unavailable() -> AppError:
    return AppError(
        "Realized-outcome source storage is unavailable.",
        code="forecast_outcome_source_store_unavailable",
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        details={"retryable": True},
    )


def _configuration_invalid() -> AppError:
    return AppError(
        "Realized-outcome source database configuration is invalid.",
        code="forecast_outcome_source_configuration_invalid",
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        details={"retryable": False},
    )


__all__ = [
    "ForecastOutcomeResolutionPolicy",
    "OUTCOME_AVAILABILITY_RULE_SET_HASH",
    "OutcomeBarVersionResolver",
    "OutcomeResolutionMisconfigured",
    "OutcomeResolutionPolicyError",
    "OutcomeResolutionPolicyMismatch",
    "OutcomeResolutionRequest",
    "SqlOutcomeBarVersionResolver",
    "build_exact_bar_version_statement",
    "outcome_availability_rule_set_document",
]
