"""Explicit, append-only registration of prospective forecast-selection policies."""

from __future__ import annotations

import hmac
from dataclasses import dataclass
from datetime import date, datetime
from typing import Protocol, runtime_checkable

from fastapi import status
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.exceptions import AppError
from app.db.models.forecast_evidence import ForecastSelectionPolicyRegistration
from app.services.forecast_selection_policies import (
    ForecastSelectionPolicyValidationError,
    ProspectiveForecastSelectionPolicy,
    canonical_selection_policy,
    parse_selection_policy,
    selection_policy_hash_for,
)

_POLICY_PRIMARY_KEY = "pk_forecast_selection_policies"
_POLICY_OUTCOME_EPOCH_UNIQUE = "uq_forecast_selection_policies_outcome_epoch"


@dataclass(frozen=True)
class ForecastSelectionPolicyRecord:
    """Detached database evidence for one immutable selection-policy registration."""

    policy_hash: str
    schema_version: int
    forecast_resolution_policy_hash: str
    forecast_availability_rule_set_hash: str
    outcome_resolution_policy_hash: str
    outcome_availability_rule_set_hash: str
    resolution_lag_seconds: int
    fit_window_start: date
    fit_window_end: date
    heldout_window_start: date
    heldout_window_end: date
    minimum_fit_member_count: int
    minimum_heldout_member_count: int
    minimum_seal_lead_seconds: int
    selected_steps: tuple[int, ...]
    canonical_policy: bytes
    recorded_at: datetime
    creator_xid: int


@dataclass(frozen=True)
class ForecastSelectionPolicyProof:
    """An exact registration replay bound to the canonical policy supplied by the caller."""

    policy: ProspectiveForecastSelectionPolicy
    record: ForecastSelectionPolicyRecord


@runtime_checkable
class ForecastSelectionPolicyStore(Protocol):
    """Register one explicit selection policy; no scientific default is supplied."""

    async def register(
        self,
        policy: ProspectiveForecastSelectionPolicy,
    ) -> ForecastSelectionPolicyProof: ...


@dataclass(frozen=True)
class _PreparedPolicy:
    policy: ProspectiveForecastSelectionPolicy
    policy_hash: str
    canonical_policy: bytes


@dataclass(frozen=True)
class SqlForecastSelectionPolicyStore:
    """Short-transaction selection-policy registry with exact race reconciliation."""

    sessionmaker: async_sessionmaker[AsyncSession]

    async def register(
        self,
        policy: ProspectiveForecastSelectionPolicy,
    ) -> ForecastSelectionPolicyProof:
        prepared = _prepare(policy)
        existing = await self._preflight(prepared)
        if existing is not None:
            return existing
        return await self._commit(prepared)

    async def _preflight(
        self,
        prepared: _PreparedPolicy,
    ) -> ForecastSelectionPolicyProof | None:
        try:
            async with self.sessionmaker() as session:
                row = await session.get(
                    ForecastSelectionPolicyRegistration,
                    prepared.policy_hash,
                )
        except SQLAlchemyTimeoutError as exc:
            raise _unavailable() from exc
        except DBAPIError as exc:
            raise _database_error(exc) from exc
        return None if row is None else _exact(row, prepared)

    async def _commit(
        self,
        prepared: _PreparedPolicy,
    ) -> ForecastSelectionPolicyProof:
        commit_pending = False
        try:
            async with self.sessionmaker() as session, session.begin():
                existing = await session.get(
                    ForecastSelectionPolicyRegistration,
                    prepared.policy_hash,
                )
                if existing is not None:
                    return _exact(existing, prepared)
                result = await session.execute(
                    text("SELECT public.register_forecast_selection_policy(:canonical_policy)"),
                    {"canonical_policy": prepared.canonical_policy},
                )
                if result.scalar_one() != prepared.policy_hash:
                    raise _corrupt()
                candidate = await session.get(
                    ForecastSelectionPolicyRegistration,
                    prepared.policy_hash,
                )
                if candidate is None:
                    raise _corrupt()
                _exact(candidate, prepared)
                commit_pending = True
            return await self._read_committed(prepared)
        except IntegrityError as exc:
            if _is_race(exc):
                winner = await self._try_reconcile(prepared)
                if winner is not None:
                    return winner
                raise _write_conflict() from exc
            raise _integrity_error() from exc
        except SQLAlchemyTimeoutError as exc:
            if commit_pending:
                return await self._reconcile_or_unknown(prepared, exc)
            raise _unavailable() from exc
        except DBAPIError as exc:
            if _is_integrity_state(exc):
                raise _integrity_error() from exc
            if _is_configuration_error(exc):
                raise _configuration_invalid() from exc
            if _is_statement_completion_unknown(exc):
                return await self._reconcile_or_unknown(prepared, exc)
            if _is_known_rollback(exc):
                raise _unavailable() from exc
            if commit_pending:
                return await self._reconcile_or_unknown(prepared, exc)
            raise _database_error(exc) from exc

    async def _read_committed(
        self,
        prepared: _PreparedPolicy,
    ) -> ForecastSelectionPolicyProof:
        try:
            async with self.sessionmaker() as session:
                row = await session.get(
                    ForecastSelectionPolicyRegistration,
                    prepared.policy_hash,
                )
        except SQLAlchemyTimeoutError as exc:
            raise _unavailable() from exc
        except DBAPIError as exc:
            raise _database_error(exc) from exc
        if row is None:
            raise _unavailable()
        return _exact(row, prepared)

    async def _reconcile_or_unknown(
        self,
        prepared: _PreparedPolicy,
        cause: BaseException,
    ) -> ForecastSelectionPolicyProof:
        proof = await self._try_reconcile(prepared)
        if proof is not None:
            return proof
        raise _commit_unknown() from cause

    async def _try_reconcile(
        self,
        prepared: _PreparedPolicy,
    ) -> ForecastSelectionPolicyProof | None:
        try:
            async with self.sessionmaker() as session:
                row = await session.get(
                    ForecastSelectionPolicyRegistration,
                    prepared.policy_hash,
                )
        except (DBAPIError, SQLAlchemyTimeoutError):
            return None
        return None if row is None else _exact(row, prepared)


def _prepare(policy: ProspectiveForecastSelectionPolicy) -> _PreparedPolicy:
    try:
        canonical = canonical_selection_policy(policy)
        normalized = parse_selection_policy(canonical)
        policy_hash = selection_policy_hash_for(canonical)
        if normalized != policy or not hmac.compare_digest(
            policy_hash,
            policy.selection_policy_hash,
        ):
            raise ForecastSelectionPolicyValidationError(
                "selection policy properties disagree with canonical evidence"
            )
    except (ForecastSelectionPolicyValidationError, AttributeError, TypeError, ValueError) as exc:
        raise _invalid() from exc
    return _PreparedPolicy(
        policy=normalized,
        policy_hash=policy_hash,
        canonical_policy=canonical,
    )


def _exact(
    row: ForecastSelectionPolicyRegistration,
    prepared: _PreparedPolicy,
) -> ForecastSelectionPolicyProof:
    try:
        record = _detach(row)
        stored = parse_selection_policy(record.canonical_policy)
        if (
            not hmac.compare_digest(
                record.policy_hash,
                selection_policy_hash_for(record.canonical_policy),
            )
            or record.schema_version != stored.schema_version
            or record.forecast_resolution_policy_hash != stored.forecast_resolution_policy_hash
            or record.forecast_availability_rule_set_hash
            != stored.forecast_availability_rule_set_hash
            or record.outcome_resolution_policy_hash != stored.outcome_resolution_policy_hash
            or record.outcome_availability_rule_set_hash
            != stored.outcome_availability_rule_set_hash
            or record.resolution_lag_seconds != stored.resolution_lag_seconds
            or record.fit_window_start != stored.fit_window.start
            or record.fit_window_end != stored.fit_window.end
            or record.heldout_window_start != stored.heldout_window.start
            or record.heldout_window_end != stored.heldout_window.end
            or record.minimum_fit_member_count != stored.minimum_fit_member_count
            or record.minimum_heldout_member_count != stored.minimum_heldout_member_count
            or record.minimum_seal_lead_seconds != stored.minimum_seal_lead_seconds
            or record.selected_steps != stored.selected_steps
            or record.creator_xid <= 0
            or record.recorded_at.tzinfo is None
            or record.recorded_at.utcoffset() is None
        ):
            raise ValueError("stored selection-policy headers disagree with canonical evidence")
    except (AttributeError, ForecastSelectionPolicyValidationError, TypeError, ValueError) as exc:
        raise _corrupt() from exc
    if (
        hmac.compare_digest(record.policy_hash, prepared.policy_hash)
        and hmac.compare_digest(record.canonical_policy, prepared.canonical_policy)
        and stored == prepared.policy
    ):
        return ForecastSelectionPolicyProof(policy=prepared.policy, record=record)
    if hmac.compare_digest(record.policy_hash, prepared.policy_hash):
        raise _semantic_conflict()
    raise _corrupt()


def _detach(row: ForecastSelectionPolicyRegistration) -> ForecastSelectionPolicyRecord:
    return ForecastSelectionPolicyRecord(
        policy_hash=row.policy_hash,
        schema_version=row.schema_version,
        forecast_resolution_policy_hash=row.forecast_resolution_policy_hash,
        forecast_availability_rule_set_hash=row.forecast_availability_rule_set_hash,
        outcome_resolution_policy_hash=row.outcome_resolution_policy_hash,
        outcome_availability_rule_set_hash=row.outcome_availability_rule_set_hash,
        resolution_lag_seconds=row.resolution_lag_seconds,
        fit_window_start=row.fit_window_start,
        fit_window_end=row.fit_window_end,
        heldout_window_start=row.heldout_window_start,
        heldout_window_end=row.heldout_window_end,
        minimum_fit_member_count=row.minimum_fit_member_count,
        minimum_heldout_member_count=row.minimum_heldout_member_count,
        minimum_seal_lead_seconds=row.minimum_seal_lead_seconds,
        selected_steps=tuple(row.selected_steps),
        canonical_policy=bytes(row.canonical_policy),
        recorded_at=row.recorded_at,
        creator_xid=row.creator_xid,
    )


def _db_error_attribute(exc: DBAPIError, name: str) -> str | None:
    pending: list[object] = [exc, exc.orig]
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
            (getattr(candidate, "__cause__", None), getattr(candidate, "__context__", None))
        )
    return None


def _is_race(exc: IntegrityError) -> bool:
    return _db_error_attribute(exc, "sqlstate") == "23505" and _db_error_attribute(
        exc, "constraint_name"
    ) in {_POLICY_PRIMARY_KEY, _POLICY_OUTCOME_EPOCH_UNIQUE}


def _is_configuration_error(exc: DBAPIError) -> bool:
    sqlstate = _db_error_attribute(exc, "sqlstate")
    return sqlstate is not None and sqlstate.startswith(("0A", "22", "28", "3D", "3F", "42"))


def _is_integrity_state(exc: DBAPIError) -> bool:
    sqlstate = _db_error_attribute(exc, "sqlstate")
    return sqlstate is not None and (sqlstate.startswith("23") or sqlstate == "55000")


def _is_statement_completion_unknown(exc: DBAPIError) -> bool:
    return _db_error_attribute(exc, "sqlstate") == "40003"


def _is_known_rollback(exc: DBAPIError) -> bool:
    sqlstate = _db_error_attribute(exc, "sqlstate")
    return sqlstate is not None and (
        (sqlstate.startswith("40") and sqlstate != "40003") or sqlstate == "57014"
    )


def _database_error(exc: DBAPIError) -> AppError:
    return _configuration_invalid() if _is_configuration_error(exc) else _unavailable()


def _invalid() -> AppError:
    return AppError(
        "Forecast selection policy is invalid.",
        code="forecast_selection_policy_invalid",
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        details={"retryable": False},
    )


def _unavailable() -> AppError:
    return AppError(
        "Forecast selection policy registry is unavailable.",
        code="forecast_selection_policy_store_unavailable",
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        details={"retryable": True},
    )


def _configuration_invalid() -> AppError:
    return AppError(
        "Forecast selection policy registry configuration is invalid.",
        code="forecast_selection_policy_configuration_invalid",
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        details={"retryable": False},
    )


def _integrity_error() -> AppError:
    return AppError(
        "Forecast selection policy registry integrity validation failed.",
        code="forecast_selection_policy_integrity_failed",
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        details={"retryable": False},
    )


def _semantic_conflict() -> AppError:
    return AppError(
        "A different selection policy occupies this content identity.",
        code="forecast_selection_policy_semantic_conflict",
        status_code=status.HTTP_409_CONFLICT,
        details={"retryable": False},
    )


def _write_conflict() -> AppError:
    return AppError(
        "Concurrent selection policy registration could not be reconciled.",
        code="forecast_selection_policy_write_conflict",
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        details={"retryable": True},
    )


def _commit_unknown() -> AppError:
    return AppError(
        "Forecast selection policy registration commit status is unknown.",
        code="forecast_selection_policy_commit_unknown",
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        details={"outcome_unknown": True, "retryable": True},
    )


def _corrupt() -> AppError:
    return AppError(
        "Persisted forecast selection policy evidence failed validation.",
        code="forecast_selection_policy_evidence_corrupt",
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        details={"retryable": False},
    )


__all__ = [
    "ForecastSelectionPolicyProof",
    "ForecastSelectionPolicyRecord",
    "ForecastSelectionPolicyStore",
    "SqlForecastSelectionPolicyStore",
]
