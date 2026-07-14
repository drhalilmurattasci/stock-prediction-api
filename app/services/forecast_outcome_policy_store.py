"""Explicit, append-only registration of realized-outcome resolution policies."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable

from fastapi import status
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.exceptions import AppError
from app.db.models.forecast_evidence import ForecastOutcomeResolutionPolicyRegistration
from app.services.forecast_outcome_resolution import ForecastOutcomeResolutionPolicy

_POLICY_PRIMARY_KEY = "pk_forecast_outcome_resolution_policies"
_POLICY_RULES_UNIQUE = "uq_forecast_outcome_resolution_policies_policy_rules"
_HASH_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_POLICY_FORMAT = "forecast-outcome-resolution-policy-v1"
_SCHEMA_VERSION = 1
_MAX_POLICY_BYTES = 262_144
_MAX_RESOLUTION_LAG_SECONDS = 366 * 24 * 60 * 60


@dataclass(frozen=True)
class ForecastOutcomePolicyRecord:
    """Detached database evidence for one immutable policy registration."""

    policy_hash: str
    availability_rule_set_hash: str
    schema_version: int
    resolution_lag_seconds: int
    canonical_policy: bytes
    recorded_at: datetime
    creator_xid: int


@dataclass(frozen=True)
class ForecastOutcomePolicyProof:
    """An exact registration replay bound to the policy supplied by the caller."""

    policy: ForecastOutcomeResolutionPolicy
    record: ForecastOutcomePolicyRecord


@runtime_checkable
class ForecastOutcomePolicyStore(Protocol):
    """Register one explicit policy; no implicit or default policy is provided."""

    async def register(
        self,
        policy: ForecastOutcomeResolutionPolicy,
    ) -> ForecastOutcomePolicyProof: ...


@dataclass(frozen=True)
class _PreparedPolicy:
    policy: ForecastOutcomeResolutionPolicy
    policy_hash: str
    availability_rule_set_hash: str
    schema_version: int
    resolution_lag_seconds: int
    canonical_policy: bytes


@dataclass(frozen=True)
class SqlForecastOutcomePolicyStore:
    """Short-transaction policy registry with exact race reconciliation."""

    sessionmaker: async_sessionmaker[AsyncSession]

    async def register(
        self,
        policy: ForecastOutcomeResolutionPolicy,
    ) -> ForecastOutcomePolicyProof:
        prepared = _prepare(policy)
        existing = await self._preflight(prepared)
        if existing is not None:
            return existing
        return await self._commit(prepared)

    async def _preflight(
        self,
        prepared: _PreparedPolicy,
    ) -> ForecastOutcomePolicyProof | None:
        try:
            async with self.sessionmaker() as session:
                row = await session.get(
                    ForecastOutcomeResolutionPolicyRegistration,
                    prepared.policy_hash,
                )
        except SQLAlchemyTimeoutError as exc:
            raise _unavailable() from exc
        except DBAPIError as exc:
            raise _database_error(exc) from exc
        return None if row is None else _exact(row, prepared)

    async def _commit(self, prepared: _PreparedPolicy) -> ForecastOutcomePolicyProof:
        commit_pending = False
        try:
            async with self.sessionmaker() as session, session.begin():
                existing = await session.get(
                    ForecastOutcomeResolutionPolicyRegistration,
                    prepared.policy_hash,
                )
                if existing is not None:
                    return _exact(existing, prepared)
                # Runtime has no raw INSERT privilege.  The security-definer
                # boundary accepts only canonical bytes; its trigger derives
                # and validates every header before returning the content ID.
                result = await session.execute(
                    text(
                        "SELECT public.register_forecast_outcome_resolution_policy("
                        ":canonical_policy)"
                    ),
                    {"canonical_policy": prepared.canonical_policy},
                )
                if result.scalar_one() != prepared.policy_hash:
                    raise _corrupt()
                candidate = await session.get(
                    ForecastOutcomeResolutionPolicyRegistration,
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

    async def _read_committed(self, prepared: _PreparedPolicy) -> ForecastOutcomePolicyProof:
        try:
            async with self.sessionmaker() as session:
                row = await session.get(
                    ForecastOutcomeResolutionPolicyRegistration,
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
    ) -> ForecastOutcomePolicyProof:
        proof = await self._try_reconcile(prepared)
        if proof is not None:
            return proof
        raise _commit_unknown() from cause

    async def _try_reconcile(
        self,
        prepared: _PreparedPolicy,
    ) -> ForecastOutcomePolicyProof | None:
        try:
            async with self.sessionmaker() as session:
                row = await session.get(
                    ForecastOutcomeResolutionPolicyRegistration,
                    prepared.policy_hash,
                )
        except (DBAPIError, SQLAlchemyTimeoutError):
            return None
        return None if row is None else _exact(row, prepared)


def _prepare(policy: ForecastOutcomeResolutionPolicy) -> _PreparedPolicy:
    try:
        document = policy.outcome_resolution_policy_document
        canonical = _canonical_json(document)
        parsed = _parse_canonical_policy(canonical)
        policy_hash = _content_id(canonical)
        declared_policy_hash = policy.outcome_resolution_policy_hash
        declared_rule_set_hash = policy.availability_rule_set_hash
        declared_lag = policy.resolution_lag_seconds
        if (
            not hmac.compare_digest(policy_hash, declared_policy_hash)
            or not hmac.compare_digest(parsed[1], declared_rule_set_hash)
            or parsed[2] != declared_lag
        ):
            raise ValueError("policy properties disagree with canonical evidence")
    except (AttributeError, TypeError, ValueError) as exc:
        raise _invalid() from exc
    return _PreparedPolicy(
        policy=policy,
        policy_hash=policy_hash,
        availability_rule_set_hash=parsed[1],
        schema_version=parsed[0],
        resolution_lag_seconds=parsed[2],
        canonical_policy=canonical,
    )


def _canonical_json(document: object) -> bytes:
    canonical = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    if not canonical or len(canonical) > _MAX_POLICY_BYTES:
        raise ValueError("canonical policy size is outside the supported bound")
    return canonical


def _parse_canonical_policy(canonical: bytes) -> tuple[int, str, int]:
    if not isinstance(canonical, bytes) or not canonical or len(canonical) > _MAX_POLICY_BYTES:
        raise ValueError("canonical policy bytes are invalid")
    document = json.loads(canonical.decode("utf-8"))
    if not isinstance(document, dict) or _canonical_json(document) != canonical:
        raise ValueError("policy JSON is not canonical")
    if document.get("format") != _POLICY_FORMAT:
        raise ValueError("policy format is unsupported")
    if document.get("schema_version") != _SCHEMA_VERSION:
        raise ValueError("policy schema version is unsupported")
    rule_set_hash = document.get("availability_rule_set_hash")
    if not isinstance(rule_set_hash, str) or _HASH_PATTERN.fullmatch(rule_set_hash) is None:
        raise ValueError("policy availability rule-set hash is invalid")
    cutoff = document.get("cutoff")
    if not isinstance(cutoff, dict):
        raise ValueError("policy cutoff is invalid")
    lag = cutoff.get("resolution_lag_seconds")
    if type(lag) is not int or not 1 <= lag <= _MAX_RESOLUTION_LAG_SECONDS:
        raise ValueError("policy resolution lag is invalid")
    return _SCHEMA_VERSION, rule_set_hash, lag


def _content_id(canonical: bytes) -> str:
    return "sha256:" + hashlib.sha256(canonical).hexdigest()


def _exact(
    row: ForecastOutcomeResolutionPolicyRegistration,
    prepared: _PreparedPolicy,
) -> ForecastOutcomePolicyProof:
    try:
        record = _detach(row)
        schema_version, rule_set_hash, lag = _parse_canonical_policy(record.canonical_policy)
        if (
            record.policy_hash != _content_id(record.canonical_policy)
            or record.schema_version != schema_version
            or record.availability_rule_set_hash != rule_set_hash
            or record.resolution_lag_seconds != lag
            or record.creator_xid <= 0
            or record.recorded_at.tzinfo is None
            or record.recorded_at.utcoffset() is None
        ):
            raise ValueError("stored policy headers disagree with canonical evidence")
    except (AttributeError, TypeError, ValueError) as exc:
        raise _corrupt() from exc
    if (
        record.policy_hash == prepared.policy_hash
        and record.availability_rule_set_hash == prepared.availability_rule_set_hash
        and record.schema_version == prepared.schema_version
        and record.resolution_lag_seconds == prepared.resolution_lag_seconds
        and hmac.compare_digest(record.canonical_policy, prepared.canonical_policy)
    ):
        return ForecastOutcomePolicyProof(policy=prepared.policy, record=record)
    # A distinct valid document with the same SHA-256 identity is not an exact
    # replay and must never be silently accepted as the requested policy.
    if record.policy_hash == prepared.policy_hash:
        raise _semantic_conflict()
    raise _corrupt()


def _detach(row: ForecastOutcomeResolutionPolicyRegistration) -> ForecastOutcomePolicyRecord:
    return ForecastOutcomePolicyRecord(
        policy_hash=row.policy_hash,
        availability_rule_set_hash=row.availability_rule_set_hash,
        schema_version=row.schema_version,
        resolution_lag_seconds=row.resolution_lag_seconds,
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
    ) in {_POLICY_PRIMARY_KEY, _POLICY_RULES_UNIQUE}


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
        "Forecast outcome resolution policy is invalid.",
        code="forecast_outcome_policy_invalid",
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        details={"retryable": False},
    )


def _unavailable() -> AppError:
    return AppError(
        "Forecast outcome policy registry is unavailable.",
        code="forecast_outcome_policy_store_unavailable",
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        details={"retryable": True},
    )


def _configuration_invalid() -> AppError:
    return AppError(
        "Forecast outcome policy registry configuration is invalid.",
        code="forecast_outcome_policy_configuration_invalid",
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        details={"retryable": False},
    )


def _integrity_error() -> AppError:
    return AppError(
        "Forecast outcome policy registry integrity validation failed.",
        code="forecast_outcome_policy_integrity_failed",
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        details={"retryable": False},
    )


def _semantic_conflict() -> AppError:
    return AppError(
        "A different outcome policy occupies this content identity.",
        code="forecast_outcome_policy_semantic_conflict",
        status_code=status.HTTP_409_CONFLICT,
        details={"retryable": False},
    )


def _write_conflict() -> AppError:
    return AppError(
        "Concurrent outcome policy registration could not be reconciled.",
        code="forecast_outcome_policy_write_conflict",
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        details={"retryable": True},
    )


def _commit_unknown() -> AppError:
    return AppError(
        "Forecast outcome policy registration commit status is unknown.",
        code="forecast_outcome_policy_commit_unknown",
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        details={"outcome_unknown": True, "retryable": True},
    )


def _corrupt() -> AppError:
    return AppError(
        "Persisted forecast outcome policy evidence failed validation.",
        code="forecast_outcome_policy_evidence_corrupt",
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        details={"retryable": False},
    )


__all__ = [
    "ForecastOutcomePolicyProof",
    "ForecastOutcomePolicyRecord",
    "ForecastOutcomePolicyStore",
    "SqlForecastOutcomePolicyStore",
]
