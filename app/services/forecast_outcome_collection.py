"""Default-off composition from a sealed forecast cohort to realized truth.

This module deliberately exposes only an internal, caller-driven seam.  It has
no API route, Celery task, Beat schedule, vendor adapter, or implicit cutoff.
The caller names one precommitted forecast step and the exact cutoff dictated
by the cohort's already-committed outcome policy.
"""

from __future__ import annotations

import hmac
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable
from uuid import UUID

from app.services.forecast_cohort_store import ForecastCohortProof
from app.services.forecast_cohorts import (
    ForecastCohortMember,
    ForecastCohortValidationError,
    member_from_scheduled_run,
    validate_cohort_seal,
)
from app.services.forecast_outcome_store import (
    ForecastOutcomeProof,
    ForecastOutcomePublicationRecord,
    ForecastOutcomePublicationSource,
)
from app.services.forecast_outcomes import (
    BarVersionEvidence,
    OutcomeValidationError,
    RealizedOutcomePayload,
    validate_outcome_record,
)
from app.services.forecast_run_store import ArchivedForecastRun
from app.services.forecast_runs import ForecastRunValidationError, parse_output

_SHA256_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_SCHEDULED_ORIGIN = "scheduled_evaluation"


class ForecastOutcomeCollectionError(ValueError):
    """A collection request or one of its persisted proofs is inconsistent."""


@dataclass(frozen=True)
class ForecastOutcomeCollectionSpec:
    """One exact precommitted forecast step and deterministic policy cutoff."""

    cohort_id: str
    forecast_id: UUID
    step: int
    resolution_cutoff: datetime


@dataclass(frozen=True)
class ForecastOutcomeCollectionProof:
    """Validated provenance bridge from sealed forecast to immutable outcome."""

    cohort: ForecastCohortProof
    member: ForecastCohortMember
    run: ArchivedForecastRun
    outcome: ForecastOutcomeProof


@runtime_checkable
class ForecastCohortReader(Protocol):
    """Fresh, validated read of a manifest and its distinct-transaction seal."""

    async def read_validated(self, cohort_id: str) -> ForecastCohortProof: ...


@runtime_checkable
class HistoricalScheduledRunReader(Protocol):
    """Historical archive read validated under the row's own policy epoch."""

    async def read_self_validated(
        self,
        forecast_id: UUID,
        *,
        expected_origin_kind: str,
    ) -> ArchivedForecastRun: ...


@runtime_checkable
class ExactOutcomeVersionResolver(Protocol):
    """Resolve one exact bar version under a code-owned policy."""

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


@runtime_checkable
class RealizedOutcomePublisher(Protocol):
    """Persist or replay one exact realized-outcome row."""

    @property
    def outcome_resolution_policy_hash(self) -> str: ...

    @property
    def availability_rule_set_hash(self) -> str: ...

    async def publish(
        self,
        payload: RealizedOutcomePayload,
        *,
        source: ForecastOutcomePublicationSource,
    ) -> ForecastOutcomeProof: ...


@dataclass(frozen=True)
class ForecastOutcomeCollectionService:
    """Collect one matured member without trusting caller-supplied forecast data."""

    cohort_store: ForecastCohortReader
    run_store: HistoricalScheduledRunReader
    resolver: ExactOutcomeVersionResolver
    outcome_store: RealizedOutcomePublisher

    async def collect(
        self,
        spec: ForecastOutcomeCollectionSpec,
    ) -> ForecastOutcomeCollectionProof:
        normalized = self._preflight(spec)
        cohort = await self.cohort_store.read_validated(normalized.cohort_id)
        manifest = self._validated_cohort(cohort, normalized.cohort_id)
        self._validate_policy_binding(
            manifest.outcome_resolution_policy_hash,
            manifest.availability_rule_set_hash,
        )

        member = next(
            (
                item
                for item in manifest.members
                if item.forecast_id == normalized.forecast_id and item.step == normalized.step
            ),
            None,
        )
        if member is None:
            raise ForecastOutcomeCollectionError(
                "requested forecast step is not a member of the sealed cohort"
            )

        run = await self.run_store.read_self_validated(
            normalized.forecast_id,
            expected_origin_kind=_SCHEDULED_ORIGIN,
        )
        try:
            derived_member = member_from_scheduled_run(run, step=normalized.step)
            response = parse_output(run.canonical_output)
        except (
            ForecastCohortValidationError,
            ForecastRunValidationError,
            TypeError,
            ValueError,
        ) as exc:
            raise ForecastOutcomeCollectionError(
                "persisted scheduled forecast evidence is invalid"
            ) from exc
        if derived_member != member:
            raise ForecastOutcomeCollectionError(
                "persisted forecast no longer matches its sealed cohort member"
            )
        selected = next(
            (item for item in response.forecasts if item.step == normalized.step),
            None,
        )
        if selected is None or _utc(selected.target_time) != member.target_time:
            raise ForecastOutcomeCollectionError(
                "persisted forecast does not contain the sealed target step"
            )
        if (
            response.symbol != run.symbol
            or response.target != "close"
            or response.provenance.series_basis != "raw"
            or response.currency != "USD"
        ):
            raise ForecastOutcomeCollectionError(
                "outcome v1 requires a persisted raw-close USD forecast"
            )

        source = await self.resolver.resolve(
            symbol=response.symbol,
            target_time=member.target_time,
            resolution_cutoff=normalized.resolution_cutoff,
        )
        payload = RealizedOutcomePayload(
            outcome_resolution_policy_hash=manifest.outcome_resolution_policy_hash,
            availability_rule_set_hash=manifest.availability_rule_set_hash,
            resolution_cutoff=normalized.resolution_cutoff,
            symbol=response.symbol,
            target="close",
            series_basis="raw",
            target_time=member.target_time,
            currency="USD",
            realized_value=source.value,
            source_version=source,
        )
        publication_source = ForecastOutcomePublicationSource(
            cohort_id=normalized.cohort_id,
            forecast_id=normalized.forecast_id,
            step=normalized.step,
        )
        publication = await self.outcome_store.publish(
            payload,
            source=publication_source,
        )
        self._validate_publication(publication, payload, publication_source)
        return ForecastOutcomeCollectionProof(
            cohort=cohort,
            member=member,
            run=run,
            outcome=publication,
        )

    def _preflight(
        self,
        spec: ForecastOutcomeCollectionSpec,
    ) -> ForecastOutcomeCollectionSpec:
        if not isinstance(spec, ForecastOutcomeCollectionSpec):
            raise TypeError("spec must be a ForecastOutcomeCollectionSpec")
        if (
            not isinstance(spec.cohort_id, str)
            or _SHA256_PATTERN.fullmatch(spec.cohort_id) is None
            or not isinstance(spec.forecast_id, UUID)
            or type(spec.step) is not int
            or not 1 <= spec.step <= 252
        ):
            raise ForecastOutcomeCollectionError("outcome collection identity is invalid")
        cutoff = _utc(spec.resolution_cutoff)
        dependencies = (
            (self.cohort_store, ForecastCohortReader),
            (self.run_store, HistoricalScheduledRunReader),
            (self.resolver, ExactOutcomeVersionResolver),
            (self.outcome_store, RealizedOutcomePublisher),
        )
        if any(not isinstance(value, protocol) for value, protocol in dependencies):
            raise ForecastOutcomeCollectionError(
                "outcome collection dependencies are not safely configured"
            )
        return ForecastOutcomeCollectionSpec(
            cohort_id=spec.cohort_id,
            forecast_id=spec.forecast_id,
            step=spec.step,
            resolution_cutoff=cutoff,
        )

    @staticmethod
    def _validated_cohort(
        proof: ForecastCohortProof,
        expected_cohort_id: str,
    ):
        if not isinstance(proof, ForecastCohortProof):
            raise ForecastOutcomeCollectionError("cohort store returned an invalid proof shape")
        try:
            manifest = validate_cohort_seal(proof.record, proof.seal)
        except (ForecastCohortValidationError, TypeError, ValueError) as exc:
            raise ForecastOutcomeCollectionError(
                "persisted forecast cohort evidence is invalid"
            ) from exc
        if proof.record.cohort_id != expected_cohort_id or proof.manifest != manifest:
            raise ForecastOutcomeCollectionError(
                "persisted forecast cohort proof has inconsistent content"
            )
        return manifest

    def _validate_policy_binding(self, policy_hash: str, rule_set_hash: str) -> None:
        identities = (
            self.resolver.outcome_resolution_policy_hash,
            self.outcome_store.outcome_resolution_policy_hash,
        )
        rules = (
            self.resolver.availability_rule_set_hash,
            self.outcome_store.availability_rule_set_hash,
        )
        if any(not _matches(value, policy_hash) for value in identities) or any(
            not _matches(value, rule_set_hash) for value in rules
        ):
            raise ForecastOutcomeCollectionError(
                "collector policy does not match the precommitted cohort policy"
            )

    def _validate_publication(
        self,
        proof: ForecastOutcomeProof,
        expected: RealizedOutcomePayload,
        expected_source: ForecastOutcomePublicationSource,
    ) -> None:
        if not isinstance(proof, ForecastOutcomeProof):
            raise ForecastOutcomeCollectionError("outcome store returned an invalid proof shape")
        try:
            payload = validate_outcome_record(
                proof.record,
                expected_outcome_resolution_policy_hash=(
                    self.resolver.outcome_resolution_policy_hash
                ),
                expected_availability_rule_set_hash=(self.resolver.availability_rule_set_hash),
            )
        except (OutcomeValidationError, TypeError, ValueError) as exc:
            raise ForecastOutcomeCollectionError(
                "persisted realized-outcome evidence is invalid"
            ) from exc
        publication = proof.publication
        if (
            proof.payload != payload
            or payload != expected
            or not isinstance(publication, ForecastOutcomePublicationRecord)
            or publication.outcome_id != proof.record.outcome_id
            or publication.cohort_id != expected_source.cohort_id
            or publication.forecast_id != expected_source.forecast_id
            or publication.step != expected_source.step
            or type(publication.publisher_xid) is not int
            or publication.publisher_xid <= 0
            or publication.published_at.tzinfo is None
            or publication.published_at.utcoffset() is None
            or publication.published_at < proof.record.sealed_at
        ):
            raise ForecastOutcomeCollectionError(
                "persisted realized outcome or publication differs from the resolved evidence"
            )


def _utc(value: object) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ForecastOutcomeCollectionError("resolution_cutoff must be timezone-aware")
    return value.astimezone(UTC)


def _matches(value: object, expected: str) -> bool:
    return (
        isinstance(value, str)
        and _SHA256_PATTERN.fullmatch(value) is not None
        and hmac.compare_digest(value, expected)
    )


__all__ = [
    "ExactOutcomeVersionResolver",
    "ForecastCohortReader",
    "ForecastOutcomeCollectionError",
    "ForecastOutcomeCollectionProof",
    "ForecastOutcomeCollectionService",
    "ForecastOutcomeCollectionSpec",
    "HistoricalScheduledRunReader",
    "RealizedOutcomePublisher",
]
