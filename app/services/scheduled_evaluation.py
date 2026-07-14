"""Policy-explicit scheduled forecast publication before outcomes exist.

This module is deliberately an internal composition seam. It has no task,
Beat, API, or default scientific policy: callers must supply one pinned
forecast request, every policy identity, and the exact steps selected for a
future calibration or held-out cohort.
"""

from __future__ import annotations

import hmac
import re
from dataclasses import dataclass
from typing import Protocol, runtime_checkable
from uuid import UUID

from app.schemas.forecast import ForecastRequest, ForecastResponse
from app.services.forecast_cohorts import (
    CohortPurpose,
    ForecastCohortManifest,
    ForecastCohortRecord,
    ForecastCohortSeal,
    ForecastCohortValidationError,
    canonical_cohort_manifest,
    member_from_scheduled_run,
    parse_cohort_manifest,
    validate_cohort_seal,
)
from app.services.forecast_run_store import ArchivedForecastRun
from app.services.forecast_runs import canonical_request, parse_request

_HASH_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_MODEL_VERSION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._@-]{0,127}$")
_BUILD_REVISION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_SCHEDULED_ORIGIN = "scheduled_evaluation"
_PURPOSES = frozenset({"calibration_fit", "heldout_evaluation"})


class ScheduledEvaluationValidationError(ValueError):
    """A scheduled-evaluation specification or persisted proof is invalid."""


@dataclass(frozen=True)
class ScheduledEvaluationSpec:
    """All policy and selection inputs required before scheduled evaluation."""

    request: ForecastRequest
    purpose: CohortPurpose
    selected_steps: tuple[int, ...]
    model_version: str
    code_version: str
    forecast_resolution_policy_hash: str
    forecast_availability_rule_set_hash: str
    selection_policy_hash: str
    outcome_resolution_policy_hash: str
    outcome_availability_rule_set_hash: str


@dataclass(frozen=True)
class ScheduledEvaluationProof:
    """Persisted run plus the two-phase, pre-outcome cohort evidence."""

    run: ArchivedForecastRun
    cohort_record: ForecastCohortRecord
    cohort_seal: ForecastCohortSeal


@runtime_checkable
class ScheduledRunReader(Protocol):
    """Validated read seam supplied by the scheduled forecast archive."""

    @property
    def resolution_policy_hash(self) -> str: ...

    @property
    def availability_rule_set_hash(self) -> str: ...

    @property
    def origin_kind(self) -> str: ...

    async def read_validated(
        self,
        forecast_id: UUID,
        *,
        expected_request: ForecastRequest,
        expected_origin_kind: str,
    ) -> ArchivedForecastRun: ...


@runtime_checkable
class ScheduledForecastPolicy(Protocol):
    """Snapshot policy identity that must match the archive policy epoch."""

    @property
    def resolution_policy_hash(self) -> str: ...

    @property
    def trusted_availability_rule_set_hash(self) -> str: ...


@runtime_checkable
class ScheduledForecastProducer(Protocol):
    """Pinned-snapshot producer composition accepted by the scheduler seam."""

    @property
    def policy(self) -> ScheduledForecastPolicy: ...

    @property
    def run_store(self) -> object | None: ...

    @property
    def code_version(self) -> str | None: ...

    def model_version_for(self, model: str) -> str: ...

    async def forecast(
        self,
        request: ForecastRequest,
        *,
        idempotency_key: str | None = None,
        principal: str | None = None,
    ) -> ForecastResponse: ...


@runtime_checkable
class CohortPublication(Protocol):
    """Structural result of two-transaction cohort publication."""

    record: ForecastCohortRecord
    seal: ForecastCohortSeal


@runtime_checkable
class CohortPublisher(Protocol):
    """Persist or replay one exact cohort manifest and availability seal."""

    async def publish(self, manifest: ForecastCohortManifest) -> CohortPublication: ...


@dataclass(frozen=True)
class ScheduledEvaluationService:
    """Archive one pinned scheduled run, then publish its selected cohort."""

    forecast_service: ScheduledForecastProducer
    run_store: ScheduledRunReader
    cohort_store: CohortPublisher

    async def publish(self, spec: ScheduledEvaluationSpec) -> ScheduledEvaluationProof:
        request_payload, normalized = self._preflight(spec)
        producer_request = parse_request(request_payload)
        response = await self.forecast_service.forecast(
            producer_request,
            idempotency_key=None,
            principal=None,
        )
        if not isinstance(response, ForecastResponse):
            raise ScheduledEvaluationValidationError(
                "forecast service must return a ForecastResponse"
            )

        # The response is only a committed-row locator. Membership and every
        # proof field below come from a fresh, validated archive read.
        expected_request = parse_request(request_payload)
        run = await self.run_store.read_validated(
            response.provenance.forecast_id,
            expected_request=expected_request,
            expected_origin_kind=_SCHEDULED_ORIGIN,
        )
        self._validate_run(run, normalized)
        members = tuple(
            member_from_scheduled_run(run, step=step) for step in normalized.selected_steps
        )
        manifest = ForecastCohortManifest(
            purpose=normalized.purpose,
            selection_policy_hash=normalized.selection_policy_hash,
            outcome_resolution_policy_hash=normalized.outcome_resolution_policy_hash,
            availability_rule_set_hash=normalized.outcome_availability_rule_set_hash,
            members=members,
        )
        expected_manifest = parse_cohort_manifest(canonical_cohort_manifest(manifest))
        publication = await self.cohort_store.publish(expected_manifest)
        if not isinstance(publication, CohortPublication):
            raise ScheduledEvaluationValidationError(
                "cohort store returned an invalid publication proof"
            )
        try:
            persisted_manifest = validate_cohort_seal(publication.record, publication.seal)
        except (ForecastCohortValidationError, TypeError, ValueError) as exc:
            raise ScheduledEvaluationValidationError(
                "persisted cohort publication failed validation"
            ) from exc
        if persisted_manifest != expected_manifest:
            raise ScheduledEvaluationValidationError(
                "persisted cohort manifest differs from the selected scheduled run"
            )
        return ScheduledEvaluationProof(
            run=run,
            cohort_record=publication.record,
            cohort_seal=publication.seal,
        )

    def _preflight(
        self,
        spec: ScheduledEvaluationSpec,
    ) -> tuple[bytes, ScheduledEvaluationSpec]:
        if not isinstance(spec, ScheduledEvaluationSpec):
            raise TypeError("spec must be a ScheduledEvaluationSpec")
        if not isinstance(spec.request, ForecastRequest):
            raise ScheduledEvaluationValidationError("request must be a ForecastRequest")
        try:
            request = ForecastRequest.model_validate(
                spec.request.model_dump(mode="python", round_trip=True)
            )
        except (TypeError, ValueError) as exc:
            raise ScheduledEvaluationValidationError("forecast request is invalid") from exc
        if request.snapshot_id is None:
            raise ScheduledEvaluationValidationError(
                "scheduled evaluation requires a pinned snapshot_id"
            )
        if request.model == "auto":
            raise ScheduledEvaluationValidationError(
                "scheduled evaluation requires an explicit model selector"
            )
        explicit_scientific_fields = {
            "horizon",
            "horizon_unit",
            "target",
            "snapshot_id",
            "model",
            "interval_coverages",
        }
        if not explicit_scientific_fields.issubset(spec.request.model_fields_set):
            raise ScheduledEvaluationValidationError(
                "scheduled evaluation requires every scientific request field explicitly"
            )
        if request.as_of is not None:
            raise ScheduledEvaluationValidationError(
                "a pinned scheduled snapshot must not include an ignored as_of cutoff"
            )
        if not isinstance(spec.selected_steps, tuple) or not spec.selected_steps:
            raise ScheduledEvaluationValidationError(
                "selected_steps must be a nonempty canonical tuple"
            )
        if any(
            type(step) is not int or not 1 <= step <= request.horizon
            for step in spec.selected_steps
        ):
            raise ScheduledEvaluationValidationError(
                "selected_steps must be integers within the forecast horizon"
            )
        if spec.selected_steps != tuple(sorted(set(spec.selected_steps))):
            raise ScheduledEvaluationValidationError(
                "selected_steps must be sorted and contain no duplicates"
            )
        if not isinstance(spec.purpose, str) or spec.purpose not in _PURPOSES:
            raise ScheduledEvaluationValidationError("cohort purpose is not supported")
        hashes = (
            spec.forecast_resolution_policy_hash,
            spec.forecast_availability_rule_set_hash,
            spec.selection_policy_hash,
            spec.outcome_resolution_policy_hash,
            spec.outcome_availability_rule_set_hash,
        )
        if any(
            not isinstance(value, str) or _HASH_PATTERN.fullmatch(value) is None for value in hashes
        ):
            raise ScheduledEvaluationValidationError(
                "scheduled evaluation policies must be canonical sha256 hashes"
            )
        if (
            not isinstance(spec.model_version, str)
            or _MODEL_VERSION_PATTERN.fullmatch(spec.model_version) is None
            or not isinstance(spec.code_version, str)
            or _BUILD_REVISION_PATTERN.fullmatch(spec.code_version) is None
        ):
            raise ScheduledEvaluationValidationError(
                "scheduled evaluation requires canonical model and build versions"
            )
        if not isinstance(self.run_store, ScheduledRunReader):
            raise ScheduledEvaluationValidationError(
                "scheduled forecast archive does not expose validated reads"
            )
        if not isinstance(self.cohort_store, CohortPublisher):
            raise ScheduledEvaluationValidationError("scheduled cohort publisher is not configured")
        if not isinstance(self.forecast_service, ScheduledForecastProducer):
            raise ScheduledEvaluationValidationError(
                "forecast producer does not expose its snapshot policy composition"
            )
        if self.forecast_service.run_store is not self.run_store:
            raise ScheduledEvaluationValidationError(
                "forecast producer is not bound to this scheduled archive"
            )
        policy = self.forecast_service.policy
        if not isinstance(policy, ScheduledForecastPolicy):
            raise ScheduledEvaluationValidationError(
                "forecast producer does not expose a valid snapshot policy"
            )
        resolved_model_version = self.forecast_service.model_version_for(request.model)
        if (
            not isinstance(resolved_model_version, str)
            or not hmac.compare_digest(resolved_model_version, spec.model_version)
            or not isinstance(self.forecast_service.code_version, str)
            or not hmac.compare_digest(
                self.forecast_service.code_version,
                spec.code_version,
            )
        ):
            raise ScheduledEvaluationValidationError(
                "forecast producer model or build version does not match the specification"
            )
        if not _matches_hash(
            policy.resolution_policy_hash,
            spec.forecast_resolution_policy_hash,
        ) or not _matches_hash(
            policy.trusted_availability_rule_set_hash,
            spec.forecast_availability_rule_set_hash,
        ):
            raise ScheduledEvaluationValidationError(
                "snapshot forecast policy does not match the scheduled specification"
            )
        if self.run_store.origin_kind != _SCHEDULED_ORIGIN:
            raise ScheduledEvaluationValidationError(
                "forecast archive is not configured for scheduled evaluation"
            )
        if not _matches_hash(
            self.run_store.resolution_policy_hash,
            spec.forecast_resolution_policy_hash,
        ) or not _matches_hash(
            self.run_store.availability_rule_set_hash,
            spec.forecast_availability_rule_set_hash,
        ):
            raise ScheduledEvaluationValidationError(
                "forecast archive policy does not match the scheduled specification"
            )
        normalized = ScheduledEvaluationSpec(
            request=request,
            purpose=spec.purpose,
            selected_steps=spec.selected_steps,
            model_version=spec.model_version,
            code_version=spec.code_version,
            forecast_resolution_policy_hash=spec.forecast_resolution_policy_hash,
            forecast_availability_rule_set_hash=spec.forecast_availability_rule_set_hash,
            selection_policy_hash=spec.selection_policy_hash,
            outcome_resolution_policy_hash=spec.outcome_resolution_policy_hash,
            outcome_availability_rule_set_hash=spec.outcome_availability_rule_set_hash,
        )
        return canonical_request(request), normalized

    @staticmethod
    def _validate_run(run: ArchivedForecastRun, spec: ScheduledEvaluationSpec) -> None:
        if not isinstance(run, ArchivedForecastRun):
            raise ScheduledEvaluationValidationError(
                "forecast archive returned an invalid detached row"
            )
        if run.recorded_at is None:
            raise ScheduledEvaluationValidationError(
                "scheduled forecast lacks database recording evidence"
            )
        if run.origin_kind != _SCHEDULED_ORIGIN:
            raise ScheduledEvaluationValidationError(
                "forecast archive returned a non-scheduled run"
            )
        if run.model_version != spec.model_version or run.code_version != spec.code_version:
            raise ScheduledEvaluationValidationError(
                "persisted scheduled run has the wrong model or build version"
            )
        if not _matches_hash(
            run.resolution_policy_hash,
            spec.forecast_resolution_policy_hash,
        ) or not _matches_hash(
            run.availability_rule_set_hash,
            spec.forecast_availability_rule_set_hash,
        ):
            raise ScheduledEvaluationValidationError(
                "persisted scheduled run has the wrong forecast policy"
            )


def _matches_hash(value: object, expected: str) -> bool:
    return (
        isinstance(value, str)
        and _HASH_PATTERN.fullmatch(value) is not None
        and hmac.compare_digest(value, expected)
    )


__all__ = [
    "CohortPublication",
    "CohortPublisher",
    "ScheduledEvaluationProof",
    "ScheduledEvaluationService",
    "ScheduledEvaluationSpec",
    "ScheduledEvaluationValidationError",
    "ScheduledForecastPolicy",
    "ScheduledForecastProducer",
    "ScheduledRunReader",
]
