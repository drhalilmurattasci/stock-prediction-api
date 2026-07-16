"""Canonical policy evidence for prospective fit and held-out selection.

This module is deliberately pure and inert.  It defines one content-addressed
scientific policy covering both forward temporal windows and can derive purpose
assignments from proposals derived from already-validated archived forecast
opportunities.  It creates no task, Beat entry, API route, database row, or
vendor request.

The combined policy identity is load-bearing: fit and held-out evidence use the
same policy hash, while purpose is derived only from the target's UTC date.
Callers cannot choose a purpose independently of the ratified windows.

Candidate and assignment values are planning values, not persisted evidence.
The future operator must derive candidates from ``read_validated`` archived
runs, bind its own code version in the execution proof, enforce the ratified
seal lead against the authoritative clock, and rely on the database uniqueness
fence for cross-transaction exclusion.  This pure module rejects incomplete or
cross-purpose opportunity bundles within one composition call.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Literal, Protocol, runtime_checkable

from app.services.forecast_cohorts import CohortPurpose

SELECTION_POLICY_SCHEMA_VERSION = 1
SELECTION_POLICY_FORMAT = "forecast-prospective-selection-policy-v1"
WINDOW_DATE_POLICY_VERSION = "utc-target-date-v1"
MEMBERSHIP_COUNTING_UNIT = "forecast_opportunity_step"
MEMBERSHIP_AGGREGATION_RULE = (
    "distinct_opportunity_steps_across_sealed_cohorts_by_policy_purpose_window"
)
MINIMUM_SEAL_LEAD_SECONDS = 4 * 60 * 60
MAX_CANONICAL_SELECTION_POLICY_BYTES = 256 * 1024

_MAX_HORIZON = 252
_MAX_SYMBOLS = 10_000
_MAX_MEMBER_COUNT = 1_000_000
_MAX_POLICY_SECONDS = 366 * 24 * 60 * 60
_SHA256_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9.\-_:]+$")
_MODEL_VERSION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._@-]{0,127}$")

_MODEL_VERSION_PREFIXES = {
    "baseline_naive": "baseline-naive@",
    "baseline_drift": "baseline-drift@",
}
_TARGET = "close"
_SERIES_BASIS = "raw"
_HORIZON_UNIT = "trading_day"
_CURRENCY = "USD"
_CADENCE = "xnys_session_daily"
_SNAPSHOT_BINDING = "explicit_snapshot_id"
_SELECTION_RULE = "complete_selected_step_bundle_within_one_utc_target_window"

type SelectionTarget = Literal["close"]
type SelectionSeriesBasis = Literal["raw"]
type SelectionHorizonUnit = Literal["trading_day"]
type SelectionCurrency = Literal["USD"]
type SelectionCadence = Literal["xnys_session_daily"]
type SnapshotBinding = Literal["explicit_snapshot_id"]
type SelectionRule = Literal["complete_selected_step_bundle_within_one_utc_target_window"]


class ForecastSelectionPolicyValidationError(ValueError):
    """A prospective selection policy or assignment is malformed or unsafe."""


@runtime_checkable
class SelectionOutcomePolicyEpoch(Protocol):
    """Exact historical outcome-policy identity used at composition time."""

    resolution_lag_seconds: int

    @property
    def outcome_resolution_policy_hash(self) -> str: ...

    @property
    def availability_rule_set_hash(self) -> str: ...


@dataclass(frozen=True, slots=True)
class ForecastSelectionWindow:
    """Inclusive UTC target-date bounds for one prospective purpose."""

    start: date
    end: date

    def __post_init__(self) -> None:
        _calendar_date(self.start, "window.start")
        _calendar_date(self.end, "window.end")
        if self.end < self.start:
            raise ForecastSelectionPolicyValidationError(
                "selection window end must be on or after its start"
            )


@dataclass(frozen=True, slots=True)
class ProspectiveForecastSelectionPolicy:
    """Every ratified choice that can change prospective cohort membership."""

    symbols: tuple[str, ...]
    target: SelectionTarget
    series_basis: SelectionSeriesBasis
    horizon_unit: SelectionHorizonUnit
    currency: SelectionCurrency
    model_selector: str
    model_version: str
    horizon: int
    selected_steps: tuple[int, ...]
    interval_coverages_millis: tuple[int, ...]
    fit_window: ForecastSelectionWindow
    heldout_window: ForecastSelectionWindow
    minimum_fit_member_count: int
    minimum_heldout_member_count: int
    minimum_seal_lead_seconds: int
    cadence: SelectionCadence
    snapshot_binding: SnapshotBinding
    selection_rule: SelectionRule
    resolution_lag_seconds: int
    forecast_resolution_policy_hash: str
    forecast_availability_rule_set_hash: str
    outcome_resolution_policy_hash: str
    outcome_availability_rule_set_hash: str
    schema_version: int = SELECTION_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _validate_policy(self)

    @property
    def canonical_policy(self) -> bytes:
        """Return strict canonical bytes suitable for later registration."""

        return canonical_selection_policy(self)

    @property
    def selection_policy_hash(self) -> str:
        """Return the content identity of :attr:`canonical_policy`."""

        return selection_policy_hash_for(self)

    @property
    def selection_policy_document(self) -> dict[str, object]:
        """Return a detached JSON-compatible copy of the canonical document."""

        document = json.loads(self.canonical_policy.decode("utf-8"))
        if not isinstance(document, dict):  # pragma: no cover - construction invariant
            raise ForecastSelectionPolicyValidationError(
                "selection policy document is not an object"
            )
        return document


@dataclass(frozen=True, slots=True)
class ForecastSelectionCandidate:
    """One untrusted archived-opportunity step proposed for assignment.

    The future operator must construct these only from an archived run returned
    by its validated read seam.  Possessing this value is not evidence of that
    provenance.
    """

    opportunity_hash: str
    step: int
    target_time: datetime

    def __post_init__(self) -> None:
        _sha256(self.opportunity_hash, "candidate.opportunity_hash")
        _bounded_step(self.step, "candidate.step")
        _utc(self.target_time, "candidate.target_time")


@dataclass(frozen=True, slots=True)
class ForecastPurposeAssignment:
    """A planning result, not proof that a cohort member was persisted."""

    candidate: ForecastSelectionCandidate
    purpose: CohortPurpose
    selection_policy_hash: str

    def __post_init__(self) -> None:
        _normalized_candidate(self.candidate)
        if self.purpose not in ("calibration_fit", "heldout_evaluation"):
            raise ForecastSelectionPolicyValidationError(
                "assignment purpose is not a prospective cohort purpose"
            )
        _sha256(self.selection_policy_hash, "assignment.selection_policy_hash")


def canonical_selection_policy(policy: ProspectiveForecastSelectionPolicy) -> bytes:
    """Return deterministic UTF-8 JSON for one validated combined policy."""

    normalized = _validated_policy(policy)
    document = {
        "format": SELECTION_POLICY_FORMAT,
        "minimum_seal_lead_seconds": normalized.minimum_seal_lead_seconds,
        "policy_epoch": {
            "forecast_availability_rule_set_hash": (normalized.forecast_availability_rule_set_hash),
            "forecast_resolution_policy_hash": normalized.forecast_resolution_policy_hash,
            "outcome_availability_rule_set_hash": (normalized.outcome_availability_rule_set_hash),
            "outcome_resolution_policy_hash": normalized.outcome_resolution_policy_hash,
            "resolution_lag_seconds": normalized.resolution_lag_seconds,
        },
        "schema_version": normalized.schema_version,
        "study": {
            "cadence": normalized.cadence,
            "currency": normalized.currency,
            "horizon": normalized.horizon,
            "horizon_unit": normalized.horizon_unit,
            "interval_coverages_millis": list(normalized.interval_coverages_millis),
            "model_selector": normalized.model_selector,
            "model_version": normalized.model_version,
            "selected_steps": list(normalized.selected_steps),
            "selection_rule": normalized.selection_rule,
            "series_basis": normalized.series_basis,
            "snapshot_binding": normalized.snapshot_binding,
            "symbols": list(normalized.symbols),
            "target": normalized.target,
        },
        "windows": {
            "membership_aggregation_rule": MEMBERSHIP_AGGREGATION_RULE,
            "membership_counting_unit": MEMBERSHIP_COUNTING_UNIT,
            "fit": {
                "end": normalized.fit_window.end.isoformat(),
                "minimum_member_count": normalized.minimum_fit_member_count,
                "start": normalized.fit_window.start.isoformat(),
            },
            "heldout": {
                "end": normalized.heldout_window.end.isoformat(),
                "minimum_member_count": normalized.minimum_heldout_member_count,
                "start": normalized.heldout_window.start.isoformat(),
            },
            "window_date_policy_version": WINDOW_DATE_POLICY_VERSION,
        },
    }
    try:
        canonical = json.dumps(
            document,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (UnicodeError, ValueError, TypeError, RecursionError) as exc:
        raise ForecastSelectionPolicyValidationError(
            "selection policy cannot be encoded canonically"
        ) from exc
    if not canonical or len(canonical) > MAX_CANONICAL_SELECTION_POLICY_BYTES:
        raise ForecastSelectionPolicyValidationError(
            "canonical selection policy exceeds the storage limit"
        )
    return canonical


def parse_selection_policy(canonical_policy: bytes) -> ProspectiveForecastSelectionPolicy:
    """Parse and recanonicalize bytes, rejecting all noncanonical representations."""

    _bounded_bytes(canonical_policy)
    try:
        document = json.loads(
            canonical_policy.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_json_constant,
        )
    except ForecastSelectionPolicyValidationError:
        raise
    except (UnicodeError, ValueError, OverflowError, RecursionError) as exc:
        raise ForecastSelectionPolicyValidationError(
            "canonical selection policy is not strict UTF-8 JSON"
        ) from exc
    if not isinstance(document, dict):
        raise ForecastSelectionPolicyValidationError("selection policy must be a JSON object")
    _exact_keys(
        document,
        {
            "format",
            "minimum_seal_lead_seconds",
            "policy_epoch",
            "schema_version",
            "study",
            "windows",
        },
        "selection policy",
    )
    if document["format"] != SELECTION_POLICY_FORMAT:
        raise ForecastSelectionPolicyValidationError("selection policy format is not supported")
    study = _object(document["study"], "study")
    _exact_keys(
        study,
        {
            "cadence",
            "currency",
            "horizon",
            "horizon_unit",
            "interval_coverages_millis",
            "model_selector",
            "model_version",
            "selected_steps",
            "selection_rule",
            "series_basis",
            "snapshot_binding",
            "symbols",
            "target",
        },
        "study",
    )
    windows = _object(document["windows"], "windows")
    _exact_keys(
        windows,
        {
            "fit",
            "heldout",
            "membership_aggregation_rule",
            "membership_counting_unit",
            "window_date_policy_version",
        },
        "windows",
    )
    if windows["membership_aggregation_rule"] != MEMBERSHIP_AGGREGATION_RULE:
        raise ForecastSelectionPolicyValidationError("membership aggregation rule is not supported")
    if windows["membership_counting_unit"] != MEMBERSHIP_COUNTING_UNIT:
        raise ForecastSelectionPolicyValidationError("membership counting unit is not supported")
    if windows["window_date_policy_version"] != WINDOW_DATE_POLICY_VERSION:
        raise ForecastSelectionPolicyValidationError("window date policy version is not supported")
    fit = _object(windows["fit"], "windows.fit")
    heldout = _object(windows["heldout"], "windows.heldout")
    window_keys = {"end", "minimum_member_count", "start"}
    _exact_keys(fit, window_keys, "windows.fit")
    _exact_keys(heldout, window_keys, "windows.heldout")
    epoch = _object(document["policy_epoch"], "policy_epoch")
    _exact_keys(
        epoch,
        {
            "forecast_availability_rule_set_hash",
            "forecast_resolution_policy_hash",
            "outcome_availability_rule_set_hash",
            "outcome_resolution_policy_hash",
            "resolution_lag_seconds",
        },
        "policy_epoch",
    )
    policy = ProspectiveForecastSelectionPolicy(
        symbols=_string_tuple(study["symbols"], "study.symbols"),
        target=_string(study["target"], "study.target"),  # type: ignore[arg-type]
        series_basis=_string(study["series_basis"], "study.series_basis"),  # type: ignore[arg-type]
        horizon_unit=_string(study["horizon_unit"], "study.horizon_unit"),  # type: ignore[arg-type]
        currency=_string(study["currency"], "study.currency"),  # type: ignore[arg-type]
        model_selector=_string(study["model_selector"], "study.model_selector"),
        model_version=_string(study["model_version"], "study.model_version"),
        horizon=_integer(study["horizon"], "study.horizon"),
        selected_steps=_integer_tuple(study["selected_steps"], "study.selected_steps"),
        interval_coverages_millis=_integer_tuple(
            study["interval_coverages_millis"],
            "study.interval_coverages_millis",
        ),
        fit_window=ForecastSelectionWindow(
            start=_parse_date(fit["start"], "windows.fit.start"),
            end=_parse_date(fit["end"], "windows.fit.end"),
        ),
        heldout_window=ForecastSelectionWindow(
            start=_parse_date(heldout["start"], "windows.heldout.start"),
            end=_parse_date(heldout["end"], "windows.heldout.end"),
        ),
        minimum_fit_member_count=_integer(
            fit["minimum_member_count"],
            "windows.fit.minimum_member_count",
        ),
        minimum_heldout_member_count=_integer(
            heldout["minimum_member_count"],
            "windows.heldout.minimum_member_count",
        ),
        minimum_seal_lead_seconds=_integer(
            document["minimum_seal_lead_seconds"],
            "minimum_seal_lead_seconds",
        ),
        cadence=_string(study["cadence"], "study.cadence"),  # type: ignore[arg-type]
        snapshot_binding=_string(study["snapshot_binding"], "study.snapshot_binding"),  # type: ignore[arg-type]
        selection_rule=_string(study["selection_rule"], "study.selection_rule"),  # type: ignore[arg-type]
        resolution_lag_seconds=_integer(
            epoch["resolution_lag_seconds"],
            "policy_epoch.resolution_lag_seconds",
        ),
        forecast_resolution_policy_hash=_string(
            epoch["forecast_resolution_policy_hash"],
            "policy_epoch.forecast_resolution_policy_hash",
        ),
        forecast_availability_rule_set_hash=_string(
            epoch["forecast_availability_rule_set_hash"],
            "policy_epoch.forecast_availability_rule_set_hash",
        ),
        outcome_resolution_policy_hash=_string(
            epoch["outcome_resolution_policy_hash"],
            "policy_epoch.outcome_resolution_policy_hash",
        ),
        outcome_availability_rule_set_hash=_string(
            epoch["outcome_availability_rule_set_hash"],
            "policy_epoch.outcome_availability_rule_set_hash",
        ),
        schema_version=_integer(document["schema_version"], "schema_version"),
    )
    normalized = _validated_policy(policy)
    if canonical_selection_policy(normalized) != canonical_policy:
        raise ForecastSelectionPolicyValidationError("selection policy bytes are not canonical")
    return normalized


def selection_policy_hash_for(
    policy_or_bytes: ProspectiveForecastSelectionPolicy | bytes,
) -> str:
    """Return the SHA-256 identity of validated canonical policy bytes."""

    if isinstance(policy_or_bytes, ProspectiveForecastSelectionPolicy):
        canonical = canonical_selection_policy(policy_or_bytes)
    else:
        canonical = canonical_selection_policy(parse_selection_policy(policy_or_bytes))
    return f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def purpose_for_target_time(
    policy: ProspectiveForecastSelectionPolicy,
    target_time: datetime,
) -> CohortPurpose | None:
    """Derive purpose from the target's UTC date; return ``None`` outside both windows."""

    normalized = _validated_policy(policy)
    target_date = _utc(target_time, "target_time").date()
    if normalized.fit_window.start <= target_date <= normalized.fit_window.end:
        return "calibration_fit"
    if normalized.heldout_window.start <= target_date <= normalized.heldout_window.end:
        return "heldout_evaluation"
    return None


def assign_selection_purposes(
    policy: ProspectiveForecastSelectionPolicy,
    candidates: tuple[ForecastSelectionCandidate, ...],
) -> tuple[ForecastPurposeAssignment, ...]:
    """Assign complete opportunity bundles and reject batch-local contamination.

    Every distinct ``opportunity_hash`` must contribute exactly one candidate
    for every ratified selected step.  This prevents incremental calls from
    bypassing the all-selected-steps rule.  Durable uniqueness across separate
    transactions belongs to the selection-policy registry migration.
    """

    normalized = _validated_policy(policy)
    if not isinstance(candidates, tuple) or not 1 <= len(candidates) <= _MAX_MEMBER_COUNT:
        raise ForecastSelectionPolicyValidationError(
            "selection candidates must be a nonempty bounded tuple"
        )
    policy_hash = selection_policy_hash_for(normalized)
    assignments: list[ForecastPurposeAssignment] = []
    keys: set[tuple[str, int]] = set()
    steps_by_opportunity: dict[str, set[int]] = {}
    purpose_by_opportunity: dict[str, CohortPurpose] = {}
    for candidate in candidates:
        normalized_candidate = _normalized_candidate(candidate)
        if normalized_candidate.step not in normalized.selected_steps:
            raise ForecastSelectionPolicyValidationError(
                "candidate step is not selected by the policy"
            )
        purpose = purpose_for_target_time(normalized, normalized_candidate.target_time)
        if purpose is None:
            raise ForecastSelectionPolicyValidationError(
                "candidate target is outside the prospective policy windows"
            )
        prior_purpose = purpose_by_opportunity.setdefault(
            normalized_candidate.opportunity_hash,
            purpose,
        )
        if prior_purpose != purpose:
            raise ForecastSelectionPolicyValidationError(
                "one forecast opportunity cannot cross fit and held-out purposes"
            )
        key = (normalized_candidate.opportunity_hash, normalized_candidate.step)
        if key in keys:
            raise ForecastSelectionPolicyValidationError(
                "selection candidates contain a duplicate opportunity step"
            )
        keys.add(key)
        steps_by_opportunity.setdefault(normalized_candidate.opportunity_hash, set()).add(
            normalized_candidate.step
        )
        assignments.append(
            ForecastPurposeAssignment(
                candidate=normalized_candidate,
                purpose=purpose,
                selection_policy_hash=policy_hash,
            )
        )
    expected_steps = set(normalized.selected_steps)
    if any(steps != expected_steps for steps in steps_by_opportunity.values()):
        raise ForecastSelectionPolicyValidationError(
            "each forecast opportunity must provide exactly all selected steps"
        )
    return tuple(
        sorted(
            assignments,
            key=lambda item: (
                item.candidate.target_time,
                item.candidate.opportunity_hash,
                item.candidate.step,
            ),
        )
    )


def validate_selection_policy_outcome_epoch(
    policy: ProspectiveForecastSelectionPolicy,
    outcome_policy: SelectionOutcomePolicyEpoch,
) -> None:
    """Match a policy to one exact registered outcome-policy epoch.

    This check is intentionally separate from canonical parsing: historical
    selection-policy bytes must remain readable after calendar or dependency
    epochs change.  The future registry/operator supplies the exact referenced
    outcome-policy artifact when composing executable work.
    """

    normalized = _validated_policy(policy)
    if not isinstance(outcome_policy, SelectionOutcomePolicyEpoch):
        raise ForecastSelectionPolicyValidationError(
            "outcome policy does not expose the required epoch identity"
        )
    lag = _bounded_policy_seconds(
        outcome_policy.resolution_lag_seconds,
        "outcome_policy.resolution_lag_seconds",
    )
    resolution_hash = _sha256(
        outcome_policy.outcome_resolution_policy_hash,
        "outcome_policy.outcome_resolution_policy_hash",
    )
    availability_hash = _sha256(
        outcome_policy.availability_rule_set_hash,
        "outcome_policy.availability_rule_set_hash",
    )
    if (
        lag != normalized.resolution_lag_seconds
        or not hmac.compare_digest(
            resolution_hash,
            normalized.outcome_resolution_policy_hash,
        )
        or not hmac.compare_digest(
            availability_hash,
            normalized.outcome_availability_rule_set_hash,
        )
    ):
        raise ForecastSelectionPolicyValidationError(
            "selection policy does not match the supplied outcome-policy epoch"
        )


def _validate_policy(policy: ProspectiveForecastSelectionPolicy) -> None:
    if type(policy.schema_version) is not int or (
        policy.schema_version != SELECTION_POLICY_SCHEMA_VERSION
    ):
        raise ForecastSelectionPolicyValidationError(
            "selection policy schema_version is not supported"
        )
    _validate_symbols(policy.symbols)
    if policy.target != _TARGET:
        raise ForecastSelectionPolicyValidationError("selection target must be raw close v1")
    if policy.series_basis != _SERIES_BASIS:
        raise ForecastSelectionPolicyValidationError("selection series_basis must be raw v1")
    if policy.horizon_unit != _HORIZON_UNIT:
        raise ForecastSelectionPolicyValidationError(
            "selection horizon_unit must be trading_day v1"
        )
    if policy.currency != _CURRENCY:
        raise ForecastSelectionPolicyValidationError("selection currency must be USD v1")
    if not isinstance(policy.model_selector, str):
        raise ForecastSelectionPolicyValidationError(
            "selection model_selector must be an explicit executable model"
        )
    expected_model_version_prefix = _MODEL_VERSION_PREFIXES.get(policy.model_selector)
    if expected_model_version_prefix is None:
        raise ForecastSelectionPolicyValidationError(
            "selection model_selector must be an explicit executable model"
        )
    if (
        not isinstance(policy.model_version, str)
        or _MODEL_VERSION_PATTERN.fullmatch(policy.model_version) is None
    ):
        raise ForecastSelectionPolicyValidationError("selection model_version is not canonical")
    if not policy.model_version.startswith(expected_model_version_prefix):
        raise ForecastSelectionPolicyValidationError(
            "selection model_version does not match model_selector"
        )
    horizon = _bounded_step(policy.horizon, "horizon")
    _validate_steps(policy.selected_steps, horizon)
    _validate_coverages(policy.interval_coverages_millis)
    fit_window = _validated_window(policy.fit_window, "fit_window")
    heldout_window = _validated_window(policy.heldout_window, "heldout_window")
    if fit_window.end >= heldout_window.start:
        raise ForecastSelectionPolicyValidationError(
            "fit window must end strictly before the held-out window starts"
        )
    _bounded_member_count(
        policy.minimum_fit_member_count,
        "minimum_fit_member_count",
    )
    _bounded_member_count(
        policy.minimum_heldout_member_count,
        "minimum_heldout_member_count",
    )
    if (
        type(policy.minimum_seal_lead_seconds) is not int
        or not MINIMUM_SEAL_LEAD_SECONDS <= policy.minimum_seal_lead_seconds <= _MAX_POLICY_SECONDS
    ):
        raise ForecastSelectionPolicyValidationError(
            "minimum_seal_lead_seconds is below the safety fence or unbounded"
        )
    if policy.cadence != _CADENCE:
        raise ForecastSelectionPolicyValidationError(
            "selection cadence must be xnys_session_daily v1"
        )
    if policy.snapshot_binding != _SNAPSHOT_BINDING:
        raise ForecastSelectionPolicyValidationError(
            "selection snapshots must be explicitly pinned v1"
        )
    if policy.selection_rule != _SELECTION_RULE:
        raise ForecastSelectionPolicyValidationError("selection rule is not supported")
    forecast_resolution = _sha256(
        policy.forecast_resolution_policy_hash,
        "forecast_resolution_policy_hash",
    )
    forecast_availability = _sha256(
        policy.forecast_availability_rule_set_hash,
        "forecast_availability_rule_set_hash",
    )
    _sha256(
        policy.outcome_resolution_policy_hash,
        "outcome_resolution_policy_hash",
    )
    _sha256(
        policy.outcome_availability_rule_set_hash,
        "outcome_availability_rule_set_hash",
    )
    del forecast_resolution, forecast_availability
    _bounded_policy_seconds(policy.resolution_lag_seconds, "resolution_lag_seconds")


def _validated_policy(
    policy: ProspectiveForecastSelectionPolicy,
) -> ProspectiveForecastSelectionPolicy:
    if not isinstance(policy, ProspectiveForecastSelectionPolicy):
        raise TypeError("policy must be a ProspectiveForecastSelectionPolicy")
    fit_window = _validated_window(policy.fit_window, "fit_window")
    heldout_window = _validated_window(policy.heldout_window, "heldout_window")
    return ProspectiveForecastSelectionPolicy(
        symbols=policy.symbols,
        target=policy.target,
        series_basis=policy.series_basis,
        horizon_unit=policy.horizon_unit,
        currency=policy.currency,
        model_selector=policy.model_selector,
        model_version=policy.model_version,
        horizon=policy.horizon,
        selected_steps=policy.selected_steps,
        interval_coverages_millis=policy.interval_coverages_millis,
        fit_window=fit_window,
        heldout_window=heldout_window,
        minimum_fit_member_count=policy.minimum_fit_member_count,
        minimum_heldout_member_count=policy.minimum_heldout_member_count,
        minimum_seal_lead_seconds=policy.minimum_seal_lead_seconds,
        cadence=policy.cadence,
        snapshot_binding=policy.snapshot_binding,
        selection_rule=policy.selection_rule,
        resolution_lag_seconds=policy.resolution_lag_seconds,
        forecast_resolution_policy_hash=policy.forecast_resolution_policy_hash,
        forecast_availability_rule_set_hash=policy.forecast_availability_rule_set_hash,
        outcome_resolution_policy_hash=policy.outcome_resolution_policy_hash,
        outcome_availability_rule_set_hash=policy.outcome_availability_rule_set_hash,
        schema_version=policy.schema_version,
    )


def _validated_window(value: object, label: str) -> ForecastSelectionWindow:
    if not isinstance(value, ForecastSelectionWindow):
        raise ForecastSelectionPolicyValidationError("selection windows have the wrong type")
    return ForecastSelectionWindow(
        start=_calendar_date(value.start, f"{label}.start"),
        end=_calendar_date(value.end, f"{label}.end"),
    )


def _normalized_candidate(candidate: ForecastSelectionCandidate) -> ForecastSelectionCandidate:
    if not isinstance(candidate, ForecastSelectionCandidate):
        raise ForecastSelectionPolicyValidationError("selection candidates have the wrong type")
    return ForecastSelectionCandidate(
        opportunity_hash=_sha256(candidate.opportunity_hash, "candidate.opportunity_hash"),
        step=_bounded_step(candidate.step, "candidate.step"),
        target_time=_utc(candidate.target_time, "candidate.target_time"),
    )


def _validate_symbols(symbols: object) -> tuple[str, ...]:
    if not isinstance(symbols, tuple) or not 1 <= len(symbols) <= _MAX_SYMBOLS:
        raise ForecastSelectionPolicyValidationError(
            "selection symbols must be a nonempty bounded tuple"
        )
    normalized = tuple(_symbol(value, "symbols") for value in symbols)
    if normalized != tuple(sorted(set(normalized))):
        raise ForecastSelectionPolicyValidationError(
            "selection symbols must be sorted and contain no duplicates"
        )
    return normalized


def _validate_steps(steps: object, horizon: int) -> tuple[int, ...]:
    if not isinstance(steps, tuple) or not steps:
        raise ForecastSelectionPolicyValidationError("selected_steps must be a nonempty tuple")
    normalized = tuple(_bounded_step(step, "selected_steps") for step in steps)
    if any(step > horizon for step in normalized):
        raise ForecastSelectionPolicyValidationError(
            "selected_steps must not exceed the forecast horizon"
        )
    if normalized != tuple(sorted(set(normalized))):
        raise ForecastSelectionPolicyValidationError(
            "selected_steps must be sorted and contain no duplicates"
        )
    return normalized


def _validate_coverages(values: object) -> tuple[int, ...]:
    if not isinstance(values, tuple) or not 1 <= len(values) <= 9:
        raise ForecastSelectionPolicyValidationError(
            "interval coverages must be a nonempty tuple with at most nine values"
        )
    normalized = tuple(_integer(value, "interval_coverages_millis") for value in values)
    if any(not 1 <= value <= 999 for value in normalized):
        raise ForecastSelectionPolicyValidationError(
            "interval coverages must be canonical thousandths within 1..999"
        )
    if normalized != tuple(sorted(set(normalized))):
        raise ForecastSelectionPolicyValidationError(
            "interval coverages must be sorted and contain no duplicates"
        )
    return normalized


def _bounded_member_count(value: object, label: str) -> int:
    integer = _integer(value, label)
    if not 1 <= integer <= _MAX_MEMBER_COUNT:
        raise ForecastSelectionPolicyValidationError(f"{label} must be a positive bounded integer")
    return integer


def _bounded_policy_seconds(value: object, label: str) -> int:
    integer = _integer(value, label)
    if not 1 <= integer <= _MAX_POLICY_SECONDS:
        raise ForecastSelectionPolicyValidationError(f"{label} must be a positive bounded integer")
    return integer


def _bounded_step(value: object, label: str) -> int:
    integer = _integer(value, label)
    if not 1 <= integer <= _MAX_HORIZON:
        raise ForecastSelectionPolicyValidationError(f"{label} must be within 1..{_MAX_HORIZON}")
    return integer


def _integer(value: object, label: str) -> int:
    if type(value) is not int:
        raise ForecastSelectionPolicyValidationError(f"{label} must be an integer")
    return value


def _string(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise ForecastSelectionPolicyValidationError(f"{label} must be a string")
    return value


def _string_tuple(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ForecastSelectionPolicyValidationError(f"{label} must be a JSON array")
    return tuple(_string(item, label) for item in value)


def _integer_tuple(value: object, label: str) -> tuple[int, ...]:
    if not isinstance(value, list):
        raise ForecastSelectionPolicyValidationError(f"{label} must be a JSON array")
    return tuple(_integer(item, label) for item in value)


def _symbol(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > 32
        or value != value.upper()
        or _SYMBOL_PATTERN.fullmatch(value) is None
    ):
        raise ForecastSelectionPolicyValidationError(
            f"{label} must contain canonical uppercase symbols"
        )
    return value


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise ForecastSelectionPolicyValidationError(f"{label} must be a canonical sha256 hash")
    return value


def _calendar_date(value: object, label: str) -> date:
    if type(value) is not date:
        raise ForecastSelectionPolicyValidationError(f"{label} must be a calendar date")
    return value


def _parse_date(value: object, label: str) -> date:
    if not isinstance(value, str):
        raise ForecastSelectionPolicyValidationError(f"{label} must be a canonical ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ForecastSelectionPolicyValidationError(f"{label} is not a valid date") from exc
    if parsed.isoformat() != value:
        raise ForecastSelectionPolicyValidationError(f"{label} must be a canonical ISO date")
    return parsed


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ForecastSelectionPolicyValidationError(f"{label} must be timezone-aware")
    try:
        return value.astimezone(UTC)
    except (ValueError, OverflowError, OSError) as exc:
        raise ForecastSelectionPolicyValidationError(
            f"{label} cannot be normalized to UTC"
        ) from exc


def _bounded_bytes(value: object) -> bytes:
    if (
        not isinstance(value, bytes)
        or not value
        or len(value) > MAX_CANONICAL_SELECTION_POLICY_BYTES
    ):
        raise ForecastSelectionPolicyValidationError(
            "canonical selection policy must be nonempty bounded bytes"
        )
    return value


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ForecastSelectionPolicyValidationError(f"{label} must be a JSON object")
    return value


def _exact_keys(value: dict[str, object], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise ForecastSelectionPolicyValidationError(f"{label} has unknown or missing keys")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ForecastSelectionPolicyValidationError(
                "canonical selection policy contains a duplicate JSON key"
            )
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    raise ForecastSelectionPolicyValidationError(f"JSON constant {value!r} is not permitted")


__all__ = [
    "MAX_CANONICAL_SELECTION_POLICY_BYTES",
    "MEMBERSHIP_AGGREGATION_RULE",
    "MEMBERSHIP_COUNTING_UNIT",
    "MINIMUM_SEAL_LEAD_SECONDS",
    "SELECTION_POLICY_FORMAT",
    "SELECTION_POLICY_SCHEMA_VERSION",
    "WINDOW_DATE_POLICY_VERSION",
    "ForecastPurposeAssignment",
    "ForecastSelectionCandidate",
    "ForecastSelectionPolicyValidationError",
    "ForecastSelectionWindow",
    "ProspectiveForecastSelectionPolicy",
    "SelectionOutcomePolicyEpoch",
    "assign_selection_purposes",
    "canonical_selection_policy",
    "parse_selection_policy",
    "purpose_for_target_time",
    "selection_policy_hash_for",
    "validate_selection_policy_outcome_epoch",
]
