from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID

import pytest

from app.schemas.forecast import (
    DataSourceLineage,
    ForecastCalibration,
    ForecastInterval,
    ForecastProvenance,
    ForecastQuantile,
    ForecastRequest,
    ForecastResponse,
    ForecastStep,
    LookaheadCheck,
)
from app.services.forecast_cohorts import (
    ForecastCohortManifest,
    ForecastCohortRecord,
    ForecastCohortSeal,
    build_cohort_record,
    parse_cohort_manifest,
)
from app.services.forecast_run_store import ArchivedForecastRun
from app.services.forecast_runs import (
    RUN_SCHEMA_VERSION,
    canonical_output,
    canonical_request,
    opportunity_hash,
    output_hash,
    request_hash,
)
from app.services.scheduled_evaluation import (
    CohortPublication,
    CohortPublisher,
    ScheduledEvaluationService,
    ScheduledEvaluationSpec,
    ScheduledEvaluationValidationError,
    ScheduledRunReader,
)

AS_OF = datetime(2026, 7, 14, 20, tzinfo=UTC)
FORECAST_ID = UUID("33333333-3333-3333-3333-333333333333")
SNAPSHOT_ID = "sha256:" + "1" * 64
FORECAST_RESOLUTION_HASH = "sha256:" + "2" * 64
FORECAST_AVAILABILITY_HASH = "sha256:" + "3" * 64
SELECTION_HASH = "sha256:" + "4" * 64
OUTCOME_RESOLUTION_HASH = "sha256:" + "5" * 64
OUTCOME_AVAILABILITY_HASH = "sha256:" + "6" * 64
MODEL_VERSION = "baseline-naive@1"
CODE_VERSION = "fixture-code-1"


def _request(**updates: object) -> ForecastRequest:
    request = ForecastRequest(
        symbol="MSFT",
        horizon=2,
        horizon_unit="trading_day",
        target="close",
        snapshot_id=SNAPSHOT_ID,
        model="baseline_naive",
        interval_coverages=[0.8],
    )
    return request.model_copy(update=updates)


def _response(
    *,
    forecast_id: UUID = FORECAST_ID,
    first_target: datetime = AS_OF + timedelta(days=1),
    point: float = 100.0,
) -> ForecastResponse:
    quantiles = [
        ForecastQuantile(level=0.1, value=point - 2.0),
        ForecastQuantile(level=0.5, value=point),
        ForecastQuantile(level=0.9, value=point + 2.0),
    ]
    intervals = [
        ForecastInterval(
            coverage=0.8,
            lower_quantile=0.1,
            upper_quantile=0.9,
            lower=point - 2.0,
            upper=point + 2.0,
        )
    ]
    return ForecastResponse(
        symbol="MSFT",
        target="close",
        horizon=2,
        horizon_unit="trading_day",
        as_of=AS_OF,
        currency="USD",
        forecasts=[
            ForecastStep(
                step=step,
                target_time=first_target + timedelta(days=step - 1),
                point=point,
                quantiles=quantiles,
                intervals=intervals,
            )
            for step in (1, 2)
        ],
        provenance=ForecastProvenance(
            forecast_id=forecast_id,
            snapshot_id=SNAPSHOT_ID,
            model_version="baseline-naive@1",
            series_basis="raw",
            feature_set_hash="sha256:" + "7" * 64,
            max_available_at=AS_OF,
            generated_at=AS_OF + timedelta(minutes=5),
            code_version=CODE_VERSION,
            data_sources=[
                DataSourceLineage(
                    name="sealed_snapshot",
                    snapshot_id=SNAPSHOT_ID,
                    max_available_at=AS_OF,
                    fields=["close"],
                )
            ],
            lookahead_check=LookaheadCheck(
                status="passed",
                checked_at=AS_OF + timedelta(minutes=5),
                max_feature_available_at=AS_OF,
            ),
        ),
        calibration=ForecastCalibration(
            calibration_set_version="uncalibrated:baseline-naive@1",
            method="none",
            sample_count=0,
        ),
    )


def _archived(
    request: ForecastRequest,
    response: ForecastResponse,
) -> ArchivedForecastRun:
    request_payload = canonical_request(request)
    output_payload = canonical_output(response)
    provenance = response.provenance
    return ArchivedForecastRun(
        forecast_id=provenance.forecast_id,
        schema_version=RUN_SCHEMA_VERSION,
        origin_kind="scheduled_evaluation",
        idempotency_token_digest=None,
        request_hash=request_hash(request_payload),
        opportunity_hash=opportunity_hash(
            response,
            resolution_policy_hash=FORECAST_RESOLUTION_HASH,
            availability_rule_set_hash=FORECAST_AVAILABILITY_HASH,
            origin_kind="scheduled_evaluation",
        ),
        output_hash=output_hash(output_payload),
        snapshot_id=provenance.snapshot_id,
        resolution_policy_hash=FORECAST_RESOLUTION_HASH,
        availability_rule_set_hash=FORECAST_AVAILABILITY_HASH,
        symbol=response.symbol,
        target=response.target,
        horizon=response.horizon,
        horizon_unit=response.horizon_unit,
        series_basis=provenance.series_basis,
        as_of=response.as_of,
        max_available_at=provenance.max_available_at,
        model_version=provenance.model_version,
        feature_set_hash=provenance.feature_set_hash,
        code_version=provenance.code_version,
        calibration_set_version=response.calibration.calibration_set_version,
        calibration_method=response.calibration.method,
        generated_at=provenance.generated_at,
        recorded_at=AS_OF + timedelta(minutes=6),
        canonical_request=request_payload,
        canonical_output=output_payload,
    )


class _ForecastService:
    def __init__(self, response: ForecastResponse, run_store: _RunStore) -> None:
        self.response = response
        self.run_store = run_store
        self.policy = _ForecastPolicy()
        self.code_version: str | None = CODE_VERSION
        self.calls: list[ForecastRequest] = []

    def model_version_for(self, model: str) -> str:
        assert model == "baseline_naive"
        return MODEL_VERSION

    async def forecast(
        self,
        request: ForecastRequest,
        *,
        idempotency_key: str | None = None,
        principal: str | None = None,
    ) -> ForecastResponse:
        assert idempotency_key is None
        assert principal is None
        self.calls.append(request)
        return self.response


class _RunStore:
    resolution_policy_hash = FORECAST_RESOLUTION_HASH
    availability_rule_set_hash = FORECAST_AVAILABILITY_HASH
    origin_kind = "scheduled_evaluation"

    def __init__(self, run: ArchivedForecastRun) -> None:
        self.run = run
        self.calls: list[tuple[UUID, ForecastRequest, str]] = []

    async def read_validated(
        self,
        forecast_id: UUID,
        *,
        expected_request: ForecastRequest,
        expected_origin_kind: str,
    ) -> ArchivedForecastRun:
        self.calls.append((forecast_id, expected_request, expected_origin_kind))
        return self.run


@dataclass(frozen=True)
class _ForecastPolicy:
    resolution_policy_hash: str = FORECAST_RESOLUTION_HASH
    trusted_availability_rule_set_hash: str = FORECAST_AVAILABILITY_HASH


@dataclass(frozen=True)
class _Publication:
    record: ForecastCohortRecord
    seal: ForecastCohortSeal


class _CohortStore:
    def __init__(self) -> None:
        self.manifests: list[ForecastCohortManifest] = []

    async def publish(self, manifest: ForecastCohortManifest) -> CohortPublication:
        self.manifests.append(manifest)
        record = build_cohort_record(
            manifest,
            recorded_at=AS_OF + timedelta(minutes=7),
            creator_xid=101,
        )
        return _Publication(
            record=record,
            seal=ForecastCohortSeal(
                cohort_id=record.cohort_id,
                manifest_recorded_at=record.recorded_at,
                sealed_at=AS_OF + timedelta(minutes=8),
                sealer_xid=102,
            ),
        )


def _spec(request: ForecastRequest | None = None, **updates: object) -> ScheduledEvaluationSpec:
    spec = ScheduledEvaluationSpec(
        request=request or _request(),
        purpose="heldout_evaluation",
        selected_steps=(1, 2),
        model_version=MODEL_VERSION,
        code_version=CODE_VERSION,
        forecast_resolution_policy_hash=FORECAST_RESOLUTION_HASH,
        forecast_availability_rule_set_hash=FORECAST_AVAILABILITY_HASH,
        selection_policy_hash=SELECTION_HASH,
        outcome_resolution_policy_hash=OUTCOME_RESOLUTION_HASH,
        outcome_availability_rule_set_hash=OUTCOME_AVAILABILITY_HASH,
    )
    return replace(spec, **updates)


def _service(
    *,
    provisional: ForecastResponse | None = None,
    run: ArchivedForecastRun | None = None,
) -> tuple[ScheduledEvaluationService, _ForecastService, _RunStore, _CohortStore]:
    request = _request()
    persisted = _response()
    run_store = _RunStore(run or _archived(request, persisted))
    producer = _ForecastService(provisional or persisted, run_store)
    cohort_store = _CohortStore()
    service = ScheduledEvaluationService(
        forecast_service=producer,
        run_store=run_store,
        cohort_store=cohort_store,
    )
    return service, producer, run_store, cohort_store


async def test_publish_uses_validated_persisted_run_and_two_phase_cohort_proof() -> None:
    service, producer, run_store, cohort_store = _service()

    proof = await service.publish(_spec())

    assert producer.calls == [_request()]
    assert run_store.calls == [(FORECAST_ID, _request(), "scheduled_evaluation")]
    assert proof.run == run_store.run
    manifest = parse_cohort_manifest(proof.cohort_record.canonical_manifest)
    assert manifest == cohort_store.manifests[0]
    assert [member.step for member in manifest.members] == [1, 2]
    assert proof.cohort_seal.manifest_recorded_at == proof.cohort_record.recorded_at


async def test_provisional_response_is_only_a_forecast_id_locator() -> None:
    provisional = _response(
        first_target=AS_OF + timedelta(days=10),
        point=900.0,
    )
    service, _, run_store, _ = _service(provisional=provisional)

    proof = await service.publish(_spec())

    manifest = parse_cohort_manifest(proof.cohort_record.canonical_manifest)
    assert all(member.output_hash == run_store.run.output_hash for member in manifest.members)
    assert manifest.members[0].target_time == AS_OF + timedelta(days=1)
    assert manifest.members[0].target_time != provisional.forecasts[0].target_time


async def test_retry_after_persisted_run_read_failure_completes_one_cohort() -> None:
    class _FailOnceRunStore(_RunStore):
        failed = False

        async def read_validated(
            self,
            forecast_id: UUID,
            *,
            expected_request: ForecastRequest,
            expected_origin_kind: str,
        ) -> ArchivedForecastRun:
            self.calls.append((forecast_id, expected_request, expected_origin_kind))
            if not self.failed:
                self.failed = True
                raise RuntimeError("injected post-run read failure")
            return self.run

    run_store = _FailOnceRunStore(_archived(_request(), _response()))
    producer = _ForecastService(_response(), run_store)
    cohort_store = _CohortStore()
    service = ScheduledEvaluationService(producer, run_store, cohort_store)

    with pytest.raises(RuntimeError, match="post-run"):
        await service.publish(_spec())
    proof = await service.publish(_spec())

    assert len(producer.calls) == 2
    assert len(run_store.calls) == 2
    assert proof.run.forecast_id == FORECAST_ID
    assert len(cohort_store.manifests) == 1


async def test_retry_after_manifest_only_completion_seals_the_same_cohort() -> None:
    class _FailAfterManifestStore(_CohortStore):
        record: ForecastCohortRecord | None = None
        seal_count = 0

        async def publish(self, manifest: ForecastCohortManifest) -> CohortPublication:
            self.manifests.append(manifest)
            if self.record is None:
                self.record = build_cohort_record(
                    manifest,
                    recorded_at=AS_OF + timedelta(minutes=7),
                    creator_xid=301,
                )
                raise RuntimeError("injected post-manifest failure")
            self.seal_count += 1
            return _Publication(
                record=self.record,
                seal=ForecastCohortSeal(
                    cohort_id=self.record.cohort_id,
                    manifest_recorded_at=self.record.recorded_at,
                    sealed_at=AS_OF + timedelta(minutes=8),
                    sealer_xid=302,
                ),
            )

    request = _request()
    run_store = _RunStore(_archived(request, _response()))
    producer = _ForecastService(_response(), run_store)
    cohort_store = _FailAfterManifestStore()
    service = ScheduledEvaluationService(producer, run_store, cohort_store)

    with pytest.raises(RuntimeError, match="post-manifest"):
        await service.publish(_spec(request))
    proof = await service.publish(_spec(request))

    assert len(producer.calls) == 2
    assert len(run_store.calls) == 2
    assert len(cohort_store.manifests) == 2
    assert cohort_store.manifests[0] == cohort_store.manifests[1]
    assert cohort_store.record is not None
    assert proof.cohort_record.cohort_id == cohort_store.record.cohort_id
    assert cohort_store.seal_count == 1


@pytest.mark.parametrize(
    "spec",
    [
        _spec(_request(snapshot_id=None)),
        _spec(_request(model="auto")),
        _spec(_request(as_of=AS_OF)),
        _spec(selected_steps=()),
        _spec(selected_steps=(2, 1)),
        _spec(selected_steps=(1, 1)),
        _spec(selected_steps=(1, 3)),
        _spec(selection_policy_hash="not-a-hash"),
        _spec(model_version="not a model version"),
        _spec(code_version="unattested build"),
    ],
)
async def test_preflight_refuses_unpinned_implicit_or_noncanonical_policy(
    spec: ScheduledEvaluationSpec,
) -> None:
    service, producer, run_store, cohort_store = _service()

    with pytest.raises(ScheduledEvaluationValidationError):
        await service.publish(spec)

    assert producer.calls == []
    assert run_store.calls == []
    assert cohort_store.manifests == []


@pytest.mark.parametrize(
    ("attribute", "value"),
    [
        ("origin_kind", "api"),
        ("resolution_policy_hash", "sha256:" + "a" * 64),
        ("availability_rule_set_hash", "sha256:" + "b" * 64),
    ],
)
async def test_preflight_refuses_a_misconfigured_forecast_archive(
    attribute: str,
    value: str,
) -> None:
    service, producer, run_store, cohort_store = _service()
    setattr(run_store, attribute, value)

    with pytest.raises(ScheduledEvaluationValidationError):
        await service.publish(_spec())

    assert producer.calls == []
    assert run_store.calls == []
    assert cohort_store.manifests == []


@pytest.mark.parametrize(
    "candidate_request",
    [
        ForecastRequest(
            symbol="MSFT",
            snapshot_id=SNAPSHOT_ID,
            model="baseline_naive",
        ),
        ForecastRequest(
            symbol="MSFT",
            snapshot_id=SNAPSHOT_ID,
            model="baseline_naive",
            horizon=2,
            horizon_unit="trading_day",
            target="close",
        ),
    ],
)
async def test_preflight_refuses_implicit_scientific_request_defaults(
    candidate_request: ForecastRequest,
) -> None:
    service, producer, run_store, cohort_store = _service()

    with pytest.raises(ScheduledEvaluationValidationError, match="scientific request field"):
        await service.publish(_spec(candidate_request))

    assert producer.calls == []
    assert run_store.calls == []
    assert cohort_store.manifests == []


@pytest.mark.parametrize(
    "miswire",
    [
        "different_store",
        "resolution",
        "availability",
        "model_version",
        "code_version",
    ],
)
async def test_preflight_refuses_snapshot_service_policy_or_store_miswiring(
    miswire: str,
) -> None:
    service, producer, run_store, cohort_store = _service()
    if miswire == "different_store":
        producer.run_store = object()
    elif miswire == "resolution":
        producer.policy = replace(
            producer.policy,
            resolution_policy_hash="sha256:" + "a" * 64,
        )
    elif miswire == "availability":
        producer.policy = replace(
            producer.policy,
            trusted_availability_rule_set_hash="sha256:" + "b" * 64,
        )
    elif miswire == "model_version":
        producer.model_version_for = lambda _model: "baseline-naive@2"  # type: ignore[method-assign]
    else:
        producer.code_version = "different-build"

    with pytest.raises(ScheduledEvaluationValidationError):
        await service.publish(_spec())

    assert producer.calls == []
    assert run_store.calls == []
    assert cohort_store.manifests == []


@pytest.mark.parametrize("missing", ["run_reader", "cohort_publisher"])
async def test_preflight_refuses_structurally_incomplete_dependencies(
    missing: str,
) -> None:
    service, producer, run_store, cohort_store = _service()
    if missing == "run_reader":

        class _NoValidatedRead:
            resolution_policy_hash = FORECAST_RESOLUTION_HASH
            availability_rule_set_hash = FORECAST_AVAILABILITY_HASH
            origin_kind = "scheduled_evaluation"

        service = replace(
            service,
            run_store=cast(ScheduledRunReader, _NoValidatedRead()),
        )
    else:
        service = replace(
            service,
            cohort_store=cast(CohortPublisher, object()),
        )

    with pytest.raises(ScheduledEvaluationValidationError):
        await service.publish(_spec())

    assert producer.calls == []
    assert run_store.calls == []
    assert cohort_store.manifests == []


@pytest.mark.parametrize(
    "run",
    [
        replace(_archived(_request(), _response()), recorded_at=None),
        replace(_archived(_request(), _response()), origin_kind="api"),
        replace(
            _archived(_request(), _response()),
            resolution_policy_hash="sha256:" + "a" * 64,
        ),
    ],
)
async def test_publish_refuses_invalid_or_wrong_policy_persisted_evidence(
    run: ArchivedForecastRun,
) -> None:
    service, producer, run_store, cohort_store = _service(run=run)

    with pytest.raises(ScheduledEvaluationValidationError):
        await service.publish(_spec())

    assert len(producer.calls) == 1
    assert len(run_store.calls) == 1
    assert cohort_store.manifests == []


async def test_publish_rejects_an_invalid_cohort_store_result() -> None:
    service, _, _, _ = _service()

    class _InvalidStore:
        async def publish(self, manifest: ForecastCohortManifest) -> CohortPublication:
            del manifest
            return cast(CohortPublication, object())

    service = replace(service, cohort_store=_InvalidStore())

    with pytest.raises(ScheduledEvaluationValidationError, match="invalid publication"):
        await service.publish(_spec())


async def test_publish_rejects_a_valid_proof_for_different_cohort_content() -> None:
    service, _, _, _ = _service()

    class _DifferentValidStore:
        async def publish(self, manifest: ForecastCohortManifest) -> CohortPublication:
            different = replace(manifest, purpose="calibration_fit")
            record = build_cohort_record(
                different,
                recorded_at=AS_OF + timedelta(minutes=7),
                creator_xid=201,
            )
            return _Publication(
                record=record,
                seal=ForecastCohortSeal(
                    cohort_id=record.cohort_id,
                    manifest_recorded_at=record.recorded_at,
                    sealed_at=AS_OF + timedelta(minutes=8),
                    sealer_xid=202,
                ),
            )

    service = replace(service, cohort_store=_DifferentValidStore())

    with pytest.raises(ScheduledEvaluationValidationError, match="differs"):
        await service.publish(_spec())
