from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
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
from app.services.forecast_cohort_store import ForecastCohortProof
from app.services.forecast_cohorts import (
    ForecastCohortManifest,
    ForecastCohortSeal,
    build_cohort_record,
    member_from_scheduled_run,
)
from app.services.forecast_outcome_collection import (
    ForecastOutcomeCollectionError,
    ForecastOutcomeCollectionService,
    ForecastOutcomeCollectionSpec,
)
from app.services.forecast_outcome_store import (
    ForecastOutcomeProof,
    ForecastOutcomePublicationRecord,
    ForecastOutcomePublicationSource,
)
from app.services.forecast_outcomes import (
    BarVersionEvidence,
    RealizedOutcomePayload,
    build_outcome_record,
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

AS_OF = datetime(2026, 7, 13, 20, tzinfo=UTC)
TARGET = datetime(2026, 7, 14, 20, tzinfo=UTC)
CUTOFF = datetime(2026, 7, 15, 20, tzinfo=UTC)
FORECAST_ID = UUID("51515151-5151-5151-5151-515151515151")
FORECAST_POLICY = "sha256:" + "1" * 64
FORECAST_RULES = "sha256:" + "2" * 64
SELECTION_POLICY = "sha256:" + "3" * 64
OUTCOME_POLICY = "sha256:" + "4" * 64
OUTCOME_RULES = "sha256:" + "5" * 64
SNAPSHOT_ID = "sha256:" + "6" * 64


def _response(*, point: float = 101.0) -> ForecastResponse:
    quantiles = [
        ForecastQuantile(level=0.1, value=point - 2.0),
        ForecastQuantile(level=0.5, value=point),
        ForecastQuantile(level=0.9, value=point + 2.0),
    ]
    return ForecastResponse(
        symbol="MSFT",
        target="close",
        horizon=1,
        horizon_unit="trading_day",
        as_of=AS_OF,
        currency="USD",
        forecasts=[
            ForecastStep(
                step=1,
                target_time=TARGET,
                point=point,
                quantiles=quantiles,
                intervals=[
                    ForecastInterval(
                        coverage=0.8,
                        lower_quantile=0.1,
                        upper_quantile=0.9,
                        lower=point - 2.0,
                        upper=point + 2.0,
                    )
                ],
            )
        ],
        provenance=ForecastProvenance(
            forecast_id=FORECAST_ID,
            snapshot_id=SNAPSHOT_ID,
            model_version="baseline-naive@1",
            series_basis="raw",
            feature_set_hash=SNAPSHOT_ID,
            max_available_at=AS_OF,
            generated_at=AS_OF + timedelta(minutes=2),
            code_version="fixture-1",
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
                checked_at=AS_OF + timedelta(minutes=2),
                max_feature_available_at=AS_OF,
            ),
        ),
        calibration=ForecastCalibration(
            calibration_set_version="uncalibrated:baseline-naive@1",
            method="none",
            sample_count=0,
        ),
    )


def _request() -> ForecastRequest:
    return ForecastRequest(
        symbol="MSFT",
        horizon=1,
        horizon_unit="trading_day",
        target="close",
        snapshot_id=SNAPSHOT_ID,
        model="baseline_naive",
        interval_coverages=[0.8],
    )


def _run(response: ForecastResponse | None = None) -> ArchivedForecastRun:
    result = response or _response()
    request_bytes = canonical_request(_request())
    output_bytes = canonical_output(result)
    return ArchivedForecastRun(
        forecast_id=FORECAST_ID,
        schema_version=RUN_SCHEMA_VERSION,
        origin_kind="scheduled_evaluation",
        idempotency_token_digest=None,
        request_hash=request_hash(request_bytes),
        opportunity_hash=opportunity_hash(
            result,
            resolution_policy_hash=FORECAST_POLICY,
            availability_rule_set_hash=FORECAST_RULES,
            origin_kind="scheduled_evaluation",
        ),
        output_hash=output_hash(output_bytes),
        snapshot_id=SNAPSHOT_ID,
        resolution_policy_hash=FORECAST_POLICY,
        availability_rule_set_hash=FORECAST_RULES,
        symbol="MSFT",
        target="close",
        horizon=1,
        horizon_unit="trading_day",
        series_basis="raw",
        as_of=AS_OF,
        max_available_at=AS_OF,
        model_version="baseline-naive@1",
        feature_set_hash=SNAPSHOT_ID,
        code_version="fixture-1",
        calibration_set_version="uncalibrated:baseline-naive@1",
        calibration_method="none",
        generated_at=AS_OF + timedelta(minutes=2),
        recorded_at=AS_OF + timedelta(minutes=3),
        canonical_request=request_bytes,
        canonical_output=output_bytes,
    )


def _cohort(run: ArchivedForecastRun | None = None) -> ForecastCohortProof:
    member = member_from_scheduled_run(run or _run(), step=1)
    manifest = ForecastCohortManifest(
        purpose="heldout_evaluation",
        selection_policy_hash=SELECTION_POLICY,
        outcome_resolution_policy_hash=OUTCOME_POLICY,
        availability_rule_set_hash=OUTCOME_RULES,
        members=(member,),
    )
    record = build_cohort_record(
        manifest,
        recorded_at=AS_OF + timedelta(minutes=4),
        creator_xid=101,
    )
    seal = ForecastCohortSeal(
        cohort_id=record.cohort_id,
        manifest_recorded_at=record.recorded_at,
        sealed_at=AS_OF + timedelta(minutes=5),
        sealer_xid=102,
    )
    return ForecastCohortProof(manifest=manifest, record=record, seal=seal)


def _source() -> BarVersionEvidence:
    return BarVersionEvidence(
        symbol="MSFT",
        timespan="day",
        multiplier=1,
        observed_at=TARGET,
        source="polygon_open_close",
        adjustment_basis="raw",
        fetched_at=TARGET + timedelta(minutes=1),
        source_as_of=TARGET + timedelta(minutes=2),
        version_recorded_at=TARGET + timedelta(minutes=3),
        available_at=TARGET + timedelta(minutes=4),
        field="close",
        value=104.25,
    )


class _CohortReader:
    def __init__(self, proof: ForecastCohortProof) -> None:
        self.proof = proof
        self.calls: list[str] = []

    async def read_validated(self, cohort_id: str) -> ForecastCohortProof:
        self.calls.append(cohort_id)
        return self.proof


class _RunReader:
    def __init__(self, run: ArchivedForecastRun) -> None:
        self.run = run
        self.calls: list[tuple[UUID, str]] = []

    async def read_self_validated(
        self,
        forecast_id: UUID,
        *,
        expected_origin_kind: str,
    ) -> ArchivedForecastRun:
        self.calls.append((forecast_id, expected_origin_kind))
        return self.run


class _Resolver:
    outcome_resolution_policy_hash = OUTCOME_POLICY
    availability_rule_set_hash = OUTCOME_RULES

    def __init__(self, source: BarVersionEvidence) -> None:
        self.source = source
        self.calls: list[tuple[str, datetime, datetime]] = []

    async def resolve(
        self,
        *,
        symbol: str,
        target_time: datetime,
        resolution_cutoff: datetime,
    ) -> BarVersionEvidence:
        self.calls.append((symbol, target_time, resolution_cutoff))
        return self.source


class _OutcomeStore:
    outcome_resolution_policy_hash = OUTCOME_POLICY
    availability_rule_set_hash = OUTCOME_RULES

    def __init__(self) -> None:
        self.payloads: list[RealizedOutcomePayload] = []
        self.sources: list[ForecastOutcomePublicationSource] = []

    async def publish(
        self,
        payload: RealizedOutcomePayload,
        *,
        source: ForecastOutcomePublicationSource,
    ) -> ForecastOutcomeProof:
        self.payloads.append(payload)
        self.sources.append(source)
        record = build_outcome_record(payload, sealed_at=CUTOFF + timedelta(minutes=1))
        return ForecastOutcomeProof(
            payload=payload,
            record=record,
            publication=ForecastOutcomePublicationRecord(
                outcome_id=record.outcome_id,
                cohort_id=source.cohort_id,
                forecast_id=source.forecast_id,
                step=source.step,
                published_at=CUTOFF + timedelta(minutes=2),
                publisher_xid=103,
            ),
        )


def _service(
    *,
    run: ArchivedForecastRun | None = None,
    cohort: ForecastCohortProof | None = None,
) -> tuple[
    ForecastOutcomeCollectionService,
    _CohortReader,
    _RunReader,
    _Resolver,
    _OutcomeStore,
]:
    archived = run or _run()
    cohort_reader = _CohortReader(cohort or _cohort(archived))
    run_reader = _RunReader(archived)
    resolver = _Resolver(_source())
    outcome_store = _OutcomeStore()
    return (
        ForecastOutcomeCollectionService(
            cohort_store=cohort_reader,
            run_store=run_reader,
            resolver=resolver,
            outcome_store=outcome_store,
        ),
        cohort_reader,
        run_reader,
        resolver,
        outcome_store,
    )


def _spec(proof: ForecastCohortProof, **updates: object) -> ForecastOutcomeCollectionSpec:
    spec = ForecastOutcomeCollectionSpec(
        cohort_id=proof.record.cohort_id,
        forecast_id=FORECAST_ID,
        step=1,
        resolution_cutoff=CUTOFF,
    )
    return replace(spec, **updates)


async def test_collect_derives_truth_only_from_sealed_and_archived_evidence() -> None:
    service, cohort_reader, run_reader, resolver, outcome_store = _service()
    spec = _spec(cohort_reader.proof)

    proof = await service.collect(spec)

    assert cohort_reader.calls == [spec.cohort_id]
    assert run_reader.calls == [(FORECAST_ID, "scheduled_evaluation")]
    assert resolver.calls == [("MSFT", TARGET, CUTOFF)]
    assert proof.member == cohort_reader.proof.manifest.members[0]
    assert proof.run == run_reader.run
    assert proof.outcome.payload == outcome_store.payloads[0]
    assert proof.outcome.payload.realized_value == 104.25
    assert proof.outcome.payload.source_version == _source()
    assert outcome_store.sources == [
        ForecastOutcomePublicationSource(
            cohort_id=spec.cohort_id,
            forecast_id=FORECAST_ID,
            step=1,
        )
    ]


@pytest.mark.parametrize(
    "updates",
    [
        {"cohort_id": "not-a-hash"},
        {"forecast_id": "not-a-uuid"},
        {"step": 0},
        {"resolution_cutoff": datetime(2026, 7, 15, 20)},
    ],
)
async def test_invalid_spec_fails_before_any_read(updates: dict[str, object]) -> None:
    service, cohort_reader, run_reader, resolver, outcome_store = _service()

    with pytest.raises(ForecastOutcomeCollectionError):
        await service.collect(_spec(cohort_reader.proof, **updates))

    assert cohort_reader.calls == []
    assert run_reader.calls == []
    assert resolver.calls == []
    assert outcome_store.payloads == []


async def test_policy_mismatch_stops_before_forecast_or_bar_reads() -> None:
    service, cohort_reader, run_reader, resolver, outcome_store = _service()
    resolver.outcome_resolution_policy_hash = "sha256:" + "9" * 64

    with pytest.raises(ForecastOutcomeCollectionError, match="precommitted"):
        await service.collect(_spec(cohort_reader.proof))

    assert len(cohort_reader.calls) == 1
    assert run_reader.calls == []
    assert resolver.calls == []
    assert outcome_store.payloads == []


async def test_nonmember_request_never_resolves_or_writes_truth() -> None:
    service, cohort_reader, run_reader, resolver, outcome_store = _service()

    with pytest.raises(ForecastOutcomeCollectionError, match="not a member"):
        await service.collect(_spec(cohort_reader.proof, step=2))

    assert run_reader.calls == []
    assert resolver.calls == []
    assert outcome_store.payloads == []


async def test_detached_run_must_rederive_the_exact_sealed_member() -> None:
    valid_run = _run()
    cohort = _cohort(valid_run)
    changed_run = _run(_response(point=999.0))
    service, cohort_reader, _, resolver, outcome_store = _service(
        run=changed_run,
        cohort=cohort,
    )

    with pytest.raises(ForecastOutcomeCollectionError, match="sealed cohort"):
        await service.collect(_spec(cohort_reader.proof))

    assert resolver.calls == []
    assert outcome_store.payloads == []


async def test_store_proof_must_equal_the_exact_resolved_payload() -> None:
    service, cohort_reader, _, _, outcome_store = _service()

    async def wrong_publish(
        payload: RealizedOutcomePayload,
        *,
        source: ForecastOutcomePublicationSource,
    ) -> ForecastOutcomeProof:
        assert source.cohort_id == cohort_reader.proof.record.cohort_id
        wrong_source = replace(payload.source_version, value=payload.realized_value + 1.0)
        wrong = replace(
            payload,
            realized_value=payload.realized_value + 1.0,
            source_version=wrong_source,
        )
        record = build_outcome_record(wrong, sealed_at=CUTOFF + timedelta(minutes=1))
        return ForecastOutcomeProof(
            payload=wrong,
            record=record,
            publication=ForecastOutcomePublicationRecord(
                outcome_id=record.outcome_id,
                cohort_id=source.cohort_id,
                forecast_id=source.forecast_id,
                step=source.step,
                published_at=CUTOFF + timedelta(minutes=2),
                publisher_xid=103,
            ),
        )

    outcome_store.publish = wrong_publish  # type: ignore[method-assign]

    with pytest.raises(ForecastOutcomeCollectionError, match="differs"):
        await service.collect(_spec(cohort_reader.proof))
