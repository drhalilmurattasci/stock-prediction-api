"""ORM model registry imported by Alembic."""

from app.db.models.adjustment_factors import (
    AdjustmentFactorEntry,
    AdjustmentFactorSetAvailability,
    AdjustmentFactorSetRecord,
)
from app.db.models.bars import Bar, BarRevision, BarVersionAvailability
from app.db.models.corporate_actions import (
    CorporateActionCollection,
    CorporateActionCollectionAvailability,
    CorporateActionCollectionMember,
    CorporateActionVersion,
)
from app.db.models.forecast_calibration import (
    ForecastFittedCalibrationSet,
    ForecastHeldoutCoverageRelease,
    ForecastHeldoutCoverageReleaseAvailability,
    ForecastHeldoutCoverageReleaseBucket,
)
from app.db.models.forecast_evidence import (
    ForecastOutcomeCohortAvailability,
    ForecastOutcomeCohortManifest,
    ForecastOutcomeCohortMember,
    ForecastOutcomeResolutionPolicyRegistration,
    ForecastRealizedOutcome,
    ForecastRealizedOutcomePublication,
)
from app.db.models.forecast_snapshots import ForecastInputSnapshot
from app.db.models.predictions import ForecastRun
from app.db.models.vendor_acquisition import VendorAcquisitionCampaignAnchor

__all__ = [
    "AdjustmentFactorEntry",
    "AdjustmentFactorSetAvailability",
    "AdjustmentFactorSetRecord",
    "Bar",
    "BarRevision",
    "BarVersionAvailability",
    "CorporateActionCollection",
    "CorporateActionCollectionAvailability",
    "CorporateActionCollectionMember",
    "CorporateActionVersion",
    "ForecastInputSnapshot",
    "ForecastFittedCalibrationSet",
    "ForecastHeldoutCoverageRelease",
    "ForecastHeldoutCoverageReleaseAvailability",
    "ForecastHeldoutCoverageReleaseBucket",
    "ForecastOutcomeCohortAvailability",
    "ForecastOutcomeCohortManifest",
    "ForecastOutcomeCohortMember",
    "ForecastOutcomeResolutionPolicyRegistration",
    "ForecastRealizedOutcome",
    "ForecastRealizedOutcomePublication",
    "ForecastRun",
    "VendorAcquisitionCampaignAnchor",
]
