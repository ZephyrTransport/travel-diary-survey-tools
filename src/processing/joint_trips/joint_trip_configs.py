"""Configuration for joint trip detection.

ALGORITHM DESIGN NOTES:
========================

This module configures joint trip detection, which uses one of two methods:

1. BUFFER METHOD:
   Strict AND across four dimensions: origin distance, destination distance,
   departure time, arrival time. Uses Haversine for spatial matching and
   absolute time differences. All four must pass thresholds.

2. MAHALANOBIS METHOD:
   Statistical distance on 4D vector
   [Δorigin_m, Δdest_m, Δdepart_min, Δarrive_min].
   Supports diagonal covariance (assumes independence) or full covariance matrix
   (captures correlations). The covariance is an assumed noise level stated in
   config, not estimated here.

TEMPORAL OVERLAP:
-----------------
Trips must have overlapping time windows to be joint (travelers physically
together). Pre-filter: max(depart_A, depart_B) <= min(arrive_A, arrive_B)
"""

from collections.abc import Sequence
from typing import Literal

import numpy as np
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from scipy import stats

# The settings each method reads; the other method's are rejected.
_METHOD_SETTINGS = {
    "buffer": ("time_threshold_minutes", "space_threshold_meters"),
    "mahalanobis": ("covariance", "confidence_level"),
}


class JointTripConfig(BaseModel):
    """Configuration for joint trip detection.

    Each method has its own settings: ``buffer`` requires the two thresholds,
    ``mahalanobis`` requires ``covariance`` and ``confidence_level``. A setting
    of the other method is rejected, so a config can't look like it tunes
    something it doesn't.

    Attributes:
        method: Detection method ('buffer' for strict thresholds,
            'mahalanobis' for statistical distance)
        time_threshold_minutes: Maximum time difference for buffer method
        space_threshold_meters: Maximum spatial distance for buffer method
        covariance: Diagonal variances (list of 4) or full covariance matrix
            (4x4 list of lists) in units [m^2, m^2, min^2, min^2]. Required
            for mahalanobis. The empirical variance of BATS
            2023 household joint trips is about [7000, 7000, 20, 20]: origin
            ~84m std (7142 m² var), dest ~84m (7089 m²), depart ~4.7min
            (22 min²), arrive ~4.6min (21 min²).
        confidence_level: Strictness of the mahalanobis cutoff. Higher values
            keep fewer pairs. It is not the share of joint pairs kept; see
            ``get_distance_threshold``.
        log_discrepancies: If True, log DEBUG-level details when detected
            joint trip size differs from reported num_travelers field
    """

    model_config = ConfigDict(extra="forbid")

    method: Literal["buffer", "mahalanobis"] = Field(
        description=("Detection method: buffer (strict) or mahalanobis (statistical)"),
    )

    time_threshold_minutes: float | None = Field(
        default=None,
        ge=0,
        description="Maximum time difference in minutes (buffer method only, required there)",
    )

    space_threshold_meters: float | None = Field(
        default=None,
        ge=0,
        description="Maximum spatial distance in meters (buffer method only, required there)",
    )

    covariance: Sequence[float | int] | Sequence[Sequence[float | int]] | None = Field(
        default=None,
        description=(
            "Diagonal (4 values) or full (4x4) covariance matrix in units "
            "[origin_m^2, dest_m^2, depart_min^2, arrive_min^2] (mahalanobis "
            "method only, required there). The empirical variance of BATS 2023 "
            "household joint trips is about diagonal [7000, 7000, 20, 20]: "
            "origin ~84m std dev (7142 m² var), dest ~84m (7089 m²), "
            "depart ~4.7min (22 min²), arrive ~4.6min (21 min²)."
        ),
    )

    confidence_level: float | None = Field(
        default=None,
        gt=0,
        lt=1,
        description=(
            "Strictness of the mahalanobis cutoff (mahalanobis method only, required there). "
            "Higher = stricter. It reads backward from the usual meaning of "
            "confidence and is not the share of joint pairs kept. See "
            "get_distance_threshold."
        ),
    )

    log_discrepancies: bool = Field(
        default=False,
        description=(
            "If True, log DEBUG-level details when detected joint trip size "
            "differs from reported num_travelers"
        ),
    )

    @staticmethod
    def _validate_diagonal_covariance(v: Sequence[float]) -> None:
        """Validate diagonal covariance vector."""
        if len(v) != 4:  # noqa: PLR2004
            msg = f"Diagonal covariance must have 4 values, got {len(v)}"
            raise ValueError(msg)
        if any(x <= 0 for x in v):
            msg = "All diagonal covariance values must be positive"
            raise ValueError(msg)

    @staticmethod
    def _validate_full_covariance(v: Sequence[Sequence[float]]) -> None:
        """Validate full covariance matrix."""
        if len(v) != 4:  # noqa: PLR2004
            msg = f"Full covariance must be 4x4, got {len(v)} rows"
            raise ValueError(msg)
        if any(len(row) != 4 for row in v):  # noqa: PLR2004
            msg = "Full covariance must be 4x4 (all rows length 4)"
            raise ValueError(msg)

        # Check symmetry
        for i in range(4):
            for j in range(i + 1, 4):
                if abs(v[i][j] - v[j][i]) > 1e-6:  # noqa: PLR2004
                    msg = "Covariance matrix must be symmetric"
                    raise ValueError(msg)

        # Check positive definiteness (all eigenvalues > 0)
        eigenvalues = np.linalg.eigvals(np.array(v))
        if any(eigenvalues <= 0):
            msg = "Covariance matrix must be positive definite (all eigenvalues > 0)"
            raise ValueError(msg)

    @field_validator("covariance")
    @classmethod
    def validate_covariance(
        cls, v: Sequence[float] | Sequence[Sequence[float]] | None
    ) -> Sequence[float] | Sequence[Sequence[float]] | None:
        """Validate covariance shape and values."""
        if v is None:
            return None

        if not isinstance(v, list) or len(v) == 0:
            msg = "Covariance must be a non-empty list"
            raise ValueError(msg)

        # Check if diagonal (1D list) or full (2D list)
        if isinstance(v[0], (int, float)):
            # Type narrow: v is Sequence[float]
            cls._validate_diagonal_covariance(v)  # type: ignore[arg-type]
        elif isinstance(v[0], list):
            # Type narrow: v is Sequence[Sequence[float]]
            cls._validate_full_covariance(v)  # type: ignore[arg-type]
        else:
            msg = "Covariance must be list of floats or list of lists"
            raise TypeError(msg)

        return v

    @model_validator(mode="after")
    def validate_method_params(self) -> "JointTripConfig":
        """Require the chosen method's settings and reject the other method's."""
        own = _METHOD_SETTINGS[self.method]
        other = [s for m, names in _METHOD_SETTINGS.items() if m != self.method for s in names]
        missing = [s for s in own if getattr(self, s) is None]
        if missing:
            msg = f"method {self.method} requires {', '.join(missing)}"
            if "covariance" in missing:
                msg += ", e.g. the BATS 2023 empirical covariance [7000, 7000, 20, 20]"
            raise ValueError(msg)
        stray = [s for s in other if s in self.model_fields_set]
        if stray:
            msg = f"method {self.method} does not use {', '.join(stray)}; remove it"
            raise ValueError(msg)
        return self

    def get_distance_threshold(self) -> float:
        """Convert confidence_level to the cutoff the mahalanobis filter uses.

        Returns ``chi2.ppf(1 - confidence_level, df=4)``. Two things make
        this read backward:

        - The level is inverted. Usually "90% confidence" would keep 90% of
          true matches; here a higher level gives a lower cutoff.
        - ``apply_mahalanobis_filter`` compares the cutoff to the distance
          ``d``, while the chi-squared distribution describes ``d**2``.

        If the covariance were exactly the noise between true joint pairs, the
        share of them kept would be ``chi2.cdf(cutoff**2, df=4)``:

        | confidence_level | cutoff | true matches kept |
        |---|---|---|
        | 0.95 | 0.711 | ~3% |
        | 0.90 | 1.064 | ~11% |
        | 0.75 | 1.923 | ~55% |
        | 0.50 | 3.357 | ~98% |

        A covariance wider than that noise loosens the cutoff in real terms.
        On BATS 2023, covariance ``[10000, 10000, 100, 100]`` at 0.90 found
        35,021 joint trips against 35,628 from the buffer method (15 min /
        100 m), with 98% of matched pairs in common.

        Returns:
            The cutoff on the Mahalanobis distance ``d``.
        """
        if self.method != "mahalanobis":
            return 0.0  # Not used for buffer method
        # Inverted on purpose: higher confidence_level, lower cutoff.
        return float(stats.chi2.ppf(1 - self.confidence_level, df=4))
