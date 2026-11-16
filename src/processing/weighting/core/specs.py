"""Data structures for the weighting pipeline.

Single source of truth for all dataclasses and named tuples used across
data preparation, balancing, validation, and diagnostics.

Kept as a leaf module with minimal imports to avoid circular dependencies.
"""

import logging
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import NamedTuple

import numpy as np
import polars as pl
from pydantic import BaseModel, ConfigDict, Field

from processing.weighting.data_prep.crosswalk import GeographyConfig

logger = logging.getLogger(__name__)

# ==============================================================================
# Data Preparation
# ==============================================================================


@dataclass
class ControlSpec:
    """Specification for a single weighting control.

    Attributes:
        name: Registry name (must exist in ``CONTROLS``).
        importance: Explicit importance weight for the balancer.  ``None`` means use
            ``default_importance``, or the MOE-derived value when
            ``moe_based_importance`` is enabled.
        dimensions: For cross-tab controls only: list of dimension control names
            (e.g. ``["h_size", "h_income"]``).  ``None`` for standard controls.
        merges: For cross-tab controls only: per-dimension merge specs applied
            at registration time.  ``None`` for standard controls.
    """

    name: str
    importance: float | None = None
    dimensions: list[str] | None = None
    merges: dict | None = None


@dataclass
class ControlTotals:
    """Result of PUMS control-total aggregation.

    Attributes:
        totals: Tidy frame with columns: [zone_group_id, control_name, category, target_total]
        pums_hh_count: Total PUMS housing unit records (before weighting).
        pums_person_count: Total PUMS person records.
        zone_group_ids: Unique geography IDs in the totals.
    """

    totals: pl.DataFrame
    pums_hh_count: int
    pums_person_count: int
    zone_group_ids: list[str]


# ---------------------------------------------------------------------------
# Incidence Handler
# ---------------------------------------------------------------------------


@dataclass
class IncidenceBundle:
    """All intermediate and final products of incidence-table construction.

    Built by
    [`build_incidence_table`][processing.weighting.data_prep.incidence.build_incidence_table]
    and available for downstream consumers (balancer, fractional imputation, diagnostics).

    Attributes:
        incidence: Combined household-level incidence table — one row per HH with
            ``{ctrl}__{member}`` columns for *all* controls (HH + person
            aggregated).  This is what the balancer operates on.
        person_pivot: Per-person 0/1 indicator table — one row per person with
            ``{p_ctrl}__{member}`` columns.  Useful for person-level
            classification (fractional imputation of null persons).
        household_pivot: HH-only 0/1 indicator table — one row per HH with
            ``{h_ctrl}__{member}`` columns.  Useful for HH-level
            classification without person-count noise.
    """

    incidence: pl.DataFrame
    person_pivot: pl.DataFrame
    household_pivot: pl.DataFrame

    def filter_households(self, hh_ids: list | pl.Series) -> "IncidenceBundle":
        """Return a new bundle keeping only the given household IDs."""
        keep = pl.col("hh_id").is_in(hh_ids)
        return IncidenceBundle(
            incidence=self.incidence.filter(keep),
            person_pivot=self.person_pivot.filter(keep),
            household_pivot=self.household_pivot.filter(keep),
        )


# ---------------------------------------------------------------------------
# Imputation Metadata
# ---------------------------------------------------------------------------


@dataclass
class ImputationSummary:
    """Per-control fractional seed imputation metadata for diagnostics.

    Describes what [`fill_null_incidence`]
    [processing.weighting.data_prep.fractional_impute.fill_null_incidence]
    filled in the seed incidence matrix -- never the survey ``imputation``
    step, which writes discrete values to canonical data and is reported
    separately.

    One entry per control target, regardless of whether any filling was
    needed.  Controls with no nulls have ``n_null == 0`` and ``None``
    for the RF metrics.
    """

    control: str
    level: str  # "person" | "household"
    n_total: int
    n_null: int
    log_loss: float | None = None
    f1_macro: float | None = None


# ---------------------------------------------------------------------------
# PUMS Configuration
# ---------------------------------------------------------------------------
class HouseholdIncome(StrEnum):
    """Whose income the survey's household income counts, so the PUMS target counts the same.

    PUMS household income (``HINCP``) counts every member of the household.
    """

    # Every member: HINCP as it is.
    ALL_MEMBERS = "all_members"
    # Related members only: each nonrelative's personal income is taken out of HINCP.
    RELATED_MEMBERS = "related_members"


class PumsWeights(StrEnum):
    """Which PUMS weight each level of target sums.

    The ACS weights persons and households separately, so person weights summed
    over a household's members need not equal its household weight times its size.
    """

    # Persons take their household's weight: the two kinds of target agree.
    HOUSEHOLD = "household"
    # Households take the mean of their members' person weights: they agree the other way.
    PERSON = "person"
    # Each level keeps its own weight, and the fit shows where the two disagree.
    INDEPENDENT = "independent"


# ==============================================================================
# Balancing Configuration
# ==============================================================================


@dataclass
class MergeSpec:
    """Category-merge specification for one control.

    Supports both 1D (standard) and N-D (cross-tab) merges.

    Attributes:
        control: Registry name (e.g. ``"p_employment"`` or ``"h_income_x_size"``).
        groups: Merge specifications. Each key is a merged label, each value is either:

            - **1D merge** (list): Base member names to combine.
              E.g. ``{"employed": ["employed_full", "employed_part"]}``

            - **N-D merge** (dict): Dimension name -> member names for cross-tabs.
              E.g. ``{"low_income_large": {"h_income": ["inc_under_25k", "inc_25k_to_50k"],
              "h_size": ["size_4", "size_5"]}}``

              The N-D merge creates a single merged cell from the cartesian product
              of all specified dimension members.
        zones: If set, apply this merge only to these geo IDs.
            ``None`` means apply globally (all zones).

    Example:
        1D merge:

    >>> MergeSpec(
    ...     control="p_employment",
    ...     groups={"employed": ["employed_full", "employed_part"]},
    ... )

    N-D merge for cross-tab:

    >>> MergeSpec(
    ...     control="h_income_x_size",
    ...     groups={
    ...         "low_income_large": {
    ...             "h_income": ["inc_under_25k", "inc_25k_to_50k"],
    ...             "h_size": ["size_4", "size_5", "size_6"],
    ...         }
    ...     },
    ... )
    """

    control: str
    groups: dict[str, list[str] | dict[str, list[str]]]
    zones: list[str] | None = None


class CensusConfig(BaseModel):
    """Where the Census controls come from: the state, the PUMS year, and optional local files."""

    model_config = ConfigDict(extra="forbid")

    state_fips: str = Field(description="Two-digit state FIPS code, e.g. '06'.")
    pums_year: int = Field(description="ACS PUMS 1-year vintage the controls are built from.")
    pums_weights: PumsWeights = Field(
        description=(
            "Which PUMS weight the targets sum: `household` (persons take their "
            "household's weight), `person` (households take the mean of their members' "
            "person weights) or `independent` (each level keeps its own)."
        ),
    )
    household_income: HouseholdIncome = Field(
        description=(
            "Whose income the survey's household income question covers: `all_members`, "
            "or `related_members` when the questionnaire told respondents to leave out "
            "roommates and other nonrelatives. The PUMS income target is built to match."
        ),
    )
    census_dir: str = Field(
        description=(
            "Folder of the census data the weighting reads: the official PUMS files "
            "in `pums_<year>/`, and Census geography and block counts in "
            "`census_<vintage>/`. A missing file is downloaded there on first use."
        ),
    )


class ReferenceWeightSetConfig(BaseModel):
    """One weight set that arrived with the survey, named for the report's comparer."""

    model_config = ConfigDict(extra="forbid")

    label: str = Field(description="How the report names the set.")
    columns: dict[str, str] = Field(
        description="Table -> the column holding this set's weight on it."
    )


class DiagnosticsConfig(BaseModel):
    """The weighting report: where it goes, what it compares, and the grid search."""

    model_config = ConfigDict(extra="forbid")

    output_path: str = Field(description="Where the HTML report is written.")
    expansion_factor_grid: list[float] | None = Field(
        description=(
            "max_expansion_factor values to re-run balancing at, charting fit "
            "against weight quality; null to skip the grid search."
        ),
    )
    compare_weights: dict[str, ReferenceWeightSetConfig] | None = Field(
        default=None,
        description="Weight sets supplied with the survey, compared against this run's fits.",
    )


class BalancingConfig(BaseModel):
    """Maximum-entropy balancer configuration, and how hard each control pulls.

    Controls Newton-Raphson iteration bounds, weight expansion limits, parallel
    execution and the importance given to controls without their own. Consumed by
    [`balance_weights`][processing.weighting.balancing.balancer.balance_weights],
    [`grid_search_expansion_factor`][processing.weighting.balancing.balancer.grid_search_expansion_factor],
    and the pipeline orchestrator.
    """

    model_config = ConfigDict(extra="forbid")

    max_expansion_factor: float = Field(
        description="Upper bound on a weight, as a multiple of its base weight."
    )
    min_expansion_factor: float = Field(
        description="Lower bound on a weight, as a multiple of its base weight."
    )
    max_base_weight_gap: float = Field(
        ge=0,
        description=(
            "Stop before balancing if a zone's base weights add up to more than this "
            "share above or below its household target. The expansion factor limits "
            "are multiples of the base weights, so they only mean what they say when "
            "the base weights already add up to the target."
        ),
    )
    min_weight: float | None = Field(description="Absolute weight floor; null for none.")
    max_weight: float | None = Field(
        default=None, description="Absolute weight ceiling; none unless given."
    )
    max_iterations: int = Field(description="Newton-Raphson iteration limit per zone.")
    n_workers: int = Field(description="Zones (or grid points) balanced in parallel.")
    moe_based_importance: bool = Field(
        description="Derive each control's importance from its PUMS replicate-weight MOE."
    )
    default_importance: float = Field(
        description="Importance for a control without its own, when not MOE-derived."
    )
    min_survey_share_ratio: float = Field(
        ge=0,
        description=(
            "Stop before balancing if any group's share of its area's survey records, "
            "divided by its share of the area's PUMS, is below this. Such a group cannot "
            "be weighted up to its target; merge it, or lower this to accept the miss."
        ),
    )


def resolve_fitted_profiles(
    weight_profiles: tuple[str, ...] | list[str] | None,
) -> tuple[str, ...]:
    """The profiles to weight: one entry per weight column set to be written.

    Shared by both weighting entry points so they cannot disagree about what a
    profile list is supposed to mean.

    Args:
        weight_profiles: Profiles to weight, each writing its own suffixed
            column set. A single profile is a list of one.

    Returns:
        One entry per profile, in the order given.

    Raises:
        ValueError: If no profile is given, or a profile is named twice.
    """
    profiles = tuple(weight_profiles or ())
    if not profiles:
        msg = (
            "The weighting needs a universe: name weight_profiles, one weight set "
            "per profile (a single profile is a list of one)."
        )
        raise ValueError(msg)

    duplicates = sorted({p for p in profiles if profiles.count(p) > 1})
    if duplicates:
        msg = f"weight_profiles names the same profile twice: {duplicates}"
        raise ValueError(msg)

    return profiles


@dataclass
class WeightingConfig:
    """Top-level configuration for the weighting pipeline.

    Groups the parameters that are *not* already covered by
    [`ControlRegistryConfig`][processing.weighting.core.specs.ControlRegistryConfig] or
    [`BalancingConfig`][processing.weighting.core.specs.BalancingConfig]
    — primarily Census/geography settings and pipeline plumbing.

    ``weight_profiles`` names the usability profiles that decide who carries
    weight, one fit each. It is required: with several profiles stamped there is
    no defensible default, and picking silently would weight a different
    universe than the formatters emit.
    """

    geography: GeographyConfig
    state_fips: str
    pums_year: int
    pums_weights: PumsWeights
    household_income: HouseholdIncome
    use_imputed_values: bool
    census_dir: Path
    weight_profiles: tuple[str, ...] = ()
    max_unplaceable_share: float = 0.01
    sample_plan: str | None = None
    expansion_factor_grid: list[float] | None = None
    strict_survey_nulls: bool = False

    def __post_init__(self) -> None:
        """Reject a weighting universe that is unstated or names a profile twice."""
        self.weight_profiles = resolve_fitted_profiles(self.weight_profiles)


@dataclass
class ControlRegistryConfig:
    """Parsed control definitions, 1-D merge specs, and derived target names.

    Cross-tab merges are not here: they are applied when the cross-tab is
    registered, from ``ControlSpec.merges``.

    Built via [`from_yaml`][processing.weighting.core.specs.ControlRegistryConfig.from_yaml]
    from the YAML ``controls`` block.
    """

    specs: list[ControlSpec]
    target_names: list[str]
    merges_1d: list[MergeSpec]

    @classmethod
    def from_yaml(cls, controls: list[dict]) -> "ControlRegistryConfig":
        """Parse the YAML controls block into a config object."""
        _spec_keys = ("name", "importance", "dimensions", "merges")
        specs = [ControlSpec(**{k: v for k, v in c.items() if k in _spec_keys}) for c in controls]
        target_names = [s.name for s in specs]

        merges_1d: list[MergeSpec] = []

        def lower(groups: dict[str, list[str]]) -> dict[str, list[str]]:
            # Group names are lower case downstream, so a label written AAPI is aapi
            return {
                label.lower(): [m.lower() for m in members] for label, members in groups.items()
            }

        for c in controls:
            global_labels: set[str] = set()
            if c.get("merge"):
                merges_1d.append(MergeSpec(control=c["name"], groups=lower(c["merge"])))
                global_labels = set(lower(c["merge"]))
            for zone_id, raw_groups in c.get("zone_merges", {}).items():
                groups = lower(raw_groups)
                overlap = set(groups.keys()) & global_labels
                if overlap:
                    msg = (
                        f"Control '{c['name']}' zone_merges for '{zone_id}' redefines global "
                        f"merge label(s) {sorted(overlap)} — the zone merge is redundant "
                        "because the global merge already applies everywhere."
                    )
                    raise ValueError(msg)
                merges_1d.append(MergeSpec(control=c["name"], groups=groups, zones=[zone_id]))

        return cls(
            specs=specs,
            target_names=target_names,
            merges_1d=merges_1d,
        )

    @property
    def importance_overrides(self) -> dict[str, float]:
        """Explicit importance overrides from YAML."""
        return {s.name: s.importance for s in self.specs if s.importance is not None}


@dataclass
class GridPoint:
    """Aggregate metrics for one expansion-factor grid point."""

    max_expansion_factor: float
    converged_zones: int
    total_zones: int
    mape: float
    p90: float
    max_error: float
    cv: float
    ess_pct: float


@dataclass
class SamplePlan:
    """Stratified sampling plan mapping Census block groups to segments.

    Each row represents a Census block group.  Block groups that share
    the same ``sample_segment`` form one stratum, split by zone group
    for initial-weight computation:
    ``base_weight = cell_households / cell_n_responses``.

    Household counts come from the decennial Census, grown to the PUMS year
    ([`grow_block_households`][processing.weighting.balancing.base_weights.grow_block_households]),
    not from this table.

    Attributes:
        strata: Required columns:

            * ``bg_geo_id``  (str) — 12-character Census block-group FIPS code.
            * ``sample_segment`` (str) — sampling-stratum label.  All block
              groups sharing a segment, within one zone, get the same base weight.
    """

    strata: pl.DataFrame

    # -- validation --
    _REQUIRED_COLS: tuple[str, ...] = field(
        default=("bg_geo_id", "sample_segment"),
        init=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        """Validate that *strata* has the required columns."""
        missing = [c for c in self._REQUIRED_COLS if c not in self.strata.columns]
        if missing:
            msg = f"SamplePlan.strata missing required columns: {missing}"
            raise ValueError(msg)


# ==============================================================================
# Balancing Internals
# ==============================================================================


class ZoneStatus(NamedTuple):
    """Per-zone convergence diagnostics."""

    zone_group_id: str
    converged: bool
    iterations: int
    delta: float
    max_gamma_diff: float


class ZoneInput(NamedTuple):
    """Pre-built numpy arrays for a single geography zone."""

    hh_ids: pl.Series
    incidence: np.ndarray
    initial: np.ndarray
    lb: np.ndarray
    ub: np.ndarray
    targets: np.ndarray
    importance: np.ndarray
    master_idx: int
    max_iterations: int
    zone_group_id: str
    verbose: bool = True


class ZoneResult(NamedTuple):
    """Balancer output for a single geography zone."""

    weights: pl.DataFrame
    status: ZoneStatus


# ==============================================================================
# Per-profile fits
# ==============================================================================


class GeographyCoverage(NamedTuple):
    """How much of one profile's universe the target zones can place.

    A household the target zones do not contain belongs to no zone group,
    so no fit can give it a weight. That is a bound on what the weighting can
    answer, not a defect in the household, and it is counted so a reader sees the
    bound rather than inferring it from nulls.
    """

    profile: str | None
    n_universe: int
    n_placed: int

    @property
    def n_unplaceable(self) -> int:
        """Households in the profile that the target zones cannot place."""
        return self.n_universe - self.n_placed

    @property
    def unplaceable_share(self) -> float:
        """Unplaceable households as a share of the profile's universe."""
        return self.n_unplaceable / self.n_universe if self.n_universe else 0.0


@dataclass
class ProfileFit:
    """Everything one balancing run produced.

    A run may fit several profiles over the same survey. Holding each fit's
    products here rather than on the pipeline keeps them from overwriting one
    another -- otherwise the last profile's seed silently becomes the one the
    diagnostics describe and the checks read.

    Attributes:
        profile: Usability profile fitted. Suffixes every weight column this
            fit writes.
        usability_flag_col: The column that gated the seed and will gate the
            propagation: ``usable_<profile>``, or ``survey_complete`` itself.
        seed_incidence: The seed this fit was solved from, carrying its own
            ``base_weight`` once balancing has run.
        pre_imputation_incidence: The same seed before null imputation, for the
            diagnostics report.
        imputation_summary: Per-control imputation metadata for this seed.
        coverage: How much of the profile's universe was in a target zone.
        weights: Balancer output, keyed by ``hh_id``, carrying the *base* weight
            column name -- the suffix is applied where it is joined on.
        statuses: Per-zone convergence diagnostics.
        grid_results: Expansion-factor grid points, when a grid was configured.
    """

    profile: str
    usability_flag_col: str
    seed_incidence: pl.DataFrame
    pre_imputation_incidence: pl.DataFrame
    imputation_summary: list[ImputationSummary]
    coverage: GeographyCoverage
    weights: pl.DataFrame | None = None
    statuses: list[ZoneStatus] = field(default_factory=list)
    grid_results: list[GridPoint] | None = None

    @property
    def unconverged_zones(self) -> int:
        """Zones this fit failed to converge."""
        return sum(not s.converged for s in self.statuses)
