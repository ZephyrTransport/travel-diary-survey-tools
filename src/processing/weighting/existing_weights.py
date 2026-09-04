"""Attach pre-computed weights to survey data tables.

This module provides the ``add_existing_weights`` pipeline step, which loads
weight CSV files and joins them to the corresponding canonical tables.
Optionally, it can derive missing weights by propagating values through the
survey hierarchy.

# Core algorithm

## Phase 1 -- Load and Join Weights

1. For each provided weight config:

    1. Validate the config key against allowed table types.
    2. Load the weight CSV from ``weight_path``.
    3. Validate required ID and weight columns exist.
    4. Handle ID column name mismatches (rename if needed).
    5. Left-join weights to the table on the ID column.

## Phase 2 -- Derive Missing Weights (when ``derive_missing_weights=True``)

1. *Hierarchical carry-forward* for household → person → day → unlinked_trip:
   validate parent has weights, then join parent weight to child via FK.
2. *Aggregated weights* for linked_trip and tour: compute mean weight per group
   (excluding nulls and zeros), then left-join. The joint levels **sum** instead
   -- see [`Agg`][processing.weighting.core.hierarchy.Agg].

Gap detection: raises an error if a middle-tier weight is missing
(e.g. household + trip weights provided but person/day weights are not).
"""

import logging
from pathlib import Path

import polars as pl
from pydantic import BaseModel, Field, field_validator, model_validator

from data_canon.core.dataclass import CanonicalData
from pipeline.decoration import step
from processing.weighting.core.hierarchy import (
    LEVELS,
    WEIGHT_CONFIG_MAPPING,
    describe_weight_columns,
    weight_col_for,
    weight_columns_for,
)
from processing.weighting.core.propagation import (
    collect_tables,
    distribute_within_scope,
    drop_unsuffixed_weights,
    is_usable,
    propagate_weights,
)
from processing.weighting.core.specs import resolve_fitted_profiles

logger = logging.getLogger(__name__)

# Rescale factors within this distance of 1.0 are floating-point drift, not work.
SCALE_TOL = 1e-9


class ExistingWeightConfig(BaseModel):
    """Configuration for a single weight file.

    Most fields use sensible defaults - typically you only need to provide weight_path.
    Override weight_id_col only if your weight file uses different ID column names.
    Override weight_col only if your weight file uses different weight column names.

    Examples:
        Basic usage with defaults:
        >>> WeightConfig(config_key="hh_weight", weight_path="hh_weights.csv")
        # Uses: weight_id_col="hh_id", weight_col="hh_weight"

        Override weight column name:
        >>> WeightConfig(
        ...     config_key="person_weight",
        ...     weight_path="person_weights.csv",
        ...     weight_col="final_weight"  # Weight file uses "final_weight" not "person_weight"
        ... )

        Override ID column name (external weight file with non-canonical IDs):
        >>> WeightConfig(
        ...     config_key="unlinked_trip_weight",
        ...     weight_path="trip_weights.csv",
        ...     weight_id_col="trip_id",  # Weight file uses "trip_id" not "unlinked_trip_id"
        ...     weight_col="trip_weight" # Weight file uses "trip_weight" not "unlinked_trip_weight"
        ... )

    Attributes:
        config_key: Key identifying the weight type (e.g., 'hh_weight')
        weight_path: Path to the CSV file containing weights
        weight_id_col: ID column name in the weight file. Defaults to canonical table ID.
        weight_col: Weight column name in the weight file. Defaults to canonical weight column.
    """

    config_key: str
    weight_path: Path
    weight_id_col: str | None = Field(
        default=None, description="Defaults to canonical table ID if not provided"
    )
    weight_col: str | None = Field(
        default=None, description="Defaults to canonical weight column if not provided"
    )
    keep_name: bool = Field(
        default=False,
        description="If True, keeps original weight_col name instead of renaming to canonical",
    )

    @field_validator("config_key")
    @classmethod
    def validate_config_key(cls, v: str) -> str:
        """Validate that config_key is one of the allowed values."""
        if v not in WEIGHT_CONFIG_MAPPING:
            msg = (
                f"Invalid weight config key: {v}. "
                f"Must be one of {list(WEIGHT_CONFIG_MAPPING.keys())}"
            )
            raise ValueError(msg)
        return v

    @field_validator("weight_path")
    @classmethod
    def validate_path_exists(cls, v: Path) -> Path:
        """Validate that the weight file exists."""
        if not v.exists():
            msg = f"Weight file does not exist: {v}"
            raise FileNotFoundError(msg)
        return v

    @model_validator(mode="after")
    def set_defaults_from_mapping(self) -> "ExistingWeightConfig":
        """Set default values for weight_id_col and weight_col from the mapping."""
        _, table_id_col, canonical_weight_col = WEIGHT_CONFIG_MAPPING[self.config_key]

        if self.weight_id_col is None:
            self.weight_id_col = table_id_col
        if self.weight_col is None:
            self.weight_col = canonical_weight_col

        return self

    @property
    def table_name(self) -> str:
        """Get the table name for this weight config."""
        return WEIGHT_CONFIG_MAPPING[self.config_key][0]

    @property
    def table_id_col(self) -> str:
        """Get the canonical table ID column."""
        return WEIGHT_CONFIG_MAPPING[self.config_key][1]

    @property
    def canonical_weight_col(self) -> str:
        """Get the canonical weight column name for this table."""
        return WEIGHT_CONFIG_MAPPING[self.config_key][2]

    def joined_weight_col(self, profile: str | None = None) -> str:
        """The column the supplied weight actually lands in under *profile*.

        ``keep_name`` suppresses the rename to the canonical name, so under it
        the vendor's own column is the only record of where the weight went --
        which is why it cannot be combined with profiles, there being one column
        and several universes to put in it.
        """
        if self.keep_name and self.weight_col:
            return self.weight_col
        return weight_col_for(self.canonical_weight_col, profile)

    def validate_columns(self, weight_df: pl.DataFrame, table_df: pl.DataFrame) -> None:
        """Validate that required columns exist in weight file and table.

        Args:
            weight_df: The weight DataFrame loaded from weight_path
            table_df: The survey data table to join weights to

        Raises:
            ValueError: If required columns are missing
        """
        # Check weight file has required columns
        if self.weight_id_col not in weight_df.columns:
            msg = f"Weight file {self.weight_path} missing required ID column: {self.weight_id_col}"
            raise ValueError(msg)

        if self.weight_col not in weight_df.columns:
            msg = (
                f"Weight file {self.weight_path} missing required weight column: {self.weight_col}"
            )
            raise ValueError(msg)

        # Check table has required ID column
        if self.table_id_col not in table_df.columns:
            msg = f"Table {self.table_name} missing required ID column: {self.table_id_col}"
            raise ValueError(msg)


def _apply_usability(
    tables: dict[str, pl.DataFrame | None],
    has_weight: dict[str, str],
    *,
    usability_flag_col: str,
) -> None:
    """Redistribute supplied weights onto usable records, preserving each total.

    Unlike the computed path, the vendor's anchor cannot be re-balanced: their
    weights already sum to their population estimate, so dropping records must
    not shrink that total. Each supplied weight is first conserved within its
    declared scope ([`distribute_within_scope`]
    [processing.weighting.core.propagation.distribute_within_scope]);
    whatever that leaves stranded — scopes with no usable record at all — is
    then closed with one table-wide factor. Modifies *tables* in place.

    Args:
        tables: Mutable dict of table_name → DataFrame (or None).
        has_weight: Dict of table_name → weight column name.
        usability_flag_col: Boolean column deciding which records may carry weight
            -- a usability profile stamped upstream, or ``complete`` to weight the
            whole valid survey.
    """
    for table_name, weight_col in has_weight.items():
        df = tables.get(table_name)
        if df is None or usability_flag_col not in df.columns:
            continue

        supplied_total = df.select(pl.col(weight_col).sum()).item() or 0.0
        scope = LEVELS[table_name].scope

        if scope is None:
            # The anchor has no scope to spread within; zero and rescale below.
            df = df.with_columns(
                pl.when(is_usable(usability_flag_col))
                .then(pl.col(weight_col))
                .otherwise(0.0)
                .alias(weight_col)
            )
        else:
            # Raises when the scope column is absent -- quietly conserving the
            # weight over some other grouping would change the numbers silently.
            df = distribute_within_scope(
                df,
                weight_col=weight_col,
                scope=scope,
                usability_flag_col=usability_flag_col,
                table=table_name,
            )

        placed_total = df.select(pl.col(weight_col).sum()).item() or 0.0
        if placed_total > 0 and supplied_total > 0:
            scale = supplied_total / placed_total
            if abs(scale - 1.0) > SCALE_TOL:
                df = df.with_columns((pl.col(weight_col) * scale).alias(weight_col))
                logger.info(
                    "%s: scaled %s by %.4f to preserve the supplied total (%.1f). This covers "
                    "the weight of parents that kept no usable record, which has no sibling "
                    "to receive it.",
                    table_name,
                    weight_col,
                    scale,
                    supplied_total,
                )
        elif supplied_total > 0:
            logger.warning(
                "%s: no usable record carries weight; the supplied total (%.1f) is lost",
                table_name,
                supplied_total,
            )

        tables[table_name] = df


def _attach_supplied(
    tables: dict[str, pl.DataFrame | None],
    supplied: dict[str, pl.DataFrame],
    configs: dict[str, ExistingWeightConfig],
    profile: str | None,
) -> dict[str, str]:
    """Join each supplied weight under its column for *profile*, in place.

    Returns:
        table -> the weight column that is actually there, which is what every
        later step reads the weight by.
    """
    by_table = {cfg.table_name: cfg for cfg in configs.values()}
    has_weight: dict[str, str] = {}

    for table_name, weight_df in supplied.items():
        df = tables.get(table_name)
        if df is None:
            continue
        cfg = by_table[table_name]
        joined_col = cfg.joined_weight_col(profile)

        # Drop the target column if it is already there, so a re-run replaces
        # rather than duplicates it.
        if joined_col in df.columns:
            df = df.drop(joined_col)

        renamed = weight_df.rename({cfg.canonical_weight_col: joined_col})
        tables[table_name] = df.join(renamed, on=cfg.table_id_col, how="left")
        has_weight[table_name] = joined_col
        logger.info("Joined %s to %s on %s", joined_col, table_name, cfg.table_id_col)

    return has_weight


def _warn_null_weights(tables: dict[str, pl.DataFrame | None], profile: str | None) -> None:
    """Report weights that came out null -- a record no supplied total covered."""
    for table, weight in weight_columns_for(profile).items():
        df = tables.get(table)
        if df is None or weight not in df.columns:
            continue
        null_count = df.select(pl.col(weight).is_null().sum()).item()
        if null_count:
            logger.warning(
                "Table %s has %d / %d NULL values in weight column %s",
                table,
                null_count,
                len(df),
                weight,
            )


@step()
def add_existing_weights(  # noqa: C901, PLR0912, PLR0913, PLR0915
    weights: dict[str, ExistingWeightConfig | dict],
    usability_flag_col: str | None = None,
    weight_profiles: list[str] | None = None,
    derive_missing_weights: bool = False,
    canonical_data: CanonicalData | None = None,
    households: pl.DataFrame | None = None,
    persons: pl.DataFrame | None = None,
    days: pl.DataFrame | None = None,
    unlinked_trips: pl.DataFrame | None = None,
    linked_trips: pl.DataFrame | None = None,
    tours: pl.DataFrame | None = None,
    joint_trips: pl.DataFrame | None = None,
    joint_tours: pl.DataFrame | None = None,
) -> dict[str, pl.DataFrame]:
    """Attach existing weights to the data.

    Loads weights from provided files and attaches them to the data.

    For any tables that exist, and do not have weights provided, we can optionally
    derive missing weights by carrying forward weights from the next logical upstream table.

    For example, if only household weights exist, all subsequent tables (persons, days, trips)
    will receive the household weight for each member record. If trip (unlinked) weights exist
    but not linked trips or tours, the unlinked trip weights will be carried forward using
    appropriate logic.

    If a "middle" weight is missing, an error will be raised if derive_missing_weights is True.
    For example, if household and trip weights are provided, but not person or day weights,
    an error will be raised as this likely indicates a misconfiguration.

    # Weight hierarchy logic

        hh_weight
          └─ person_weight        (carry forward via hh_id)
              └─ day_weight        (split: person_weight / n_usable_days)
                  └─ unlinked_trip_weight  (carry forward via day_id)
                      ├─ linked_trip_weight   (mean agg via linked_trip_id)
                      │   └─ joint_trip_weight (SUM agg via joint_trip_id)
                      └─ tour_weight          (mean agg via tour_id)
                          └─ joint_tour_weight (SUM agg via joint_tour_id)

    The shape is declared once in
    [`HIERARCHY`][processing.weighting.core.hierarchy.HIERARCHY]; this
    step only supplies the weights it is given and derives the rest from it.

    Days are the *average-day* split: a person's usable days share their person
    weight equally, so day totals read as persons on an average day (the
    vendor's own convention). Supplied day weights are redistributed within
    each person the same way.

    Carry-forward levels are checked against what their parents represent:

    - ``sum(person_weight) ≈ sum(hh_weight x num_persons)``
    - ``sum(day_weight) ≈ person_weight`` (per person, usable days)
    - ``sum(unlinked_trip_weight) ≈ sum(day_weight x num_trips)``

    The *mean* aggregate levels have no such identity: a mean over members does
    **not** sum to the members' total, so ``sum(linked_trip_weight)`` is
    unrelated to ``sum(unlinked_trip_weight)`` and is not checked.

    The *sum* levels -- the joint tables -- do, and it is checked:

    - ``sum(joint_trip_weight) ≈ sum(linked_trip_weight of member trips)``
    - ``sum(joint_tour_weight) ≈ sum(tour_weight of member tours)``

    Args:
        weights: A dict mapping config keys to weight file paths.
            Each entry specifies a weight CSV to load.  Supported config
            keys: ``hh_weight``, ``person_weight``,
            ``day_weight``, ``unlinked_trip_weight``,
            ``linked_trip_weight``, ``joint_trip_weight``,
            ``tour_weight``.

            Config options per entry:

            - ``weight_path``: Path to CSV file containing weights (required).
            - ``weight_id_col``: ID column name in the weight file
              (optional, defaults to canonical table ID).
            - ``weight_col``: Weight column name in the weight file
              (optional, defaults to canonical weight column).

        derive_missing_weights: Whether to derive weights for tables
            without provided weight files (default: False).
        usability_flag_col: Boolean column deciding which records may carry
            weight -- one of the usability profiles stamped upstream by the
            ``cascade_completeness`` step, matching the tours that profile's
            consumer keeps; pass ``complete`` to weight the whole valid survey
            including partial and overnight tours. Supplied weights are
            redistributed onto the usable records rather than simply zeroed, so
            each table's supplied total is preserved — the vendor's anchor
            cannot be re-balanced from here. Mutually exclusive with
            *weight_profiles*.
        weight_profiles: Profiles to weight, each writing its own column set
            suffixed with the profile name. The supplied files are read once and
            redistributed onto each profile's usable records, so every set
            preserves the same supplied totals over a different universe.
        canonical_data: Canonical data container. When present, the suffixed
            weight columns are registered on it, since names that come from
            config cannot be model fields.
        households: Households DataFrame.
        persons: Persons DataFrame.
        days: Days DataFrame.
        unlinked_trips: Unlinked trips DataFrame.
        linked_trips: Linked trips DataFrame.
        tours: Tours DataFrame.
        joint_trips: Joint trips DataFrame.
        joint_tours: Joint tours DataFrame.

    Returns:
        Dict of tables with attached weights.

    # Example config

    ```yaml
        - name: add_existing_weights
          params:
            derive_missing_weights: true
            weights:
              hh_weight:
                weight_path: "weights/hh_weights.csv"
                # defaults: id_col='hh_id', weight_col='hh_weight'
              person_weight:
                weight_path: "weights/person_weights.csv"
              unlinked_trip_weight:
                weight_path: "weights/trip_weights.csv"
                weight_id_col: "trip_id"
                weight_col: "trip_weight"
    ```
    """
    # Collect all provided tables
    tables = collect_tables(
        households=households,
        persons=persons,
        days=days,
        unlinked_trips=unlinked_trips,
        linked_trips=linked_trips,
        joint_trips=joint_trips,
        joint_tours=joint_tours,
        tours=tours,
    )

    profiles = resolve_fitted_profiles(usability_flag_col, weight_profiles)

    # Track which weights are provided, and the supplied numbers themselves --
    # read once, then redistributed per profile.
    provided_weights = set()
    supplied: dict[str, pl.DataFrame] = {}

    # Validate and convert weight configs to WeightConfig objects
    validated_weights: dict[str, ExistingWeightConfig] = {}
    for config_key, value in weights.items():
        if isinstance(value, ExistingWeightConfig):
            validated_weights[config_key] = value
        elif isinstance(value, dict):
            # Auto-infer config_key from dict key if not provided
            validated_weights[config_key] = ExistingWeightConfig(config_key=config_key, **value)
        else:
            msg = (
                f"Weight config for {config_key} must be a ExistingWeightConfig or dict, "
                f"got {type(value)}"
            )
            raise TypeError(msg)

    # Load and join provided weight files
    for cfg in validated_weights.values():
        table_name = cfg.table_name
        provided_weights.add(table_name)

        df = tables.get(table_name)
        if df is None:
            logger.warning("Weight file provided for %s but table not found in data", table_name)
            continue

        # Load weight file
        logger.info("Loading weights from %s for %s", cfg.weight_path, table_name)
        weight_df = pl.read_csv(cfg.weight_path)

        # Validate columns exist before selecting
        cfg.validate_columns(weight_df, df)

        # Select only needed columns
        weight_df = weight_df.select([cfg.weight_id_col, cfg.weight_col])

        # Handle potential column name mismatches
        rename_map = {}

        # Rename weight file ID column to match table if needed
        if cfg.weight_id_col != cfg.table_id_col:
            if cfg.weight_id_col is None:
                msg = f"weight_id_col should never be None due to model_validator for {table_name}"
                raise ValueError(msg)
            rename_map[cfg.weight_id_col] = cfg.table_id_col

        if cfg.keep_name and profiles != (None,):
            msg = (
                f"{cfg.config_key}: keep_name cannot be combined with weight_profiles. "
                f"The vendor's column {cfg.weight_col!r} is one name, and there are "
                f"{len(profiles)} universes to write; suffixing it would defeat the only "
                "reason to keep it. Drop keep_name, or weight a single profile."
            )
            raise ValueError(msg)

        # Land on the canonical name here so what was read is uniform whatever the
        # vendor called it; `_attach_supplied` renames to the column each pass
        # writes, which under keep_name is the vendor's name again.
        if cfg.weight_col != cfg.canonical_weight_col:
            if cfg.weight_col is None:
                msg = f"weight_col should never be None due to model_validator for {table_name}"
                raise ValueError(msg)
            rename_map[cfg.weight_col] = cfg.canonical_weight_col

        if rename_map:
            weight_df = weight_df.rename(rename_map)

        logger.info("Loaded %d %s weights from %s", weight_df.height, table_name, cfg.weight_path)

        supplied[table_name] = weight_df

    # One column set per profile, all from the same supplied numbers: the files
    # are read once above, and each pass starts from those values rather than
    # from the previous profile's redistributed ones.
    for profile in profiles:
        flag = profile if profile is not None else usability_flag_col
        if flag is None:  # pragma: no cover - resolve_fitted_profiles forbids it
            msg = "No usability flag to weight against"
            raise ValueError(msg)

        has_weight = _attach_supplied(tables, supplied, validated_weights, profile)

        # Redistribute each supplied weight onto the usable records, preserving
        # the supplied total. Usability is stamped upstream by
        # ``cascade_completeness``.
        _apply_usability(tables, has_weight, usability_flag_col=flag)

        # Derive missing weights if requested
        if derive_missing_weights:
            propagate_weights(
                tables,
                has_weight,
                skip=provided_weights,
                usability_flag_col=flag,
                profile=profile,
            )

        _warn_null_weights(tables, profile)

    drop_unsuffixed_weights(tables, profiles)

    if canonical_data is not None:
        for table, described in describe_weight_columns(profiles).items():
            canonical_data.register_generated_columns(table, described)

    logger.info(
        "Weight attachment complete for %s. Tables with weights: %s",
        ", ".join(p or "the whole survey" for p in profiles),
        sorted(supplied),
    )

    return {name: df for name, df in tables.items() if df is not None}
