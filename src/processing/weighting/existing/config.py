"""Settings for ``add_existing_weights``: each supplied weight file, and the step's own."""

import logging
from pathlib import Path

import polars as pl
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from processing.weighting.core.hierarchy import LEVEL_BY_WEIGHT_COL, weight_col_for
from processing.weighting.core.propagation import safe_join_weight

logger = logging.getLogger(__name__)


class ExistingWeightConfig(BaseModel):
    """Configuration for a single weight file.

    Most fields use sensible defaults - typically you only need to provide weight_path.
    Override weight_id_col only if your weight file uses different ID column names.
    Override weight_col only if your weight file uses different weight column names.

    Examples:
        Basic usage with defaults:
        >>> ExistingWeightConfig(config_key="hh_weight", weight_path="hh_weights.csv")
        # Uses: weight_id_col="hh_id", weight_col="hh_weight"

        Override weight column name:
        >>> ExistingWeightConfig(
        ...     config_key="person_weight",
        ...     weight_path="person_weights.csv",
        ...     weight_col="final_weight"  # Weight file uses "final_weight" not "person_weight"
        ... )

        Override ID column name (external weight file with non-canonical IDs):
        >>> ExistingWeightConfig(
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

    @field_validator("config_key")
    @classmethod
    def validate_config_key(cls, v: str) -> str:
        """Validate that config_key is one of the allowed values."""
        if v not in LEVEL_BY_WEIGHT_COL:
            msg = f"Invalid weight config key: {v}. Must be one of {list(LEVEL_BY_WEIGHT_COL)}"
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
        """Set default values for weight_id_col and weight_col from the hierarchy."""
        if self.weight_id_col is None:
            self.weight_id_col = self.table_id_col
        if self.weight_col is None:
            self.weight_col = self.canonical_weight_col
        return self

    @property
    def table_name(self) -> str:
        """Get the table name for this weight config."""
        return LEVEL_BY_WEIGHT_COL[self.config_key].table

    @property
    def table_id_col(self) -> str:
        """Get the canonical table ID column."""
        return LEVEL_BY_WEIGHT_COL[self.config_key].id_col

    @property
    def canonical_weight_col(self) -> str:
        """Get the canonical weight column name for this table."""
        return LEVEL_BY_WEIGHT_COL[self.config_key].weight_col

    def joined_weight_col(self, profile: str) -> str:
        """The column the supplied weight lands in under *profile*."""
        return weight_col_for(self.canonical_weight_col, profile)

    def validate_columns(self, weight_df: pl.DataFrame) -> None:
        """Validate that the weight file has its ID and weight columns.

        Raises:
            ValueError: If either column is missing.
        """
        if self.weight_id_col not in weight_df.columns:
            msg = f"Weight file {self.weight_path} missing required ID column: {self.weight_id_col}"
            raise ValueError(msg)

        if self.weight_col not in weight_df.columns:
            msg = (
                f"Weight file {self.weight_path} missing required weight column: {self.weight_col}"
            )
            raise ValueError(msg)


def attach_supplied(
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
        renamed = weight_df.rename({cfg.canonical_weight_col: joined_col})
        # A re-run replaces the column rather than duplicating it
        tables[table_name] = safe_join_weight(df, renamed, cfg.table_id_col)
        has_weight[table_name] = joined_col
        logger.info("Joined %s to %s on %s", joined_col, table_name, cfg.table_id_col)

    return has_weight


class ExistingWeightsConfig(BaseModel):
    """Settings for ``add_existing_weights``: how supplied weights move onto usable records."""

    model_config = ConfigDict(extra="forbid")

    preserve_travel_totals: bool = Field(
        description=(
            "Households are always held to their total and persons to their "
            "household, as known populations, and a person's days carry that "
            "person's weight. Every unlinked trip moves with its day, by the "
            "day's final weight over its supplied weight. Trips and tours are not "
            "targets, so whether a dropped trip's weight moves is a choice. true: "
            "a kept day's dropped trips hand their weight to its kept trips, in "
            "proportion, so the day's trips keep their total. false: every kept "
            "trip keeps its own weight times its day's factor, and a dropped one's "
            "weight is gone. Either way, linked trips and tours are the mean of "
            "their final trips, and joint trips and joint tours the sum of their "
            "kept members."
        ),
    )
    household_size_top_group: int = Field(
        description=(
            "Households are scaled by one factor per household size (number of "
            "persons), so dropping more large households than small ones does not "
            "move the person total. Sizes from this one up share a factor, which "
            "keeps a size with few households from carrying an extreme one."
        ),
    )
    weight_profiles: list[str] = Field(
        description=(
            "Usability profiles to weight, each writing its own column set "
            "suffixed with the profile name; a single profile is a list of one. "
            "Each is stamped upstream by cascade_usability; name "
            "survey_complete to weight the whole valid survey including partial "
            "and overnight tours. The supplied files are read once and moved onto "
            "each profile's usable records rather than simply zeroed, so every set "
            "holds the same household and person totals -- the vendor's anchor "
            "cannot be re-balanced from here."
        ),
    )
    derive_missing_weights: bool = Field(
        description="Whether to derive weights for tables without supplied weight files.",
    )
    max_weight_factor: dict[str, float] | None = Field(
        default=None,
        description=(
            "Optional limits, table -> the most a record's final weight may be, as "
            "a multiple of its own supplied weight. A table left out has no limit. "
            "The step raises naming the records past it. Each table named must "
            "have a supplied weight."
        ),
    )
