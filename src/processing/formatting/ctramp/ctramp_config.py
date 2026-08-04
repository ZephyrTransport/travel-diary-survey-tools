"""Configuration model for CT-RAMP formatting."""

from pydantic import BaseModel, ConfigDict, Field, model_validator


class CTRAMPConfig(BaseModel):
    """Configuration for CT-RAMP formatting.

    Attributes:
        income_low_threshold: Income threshold for low income bracket
        income_med_threshold: Income threshold for medium income bracket
        income_high_threshold: Income threshold for high income bracket
        drop_missing_taz: If True, remove households without valid TAZ IDs
        drop_invalid_tours: If True, remove tours flagged as invalid
        age_adult: Age threshold for adult vs child in joint tour composition
        income_survey_year_to_ctramp_year: Factor converting survey-year dollars
            to CT-RAMP-year dollars
        gender_default_for_missing: Default gender for missing/non-binary
    """

    income_low_threshold: int = Field(
        ge=0,
        description=(
            "Income threshold for low income bracket (in dollars). "
            "Income below this is classified as 'work_low'. "
            "Example: 60000 for $60k annual income"
        ),
    )

    income_med_threshold: int = Field(
        ge=0,
        description=(
            "Income threshold for medium income bracket (in dollars). "
            "Household income below this threshold (but above low) is "
            "classified as 'work_med'. "
            "Example: 150000 for $150k annual income"
        ),
    )

    income_high_threshold: int = Field(
        ge=0,
        description=(
            "Income threshold for high income bracket (in dollars). "
            "Household income below this threshold (but above med) is "
            "classified as 'work_high'. Above this is 'work_very high'. "
            "Example: 240000 for $240k annual income"
        ),
    )

    taz_field: str = Field(
        default="taz",
        description="The field name in the household data that contains the TAZ ID for CTRAMP formatting.",  # noqa: E501
    )

    drop_missing_taz: bool = Field(
        default=True,
        description="If True, remove households without valid TAZ IDs",
    )

    filter_zero_weight: bool = Field(
        default=True,
        description=(
            "If True, remove households with null or zero hh_weight before "
            "formatting, cascading to persons, tours, and trips."
        ),
    )

    drop_invalid_tours: bool = Field(
        default=True,
        description=(
            "If True, remove tours that are not VALID (single-trip, loop, "
            "missing-anchor, change-mode, indeterminate) or not COMPLETE (do not "
            "start and end at home). Mirrors the DaySim formatter, which drops "
            "both, so both outputs keep the same tours. Cascades to linked and "
            "joint trips."
        ),
    )

    # Age thresholds
    age_adult: int = Field(
        default=4,
        ge=0,
        description=(
            "Age category threshold for adult vs child in joint tours. "
            "Use AgeCategory enum value (e.g., 4 = AGE_18_TO_24 and higher are adults). "
            "Default: 4 (18+)"
        ),
    )

    # Economic parameters
    income_survey_year_to_ctramp_year: float = Field(
        gt=0,
        description=(
            "Factor to convert survey-year dollars to CT-RAMP-year dollars. "
            "Survey income is multiplied by this factor. For BATS 2023 -> CT-RAMP "
            "year 2000 the factor is 1 / 1.88 ~= 0.532 (2000 -> 2023 inflation is 1.88) per "
            "https://github.com/BayAreaMetro/modeling-website/wiki/InflationAssumptions"
        ),
    )

    # Gender handling
    gender_default_for_missing: str = Field(
        default="f",
        pattern="^[mf]$",
        description=(
            "Default gender ('m' or 'f') for missing/non-binary values "
            "when CT-RAMP requires binary gender. Default: 'f'"
        ),
    )

    # spatial buffer (meters) for matching fixed locations to trip destinations
    fixed_location_buffer_meters: float = Field(
        default=200.0,
        ge=0,
        description=(
            "Spatial buffer in meters for matching fixed work/school "
            "locations to trip destinations when calculating distances."
        ),
    )

    # Distance units
    distance_unit: str = Field(
        default="miles",
        pattern="^(miles|kilometers|meters)$",
        description="Distance unit for CT-RAMP formatting ('miles', 'kilometers', or 'meters').",
    )

    @model_validator(mode="after")
    def validate_income_ordering(self) -> "CTRAMPConfig":
        """Validate that income thresholds are in proper order."""
        if self.income_low_threshold >= self.income_med_threshold:
            msg = (
                f"income_low_threshold ({self.income_low_threshold})"
                "must be less than "
                f"income_med_threshold ({self.income_med_threshold})"
            )
            raise ValueError(msg)

        if self.income_med_threshold >= self.income_high_threshold:
            msg = (
                f"income_med_threshold ({self.income_med_threshold}) "
                "must be less than "
                f"income_high_threshold ({self.income_high_threshold})"
            )
            raise ValueError(msg)

        return self

    model_config = ConfigDict(extra="forbid")
