"""Household formatting for CT-RAMP.

Transforms canonical household data into CT-RAMP model format, including:

- **Income Conversion**: Converts categorical income values to midpoint dollars
  in configurable base year
- **Income Bracketing**: Classifies households into low/med/high/very high
  income brackets using user-defined thresholds
- **Spatial Fields**: Maps home TAZ (optionally filters households without
  valid TAZ)
- **Joint Tour Frequency**: Derives jtf_choice field from joint tour counts by
  purpose category
- **Excludes**: Random number fields (ao_rn, fp_rn, cdap_rn) and auto_suff
  (model simulation fields)

Note: Model-output fields (walk_subzone, humanVehicles, autonomousVehicles,
random number fields, auto_suff) are excluded as they are not derivable from
survey data.
"""

import logging

import polars as pl

from data_canon.codebook.ctramp import JTFChoice
from data_canon.codebook.households import IncomeDetailed, IncomeFollowup
from data_canon.codebook.persons import Employment
from processing.formatting.ctramp.mappings import PURPOSECATEGORY_TO_JTF_GROUP
from utils.helpers import get_income_midpoint

from .ctramp_config import CTRAMPConfig

logger = logging.getLogger(__name__)


def _compute_jtf_choice(
    households: pl.DataFrame,
    tours_canonical: pl.DataFrame,
) -> pl.DataFrame:
    """Compute joint tour frequency choice for households.

    Maps combinations of joint tour purposes to JTFChoice enum values.

    Args:
        households: DataFrame with hh_id column
        tours_canonical: Canonical tours DataFrame with tour_purpose
            (PurposeCategory value) and joint_tour_id

    Returns:
        households DataFrame with jtf_choice column added
    """
    # Check if tours is empty or doesn't have the joint_tour_id column
    if len(tours_canonical) == 0 or "joint_tour_id" not in tours_canonical.columns:
        # All households get NONE_NONE
        return households.with_columns(pl.lit(JTFChoice.NONE_NONE.value).alias("jtf_choice"))

    # Filter to joint tours
    joint_tours = tours_canonical.filter(pl.col("joint_tour_id").is_not_null())

    if len(joint_tours) == 0:
        # All households get NONE_NONE
        return households.with_columns(pl.lit(JTFChoice.NONE_NONE.value).alias("jtf_choice"))

    # Tag each joint tour with its JTF group
    joint_tours = joint_tours.with_columns(
        pl.col("tour_purpose").replace_strict(PURPOSECATEGORY_TO_JTF_GROUP).alias("_jtf_group")
    )

    # Aggregate by household: count tours by category
    hh_jtf_agg = joint_tours.group_by("hh_id").agg(
        [
            (pl.col("_jtf_group") == "S").sum().alias("shop_count"),
            (pl.col("_jtf_group") == "M").sum().alias("maint_count"),
            (pl.col("_jtf_group") == "E").sum().alias("eatout_count"),
            (pl.col("_jtf_group") == "V").sum().alias("visit_count"),
            (pl.col("_jtf_group") == "D").sum().alias("discr_count"),
        ]
    )

    # Define JTF mapping rules: (shop, maint, eatout, visit, discr) -> JTFChoice
    # Use -1 for "any", 0 for "exactly 0", >=1 for "at least 1", >=2 for "at least 2"
    jtf_rules = [
        # Two of same type
        (2, 0, 0, 0, 0, JTFChoice.TWO_SHOP),
        (0, 2, 0, 0, 0, JTFChoice.TWO_MAINT),
        (0, 0, 2, 0, 0, JTFChoice.TWO_EATOUT),
        (0, 0, 0, 2, 0, JTFChoice.TWO_VISIT),
        (0, 0, 0, 0, 2, JTFChoice.TWO_DISCR),
        # Shop + one other
        (1, 1, 0, 0, 0, JTFChoice.ONE_SHOP_ONE_MAINT),
        (1, 0, 1, 0, 0, JTFChoice.ONE_SHOP_ONE_EATOUT),
        (1, 0, 0, 1, 0, JTFChoice.ONE_SHOP_ONE_VISIT),
        (1, 0, 0, 0, 1, JTFChoice.ONE_SHOP_ONE_DISCR),
        # Maint + one other (excluding shop)
        (0, 1, 1, 0, 0, JTFChoice.ONE_MAINT_ONE_EATOUT),
        (0, 1, 0, 1, 0, JTFChoice.ONE_MAINT_ONE_VISIT),
        (0, 1, 0, 0, 1, JTFChoice.ONE_MAINT_ONE_DISCR),
        # Eatout + one other (excluding shop, maint)
        (0, 0, 1, 1, 0, JTFChoice.ONE_EATOUT_ONE_VISIT),
        (0, 0, 1, 0, 1, JTFChoice.ONE_EATOUT_ONE_DISCR),
        # Visit + one other (excluding shop, maint, eatout)
        (0, 0, 0, 1, 1, JTFChoice.ONE_VISIT_ONE_DISCR),
        # Single tours
        (1, 0, 0, 0, 0, JTFChoice.ONE_SHOP),
        (0, 1, 0, 0, 0, JTFChoice.ONE_MAINT),
        (0, 0, 1, 0, 0, JTFChoice.ONE_EATOUT),
        (0, 0, 0, 1, 0, JTFChoice.ONE_VISIT),
        (0, 0, 0, 0, 1, JTFChoice.ONE_DISCR),
    ]

    # Build when/then chain from rules
    expr = None
    for shop, maint, eatout, visit, discr, choice in jtf_rules:
        condition = (
            (pl.col("shop_count") >= shop)
            & (pl.col("maint_count") >= maint)
            & (pl.col("eatout_count") >= eatout)
            & (pl.col("visit_count") >= visit)
            & (pl.col("discr_count") >= discr)
        )
        if expr is None:
            expr = pl.when(condition).then(pl.lit(choice.value))
        else:
            expr = expr.when(condition).then(pl.lit(choice.value))

    if expr is None:
        msg = "No JTF rules defined"
        raise ValueError(msg)

    hh_jtf_agg = hh_jtf_agg.with_columns(expr.otherwise(pl.lit(None)).alias("jtf_choice"))

    # Check for unmapped combinations
    unmapped = hh_jtf_agg.filter(pl.col("jtf_choice").is_null())
    if len(unmapped) > 0:
        combinations = unmapped.select(
            ["shop_count", "maint_count", "eatout_count", "visit_count", "discr_count"]
        ).unique()
        msg = f"Unmapped joint tour combinations found: {combinations}"
        raise ValueError(msg)

    # Join with households and fill nulls for households with no joint tours
    return households.join(
        hh_jtf_agg.select(["hh_id", "jtf_choice"]), on="hh_id", how="left"
    ).with_columns(pl.col("jtf_choice").fill_null(JTFChoice.NONE_NONE.value))


def format_households(
    households_canonical: pl.DataFrame,
    persons_canonical: pl.DataFrame,
    tours_canonical: pl.DataFrame,
    config: CTRAMPConfig,
) -> pl.DataFrame:
    """Format household data to CT-RAMP specification.

    Transforms household data from canonical format to
    [`HouseholdCTRAMPModel`][data_canon.models.ctramp.HouseholdCTRAMPModel]
    Key transformations:

    - Rename fields to CT-RAMP conventions
    - Convert income categories to midpoint values
    - Compute household aggregates (size, workers, vehicles)
    - Map TAZ using configuration

    Args:
        households_canonical: Canonical households DataFrame with hh_id, home_taz,
            income_detailed, income_followup, num_vehicles
        persons_canonical: Canonical persons DataFrame with hh_id, employment (for
            computing household size and worker count)
        tours_canonical: Canonical tours DataFrame with hh_id, joint_tour_id,
            tour_purpose (for computing joint tour frequency)
        config: CTRAMPConfig instance with configuration parameters

    Returns:
        DataFrame with CT-RAMP household fields:

            - hh_id: Household ID
            - taz: Home TAZ
            - income: Annual household income (midpoint value)
            - autos: Number of automobiles
            - size: Number of persons
            - workers: Number of workers
            - jtf_choice: Joint tour frequency (count of unique joint tours)

    Notes:
        - Model-output fields (walk_subzone, humanVehicles, autonomousVehicles,
          random number fields, auto_suff) are excluded as they are not
          derivable from survey data
    """
    logger.info("Formatting household data for CT-RAMP")

    # Compute household aggregates from persons table
    household_aggregates = persons_canonical.group_by("hh_id").agg(
        [
            pl.len().alias("size"),
            # Count employed persons
            pl.col("employment")
            .is_in(
                [
                    Employment.EMPLOYED_FULLTIME.value,
                    Employment.EMPLOYED_PARTTIME.value,
                    Employment.EMPLOYED_SELF.value,
                    Employment.EMPLOYED_UNPAID.value,
                    Employment.EMPLOYED_FURLOUGHED.value,
                ]
            )
            .sum()
            .alias("workers"),
        ]
    )

    # Join aggregates with households
    households_ctramp = households_canonical.join(household_aggregates, on="hh_id", how="left")

    # Ensure income columns have proper nullable int type before any operations
    households_ctramp = households_ctramp.with_columns(
        pl.col("income_detailed").cast(pl.Int64),
        pl.col("income_followup").cast(pl.Int64),
    )

    # Map income categories to midpoint values
    income_detailed_map = {
        income_cat.value: int(get_income_midpoint(income_cat))
        for income_cat in IncomeDetailed
        if "Prefer not to answer" not in income_cat.label and "Missing" not in income_cat.label
    }
    income_followup_map = {
        income_cat.value: int(get_income_midpoint(income_cat))
        for income_cat in IncomeFollowup
        if "Prefer not to answer" not in income_cat.label and "Missing" not in income_cat.label
    }

    households_ctramp = households_ctramp.with_columns(
        pl.col("income_detailed").replace_strict(income_detailed_map, default=None),
        pl.col("income_followup").replace_strict(income_followup_map, default=None),
    )

    # Use income_detailed if available, otherwise income_followup
    households_ctramp = households_ctramp.with_columns(
        income=pl.coalesce(pl.col("income_detailed"), pl.col("income_followup"))
    )

    # Compute jtf_choice for all households
    if tours_canonical is None:
        msg = "Tours data is required to compute jtf_choice"
        raise ValueError(msg)

    households_ctramp = _compute_jtf_choice(households_ctramp, tours_canonical)

    # Validate num_vehicles is present
    if households_ctramp["num_vehicles"].null_count() > 0:
        msg = "num_vehicles contains null values - this field is required"
        raise ValueError(msg)

    # Add vehicle count (autos field)
    households_ctramp = households_ctramp.with_columns(
        pl.col("num_vehicles").cast(pl.Int64).alias("autos")
    )

    # Add weight and sampleRate if hh_weight exists
    if "hh_weight" in households_ctramp.columns:
        households_ctramp = households_ctramp.with_columns(
            pl.when(pl.col("hh_weight") > 0)
            .then(pl.col("hh_weight").pow(-1))
            .otherwise(None)
            .alias("sampleRate")
        )

    # Rename home_taz to taz and alias it for format_mandatory_location
    households_ctramp = households_ctramp.with_columns(
        pl.col(f"home_{config.taz_field}").alias("taz")
    )

    logger.info("Formatted %d households for CT-RAMP", len(households_ctramp))

    return households_ctramp
