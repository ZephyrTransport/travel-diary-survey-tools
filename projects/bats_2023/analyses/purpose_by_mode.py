"""Processed linked_trip and summarize the purpose by mode."""

from pathlib import Path

import polars as pl

from data_canon.codebook.trips import (
    ModeType,
    PurposeCategory,
)

# Output path for the results
output_path = Path(
    r"\\models.ad.mtc.ca.gov\data\models\Data\HomeInterview\Bay Area Travel Study (across-years)"
    r"\pipeline_lmz_20260209\survey"
)

# Load linked trips data
linked_trips = pl.read_csv(output_path / "linked_trips_2023.csv")

# Add string labels for mode types and purpose categories
mode_map = ModeType.to_dict()
purpose_map = PurposeCategory.to_dict()


# Summarize the weighted purpose by mode
purpose_by_mode_long = (
    linked_trips.group_by(["mode_type", "d_purpose_category"])
    .agg(pl.sum("linked_trip_weight").alias("Weight"), pl.len().alias("Count"))
    .sort("Weight", descending=True)
    .with_columns(
        pl.col("mode_type").replace_strict(mode_map).alias("Mode"),
        pl.col("d_purpose_category").replace_strict(purpose_map).alias("Purpose"),
        (pl.col("Weight") / pl.col("Weight").sum().over("d_purpose_category")).alias("Percentage"),
    )
)

# Pivot weighted summary to wide format
purpose_by_mode_wide = purpose_by_mode_long.pivot(
    on="Purpose",
    index="Mode",
    values="Percentage",
).fill_null(0)

# Add total row
purpose_columns = [col for col in purpose_by_mode_wide.columns if col != "Mode"]
total_row = pl.DataFrame(
    {"Mode": ["Total"], **{col: [purpose_by_mode_wide[col].sum()] for col in purpose_columns}}
)

purpose_by_mode_wide = pl.concat([purpose_by_mode_wide, total_row])

# Save the results to CSV
purpose_by_mode_wide.write_csv(output_path / "purpose_by_mode.csv")
