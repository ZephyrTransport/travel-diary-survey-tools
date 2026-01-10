"""Generate summary statistics for pipeline inputs and outputs.

This script produces a text report comparing input and output data from the
BATS 2023 processing pipeline to verify what processing was done.
"""

import sys
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

# Constants
MAX_COLUMNS_DISPLAY = 20
ARG_INDEX_INPUT_DIR = 1
ARG_INDEX_OUTPUT_DIR = 2
ARG_INDEX_REPORT_FILE = 3


def format_number(n: float) -> str:
    """Format number with commas for readability."""
    return f"{n:,}"


def summarize_dataframe(df: pl.DataFrame, name: str) -> dict:
    """Generate summary statistics for a dataframe."""
    return {
        "name": name,
        "rows": len(df),
        "columns": len(df.columns),
        "column_list": df.columns,
    }


def write_summary_report(  # noqa: C901, PLR0912, PLR0915 ignore since its a script
    input_dir: Path,
    output_dir: Path,
    output_file: Path,
) -> None:
    """Generate and write summary report."""
    report_lines = []

    # Header
    report_lines.append("=" * 80)
    report_lines.append("BATS 2023 PIPELINE SUMMARY REPORT")
    report_lines.append("=" * 80)
    report_lines.append(f"Generated: {datetime.now(tz=UTC).strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append("")
    report_lines.append(f"Input Directory:  {input_dir}")
    report_lines.append(f"Output Directory: {output_dir}")
    report_lines.append("")

    # -------------------------------------------------------------------------
    # Input Data Summary
    # -------------------------------------------------------------------------
    report_lines.append("=" * 80)
    report_lines.append("INPUT DATA SUMMARY")
    report_lines.append("=" * 80)
    report_lines.append("")

    input_files = {
        "households": input_dir / "hh.csv",
        "persons": input_dir / "person.csv",
        "days": input_dir / "day.csv",
        "trips": input_dir / "trip.csv",
    }

    input_summaries = {}
    for name, filepath in input_files.items():
        if filepath.exists():
            df = pl.read_csv(filepath)
            summary = summarize_dataframe(df, name)
            input_summaries[name] = summary

            report_lines.append(f"{name.upper()}")
            report_lines.append(f"  Rows:    {format_number(summary['rows'])}")
            report_lines.append(f"  Columns: {summary['columns']}")
            report_lines.append("")
        else:
            report_lines.append(f"{name.upper()}: FILE NOT FOUND")
            report_lines.append("")

    # -------------------------------------------------------------------------
    # Output Data Summary - Standard Format
    # -------------------------------------------------------------------------
    report_lines.append("=" * 80)
    report_lines.append("OUTPUT DATA SUMMARY - STANDARD FORMAT")
    report_lines.append("=" * 80)
    report_lines.append("")

    standard_files = {
        "households": output_dir / "households.csv",
        "persons": output_dir / "persons.csv",
        "days": output_dir / "days.csv",
        "unlinked_trips": output_dir / "unlinked_trips.csv",
        "linked_trips": output_dir / "linked_trips.csv",
        "tours": output_dir / "tours.csv",
        "joint_trips": output_dir / "joint_trips.csv",
    }

    standard_summaries = {}
    for name, filepath in standard_files.items():
        if filepath.exists():
            df = pl.read_csv(filepath)
            summary = summarize_dataframe(df, name)
            standard_summaries[name] = summary

            report_lines.append(f"{name.upper()}")
            report_lines.append(f"  Rows:    {format_number(summary['rows'])}")
            report_lines.append(f"  Columns: {summary['columns']}")
            report_lines.append("")
        else:
            report_lines.append(f"{name.upper()}: FILE NOT FOUND")
            report_lines.append("")

    # -------------------------------------------------------------------------
    # Output Data Summary - DaySim Format
    # -------------------------------------------------------------------------
    report_lines.append("=" * 80)
    report_lines.append("OUTPUT DATA SUMMARY - DAYSIM FORMAT")
    report_lines.append("=" * 80)
    report_lines.append("")

    daysim_files = {
        "households_daysim": output_dir / "hh_daysim.csv",
        "persons_daysim": output_dir / "person_daysim.csv",
        "days_daysim": output_dir / "day_daysim.csv",
        "linked_trips_daysim": output_dir / "linked_trip_daysim.csv",
        "tours_daysim": output_dir / "tour_daysim.csv",
    }

    daysim_summaries = {}
    for name, filepath in daysim_files.items():
        if filepath.exists():
            df = pl.read_csv(filepath)
            summary = summarize_dataframe(df, name)
            daysim_summaries[name] = summary

            report_lines.append(f"{name.upper()}")
            report_lines.append(f"  Rows:    {format_number(summary['rows'])}")
            report_lines.append(f"  Columns: {summary['columns']}")
            report_lines.append("")
        else:
            report_lines.append(f"{name.upper()}: FILE NOT FOUND")
            report_lines.append("")

    # -------------------------------------------------------------------------
    # Processing Changes Summary
    # -------------------------------------------------------------------------
    report_lines.append("=" * 80)
    report_lines.append("PROCESSING CHANGES")
    report_lines.append("=" * 80)
    report_lines.append("")

    # Compare record counts
    if "households" in input_summaries and "households" in standard_summaries:
        hh_in = input_summaries["households"]["rows"]
        hh_out = standard_summaries["households"]["rows"]
        hh_diff = hh_out - hh_in
        report_lines.append(
            f"Households: {format_number(hh_in)} → {format_number(hh_out)} ({hh_diff:+,})"
        )

    if "persons" in input_summaries and "persons" in standard_summaries:
        per_in = input_summaries["persons"]["rows"]
        per_out = standard_summaries["persons"]["rows"]
        per_diff = per_out - per_in
        report_lines.append(
            f"Persons:    {format_number(per_in)} → {format_number(per_out)} ({per_diff:+,})"
        )

    if "days" in input_summaries and "days" in standard_summaries:
        day_in = input_summaries["days"]["rows"]
        day_out = standard_summaries["days"]["rows"]
        day_diff = day_out - day_in
        report_lines.append(
            f"Days:       {format_number(day_in)} → {format_number(day_out)} ({day_diff:+,})"
        )

    if "trips" in input_summaries and "unlinked_trips" in standard_summaries:
        trip_in = input_summaries["trips"]["rows"]
        trip_out = standard_summaries["unlinked_trips"]["rows"]
        trip_diff = trip_out - trip_in
        report_lines.append(
            f"Trips:      {format_number(trip_in)} → "
            f"{format_number(trip_out)} unlinked ({trip_diff:+,})"
        )

    report_lines.append("")

    # New data created
    report_lines.append("New Data Created:")
    if "linked_trips" in standard_summaries:
        linked_count = standard_summaries["linked_trips"]["rows"]
        report_lines.append(f"  Linked Trips: {format_number(linked_count)}")
    if "tours" in standard_summaries:
        tours_count = standard_summaries["tours"]["rows"]
        report_lines.append(f"  Tours:        {format_number(tours_count)}")
    if "joint_trips" in standard_summaries:
        joint_count = standard_summaries["joint_trips"]["rows"]
        report_lines.append(f"  Joint Trips:  {format_number(joint_count)}")

    report_lines.append("")

    # DaySim filtering
    report_lines.append("DaySim Filtering:")
    report_lines.append("  Reasons tours/trips are dropped:")
    report_lines.append("    1. Invalid tours (tour_data_quality != VALID)")
    report_lines.append("    2. Partial/incomplete tours (tour_category != COMPLETE)")
    report_lines.append("    3. Missing TAZ/MAZ assignments for households")
    report_lines.append("")

    # Get detailed breakdown of tour data quality issues
    if "tours" in standard_summaries:
        tours_df = pl.read_csv(output_dir / "tours.csv")

        # Tour data quality breakdown
        if "tour_data_quality" in tours_df.columns:
            quality_labels = {
                0: "VALID - Valid tour",
                1: "INVALID - Single-trip tour",
                2: "INVALID - Home-based loop trip",
                3: "INVALID - No home anchor at either end",
                4: "INVALID - Indeterminate cause",
                5: "INVALID - Change mode as primary purpose (linking failure)",
            }
            report_lines.append("  Tour Data Quality Breakdown:")
            quality_counts = (
                tours_df.group_by("tour_data_quality").agg(pl.len()).sort("tour_data_quality")
            )
            for row in quality_counts.iter_rows():
                quality_code, count = row
                quality_label = quality_labels.get(quality_code, f"Unknown ({quality_code})")
                pct = count / len(tours_df) * 100
                report_lines.append(
                    f"    {quality_label:60s}: {format_number(count):>8s} ({pct:5.1f}%)"
                )
            report_lines.append("")

        # Tour category breakdown
        if "tour_category" in tours_df.columns:
            category_labels = {
                1: "COMPLETE - Start at home, end at home",
                2: "PARTIAL - Start at home, end not at home",
                3: "PARTIAL - Start not at home, end at home",
                4: "PARTIAL - Start not at home, end not at home",
            }
            report_lines.append("  Tour Category Breakdown:")
            category_counts = tours_df.group_by("tour_category").agg(pl.len()).sort("tour_category")
            for row in category_counts.iter_rows():
                category_code, count = row
                category_label = category_labels.get(category_code, f"Unknown ({category_code})")
                pct = count / len(tours_df) * 100
                report_lines.append(
                    f"    {category_label:60s}: {format_number(count):>8s} ({pct:5.1f}%)"
                )
            report_lines.append("")

    if "tours" in standard_summaries and "tours_daysim" in daysim_summaries:
        tours_std = standard_summaries["tours"]["rows"]
        tours_ds = daysim_summaries["tours_daysim"]["rows"]
        tours_dropped = tours_std - tours_ds
        pct_dropped = (tours_dropped / tours_std * 100) if tours_std > 0 else 0
        report_lines.append(
            f"  Total Tours Dropped: {format_number(tours_dropped)} of "
            f"{format_number(tours_std)} ({pct_dropped:.1f}%)"
        )

    if "linked_trips" in standard_summaries and "linked_trips_daysim" in daysim_summaries:
        trips_std = standard_summaries["linked_trips"]["rows"]
        trips_ds = daysim_summaries["linked_trips_daysim"]["rows"]
        trips_dropped = trips_std - trips_ds
        pct_dropped = (trips_dropped / trips_std * 100) if trips_std > 0 else 0
        report_lines.append(
            f"  Total Linked Trips Dropped: {format_number(trips_dropped)} "
            f"of {format_number(trips_std)} ({pct_dropped:.1f}%)"
        )

    report_lines.append("")

    # -------------------------------------------------------------------------
    # Key Fields Added
    # -------------------------------------------------------------------------
    report_lines.append("=" * 80)
    report_lines.append("KEY FIELDS ADDED BY PROCESSING")
    report_lines.append("=" * 80)
    report_lines.append("")

    # Compare input vs output columns
    if "households" in input_summaries and "households" in standard_summaries:
        in_cols = set(input_summaries["households"]["column_list"])
        out_cols = set(standard_summaries["households"]["column_list"])
        new_cols = sorted(out_cols - in_cols)
        if new_cols:
            report_lines.append("Households - New Fields:")
            report_lines.extend(f"  • {col}" for col in new_cols)
            report_lines.append("")

    if "persons" in input_summaries and "persons" in standard_summaries:
        in_cols = set(input_summaries["persons"]["column_list"])
        out_cols = set(standard_summaries["persons"]["column_list"])
        new_cols = sorted(out_cols - in_cols)
        if new_cols:
            report_lines.append("Persons - New Fields:")
            report_lines.extend(f"  • {col}" for col in new_cols)
            report_lines.append("")

    if "trips" in input_summaries and "linked_trips" in standard_summaries:
        in_cols = set(input_summaries["trips"]["column_list"])
        out_cols = set(standard_summaries["linked_trips"]["column_list"])
        new_cols = sorted(out_cols - in_cols)
        if new_cols:
            report_lines.append("Linked Trips - New Fields:")
            report_lines.extend(f"  • {col}" for col in new_cols[:MAX_COLUMNS_DISPLAY])
            if len(new_cols) > MAX_COLUMNS_DISPLAY:
                remaining = len(new_cols) - MAX_COLUMNS_DISPLAY
                report_lines.append(f"  ... and {remaining} more")
            report_lines.append("")

    # -------------------------------------------------------------------------
    # Cross Tabulations
    # -------------------------------------------------------------------------
    report_lines.append("=" * 80)
    report_lines.append("CROSS TABULATIONS")
    report_lines.append("=" * 80)
    report_lines.append("")

    # Tours by purpose
    if "tours" in standard_summaries:
        tours_df = pl.read_csv(output_dir / "tours.csv")
        if "tour_purpose" in tours_df.columns:
            report_lines.append("Tours by Purpose:")
            purpose_counts = tours_df.group_by("tour_purpose").agg(pl.len()).sort("tour_purpose")
            for row in purpose_counts.iter_rows():
                purpose, count = row
                pct = count / len(tours_df) * 100
                line = f"  {purpose!s:20s}: {format_number(count):>10s}"
                report_lines.append(f"{line} ({pct:5.1f}%)")
            report_lines.append("")

    # Linked trips by mode
    if "linked_trips" in standard_summaries:
        trips_df = pl.read_csv(output_dir / "linked_trips.csv")
        if "mode" in trips_df.columns:
            report_lines.append("Linked Trips by Mode:")
            mode_counts = trips_df.group_by("mode").agg(pl.len()).sort("mode")
            for row in mode_counts.iter_rows():
                mode, count = row
                pct = count / len(trips_df) * 100
                report_lines.append(f"  {mode!s:20s}: {format_number(count):>10s} ({pct:5.1f}%)")
            report_lines.append("")

    # Tours by person category
    if "tours" in standard_summaries:
        tours_df = pl.read_csv(output_dir / "tours.csv")
        if "tour_category" in tours_df.columns:
            report_lines.append("Tours by Category:")
            person_counts = tours_df.group_by("tour_category").agg(pl.len()).sort("tour_category")
            for row in person_counts.iter_rows():
                category, count = row
                pct = count / len(tours_df) * 100
                line = f"  {category!s:20s}: {format_number(count):>10s}"
                report_lines.append(f"{line} ({pct:5.1f}%)")
            report_lines.append("")

    # Household size distribution
    if "households" in standard_summaries:
        hh_df = pl.read_csv(output_dir / "households.csv")
        if "num_persons" in hh_df.columns:
            report_lines.append("Household Size Distribution:")
            size_counts = hh_df.group_by("num_persons").agg(pl.len()).sort("num_persons")
            for row in size_counts.iter_rows():
                size, count = row
                pct = count / len(hh_df) * 100
                line = f"  {size} person(s): {format_number(count):>10s}"
                report_lines.append(f"{line} ({pct:5.1f}%)")
            report_lines.append("")

    # Persons by age category (enumerated values 1-11)
    if "persons" in standard_summaries:
        persons_df = pl.read_csv(output_dir / "persons.csv")
        if "age" in persons_df.columns:
            report_lines.append("Persons by Age Category:")
            age_labels = {
                1: "Under 5",
                2: "5 to 15",
                3: "16 to 17",
                4: "18 to 24",
                5: "25 to 34",
                6: "35 to 44",
                7: "45 to 54",
                8: "55 to 64",
                9: "65 to 74",
                10: "75 to 84",
                11: "85 and up",
            }
            age_counts = persons_df.group_by("age").agg(pl.len()).sort("age")
            for row in age_counts.iter_rows():
                age_code, count = row
                age_label = age_labels.get(age_code, f"Unknown ({age_code})")
                pct = count / len(persons_df) * 100
                line = f"  {age_label:20s}: {format_number(count):>10s}"
                report_lines.append(f"{line} ({pct:5.1f}%)")
            report_lines.append("")

    # Joint trips summary
    if "joint_trips" in standard_summaries:
        joint_df = pl.read_csv(output_dir / "joint_trips.csv")
        report_lines.append("Joint Trips Summary:")
        report_lines.append(f"  Total joint trip pairs: {format_number(len(joint_df))}")
        if "linked_trip_id_1" in joint_df.columns:
            unique_trips = len(
                set(joint_df["linked_trip_id_1"].to_list() + joint_df["linked_trip_id_2"].to_list())
            )
            report_lines.append(f"  Unique trips involved:  {format_number(unique_trips)}")
        report_lines.append("")

    # Tours per person distribution
    if "tours" in standard_summaries:
        tours_df = pl.read_csv(output_dir / "tours.csv")
        if "person_id" in tours_df.columns:
            report_lines.append("Tours per Person Distribution:")
            tours_per_person = tours_df.group_by("person_id").agg(pl.len().alias("num_tours"))
            tour_dist = (
                tours_per_person.group_by("num_tours")
                .agg(pl.len().alias("num_persons"))
                .sort("num_tours")
            )
            for row in tour_dist.iter_rows():
                num_tours, num_persons = row
                pct = num_persons / len(tours_per_person) * 100
                line = f"  {num_tours} tour(s): "
                line += f"{format_number(num_persons):>10s} persons"
                report_lines.append(f"{line} ({pct:5.1f}%)")
            report_lines.append("")

    # -------------------------------------------------------------------------
    # Footer
    # -------------------------------------------------------------------------
    report_lines.append("=" * 80)
    report_lines.append("END OF REPORT")
    report_lines.append("=" * 80)

    # Write to file
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))

    # Also print to console
    print("\n".join(report_lines))
    print(f"\nReport written to: {output_file}")


if __name__ == "__main__":
    # Default paths
    INPUT_DIR = Path(
        "M:/Data/HomeInterview/Bay Area Travel Study 2023/Data/"
        "Full Weighted 2023 Dataset/WeightedDataset_02212025"
    )
    OUTPUT_DIR = Path(
        "M:/Data/HomeInterview/Bay Area Travel Study 2023/Data/Processed/2023_pipeline_output"
    )
    REPORT_FILE = OUTPUT_DIR / "pipeline_summary_report.txt"

    # Allow command-line overrides
    if len(sys.argv) > ARG_INDEX_INPUT_DIR:
        INPUT_DIR = Path(sys.argv[ARG_INDEX_INPUT_DIR])
    if len(sys.argv) > ARG_INDEX_OUTPUT_DIR:
        OUTPUT_DIR = Path(sys.argv[ARG_INDEX_OUTPUT_DIR])
    if len(sys.argv) > ARG_INDEX_REPORT_FILE:
        REPORT_FILE = Path(sys.argv[ARG_INDEX_REPORT_FILE])

    write_summary_report(INPUT_DIR, OUTPUT_DIR, REPORT_FILE)
