"""Helper functions for data loading and comparison."""

import logging
from pathlib import Path

import polars as pl

from pipeline.pipeline import Pipeline

logger = logging.getLogger(__name__)

# Table configuration
TABLES = ["hh", "person", "personday", "tour", "trip"]
TABLE_NAMES = ["Households", "Persons", "Person-days", "Tours", "Trips"]


def _log_distribution(title: str, df: pl.DataFrame, col: str) -> None:
    """Log distribution statistics for a column."""
    dist = df.group_by(col).agg(pl.len().alias("count")).sort(col)
    logger.info("%s\n%s", title, str(dist))


def load_legacy_data(legacy_dir: Path) -> dict[str, pl.DataFrame]:
    """Load legacy Daysim CSV files."""
    logger.info("Loading legacy Daysim data...")
    data = {name: pl.read_csv(legacy_dir / f"{name}.csv") for name in TABLES}
    for name, table_name in zip(TABLES, TABLE_NAMES, strict=True):
        logger.info("  %s: %s", table_name, f"{len(data[name]):,}")
    return data


def load_new_pipeline_data(
    config_path: Path,
    cache_dir: Path | str | None = None,
) -> dict[str, pl.DataFrame]:
    """Load new pipeline Daysim-formatted tables from cache."""
    logger.info("\nLoading new pipeline data...")
    caching = str(cache_dir) if cache_dir else True
    pipeline = Pipeline(config_path=str(config_path), steps=[], caching=caching)

    data_keys = [
        "households_daysim",
        "persons_daysim",
        "days_daysim",
        "tours_daysim",
        "linked_trips_daysim",
    ]
    data = {name: pipeline.get_data(key) for name, key in zip(TABLES, data_keys, strict=True)}

    for name, table_name in zip(TABLES, TABLE_NAMES, strict=True):
        logger.info("  %s: %s", table_name, f"{len(data[name]):,}")
    return data


def compare_row_counts(
    legacy_data: dict[str, pl.DataFrame],
    new_data: dict[str, pl.DataFrame],
) -> None:
    """Compare row counts between legacy and new pipeline data."""
    sep = "=" * 80
    output = [
        "",
        sep,
        "ROW COUNT COMPARISON",
        sep,
        "",
        f"{'Table':<15} {'Legacy':<12} {'New':<12} {'Difference':<12} {'% Diff':<10}",
        "-" * 80,
    ]

    for table, name in zip(TABLES, TABLE_NAMES, strict=True):
        leg_cnt, new_cnt = len(legacy_data[table]), len(new_data[table])
        diff = new_cnt - leg_cnt
        pct = (diff / leg_cnt * 100) if leg_cnt > 0 else 0
        output.append(f"{name:<15} {leg_cnt:<12,} {new_cnt:<12,} {diff:+12,} {pct:+9.2f}%")

    logger.info("\n".join(output))


def compare_columns(
    legacy_data: dict[str, pl.DataFrame],
    new_data: dict[str, pl.DataFrame],
) -> None:
    """Compare column names between legacy and new pipeline data."""
    sep = "=" * 80
    output = ["", sep, "COLUMN COMPARISON", sep]

    for table, name in zip(TABLES, TABLE_NAMES, strict=True):
        leg_cols = set(legacy_data[table].columns)
        new_cols = set(new_data[table].columns)
        common = sorted(leg_cols & new_cols)
        leg_only = sorted(leg_cols - new_cols)
        new_only = sorted(new_cols - leg_cols)

        output.extend(
            [
                "",
                f"--- {name} ---",
                f"Total columns: Legacy={len(leg_cols)}, New={len(new_cols)}, Common={len(common)}",
            ]
        )

        if leg_only:
            output.extend(
                [
                    "",
                    f"Columns in legacy missing from new ({len(leg_only)}):",
                    "  " + ", ".join(leg_only),
                ]
            )
        if new_only and leg_only:
            output.extend(
                [
                    "",
                    f"Columns only in new ({len(new_only)}):",
                    "  " + ", ".join(new_only),
                ]
            )
        if not leg_only:
            output.extend(["", "✓ Columns match"])

    logger.info("\n".join(output))


def print_summary_statistics(
    legacy_data: dict[str, pl.DataFrame],
    new_data: dict[str, pl.DataFrame],
) -> None:
    """Print summary statistics comparing key distributions."""
    sep = "=" * 80
    logger.info("\n%s\nSUMMARY STATISTICS\n%s", sep, sep)

    # Distribution comparisons
    for col, table, title in [
        ("pdpurp", "tour", "Tour Purpose Distribution"),
        ("mode", "tour", "Tour Mode Distribution"),
        ("mode", "trip", "Trip Mode Distribution"),
    ]:
        if col in legacy_data[table].columns and col in new_data[table].columns:
            logger.info("\n--- %s ---\n", title)
            _log_distribution("Legacy:", legacy_data[table], col)
            logger.info("")
            _log_distribution("New Pipeline:", new_data[table], col)

    # TAZ coverage
    logger.info("\n--- Household TAZ Coverage ---")
    for data, label in [(legacy_data, "Legacy"), (new_data, "New")]:
        if "hhtaz" in data["hh"].columns:
            null_taz = data["hh"].filter(pl.col("hhtaz").is_null() | (pl.col("hhtaz") == -1)).height
            logger.info(
                "%s: %s households with missing/invalid TAZ",
                label,
                f"{null_taz:,}",
            )

    # Weight totals
    logger.info("\n--- Weight Totals ---")
    for data, label in [(legacy_data, "Legacy"), (new_data, "New")]:
        if "hhwgt" in data["hh"].columns:
            weight = data["hh"]["hhwgt"].sum()
            logger.info(
                "%s household weight total: %s",
                label,
                f"{weight:,.2f}",
            )
