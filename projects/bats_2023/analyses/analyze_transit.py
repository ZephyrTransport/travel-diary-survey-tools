"""Summarize transit trips for analysis."""

import logging

import polars as pl

from pipeline.decoration import step

logger = logging.getLogger(__name__)


@step()
def summarize_transit_trips(
    unlinked_trips: pl.DataFrame,
    linked_trips: pl.DataFrame,
    trip_weights: pl.DataFrame,
    transit_mode_codes: list[int],
) -> dict[str, pl.DataFrame]:
    """Summarize transit trips for analysis."""
    county_names = {
        6001: "Alameda County",
        6007: "Butte County",
        6013: "Contra Costa County",
        6015: "Del Norte County",
        6017: "El Dorado County",
        6019: "Fresno County",
        6023: "Humboldt County",
        6029: "Kern County",
        6037: "Los Angeles County",
        6039: "Madera County",
        6041: "Marin County",
        6043: "Mariposa County",
        6045: "Mendocino County",
        6047: "Merced County",
        6053: "Monterey County",
        6055: "Napa County",
        6057: "Nevada County",
        6059: "Orange County",
        6061: "Placer County",
        6065: "Riverside County",
        6067: "Sacramento County",
        6069: "San Benito County",
        6071: "San Bernardino County",
        6073: "San Diego County",
        6075: "San Francisco County",
        6077: "San Joaquin County",
        6079: "San Luis Obispo County",
        6081: "San Mateo County",
        6085: "Santa Clara County",
        6087: "Santa Cruz County",
        6089: "Shasta County",
        6091: "Sierra County",
        6095: "Solano County",
        6097: "Sonoma County",
        6099: "Stanislaus County",
        6107: "Tulare County",
        6109: "Tuolumne County",
        6111: "Ventura County",
        6113: "Yolo County",
        9998: "Another County in California",
    }

    # Drop any existing old weight columns to avoid confusion
    if "trip_weight" in unlinked_trips.columns:
        unlinked_trips.drop_in_place("trip_weight")
    if "linked_trip_weight" in linked_trips.columns:
        linked_trips.drop_in_place("linked_trip_weight")

    # Before join, check that the two dataframes have matching trip_ids
    # If there are weights withought matching trips, stop
    # If there are trips without weights, check which days are missing
    # (e.g., Fri/Sat/Sun)

    unlinked_trip_ids = set(unlinked_trips.select("trip_id").to_series().to_list())
    trip_weight_ids = set(trip_weights.select("trip_id").to_series().to_list())
    missing_ids = trip_weight_ids - unlinked_trip_ids
    if missing_ids:
        msg = (
            f"trip_weights contain trip_ids not found in unlinked_trips: "
            f"{list(missing_ids)[:10]} "
            f"(showing up to 10 IDs of {len(missing_ids)} total)"
        )
        raise ValueError(msg)

    # Join trip weights to unlinked trips for analysis
    unlinked_trips = unlinked_trips.join(
        trip_weights,
        on="trip_id",
        how="left",
    )

    # Aggregate weights over linked trips
    linked_trips_weights = unlinked_trips.group_by("linked_trip_id").agg(
        pl.col("trip_weight").mean().alias("linked_trip_weight"),
        pl.col("o_county").first().alias("o_county"),
        pl.col("d_county").first().alias("d_county"),
    )

    # Calculate transit boardings per linked trip and join back to linked trips
    total_boardings = (
        unlinked_trips.filter(pl.col("mode_type").is_in(transit_mode_codes))
        .group_by("linked_trip_id")
        .agg(pl.count("trip_id").alias("boardings"))
    )

    # Join back to linked trips
    linked_trips = linked_trips.join(
        linked_trips_weights,
        on="linked_trip_id",
        how="left",
    ).join(
        total_boardings,
        on="linked_trip_id",
        how="left",
    )

    # Filter to transit trips only
    linked_transit_trips = linked_trips.filter(pl.col("mode_type").is_in(transit_mode_codes))

    # Summarize total transit trips by origin and destination county
    transit_summary = (
        (
            linked_transit_trips.group_by(["o_county", "d_county"]).agg(
                pl.col("linked_trip_weight").sum().alias("total_weighted_transit_trips")
            )
        )
        .with_columns(
            pl.col("o_county").cast(pl.Utf8).replace(county_names).alias("o_county_name"),
            pl.col("d_county").cast(pl.Utf8).replace(county_names).alias("d_county_name"),
        )
        .pivot(
            values="total_weighted_transit_trips",
            index="o_county_name",
            columns="d_county_name",
        )
        .fill_null(0)
    )
    # Add row totals
    transit_summary = transit_summary.with_columns(
        pl.sum_horizontal([col for col in transit_summary.columns if col != "o_county_name"]).alias(
            "Total"
        )
    )
    # Add column totals
    total_row = transit_summary.select(
        [
            pl.lit("Total").alias("o_county_name"),
            *[
                pl.col(col).sum().alias(col)
                for col in transit_summary.columns
                if col != "o_county_name"
            ],
        ]
    )
    transit_summary = pl.concat([transit_summary, total_row], how="vertical")

    # Calculate overall total
    total_weighted_transit_trips = linked_transit_trips.select(
        pl.col("linked_trip_weight").sum().alias("total_weighted_transit_trips")
    ).item()

    # Calculate average boardings as total boardings / total transit trips
    total_boardings = linked_transit_trips.select(
        (pl.col("boardings") * pl.col("linked_trip_weight")).sum()
    ).item()

    avg_weighted_boardings_per_trip = total_boardings / total_weighted_transit_trips

    # Compare to expected ~ 971,588
    expected = 971588
    pct_chg = (total_weighted_transit_trips - expected) / expected * 100
    msg = (
        f"================= Transit Trip Summary ================"
        f"\nTotal weighted transit trips: {total_weighted_transit_trips:,.0f} "
        f"\nExpected ~971,588"
        f"\nDifference: {pct_chg:.2f}%"
        "\nAverage boardings per trip: "
        f"{avg_weighted_boardings_per_trip:.2f}"
        "\n======================================================"
    )
    logger.info(msg)

    return {
        "transit_summary": transit_summary,
        "transit_report": msg,
    }
