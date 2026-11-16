"""Column helpers shared by the CT-RAMP table formatters."""

import polars as pl

from data_canon.codebook.tours import TourDirection


def sample_rate(weight_col: str) -> pl.Expr:
    """CT-RAMP's ``sampleRate``: the inverse of a positive weight, else null."""
    return (
        pl.when(pl.col(weight_col) > 0)
        .then(pl.col(weight_col).pow(-1))
        .otherwise(None)
        .alias("sampleRate")
    )


def order_columns(df: pl.DataFrame, order: list[str]) -> pl.DataFrame:
    """Put the columns of *order* that *df* has first, in that order, then the rest."""
    ordered = [c for c in order if c in df.columns]
    return df.select(ordered + [c for c in df.columns if c not in ordered])


def add_inbound_and_stop_id(trips: pl.DataFrame, tour_key: str, half_tour: pl.Expr) -> pl.DataFrame:
    """Add CT-RAMP's ``inbound`` flag and ``stop_id``, sorting trips into legs.

    ``inbound`` is 1 on the inbound half of the tour, else 0. ``stop_id``
    numbers the trips of each half-tour leg from 0 in order of departure; a leg
    of a single trip has no intermediate stop and gets -1. The trips come back
    sorted by tour, leg, departure and arrival.

    Args:
        trips: Trips with ``tour_direction``, ``depart_time`` and ``arrive_time``.
        tour_key: Column identifying the tour, unique across persons and days.
        half_tour: Expression labelling each trip's leg; trips sharing a label
            within a tour are numbered together.
    """
    leg = [tour_key, "tour_direction_str"]
    return (
        trips.with_columns(half_tour.alias("tour_direction_str"))
        .sort([*leg, "depart_time", "arrive_time"])
        .with_columns(
            (pl.col("depart_time").rank("ordinal").over(leg) - 1).cast(pl.Int64).alias("_stop_seq")
        )
        .with_columns(
            pl.when(pl.col("_stop_seq").max().over(leg) == 0)
            .then(pl.lit(-1))
            .otherwise(pl.col("_stop_seq"))
            .cast(pl.Int64)
            .alias("stop_id"),
            pl.when(pl.col("tour_direction") == TourDirection.INBOUND.value)
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias("inbound"),
        )
        .drop("_stop_seq")
    )
