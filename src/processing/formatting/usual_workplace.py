"""A model's usual workplace: the reported one, else the one found from trips.

Activity-based models send every work tour to the person's usual workplace, and
a worker without one makes no work tours. The survey only asks for a workplace
when the job has one fixed location, so a worker whose location varies, or who
works remotely, has none -- even when their trips show them working at the same
place day after day.

Here that observed workplace stands in. It is an inference, made only for the
model formatters and only where the survey gave nothing: canonical persons keep
the reported workplace alone, a reported workplace is never replaced, and only
someone employed gets one.
"""

import logging

import polars as pl

from data_canon.codebook.generic import LocationSource, LocationType
from data_canon.codebook.persons import EMPLOYED

logger = logging.getLogger(__name__)


def _main_observed_workplace(
    habitual_locations: pl.DataFrame,
    habitual_location_days: pl.DataFrame,
    zone_names: list[str],
) -> pl.DataFrame:
    """Each person's observed workplace with the most total time spent there.

    Total dwell across every day, not the longest single stay: the usual place
    is the one the person keeps returning to. A tie goes to the lowest
    ``location_num``, so the answer does not depend on row order.
    """
    dwell = habitual_location_days.group_by("habitual_location_id").agg(
        pl.col("dwell_minutes").sum().alias("_total_dwell")
    )
    return (
        habitual_locations.filter(
            (pl.col("location_type") == LocationType.WORK.value)
            & (pl.col("source") == LocationSource.OBSERVED.value)
        )
        .join(dwell, on="habitual_location_id", how="left")
        .sort(
            ["person_id", "_total_dwell", "location_num"],
            descending=[False, True, False],
            nulls_last=True,
        )
        .unique("person_id", keep="first", maintain_order=True)
        .select(
            "person_id",
            pl.col("lat").alias("_obs_lat"),
            pl.col("lon").alias("_obs_lon"),
            *(pl.col(z).alias(f"_obs_{z}") for z in zone_names),
        )
    )


def fill_usual_workplace(
    persons: pl.DataFrame,
    habitual_locations: pl.DataFrame,
    habitual_location_days: pl.DataFrame,
    *,
    zone_names: list[str],
) -> pl.DataFrame:
    """Give employed persons with no reported workplace their observed one.

    Args:
        persons: Persons with ``employment``, ``work_lat``, ``work_lon`` and
            ``work_<zone>``.
        habitual_locations: Habitual locations with ``lat``, ``lon`` and a column
            per zone in *zone_names* (see ``add_zone_ids``).
        habitual_location_days: Time spent at each location per day.
        zone_names: The zone systems to fill, e.g. ``["TAZ1454"]`` fills
            ``work_TAZ1454``.

    Returns:
        *persons* with the workplace columns filled where the rule applies.
    """
    observed = _main_observed_workplace(habitual_locations, habitual_location_days, zone_names)
    takes = (
        pl.col("work_lat").is_null()
        & pl.col("_obs_lat").is_not_null()
        & pl.col("employment").is_in([e.value for e in EMPLOYED])
    )
    fill = [
        pl.when(takes).then(pl.col(f"_obs_{src}")).otherwise(pl.col(dst)).alias(dst)
        for src, dst in [
            ("lat", "work_lat"),
            ("lon", "work_lon"),
            *((z, f"work_{z}") for z in zone_names),
        ]
    ]
    out = persons.join(observed, on="person_id", how="left")
    n_filled = out.select(takes.sum()).item()
    logger.info(
        "Usual workplace: %d employed person(s) with no reported workplace take "
        "their most-visited observed one.",
        n_filled,
    )
    return out.with_columns(fill).drop(observed.columns[1:])
