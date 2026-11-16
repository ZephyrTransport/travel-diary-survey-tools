"""The ``detect_habitual_locations`` step: add observed locations to the delivered table."""

import logging

import polars as pl

from data_canon.codebook.generic import LocationSource
from pipeline.decoration import Tables, step

from .episodes import build_presence_episodes
from .habitual_location_configs import HabitualLocationConfig
from .location_days import build_location_days
from .numbering import LOCATION_COLUMNS, number_observed
from .observed import OBSERVED_KINDS, observed_homes, observed_locations

logger = logging.getLogger(__name__)


def _check_delivered(delivered: pl.DataFrame) -> None:
    """Raise unless the delivered table holds only reported locations, one primary per kind.

    Observed locations are this step's to find; a delivered one would be
    somebody else's rule standing in for ours.
    """
    not_reported = delivered.filter(pl.col("source") != LocationSource.REPORTED.value)
    if not_reported.height > 0:
        msg = (
            f"{not_reported.height} delivered habitual location(s) are not REPORTED. "
            "Survey cleaning delivers reported locations only; observed ones are "
            "found by detect_habitual_locations."
        )
        raise ValueError(msg)
    doubled = (
        delivered.filter(pl.col("is_primary").fill_null(value=False))
        .group_by("person_id", "location_type")
        .len()
        .filter(pl.col("len") > 1)
    )
    if doubled.height > 0:
        msg = f"{doubled.height} person-kind(s) have more than one primary reported location."
        raise ValueError(msg)


def add_observed_locations(
    delivered: pl.DataFrame,
    linked_trips: pl.DataFrame,
    person_days: pl.DataFrame,
    config: HabitualLocationConfig,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Append observed locations to the delivered ones, and build the day table.

    Args:
        delivered: The delivered ``habitual_locations``: reported rows only.
        linked_trips: Linked trips with coordinates, purposes, times and
            ``d_activity_duration``.
        person_days: Days with ``person_day_id``, ``begin_day`` and ``end_day``.
        config: Detection and matching rules, including whether to add
            observed homes.

    Returns:
        ``(habitual_locations, habitual_location_days)``: the delivered rows
        unchanged, the observed ones after them.
    """
    _check_delivered(delivered)

    # The locations delivered or cleaned by vendors (reported ones only)
    delivered = delivered.select(LOCATION_COLUMNS).with_columns(
        pl.col("person_id", "location_type", "location_num", "source").cast(pl.Int64),
        pl.col("habitual_location_id").cast(pl.Int64),
        pl.col("lat", "lon").cast(pl.Float64),
    )

    # Raw episodes of presence derived from linked trips
    episodes = build_presence_episodes(linked_trips)

    # Identify observed habitual locations meeting criteria
    homes = (
        [observed_homes(episodes, person_days, delivered, config)]
        if config.include_observed_homes
        else []
    )
    found = pl.concat(
        [
            *homes,
            *(
                observed_locations(episodes, delivered, location_type, config)
                for location_type in OBSERVED_KINDS
            ),
        ],
        how="vertical",
    )

    # Combine delivered and observed locations, ensuring unique location numbers per person and type
    locations = pl.concat(
        [delivered, number_observed(found, delivered)], how="vertical_relaxed"
    ).sort(["person_id", "location_type", "location_num"])

    # Build the habitual location days table based on the combined locations
    location_days = build_location_days(episodes, locations, config, person_days)

    logger.info(
        "Habitual locations: %d reported, %d observed, with %d location-days",
        delivered.height,
        locations.height - delivered.height,
        location_days.height,
    )
    return locations, location_days


@step(
    requires={
        "habitual_locations": {
            "habitual_location_id",
            "person_id",
            "location_type",
            "location_num",
            "is_primary",
            "lat",
            "lon",
            "source",
        },
        "linked_trips": {
            "linked_trip_id",
            "person_id",
            "person_day_id",
            "depart_time",
            "arrive_time",
            "o_lat",
            "o_lon",
            "d_lat",
            "d_lon",
            "o_purpose",
            "o_purpose_category",
            "d_purpose",
            "d_purpose_category",
            "d_activity_duration",
        },
        "person_days": {"person_day_id", "begin_day", "end_day"},
    },
    produces={
        "habitual_locations": {"habitual_location_id"},
        "habitual_location_days": {"habitual_location_id"},
    },
)
def detect_habitual_locations(
    tables: Tables,
    locations: HabitualLocationConfig,
) -> dict[str, pl.DataFrame]:
    """Find observed homes, workplaces and schools, and record presence per day.

    Runs after trip linking. The table arrives from survey cleaning holding the
    reported locations (see ``reported_habitual_locations``); this appends what
    the travel shows, and never changes a delivered row. See the package
    docstring for the rules.

    Args:
        tables: ``habitual_locations`` (the delivered table, reported locations
            only), ``linked_trips`` and ``person_days``.
        locations: Detection and matching rules, including whether to add
            observed homes. See ``HabitualLocationConfig``.

    Returns:
        ``habitual_locations`` and ``habitual_location_days``.
    """
    observed, location_days = add_observed_locations(
        tables["habitual_locations"],
        tables["linked_trips"],
        tables["person_days"],
        locations,
    )
    return {"habitual_locations": observed, "habitual_location_days": location_days}
