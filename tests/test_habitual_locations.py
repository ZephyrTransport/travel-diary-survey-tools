"""Tests for the habitual-location tables (issue #71).

Covers:
- Reported scalar locations unpivot into tall rows and reproduce the input coords
- "Same place" is a radius, so nearby repeat visits form one location rather than
  fragmenting, and a cluster coinciding with a reported location is not re-added
- Observed work/school are promoted on dwell; observed home is promoted on
  staying overnight or returning across days, since home dwell is never measured
- The day table records presence facts at the right grain, including the
  distinction between an unmeasurable stay and a zero-length one
"""

from datetime import datetime, timedelta

import polars as pl

from data_canon.codebook.generic import LocationSource, LocationType
from data_canon.codebook.trips import Purpose, PurposeToCategoryMap
from data_canon.models.survey import HabitualLocationDayModel, HabitualLocationModel
from processing.tours.habitual_locations import (
    HabitualLocationConfig,
    build_habitual_locations,
    build_presence_episodes,
    build_reported_locations,
    derive_observed_locations,
)

HOME = (37.80, -122.40)
USUAL_WORK = (37.85, -122.45)
ALT_WORK = (37.95, -122.55)
ALT_SCHOOL = (37.99, -122.59)
OTHER_HOME = (37.60, -122.20)

# Degrees of latitude per metre, for placing points a known distance apart.
_DEG_PER_M = 1 / 111_320


def _offset(point: tuple[float, float], meters: float) -> tuple[float, float]:
    """Return a point shifted north by the given number of metres."""
    return (point[0] + meters * _DEG_PER_M, point[1])


def _person_locations() -> pl.DataFrame:
    """Two people: person 1 has home+work+school, person 2 only home (nulls)."""
    return pl.DataFrame(
        {
            "person_id": [1, 2],
            "home_lat": [HOME[0], 37.70],
            "home_lon": [HOME[1], -122.30],
            "work_lat": [USUAL_WORK[0], None],
            "work_lon": [USUAL_WORK[1], None],
            "school_lat": [37.90, None],
            "school_lon": [-122.50, None],
        }
    )


def _trip(
    person_id: int,
    day_id: int,
    dest: tuple[float, float],
    dwell: int,
    purpose: Purpose = Purpose.WORK_ACTIVITY,
    origin: tuple[float, float] = HOME,
    origin_purpose: Purpose = Purpose.HOME,
    hour: int = 8,
) -> dict:
    """Build one linked-trip row with the columns the builders read."""
    return {
        "person_id": person_id,
        "day_id": day_id,
        "depart_time": datetime(2023, 5, 1) + timedelta(days=day_id, hours=hour),
        "o_lat": origin[0],
        "o_lon": origin[1],
        "o_purpose": origin_purpose.value,
        "o_purpose_category": PurposeToCategoryMap.PURPOSE_TO_CATEGORY[origin_purpose].value,
        "d_lat": dest[0],
        "d_lon": dest[1],
        "d_purpose": purpose.value,
        "d_purpose_category": PurposeToCategoryMap.PURPOSE_TO_CATEGORY[purpose].value,
        "d_activity_duration": dwell,
    }


def _no_reported() -> pl.DataFrame:
    """An empty reported table with the right columns, for seedless clustering."""
    return build_reported_locations(
        pl.DataFrame(
            {"person_id": [1], "home_lat": [None], "home_lon": [None]},
            schema={"person_id": pl.Int64, "home_lat": pl.Float64, "home_lon": pl.Float64},
        )
    )


# --- build_reported_locations -----------------------------------------------


def test_reported_locations_reproduce_scalar_coordinates():
    """Reported scalars unpivot to tall rows preserving type, coords, provenance."""
    reported = build_reported_locations(_person_locations())

    # person 1: home, work, school. person 2: home only (work/school null skipped)
    assert reported.height == 4
    p1 = reported.filter(pl.col("person_id") == 1).sort("location_type")
    assert p1["location_type"].to_list() == [
        LocationType.HOME.value,
        LocationType.WORK.value,
        LocationType.SCHOOL.value,
    ]
    work = p1.filter(pl.col("location_type") == LocationType.WORK.value)
    assert work["lat"].item() == USUAL_WORK[0]
    assert work["lon"].item() == USUAL_WORK[1]

    assert reported["source"].unique().to_list() == [LocationSource.REPORTED.value]
    assert reported["is_primary"].unique().to_list() == [True]


def test_reported_locations_skip_null_coordinates():
    """A person with only a home (null work/school) yields a single home row."""
    p2 = build_reported_locations(_person_locations()).filter(pl.col("person_id") == 2)
    assert p2.height == 1
    assert p2["location_type"].item() == LocationType.HOME.value


# --- build_presence_episodes ------------------------------------------------


def test_episodes_include_each_days_first_origin():
    """The day's first origin is a stay; later origins repeat a destination."""
    trips = pl.DataFrame(
        [
            _trip(1, 10, ALT_WORK, 120, hour=8),
            _trip(1, 10, ALT_SCHOOL, 60, origin=ALT_WORK, hour=13),
        ]
    )
    episodes = build_presence_episodes(trips)

    # two destinations plus one first origin
    assert episodes.height == 3
    starts = episodes.filter(pl.col("is_day_start"))
    assert starts.height == 1
    assert starts["lat"].item() == HOME[0]
    # a stay that began before the diary day has no known length
    assert starts["dwell_minutes"].item() is None


def test_episodes_flag_the_last_arrival_of_the_day():
    """is_day_end marks where the person was when the diary day ran out."""
    trips = pl.DataFrame(
        [
            _trip(1, 10, ALT_WORK, 120, hour=8),
            _trip(1, 10, OTHER_HOME, -2, origin=ALT_WORK, hour=18),
        ]
    )
    episodes = build_presence_episodes(trips)
    ends = episodes.filter(pl.col("is_day_end"))
    assert ends.height == 1
    assert ends["lat"].item() == OTHER_HOME[0]
    # -2 is a sentinel, not a duration
    assert ends["dwell_minutes"].item() is None


def test_episode_dwell_drops_sentinels():
    """Sentinel activity durations become null rather than negative dwell."""
    trips = pl.DataFrame([_trip(1, 10, HOME, -1, purpose=Purpose.HOME)])
    episodes = build_presence_episodes(trips)
    assert episodes.filter(~pl.col("is_day_start"))["dwell_minutes"].item() is None


# --- observed work and school (the dwell rule) ------------------------------


def test_worksite_with_sufficient_dwell_is_promoted():
    """A place with a long-enough stay becomes an OBSERVED WORK location."""
    trips = pl.DataFrame(
        [
            _trip(1, 10, ALT_WORK, 120),
            _trip(1, 11, ALT_WORK, 130),
        ]
    )
    observed = derive_observed_locations(build_presence_episodes(trips), _no_reported())
    assert observed.height == 1
    row = observed.row(0, named=True)
    assert row["location_type"] == LocationType.WORK.value
    assert row["source"] == LocationSource.OBSERVED.value
    assert row["is_primary"] is None
    assert abs(row["lat"] - ALT_WORK[0]) < 1e-6


def test_short_stays_do_not_promote_a_worksite():
    """A cluster of brief stops stays below the dwell cutoff and is dropped."""
    trips = pl.DataFrame(
        [
            _trip(1, 10, ALT_WORK, 8),
            _trip(1, 11, ALT_WORK, 12),
        ]
    )
    assert derive_observed_locations(build_presence_episodes(trips), _no_reported()).height == 0


def test_single_day_worksite_is_promoted():
    """Recurrence is not required: single-travel-day platforms still qualify."""
    trips = pl.DataFrame([_trip(1, 10, ALT_WORK, 300)])
    assert derive_observed_locations(build_presence_episodes(trips), _no_reported()).height == 1


def test_dwell_cutoff_is_tunable():
    """The dwell cutoff is the tuning knob: 20 min fails 90, passes 15."""
    trips = pl.DataFrame([_trip(1, 10, ALT_WORK, 20)])
    episodes = build_presence_episodes(trips)
    assert derive_observed_locations(episodes, _no_reported()).height == 0
    loose = HabitualLocationConfig(min_dwell_minutes=15)
    assert derive_observed_locations(episodes, _no_reported(), loose).height == 1


def test_discretionary_destinations_are_not_habitual_locations():
    """Shopping stops never become habitual locations however long they last."""
    trips = pl.DataFrame(
        [
            _trip(1, 10, ALT_WORK, 200, purpose=Purpose.GROCERY),
            _trip(1, 11, ALT_WORK, 200, purpose=Purpose.GROCERY),
        ]
    )
    assert derive_observed_locations(build_presence_episodes(trips), _no_reported()).height == 0


def test_school_pool_promotes_observed_school():
    """School + school-related destinations pool into an OBSERVED SCHOOL row."""
    trips = pl.DataFrame(
        [
            _trip(1, 10, ALT_SCHOOL, 200, purpose=Purpose.K12_SCHOOL),
            _trip(1, 11, ALT_SCHOOL, 210, purpose=Purpose.OTHER_CLASS),
        ]
    )
    observed = derive_observed_locations(build_presence_episodes(trips), _no_reported())
    assert observed.height == 1
    assert observed.row(0, named=True)["location_type"] == LocationType.SCHOOL.value


# --- observed home (the staying rule) ---------------------------------------


def test_other_residence_where_the_day_ended_is_promoted():
    """Sleeping somewhere is what makes it a home, and it has no measurable dwell."""
    trips = pl.DataFrame([_trip(1, 10, OTHER_HOME, -2, purpose=Purpose.OTHER_RESIDENCE, hour=19)])
    observed = derive_observed_locations(build_presence_episodes(trips), _no_reported())
    assert observed.height == 1
    row = observed.row(0, named=True)
    assert row["location_type"] == LocationType.HOME.value
    assert row["_max_dwell"] is None  # promoted on staying, not on duration


def test_other_residence_visited_across_days_is_promoted():
    """Returning on another day promotes a residence even with no overnight."""
    trips = pl.DataFrame(
        [
            _trip(1, 10, OTHER_HOME, 90, purpose=Purpose.OTHER_RESIDENCE),
            _trip(1, 11, OTHER_HOME, 60, purpose=Purpose.OTHER_RESIDENCE),
            # a later trip each day, so neither residence visit ends the day
            _trip(1, 10, ALT_WORK, 30, origin=OTHER_HOME, hour=15),
            _trip(1, 11, ALT_WORK, 30, origin=OTHER_HOME, hour=15),
        ]
    )
    observed = derive_observed_locations(build_presence_episodes(trips), _no_reported())
    homes = observed.filter(pl.col("location_type") == LocationType.HOME.value)
    assert homes.height == 1


def test_one_off_visit_to_another_residence_is_not_a_home():
    """Visiting a friend once is not habitual, however long you stay."""
    trips = pl.DataFrame(
        [
            _trip(1, 10, OTHER_HOME, 240, purpose=Purpose.OTHER_RESIDENCE),
            _trip(1, 10, ALT_WORK, 30, origin=OTHER_HOME, hour=15),
        ]
    )
    observed = derive_observed_locations(build_presence_episodes(trips), _no_reported())
    assert observed.filter(pl.col("location_type") == LocationType.HOME.value).height == 0


def test_temporary_lodging_is_not_a_home():
    """A hotel is somewhere you sleep but not a habitual location."""
    trips = pl.DataFrame([_trip(1, 10, OTHER_HOME, -2, purpose=Purpose.TEMP_LODGING, hour=19)])
    observed = derive_observed_locations(build_presence_episodes(trips), _no_reported())
    assert observed.height == 0


# --- "same place" is a radius ------------------------------------------------


def test_nearby_repeat_visits_form_one_location():
    """GPS scatter around one building is one location, not several.

    The previous grid-cell grouping split points across cell boundaries, which
    fragmented a real workplace into several locations, each with less evidence.
    """
    trips = pl.DataFrame(
        [
            _trip(1, 10, ALT_WORK, 200),
            _trip(1, 11, _offset(ALT_WORK, 40), 200),
            _trip(1, 12, _offset(ALT_WORK, 80), 200),
        ]
    )
    observed = derive_observed_locations(build_presence_episodes(trips), _no_reported())
    assert observed.height == 1


def test_distinct_worksites_stay_separate():
    """Two places beyond the radius of one another remain two locations."""
    trips = pl.DataFrame(
        [
            _trip(1, 10, ALT_WORK, 200),
            _trip(1, 11, _offset(ALT_WORK, 400), 200),
        ]
    )
    observed = derive_observed_locations(build_presence_episodes(trips), _no_reported())
    assert observed.height == 2


def test_cluster_on_the_reported_workplace_is_not_added_again():
    """The reported location seeds the clustering, so its cluster is not promoted."""
    trips = pl.DataFrame(
        [
            _trip(1, 10, USUAL_WORK, 200, purpose=Purpose.PRIMARY_WORKPLACE),
            _trip(1, 11, _offset(USUAL_WORK, 50), 200, purpose=Purpose.PRIMARY_WORKPLACE),
        ]
    )
    locations, _days = build_habitual_locations(_person_locations(), trips)
    work = locations.filter(
        (pl.col("person_id") == 1) & (pl.col("location_type") == LocationType.WORK.value)
    )
    assert work.height == 1
    assert work["source"].item() == LocationSource.REPORTED.value


def test_home_radius_is_generous_enough_for_geocoding_noise():
    """A stay a few hundred metres from the reported home is that same home."""
    trips = pl.DataFrame(
        [
            _trip(1, 10, _offset(HOME, 300), -2, purpose=Purpose.OTHER_RESIDENCE, hour=19),
            _trip(1, 11, _offset(HOME, 300), -2, purpose=Purpose.OTHER_RESIDENCE, hour=19),
        ]
    )
    locations, _days = build_habitual_locations(_person_locations(), trips)
    homes = locations.filter(
        (pl.col("person_id") == 1) & (pl.col("location_type") == LocationType.HOME.value)
    )
    assert homes.height == 1
    assert homes["source"].item() == LocationSource.REPORTED.value


# --- the combined tables -----------------------------------------------------


def test_reported_only_when_no_trips():
    """Without linked trips the table is exactly the reported locations."""
    locations, days = build_habitual_locations(_person_locations())
    assert locations.height == 4
    assert locations["source"].unique().to_list() == [LocationSource.REPORTED.value]
    assert locations["location_num"].unique().to_list() == [1]
    assert days.height == 0


def test_observed_worksite_numbered_after_reported_primary():
    """Reported work is location_num 1 (primary); observed work follows as 2."""
    trips = pl.DataFrame(
        [
            _trip(1, 10, ALT_WORK, 120),
            _trip(1, 11, ALT_WORK, 130),
        ]
    )
    locations, _days = build_habitual_locations(_person_locations(), trips)
    work = locations.filter(
        (pl.col("person_id") == 1) & (pl.col("location_type") == LocationType.WORK.value)
    ).sort("location_num")
    assert work["location_num"].to_list() == [1, 2]
    assert work["source"].to_list() == [
        LocationSource.REPORTED.value,
        LocationSource.OBSERVED.value,
    ]
    assert work["is_primary"].to_list() == [True, False]


def test_observed_location_without_reported_primary_has_unknown_primacy():
    """With no reported location of that kind, observed primacy is unknown (None)."""
    # person 2 has no reported work; a multi-site worker with no single office.
    trips = pl.DataFrame(
        [
            _trip(2, 20, ALT_WORK, 200),
            _trip(2, 21, USUAL_WORK, 210),
        ]
    )
    locations, _days = build_habitual_locations(_person_locations(), trips)
    work = locations.filter(
        (pl.col("person_id") == 2) & (pl.col("location_type") == LocationType.WORK.value)
    )
    assert work.height == 2
    assert work["source"].unique().to_list() == [LocationSource.OBSERVED.value]
    assert work["is_primary"].to_list() == [None, None]


def test_location_id_encodes_person_kind_and_number():
    """habitual_location_id packs the kind and number onto the person id."""
    locations, _days = build_habitual_locations(_person_locations())
    row = locations.filter(
        (pl.col("person_id") == 1) & (pl.col("location_type") == LocationType.WORK.value)
    ).row(0, named=True)
    assert row["habitual_location_id"] == 1 * 1000 + LocationType.WORK.value * 100 + 1
    assert locations["habitual_location_id"].n_unique() == locations.height


# --- the day table -----------------------------------------------------------


def test_day_rows_are_recorded_for_reported_locations_too():
    """Visits to the usual workplace are a lookup, not a re-derivation from trips."""
    trips = pl.DataFrame([_trip(1, 10, USUAL_WORK, 300, purpose=Purpose.PRIMARY_WORKPLACE)])
    locations, days = build_habitual_locations(_person_locations(), trips)
    work_id = locations.filter(
        (pl.col("person_id") == 1)
        & (pl.col("location_type") == LocationType.WORK.value)
        & (pl.col("source") == LocationSource.REPORTED.value)
    )["habitual_location_id"].item()
    assert days.filter(pl.col("habitual_location_id") == work_id).height == 1


def test_day_row_counts_visits_and_totals_dwell():
    """Two stays in a day are one row: two visits, dwell summed."""
    trips = pl.DataFrame(
        [
            _trip(1, 10, USUAL_WORK, 200, purpose=Purpose.PRIMARY_WORKPLACE, hour=8),
            _trip(1, 10, ALT_SCHOOL, 45, origin=USUAL_WORK, hour=12),
            _trip(
                1,
                10,
                USUAL_WORK,
                100,
                purpose=Purpose.PRIMARY_WORKPLACE,
                origin=ALT_SCHOOL,
                hour=13,
            ),
        ]
    )
    locations, days = build_habitual_locations(_person_locations(), trips)
    work_id = locations.filter(
        (pl.col("person_id") == 1)
        & (pl.col("location_type") == LocationType.WORK.value)
        & (pl.col("source") == LocationSource.REPORTED.value)
    )["habitual_location_id"].item()
    row = days.filter(pl.col("habitual_location_id") == work_id).row(0, named=True)
    assert row["n_visits"] == 2
    assert row["dwell_minutes"] == 300


def test_unmeasurable_dwell_is_null_not_zero():
    """A stay with no recorded end is unknown-length, which is not zero-length."""
    trips = pl.DataFrame([_trip(1, 10, HOME, -1, purpose=Purpose.HOME, origin=USUAL_WORK)])
    locations, days = build_habitual_locations(_person_locations(), trips)
    home_id = locations.filter(
        (pl.col("person_id") == 1) & (pl.col("location_type") == LocationType.HOME.value)
    )["habitual_location_id"].item()
    row = days.filter(pl.col("habitual_location_id") == home_id).row(0, named=True)
    assert row["dwell_minutes"] is None
    assert row["n_visits"] == 1


def test_day_rows_flag_where_the_day_began_and_ended():
    """The day's first origin and last destination are marked on their locations."""
    trips = pl.DataFrame(
        [
            _trip(1, 10, ALT_WORK, 200, origin=HOME, hour=8),
            _trip(1, 10, HOME, -1, purpose=Purpose.HOME, origin=ALT_WORK, hour=18),
        ]
    )
    locations, days = build_habitual_locations(_person_locations(), trips)
    home_id = locations.filter(
        (pl.col("person_id") == 1) & (pl.col("location_type") == LocationType.HOME.value)
    )["habitual_location_id"].item()
    row = days.filter(pl.col("habitual_location_id") == home_id).row(0, named=True)
    assert row["is_day_start"] is True
    assert row["is_day_end"] is True


def test_stays_away_from_habitual_locations_are_not_recorded():
    """The day table is about habitual locations, not all travel."""
    trips = pl.DataFrame([_trip(1, 10, ALT_SCHOOL, 45, purpose=Purpose.GROCERY, origin=HOME)])
    _locations, days = build_habitual_locations(_person_locations(), trips)
    # only the day's first origin (home) is at a habitual location
    assert days.height == 1
    assert days["location_type"].item() == LocationType.HOME.value


# --- schema conformance ------------------------------------------------------


def test_rows_conform_to_models():
    """Every row of both tables validates against its model."""
    trips = pl.DataFrame(
        [
            _trip(1, 10, ALT_WORK, 120),
            _trip(1, 11, ALT_WORK, 130),
            _trip(1, 12, ALT_SCHOOL, 200, purpose=Purpose.K12_SCHOOL),
            _trip(1, 13, OTHER_HOME, -2, purpose=Purpose.OTHER_RESIDENCE, hour=19),
            _trip(1, 14, OTHER_HOME, -2, purpose=Purpose.OTHER_RESIDENCE, hour=19),
        ]
    )
    locations, days = build_habitual_locations(_person_locations(), trips)
    for row in locations.iter_rows(named=True):
        HabitualLocationModel(**row)  # raises on any schema violation
    for row in days.iter_rows(named=True):
        HabitualLocationDayModel(**row)
