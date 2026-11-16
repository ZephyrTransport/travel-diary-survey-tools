"""Tests for the habitual-location tables and the trip-end match."""

from datetime import datetime, timedelta

import polars as pl
import pytest
from pydantic import ValidationError

from data_canon.codebook.days import BeginEndDay
from data_canon.codebook.generic import LocationSource, LocationType
from data_canon.codebook.trips import Purpose, PurposeToCategoryMap
from data_canon.models.survey import HabitualLocationDayModel, HabitualLocationModel
from data_canon.validation.custom import check_reported_locations_match_coordinates
from processing.habitual_locations import (
    HabitualLocationConfig,
    MatchConfig,
    add_observed_locations,
    match_trip_ends,
    reported_habitual_locations,
)
from processing.habitual_locations.episodes import build_presence_episodes

HOME = (37.80, -122.40)
USUAL_WORK = (37.85, -122.45)
ALT_WORK = (37.95, -122.55)
ALT_SCHOOL = (37.99, -122.59)
OTHER_HOME = (37.60, -122.20)
REPORTED, OBSERVED = LocationSource.REPORTED.value, LocationSource.OBSERVED.value

# Degrees of latitude per metre, for placing points a known distance apart.
_DEG_PER_M = 1 / 111_320
# The detection rules as the BATS configs state them.
_CONFIG = HabitualLocationConfig(buffer_meters=300.0, include_observed_homes=True)


def _offset(point: tuple[float, float], meters: float) -> tuple[float, float]:
    """Return a point shifted north by the given number of metres."""
    return (point[0] + meters * _DEG_PER_M, point[1])


def _households() -> pl.DataFrame:
    """Two households, one person each."""
    return pl.DataFrame(
        {"hh_id": [1, 2], "home_lat": [HOME[0], 37.70], "home_lon": [HOME[1], -122.30]}
    )


def _persons(work: tuple[float, float] = USUAL_WORK) -> pl.DataFrame:
    """Person 1 has work and school coordinates; person 2 has neither."""
    return pl.DataFrame(
        {
            "person_id": [1, 2],
            "hh_id": [1, 2],
            "work_lat": [work[0], None],
            "work_lon": [work[1], None],
            "school_lat": [37.90, None],
            "school_lon": [-122.50, None],
        }
    )


def _delivered(extra: pl.DataFrame | None = None, persons=None) -> pl.DataFrame:
    """The table as survey cleaning delivers it."""
    return reported_habitual_locations(
        _households(), persons if persons is not None else _persons(), extra=extra
    )


def _second_home(lat_lon: tuple[float, float], person_id: int = 1) -> pl.DataFrame:
    """A further reported home, as cleaning hands it to the helper."""
    return pl.DataFrame(
        {
            "person_id": [person_id],
            "location_type": [LocationType.HOME.value],
            "lat": [lat_lon[0]],
            "lon": [lat_lon[1]],
        }
    )


def _trip(
    person_id: int,
    person_day_id: int,
    dest: tuple[float, float],
    dwell: int | None,
    purpose: Purpose = Purpose.PRIMARY_WORKPLACE,
    origin: tuple[float, float] = HOME,
    origin_purpose: Purpose = Purpose.HOME,
    hour: int = 8,
) -> dict:
    """Build one linked-trip row with the columns the builders read."""
    return {
        "person_id": person_id,
        "person_day_id": person_day_id,
        "depart_time": datetime(2023, 5, 1) + timedelta(days=person_day_id, hours=hour),
        "arrive_time": datetime(2023, 5, 1) + timedelta(days=person_day_id, hours=hour, minutes=30),
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


def _trips(*rows: dict) -> pl.DataFrame:
    """Linked trips from rows, numbered in the order given."""
    return pl.DataFrame(list(rows)).with_row_index("linked_trip_id", offset=1)


def _days(*rows: tuple[int, BeginEndDay | None, BeginEndDay | None]) -> pl.DataFrame:
    """Days as (person_day_id, begin_day, end_day)."""
    return pl.DataFrame(
        {
            "person_day_id": [r[0] for r in rows],
            "begin_day": [r[1].value if r[1] else None for r in rows],
            "end_day": [r[2].value if r[2] else None for r in rows],
        },
        schema={"person_day_id": pl.Int64, "begin_day": pl.Int64, "end_day": pl.Int64},
    )


def _of(locations: pl.DataFrame, person_id: int, kind: LocationType) -> pl.DataFrame:
    """One person's locations of one kind, in number order."""
    return locations.filter(
        (pl.col("person_id") == person_id) & (pl.col("location_type") == kind.value)
    ).sort("location_num")


def _not_asked(trips: pl.DataFrame) -> pl.DataFrame:
    """Days for the trips, with no stated start or end."""
    return _days(*((d, None, None) for d in sorted(set(trips["person_day_id"].to_list()))))


def _build(trips, person_days=None, extra=None, persons=None, *, observed_homes=True, **config):
    """Deliver the table for the standard two people, then detect."""
    return add_observed_locations(
        _delivered(extra, persons),
        trips,
        person_days if person_days is not None else _not_asked(trips),
        HabitualLocationConfig(
            **{**_CONFIG.model_dump(), "include_observed_homes": observed_homes, **config}
        ),
    )


def test_delivered_rows_reproduce_the_coordinate_columns():
    """Each reported place becomes a numbered primary row; missing ones make none."""
    delivered = _delivered()

    assert delivered.height == 4
    p1 = delivered.filter(pl.col("person_id") == 1).sort("location_type")
    assert p1["location_type"].to_list() == [
        LocationType.HOME.value,
        LocationType.WORK.value,
        LocationType.SCHOOL.value,
    ]
    work = p1.filter(pl.col("location_type") == LocationType.WORK.value)
    assert (work["lat"].item(), work["lon"].item()) == USUAL_WORK
    assert work["habitual_location_id"].item() == 1 * 1000 + LocationType.WORK.value * 100 + 1
    assert delivered["source"].unique().to_list() == [REPORTED]
    assert delivered["is_primary"].unique().to_list() == [True]
    for row in delivered.iter_rows(named=True):
        HabitualLocationModel(**row)


def test_second_home_follows_the_primary():
    """A vendor's second home is a reported home, numbered 2."""
    homes = _of(_delivered(_second_home(OTHER_HOME)), 1, LocationType.HOME)
    assert homes["is_primary"].to_list() == [True, False]
    assert homes["source"].to_list() == [REPORTED] * 2
    assert (homes["lat"][1], homes["lon"][1]) == OTHER_HOME


def test_second_home_beside_the_primary_is_dropped():
    """Within the buffer the two cannot be told apart, so only the primary stays."""
    assert _of(_delivered(_second_home(_offset(HOME, 100))), 1, LocationType.HOME).height == 1


def test_delivered_rows_are_never_changed():
    """Detection appends; every delivered row comes out exactly as it went in."""
    delivered = _delivered(_second_home(OTHER_HOME))
    locations, _ = _build(
        _trips(_trip(1, 10, ALT_WORK, 200), _trip(1, 11, _offset(USUAL_WORK, 100), 200)),
        extra=_second_home(OTHER_HOME),
    )
    reported = locations.filter(pl.col("source") == REPORTED)
    assert reported.equals(delivered)
    assert locations.filter(pl.col("source") == OBSERVED).height == 1


def test_detection_refuses_a_delivered_observed_row():
    """Observed locations are detection's to find, not the vendor's."""
    delivered = _delivered(_second_home(OTHER_HOME)).with_columns(
        pl.when(pl.col("location_num") == 2)
        .then(pl.lit(OBSERVED))
        .otherwise(pl.col("source"))
        .alias("source")
    )
    trips = _trips(_trip(1, 10, ALT_WORK, 200))
    with pytest.raises(ValueError, match="not REPORTED"):
        add_observed_locations(delivered, trips, _not_asked(trips), _CONFIG)


def test_detection_refuses_two_primaries_of_one_kind():
    """A primary second home would contradict the household's reported home."""
    delivered = _delivered(_second_home(OTHER_HOME)).with_columns(
        pl.lit(value=True).alias("is_primary")
    )
    trips = _trips(_trip(1, 10, ALT_WORK, 200))
    with pytest.raises(ValueError, match="more than one primary"):
        add_observed_locations(delivered, trips, _not_asked(trips), _CONFIG)


@pytest.mark.parametrize(
    ("work_lat", "agrees"),
    [
        (pl.col("work_lat"), True),
        (pl.col("work_lat") + 1e-9, True),  # a float round-trip, not a move
        (pl.col("work_lat") + 0.01, False),  # the coordinates moved, the row did not
        (pl.lit(None, dtype=pl.Float64), False),  # a row with no coordinates behind it
    ],
    ids=["same", "rounding", "moved", "missing"],
)
def test_validation_holds_rows_and_coordinate_columns_equal(work_lat, agrees):
    """Other steps still read the coordinate columns; the rows may never drift."""
    persons = _persons().with_columns(work_lat.alias("work_lat"))
    errors = check_reported_locations_match_coordinates(_delivered(), _households(), persons)
    assert (errors == []) is agrees


def test_episodes_include_each_days_first_origin():
    """The day's first origin is a stay; later origins repeat a destination."""
    episodes = build_presence_episodes(
        _trips(
            _trip(1, 10, ALT_WORK, 120, hour=8),
            _trip(1, 10, ALT_SCHOOL, 60, origin=ALT_WORK, hour=13),
        )
    )
    assert episodes.height == 3
    starts = episodes.filter(pl.col("is_day_start"))
    assert starts["lat"].item() == HOME[0]
    # a stay that began before the diary day has no known length
    assert starts["dwell_minutes"].item() is None


def test_long_primary_workplace_stay_makes_a_workplace():
    """A 90+ minute stop at a place called the workplace is an observed workplace."""
    locations, _ = _build(_trips(_trip(1, 10, ALT_WORK, 120)))
    work = _of(locations, 1, LocationType.WORK)
    assert work["source"].to_list() == [REPORTED, OBSERVED]
    assert work["is_primary"].to_list() == [True, False]
    assert abs(work["lat"][1] - ALT_WORK[0]) < 1e-9


@pytest.mark.parametrize(
    ("dwell", "purpose", "made"),
    [
        (60, Purpose.PRIMARY_WORKPLACE, False),  # too short
        (120, Purpose.PRIMARY_WORKPLACE, True),
        (120, Purpose.WORK_ACTIVITY, False),  # a meeting, not a place of work
        (480, Purpose.WORK_ACTIVITY, True),  # a working day says they work there
        (480, Purpose.GROCERY, False),  # not a work purpose at all
    ],
)
def test_a_workplace_needs_a_long_enough_stop_of_a_work_purpose(dwell, purpose, made):
    """Work-related stops count, but only at their own four-hour cutoff."""
    locations, _ = _build(_trips(_trip(1, 10, ALT_WORK, dwell, purpose=purpose)))
    assert (_of(locations, 1, LocationType.WORK).height == 2) is made


def test_stay_length_cutoff_is_tunable():
    """60 minutes fails the default 90, passes 45."""
    trips = _trips(_trip(1, 10, ALT_WORK, 60))
    assert _of(_build(trips)[0], 1, LocationType.WORK).height == 1
    assert _of(_build(trips, min_dwell_minutes=45)[0], 1, LocationType.WORK).height == 2


@pytest.mark.parametrize(
    ("purpose", "made"),
    [
        (Purpose.COLLEGE, True),  # a class and leave: 45 minutes is enough
        (Purpose.K12_SCHOOL, False),  # the general 90 minutes applies
        (Purpose.OTHER_CLASS, False),  # school-related never makes a school
    ],
)
def test_school_stay_cutoff_is_per_purpose(purpose, made):
    """College has its own lower cutoff; school-related purposes make nothing."""
    locations, _ = _build(_trips(_trip(1, 10, ALT_SCHOOL, 50, purpose=purpose)))
    assert (_of(locations, 1, LocationType.SCHOOL).height == 2) is made


def test_nearby_repeat_visits_form_one_location():
    """GPS scatter around one building is one location, not several."""
    locations, _ = _build(
        _trips(
            _trip(1, 10, ALT_WORK, 200),
            _trip(1, 11, _offset(ALT_WORK, 150), 200),
            _trip(1, 12, _offset(ALT_WORK, 280), 200),
        )
    )
    assert _of(locations, 1, LocationType.WORK).height == 2


def test_distinct_worksites_stay_separate():
    """Two places beyond the buffer of one another remain two locations."""
    locations, _ = _build(
        _trips(_trip(1, 10, ALT_WORK, 200), _trip(1, 11, _offset(ALT_WORK, 400), 200))
    )
    assert _of(locations, 1, LocationType.WORK).height == 3


def test_cluster_at_the_reported_workplace_is_not_added_again():
    """A stop within the buffer of the reported workplace is that workplace."""
    locations, _ = _build(
        _trips(_trip(1, 10, _offset(USUAL_WORK, 250), 200), _trip(1, 11, USUAL_WORK, 200))
    )
    work = _of(locations, 1, LocationType.WORK)
    assert work["source"].to_list() == [REPORTED]
    assert (work["lat"].item(), work["lon"].item()) == USUAL_WORK


def test_travel_alone_never_makes_a_home():
    """Nights at "another residence" may be a friend's flat: only the respondent knows."""
    trips = _trips(
        _trip(1, 10, OTHER_HOME, None, purpose=Purpose.OTHER_RESIDENCE, hour=19),
        _trip(1, 11, OTHER_HOME, None, purpose=Purpose.OTHER_RESIDENCE, hour=19),
    )
    assert _of(_build(trips)[0], 1, LocationType.HOME).height == 1


@pytest.mark.parametrize("answer", [BeginEndDay.OTHER_HOME, BeginEndDay.HOME])
def test_day_ending_at_a_home_away_from_the_reported_one_is_another_home(answer):
    """They said the day ended at home, and it is not the one they reported."""
    trips = _trips(_trip(1, 10, OTHER_HOME, None, purpose=Purpose.OTHER_RESIDENCE, hour=19))
    locations, _ = _build(trips, _days((10, None, answer)))
    homes = _of(locations, 1, LocationType.HOME)
    assert homes["source"].to_list() == [REPORTED, OBSERVED]
    assert homes["is_primary"].to_list() == [True, False]
    assert abs(homes["lat"][1] - OTHER_HOME[0]) < 1e-9


@pytest.mark.parametrize("answer", [BeginEndDay.OTHER_HOME, BeginEndDay.HOME])
def test_observed_homes_off_keeps_only_the_reported_homes(answer):
    """Off, a stated day end adds no home; workplaces are still found."""
    trips = _trips(
        _trip(1, 10, OTHER_HOME, None, purpose=Purpose.OTHER_RESIDENCE, hour=19),
        _trip(1, 11, ALT_WORK, 200),
    )
    locations, _ = _build(trips, _days((10, None, answer), (11, None, None)), observed_homes=False)
    assert _of(locations, 1, LocationType.HOME)["source"].to_list() == [REPORTED]
    assert OBSERVED in _of(locations, 1, LocationType.WORK)["source"].to_list()


def test_observed_homes_has_no_default():
    """Whether to trust stated day ends is the analyst's call, so it must be stated."""
    with pytest.raises(ValidationError, match="include_observed_homes"):
        HabitualLocationConfig(buffer_meters=300.0)


def test_day_beginning_at_another_home_places_it_at_the_first_origin():
    """The first origin is where the day began."""
    trips = _trips(_trip(1, 10, ALT_WORK, 30, purpose=Purpose.GROCERY, origin=OTHER_HOME))
    locations, _ = _build(trips, _days((10, BeginEndDay.OTHER_HOME, None)))
    assert _of(locations, 1, LocationType.HOME).height == 2


def test_day_ending_at_the_reported_home_adds_nothing():
    """Within the buffer of the reported home, the stated home is that home."""
    trips = _trips(_trip(1, 10, _offset(HOME, 200), -1, purpose=Purpose.HOME, origin=ALT_WORK))
    locations, _ = _build(trips, _days((10, None, BeginEndDay.HOME)))
    assert _of(locations, 1, LocationType.HOME).height == 1


def test_stated_home_at_the_reported_second_home_is_not_added_again():
    """A vendor second home and the day answers naming it are one home."""
    trips = _trips(_trip(1, 10, OTHER_HOME, None, purpose=Purpose.OTHER_RESIDENCE, hour=19))
    locations, _ = _build(
        trips, _days((10, None, BeginEndDay.OTHER_HOME)), extra=_second_home(OTHER_HOME)
    )
    homes = _of(locations, 1, LocationType.HOME)
    assert homes["source"].to_list() == [REPORTED] * 2


def _match(trips, person_days=None, extra=None, persons=None) -> dict:
    """Build the locations, match the trip ends, and return the last trip's row."""
    person_days = person_days if person_days is not None else _not_asked(trips)
    locations, _ = _build(trips, person_days, extra, persons)
    matched = match_trip_ends(
        trips, locations, MatchConfig(buffer_meters=_CONFIG.buffer_meters), person_days
    )
    return matched.sort("linked_trip_id").row(-1, named=True)


@pytest.mark.parametrize(
    ("purpose", "meters", "at_home"),
    [
        (Purpose.HOME, 250, True),  # agrees, within the buffer
        (Purpose.HOME, 400, False),  # agrees, but too far
        (Purpose.GROCERY, 250, False),  # the corner store is not home
        (Purpose.MISSING, 250, False),  # unknown agrees with nothing
        (Purpose.PNTA, 250, False),
        (Purpose.GROCERY, 50, True),  # at their own front door, whatever they did
        (Purpose.MISSING, 50, True),
        (Purpose.EXERCISE, 50, True),
    ],
)
def test_a_trip_end_is_home_by_purpose_beyond_the_doorstep_and_by_distance_at_it(
    purpose, meters, at_home
):
    """Beyond the address, distance and purpose must both say home; at it, distance is enough."""
    row = _match(_trips(_trip(1, 10, _offset(HOME, meters), -1, purpose=purpose, origin=ALT_WORK)))
    assert (row["_d_home_id"] is not None) is at_home


def test_the_doorstep_rule_is_for_the_primary_home_only():
    """A second home is placed from trip ends, so proximity alone would over-attribute."""
    second_home = _second_home(OTHER_HOME)
    row = _match(
        _trips(_trip(1, 10, _offset(OTHER_HOME, 50), 60, purpose=Purpose.GROCERY, origin=HOME)),
        extra=second_home,
    )
    assert row["_d_home_id"] is None


def test_lunch_by_the_office_is_not_work():
    """Within the buffer of the workplace, but the purpose says otherwise."""
    row = _match(_trips(_trip(1, 10, _offset(USUAL_WORK, 100), 45, purpose=Purpose.DINING)))
    assert row["_d_reported_work_id"] is None


def test_a_work_related_stop_at_the_workplace_is_at_work():
    """Respondents call their own workplace work-related; the distance says which place."""
    at_work = _match(
        _trips(_trip(1, 10, _offset(USUAL_WORK, 100), 45, purpose=Purpose.WORK_ACTIVITY))
    )
    elsewhere = _match(_trips(_trip(1, 10, ALT_WORK, 45, purpose=Purpose.WORK_ACTIVITY)))
    assert at_work["_d_reported_work_id"] is not None
    assert elsewhere["_d_reported_work_id"] is None


def test_other_residence_agrees_only_with_a_home_that_is_not_the_primary():
    """It is how people code arriving at their second home, never their main one."""
    second_home = _second_home(OTHER_HOME)
    at_second = _match(
        _trips(_trip(1, 10, OTHER_HOME, None, purpose=Purpose.OTHER_RESIDENCE)), extra=second_home
    )
    at_primary = _match(
        _trips(
            _trip(
                1,
                10,
                _offset(HOME, 250),
                -2,
                purpose=Purpose.OTHER_RESIDENCE,
                origin=ALT_WORK,
            )
        ),
        extra=second_home,
    )
    assert at_second["_d_at_other_home"] is True
    assert at_primary["_d_home_id"] is None


def test_stated_home_counts_where_the_trip_purpose_describes_the_activity():
    """Working from home: the last trip says work, the day says it ended at home."""
    trips = _trips(
        _trip(1, 10, _offset(HOME, 250), -2, purpose=Purpose.PRIMARY_WORKPLACE, origin=ALT_WORK)
    )
    assert _match(trips)["_d_home_id"] is None
    assert _match(trips, _days((10, None, BeginEndDay.HOME)))["_d_home_id"] is not None


def test_stated_work_is_not_evidence():
    """A day said to end at work does not make a shopping trip beside it the workplace."""
    trips = _trips(_trip(1, 10, _offset(USUAL_WORK, 100), -2, purpose=Purpose.GROCERY))
    row = _match(trips, _days((10, None, BeginEndDay.WORK)))
    assert row["_d_reported_work_id"] is None


def test_a_home_office_is_both_home_and_work():
    """Kinds are matched separately, so one end can be at a home and a workplace."""
    trips = _trips(_trip(1, 10, HOME, -2, purpose=Purpose.PRIMARY_WORKPLACE, origin=ALT_SCHOOL))
    row = _match(trips, _days((10, None, BeginEndDay.HOME)), persons=_persons(work=HOME))
    assert row["_d_home_id"] is not None
    assert row["_d_reported_work_id"] is not None


def test_observed_locations_are_numbered_by_first_visit():
    """Reported first; then whichever place the person went to first."""
    later, earlier = ALT_WORK, _offset(ALT_WORK, 2000)
    locations, _ = _build(_trips(_trip(1, 11, later, 200), _trip(1, 10, earlier, 200)))
    work = _of(locations, 1, LocationType.WORK)
    assert work["location_num"].to_list() == [1, 2, 3]
    assert abs(work["lat"][1] - earlier[0]) < 1e-9


def test_tables_do_not_depend_on_row_order():
    """The same trips in another order give the same tables, bit for bit."""
    rows = [
        _trip(1, 10, ALT_WORK, 200),
        _trip(1, 10, _offset(ALT_WORK, 120), 200, origin=ALT_WORK, hour=14),
        _trip(1, 11, _offset(ALT_WORK, 2000), 200),
        _trip(1, 12, ALT_SCHOOL, 100, purpose=Purpose.COLLEGE),
        _trip(1, 12, OTHER_HOME, None, purpose=Purpose.OTHER_RESIDENCE, origin=ALT_SCHOOL, hour=18),
    ]
    person_days = _days((10, None, None), (11, None, None), (12, None, BeginEndDay.OTHER_HOME))
    forward = _build(_trips(*rows), person_days)
    backward = _build(_trips(*reversed(rows)), person_days)
    assert forward[0].equals(backward[0])
    assert forward[1].equals(backward[1])


def test_observed_location_without_reported_primary_has_unknown_primacy():
    """With no reported location of that kind, observed primacy is unknown (None)."""
    locations, _ = _build(_trips(_trip(2, 20, ALT_WORK, 200), _trip(2, 21, USUAL_WORK, 210)))
    work = _of(locations, 2, LocationType.WORK)
    assert work["source"].to_list() == [OBSERVED] * 2
    assert work["is_primary"].to_list() == [None, None]


def test_observed_identifiers_continue_after_the_delivered_ones():
    """A stated home comes after the delivered primary and second home, as number 3."""
    locations, _ = _build(
        _trips(_trip(1, 10, _offset(OTHER_HOME, 5000), -2, purpose=Purpose.OTHER_RESIDENCE)),
        _days((10, None, BeginEndDay.OTHER_HOME)),
        extra=_second_home(OTHER_HOME),
    )
    homes = _of(locations, 1, LocationType.HOME)
    assert homes["location_num"].to_list() == [1, 2, 3]
    assert homes["source"].to_list() == [REPORTED, REPORTED, OBSERVED]
    assert homes["habitual_location_id"][2] == 1 * 1000 + LocationType.HOME.value * 100 + 3
    assert locations["habitual_location_id"].n_unique() == locations.height


def test_day_row_counts_visits_and_totals_dwell():
    """Two stays in a day are one row: two visits, dwell summed."""
    _locations, person_days = _build(
        _trips(
            _trip(1, 10, USUAL_WORK, 200, hour=8),
            _trip(1, 10, ALT_SCHOOL, 45, purpose=Purpose.DINING, origin=USUAL_WORK, hour=12),
            _trip(1, 10, USUAL_WORK, 100, origin=ALT_SCHOOL, hour=13),
        )
    )
    row = person_days.filter(pl.col("location_type") == LocationType.WORK.value).row(0, named=True)
    assert row["n_visits"] == 2
    assert row["dwell_minutes"] == 300


def test_unmeasurable_dwell_is_null_not_zero():
    """A stay with no recorded end is unknown-length, which is not zero-length."""
    _locations, person_days = _build(
        _trips(_trip(1, 10, HOME, -1, purpose=Purpose.HOME, origin=USUAL_WORK))
    )
    row = person_days.filter(~pl.col("is_day_start")).row(0, named=True)
    assert row["location_type"] == LocationType.HOME.value
    assert row["dwell_minutes"] is None


def test_day_rows_flag_where_the_day_began_and_ended():
    """The day's first origin and last destination are marked on their locations."""
    _locations, person_days = _build(
        _trips(
            _trip(1, 10, ALT_SCHOOL, 60, purpose=Purpose.DINING, hour=8),
            _trip(1, 10, HOME, -1, purpose=Purpose.HOME, origin=ALT_SCHOOL, hour=18),
        )
    )
    row = person_days.row(0, named=True)
    assert person_days.height == 1
    assert (row["is_day_start"], row["is_day_end"]) == (True, True)


def test_rows_conform_to_models():
    """Every row of both tables validates against its model."""
    locations, person_days = _build(
        _trips(
            _trip(1, 10, ALT_WORK, 120),
            _trip(1, 12, ALT_SCHOOL, 200, purpose=Purpose.K12_SCHOOL),
            _trip(1, 13, OTHER_HOME, None, purpose=Purpose.OTHER_RESIDENCE, hour=19),
        ),
        _days((10, None, None), (12, None, None), (13, None, BeginEndDay.OTHER_HOME)),
        extra=_second_home(_offset(OTHER_HOME, 5000)),
    )
    for row in locations.iter_rows(named=True):
        HabitualLocationModel(**row)
    for row in person_days.iter_rows(named=True):
        HabitualLocationDayModel(**row)
