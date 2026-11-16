"""A worker's usual workplace: the reported one, else the observed one with the most time there."""

import polars as pl

from data_canon.codebook.generic import LocationSource, LocationType
from data_canon.codebook.persons import Employment
from processing.formatting.usual_workplace import fill_usual_workplace

ZONES = ["taz"]
OBSERVED = LocationSource.OBSERVED.value
REPORTED = LocationSource.REPORTED.value
WORKS = Employment.EMPLOYED_FULLTIME.value
RETIRED = Employment.UNEMPLOYED_NOT_LOOKING.value


def _persons(*rows):
    """Persons as ``(person_id, work_lat, work_lon, work_taz, employment)``."""
    return pl.DataFrame(
        rows,
        schema={
            "person_id": pl.Int64,
            "work_lat": pl.Float64,
            "work_lon": pl.Float64,
            "work_taz": pl.Int64,
            "employment": pl.Int64,
        },
        orient="row",
    )


def _workplaces(*rows):
    """Habitual workplaces as ``(id, person_id, location_num, source, lat, lon, taz)``."""
    return pl.DataFrame(
        rows,
        schema={
            "habitual_location_id": pl.Int64,
            "person_id": pl.Int64,
            "location_num": pl.Int64,
            "source": pl.Int64,
            "lat": pl.Float64,
            "lon": pl.Float64,
            "taz": pl.Int64,
        },
        orient="row",
    ).with_columns(pl.lit(LocationType.WORK.value).alias("location_type"))


def _days(*rows):
    """Location-days as ``(habitual_location_id, dwell_minutes)``."""
    return pl.DataFrame(
        rows, schema={"habitual_location_id": pl.Int64, "dwell_minutes": pl.Float64}, orient="row"
    )


def _fill(persons, workplaces, person_days):
    return fill_usual_workplace(persons, workplaces, person_days, zone_names=ZONES).sort(
        "person_id"
    )


class TestUsualWorkplace:
    """Who gets a usual workplace from their trips, and which one."""

    def test_a_worker_with_no_reported_workplace_takes_the_observed_one(self):
        """Its coordinates and zone both stand in, so distances and TAZ agree."""
        persons = _persons((1, None, None, None, WORKS))
        workplaces = _workplaces((11, 1, 1, OBSERVED, 37.8, -122.4, 500))

        out = _fill(persons, workplaces, _days((11, 480.0)))

        assert out.row(0, named=True) == {
            "person_id": 1,
            "work_lat": 37.8,
            "work_lon": -122.4,
            "work_taz": 500,
            "employment": WORKS,
        }

    def test_a_reported_workplace_is_never_replaced(self):
        """What the respondent told us outranks what their trips suggest."""
        persons = _persons((1, 37.0, -122.0, 100, WORKS))
        workplaces = _workplaces((11, 1, 1, OBSERVED, 37.8, -122.4, 500))

        out = _fill(persons, workplaces, _days((11, 480.0)))

        assert out["work_taz"].to_list() == [100]
        assert out["work_lat"].to_list() == [37.0]

    def test_the_observed_workplace_with_the_most_time_there_wins(self):
        """Total time across all days, not the longest single stay."""
        persons = _persons((1, None, None, None, WORKS))
        workplaces = _workplaces(
            (11, 1, 1, OBSERVED, 37.1, -122.1, 501),
            (12, 1, 2, OBSERVED, 37.2, -122.2, 502),
        )
        person_days = _days((11, 300.0), (12, 250.0), (12, 250.0))

        assert _fill(persons, workplaces, person_days)["work_taz"].to_list() == [502]

    def test_a_tie_goes_to_the_lowest_location_number(self):
        """Row order must not decide the answer."""
        persons = _persons((1, None, None, None, WORKS))
        workplaces = _workplaces(
            (12, 1, 2, OBSERVED, 37.2, -122.2, 502),
            (11, 1, 1, OBSERVED, 37.1, -122.1, 501),
        )
        person_days = _days((11, 300.0), (12, 300.0))

        assert _fill(persons, workplaces, person_days)["work_taz"].to_list() == [501]

    def test_someone_not_employed_gets_none(self):
        """A retiree's long volunteering stop is not a usual workplace."""
        persons = _persons((1, None, None, None, RETIRED))
        workplaces = _workplaces((11, 1, 1, OBSERVED, 37.8, -122.4, 500))

        out = _fill(persons, workplaces, _days((11, 480.0)))

        assert out["work_taz"].to_list() == [None]

    def test_a_reported_workplace_in_the_table_is_not_an_observed_one(self):
        """Only observed workplaces stand in; a reported one is already on the person."""
        persons = _persons((1, None, None, None, WORKS))
        workplaces = _workplaces((11, 1, 1, REPORTED, 37.8, -122.4, 500))

        out = _fill(persons, workplaces, _days((11, 480.0)))

        assert out["work_taz"].to_list() == [None]
