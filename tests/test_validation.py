"""Tests for data validation framework."""

from datetime import datetime

import polars as pl
import pytest

from data_canon.codebook.days import TravelDow
from data_canon.codebook.persons import AgeCategory, Gender
from data_canon.codebook.tours import TourDataQuality
from data_canon.codebook.trips import PurposeCategory
from data_canon.core.dataclass import CanonicalData
from data_canon.core.exceptions import DataValidationError
from data_canon.validation.custom import (
    check_trip_spatial_continuity,
    check_valid_tours_are_complete,
)
from tests.fixtures.records import create_household, create_person

HOUSEHOLD_KWARGS = [
    {"hh_id": 1, "home_taz": 100, "income": 50000, "num_vehicles": 1},
    {
        "hh_id": 2,
        "home_taz": 200,
        "home_lat": 37.8,
        "home_lon": -122.5,
        "income": 75000,
        "num_vehicles": 2,
    },
    {
        "hh_id": 3,
        "home_taz": 300,
        "home_lat": 37.9,
        "home_lon": -122.6,
        "income": 100000,
        "num_vehicles": 2,
    },
]


def _households(hh_ids: list[int], *, num_people: int = 1) -> pl.DataFrame:
    """Households by id, drawn from a fixed block so the ids are the only variable."""
    by_id = {kwargs["hh_id"]: kwargs for kwargs in HOUSEHOLD_KWARGS}
    return pl.DataFrame(
        [create_household(**by_id[hh_id], num_people=num_people) for hh_id in hh_ids]
    )


def _persons(hh_ids: list[int]) -> pl.DataFrame:
    """One person per entry, numbered 101 upwards, in the household named."""
    return pl.DataFrame(
        [
            create_person(
                person_id=101 + i,
                hh_id=hh_id,
                age=AgeCategory.AGE_5_TO_15,
                gender=Gender.MALE if i % 2 == 0 else Gender.FEMALE,
            )
            for i, hh_id in enumerate(hh_ids)
        ]
    )


def _linked(households: list[int], persons: list[int]) -> CanonicalData:
    data = CanonicalData()
    data.households = _households(households)
    data.persons = _persons(persons)
    return data


class TestUniqueConstraints:
    """Tests for uniqueness validation."""

    def test_unique_passes(self):
        """Unique ids pass."""
        data = CanonicalData()
        data.households = _households([1, 2, 3], num_people=2)
        data.validate("households", step="link_trips")

    def test_unique_fails_with_duplicates(self):
        """A duplicate id fails the unique constraint."""
        data = CanonicalData()
        data.households = pl.concat([_households([1, 2], num_people=2), _households([2])])
        with pytest.raises(DataValidationError) as exc:
            data.validate("households", step="link_trips")
        assert exc.value.rule == "unique_constraint"


class TestForeignKeys:
    """Tests for FK validation, in both directions."""

    def test_fk_and_required_children_pass(self):
        """Every person has a household, and every household has a person."""
        data = _linked(households=[1, 2], persons=[1, 2])
        data.validate("persons", step="link_trips")
        data.validate("households", step="link_trips")

    @pytest.mark.parametrize(
        ("households", "persons", "table", "rule"),
        [
            pytest.param([1, 2], [1, 999], "persons", "foreign_key", id="person-without-household"),
            pytest.param(
                [1, 2, 3], [1, 2], "households", "required_children", id="empty-household"
            ),
        ],
    )
    def test_a_broken_link_fails(self, households, persons, table, rule):
        """A person with no household, or a household with no person, fails."""
        data = _linked(households=households, persons=persons)
        with pytest.raises(DataValidationError) as exc:
            data.validate(table, step="link_trips")
        assert exc.value.rule == rule


class TestRequiredChildrenWhen:
    """Only surveyable persons need a day (``required_child_when``)."""

    def _data(self, *, surveyable_values: list[bool | None], with_days: list[int]):
        data = CanonicalData()
        data.households = pl.DataFrame(
            [create_household(hh_id=1, home_taz=100, income=50000, num_people=2, num_vehicles=1)]
        )
        data.persons = pl.DataFrame(
            [
                create_person(
                    person_id=101 + i,
                    hh_id=1,
                    age=AgeCategory.AGE_35_TO_44,
                    gender=Gender.FEMALE,
                )
                for i in range(len(surveyable_values))
            ]
        ).with_columns(pl.Series("surveyable", surveyable_values, dtype=pl.Boolean))
        data.person_days = pl.DataFrame(
            {
                "person_day_id": [1000 + p for p in with_days],
                "person_id": with_days,
                "hh_id": [1] * len(with_days),
                "travel_date": [datetime(2024, 1, 17)] * len(with_days),
                "travel_dow": [TravelDow.WEDNESDAY.value] * len(with_days),
            }
        )
        return data

    def test_unsurveyable_person_may_have_no_days(self):
        """A person marked unsurveyable is legitimately childless."""
        data = self._data(surveyable_values=[True, False], with_days=[101])
        data.validate("persons", step="write_data")

    @pytest.mark.parametrize(
        ("surveyable", "drop_column"),
        [
            pytest.param(True, False, id="surveyable"),
            pytest.param(None, False, id="null-counts-as-surveyable"),
            pytest.param(True, True, id="no-column-requires-every-row"),
        ],
    )
    def test_a_person_who_needs_a_day_and_has_none_fails(self, surveyable, drop_column):
        """A surveyable or null person with no day fails; with no column, every person needs one."""
        data = self._data(surveyable_values=[True, surveyable], with_days=[101])
        if drop_column:
            data.persons = data.persons.drop("surveyable")
        with pytest.raises(DataValidationError) as exc:
            data.validate("persons", step="write_data")
        assert exc.value.rule == "required_children"
        assert "surveyable" in str(exc.value)


class TestCustomValidators:
    """Tests for running the custom validators registered on a table."""

    def test_multi_table_validator(self):
        """A validator taking two tables is given both."""
        data = CanonicalData()

        def check_size(
            persons: pl.DataFrame,
            households: pl.DataFrame,
        ) -> list[str]:
            actual = persons.group_by("hh_id").agg(pl.len().alias("n"))
            merged = households.join(actual, on="hh_id", how="left")
            bad = merged.filter(pl.col("num_people") != pl.col("n"))
            if len(bad) > 0:
                return ["Size mismatch"]
            return []

        data.custom_validators["persons"].append(check_size)
        data.households = _households([1, 2])
        data.persons = _persons([1, 2])
        data.validate("persons", step="link_trips")

    def test_a_validator_on_an_absent_table_skips_only_itself(self):
        """A validator, or a foreign key, whose table is not loaded is skipped; later ones run."""
        data = CanonicalData()

        def needs_tours(persons: pl.DataFrame, tours: pl.DataFrame) -> list[str]:
            return [f"should not run on {persons.height} persons, {tours.height} tours"]

        def always_fails(persons: pl.DataFrame) -> list[str]:
            return [f"ran on {persons.height} persons"]

        data.custom_validators["persons"] = [needs_tours, always_fails]
        data.persons = _persons([1, 2])
        with pytest.raises(DataValidationError) as exc:
            data.validate("persons", step="link_trips")
        assert exc.value.rule == "always_fails"


class TestValidToursAreComplete:
    """Tests for the check_valid_tours_are_complete custom validator."""

    def _tours(self, quality, *, single_trip, purpose):
        return pl.DataFrame(
            {
                "tour_id": [1],
                "tour_data_quality": [quality],
                "trip_count": [1 if single_trip else 3],
                "tour_purpose": [purpose],
            },
            schema={
                "tour_id": pl.Int64,
                "tour_data_quality": pl.Int64,
                "trip_count": pl.Int64,
                "tour_purpose": pl.Int64,
            },
        )

    @pytest.mark.parametrize(
        ("quality", "single_trip", "purpose", "n_errors", "says"),
        [
            pytest.param(
                TourDataQuality.VALID.value,
                False,
                PurposeCategory.WORK.value,
                0,
                "",
                id="valid-and-complete",
            ),
            # A single-trip tour flagged non-VALID is allowed to lack a purpose.
            pytest.param(
                TourDataQuality.NO_DESTINATION.value, True, None, 0, "", id="invalid-single-trip"
            ),
            pytest.param(
                TourDataQuality.VALID.value,
                True,
                PurposeCategory.WORK.value,
                1,
                "VALID",
                id="valid-but-single-trip",
            ),
            pytest.param(
                TourDataQuality.VALID.value, False, None, 1, "VALID", id="valid-but-null-purpose"
            ),
        ],
    )
    def test_valid_tours_must_be_complete(
        self, quality: int, single_trip: bool, purpose: int | None, n_errors: int, says: str
    ):
        """A tour labelled VALID has to hold a real, multi-trip, purposeful tour."""
        errors = check_valid_tours_are_complete(
            self._tours(quality, single_trip=single_trip, purpose=purpose)
        )

        assert len(errors) == n_errors
        if says:
            assert says in errors[0]

    def test_missing_quality_column_is_noop(self):
        """Frames without tour_data_quality produce no errors."""
        tours = pl.DataFrame({"tour_id": [1], "trip_count": [1], "tour_purpose": [None]})
        assert check_valid_tours_are_complete(tours) == []


class TestTripSpatialContinuity:
    """Tests for the check_trip_spatial_continuity custom validator."""

    def _trips(self, points):
        """Linked trips from (person, day, depart, (o_lat, o_lon), (d_lat, d_lon)) points."""
        return pl.DataFrame(
            {
                "linked_trip_id": list(range(1, len(points) + 1)),
                "person_id": [p[0] for p in points],
                "person_day_id": [p[1] for p in points],
                "depart_time": [p[2] for p in points],
                "o_lat": [p[3][0] for p in points],
                "o_lon": [p[3][1] for p in points],
                "d_lat": [p[4][0] for p in points],
                "d_lon": [p[4][1] for p in points],
            }
        )

    def test_continuous_trips_pass(self):
        """A day where each trip resumes where the last ended has no gaps."""
        home, work = (37.70, -122.40), (37.80, -122.45)
        trips = self._trips(
            [
                (1, 1, 8.0, home, work),
                (1, 1, 17.0, work, home),
            ]
        )
        assert check_trip_spatial_continuity(trips) == []

    def test_small_sample_with_gap_never_fails(self):
        """A high gap rate over only a few junctions is noise, not a failure."""
        home, a = (37.70, -122.40), (37.75, -122.42)
        far = (38.50, -123.20)
        # One person-day, one junction that jumps: 100% rate but tiny sample.
        trips = self._trips(
            [
                (1, 1, 8.0, home, a),
                (1, 1, 17.0, far, home),
            ]
        )
        assert check_trip_spatial_continuity(trips) == []

    def test_low_rate_at_scale_passes(self):
        """A small fraction of gaps across many junctions is normal survey noise."""
        home, a = (37.70, -122.40), (37.75, -122.42)
        far = (38.20, -122.90)
        points = []
        # 1,200 continuous person-days (1 junction each, gap 0)
        for person in range(1, 1201):
            points.append((person, 1, 8.0, home, a))
            points.append((person, 1, 17.0, a, home))
        # 20 person-days with a genuine gap -> ~1.6% << 15% ceiling
        for person in range(1201, 1221):
            points.append((person, 1, 8.0, home, a))
            points.append((person, 1, 17.0, far, home))
        assert check_trip_spatial_continuity(self._trips(points)) == []

    def test_high_rate_at_scale_fails(self):
        """A high gap rate over a meaningful sample flags a systemic problem."""
        home, a = (37.70, -122.40), (37.75, -122.42)
        far = (38.50, -123.20)
        points = []
        # 1,200 person-days where every junction jumps -> 100% rate at scale.
        for person in range(1, 1201):
            points.append((person, 1, 8.0, home, a))
            points.append((person, 1, 17.0, far, home))
        errors = check_trip_spatial_continuity(self._trips(points))
        assert len(errors) == 1
        assert "systemic" in errors[0].lower()

    def test_missing_columns_is_noop(self):
        """Frames without coordinate columns produce no errors."""
        trips = pl.DataFrame({"linked_trip_id": [1], "person_id": [1], "person_day_id": [1]})
        assert check_trip_spatial_continuity(trips) == []
