"""Tests for data validation framework."""

from datetime import datetime

import polars as pl
import pytest

from data_canon.codebook.persons import AgeCategory, Gender
from data_canon.codebook.tours import TourDataQuality
from data_canon.codebook.trips import ModeType, Purpose, PurposeCategory
from data_canon.core.dataclass import CanonicalData
from data_canon.core.exceptions import DataValidationError
from data_canon.validation.custom import (
    check_trip_spatial_continuity,
    check_valid_tours_are_complete,
)
from tests.fixtures import create_household, create_person


class TestUniqueConstraints:
    """Tests for uniqueness validation."""

    def test_unique_passes(self):
        """Should pass with unique IDs."""
        data = CanonicalData()
        data.households = pl.DataFrame(
            [
                create_household(
                    hh_id=1,
                    home_taz=100,
                    income=50000,
                    num_people=2,
                    num_vehicles=1,
                ),
                create_household(
                    hh_id=2,
                    home_taz=200,
                    home_lat=37.8,
                    home_lon=-122.5,
                    income=75000,
                    num_people=3,
                    num_vehicles=2,
                ),
                create_household(
                    hh_id=3,
                    home_taz=300,
                    home_lat=37.9,
                    home_lon=-122.6,
                    income=100000,
                    num_people=4,
                    num_vehicles=2,
                ),
            ]
        )
        data.validate("households", step="link_trips")

    def test_unique_fails_with_duplicates(self):
        """Should fail with duplicate IDs."""
        data = CanonicalData()
        data.households = pl.DataFrame(
            [
                create_household(
                    hh_id=1,
                    home_taz=100,
                    income=50000,
                    num_people=2,
                    num_vehicles=1,
                ),
                create_household(
                    hh_id=2,
                    home_taz=200,
                    home_lat=37.8,
                    home_lon=-122.5,
                    income=75000,
                    num_people=3,
                    num_vehicles=2,
                ),
                create_household(
                    hh_id=2,
                    home_taz=300,
                    home_lat=37.9,
                    home_lon=-122.6,
                    income=100000,
                    num_people=4,
                    num_vehicles=2,
                ),
            ]
        )
        with pytest.raises(DataValidationError) as exc:
            data.validate("households", step="link_trips")
        assert exc.value.rule == "unique_constraint"


class TestForeignKeys:
    """Tests for FK validation."""

    def test_fk_passes(self):
        """Should pass with valid FKs."""
        data = CanonicalData()
        data.households = pl.DataFrame(
            [
                create_household(
                    hh_id=1,
                    home_taz=100,
                    income=50000,
                    num_people=1,
                    num_vehicles=1,
                ),
                create_household(
                    hh_id=2,
                    home_taz=200,
                    home_lat=37.8,
                    home_lon=-122.5,
                    income=75000,
                    num_people=1,
                    num_vehicles=2,
                ),
            ]
        )
        data.persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    age=AgeCategory.AGE_5_TO_15,
                    gender=Gender.MALE,
                ),
                create_person(
                    person_id=102,
                    hh_id=2,
                    age=AgeCategory.AGE_5_TO_15,
                    gender=Gender.FEMALE,
                ),
            ]
        )
        data.validate("persons", step="link_trips")

    def test_fk_fails_with_orphans(self):
        """Should fail with orphaned FKs."""
        data = CanonicalData()
        data.households = pl.DataFrame(
            [
                create_household(
                    hh_id=1,
                    home_taz=100,
                    income=50000,
                    num_people=1,
                    num_vehicles=1,
                ),
                create_household(
                    hh_id=2,
                    home_taz=200,
                    home_lat=37.8,
                    home_lon=-122.5,
                    income=75000,
                    num_people=1,
                    num_vehicles=2,
                ),
            ]
        )
        data.persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    age=AgeCategory.AGE_5_TO_15,
                    gender=Gender.MALE,
                ),
                create_person(
                    person_id=102,
                    hh_id=999,
                    age=AgeCategory.AGE_5_TO_15,
                    gender=Gender.FEMALE,
                ),
            ]
        )
        with pytest.raises(DataValidationError) as exc:
            data.validate("persons", step="link_trips")
        assert exc.value.rule == "foreign_key"


class TestRequiredChildren:
    """Tests for bidirectional FK validation."""

    def test_required_children_passes(self):
        """Should pass when all parents have children."""
        data = CanonicalData()
        data.households = pl.DataFrame(
            [
                create_household(
                    hh_id=1,
                    home_taz=100,
                    income=50000,
                    num_people=1,
                    num_vehicles=1,
                ),
                create_household(
                    hh_id=2,
                    home_taz=200,
                    home_lat=37.8,
                    home_lon=-122.5,
                    income=75000,
                    num_people=1,
                    num_vehicles=2,
                ),
            ]
        )
        data.persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    age=AgeCategory.AGE_5_TO_15,
                    gender=Gender.MALE,
                ),
                create_person(
                    person_id=102,
                    hh_id=2,
                    age=AgeCategory.AGE_5_TO_15,
                    gender=Gender.FEMALE,
                ),
            ]
        )
        data.validate("households", step="link_trips")

    def test_required_children_fails(self):
        """Should fail when parent missing children."""
        data = CanonicalData()
        data.households = pl.DataFrame(
            [
                create_household(
                    hh_id=1,
                    home_taz=100,
                    income=50000,
                    num_people=1,
                    num_vehicles=1,
                ),
                create_household(
                    hh_id=2,
                    home_taz=200,
                    home_lat=37.8,
                    home_lon=-122.5,
                    income=75000,
                    num_people=1,
                    num_vehicles=2,
                ),
                create_household(
                    hh_id=3,
                    home_taz=300,
                    home_lat=37.9,
                    home_lon=-122.6,
                    income=100000,
                    num_people=1,
                    num_vehicles=2,
                ),
            ]
        )
        data.persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    age=AgeCategory.AGE_5_TO_15,
                    gender=Gender.MALE,
                ),
                create_person(
                    person_id=102,
                    hh_id=2,
                    age=AgeCategory.AGE_5_TO_15,
                    gender=Gender.FEMALE,
                ),
            ]
        )
        with pytest.raises(DataValidationError) as exc:
            data.validate("households", step="link_trips")
        assert exc.value.rule == "required_children"


class TestCustomValidators:
    """Tests for custom validator registration."""

    def test_single_table_validator(self):
        """Should run custom validator on single table."""
        data_obj = CanonicalData()

        @data_obj.register_validator("unlinked_trips")
        def check_trip_duration(unlinked_trips: pl.DataFrame) -> list[str]:
            """Check that trips are not unreasonably long (>4 hours)."""
            errors = []
            unlinked_trips = unlinked_trips.with_columns(
                ((pl.col("arrive_time") - pl.col("depart_time")).dt.total_seconds() / 3600).alias(
                    "duration_hours"
                )
            )
            long_trips = unlinked_trips.filter(pl.col("duration_hours") > 4)
            if len(long_trips) > 0:
                trip_ids = long_trips["unlinked_trip_id"].to_list()[:5]
                errors.append(f"Found {len(long_trips)} trips longer than 4 hours: {trip_ids}")
            return errors

        # Include all required fields for UnlinkedTripModel
        data_obj.unlinked_trips = pl.DataFrame(
            {
                "unlinked_trip_id": [1, 2, 3],
                "person_id": [101, 101, 101],
                "hh_id": [1, 1, 1],
                "day_id": [10101, 10101, 10101],
                "depart_date": ["2024-01-15", "2024-01-15", "2024-01-15"],
                "depart_hour": [10, 11, 8],
                "depart_minute": [0, 0, 0],
                "depart_seconds": [0, 0, 0],
                "arrive_date": ["2024-01-15", "2024-01-15", "2024-01-15"],
                "arrive_hour": [10, 11, 18],  # Third trip is 10 hours long
                "arrive_minute": [30, 30, 0],
                "arrive_seconds": [0, 0, 0],
                "o_lon": [-122.4194, -122.4194, -122.4194],
                "o_lat": [37.7749, 37.7749, 37.7749],
                "d_lon": [-122.4094, -122.4094, -122.4094],
                "d_lat": [37.7849, 37.7849, 37.7849],
                "o_purpose": [
                    Purpose.HOME.value,
                    Purpose.PRIMARY_WORKPLACE.value,
                    Purpose.HOME.value,
                ],
                "d_purpose": [
                    Purpose.PRIMARY_WORKPLACE.value,
                    Purpose.HOME.value,
                    Purpose.HOME.value,
                ],
                "o_purpose_category": [
                    PurposeCategory.HOME.value,
                    PurposeCategory.WORK.value,
                    PurposeCategory.HOME.value,
                ],
                "d_purpose_category": [
                    PurposeCategory.WORK.value,
                    PurposeCategory.HOME.value,
                    PurposeCategory.HOME.value,
                ],
                "mode_type": [
                    ModeType.WALK.value,
                    ModeType.BIKE.value,
                    ModeType.WALK.value,
                ],
                "duration_minutes": [
                    30.0,
                    30.0,
                    600.0,
                ],  # 10 hours = 600 minutes
                "distance_miles": [5.0, 10.0, 50.0],
                "depart_time": [
                    datetime(2024, 1, 15, 10, 0, 0),
                    datetime(2024, 1, 15, 11, 0, 0),
                    datetime(2024, 1, 15, 8, 0, 0),
                ],
                "arrive_time": [
                    datetime(2024, 1, 15, 10, 30, 0),
                    datetime(2024, 1, 15, 11, 30, 0),
                    datetime(2024, 1, 15, 18, 0, 0),  # 10 hours later!
                ],
            }
        )
        with pytest.raises(DataValidationError) as exc:
            data_obj.validate("unlinked_trips", step="link_trips")
        assert exc.value.rule == "check_trip_duration"

    def test_multi_table_validator(self):
        """Should run custom validator with multiple tables."""
        data = CanonicalData()

        @data.register_validator("persons")
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

        data.households = pl.DataFrame(
            [
                create_household(
                    hh_id=1,
                    home_taz=100,
                    income=50000,
                    num_people=1,
                    num_vehicles=1,
                ),
                create_household(
                    hh_id=2,
                    home_taz=200,
                    home_lat=37.8,
                    home_lon=-122.5,
                    income=75000,
                    num_people=1,
                    num_vehicles=2,
                ),
            ]
        )
        data.persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    age=AgeCategory.AGE_5_TO_15,
                    gender=Gender.MALE,
                ),
                create_person(
                    person_id=102,
                    hh_id=2,
                    age=AgeCategory.AGE_5_TO_15,
                    gender=Gender.FEMALE,
                ),
            ]
        )
        data.validate("persons", step="link_trips")


class TestValidToursAreComplete:
    """Tests for the check_valid_tours_are_complete custom validator."""

    def _tours(self, quality, *, single_trip, purpose):
        """Build a one-row tours frame with the given quality/flag/purpose."""
        return pl.DataFrame(
            {
                "tour_id": [1],
                "tour_data_quality": [quality],
                "single_trip_tour": [single_trip],
                "tour_purpose": [purpose],
            },
            schema={
                "tour_id": pl.Int64,
                "tour_data_quality": pl.Int64,
                "single_trip_tour": pl.Boolean,
                "tour_purpose": pl.Int64,
            },
        )

    def test_valid_complete_tour_passes(self):
        """A VALID tour that is not single-trip and has a purpose is clean."""
        tours = self._tours(
            TourDataQuality.VALID.value, single_trip=False, purpose=PurposeCategory.WORK.value
        )
        assert check_valid_tours_are_complete(tours) == []

    def test_single_trip_flagged_invalid_passes(self):
        """A single-trip tour flagged non-VALID is allowed to lack a purpose."""
        tours = self._tours(TourDataQuality.SINGLE_TRIP.value, single_trip=True, purpose=None)
        assert check_valid_tours_are_complete(tours) == []

    def test_valid_but_single_trip_fails(self):
        """A tour mislabeled VALID but flagged single-trip is reported."""
        tours = self._tours(
            TourDataQuality.VALID.value, single_trip=True, purpose=PurposeCategory.WORK.value
        )
        errors = check_valid_tours_are_complete(tours)
        assert len(errors) == 1
        assert "VALID" in errors[0]

    def test_valid_but_null_purpose_fails(self):
        """A tour marked VALID with a null purpose is reported."""
        tours = self._tours(TourDataQuality.VALID.value, single_trip=False, purpose=None)
        errors = check_valid_tours_are_complete(tours)
        assert len(errors) == 1

    def test_missing_quality_column_is_noop(self):
        """Frames without tour_data_quality produce no errors."""
        tours = pl.DataFrame({"tour_id": [1], "single_trip_tour": [True], "tour_purpose": [None]})
        assert check_valid_tours_are_complete(tours) == []


class TestTripSpatialContinuity:
    """Tests for the check_trip_spatial_continuity custom validator."""

    def _trips(self, points):
        """Build a linked_trips frame from (person, day, depart, o, d) points.

        Each origin/destination is a (lat, lon) tuple.
        """
        return pl.DataFrame(
            {
                "linked_trip_id": list(range(1, len(points) + 1)),
                "person_id": [p[0] for p in points],
                "day_id": [p[1] for p in points],
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
                (1, 1, 17.0, work, home),  # resumes at work -> continuous
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
        trips = pl.DataFrame({"linked_trip_id": [1], "person_id": [1], "day_id": [1]})
        assert check_trip_spatial_continuity(trips) == []
