"""Tests for data validation framework."""

from datetime import datetime

import polars as pl
import pytest

from data_canon.codebook.persons import AgeCategory, Gender
from data_canon.codebook.trips import ModeType, Purpose, PurposeCategory
from data_canon.core.dataclass import CanonicalData
from data_canon.core.exceptions import DataValidationError
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
                trip_ids = long_trips["trip_id"].to_list()[:5]
                errors.append(f"Found {len(long_trips)} trips longer than 4 hours: {trip_ids}")
            return errors

        # Include all required fields for UnlinkedTripModel
        data_obj.unlinked_trips = pl.DataFrame(
            {
                "trip_id": [1, 2, 3],
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
