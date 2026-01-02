"""Tests for data validation framework."""

from datetime import datetime

import polars as pl
import pytest

from data_canon.core.dataclass import CanonicalData
from data_canon.core.exceptions import DataValidationError


class TestUniqueConstraints:
    """Tests for uniqueness validation."""

    def test_unique_passes(self):
        """Should pass with unique IDs."""
        data = CanonicalData()
        data.households = pl.DataFrame(
            {
                "hh_id": [1, 2, 3],
                "home_taz": [100, 200, 300],
                "home_lat": [37.7, 37.8, 37.9],
                "home_lon": [-122.4, -122.5, -122.6],
                "income": [50000, 75000, 100000],
                "hh_size": [2, 3, 4],
                "num_vehicles": [1, 2, 2],
            }
        )
        data.validate("households", step="link_trips")

    def test_unique_fails_with_duplicates(self):
        """Should fail with duplicate IDs."""
        data = CanonicalData()
        data.households = pl.DataFrame(
            {
                "hh_id": [1, 2, 2],
                "home_taz": [100, 200, 300],
                "home_lat": [37.7, 37.8, 37.9],
                "home_lon": [-122.4, -122.5, -122.6],
                "income": [50000, 75000, 100000],
                "hh_size": [2, 3, 4],
                "num_vehicles": [1, 2, 2],
            }
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
            {
                "hh_id": [1, 2],
                "home_taz": [100, 200],
                "home_lat": [37.7, 37.8],
                "home_lon": [-122.4, -122.5],
                "income": [50000, 75000],
                "hh_size": [1, 1],
                "num_vehicles": [1, 2],
            }
        )
        data.persons = pl.DataFrame(
            {
                "person_id": [101, 102],
                "hh_id": [1, 2],
                "age": [6, 8],
                "gender": ["male", "female"],
                "worker": [True, True],
                "student": [False, False],
            }
        )
        data.validate("persons", step="link_trips")

    def test_fk_fails_with_orphans(self):
        """Should fail with orphaned FKs."""
        data = CanonicalData()
        data.households = pl.DataFrame(
            {
                "hh_id": [1, 2],
                "home_taz": [100, 200],
                "home_lat": [37.7, 37.8],
                "home_lon": [-122.4, -122.5],
                "income": [50000, 75000],
                "hh_size": [1, 1],
                "num_vehicles": [1, 2],
            }
        )
        data.persons = pl.DataFrame(
            {
                "person_id": [101, 102],
                "hh_id": [1, 999],
                "age": [6, 8],
                "gender": ["male", "female"],
                "worker": [True, True],
                "student": [False, False],
            }
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
            {
                "hh_id": [1, 2],
                "home_taz": [100, 200],
                "home_lat": [37.7, 37.8],
                "home_lon": [-122.4, -122.5],
                "income": [50000, 75000],
                "hh_size": [1, 1],
                "num_vehicles": [1, 2],
            }
        )
        data.persons = pl.DataFrame(
            {
                "person_id": [101, 102],
                "hh_id": [1, 2],
                "age": [6, 8],
                "gender": ["male", "female"],
                "worker": [True, True],
                "student": [False, False],
            }
        )
        data.validate("households", step="link_trips")

    def test_required_children_fails(self):
        """Should fail when parent missing children."""
        data = CanonicalData()
        data.households = pl.DataFrame(
            {
                "hh_id": [1, 2, 3],
                "home_taz": [100, 200, 300],
                "home_lat": [37.7, 37.8, 37.9],
                "home_lon": [-122.4, -122.5, -122.6],
                "income": [50000, 75000, 100000],
                "hh_size": [1, 1, 1],
                "num_vehicles": [1, 2, 2],
            }
        )
        data.persons = pl.DataFrame(
            {
                "person_id": [101, 102],
                "hh_id": [1, 2],
                "age": [6, 8],
                "gender": ["male", "female"],
                "worker": [True, True],
                "student": [False, False],
            }
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
                "o_purpose": [1, 2, 1],  # HOME, WORK, HOME
                "d_purpose": [2, 1, 1],  # WORK, HOME, HOME
                "o_purpose_category": [1, 2, 1],  # HOME, WORK, HOME
                "d_purpose_category": [2, 1, 1],  # WORK, HOME, HOME
                "mode_type": [1, 2, 1],  # WALK, BIKE, WALK
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
                    datetime(2024, 1, 15, 18, 0, 0),  # 10 hours later - too long!
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
            bad = merged.filter(pl.col("hh_size") != pl.col("n"))
            if len(bad) > 0:
                return ["Size mismatch"]
            return []

        data.households = pl.DataFrame(
            {
                "hh_id": [1, 2],
                "home_taz": [100, 200],
                "home_lat": [37.7, 37.8],
                "home_lon": [-122.4, -122.5],
                "income": [50000, 75000],
                "hh_size": [1, 1],
                "num_vehicles": [1, 2],
            }
        )
        data.persons = pl.DataFrame(
            {
                "person_id": [101, 102],
                "hh_id": [1, 2],
                "age": [6, 8],
                "gender": ["male", "female"],
                "worker": [True, True],
                "student": [False, False],
            }
        )
        data.validate("persons", step="link_trips")
