"""Unit tests for utility functions."""

import polars as pl
import pytest

from utils.helpers import (
    add_time_columns,
    expr_haversine,
)

# Test constants
EXPECTED_SAN_JOSE_DISTANCE_MIN = 1300  # meters
EXPECTED_SAN_JOSE_DISTANCE_MAX = 1500  # meters
EXPECTED_SF_OAKLAND_DISTANCE_MIN = 12000  # meters
EXPECTED_SF_OAKLAND_DISTANCE_MAX = 14000  # meters

# Fixtures ---------------------------------------------------------------------


@pytest.fixture
def basic_trip_data() -> pl.DataFrame:
    """Create basic trip data for testing."""
    return pl.DataFrame(
        {
            "trip_id": [1, 2, 3],
            "day_id": [101, 101, 101],
            "person_id": [1, 1, 1],
            "hh_id": [1, 1, 1],
            "depart_date": ["2023-01-01", "2023-01-01", "2023-01-01"],
            "depart_hour": [8, 9, 10],
            "depart_minute": [0, 0, 0],
            "depart_seconds": [0, 0, 0],
            "arrive_date": ["2023-01-01", "2023-01-01", "2023-01-01"],
            "arrive_hour": [8, 9, 10],
            "arrive_minute": [30, 30, 30],
            "arrive_seconds": [0, 0, 0],
        }
    )


# Utility Function Tests -------------------------------------------------------


def test_add_time_columns(basic_trip_data: pl.DataFrame) -> None:
    """Test that datetime columns are correctly added."""
    # Remove time columns to test creation (if they exist)
    cols_to_drop = []
    if "depart_time" in basic_trip_data.columns:
        cols_to_drop.append("depart_time")
    if "arrive_time" in basic_trip_data.columns:
        cols_to_drop.append("arrive_time")

    df = basic_trip_data.drop(cols_to_drop) if cols_to_drop else basic_trip_data

    df_with_time = add_time_columns(df)

    assert "depart_time" in df_with_time.columns
    assert "arrive_time" in df_with_time.columns
    assert df_with_time["depart_time"].dtype == pl.Datetime
    assert df_with_time["arrive_time"].dtype == pl.Datetime


def test_add_time_columns_idempotent(basic_trip_data: pl.DataFrame) -> None:
    """Test that add_time_columns doesn't duplicate if columns exist."""
    df = add_time_columns(basic_trip_data)
    df2 = add_time_columns(df)

    assert df.equals(df2)


def test_expr_haversine() -> None:
    """Test Haversine distance calculation."""
    df = pl.DataFrame(
        {
            "lat1": [37.7749],
            "lon1": [-122.4194],
            "lat2": [37.7849],
            "lon2": [-122.4294],
        }
    )

    result = df.select(
        [
            expr_haversine(
                pl.col("lat1"),
                pl.col("lon1"),
                pl.col("lat2"),
                pl.col("lon2"),
            ).alias("distance"),
        ]
    )

    # Distance should be roughly 1.4 km (1400 meters)
    distance = result["distance"][0]
    assert EXPECTED_SAN_JOSE_DISTANCE_MIN < distance < EXPECTED_SAN_JOSE_DISTANCE_MAX


def test_expr_haversine_same_location() -> None:
    """Test that Haversine returns 0 for same location."""
    df = pl.DataFrame(
        {
            "lat1": [37.7749],
            "lon1": [-122.4194],
            "lat2": [37.7749],
            "lon2": [-122.4194],
        }
    )

    result = df.select(
        [
            expr_haversine(
                pl.col("lat1"),
                pl.col("lon1"),
                pl.col("lat2"),
                pl.col("lon2"),
            ).alias("distance"),
        ]
    )

    assert result["distance"][0] < 1.0  # Should be essentially 0


def test_expr_haversine_known_distance() -> None:
    """Test Haversine with a known distance."""
    # San Francisco to Oakland (approximately 13 km)
    df = pl.DataFrame(
        {
            "lat1": [37.7749],  # SF
            "lon1": [-122.4194],
            "lat2": [37.8044],  # Oakland
            "lon2": [-122.2712],
        }
    )

    result = df.select(
        [
            expr_haversine(
                pl.col("lat1"),
                pl.col("lon1"),
                pl.col("lat2"),
                pl.col("lon2"),
            ).alias("distance"),
        ]
    )

    # Distance should be roughly 13 km (13000 meters)
    distance = result["distance"][0]
    assert EXPECTED_SF_OAKLAND_DISTANCE_MIN < distance < EXPECTED_SF_OAKLAND_DISTANCE_MAX
