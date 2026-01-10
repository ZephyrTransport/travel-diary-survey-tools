"""Unit tests for utility functions."""

import polars as pl
import pytest

from data_canon.codebook.households import IncomeDetailed, IncomeFollowup
from utils.helpers import (
    add_time_columns,
    expr_haversine,
    get_income_midpoint,
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


# Income Midpoint Tests --------------------------------------------------------


def test_get_income_midpoint_range_format() -> None:
    """Test income midpoint calculation for range format ($X-$Y)."""
    # $50,000-$74,999 should yield midpoint rounded to nearest $1000
    midpoint = get_income_midpoint(IncomeDetailed.INCOME_50TO75)
    assert midpoint == 62000  # round((50000 + 74999) / 2, -3)

    # $25,000-$49,999 should yield midpoint rounded to nearest $1000
    midpoint = get_income_midpoint(IncomeFollowup.INCOME_25TO50)
    assert midpoint == 37000  # round((25000 + 49999) / 2, -3)


def test_get_income_midpoint_under_format() -> None:
    """Test income midpoint calculation for 'Under $X' format."""
    # "Under $15,000" should use $0 as lower bound, rounded to nearest $1000
    # Midpoint = round(15000 / 2, -3) = round(7500, -3) = 8000
    midpoint = get_income_midpoint(IncomeDetailed.INCOME_UNDER15)
    assert midpoint == 8000

    # "Under $25,000" should use $0 as lower bound, rounded to nearest $1000
    # Midpoint = round(25000 / 2, -3) = round(12500, -3) = 12000
    midpoint = get_income_midpoint(IncomeFollowup.INCOME_UNDER25)
    assert midpoint == 12000


def test_get_income_midpoint_or_more_format() -> None:
    """Test income midpoint calculation for '$X or more' format."""
    # "$250,000 or more" should use 1.25x multiplier, rounded to nearest $1000
    # Upper bound = 250000 * 1.25 = 312500
    # Midpoint = round((250000 + 312500) / 2, -3) = 281000
    midpoint = get_income_midpoint(IncomeDetailed.INCOME_250_OR_MORE)
    assert midpoint == 281000

    # "$200,000 or more" should use 1.25x multiplier, rounded to nearest $1000
    # Upper bound = 200000 * 1.25 = 250000
    # Midpoint = round((200000 + 250000) / 2, -3) = 225000 (already at 1000s)
    midpoint = get_income_midpoint(IncomeFollowup.INCOME_200_OR_MORE)
    assert midpoint == 225000


def test_get_income_midpoint_pnta_raises_error() -> None:
    """Test that PNTA (Prefer not to answer) raises ValueError."""
    with pytest.raises(ValueError, match="Cannot calculate midpoint"):
        get_income_midpoint(IncomeDetailed.PNTA)

    with pytest.raises(ValueError, match="Cannot calculate midpoint"):
        get_income_midpoint(IncomeFollowup.PNTA)


def test_get_income_midpoint_missing_raises_error() -> None:
    """Test that Missing response raises ValueError."""
    with pytest.raises(ValueError, match="Cannot calculate midpoint"):
        get_income_midpoint(IncomeFollowup.MISSING)


def test_get_income_midpoint_all_income_detailed() -> None:
    """Test that all IncomeDetailed categories can be processed."""
    expected_midpoints = {
        IncomeDetailed.INCOME_UNDER15: 8000,
        IncomeDetailed.INCOME_15TO25: 20000,
        IncomeDetailed.INCOME_25TO35: 30000,
        IncomeDetailed.INCOME_35TO50: 42000,
        IncomeDetailed.INCOME_50TO75: 62000,
        IncomeDetailed.INCOME_75TO100: 87000,
        IncomeDetailed.INCOME_100TO150: 125000,
        IncomeDetailed.INCOME_150TO200: 175000,
        IncomeDetailed.INCOME_200TO250: 225000,
        IncomeDetailed.INCOME_250_OR_MORE: 281000,
    }

    for income_cat, expected in expected_midpoints.items():
        midpoint = get_income_midpoint(income_cat)
        assert midpoint == expected, (
            f"Failed for {income_cat.name}: got {midpoint}, expected {expected}"
        )


def test_get_income_midpoint_all_income_followup() -> None:
    """Test that all IncomeFollowup categories can be processed."""
    expected_midpoints = {
        IncomeFollowup.INCOME_UNDER25: 12000,
        IncomeFollowup.INCOME_25TO50: 37000,
        IncomeFollowup.INCOME_50TO75: 62000,
        IncomeFollowup.INCOME_75TO100: 87000,
        IncomeFollowup.INCOME_100TO200: 150000,
        IncomeFollowup.INCOME_200_OR_MORE: 225000,
    }

    for income_cat, expected in expected_midpoints.items():
        midpoint = get_income_midpoint(income_cat)
        assert midpoint == expected, (
            f"Failed for {income_cat.name}: got {midpoint}, expected {expected}"
        )
