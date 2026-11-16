"""Unit tests for utility helpers: time columns, haversine, income, enum lookup."""

from datetime import datetime

import polars as pl
import pytest

from data_canon.codebook.households import IncomeBroad, ResidenceRentOwn
from data_canon.codebook.persons import Gender
from utils.enum_helpers import get_enum_class_for_field, resolve_enum_labels
from utils.helpers import add_time_columns, expr_haversine, get_income_midpoint


@pytest.fixture
def basic_trip_data() -> pl.DataFrame:
    """Three trips on one day, each departing on the hour and arriving 30 minutes later."""
    return pl.DataFrame(
        {
            "unlinked_trip_id": [1, 2, 3],
            "person_day_id": [101, 101, 101],
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


def test_add_time_columns(basic_trip_data: pl.DataFrame) -> None:
    """Datetime columns are built from the date/hour/minute/second components."""
    df_with_time = add_time_columns(basic_trip_data)

    assert df_with_time["depart_time"].dtype == pl.Datetime
    assert df_with_time["arrive_time"].dtype == pl.Datetime
    assert df_with_time["depart_time"].to_list() == [
        datetime(2023, 1, 1, 8, 0, 0),
        datetime(2023, 1, 1, 9, 0, 0),
        datetime(2023, 1, 1, 10, 0, 0),
    ]
    assert df_with_time["arrive_time"].to_list() == [
        datetime(2023, 1, 1, 8, 30, 0),
        datetime(2023, 1, 1, 9, 30, 0),
        datetime(2023, 1, 1, 10, 30, 0),
    ]


def test_add_time_columns_idempotent(basic_trip_data: pl.DataFrame) -> None:
    """Running add_time_columns twice gives the same frame."""
    df = add_time_columns(basic_trip_data)
    df2 = add_time_columns(df)

    assert df.equals(df2)


@pytest.mark.parametrize(
    ("lat1", "lon1", "lat2", "lon2", "min_m", "max_m"),
    [
        # A short hop across downtown San Francisco, roughly 1.4 km.
        pytest.param(37.7749, -122.4194, 37.7849, -122.4294, 1300, 1500, id="sf-short-hop"),
        # Identical coordinates must collapse to zero, not to a rounding artefact.
        pytest.param(37.7749, -122.4194, 37.7749, -122.4194, -1, 1, id="same-location"),
        # San Francisco to Oakland, roughly 13 km.
        pytest.param(37.7749, -122.4194, 37.8044, -122.2712, 12000, 14000, id="sf-to-oakland"),
    ],
)
def test_expr_haversine(
    lat1: float, lon1: float, lat2: float, lon2: float, min_m: int, max_m: int
) -> None:
    """Haversine distance in metres falls inside the known bounds for each pair."""
    df = pl.DataFrame({"lat1": [lat1], "lon1": [lon1], "lat2": [lat2], "lon2": [lon2]})

    result = df.select(
        expr_haversine(
            pl.col("lat1"),
            pl.col("lon1"),
            pl.col("lat2"),
            pl.col("lon2"),
        ).alias("distance")
    )

    assert min_m < result["distance"][0] < max_m


@pytest.mark.parametrize(
    ("category", "expected"),
    [
        # "Under $25,000" takes 0 as the lower bound: round(25000 / 2, -3).
        pytest.param(IncomeBroad.INCOME_UNDER25, 12000, id="under-format"),
        pytest.param(IncomeBroad.INCOME_25TO50, 37000, id="range-25to50"),
        pytest.param(IncomeBroad.INCOME_50TO75, 62000, id="range-50to75"),
        pytest.param(IncomeBroad.INCOME_75TO100, 87000, id="range-75to100"),
        pytest.param(IncomeBroad.INCOME_100TO200, 150000, id="range-100to200"),
        # "$200,000 or more" takes a 1.25x upper bound: round((200000 + 250000) / 2, -3).
        pytest.param(IncomeBroad.INCOME_200_OR_MORE, 225000, id="or-more-format"),
    ],
)
def test_get_income_midpoint(category: IncomeBroad, expected: int) -> None:
    """Every reportable IncomeBroad category maps to its rounded midpoint."""
    assert get_income_midpoint(category) == expected


@pytest.mark.parametrize("category", [IncomeBroad.PNTA, IncomeBroad.MISSING])
def test_get_income_midpoint_non_response_raises(category: IncomeBroad) -> None:
    """Non-response categories have no midpoint and must raise."""
    with pytest.raises(ValueError, match="Cannot calculate midpoint"):
        get_income_midpoint(category)


@pytest.mark.parametrize(
    ("table", "field", "expected"),
    [
        pytest.param("persons", "gender", Gender, id="direct-enum"),
        pytest.param("households", "residence_rent_own", ResidenceRentOwn, id="optional-enum"),
        pytest.param("households", "income_bin", IncomeBroad, id="income-broad"),
    ],
)
def test_get_enum_class_for_field(table: str, field: str, expected: type) -> None:
    """The enum class behind a model field is found, Optional wrapper or not."""
    assert get_enum_class_for_field(table, field) is expected


@pytest.mark.parametrize(
    ("table", "field", "match"),
    [
        pytest.param("households", "home_lat", "No enum class found", id="non-enum-field"),
        pytest.param("nonexistent_table", "some_field", "Unknown table name", id="unknown-table"),
        pytest.param("persons", "nonexistent_field", "not found in model", id="unknown-field"),
    ],
)
def test_get_enum_class_for_field_raises(table: str, field: str, match: str) -> None:
    """Fields with no enum, unknown tables and unknown fields each raise."""
    with pytest.raises(ValueError, match=match):
        get_enum_class_for_field(table, field)


@pytest.mark.parametrize(
    ("table", "field", "labels", "expected"),
    [
        pytest.param("persons", "gender", ["FEMALE"], [Gender.FEMALE.value], id="single-label"),
        pytest.param("persons", "gender", ["MISSING", "PNTA"], [995, 999], id="multiple-labels"),
        pytest.param(
            "households",
            "income_bin",
            ["MISSING", "PNTA"],
            [IncomeBroad.MISSING.value, IncomeBroad.PNTA.value],
            id="income-broad",
        ),
        pytest.param(
            "households",
            "residence_rent_own",
            ["OWN", "RENT"],
            [ResidenceRentOwn.OWN.value, ResidenceRentOwn.RENT.value],
            id="household-enum",
        ),
    ],
)
def test_resolve_enum_labels(
    table: str, field: str, labels: list[str], expected: list[int]
) -> None:
    """Enum member names resolve to their integer values, in the order given."""
    assert resolve_enum_labels(table, field, labels) == expected


@pytest.mark.parametrize(
    ("table", "field", "labels", "match"),
    [
        pytest.param("persons", "gender", ["INVALID_LABEL"], "not found in enum", id="bad-label"),
        pytest.param(
            "households", "home_lat", ["SOMETHING"], "No enum class found", id="non-enum-field"
        ),
    ],
)
def test_resolve_enum_labels_raises(table: str, field: str, labels: list[str], match: str) -> None:
    """An unknown label, or a field with no enum at all, raises."""
    with pytest.raises(ValueError, match=match):
        resolve_enum_labels(table, field, labels)
