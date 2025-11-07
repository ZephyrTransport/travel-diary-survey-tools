"""Unit tests for trip linking functions."""
import polars as pl
import pytest

from travel_diary_survey_tools.linker import (
    aggregate_linked_trips,
    link_trip_ids,
    link_trips,
)
from travel_diary_survey_tools.utils import add_time_columns

# Test Constants
CHANGE_MODE_CODE = 11
TRANSIT_MODE_CODES = [12, 13, 14]
TRANSIT_MODE = 12
NUM_PEOPLE_IN_MULTI_PERSON_TEST = 2
NUM_TRIPS_IN_SEPARATED_TEST = 3
DISTANCE_TOLERANCE = 0.01  # For floating point comparisons


# Helper Functions -------------------------------------------------------------

def create_trip_data(
    n_trips: int = 3,
    trip_id: list[int] | None = None,
    day_id: int = 101,
    person_id: int = 1,
    hh_id: int = 1,
    depart_date: str = "2023-01-01",
    depart_hour: list[int] | None = None,
    depart_minute: list[int] | None = None,
    depart_seconds: list[int] | None = None,
    arrive_hour: list[int] | None = None,
    arrive_minute: list[int] | None = None,
    arrive_seconds: list[int] | None = None,
    o_purpose_category: list[int] | None = None,
    d_purpose_category: list[int] | None = None,
    mode_type: list[int] | None = None,
    duration_minutes: list[float] | None = None,
    distance_miles: list[float] | None = None,
    o_lat: list[float] | None = None,
    o_lon: list[float] | None = None,
    d_lat: list[float] | None = None,
    d_lon: list[float] | None = None,
    trip_weight: list[float] | None = None,
) -> pl.DataFrame:
    """Create trip data with sensible defaults.

    Generates a DataFrame with trip data. All list parameters should have
    length equal to n_trips if provided. If not provided, sensible defaults
    are used.
    """
    # Set defaults
    trip_id = trip_id or list(range(1, n_trips + 1))
    depart_hour = depart_hour or [8 + i for i in range(n_trips)]
    depart_minute = depart_minute or [0] * n_trips
    depart_seconds = depart_seconds or [0] * n_trips
    arrive_hour = arrive_hour or list(depart_hour)
    arrive_minute = arrive_minute or [30] * n_trips
    arrive_seconds = arrive_seconds or [0] * n_trips
    o_purpose_category = o_purpose_category or [1] * n_trips
    d_purpose_category = d_purpose_category or [2] * n_trips
    mode_type = mode_type or [1] * n_trips
    duration_minutes = duration_minutes or [30.0] * n_trips
    distance_miles = distance_miles or [5.0] * n_trips

    # Generate sequential lat/lon if not provided
    if o_lat is None:
        o_lat = [37.7749 + (i * 0.01) for i in range(n_trips)]
    if o_lon is None:
        o_lon = [-122.4194 - (i * 0.01) for i in range(n_trips)]
    if d_lat is None:
        d_lat = [lat + 0.01 for lat in o_lat]
    if d_lon is None:
        d_lon = [lon - 0.01 for lon in o_lon]

    trip_weight = trip_weight or [1.0] * n_trips

    return pl.DataFrame({
        "trip_id": trip_id,
        "day_id": [day_id] * n_trips,
        "person_id": [person_id] * n_trips,
        "hh_id": [hh_id] * n_trips,
        "depart_date": [depart_date] * n_trips,
        "depart_hour": depart_hour,
        "depart_minute": depart_minute,
        "depart_seconds": depart_seconds,
        "arrive_date": [depart_date] * n_trips,
        "arrive_hour": arrive_hour,
        "arrive_minute": arrive_minute,
        "arrive_seconds": arrive_seconds,
        "o_purpose_category": o_purpose_category,
        "d_purpose_category": d_purpose_category,
        "mode_type": mode_type,
        "duration_minutes": duration_minutes,
        "distance_miles": distance_miles,
        "o_lat": o_lat,
        "o_lon": o_lon,
        "d_lat": d_lat,
        "d_lon": d_lon,
        "trip_weight": trip_weight,
    })


# Fixtures ---------------------------------------------------------------------

@pytest.fixture
def basic_trip_data():
    """Create basic trip data for testing."""
    return create_trip_data(
        n_trips=3,
        o_purpose_category=[1, CHANGE_MODE_CODE, 2],
        d_purpose_category=[CHANGE_MODE_CODE, 2, 1],
        mode_type=[1, 2, 1],
        distance_miles=[5.0, 2.0, 5.0],
    )


@pytest.fixture
def transit_trip_data():
    """Create trip data with transit modes for testing."""
    return create_trip_data(
        n_trips=3,
        day_id=102,
        person_id=2,
        hh_id=2,
        depart_hour=[7, 7, 8],
        depart_minute=[0, 15, 0],
        arrive_hour=[7, 7, 8],
        arrive_minute=[10, 55, 30],
        o_purpose_category=[1, CHANGE_MODE_CODE, CHANGE_MODE_CODE],
        d_purpose_category=[CHANGE_MODE_CODE, CHANGE_MODE_CODE, 2],
        mode_type=[1, 12, 1],  # Walk, Bus (transit), Walk
        duration_minutes=[10.0, 40.0, 30.0],
        distance_miles=[0.5, 10.0, 0.5],
    )


@pytest.fixture
def separated_trip_data():
    """Create trip data with large time gap between trips."""
    return create_trip_data(
        n_trips=2,
        day_id=103,
        person_id=3,
        hh_id=3,
        depart_hour=[8, 14],  # 6 hour gap
        arrive_hour=[8, 14],
        o_purpose_category=[1, 2],
        d_purpose_category=[CHANGE_MODE_CODE, 1],
        # Same origin for both trips
        o_lat=[37.7749, 37.7749],
        o_lon=[-122.4194, -122.4194],
        d_lat=[37.7849, 37.7849],
        d_lon=[-122.4294, -122.4294],
    )


# Link Trip IDs Tests ----------------------------------------------------------

def test_link_trip_ids_basic(basic_trip_data):
    """Test basic trip linking based on change_mode purpose."""
    df = add_time_columns(basic_trip_data)

    result = link_trip_ids(df, CHANGE_MODE_CODE)

    assert "linked_trip_id" in result.columns
    # First two trips should be linked (trip 1 ends with change_mode)
    assert result["linked_trip_id"][0] == result["linked_trip_id"][1]
    # Third trip should be separate
    assert result["linked_trip_id"][2] != result["linked_trip_id"][0]


def test_link_trip_ids_all_separate(separated_trip_data):
    """Test that trips with large time gaps are not linked."""
    df = add_time_columns(separated_trip_data)

    result = link_trip_ids(
        df,
        CHANGE_MODE_CODE,
        max_dwell_time=120,  # 2 hours
    )

    # Despite change_mode purpose, trips should be separate due to time gap
    assert result["linked_trip_id"][0] != result["linked_trip_id"][1]


def test_link_trip_ids_unique_across_days():
    """Test that linked_trip_ids are unique across different days."""
    # Create two trips on different days
    df = create_trip_data(
        n_trips=2,
        depart_hour=[8, 8],
        # Same locations to ensure day_id is what separates them
        o_lat=[37.7749, 37.7749],
        o_lon=[-122.4194, -122.4194],
        d_lat=[37.7849, 37.7849],
        d_lon=[-122.4294, -122.4294],
    )

    # Manually set different day_ids and dates
    df = df.with_columns([
        pl.Series("day_id", [101, 102]),
        pl.Series("depart_date", ["2023-01-01", "2023-01-02"]),
        pl.Series("arrive_date", ["2023-01-01", "2023-01-02"]),
    ])

    df = add_time_columns(df)
    result = link_trip_ids(df, CHANGE_MODE_CODE)

    # linked_trip_ids should be different (they incorporate day_id)
    assert result["linked_trip_id"][0] != result["linked_trip_id"][1]


# Aggregate Linked Trips Tests -------------------------------------------------

def test_aggregate_linked_trips_basic(basic_trip_data):
    """Test basic aggregation of linked trips."""
    df = add_time_columns(basic_trip_data)
    df = link_trip_ids(df, CHANGE_MODE_CODE)

    result = aggregate_linked_trips(df, TRANSIT_MODE_CODES)

    # Should have fewer rows than original (trips are linked)
    assert len(result) < len(df)
    assert "linked_trip_id" in result.columns
    assert "num_segments" in result.columns


def test_aggregate_linked_trips_transit_mode(transit_trip_data):
    """Test that transit mode is selected when present."""
    df = add_time_columns(transit_trip_data)
    df = link_trip_ids(df, CHANGE_MODE_CODE)

    result = aggregate_linked_trips(df, TRANSIT_MODE_CODES)

    # All three trips should be linked
    assert len(result) == 1
    # Mode should be transit even though it's not the longest segment
    assert result["mode_type"][0] == TRANSIT_MODE


def test_aggregate_linked_trips_preserves_origin_destination(basic_trip_data):
    """Test that origin and destination are correctly preserved."""
    df = add_time_columns(basic_trip_data)
    df = link_trip_ids(df, CHANGE_MODE_CODE)

    result = aggregate_linked_trips(df, TRANSIT_MODE_CODES)

    # Find the linked trip that contains trips 1 and 2
    linked_trip = result.filter(pl.col("num_segments") > 1).row(0, named=True)

    # Origin should be from first trip
    assert (
        linked_trip["o_purpose_category"]
        == basic_trip_data["o_purpose_category"][0]
    )
    # Destination should be from last trip in the link
    assert (
        linked_trip["d_purpose_category"]
        == basic_trip_data["d_purpose_category"][1]
    )


def test_aggregate_linked_trips_sums_distances(transit_trip_data):
    """Test that trip distances are summed correctly."""
    df = add_time_columns(transit_trip_data)
    df = link_trip_ids(df, CHANGE_MODE_CODE)

    result = aggregate_linked_trips(df, TRANSIT_MODE_CODES)

    # Total distance should be sum of all segments
    expected_distance = transit_trip_data["distance_miles"].sum()
    assert (
        abs(result["distance_miles"][0] - expected_distance)
        < DISTANCE_TOLERANCE
    )


def test_aggregate_linked_trips_calculates_dwell_time(transit_trip_data):
    """Test that dwell time at transfer points is calculated."""
    df = add_time_columns(transit_trip_data)
    df = link_trip_ids(df, CHANGE_MODE_CODE)

    result = aggregate_linked_trips(df, TRANSIT_MODE_CODES)

    # Should have dwell_duration_minutes column
    assert "dwell_duration_minutes" in result.columns
    # For this transit trip with transfers, should have some dwell time
    assert result["dwell_duration_minutes"][0] >= 0


# Integration Tests ------------------------------------------------------------

def test_link_trips_end_to_end(basic_trip_data):
    """Test the complete link_trips function."""
    df = add_time_columns(basic_trip_data)

    trips_with_ids, linked_trips = link_trips(
        df,
        CHANGE_MODE_CODE,
        TRANSIT_MODE_CODES,
    )

    # Check trips_with_ids
    assert "linked_trip_id" in trips_with_ids.columns
    assert len(trips_with_ids) == len(df)

    # Check linked_trips
    assert len(linked_trips) <= len(df)
    assert "linked_trip_id" in linked_trips.columns
    assert "num_segments" in linked_trips.columns


def test_link_trips_multiple_persons():
    """Test linking with multiple persons."""
    # Create trips for person 1
    df1 = create_trip_data(
        n_trips=2,
        day_id=101,
        person_id=1,
        hh_id=1,
        depart_hour=[8, 9],
        arrive_hour=[8, 9],
        o_purpose_category=[1, CHANGE_MODE_CODE],
        d_purpose_category=[CHANGE_MODE_CODE, 2],
        mode_type=[1, 2],
        # Same location for simplicity
        o_lat=[37.7749, 37.7749],
        o_lon=[-122.4194, -122.4194],
        d_lat=[37.7849, 37.7849],
        d_lon=[-122.4294, -122.4294],
    )

    # Create trips for person 2
    df2 = create_trip_data(
        n_trips=2,
        trip_id=[3, 4],
        day_id=102,
        person_id=2,
        hh_id=2,
        depart_hour=[8, 9],
        arrive_hour=[8, 9],
        o_purpose_category=[1, 2],
        d_purpose_category=[2, 1],
        # Same location for simplicity
        o_lat=[37.7749, 37.7749],
        o_lon=[-122.4194, -122.4194],
        d_lat=[37.7849, 37.7849],
        d_lon=[-122.4294, -122.4294],
    )

    df = pl.concat([df1, df2])

    df = add_time_columns(df)
    _trips_with_ids, linked_trips = link_trips(
        df,
        CHANGE_MODE_CODE,
        TRANSIT_MODE_CODES,
    )

    # Should have linked trips for each person
    assert (
        linked_trips["person_id"].n_unique()
        == NUM_PEOPLE_IN_MULTI_PERSON_TEST
    )


def test_link_trips_with_custom_thresholds():
    """Test linking with custom distance and time thresholds."""
    # Test with time threshold - 90 minute gap
    df = create_trip_data(
        n_trips=2,
        depart_hour=[8, 10],
        depart_minute=[0, 30],  # 90 min gap
        arrive_hour=[8, 11],
        arrive_minute=[30, 0],
        d_purpose_category=[CHANGE_MODE_CODE, 1],
        # Second trip starts where first ended
        o_lat=[37.7749, 37.7849],
        o_lon=[-122.4194, -122.4294],
        d_lat=[37.7849, 37.7949],
        d_lon=[-122.4294, -122.4394],
    )

    df = add_time_columns(df)

    # With 120 min threshold, should be linked
    # (90 min gap, prev ends with change_mode)
    trips_with_ids1, _ = link_trips(
        df, CHANGE_MODE_CODE, TRANSIT_MODE_CODES,
        max_dwell_time=120,  # 2 hours
    )
    linked_id_0 = trips_with_ids1["linked_trip_id"][0]
    linked_id_1 = trips_with_ids1["linked_trip_id"][1]
    assert linked_id_0 == linked_id_1

    # Test with distance threshold - same time but far apart
    df2 = create_trip_data(
        n_trips=2,
        depart_hour=[8, 8],
        depart_minute=[0, 35],  # 5 min gap
        arrive_hour=[8, 9],
        arrive_minute=[30, 5],
        d_purpose_category=[CHANGE_MODE_CODE, 1],
        # Far apart (>100m) - about 11km difference
        o_lat=[37.7749, 37.8749],
        o_lon=[-122.4194, -122.5194],
        d_lat=[37.7849, 37.8849],
        d_lon=[-122.4294, -122.5294],
    )

    df2 = add_time_columns(df2)

    # Even with change_mode, if distance is too great, don't link
    trips_with_ids2, _ = link_trips(
        df2, CHANGE_MODE_CODE, TRANSIT_MODE_CODES,
        dwell_buffer_distance=100,  # 100 meters
    )
    linked_id2_0 = trips_with_ids2["linked_trip_id"][0]
    linked_id2_1 = trips_with_ids2["linked_trip_id"][1]
    assert linked_id2_0 != linked_id2_1


# Edge Cases -------------------------------------------------------------------

def test_link_trips_single_trip():
    """Test that a single trip works correctly."""
    df = create_trip_data(n_trips=1)

    df = add_time_columns(df)
    _trips_with_ids, linked_trips = link_trips(
        df,
        CHANGE_MODE_CODE,
        TRANSIT_MODE_CODES,
    )

    assert len(linked_trips) == 1
    assert linked_trips["num_segments"][0] == 1


def test_link_trips_no_change_mode():
    """Test trips with no change_mode purposes."""
    df = create_trip_data(
        n_trips=3,
        o_purpose_category=[1, 2, 3],
        d_purpose_category=[2, 3, 1],  # No change_mode
        # Same location to ensure they're not separated by distance
        o_lat=[37.7749] * 3,
        o_lon=[-122.4194] * 3,
        d_lat=[37.7849] * 3,
        d_lon=[-122.4294] * 3,
    )

    df = add_time_columns(df)
    _trips_with_ids, linked_trips = link_trips(
        df,
        CHANGE_MODE_CODE,
        TRANSIT_MODE_CODES,
    )

    # All trips should be separate
    assert len(linked_trips) == NUM_TRIPS_IN_SEPARATED_TEST
    assert all(linked_trips["num_segments"] == 1)


def test_link_trips_identical_depart_times():
    """Test handling of trips with identical depart times (synthetic trips).

    Synthetic access/egress trips can have identical depart/arrive times
    as the main trip. The function should handle this gracefully and use
    trip_id to maintain consistent ordering even when trips are not provided
    in order.
    """
    # Create trips OUT OF ORDER to test that sorting works correctly
    df = create_trip_data(
        n_trips=3,
        trip_id=[3, 1, 2],  # Out of order: egress, access, main
        # Trip 1 (access): synthetic, 0 duration, departs at 8:00
        # Trip 2 (main): transit trip, departs at 8:00, arrives at 9:00
        # Trip 3 (egress): synthetic, 0 duration, departs/arrives at 9:00
        # Trips 1 and 2 have same depart time,
        # trips 2 and 3 have same arrive time
        depart_hour=[9, 8, 8],  # Out of order: egress first
        depart_minute=[0, 0, 0],
        depart_seconds=[0, 0, 0],
        arrive_hour=[9, 8, 9],  # Out of order
        arrive_minute=[0, 0, 0],  # Synthetic trips have 0 duration
        arrive_seconds=[0, 0, 0],
        o_purpose_category=[CHANGE_MODE_CODE, 1, CHANGE_MODE_CODE],
        d_purpose_category=[2, CHANGE_MODE_CODE, CHANGE_MODE_CODE],
        mode_type=[1, 1, 12],  # Out of order: Walk, Walk, Transit
        duration_minutes=[0.0, 0.0, 60.0],  # Synthetic trips have 0 duration
        distance_miles=[0.0, 0.0, 10.0],  # Synthetic trips have 0 distance
        # Trips close together in space
        o_lat=[37.7849, 37.7749, 37.7749],  # Out of order
        o_lon=[-122.4294, -122.4194, -122.4194],
        d_lat=[37.7849, 37.7749, 37.7849],  # Synthetic trips same o/d
        d_lon=[-122.4294, -122.4194, -122.4294],
    )

    df = add_time_columns(df)
    _trips_with_ids, linked_trips = link_trips(
        df,
        CHANGE_MODE_CODE,
        TRANSIT_MODE_CODES,
    )

    # All three trips should be linked into one journey
    num_expected_segments = 3
    assert len(linked_trips) == 1
    assert linked_trips["num_segments"][0] == num_expected_segments

    # Check that the linked trip has correct properties
    linked_trip = linked_trips.row(0, named=True)
    assert linked_trip["mode_type"] == TRANSIT_MODE  # Transit mode selected
    # Total distance should be sum of all segments (only main trip has distance)
    expected_distance = df["distance_miles"].sum()
    assert (
        abs(linked_trip["distance_miles"] - expected_distance)
        < DISTANCE_TOLERANCE
    )
    # Origin should be from first trip (trip_id=1, the access trip)
    expected_origin_purpose = 1
    assert linked_trip["o_purpose_category"] == expected_origin_purpose
    # Destination should be from last trip (trip_id=3, the egress trip)
    expected_dest_purpose = 2
    assert linked_trip["d_purpose_category"] == expected_dest_purpose
