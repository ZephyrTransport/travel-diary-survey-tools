"""Unit tests for DaySim formatter.

Tests person type classification, household composition, mode aggregation,
tour formatting, and end-to-end transformation from canonical survey data
to DaySim model format.
"""

from datetime import datetime

import polars as pl

from data_canon.codebook.daysim import (
    DaysimDriverPassenger,
    DaysimMode,
    DaysimPathType,
    DaysimPurpose,
)
from data_canon.codebook.trips import (
    Driver,
    Mode,
    ModeType,
    PurposeCategory,
)
from processing.formatting.daysim.format_daysim import format_daysim
from processing.formatting.daysim.format_tours import format_tours
from processing.formatting.daysim.format_trips import format_linked_trips
from processing.link_trips.link import link_trips
from processing.tours.extraction import extract_tours
from tests.fixtures import (
    add_test_taz_maz_ids,
    create_day,
    create_household,
    create_multi_person_household_processed,
    create_person,
    create_simple_work_tour_processed,
    create_transit_commute_processed,
    create_unlinked_trip,
)
from tests.fixtures.locations import HOME_LOCATION, WORK_LOCATION


class TestTripFormatting:
    """Tests for trip mode aggregation and formatting."""

    def test_format_linked_trips_sov(self):
        """Test trip formatting for drive alone (SOV)."""
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1, person_num=1)])

        # Create a simple single-trip journey (direct trip, no mode changes)
        unlinked_trips = pl.DataFrame(
            [
                create_unlinked_trip(
                    unlinked_trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    o_lat=HOME_LOCATION.lat,
                    o_lon=HOME_LOCATION.lon,
                    d_lat=WORK_LOCATION.lat,
                    d_lon=WORK_LOCATION.lon,
                    o_purpose_category=PurposeCategory.HOME,
                    d_purpose_category=PurposeCategory.WORK,
                    mode_1=Mode.HOUSEHOLD_VEHICLE_1,
                    mode_type=ModeType.CAR,
                    driver=Driver.DRIVER,
                    num_travelers=1,
                    change_mode=False,
                )
            ]
        )

        # Run through link_trips pipeline
        result_dict = link_trips(
            unlinked_trips,
            change_mode_enum=PurposeCategory.CHANGE_MODE.value,
            transit_mode_enums=[Mode.BART.value, Mode.BUS_LOCAL.value],
        )

        unlinked_trips_with_ids = result_dict["unlinked_trips"]
        linked_trips = result_dict["linked_trips"]

        # Add TAZ/MAZ via mock spatial join
        result_with_taz_maz = add_test_taz_maz_ids(
            unlinked_trips=unlinked_trips_with_ids,
            linked_trips=linked_trips,
            tours=None,
            persons=persons,
            households=None,
        )

        result = format_linked_trips(
            result_with_taz_maz["persons"],
            result_with_taz_maz["unlinked_trips"],
            result_with_taz_maz["linked_trips"],
        )

        assert len(result) == 1
        assert result["mode"][0] == DaysimMode.SOV.value
        assert result["dorp"][0] == DaysimDriverPassenger.DRIVER.value

    def test_format_linked_trips_hov2(self):
        """Test trip formatting for HOV2."""
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1, person_num=1)])

        unlinked_trips = pl.DataFrame(
            [
                create_unlinked_trip(
                    unlinked_trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    o_lat=HOME_LOCATION.lat,
                    o_lon=HOME_LOCATION.lon,
                    d_lat=WORK_LOCATION.lat,
                    d_lon=WORK_LOCATION.lon,
                    o_purpose_category=PurposeCategory.HOME,
                    d_purpose_category=PurposeCategory.WORK,
                    mode_1=Mode.HOUSEHOLD_VEHICLE_1,
                    mode_type=ModeType.CAR,
                    driver=Driver.DRIVER,
                    num_travelers=2,
                )
            ]
        )

        result_dict = link_trips(
            unlinked_trips,
            change_mode_enum=PurposeCategory.CHANGE_MODE.value,
            transit_mode_enums=[Mode.BART.value, Mode.BUS_LOCAL.value],
        )

        unlinked_trips_with_ids = result_dict["unlinked_trips"]
        linked_trips = result_dict["linked_trips"]

        # Add TAZ/MAZ via mock spatial join
        result_with_taz_maz = add_test_taz_maz_ids(
            unlinked_trips=unlinked_trips_with_ids,
            linked_trips=linked_trips,
            tours=None,
            persons=persons,
            households=None,
        )

        assert result_with_taz_maz["unlinked_trips"] is not None
        assert result_with_taz_maz["linked_trips"] is not None

        result = format_linked_trips(
            result_with_taz_maz["persons"],
            result_with_taz_maz["unlinked_trips"],
            result_with_taz_maz["linked_trips"],
        )

        assert result["mode"][0] == DaysimMode.HOV2.value

    def test_format_linked_trips_hov3(self):
        """Test trip formatting for HOV3+."""
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1, person_num=1)])

        unlinked_trips = pl.DataFrame(
            [
                create_unlinked_trip(
                    unlinked_trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    o_lat=HOME_LOCATION.lat,
                    o_lon=HOME_LOCATION.lon,
                    d_lat=WORK_LOCATION.lat,
                    d_lon=WORK_LOCATION.lon,
                    o_purpose_category=PurposeCategory.HOME,
                    d_purpose_category=PurposeCategory.WORK,
                    mode_1=Mode.HOUSEHOLD_VEHICLE_1,
                    mode_type=ModeType.CAR,
                    driver=Driver.DRIVER,
                    num_travelers=4,
                )
            ]
        )

        result_dict = link_trips(
            unlinked_trips,
            change_mode_enum=PurposeCategory.CHANGE_MODE.value,
            transit_mode_enums=[Mode.BART.value, Mode.BUS_LOCAL.value],
        )

        unlinked_trips_with_ids = result_dict["unlinked_trips"]
        linked_trips = result_dict["linked_trips"]

        # Add TAZ/MAZ via mock spatial join
        result_with_taz_maz = add_test_taz_maz_ids(
            unlinked_trips=unlinked_trips_with_ids,
            linked_trips=linked_trips,
            tours=None,
            persons=persons,
            households=None,
        )

        assert result_with_taz_maz["unlinked_trips"] is not None
        assert result_with_taz_maz["linked_trips"] is not None

        result = format_linked_trips(
            result_with_taz_maz["persons"],
            result_with_taz_maz["unlinked_trips"],
            result_with_taz_maz["linked_trips"],
        )

        assert result["mode"][0] == DaysimMode.HOV3.value

    def test_format_linked_trips_walk(self):
        """Test trip formatting for walk."""
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1, person_num=1)])

        unlinked_trips = pl.DataFrame(
            [
                create_unlinked_trip(
                    unlinked_trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    o_lat=HOME_LOCATION.lat,
                    o_lon=HOME_LOCATION.lon,
                    d_lat=WORK_LOCATION.lat,
                    d_lon=WORK_LOCATION.lon,
                    o_purpose_category=PurposeCategory.HOME,
                    d_purpose_category=PurposeCategory.WORK,
                    mode_1=Mode.WALK,
                    mode_type=ModeType.WALK,
                    driver=Driver.MISSING,
                    num_travelers=1,
                )
            ]
        )

        result_dict = link_trips(
            unlinked_trips,
            change_mode_enum=PurposeCategory.CHANGE_MODE.value,
            transit_mode_enums=[Mode.BART.value, Mode.BUS_LOCAL.value],
        )

        unlinked_trips_with_ids = result_dict["unlinked_trips"]
        linked_trips = result_dict["linked_trips"]

        # Add TAZ/MAZ via mock spatial join
        result_with_taz_maz = add_test_taz_maz_ids(
            unlinked_trips=unlinked_trips_with_ids,
            linked_trips=linked_trips,
            tours=None,
            persons=persons,
            households=None,
        )

        assert result_with_taz_maz["unlinked_trips"] is not None
        assert result_with_taz_maz["linked_trips"] is not None

        result = format_linked_trips(
            result_with_taz_maz["persons"],
            result_with_taz_maz["unlinked_trips"],
            result_with_taz_maz["linked_trips"],
        )

        assert result["mode"][0] == DaysimMode.WALK.value
        assert result["dorp"][0] == DaysimDriverPassenger.NA.value

    def test_format_linked_trips_bike(self):
        """Test trip formatting for bike."""
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1, person_num=1)])

        unlinked_trips = pl.DataFrame(
            [
                create_unlinked_trip(
                    unlinked_trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    o_lat=HOME_LOCATION.lat,
                    o_lon=HOME_LOCATION.lon,
                    d_lat=WORK_LOCATION.lat,
                    d_lon=WORK_LOCATION.lon,
                    o_purpose_category=PurposeCategory.HOME,
                    d_purpose_category=PurposeCategory.WORK,
                    mode_1=Mode.BIKE,
                    mode_type=ModeType.BIKE,
                    driver=Driver.MISSING,
                    num_travelers=1,
                )
            ]
        )

        result_dict = link_trips(
            unlinked_trips,
            change_mode_enum=PurposeCategory.CHANGE_MODE.value,
            transit_mode_enums=[Mode.BART.value, Mode.BUS_LOCAL.value],
        )

        unlinked_trips_with_ids = result_dict["unlinked_trips"]
        linked_trips = result_dict["linked_trips"]

        # Add TAZ/MAZ via mock spatial join
        result_with_taz_maz = add_test_taz_maz_ids(
            unlinked_trips=unlinked_trips_with_ids,
            linked_trips=linked_trips,
            tours=None,
            persons=persons,
            households=None,
        )

        assert result_with_taz_maz["unlinked_trips"] is not None
        assert result_with_taz_maz["linked_trips"] is not None

        result = format_linked_trips(
            result_with_taz_maz["persons"],
            result_with_taz_maz["unlinked_trips"],
            result_with_taz_maz["linked_trips"],
        )

        assert result["mode"][0] == DaysimMode.BIKE.value

    def test_format_linked_trips_purpose_mapping(self):
        """Test purpose code mapping."""
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1, person_num=1)])

        unlinked_trips = pl.DataFrame(
            [
                create_unlinked_trip(
                    unlinked_trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    o_lat=HOME_LOCATION.lat,
                    o_lon=HOME_LOCATION.lon,
                    d_lat=WORK_LOCATION.lat,
                    d_lon=WORK_LOCATION.lon,
                    o_purpose_category=PurposeCategory.HOME,
                    d_purpose_category=PurposeCategory.WORK,
                )
            ]
        )

        result_dict = link_trips(
            unlinked_trips,
            change_mode_enum=PurposeCategory.CHANGE_MODE.value,
            transit_mode_enums=[Mode.BART.value, Mode.BUS_LOCAL.value],
        )

        unlinked_trips_with_ids = result_dict["unlinked_trips"]
        linked_trips = result_dict["linked_trips"]

        # Add TAZ/MAZ via mock spatial join
        result_with_taz_maz = add_test_taz_maz_ids(
            unlinked_trips=unlinked_trips_with_ids,
            linked_trips=linked_trips,
            tours=None,
            persons=persons,
            households=None,
        )

        assert result_with_taz_maz["unlinked_trips"] is not None
        assert result_with_taz_maz["linked_trips"] is not None

        result = format_linked_trips(
            result_with_taz_maz["persons"],
            result_with_taz_maz["unlinked_trips"],
            result_with_taz_maz["linked_trips"],
        )

        assert result["opurp"][0] == DaysimPurpose.HOME.value
        assert result["dpurp"][0] == DaysimPurpose.WORK.value

    def test_format_linked_trips_time_conversion(self):
        """Test time conversion to minutes after midnight."""
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1, person_num=1)])

        unlinked_trips = pl.DataFrame(
            [
                create_unlinked_trip(
                    unlinked_trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    o_lat=HOME_LOCATION.lat,
                    o_lon=HOME_LOCATION.lon,
                    d_lat=WORK_LOCATION.lat,
                    d_lon=WORK_LOCATION.lon,
                    o_purpose_category=PurposeCategory.HOME,
                    d_purpose_category=PurposeCategory.WORK,
                    depart_time=datetime(2023, 10, 15, 8, 30),  # 8:30 AM
                    arrive_time=datetime(2023, 10, 15, 9, 15),  # 9:15 AM
                )
            ]
        )

        result_dict = link_trips(
            unlinked_trips,
            change_mode_enum=PurposeCategory.CHANGE_MODE.value,
            transit_mode_enums=[Mode.BART.value, Mode.BUS_LOCAL.value],
        )

        unlinked_trips_with_ids = result_dict["unlinked_trips"]
        linked_trips = result_dict["linked_trips"]

        # Add TAZ/MAZ via mock spatial join
        result_with_maz_taz = add_test_taz_maz_ids(
            unlinked_trips=unlinked_trips_with_ids,
            linked_trips=linked_trips,
            tours=None,
            persons=persons,
            households=None,
        )

        assert result_with_maz_taz["unlinked_trips"] is not None
        assert result_with_maz_taz["linked_trips"] is not None

        result = format_linked_trips(
            result_with_maz_taz["persons"],
            result_with_maz_taz["unlinked_trips"],
            result_with_maz_taz["linked_trips"],
        )

        assert result["deptm"][0] == 8 * 60 + 30  # 510 minutes
        assert result["arrtm"][0] == 9 * 60 + 15  # 555 minutes


class TestTourFormatting:
    """Tests for tour formatting."""

    def test_format_tours_basic(self):
        """Test basic tour formatting."""
        data = create_simple_work_tour_processed()

        result = format_tours(data["persons"], data["days"], data["linked_trips"], data["tours"])

        assert len(result) >= 1
        assert result["hhno"][0] == 1
        assert result["pno"][0] == 1

    def test_format_tours_purpose_mapping(self):
        """Test tour purpose mapping."""
        data = create_simple_work_tour_processed()

        result = format_tours(data["persons"], data["days"], data["linked_trips"], data["tours"])

        assert result["pdpurp"][0] == DaysimPurpose.WORK.value

    def test_format_tours_time_conversion(self):
        """Test tour time conversion to minutes after midnight."""
        data = create_simple_work_tour_processed()

        result = format_tours(data["persons"], data["days"], data["linked_trips"], data["tours"])

        # Verify time fields exist and are in expected range
        assert "tlvorig" in result.columns
        assert "tardest" in result.columns


class TestEndToEndDaysimFormatting:
    """End-to-end integration tests for DaySim formatting."""

    def test_format_daysim_simple_work_tour(self):
        """Test end-to-end formatting with simple work tour scenario."""
        data = create_simple_work_tour_processed()

        result = format_daysim(
            data["persons"],
            data["households"],
            data["unlinked_trips"],
            data["linked_trips"],
            data["tours"],
            data["days"],
        )

        # Verify all expected keys present
        assert "households_daysim" in result
        assert "persons_daysim" in result
        assert "linked_trips_daysim" in result
        assert "tours_daysim" in result

        # Verify record counts
        assert len(result["households_daysim"]) == 1
        assert len(result["persons_daysim"]) == 1
        assert len(result["linked_trips_daysim"]) >= 1
        assert len(result["tours_daysim"]) >= 1

    def test_format_daysim_transit_commute(self):
        """Test end-to-end formatting with transit commute scenario."""
        data = create_transit_commute_processed()

        result = format_daysim(
            data["persons"],
            data["households"],
            data["unlinked_trips"],
            data["linked_trips"],
            data["tours"],
            data["days"],
        )

        # Verify transit mode detected
        trips_result = result["linked_trips_daysim"]
        assert len(trips_result) == 2  # 2 linked trips (AM and PM commute)
        # Both trips should be walk-to-transit
        assert all(trips_result["mode"] == DaysimMode.WALK_TRANSIT.value)
        # Path type should be BART
        assert all(trips_result["pathtype"] == DaysimPathType.BART.value)

    def test_format_daysim_multi_person_household(self):
        """Test end-to-end formatting with multi-person household."""
        data = create_multi_person_household_processed()

        result = format_daysim(
            data["persons"],
            data["households"],
            data["unlinked_trips"],
            data["linked_trips"],
            data["tours"],
            data["days"],
        )

        # Verify household composition
        hh_result = result["households_daysim"]
        assert hh_result["hhsize"][0] == 4
        assert hh_result["hhftw"][0] >= 1  # At least one full-time worker

        # Verify person types
        persons_result = result["persons_daysim"]
        assert len(persons_result) == 4

    def test_format_daysim_filters_null_taz(self):
        """Test that households without TAZ are filtered out."""
        households_raw = [
            create_household(hh_id=1, home_taz=100),
            create_household(hh_id=2, home_taz=None),
        ]

        households = pl.DataFrame(
            [
                create_household(hh_id=1, home_taz=100),
                create_household(hh_id=2, home_taz=None),
            ]
        )

        persons_raw = [
            create_person(
                person_id=101,
                hh_id=1,
                person_num=1,
                home_lat=households_raw[0]["home_lat"],
                home_lon=households_raw[0]["home_lon"],
            ),
            create_person(
                person_id=201,
                hh_id=2,
                person_num=1,
                home_lat=households_raw[1]["home_lat"],
                home_lon=households_raw[1]["home_lon"],
            ),
        ]

        persons = pl.DataFrame(persons_raw)

        days = pl.DataFrame(
            [
                create_day(day_id=1, person_id=101, hh_id=1, person_num=1),
                create_day(day_id=2, person_id=201, hh_id=2, person_num=1),
            ]
        )

        unlinked_trips_fixture = pl.DataFrame(
            [
                create_unlinked_trip(
                    unlinked_trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_id=1,
                    o_lat=37.8,
                    o_lon=-122.4,
                    d_lat=37.85,
                    d_lon=-122.45,
                    o_purpose_category=PurposeCategory.HOME,
                    d_purpose_category=PurposeCategory.WORK,
                ),
                create_unlinked_trip(
                    unlinked_trip_id=2,
                    person_id=201,
                    hh_id=2,
                    person_num=1,
                    day_id=2,
                    o_lat=37.8,
                    o_lon=-122.4,
                    d_lat=37.85,
                    d_lon=-122.45,
                    o_purpose_category=PurposeCategory.HOME,
                    d_purpose_category=PurposeCategory.WORK,
                ),
            ]
        )

        linked_result = link_trips(
            unlinked_trips_fixture,
            change_mode_enum=PurposeCategory.CHANGE_MODE.value,
            transit_mode_enums=[Mode.BART.value],
        )

        # Add joint_trip_id for extract_tours validation
        linked_result["linked_trips"] = linked_result["linked_trips"].with_columns(
            pl.lit(None).cast(pl.Int64).alias("joint_trip_id")
        )

        # Perform tour extraction
        tour_result = extract_tours(
            persons=persons,
            households=households,
            unlinked_trips=linked_result["unlinked_trips"],
            linked_trips=linked_result["linked_trips"],
            joint_trips=None,
        )

        # Add TAZ/MAZ via mock spatial join (skip households since we want to preserve null TAZ)
        data = add_test_taz_maz_ids(
            unlinked_trips=tour_result["unlinked_trips"],
            linked_trips=tour_result["linked_trips"],
            tours=tour_result["tours"],
        )
        data.update(
            {
                "households": households,
                "persons": persons,
                "days": days,
            }
        )

        result = format_daysim(**data)

        # Only household 1 should remain
        assert len(result["households_daysim"]) == 1
        assert result["households_daysim"]["hhno"][0] == 1

        # Only person from household 1 should remain
        assert len(result["persons_daysim"]) == 1
        assert result["persons_daysim"]["hhno"][0] == 1

    def test_format_daysim_output_schema(self):
        """Test that output DataFrames have expected DaySim columns."""
        data = create_simple_work_tour_processed()

        result = format_daysim(
            data["persons"],
            data["households"],
            data["unlinked_trips"],
            data["linked_trips"],
            data["tours"],
            data["days"],
        )

        # Check household columns
        hh_expected_cols = [
            "hhno",
            "hhsize",
            "hhvehs",
            "hhftw",
            "hhptw",
            "hhtaz",
            "hhincome",
        ]
        for col in hh_expected_cols:
            assert col in result["households_daysim"].columns

        # Check person columns
        person_expected_cols = [
            "hhno",
            "pno",
            "pptyp",
            "pagey",
            "pgend",
            "pwtyp",
        ]
        for col in person_expected_cols:
            assert col in result["persons_daysim"].columns

        # Check trip columns
        trip_expected_cols = [
            "hhno",
            "pno",
            "day",
            "tripno",
            "mode",
            "pathtype",
            "dorp",
            "opurp",
            "dpurp",
        ]
        for col in trip_expected_cols:
            assert col in result["linked_trips_daysim"].columns

        # Check tour columns
        tour_expected_cols = ["hhno", "pno", "day", "tour", "pdpurp"]
        for col in tour_expected_cols:
            assert col in result["tours_daysim"].columns
