"""Test formatting of WORK_RELATED tours."""

from datetime import datetime, time

import polars as pl
import pytest

from data_canon.codebook.ctramp import (
    CTRAMPPersonType,
)
from data_canon.codebook.households import IncomeBroad
from data_canon.codebook.persons import (
    Employment,
)
from data_canon.codebook.tours import TourDirection
from data_canon.codebook.trips import PurposeCategory
from processing.formatting.ctramp.ctramp_config import CTRAMPConfig
from processing.formatting.ctramp.format_households import format_households
from processing.formatting.ctramp.format_tours import (
    format_individual_tour,
)
from tests.fixtures import (
    create_household,
    create_linked_trip,
    create_person,
    create_tour,
    get_tour_schema,
)


@pytest.fixture
def standard_config():
    """Standard test configuration with explicit parameters."""
    return CTRAMPConfig(
        income_low_threshold=30000,  # $30k ($2000, MTC)
        income_med_threshold=60000,  # $60k ($2000, MTC)
        income_high_threshold=100000,  # $100k ($2000, MTC)
        income_survey_year_to_ctramp_year=0.5319148936,
        age_adult=4,  # AGE_18_TO_24 = category 4 (18+ are adults)
    )


class TestWorkRelatedMapping:
    """Tests for formatting of WORK_RELATED tours."""

    def test_basic_work_tour(self, standard_config):
        """Test formatting of a basic work tour with outbound/inbound trips."""
        # Create canonical data
        households_canonical = pl.DataFrame(
            [create_household(hh_id=1, income_bin=IncomeBroad.INCOME_75TO100)]
        )
        persons_canonical = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    employment=Employment.EMPLOYED_FULLTIME,
                    person_type=CTRAMPPersonType.FULL_TIME_WORKER.value,
                )
            ]
        )
        tours_canonical = pl.DataFrame(
            [
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    tour_purpose=PurposeCategory.WORK,
                    o_taz=100,
                    d_taz=200,
                    origin_depart_time=datetime.combine(datetime(2024, 1, 1), time(8, 0)),
                    origin_arrive_time=datetime.combine(datetime(2024, 1, 1), time(17, 0)),
                    student_category="Not student",
                )
            ],
            schema=get_tour_schema(),
        )

        # Format to CTRAMP (tours formatter needs formatted households/persons)
        households = format_households(
            households_canonical, persons_canonical, tours_canonical, standard_config
        )
        # Pass canonical persons for person_type and school_type
        tours = tours_canonical
        trips_canonical = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10002,
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    tour_direction=TourDirection.INBOUND,
                ),
            ]
        )
        trips = trips_canonical

        result = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons_canonical,
            households_ctramp=households,
            config=standard_config,
        )

        assert len(result) == 1
        assert result["tour_id"][0] == 0  # CTRAMP tour_id is 0-based (0 for first tour)
        assert result["hh_id"][0] == 1
        assert result["person_id"][0] == 101
        assert result["orig_taz"][0] == 100
        assert result["dest_taz"][0] == 200
        assert result["start_hour"][0] == 8
        assert result["end_hour"][0] == 17
        assert result["num_ob_stops"][0] == 0  # 1 OB trip = 0 stops
        assert result["num_ib_stops"][0] == 0  # 1 IB trip = 0 stops
        assert result["atWork_freq"][0] == 1  # Work tour with no subtours
        # Purpose should be work_med (income 100-150k is in med bracket)
        assert result["tour_purpose"][0] == "work_med"

    def test_at_work_tour(self, standard_config):
        """Test formatting of a basic work tour with outbound/inbound trips."""
        # Create canonical data
        households_canonical = pl.DataFrame(
            [create_household(hh_id=1, income_bin=IncomeBroad.INCOME_75TO100)]
        )
        persons_canonical = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    person_type=CTRAMPPersonType.NON_WORKER.value,
                )
            ]
        )
        tours_canonical = pl.DataFrame(
            [
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    tour_purpose=PurposeCategory.WORK_RELATED,
                    o_taz=100,
                    d_taz=200,
                    origin_depart_time=datetime.combine(datetime(2024, 1, 1), time(8, 0)),
                    origin_arrive_time=datetime.combine(datetime(2024, 1, 1), time(17, 0)),
                    student_category="Not student",
                )
            ],
            schema=get_tour_schema(),
        )

        # Format to CTRAMP (tours formatter needs formatted households/persons)
        households = format_households(
            households_canonical, persons_canonical, tours_canonical, standard_config
        )
        # Pass canonical persons for person_type and school_type
        tours = tours_canonical
        trips_canonical = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10002,
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    tour_direction=TourDirection.INBOUND,
                ),
            ]
        )
        trips = trips_canonical

        result = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons_canonical,
            households_ctramp=households,
            config=standard_config,
        )

        assert len(result) == 1
        assert result["tour_id"][0] == 0  # CTRAMP tour_id is 0-based (0 for first tour)
        assert result["hh_id"][0] == 1
        assert result["person_id"][0] == 101
        assert result["orig_taz"][0] == 100
        assert result["dest_taz"][0] == 200
        assert result["start_hour"][0] == 8
        assert result["end_hour"][0] == 17
        assert result["num_ob_stops"][0] == 0  # 1 OB trip = 0 stops
        assert result["num_ib_stops"][0] == 0  # 1 IB trip = 0 stops
        assert result["atWork_freq"][0] == 0  # No subtours
        # Purpose should be work_med (income 100-150k is in med bracket)
        assert result["tour_purpose"][0] == "othdiscr"

    def test_at_work_tour_worker(self, standard_config):
        """Test that a WORK_RELATED tour for a worker with an existing WORK tour maps to atwork_business."""  # noqa: E501
        # Create canonical data
        households_canonical = pl.DataFrame(
            [create_household(hh_id=1, income_bin=IncomeBroad.INCOME_75TO100)]
        )
        persons_canonical = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    employment=Employment.EMPLOYED_FULLTIME,
                    person_type=CTRAMPPersonType.FULL_TIME_WORKER.value,
                )
            ]
        )
        tours_canonical = pl.DataFrame(
            [
                # Worker needs a WORK tour so the WORK_RELATED tour is not
                # promoted to WORK by _promote_work_related_to_work_for_workers
                create_tour(
                    tour_id=1000,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    tour_num=1,
                    tour_purpose=PurposeCategory.WORK,
                    o_taz=100,
                    d_taz=200,
                    origin_depart_time=datetime.combine(datetime(2024, 1, 1), time(7, 0)),
                    origin_arrive_time=datetime.combine(datetime(2024, 1, 1), time(16, 0)),
                    student_category="Not student",
                ),
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    parent_tour_id=1000,
                    hh_id=1,
                    person_num=1,
                    tour_num=2,
                    tour_purpose=PurposeCategory.WORK,
                    o_taz=100,
                    d_taz=200,
                    origin_depart_time=datetime.combine(datetime(2024, 1, 1), time(8, 0)),
                    origin_arrive_time=datetime.combine(datetime(2024, 1, 1), time(17, 0)),
                    student_category="Not student",
                ),
            ],
            schema=get_tour_schema(),
        )

        # Format to CTRAMP (tours formatter needs formatted households/persons)
        households = format_households(
            households_canonical, persons_canonical, tours_canonical, standard_config
        )
        # Pass canonical persons for person_type and school_type
        tours = tours_canonical
        trips_canonical = pl.DataFrame(
            [
                # Trips for the WORK tour
                create_linked_trip(
                    trip_id=10000,
                    tour_id=1000,
                    person_id=101,
                    hh_id=1,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1000,
                    person_id=101,
                    hh_id=1,
                    tour_direction=TourDirection.INBOUND,
                ),
                # Trips for the WORK_RELATED tour
                create_linked_trip(
                    trip_id=10002,
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10003,
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    tour_direction=TourDirection.INBOUND,
                ),
            ]
        )
        trips = trips_canonical

        result = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons_canonical,
            households_ctramp=households,
            config=standard_config,
        )

        # Filter to the at-work subtour (parent tour_id 0 -> subtour encoded as 11)
        atwork_tour = result.filter(pl.col("tour_id") == 11)
        assert len(atwork_tour) == 1
        assert atwork_tour["tour_purpose"][0] == "atwork_business"
