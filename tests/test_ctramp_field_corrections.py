"""CT-RAMP fields derived from other tables: joint tour, tour frequency, WFH, purpose codes."""

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from data_canon.codebook.ctramp import (
    CTRAMPPersonType,
    CTRAMPTourCategory,
    JTFChoice,
    WFHChoice,
    build_alternatives,
    load_alternatives_from_csv,
)
from data_canon.codebook.households import IncomeBroad
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    JobType,
)
from data_canon.codebook.tours import TourDirection
from data_canon.codebook.trips import PurposeCategory
from data_canon.models.survey import JointTourModel
from processing.formatting.ctramp.ctramp_config import CTRAMPConfig
from processing.formatting.ctramp.format_ctramp import format_ctramp
from processing.formatting.ctramp.format_households import format_households
from processing.formatting.ctramp.format_persons import format_persons
from processing.formatting.ctramp.format_tours import (
    format_individual_tour,
)
from processing.formatting.ctramp.format_trips import (
    format_individual_trip,
)
from tests.fixtures.records import create_household, create_linked_trip, create_person, create_tour
from tests.fixtures.schema_utils import (
    days_for_persons,
    empty,
    get_tour_schema,
    model_to_polars_schema,
    reported_locations,
    step_tables,
)


def joint_tours_for(tours: pl.DataFrame) -> pl.DataFrame:
    """A joint_tours row per joint_tour_id, with hh_id and person_day_id from the member tours."""
    schema = model_to_polars_schema(JointTourModel)
    schema["usable_test"] = pl.Boolean
    members = tours.filter(pl.col("joint_tour_id").is_not_null())
    if members.is_empty():
        return pl.DataFrame(schema=schema)
    return (
        members.group_by("joint_tour_id")
        .agg(
            pl.col("hh_id").first(),
            pl.col("person_day_id").first(),
            pl.len().cast(pl.Int64).alias("num_participants"),
        )
        .with_columns(pl.lit(value=True).alias("usable_test"))
        .sort("joint_tour_id")
    )


def format_tours_for(
    persons: pl.DataFrame, tours: pl.DataFrame, config: CTRAMPConfig, **trip_fields
):
    """Format household 1's tours with a round trip per tour; ``trip_fields`` go on every trip.

    Returns the formatted tours, the formatted households and the canonical trips.
    """
    households = pl.DataFrame([create_household(hh_id=1, income_bin=IncomeBroad.INCOME_75TO100)])
    households_formatted = format_households(households, persons, tours, config)
    trips = pl.DataFrame(
        [
            create_linked_trip(
                trip_id=10000 + 2 * i + offset,
                tour_id=row["tour_id"],
                person_id=row["person_id"],
                hh_id=1,
                tour_direction=direction,
                **trip_fields,
            )
            for i, row in enumerate(tours.iter_rows(named=True))
            for offset, direction in ((0, TourDirection.OUTBOUND), (1, TourDirection.INBOUND))
        ]
    )
    tours_formatted = format_individual_tour(
        tours_canonical=tours,
        linked_trips_canonical=trips,
        unlinked_trips_canonical=pl.DataFrame(),
        persons_canonical=persons,
        households_ctramp=households_formatted,
        config=config,
    )
    return tours_formatted, households_formatted, trips


class TestHouseholdFieldCorrections:
    """Tests for household field corrections."""

    def test_jtf_choice_computed_from_joint_tours(self, standard_config):
        """Two joint shopping tours give TWO_SHOP through the whole format_ctramp run."""
        households = pl.DataFrame([create_household(hh_id=1, home_taz=100)])
        persons = pl.DataFrame(
            [
                create_person(person_id=101, hh_id=1),
                create_person(person_id=102, hh_id=1),
            ]
        )

        # (tour_id, person_id, person_day_id, joint_tour_id): two shopping tours, both members.
        members = [
            (1001, 101, 10101, 9001),
            (1002, 102, 10201, 9001),
            (1003, 101, 10101, 9002),
            (1004, 102, 10201, 9002),
        ]
        tours = pl.DataFrame(
            [
                create_tour(
                    tour_id=tour_id,
                    hh_id=1,
                    person_id=person_id,
                    person_day_id=person_day_id,
                    joint_tour_id=joint_tour_id,
                    tour_purpose=PurposeCategory.SHOP,
                )
                for tour_id, person_id, person_day_id, joint_tour_id in members
            ],
            schema=get_tour_schema(),
        )
        # Both legs: a one-trip tour is invalid and would be dropped before it counted.
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    linked_trip_id=tour_id * 10 + leg,
                    tour_id=tour_id,
                    person_id=person_id,
                    person_day_id=person_day_id,
                    tour_direction=direction,
                    joint_tour_id=joint_tour_id,
                )
                for tour_id, person_id, person_day_id, joint_tour_id in members
                for leg, direction in enumerate((TourDirection.OUTBOUND, TourDirection.INBOUND))
            ]
        )

        result = format_ctramp(
            **step_tables(
                "format_ctramp",
                {
                    "persons": persons,
                    "households": households,
                    "linked_trips": trips,
                    "tours": tours,
                    "joint_trips": empty("joint_trips"),
                    "unlinked_trips": empty("unlinked_trips"),
                    "joint_tours": joint_tours_for(tours),
                    "person_days": days_for_persons(persons),
                    **reported_locations(households, persons, ["taz"]),
                },
            ),
            ctramp=standard_config,
        )

        assert result["households_ctramp"]["jtf_choice"][0] == JTFChoice.TWO_SHOP.value


class TestPersonFieldCorrections:
    """Tests for person field corrections."""

    def test_inmf_matches_csv_fixture(self):
        """build_alternatives reproduces the CT-RAMP alternatives CSV row by row."""
        csv_alternatives = load_alternatives_from_csv(
            Path(__file__).parent
            / "fixtures"
            / "CTRAMP_IndividualNonMandatoryTourFrequencyAlternatives.csv"
        )
        py_alternatives = build_alternatives(
            maxes={
                "escort": 2,
                "shopping": 1,
                "othmaint": 1,
                "othdiscr": 1,
                "eatout": 1,
                "social": 1,
            }
        )

        assert py_alternatives == csv_alternatives

    def test_age_continuous_from_category_midpoint(self, standard_config):
        """Age is the category midpoint, not the category code."""
        persons = pl.DataFrame(
            [
                create_person(person_id=101, age=AgeCategory.AGE_UNDER_5),  # 2.5
                create_person(person_id=102, age=AgeCategory.AGE_5_TO_15),  # 10
                create_person(person_id=103, age=AgeCategory.AGE_35_TO_44),  # 39.5
                create_person(person_id=104, age=AgeCategory.AGE_85_AND_UP),  # 87.5
            ]
        )

        result = format_persons(persons, pl.DataFrame(), standard_config)

        assert result["age"].to_list() == [2, 10, 39, 87]

    @pytest.mark.parametrize(
        ("purposes", "expected_code"),
        [
            # Codes are the alternative numbers in
            # CTRAMP_IndividualNonMandatoryTourFrequencyAlternatives.csv, keyed
            # on (escort, shopping, othmaint, othdiscr, eatout, social).
            pytest.param([PurposeCategory.WORK], 0, id="mandatory_tours_do_not_count"),
            pytest.param([PurposeCategory.OTHER], 2, id="one_othdiscr"),
            pytest.param([PurposeCategory.MEAL], 5, id="one_eatout"),
            pytest.param([PurposeCategory.ERRAND], 9, id="one_othmaint"),
            pytest.param([PurposeCategory.SHOP], 17, id="one_shopping"),
            pytest.param(
                [PurposeCategory.SHOP, PurposeCategory.MEAL, PurposeCategory.SOCIALREC],
                23,
                id="shop_eatout_social",
            ),
            pytest.param([PurposeCategory.ESCORT], 33, id="one_escort"),
            pytest.param([PurposeCategory.ESCORT] * 2, 65, id="two_escort"),
            # Counts above the codebook maximum are capped, not overflowed.
            pytest.param([PurposeCategory.ESCORT] * 3, 65, id="three_escort_caps_to_two"),
            pytest.param([PurposeCategory.SHOP] * 2, 17, id="two_shopping_caps_to_one"),
            pytest.param(
                [
                    PurposeCategory.ESCORT,
                    PurposeCategory.ESCORT,
                    PurposeCategory.SHOP,
                    PurposeCategory.ERRAND,
                    PurposeCategory.OTHER,
                    PurposeCategory.MEAL,
                    PurposeCategory.SOCIALREC,
                ],
                96,
                id="every_category_at_its_maximum",
            ),
        ],
    )
    def test_inmf_choice_binned_to_codebook(self, purposes, expected_code, standard_config):
        """inmf_choice bins a person's non-mandatory tours to a codebook alternative."""
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [
                create_tour(tour_id=1001 + i, person_id=101, tour_num=i + 1, tour_purpose=purpose)
                for i, purpose in enumerate(purposes)
            ],
            schema=get_tour_schema(),
        )

        tours_formatted, _, _ = format_tours_for(persons, tours, standard_config)
        result = format_persons(persons, tours_formatted, standard_config)

        assert result["inmf_choice"][0] == expected_code

    def test_wfh_choice_detects_work_from_home(self, standard_config):
        """Without telecommute_time, an employed person with a WFH job_type works from home."""
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    employment=Employment.EMPLOYED_FULLTIME,
                    job_type=JobType.FIXED.value,  # Not WFH
                ),
                create_person(
                    person_id=102,
                    hh_id=1,
                    employment=Employment.EMPLOYED_FULLTIME,
                    job_type=JobType.WFH.value,  # WFH
                ),
                create_person(
                    person_id=103,
                    hh_id=1,
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    job_type=JobType.WFH.value,  # Non-worker, so not WFH
                ),
            ]
        )

        tours = pl.DataFrame(
            [
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    tour_purpose=PurposeCategory.WORK,
                )
            ],
            schema=get_tour_schema(),
        )

        tours_formatted, _, _ = format_tours_for(persons, tours, standard_config)
        result = format_persons(persons, tours_formatted, standard_config)

        assert result["wfh_choice"].to_list() == [
            WFHChoice.NON_WORKER_OR_NO_WFH.value,  # employed, FIXED job
            WFHChoice.WORKS_FROM_HOME.value,  # employed, WFH job
            WFHChoice.NON_WORKER_OR_NO_WFH.value,  # non-worker, WFH job
        ]

    def test_reported_telecommute_time_is_preferred_over_the_job_type(self, standard_config):
        """telecommute_time, where present, decides WFH; here it disagrees with job_type."""
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    employment=Employment.EMPLOYED_FULLTIME,
                    job_type=JobType.FIXED.value,
                ),
                create_person(
                    person_id=102,
                    hh_id=1,
                    employment=Employment.EMPLOYED_FULLTIME,
                    job_type=JobType.FIXED.value,
                ),
            ]
        ).with_columns(pl.Series("telecommute_time", [0, 240]))

        tours = pl.DataFrame(
            [create_tour(tour_id=1001, person_id=101, tour_purpose=PurposeCategory.WORK)],
            schema=get_tour_schema(),
        )

        tours_formatted, _, _ = format_tours_for(persons, tours, standard_config)
        result = format_persons(persons, tours_formatted, standard_config)

        # Person 102 telecommutes and made no work tour, despite a FIXED job_type.
        assert result["wfh_choice"].to_list() == [
            WFHChoice.NON_WORKER_OR_NO_WFH.value,
            WFHChoice.WORKS_FROM_HOME.value,
        ]


class TestIndividualTripFieldCorrections:
    """Tests for individual trip field corrections."""

    def test_trip_carries_the_hour_and_the_tour_purpose(self, standard_config):
        """depart_hour is the hour of depart_time; tour_purpose is joined from the tours."""
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [create_tour(tour_id=1001, person_id=101, hh_id=1, tour_purpose=PurposeCategory.WORK)],
            schema=get_tour_schema(),
        )
        tours_formatted, households_formatted, trips = format_tours_for(
            persons, tours, standard_config, depart_time=datetime(2024, 1, 1, 8, 30)
        )

        result = format_individual_trip(
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            tours_ctramp=tours_formatted,
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        assert result["depart_hour"][0] == 8
        assert result["tour_purpose"][0] == "work_med"


class TestIndividualTourFieldCorrections:
    """Tests for individual tour field corrections."""

    def test_person_type_on_tours_is_the_integer_code(self, standard_config):
        """person_type reaches the tour table as the integer code, not the label in ``type``."""
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    age=AgeCategory.AGE_35_TO_44,
                    employment=Employment.EMPLOYED_FULLTIME,
                )
            ]
        )
        tours = pl.DataFrame(
            [create_tour(tour_id=1001, person_id=101, hh_id=1)], schema=get_tour_schema()
        )

        result, _, _ = format_tours_for(persons, tours, standard_config)

        assert isinstance(result["person_type"][0], int)
        assert result["person_type"][0] == CTRAMPPersonType.FULL_TIME_WORKER.value

    def test_tour_purpose_and_category_by_purpose(self, standard_config):
        """Each purpose maps to its own CT-RAMP purpose and category label, not all to othdiscr."""
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        purposes = [
            PurposeCategory.WORK,
            PurposeCategory.SCHOOL,
            PurposeCategory.SHOP,
            PurposeCategory.MEAL,
        ]
        tours = pl.DataFrame(
            [
                create_tour(tour_id=1001 + i, person_id=101, tour_purpose=purpose)
                for i, purpose in enumerate(purposes)
            ],
            schema=get_tour_schema(),
        )

        result, _, _ = format_tours_for(persons, tours, standard_config)

        assert result["tour_purpose"].to_list() == [
            "work_med",  # income-segmented
            "school_grade",  # the default fixture person is not a college student
            "shopping",
            "eatout",
        ]
        mandatory = CTRAMPTourCategory.MANDATORY.value
        non_mandatory = CTRAMPTourCategory.INDIVIDUAL_NON_MANDATORY.value
        assert result["tour_category"].to_list() == [
            mandatory,
            mandatory,
            non_mandatory,
            non_mandatory,
        ]
