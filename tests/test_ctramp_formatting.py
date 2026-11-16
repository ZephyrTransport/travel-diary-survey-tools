"""Unit tests for the CT-RAMP formatter, table by table and end to end."""

import logging
from typing import get_args

import polars as pl
import pytest

from data_canon.codebook.ctramp import (
    AtWorkFreq,
    CTRAMPModeType,
    CTRAMPPersonType,
    CTRAMPTourCategory,
    FreeParkingChoice,
    JTFChoice,
    TourComposition,
    WFHChoice,
)
from data_canon.codebook.generic import BooleanYesNo
from data_canon.codebook.households import IncomeBroad
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    Gender,
    SchoolType,
    Student,
)
from data_canon.codebook.tours import TourDirection
from data_canon.codebook.trips import AccessEgressMode, ModeType, PurposeCategory
from data_canon.models.ctramp import (
    AllTripCTRAMPModel,
    HouseholdCTRAMPModel,
    IndividualTourCTRAMPModel,
    IndividualTripCTRAMPModel,
    JointTourCTRAMPModel,
    JointTripCTRAMPModel,
    PersonCTRAMPModel,
)
from processing.formatting.ctramp.format_ctramp import _drop_excess_fields, format_ctramp
from processing.formatting.ctramp.format_households import format_households
from processing.formatting.ctramp.format_mandatory_location import format_mandatory_location
from processing.formatting.ctramp.format_persons import format_persons
from processing.formatting.ctramp.format_tours import format_individual_tour
from processing.formatting.ctramp.format_trips import format_individual_trip
from tests.fixtures.ctramp_joint import (
    BOTH_LEGS,
    format_all,
    joint_trips_table,
    mixed_tours,
    shared_tour,
    trips_for,
    two_person_household,
)
from tests.fixtures.records import create_household, create_linked_trip, create_person, create_tour
from tests.fixtures.scenario_builders import (
    create_family_household,
    create_retired_household,
    create_single_adult_household,
    create_university_student_household,
)
from tests.fixtures.schema_utils import (
    days_for_persons,
    empty,
    get_tour_schema,
    reported_locations,
    step_tables,
)


def get_required_non_null_fields(model):
    """Names of the model's fields whose type does not admit None."""
    return [
        name
        for name, field in model.model_fields.items()
        if type(None) not in get_args(field.annotation)
    ]


def _format_person_tours(standard_config, person, tour_specs, legs=BOTH_LEGS):
    """Format one person's tours in household 1, each with one trip per entry of ``legs``.

    ``tour_specs`` holds ``(tour_id, tour_num, tour_purpose, parent_tour_id)`` per
    tour; a parent of None makes a home-based tour.
    """
    households = pl.DataFrame([create_household(hh_id=1)])
    persons = pl.DataFrame([person])
    tours = pl.DataFrame(
        [
            create_tour(
                tour_id=tour_id,
                person_id=person["person_id"],
                hh_id=1,
                tour_num=tour_num,
                tour_purpose=purpose,
                **({"parent_tour_id": parent} if parent is not None else {}),
            )
            for tour_id, tour_num, purpose, parent in tour_specs
        ],
        schema=get_tour_schema(),
    )
    trips = pl.DataFrame(
        [
            create_linked_trip(
                trip_id=tour_id * 10 + leg,
                tour_id=tour_id,
                person_id=person["person_id"],
                hh_id=1,
                tour_direction=direction,
            )
            for tour_id, *_ in tour_specs
            for leg, direction in enumerate(legs, start=1)
        ]
    )
    return format_individual_tour(
        tours_canonical=tours,
        linked_trips_canonical=trips,
        unlinked_trips_canonical=pl.DataFrame(),
        persons_canonical=persons,
        households_ctramp=format_households(households, persons, tours, standard_config),
        config=standard_config,
    )


def _format_mixed(config, *, include_joint=False):
    """Format ``mixed_tours``: a joint tour for persons 101 and 102 plus 101's work tour."""
    households, persons = two_person_household()
    tours = mixed_tours()
    trips = trips_for(tours)
    out = format_all(tours, trips, persons, households, config, include_joint=include_joint)
    return out, persons, tours, trips


class TestFreeParkingChoice:
    """Tests for free parking choice in person formatting."""

    @pytest.mark.parametrize(
        ("use_free", "use_discounted", "expected"),
        [
            (BooleanYesNo.YES, BooleanYesNo.NO, FreeParkingChoice.PARK_FOR_FREE),
            (BooleanYesNo.NO, BooleanYesNo.YES, FreeParkingChoice.PARK_FOR_FREE),
            (BooleanYesNo.YES, BooleanYesNo.YES, FreeParkingChoice.PARK_FOR_FREE),
            (BooleanYesNo.NO, BooleanYesNo.NO, FreeParkingChoice.PAY_TO_PARK),
            # 995 is "missing", which is not evidence of a subsidy.
            (BooleanYesNo.MISSING, BooleanYesNo.MISSING, FreeParkingChoice.PAY_TO_PARK),
        ],
    )
    def test_fp_choice_from_parking_subsidies(
        self, use_free, use_discounted, expected, standard_config
    ):
        """Either subsidy in use means the person does not pay to park."""
        persons = pl.DataFrame(
            [
                create_person(
                    commute_subsidy_use_free_parking=use_free,
                    commute_subsidy_use_discounted_parking=use_discounted,
                )
            ]
        )
        result = format_persons(persons, pl.DataFrame(), standard_config)
        assert result["fp_choice"][0] == expected.value


class TestHouseholdFormatting:
    """Tests for household formatting."""

    def test_basic_household_formatting(self, standard_config):
        """Test basic household formatting with all required fields."""
        households = pl.DataFrame(
            [
                create_household(
                    hh_id=1,
                    home_taz=100,
                    num_people=2,
                    num_vehicles=1,
                    num_workers=1,
                    income_bin=IncomeBroad.INCOME_75TO100,
                )
            ]
        )

        persons = pl.DataFrame(
            [
                {
                    "hh_id": 1,
                    "person_id": 1,
                    "employment": Employment.EMPLOYED_FULLTIME.value,
                },
                {
                    "hh_id": 1,
                    "person_id": 2,
                    "employment": Employment.UNEMPLOYED_NOT_LOOKING.value,
                },
            ]
        )
        tours = pl.DataFrame([], schema=get_tour_schema())
        result = format_households(households, persons, tours, standard_config)

        assert len(result) == 1
        assert result["hh_id"][0] == 1
        assert result["taz"][0] == 100
        assert result["income"][0] == 46277  # $87k (2023) midpoint deflated to $2000 (/1.88)
        assert result["autos"][0] == 1
        assert result["size"][0] == 2
        assert result["workers"][0] == 1
        assert result["jtf_choice"][0] == JTFChoice.NONE_NONE.value

    @pytest.mark.parametrize(
        ("income_column", "expected_income"),
        [
            # No income column at all: the bin midpoint stands in, deflated to
            # $2000 by income_survey_year_to_ctramp_year ($62k / 1.88).
            pytest.param(None, 32979, id="bin_midpoint_only"),
            # A null income still falls back to the bin midpoint...
            pytest.param([None], 32979, id="null_income_falls_back"),
            # ...but a reported income is kept, and only deflated.
            pytest.param([100000], 53191, id="reported_income_wins"),
        ],
    )
    def test_income_from_bin_or_reported_value(
        self, income_column, expected_income, standard_config
    ):
        """Income is the reported value where there is one, else the bin midpoint."""
        households = pl.DataFrame([create_household(hh_id=1, income_bin=IncomeBroad.INCOME_50TO75)])
        households = (
            households.drop("income")
            if income_column is None
            else households.with_columns(income=pl.Series(income_column, dtype=pl.Int64))
        )
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame([], schema=get_tour_schema())

        result = format_households(households, persons, tours, standard_config)

        assert result["income"][0] == expected_income


class TestPersonFormatting:
    """Tests for person formatting."""

    def test_basic_person_formatting(self, standard_config):
        """Test basic person formatting with all required fields."""
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    age=AgeCategory.AGE_35_TO_44,
                    gender=Gender.MALE,
                    employment=Employment.EMPLOYED_FULLTIME,
                    student=Student.NONSTUDENT,
                    commute_subsidy_use_free_parking=BooleanYesNo.YES,
                )
            ]
        )

        result = format_persons(persons, pl.DataFrame(), standard_config)

        assert len(result) == 1
        assert result["hh_id"][0] == 1
        assert result["person_id"][0] == 101
        assert result["person_num"][0] == 1
        assert result["age"][0] == 39  # Midpoint of 35-44
        assert result["gender"][0] == "m"
        assert result["type"][0] == CTRAMPPersonType.FULL_TIME_WORKER.label
        assert result["fp_choice"][0] == FreeParkingChoice.PARK_FOR_FREE.value
        assert result["activity_pattern"][0] == "H"  # Placeholder
        assert result["imf_choice"][0] == 0  # Placeholder
        assert result["inmf_choice"][0] == 0  # Placeholder (default)
        assert result["wfh_choice"][0] == WFHChoice.NON_WORKER_OR_NO_WFH.value  # Placeholder

    def test_gender_mapping(self, standard_config):
        """Test gender mapping to m/f format."""
        persons = pl.DataFrame(
            [
                create_person(person_id=101, gender=Gender.FEMALE),
                create_person(person_id=102, gender=Gender.MALE),
                create_person(person_id=103, gender=Gender.OTHER),
            ]
        )

        result = format_persons(persons, pl.DataFrame(), standard_config)

        assert result["gender"][0] == "f"
        assert result["gender"][1] == "m"
        assert result["gender"][2] == "f"  # Defaults to f


class TestEndToEndFormatting:
    """Tests for end-to-end CT-RAMP formatting."""

    @pytest.mark.parametrize(
        ("build_household", "expected_types"),
        [
            pytest.param(
                create_single_adult_household,
                [CTRAMPPersonType.FULL_TIME_WORKER],
                id="single_adult",
            ),
            pytest.param(
                create_family_household,
                [
                    CTRAMPPersonType.FULL_TIME_WORKER,
                    CTRAMPPersonType.PART_TIME_WORKER,
                    CTRAMPPersonType.STUDENT_DRIVING_AGE,
                    CTRAMPPersonType.STUDENT_NON_DRIVING_AGE,
                ],
                id="family",
            ),
            pytest.param(
                create_retired_household,
                [CTRAMPPersonType.RETIRED, CTRAMPPersonType.RETIRED],
                id="retired",
            ),
            pytest.param(
                create_university_student_household,
                [CTRAMPPersonType.UNIVERSITY_STUDENT],
                id="university_student",
            ),
        ],
    )
    def test_person_types_by_household_scenario(
        self, build_household, expected_types, standard_config
    ):
        """Each scenario builder produces exactly the person types it is named for."""
        households, persons = build_household()

        result = format_ctramp(
            **step_tables(
                "format_ctramp",
                {
                    "persons": persons,
                    "households": households,
                    "linked_trips": empty("linked_trips"),
                    "tours": empty("tours"),
                    "joint_trips": empty("joint_trips"),
                    "unlinked_trips": empty("unlinked_trips"),
                    "joint_tours": empty("joint_tours"),
                    "person_days": days_for_persons(persons),
                    **reported_locations(households, persons, ["taz"]),
                },
            ),
            ctramp=standard_config,
        )

        # CT-RAMP ids are person-day encoded: hh_id * 100 + day_num.
        assert result["households_ctramp"]["hh_id"].to_list() == [households["hh_id"][0] * 100 + 1]
        assert sorted(result["persons_ctramp"]["type"].to_list()) == sorted(
            person_type.label for person_type in expected_types
        )


class TestColumnPresence:
    """Every required CT-RAMP column is on the formatted table."""

    @pytest.mark.parametrize(
        ("table", "model"),
        [
            pytest.param("households", HouseholdCTRAMPModel, id="households"),
            pytest.param("persons", PersonCTRAMPModel, id="persons"),
            pytest.param("individual_tours", IndividualTourCTRAMPModel, id="individual_tours"),
            pytest.param("individual_trips", IndividualTripCTRAMPModel, id="individual_trips"),
            pytest.param("joint_tours", JointTourCTRAMPModel, id="joint_tours"),
            pytest.param("joint_trips", JointTripCTRAMPModel, id="joint_trips"),
        ],
    )
    def test_required_columns_are_present(self, table, model, standard_config):
        """Each formatter writes every non-optional field of its model."""
        out, persons, _, _ = _format_mixed(standard_config)
        out["persons"] = format_persons(persons, pl.DataFrame(), standard_config)

        missing = [c for c in get_required_non_null_fields(model) if c not in out[table].columns]
        assert not missing


class TestIndividualTourFormatting:
    """Tests for individual tour formatting."""

    def test_basic_work_tour(self, standard_config):
        """A work tour from the fixture defaults: TAZ 100 to 200, 08:00 to 17:00, $75-100k."""
        result = _format_person_tours(
            standard_config,
            create_person(person_id=101, hh_id=1),
            [(1001, 1, PurposeCategory.WORK, None)],
        )

        assert len(result) == 1
        assert result["tour_id"][0] == 0  # CTRAMP tour_id is 0-based (0 for first tour)
        assert result["hh_id"][0] == 1
        assert result["person_id"][0] == 101
        assert result["orig_taz"][0] == 100
        assert result["dest_taz"][0] == 200
        assert result["start_hour"][0] == 8
        assert result["end_hour"][0] == 17
        assert result["num_ob_stops"][0] == 0
        assert result["num_ib_stops"][0] == 0
        # A work tour with no subtours is NO_SUBTOUR (1); 0 means "tour is not at work".
        assert result["atWork_freq"][0] == AtWorkFreq.NO_SUBTOUR.value
        assert result["tour_purpose"][0] == "work_med"

    def test_stop_counting_multiple_stops(self, standard_config):
        """Stops on a leg are its trips less one: 3 outbound and 2 inbound trips."""
        out, back = TourDirection.OUTBOUND, TourDirection.INBOUND
        result = _format_person_tours(
            standard_config,
            create_person(person_id=101, hh_id=1),
            [(1001, 1, PurposeCategory.WORK, None)],
            legs=(out, out, out, back, back),
        )

        assert result["num_ob_stops"][0] == 2
        assert result["num_ib_stops"][0] == 1

    def test_subtour_counting(self, standard_config):
        """Subtours are numbered <parent tour #><subtour #>; atWork_freq is their category."""
        result = _format_person_tours(
            standard_config,
            create_person(person_id=101, hh_id=1),
            [
                (1001, 1, PurposeCategory.WORK, None),
                (1002, 2, PurposeCategory.WORK_RELATED, 1001),
                (1003, 3, PurposeCategory.MEAL, 1001),
            ],
        )

        at_work_freq = dict(zip(result["tour_id"], result["atWork_freq"], strict=True))
        assert at_work_freq == {
            0: AtWorkFreq.ONE_EAT_ONE_BUSINESS.value,
            # Subtours are not themselves at work.
            11: AtWorkFreq.NONE_NOT_WORK.value,
            12: AtWorkFreq.NONE_NOT_WORK.value,
        }

    def test_a_work_subtour_is_atwork_business(self, standard_config):
        """A worker's work tour made from a work tour is the at-work subtour atwork_business."""
        result = _format_person_tours(
            standard_config,
            create_person(person_id=101, hh_id=1, employment=Employment.EMPLOYED_FULLTIME),
            [(1000, 1, PurposeCategory.WORK, None), (1001, 2, PurposeCategory.WORK, 1000)],
        )

        assert result.filter(pl.col("tour_id") == 11)["tour_purpose"].to_list() == [
            "atwork_business"
        ]

    def test_zero_trip_tour_validation(self, standard_config):
        """A tour with no trips raises."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [create_tour(tour_id=1001, person_id=101, hh_id=1)], schema=get_tour_schema()
        )

        with pytest.raises(ValueError, match="Found 1 tours with zero trips"):
            format_individual_tour(
                tours_canonical=tours,
                linked_trips_canonical=pl.DataFrame([]),
                unlinked_trips_canonical=pl.DataFrame(),
                persons_canonical=persons,
                households_ctramp=format_households(households, persons, tours, standard_config),
                config=standard_config,
            )


class TestWorkRelatedCountsAsWork:
    """A worker's work-related tour is a work tour; a non-worker's is not.

    The survey separates going to the workplace from other work activity -- a
    meeting, a jobsite, volunteering. CT-RAMP does not: a worker who spends the
    day working somewhere has made a work tour. A non-worker's work-related tour
    (volunteering, a child carried along on a parent's trip) stays non-mandatory,
    because CT-RAMP gives those person types no work tours.
    """

    def test_a_workers_work_related_tour_is_a_mandatory_work_tour(self, standard_config):
        """A meeting or jobsite day by a worker is a work tour, segmented by income."""
        worker = create_person(person_id=101, hh_id=1, employment=Employment.EMPLOYED_FULLTIME)
        result = _format_person_tours(
            standard_config, worker, [(1001, 1, PurposeCategory.WORK_RELATED, None)]
        )

        assert result["tour_purpose"][0].startswith("work_")
        assert result["tour_category"][0] == CTRAMPTourCategory.MANDATORY.value
        assert result["atWork_freq"][0] == AtWorkFreq.NO_SUBTOUR.value

    def test_a_subtour_of_a_work_related_tour_is_at_work(self, standard_config):
        """Lunch away from a work-related site is an at-work subtour of that work tour."""
        worker = create_person(person_id=101, hh_id=1, employment=Employment.EMPLOYED_FULLTIME)
        result = _format_person_tours(
            standard_config,
            worker,
            [
                (1001, 1, PurposeCategory.WORK_RELATED, None),
                (1002, 2, PurposeCategory.MEAL, 1001),
            ],
        )

        parent = result.filter(pl.col("tour_id") == 0)
        subtour = result.filter(pl.col("tour_id") == 11)
        assert subtour["tour_category"][0] == CTRAMPTourCategory.AT_WORK.value
        assert subtour["tour_purpose"][0] == "atwork_eat"
        assert parent["atWork_freq"][0] == AtWorkFreq.ONE_EAT.value

    def test_an_employed_students_work_related_tour_is_work(self, standard_config):
        """Employment decides, not person type: a student with a job works too."""
        student = create_person(
            person_id=101,
            hh_id=1,
            age=AgeCategory.AGE_18_TO_24,
            employment=Employment.EMPLOYED_PARTTIME,
            student=Student.FULLTIME_INPERSON,
            school_type=SchoolType.COLLEGE_4YEAR,
        )
        result = _format_person_tours(
            standard_config, student, [(1001, 1, PurposeCategory.WORK_RELATED, None)]
        )

        assert result["tour_purpose"][0].startswith("work_")
        assert result["tour_category"][0] == CTRAMPTourCategory.MANDATORY.value

    def test_a_non_workers_work_related_tour_stays_non_mandatory(self, standard_config):
        """A retiree's volunteering is not a work tour; CT-RAMP gives them none."""
        retiree = create_person(
            person_id=101,
            hh_id=1,
            age=AgeCategory.AGE_65_TO_74,
            employment=Employment.UNEMPLOYED_NOT_LOOKING,
        )
        result = _format_person_tours(
            standard_config, retiree, [(1001, 1, PurposeCategory.WORK_RELATED, None)]
        )

        assert result["tour_purpose"][0] == "othdiscr"
        assert result["tour_category"][0] == CTRAMPTourCategory.INDIVIDUAL_NON_MANDATORY.value
        assert result["atWork_freq"][0] == AtWorkFreq.NONE_NOT_WORK.value

    def test_a_non_workers_work_tour_is_still_work(self, standard_config):
        """Unchanged: the rule widens work for workers, it narrows nothing."""
        retiree = create_person(
            person_id=101,
            hh_id=1,
            age=AgeCategory.AGE_65_TO_74,
            employment=Employment.UNEMPLOYED_NOT_LOOKING,
        )
        result = _format_person_tours(
            standard_config, retiree, [(1001, 1, PurposeCategory.WORK, None)]
        )

        assert result["tour_purpose"][0].startswith("work_")
        assert result["tour_category"][0] == CTRAMPTourCategory.MANDATORY.value


def _tours_ctramp(person_id, *purposes):
    """CT-RAMP tours for one person, one row per purpose string."""
    return pl.DataFrame({"person_id": [person_id] * len(purposes), "tour_purpose": list(purposes)})


class TestWhatTheModelCannotTakeIsLogged:
    """Days and tours the model has no alternative for are reported, never changed.

    The model chooses a person-day's mandatory pattern from a fixed set it
    narrows by person type, and sends work and school tours to a usual
    location. Survey days outside that are real; the log says how many.
    """

    def test_a_pattern_not_offered_to_the_person_type(self, standard_config, caplog):
        """A 16-17 student's day of work alone: the model gives them work only with school."""
        student = create_person(
            person_id=101,
            hh_id=1,
            age=AgeCategory.AGE_16_TO_17,
            employment=Employment.EMPLOYED_PARTTIME,
            student=Student.FULLTIME_INPERSON,
            school_type=SchoolType.HIGH_SCHOOL,
        )

        with caplog.at_level(logging.WARNING):
            result = format_persons(
                pl.DataFrame([student]), _tours_ctramp(101, "work_med"), standard_config
            )

        assert result["imf_choice"][0] == 1  # kept: one work tour
        assert "Student of driving age" in caplog.text
        assert "one work tour" in caplog.text

    def test_work_and_school_tours_with_no_usual_location(self, standard_config, caplog):
        """The tours stay work and school tours; the location stays 0."""
        worker = create_person(person_id=101, hh_id=1, employment=Employment.EMPLOYED_FULLTIME)
        persons = pl.DataFrame([worker])
        tours = _tours_ctramp(101, "work_med", "university")
        households = format_households(
            pl.DataFrame([create_household(hh_id=1)]), persons, empty("tours"), standard_config
        )

        with caplog.at_level(logging.WARNING):
            result = format_mandatory_location(
                format_persons(persons, tours, standard_config),
                households,
                pl.DataFrame(),
                standard_config,
            )

        assert result["WorkLocation"][0] == 0
        assert "1 work tour(s)" in caplog.text
        assert "no usual workplace" in caplog.text
        assert "1 school tour(s)" in caplog.text
        assert "no usual school" in caplog.text


def test_basic_joint_tour(standard_config):
    """An adult and a child on one shopping tour: joint tour 0, no stops, adults and children."""
    households, persons = two_person_household(age=AgeCategory.AGE_5_TO_15)
    tours = shared_tour(PurposeCategory.SHOP)
    trips = trips_for(tours)

    result = format_all(
        tours,
        trips,
        persons,
        households,
        standard_config,
        # Unweighted joint tables.
        joint_tours_canonical=pl.DataFrame(),
        joint_trips_canonical=joint_trips_table(trips).drop("joint_trip_weight"),
    )["joint_tours"]

    assert len(result) == 1
    assert result["tour_id"][0] == 0  # CTRAMP joint tour_id is 0-based per household
    assert result["hh_id"][0] == 1
    assert result["num_ob_stops"][0] == 0
    assert result["num_ib_stops"][0] == 0
    assert result["tour_composition"][0] == TourComposition.ADULTS_AND_CHILDREN.value


def test_joint_tour_stops_are_one_members(standard_config):
    """Two members each stopping once on the way out make a tour with one outbound stop."""
    households, persons = two_person_household()
    tours = shared_tour(PurposeCategory.SHOP)
    trips = trips_for(tours)
    stop_legs = trips.filter(pl.col("tour_direction") == TourDirection.OUTBOUND.value).with_columns(
        pl.col("linked_trip_id") + 100, pl.col("joint_trip_id") + 2
    )

    result = format_all(tours, pl.concat([trips, stop_legs]), persons, households, standard_config)

    assert result["joint_tours"].select("num_ob_stops", "num_ib_stops").row(0) == (1, 0)


@pytest.mark.parametrize(
    ("access", "expected"),
    [
        pytest.param(AccessEgressMode.WALK, CTRAMPModeType.WLK_LOC_WLK, id="walked"),
        pytest.param(AccessEgressMode.CAR_HOUSEHOLD, CTRAMPModeType.DRV_LOC_WLK, id="drove"),
    ],
)
def test_joint_transit_tour_mode_follows_its_access(standard_config, access, expected):
    """A joint transit tour is drive-transit when its members drove to transit."""
    households, persons = two_person_household()
    tours = shared_tour(PurposeCategory.SHOP).with_columns(
        pl.lit(ModeType.TRANSIT.value).alias("tour_mode")
    )
    trips = trips_for(tours).with_columns(
        pl.lit(ModeType.TRANSIT.value).alias("mode_type"),
        pl.lit(access.value).alias("access_mode"),
        pl.lit(AccessEgressMode.WALK.value).alias("egress_mode"),
    )

    result = format_all(tours, trips, persons, households, standard_config)

    assert result["joint_tours"]["tour_mode"][0] == expected.value


LEVELS = [
    pytest.param("_households", "hh_weight", id="household"),
    pytest.param("_persons", "person_weight", id="person"),
    pytest.param("_tours", "tour_weight", id="tour"),
    # The trip table renames linked_trip_weight to trip_weight on the way out.
    pytest.param("_trips", "trip_weight", id="trip"),
]


class TestWeightsAndSampleRateFormatting:
    """``sampleRate`` is 1/weight, per level from that level's own weight; null for zero or null.

    Each builder takes the input weights, or None to leave the weight column off,
    and returns the formatted frame in input order.
    """

    @staticmethod
    def _households(weights, config):
        """Format one household per weight, each with a single person."""
        if weights is None:
            households = pl.DataFrame([create_household(hh_id=1)]).drop("hh_weight")
        else:
            households = pl.DataFrame(
                [create_household(hh_id=i, hh_weight=w) for i, w in enumerate(weights, start=1)],
                schema_overrides={"hh_weight": pl.Float64},
            )
        persons = pl.DataFrame(
            [create_person(person_id=100 * i + 1, hh_id=i) for i in range(1, len(households) + 1)]
        )
        tours = pl.DataFrame([], schema=get_tour_schema())
        return format_households(households, persons, tours, config).sort("hh_id")

    @staticmethod
    def _persons(weights, config):
        """Format one household's persons, one per weight."""
        if weights is None:
            persons = pl.DataFrame([create_person(person_id=101, hh_id=1)]).drop("person_weight")
        else:
            persons = pl.DataFrame(
                [
                    create_person(person_id=100 + i, hh_id=1, person_weight=w)
                    for i, w in enumerate(weights, start=1)
                ],
                schema_overrides={"person_weight": pl.Float64},
            )
        return format_persons(persons, pl.DataFrame(), config).sort("person_id")

    @staticmethod
    def _tours(weights, config):
        """Format one person's tours, one per weight, each a simple round trip."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        if weights is None:
            tours = pl.DataFrame(
                [create_tour(tour_id=1001, person_id=101, hh_id=1)],
                schema=get_tour_schema(),
            ).drop("tour_weight")
        else:
            tours = pl.DataFrame(
                [
                    create_tour(tour_id=1000 + i, person_id=101, hh_id=1, tour_num=i, tour_weight=w)
                    for i, w in enumerate(weights, start=1)
                ],
                schema={**get_tour_schema(), "tour_weight": pl.Float64},
            )
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10000 + 2 * i + leg,
                    tour_id=tour_id,
                    person_id=101,
                    tour_direction=direction,
                )
                for i, tour_id in enumerate(tours["tour_id"].to_list())
                for leg, direction in enumerate((TourDirection.OUTBOUND, TourDirection.INBOUND))
            ]
        )
        households_ctramp = format_households(households, persons, tours, config)
        return format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_ctramp,
            config=config,
        ).sort("tour_id")

    @staticmethod
    def _trips(weights, config):
        """Format one tour's trips, one per weight."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [create_tour(tour_id=1001, person_id=101, hh_id=1)],
            schema=get_tour_schema(),
        )
        legs = (TourDirection.OUTBOUND, TourDirection.INBOUND, TourDirection.INBOUND)
        if weights is None:
            trips = pl.DataFrame(
                [
                    create_linked_trip(
                        linked_trip_id=10000 + i, tour_id=1001, person_id=101, tour_direction=leg
                    )
                    for i, leg in enumerate(legs[:2], start=1)
                ]
            ).drop("linked_trip_weight")
        else:
            trips = pl.DataFrame(
                [
                    create_linked_trip(
                        linked_trip_id=10000 + i,
                        tour_id=1001,
                        person_id=101,
                        tour_direction=legs[i - 1],
                        linked_trip_weight=w,
                    )
                    for i, w in enumerate(weights, start=1)
                ],
                schema_overrides={"linked_trip_weight": pl.Float64},
            )
        households_ctramp = format_households(households, persons, tours, config)
        tours_ctramp = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_ctramp,
            config=config,
        )
        return format_individual_trip(
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            tours_ctramp=tours_ctramp,
            persons_canonical=persons,
            households_ctramp=households_ctramp,
            config=config,
        ).sort("linked_trip_id")

    @pytest.mark.parametrize(("level", "weight_column"), LEVELS)
    def test_sample_rate_is_the_inverse_of_the_weight(self, level, weight_column, standard_config):
        """A usable weight inverts; zero and null produce no rate at all."""
        result = getattr(self, level)([2.5, 0.0, None], standard_config)

        assert result[weight_column].to_list() == [2.5, 0.0, None]
        assert result["sampleRate"].to_list() == [0.4, None, None]

    @pytest.mark.parametrize(("level", "weight_column"), LEVELS)
    def test_no_weight_column_means_no_sample_rate(self, level, weight_column, standard_config):
        """An unweighted input stays unweighted: neither column is invented."""
        result = getattr(self, level)(None, standard_config)

        assert weight_column not in result.columns
        assert "sampleRate" not in result.columns


class TestAllTables:
    """The All* tables: one row per person-record, joint travel included.

    CT-RAMP splits tours and trips across an individual and a joint file with
    different weight conventions; these keep the per-person form, so a total
    needs no reconciliation between the two.
    """

    def test_joint_members_appear_as_their_own_person_tours(self, standard_config):
        """A joint tour adds one row per member, each keeping the weight it had."""
        individual = _format_mixed(standard_config)[0]["individual_tours"]
        out, _, tours, _ = _format_mixed(standard_config, include_joint=True)
        all_tours = out["individual_tours"]

        assert len(individual) == 1
        assert len(all_tours) == 3
        assert all_tours["joint_tour_id"].null_count() == 1
        assert sorted(all_tours["tour_weight"]) == sorted(tours["tour_weight"])

    def test_all_trips_reconcile_with_the_canonical_trips(self, standard_config):
        """Every trip appears once with its weight, and shared trips keep their joint ids."""
        out, _, _, trips = _format_mixed(standard_config, include_joint=True)
        all_trips = out["individual_trips"]

        assert len(all_trips) == len(trips)
        assert all_trips["trip_weight"].sum() == pytest.approx(trips["linked_trip_weight"].sum())
        assert all_trips.filter(pl.col("joint_tour_id").is_not_null()).height == 4

    def test_joint_columns_survive_the_model_trim(self):
        """The All* models declare the joint ids, so _drop_excess_fields keeps them."""
        frame = pl.DataFrame(
            {"hh_id": [1], "joint_trip_id": [500], "joint_tour_id": [900], "spurious": [1]}
        )

        kept = _drop_excess_fields(frame, AllTripCTRAMPModel)
        assert "joint_trip_id" in kept.columns
        assert "joint_tour_id" in kept.columns
        assert "spurious" not in kept.columns
        assert "joint_trip_id" not in _drop_excess_fields(frame, IndividualTripCTRAMPModel).columns
