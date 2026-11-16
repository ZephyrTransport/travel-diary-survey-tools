"""Unit tests for the DaySim formatter, table by table and end to end."""

from datetime import datetime

import polars as pl
import pytest

from data_canon.codebook.days import TravelDow
from data_canon.codebook.daysim import (
    DaysimDriverPassenger,
    DaysimMode,
    DaysimPathType,
    DaysimPersonType,
    DaysimPurpose,
)
from data_canon.codebook.households import (
    IncomeBroad,
    ResidenceRentOwn,
    ResidenceType,
)
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    Gender,
    SchoolType,
    Student,
)
from data_canon.codebook.trips import (
    Driver,
    Mode,
    ModeType,
    PurposeCategory,
)
from processing.formatting.daysim.format_days import format_days
from processing.formatting.daysim.format_daysim import format_daysim
from processing.formatting.daysim.format_households import format_households
from processing.formatting.daysim.format_persons import (
    compute_day_completeness,
    format_persons,
)
from processing.formatting.daysim.format_tours import format_tours
from processing.formatting.daysim.format_trips import format_linked_trips
from processing.link_trips.link import link_trips
from tests.fixtures.fixtures import (
    add_test_taz_maz_ids,
    create_multi_person_household_processed,
    create_simple_work_tour_processed,
    create_transit_commute_processed,
)
from tests.fixtures.locations import HOME_LOCATION, WORK_LOCATION
from tests.fixtures.records import create_day, create_household, create_person, create_unlinked_trip
from tests.fixtures.schema_utils import reported_locations, step_tables


def _days(*schedule, person_id: int = 101, hh_id: int = 1, person_num: int = 1) -> list[dict]:
    """Day records for one person, one per ``(travel_dow, survey_complete)`` pair."""
    return [
        create_day(
            person_day_id=day_num,
            person_id=person_id,
            hh_id=hh_id,
            person_num=person_num,
            day_num=day_num,
            travel_dow=travel_dow,
            survey_complete=complete,
        )
        for day_num, (travel_dow, complete) in enumerate(schedule, start=1)
    ]


class TestDayCompleteness:
    """compute_day_completeness: one row per person, a flag per weekday and weekday counts."""

    @pytest.mark.parametrize(
        ("days", "expected"),
        [
            pytest.param(
                _days((TravelDow.MONDAY, True)),
                {
                    1: {
                        "hhno": 1,
                        "mon_complete": 1,
                        "tue_complete": 0,
                        "num_days_complete_3dayweekday": 0,  # Tue+Wed+Thu
                        "num_days_complete_4dayweekday": 1,  # Mon+Tue+Wed+Thu
                        "num_days_complete_5dayweekday": 1,  # Mon-Fri
                    }
                },
                id="one_complete_monday",
            ),
            pytest.param(
                _days(*[(TravelDow(i), True) for i in range(1, 8)], person_id=201, hh_id=2),
                {
                    1: {
                        "hhno": 2,
                        "mon_complete": 1,
                        "sun_complete": 1,
                        "num_days_complete_3dayweekday": 3,
                        "num_days_complete_4dayweekday": 4,
                        "num_days_complete_5dayweekday": 5,
                    }
                },
                id="full_week",
            ),
            pytest.param(
                _days(
                    (TravelDow.TUESDAY, True),
                    (TravelDow.WEDNESDAY, False),
                    (TravelDow.THURSDAY, True),
                    person_id=301,
                    hh_id=3,
                ),
                {
                    1: {
                        "tue_complete": 1,
                        "wed_complete": 0,
                        "thu_complete": 1,
                        "num_days_complete_3dayweekday": 2,
                    }
                },
                id="an_incomplete_day",
            ),
            pytest.param(
                _days((TravelDow.MONDAY, True), person_id=101, person_num=1)
                + _days((TravelDow.MONDAY, False), person_id=102, person_num=2),
                {1: {"mon_complete": 1}, 2: {"mon_complete": 0}},
                id="two_persons",
            ),
        ],
    )
    def test_compute_day_completeness(self, days, expected):
        """Each person's row carries the flags and counts of their own days."""
        result = compute_day_completeness(pl.DataFrame(days))

        assert len(result) == len(expected)
        for pno, values in expected.items():
            row = result.filter(pl.col("pno") == pno).row(0, named=True)
            assert {column: row[column] for column in values} == values, pno


class TestPersonFormatting:
    """Tests for person formatting and type classification."""

    @pytest.mark.parametrize(
        ("attributes", "expected"),
        [
            pytest.param(
                {
                    "employment": Employment.EMPLOYED_FULLTIME,
                    "age": AgeCategory.AGE_35_TO_44,
                    "work_mode": Mode.HOUSEHOLD_VEHICLE_1,
                    "work_taz": 200,
                    "work_maz": 2000,
                },
                {
                    "pptyp": DaysimPersonType.FULL_TIME_WORKER.value,
                    "pwtyp": 1,
                    "pagey": 40,  # Midpoint of AGE_35_TO_44
                    "pwtaz": 200,
                    "pwpcl": 2000,
                },
                id="full_time_worker",
            ),
            pytest.param(
                {
                    "employment": Employment.EMPLOYED_PARTTIME,
                    "age": AgeCategory.AGE_25_TO_34,
                    "work_mode": Mode.HOUSEHOLD_VEHICLE_1,
                    "work_taz": 200,
                    "work_maz": 2000,
                },
                {"pptyp": DaysimPersonType.PART_TIME_WORKER.value, "pwtyp": 2},
                id="part_time_worker",
            ),
            pytest.param(
                {
                    "employment": Employment.UNEMPLOYED_NOT_LOOKING,
                    "student": Student.FULLTIME_INPERSON,
                    "age": AgeCategory.AGE_18_TO_24,
                    "school_taz": 300,
                    "school_maz": 3000,
                    "school_type": SchoolType.COLLEGE_4YEAR,
                    "work_mode": Mode.MISSING,
                },
                {
                    "pptyp": DaysimPersonType.UNIVERSITY_STUDENT.value,
                    "pwtaz": -1,  # No work location
                    "pstaz": 300,
                    "pspcl": 3000,
                },
                id="university_student",
            ),
            pytest.param(
                {
                    "employment": Employment.UNEMPLOYED_NOT_LOOKING,
                    "student": Student.FULLTIME_INPERSON,
                    "age": AgeCategory.AGE_16_TO_17,
                    "school_taz": 150,
                    "school_maz": 1500,
                    "school_type": SchoolType.HIGH_SCHOOL,
                    "work_mode": Mode.MISSING,
                },
                {
                    "pptyp": DaysimPersonType.CHILD_DRIVING_AGE.value,
                    "pstaz": 150,
                    "pspcl": 1500,
                },
                id="high_school_student",
            ),
            pytest.param(
                {
                    "employment": Employment.UNEMPLOYED_NOT_LOOKING,
                    "student": Student.NONSTUDENT,
                    "age": AgeCategory.AGE_65_TO_74,
                    "work_mode": Mode.MISSING,
                },
                {"pptyp": DaysimPersonType.RETIRED.value, "pwtaz": -1},
                id="retiree",
            ),
            pytest.param(
                {
                    "employment": Employment.UNEMPLOYED_NOT_LOOKING,
                    "student": Student.NONSTUDENT,
                    "age": AgeCategory.AGE_25_TO_34,
                },
                {"pptyp": DaysimPersonType.NON_WORKER.value},
                id="non_working_adult",
            ),
            pytest.param(
                {
                    "employment": Employment.UNEMPLOYED_NOT_LOOKING,
                    "student": Student.NONSTUDENT,
                    "age": AgeCategory.AGE_5_TO_15,
                },
                {"pptyp": DaysimPersonType.CHILD_NON_DRIVING_AGE.value, "pagey": 10},
                id="child_non_driving",
            ),
            pytest.param(
                {
                    "employment": Employment.UNEMPLOYED_NOT_LOOKING,
                    "student": Student.NONSTUDENT,
                    "age": AgeCategory.AGE_UNDER_5,
                },
                {"pptyp": DaysimPersonType.CHILD_UNDER_5.value, "pagey": 3},
                id="child_under_5",
            ),
        ],
    )
    def test_format_persons_person_type(self, attributes, expected):
        """Pptyp and the mandatory locations come from age, employment and study.

        ``person_type`` is never read off the input: ``format_persons`` derives
        it from ``pagey``/``employment``/``student``, so these rows pass none.
        """
        days_list = _days((TravelDow.MONDAY, True))
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101, hh_id=1, person_num=1, person_days=days_list, **attributes
                )
            ]
        )

        result = format_persons(persons, pl.DataFrame(days_list))

        assert len(result) == 1
        for column, value in expected.items():
            assert result[column][0] == value, column

    def test_format_persons_gender_mapping(self):
        """Test gender code mapping."""
        days_list = _days((TravelDow.MONDAY, True)) + _days(
            (TravelDow.MONDAY, True), person_id=102, person_num=2
        )
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=person_id,
                    hh_id=1,
                    person_num=person_num,
                    age=AgeCategory.AGE_35_TO_44,
                    employment=Employment.EMPLOYED_FULLTIME,
                    gender=gender,
                    person_days=[d for d in days_list if d["person_id"] == person_id],
                )
                for person_id, person_num, gender in (
                    (101, 1, Gender.MALE),
                    (102, 2, Gender.FEMALE),
                )
            ]
        )

        result = format_persons(persons, pl.DataFrame(days_list))

        assert result.filter(pl.col("pno") == 1)["pgend"][0] == 1  # Male
        assert result.filter(pl.col("pno") == 2)["pgend"][0] == 2  # Female


class TestHouseholdFormatting:
    """Tests for household formatting and composition."""

    def test_format_households_single_person(self):
        """Test household formatting with single person."""
        households = pl.DataFrame(
            [
                create_household(
                    hh_id=1,
                    home_taz=100,
                    home_maz=1000,
                    num_vehicles=1,
                    income_bin=IncomeBroad.INCOME_75TO100,
                    residence_rent_own=ResidenceRentOwn.OWN,
                    residence_type=ResidenceType.SFH,
                    num_workers=1,
                )
            ]
        )

        days_list = _days((TravelDow.MONDAY, True))
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    person_type=DaysimPersonType.FULL_TIME_WORKER,
                    person_days=days_list,
                )
            ]
        )

        persons_daysim = format_persons(persons, pl.DataFrame(days_list))

        result = format_households(households, persons_daysim)

        assert len(result) == 1
        assert result["hhno"][0] == 1
        assert result["hhsize"][0] == 1
        assert result["hhvehs"][0] == 1
        assert result["hhftw"][0] == 1  # One full-time worker
        assert result["hhtaz"][0] == 100
        assert "hhincome" in result.columns

    def test_format_households_multi_person_composition(self):
        """Test household composition with multiple person types."""
        households = pl.DataFrame(
            [
                create_household(
                    hh_id=1,
                    home_taz=100,
                    home_maz=1000,
                    num_people=4,
                    num_vehicles=2,
                    residence_rent_own=ResidenceRentOwn.OWN,
                    residence_type=ResidenceType.SFH,
                )
            ]
        )

        persons_daysim = pl.DataFrame(
            [
                {"hhno": 1, "pno": 1, "pptyp": DaysimPersonType.FULL_TIME_WORKER.value, "pwtyp": 1},
                {"hhno": 1, "pno": 2, "pptyp": DaysimPersonType.PART_TIME_WORKER.value, "pwtyp": 2},
                {
                    "hhno": 1,
                    "pno": 3,
                    "pptyp": DaysimPersonType.CHILD_DRIVING_AGE.value,
                    "pwtyp": 0,
                },
                {
                    "hhno": 1,
                    "pno": 4,
                    "pptyp": DaysimPersonType.CHILD_NON_DRIVING_AGE.value,
                    "pwtyp": 0,
                },
            ]
        )

        result = format_households(households, persons_daysim)

        assert result["hhsize"][0] == 4
        assert result["hhftw"][0] == 1  # One full-time worker
        assert result["hhptw"][0] == 1  # One part-time worker
        assert result["hhhsc"][0] == 1  # One high school student
        assert result["hh515"][0] == 1  # One child 5-15

    @pytest.mark.parametrize(
        ("income_kwargs", "expected_income"),
        [
            # Midpoint of $50,000-$74,999.
            pytest.param({"income_bin": IncomeBroad.INCOME_50TO75}, 62000, id="from_bin_midpoint"),
            # A pre-computed income passes through instead of the bin midpoint.
            pytest.param(
                {"income": 99999, "income_bin": IncomeBroad.INCOME_50TO75},
                99999,
                id="income_passthrough",
            ),
        ],
    )
    def test_format_households_income(self, income_kwargs, expected_income):
        """Hhincome is the bin midpoint unless a computed income is already there."""
        households = pl.DataFrame(
            [
                create_household(
                    hh_id=1,
                    home_maz=1000,
                    residence_rent_own=ResidenceRentOwn.OWN,
                    residence_type=ResidenceType.SFH,
                    **income_kwargs,
                )
            ]
        )

        persons_daysim = pl.DataFrame([{"hhno": 1, "pno": 1, "pptyp": 1, "pwtyp": 1}])

        result = format_households(households, persons_daysim)

        assert result["hhincome"][0] == expected_income


def _format_one_trip(**trip_kwargs) -> pl.DataFrame:
    """Link and format a single home-to-work trip built from ``trip_kwargs``."""
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
                **trip_kwargs,
            )
        ]
    )

    linked = link_trips(
        unlinked_trips=unlinked_trips,
        change_mode_enum=PurposeCategory.CHANGE_MODE.value,
        transit_mode_enums=[Mode.BART.value, Mode.BUS_LOCAL.value],
        split_on_occupancy=False,
    )
    with_zones = add_test_taz_maz_ids(
        unlinked_trips=linked["unlinked_trips"],
        linked_trips=linked["linked_trips"],
        tours=None,
        persons=persons,
        households=None,
    )

    return format_linked_trips(
        with_zones["persons"], with_zones["unlinked_trips"], with_zones["linked_trips"]
    )


class TestTripFormatting:
    """Tests for trip mode aggregation and formatting."""

    @pytest.mark.parametrize(
        ("trip_kwargs", "expected_mode", "expected_dorp"),
        [
            pytest.param(
                {
                    "mode_1": Mode.HOUSEHOLD_VEHICLE_1,
                    "mode_type": ModeType.CAR,
                    "driver": Driver.DRIVER,
                    "num_travelers": 1,
                    "change_mode": False,
                },
                DaysimMode.SOV,
                DaysimDriverPassenger.DRIVER,
                id="sov",
            ),
            pytest.param(
                {
                    "mode_1": Mode.HOUSEHOLD_VEHICLE_1,
                    "mode_type": ModeType.CAR,
                    "driver": Driver.DRIVER,
                    "num_travelers": 2,
                },
                DaysimMode.HOV2,
                DaysimDriverPassenger.DRIVER,
                id="hov2",
            ),
            pytest.param(
                {
                    "mode_1": Mode.HOUSEHOLD_VEHICLE_1,
                    "mode_type": ModeType.CAR,
                    "driver": Driver.DRIVER,
                    "num_travelers": 4,
                },
                DaysimMode.HOV3,
                DaysimDriverPassenger.DRIVER,
                id="hov3",
            ),
            pytest.param(
                {
                    "mode_1": Mode.WALK,
                    "mode_type": ModeType.WALK,
                    "driver": Driver.MISSING,
                    "num_travelers": 1,
                },
                DaysimMode.WALK,
                DaysimDriverPassenger.NA,
                id="walk",
            ),
            pytest.param(
                {
                    "mode_1": Mode.BIKE,
                    "mode_type": ModeType.BIKE,
                    "driver": Driver.MISSING,
                    "num_travelers": 1,
                },
                DaysimMode.BIKE,
                DaysimDriverPassenger.NA,
                id="bike",
            ),
        ],
    )
    def test_format_linked_trips_mode_and_dorp(self, trip_kwargs, expected_mode, expected_dorp):
        """Occupancy decides SOV/HOV2/HOV3; a non-vehicle trip has no driver role."""
        result = _format_one_trip(**trip_kwargs)

        assert len(result) == 1
        assert result["mode"][0] == expected_mode.value
        assert result["dorp"][0] == expected_dorp.value

    def test_format_linked_trips_purpose_mapping(self):
        """Test purpose code mapping."""
        result = _format_one_trip()

        assert result["opurp"][0] == DaysimPurpose.HOME.value
        assert result["dpurp"][0] == DaysimPurpose.WORK.value

    def test_format_linked_trips_time_conversion(self):
        """Test time conversion to minutes after midnight."""
        result = _format_one_trip(
            depart_time=datetime(2023, 10, 15, 8, 30),  # 8:30 AM
            arrive_time=datetime(2023, 10, 15, 9, 15),  # 9:15 AM
        )

        assert result["deptm"][0] == 8 * 60 + 30  # 510 minutes
        assert result["arrtm"][0] == 9 * 60 + 15  # 555 minutes


class TestTourFormatting:
    """Tests for tour formatting."""

    def test_format_tours_purpose_and_times(self):
        """A work tour keeps its identity, purpose and clock times.

        Times are minutes after midnight: the fixture leaves home at 08:00,
        reaches work at 09:00, leaves at 17:00 and is home again at 18:00.
        """
        data = create_simple_work_tour_processed()

        result = format_tours(
            data["persons"], data["person_days"], data["linked_trips"], data["tours"]
        )

        assert len(result) == 1
        assert result["hhno"][0] == 1
        assert result["pno"][0] == 1
        assert result["pdpurp"][0] == DaysimPurpose.WORK.value
        assert result["tlvorig"][0] == 8 * 60
        assert result["tardest"][0] == 9 * 60
        assert result["tlvdest"][0] == 17 * 60
        assert result["tarorig"][0] == 18 * 60


def _work_related_day(*, worker: bool) -> dict[str, pl.DataFrame]:
    """The simple work tour with its workplace stop coded work-related.

    With ``worker=False`` the person is a retiree, so the same stop is someone
    volunteering rather than working.
    """
    data = create_simple_work_tour_processed()
    work, work_related = PurposeCategory.WORK.value, PurposeCategory.WORK_RELATED.value

    def recode(col: str) -> pl.Expr:
        return (
            pl.when(pl.col(col) == work)
            .then(pl.lit(work_related))
            .otherwise(pl.col(col))
            .cast(pl.Int64)
            .alias(col)
        )

    data["tours"] = data["tours"].with_columns(recode("tour_purpose"))
    data["linked_trips"] = data["linked_trips"].with_columns(
        recode("o_purpose_category"), recode("d_purpose_category")
    )
    if not worker:
        data["persons"] = data["persons"].with_columns(
            pl.lit(AgeCategory.AGE_65_TO_74.value).cast(pl.Int64).alias("age"),
            pl.lit(Employment.UNEMPLOYED_NOT_LOOKING.value).cast(pl.Int64).alias("employment"),
        )
    return data


def _outbound_dpurp(data: dict[str, pl.DataFrame]) -> int:
    """DaySim destination purpose of the trip that reaches the workplace stop."""
    trips = format_linked_trips(data["persons"], data["unlinked_trips"], data["linked_trips"])
    return trips.filter(pl.col("dpurp") != DaysimPurpose.HOME.value)["dpurp"][0]


class TestWorkRelatedCountsAsWork:
    """A worker's work-related tour is a DaySim work tour; a non-worker's is not.

    DaySim, like CT-RAMP, does not separate the workplace from a meeting, jobsite
    or volunteering, so for a worker both are work. A non-worker's work-related
    stop stays personal business: DaySim's work models are for workers, and a
    child's is a parent's purpose copied onto a shared trip.
    """

    def test_a_workers_work_related_tour_is_work(self):
        """The tour, its trip to the stop, and the day's usual-work count all say work."""
        data = _work_related_day(worker=True)

        tours = format_tours(
            data["persons"], data["person_days"], data["linked_trips"], data["tours"]
        )
        person_days = format_days(data["persons"], data["person_days"], data["tours"])

        assert tours["pdpurp"][0] == DaysimPurpose.WORK.value
        assert _outbound_dpurp(data) == DaysimPurpose.WORK.value
        assert person_days["uwtours"][0] == 1

    def test_an_employed_students_work_related_tour_is_work(self):
        """Employment decides, not person type: a student with a job works too."""
        data = _work_related_day(worker=True)
        data["persons"] = data["persons"].with_columns(
            pl.lit(AgeCategory.AGE_18_TO_24.value).cast(pl.Int64).alias("age"),
            pl.lit(Employment.EMPLOYED_PARTTIME.value).cast(pl.Int64).alias("employment"),
            pl.lit(Student.FULLTIME_INPERSON.value).cast(pl.Int64).alias("student"),
            pl.lit(SchoolType.COLLEGE_4YEAR.value).cast(pl.Int64).alias("school_type"),
        )

        tours = format_tours(
            data["persons"], data["person_days"], data["linked_trips"], data["tours"]
        )

        assert tours["pdpurp"][0] == DaysimPurpose.WORK.value

    def test_a_non_workers_work_related_tour_is_personal_business(self):
        """A retiree's volunteering stays personal business, and is no usual-work tour."""
        data = _work_related_day(worker=False)

        tours = format_tours(
            data["persons"], data["person_days"], data["linked_trips"], data["tours"]
        )
        person_days = format_days(data["persons"], data["person_days"], data["tours"])

        assert tours["pdpurp"][0] == DaysimPurpose.PERSONAL_BUSINESS.value
        assert _outbound_dpurp(data) == DaysimPurpose.PERSONAL_BUSINESS.value
        assert person_days["uwtours"][0] == 0


def _run_daysim(data: dict[str, pl.DataFrame]) -> dict[str, pl.DataFrame]:
    """Run format_daysim over a processed scenario with reported TAZ and MAZ locations."""
    return format_daysim(
        **step_tables(
            "format_daysim",
            {**data, **reported_locations(data["households"], data["persons"], ["taz", "maz"])},
        ),
        usability_profile="test",
    )


class TestEndToEndDaysimFormatting:
    """End-to-end integration tests for DaySim formatting."""

    def test_format_daysim_simple_work_tour(self):
        """One household, one person, one work tour of two trips, with the DaySim columns."""
        result = _run_daysim(create_simple_work_tour_processed())

        # table: (rows, DaySim columns it must carry)
        expected = {
            "households_daysim": (
                1,
                ["hhno", "hhsize", "hhvehs", "hhftw", "hhptw", "hhtaz", "hhincome"],
            ),
            "persons_daysim": (1, ["hhno", "pno", "pptyp", "pagey", "pgend", "pwtyp"]),
            "linked_trips_daysim": (
                2,
                ["hhno", "pno", "day", "tripno", "mode", "pathtype", "dorp", "opurp", "dpurp"],
            ),
            "tours_daysim": (1, ["hhno", "pno", "day", "tour", "pdpurp"]),
        }
        for table, (rows, columns) in expected.items():
            assert len(result[table]) == rows, table
            assert set(columns) <= set(result[table].columns), table

    def test_format_daysim_transit_commute(self):
        """Both commute trips are walk-to-transit on the BART path."""
        trips = _run_daysim(create_transit_commute_processed())["linked_trips_daysim"]

        assert len(trips) == 2
        assert all(trips["mode"] == DaysimMode.WALK_TRANSIT.value)
        assert all(trips["pathtype"] == DaysimPathType.BART.value)

    def test_format_daysim_multi_person_household(self):
        """Four persons, two of them full-time workers."""
        result = _run_daysim(create_multi_person_household_processed())

        households = result["households_daysim"]
        assert households["hhsize"][0] == 4
        assert households["hhftw"][0] == 2
        assert households["hhptw"][0] == 0
        assert len(result["persons_daysim"]) == 4
