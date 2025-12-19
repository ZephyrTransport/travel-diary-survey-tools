"""Unit tests for DaySim formatter.

Tests person type classification, household composition, mode aggregation,
tour formatting, and end-to-end transformation from canonical survey data
to DaySim model format.
"""

from datetime import datetime

import polars as pl

from data_canon.codebook.days import TravelDow
from data_canon.codebook.daysim import (
    DaysimDriverPassenger,
    DaysimMode,
    DaysimPathType,
    DaysimPurpose,
)
from data_canon.codebook.households import (
    IncomeDetailed,
    IncomeFollowup,
    ResidenceRentOwn,
    ResidenceType,
)
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    Gender,
    PersonType,
    SchoolType,
    Student,
)
from data_canon.codebook.trips import (
    Driver,
    Mode,
    ModeType,
    PurposeCategory,
)
from processing.formatting.daysim.format_daysim import format_daysim
from processing.formatting.daysim.format_households import format_households
from processing.formatting.daysim.format_persons import (
    compute_day_completeness,
    format_persons,
)
from processing.formatting.daysim.format_tours import format_tours
from processing.formatting.daysim.format_trips import format_linked_trips
from tests.fixtures.daysim_test_data import (
    DaysimScenarioBuilder,
    DaysimTestDataBuilder,
)


class TestDayCompleteness:
    """Tests for day completeness computation."""

    def test_compute_day_completeness_single_person_weekday(self):
        """Test day completeness for single person with one complete weekday."""
        days = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_day(
                    day_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    travel_dow=TravelDow.MONDAY,
                    is_complete=True,
                )
            ]
        )

        result = compute_day_completeness(days)

        assert len(result) == 1
        assert result["hhno"][0] == 1
        assert result["pno"][0] == 1
        assert result["mon_complete"][0] == 1
        assert result["tue_complete"][0] == 0
        assert result["num_days_complete_3dayweekday"][0] == 0  # Tue+Wed+Thu
        assert (
            result["num_days_complete_4dayweekday"][0] == 1
        )  # Mon+Tue+Wed+Thu
        assert result["num_days_complete_5dayweekday"][0] == 1  # Mon-Fri

    def test_compute_day_completeness_full_week(self):
        """Test day completeness for person with complete week."""
        days = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_day(
                    day_id=i,
                    person_id=201,
                    hh_id=2,
                    person_num=1,
                    day_num=i,
                    travel_dow=TravelDow(i),
                    is_complete=True,
                )
                for i in range(1, 8)
            ]
        )

        result = compute_day_completeness(days)

        assert len(result) == 1
        assert result["hhno"][0] == 2
        assert result["pno"][0] == 1
        assert result["mon_complete"][0] == 1
        assert result["sun_complete"][0] == 1
        assert result["num_days_complete_3dayweekday"][0] == 3
        assert result["num_days_complete_4dayweekday"][0] == 4
        assert result["num_days_complete_5dayweekday"][0] == 5

    def test_compute_day_completeness_incomplete_days(self):
        """Test day completeness with some incomplete days."""
        days = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_day(
                    day_id=1,
                    person_id=301,
                    hh_id=3,
                    person_num=1,
                    day_num=1,
                    travel_dow=TravelDow.TUESDAY,
                    is_complete=True,
                ),
                DaysimTestDataBuilder.create_day(
                    day_id=2,
                    person_id=301,
                    hh_id=3,
                    person_num=1,
                    day_num=2,
                    travel_dow=TravelDow.WEDNESDAY,
                    is_complete=False,
                ),
                DaysimTestDataBuilder.create_day(
                    day_id=3,
                    person_id=301,
                    hh_id=3,
                    person_num=1,
                    day_num=3,
                    travel_dow=TravelDow.THURSDAY,
                    is_complete=True,
                ),
            ]
        )

        result = compute_day_completeness(days)

        assert len(result) == 1
        assert result["tue_complete"][0] == 1
        assert result["wed_complete"][0] == 0
        assert result["thu_complete"][0] == 1
        assert result["num_days_complete_3dayweekday"][0] == 2  # Tue+Thu only

    def test_compute_day_completeness_multiple_persons(self):
        """Test day completeness with multiple persons."""
        days = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_day(
                    day_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    travel_dow=TravelDow.MONDAY,
                    is_complete=True,
                ),
                DaysimTestDataBuilder.create_day(
                    day_id=2,
                    person_id=102,
                    hh_id=1,
                    person_num=2,
                    travel_dow=TravelDow.MONDAY,
                    is_complete=False,
                ),
            ]
        )

        result = compute_day_completeness(days)

        assert len(result) == 2
        assert result.filter(pl.col("pno") == 1)["mon_complete"][0] == 1
        assert result.filter(pl.col("pno") == 2)["mon_complete"][0] == 0


class TestPersonFormatting:
    """Tests for person formatting and type classification."""

    def test_format_persons_full_time_worker(self):
        """Test person formatting for full-time worker."""
        persons = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    person_type=PersonType.FULL_TIME_WORKER,
                    employment=Employment.EMPLOYED_FULLTIME,
                    age=AgeCategory.AGE_35_TO_44,
                    age_years=40,
                    work_mode=Mode.HOUSEHOLD_VEHICLE_1,
                    work_taz=200,
                    work_maz=2000,
                    is_proxy=False,
                    num_days_complete=1,
                )
            ]
        )

        days = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_day(
                    day_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    travel_dow=TravelDow.MONDAY,
                    is_complete=True,
                )
            ]
        )

        result = format_persons(persons, days)

        assert len(result) == 1
        assert result["hhno"][0] == 1
        assert result["pno"][0] == 1
        assert result["pptyp"][0] == PersonType.FULL_TIME_WORKER.value
        assert result["pwtyp"][0] == 1  # Full-time worker
        assert result["pagey"][0] == 40  # Age in years
        assert result["pwtaz"][0] == 200
        assert result["pwpcl"][0] == 2000

    def test_format_persons_part_time_worker(self):
        """Test person formatting for part-time worker."""
        persons = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    person_type=PersonType.PART_TIME_WORKER,
                    employment=Employment.EMPLOYED_PARTTIME,
                    age=AgeCategory.AGE_25_TO_34,
                    age_years=30,
                    work_taz=200,
                    work_maz=2000,
                    work_mode=Mode.HOUSEHOLD_VEHICLE_1,
                    is_proxy=False,
                    num_days_complete=1,
                )
            ]
        )

        days = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_day(
                    day_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    travel_dow=TravelDow.TUESDAY,
                    is_complete=True,
                )
            ]
        )

        result = format_persons(persons, days)

        assert result["pptyp"][0] == PersonType.PART_TIME_WORKER.value
        assert result["pwtyp"][0] == 2  # Part-time worker

    def test_format_persons_university_student(self):
        """Test person formatting for university student."""
        persons = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    person_type=PersonType.UNIVERSITY_STUDENT,
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    student=Student.FULLTIME_INPERSON,
                    age=AgeCategory.AGE_18_TO_24,
                    age_years=21,
                    work_lat=None,
                    work_lon=None,
                    work_taz=None,
                    work_maz=None,
                    school_taz=300,
                    school_maz=3000,
                    school_type=SchoolType.COLLEGE_4YEAR,
                    work_mode=Mode.MISSING,
                    is_proxy=False,
                    num_days_complete=1,
                )
            ]
        )

        days = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_day(
                    day_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    travel_dow=TravelDow.WEDNESDAY,
                    is_complete=True,
                )
            ]
        )

        result = format_persons(persons, days)

        assert result["pptyp"][0] == PersonType.UNIVERSITY_STUDENT.value
        assert result["pwtaz"][0] == -1  # No work location
        assert result["pstaz"][0] == 300  # School location
        assert result["pspcl"][0] == 3000

    def test_format_persons_high_school_student(self):
        """Test person formatting for high school student."""
        persons = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    person_type=PersonType.HIGH_SCHOOL_STUDENT,
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    student=Student.FULLTIME_INPERSON,
                    age=AgeCategory.AGE_16_TO_17,
                    age_years=16,
                    work_lat=None,
                    work_lon=None,
                    work_taz=None,
                    work_maz=None,
                    school_taz=150,
                    school_maz=1500,
                    school_type=SchoolType.HIGH_SCHOOL,
                    work_mode=Mode.MISSING,
                    is_proxy=False,
                    num_days_complete=1,
                )
            ]
        )

        days = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_day(
                    day_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    travel_dow=TravelDow.THURSDAY,
                    is_complete=True,
                )
            ]
        )

        result = format_persons(persons, days)

        assert result["pptyp"][0] == PersonType.HIGH_SCHOOL_STUDENT.value
        assert result["pstaz"][0] == 150
        assert result["pspcl"][0] == 1500

    def test_format_persons_retiree(self):
        """Test person formatting for retiree."""
        persons = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    person_type=PersonType.RETIRED,
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    student=Student.NONSTUDENT,
                    age=AgeCategory.AGE_65_TO_74,
                    age_years=70,
                    work_lat=None,
                    work_lon=None,
                    work_taz=None,
                    work_maz=None,
                    work_mode=Mode.MISSING,
                    is_proxy=False,
                    num_days_complete=1,
                )
            ]
        )

        days = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_day(
                    day_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    travel_dow=TravelDow.FRIDAY,
                    is_complete=True,
                )
            ]
        )

        result = format_persons(persons, days)

        assert result["pptyp"][0] == PersonType.RETIRED.value
        assert result["pwtaz"][0] == -1

    def test_format_persons_non_working_adult(self):
        """Test person formatting for non-working adult."""
        persons = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    person_type=PersonType.NON_WORKER,
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    student=Student.NONSTUDENT,
                    age=AgeCategory.AGE_25_TO_34,
                    age_years=30,
                    work_lat=None,
                    work_lon=None,
                    work_taz=None,
                    work_maz=None,
                )
            ]
        )

        days = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_day(
                    day_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    travel_dow=TravelDow.SATURDAY,
                    is_complete=True,
                )
            ]
        )

        result = format_persons(persons, days)

        assert result["pptyp"][0] == PersonType.NON_WORKER.value

    def test_format_persons_child_5_15(self):
        """Test person formatting for child aged 5-15."""
        persons = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    person_type=PersonType.CHILD_5_15,
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    student=Student.NONSTUDENT,
                    age=AgeCategory.AGE_5_TO_15,
                    age_years=10,
                    work_lat=None,
                    work_lon=None,
                    work_taz=None,
                    work_maz=None,
                )
            ]
        )

        days = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_day(
                    day_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    travel_dow=TravelDow.SUNDAY,
                    is_complete=True,
                )
            ]
        )

        result = format_persons(persons, days)

        assert result["pptyp"][0] == PersonType.CHILD_5_15.value
        assert result["pagey"][0] == 10

    def test_format_persons_child_under_5(self):
        """Test person formatting for child under 5."""
        persons = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    person_type=PersonType.CHILD_UNDER_5,
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    student=Student.NONSTUDENT,
                    age=AgeCategory.AGE_UNDER_5,
                    age_years=3,
                    work_lat=None,
                    work_lon=None,
                    work_taz=None,
                    work_maz=None,
                )
            ]
        )

        days = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_day(
                    day_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    travel_dow=TravelDow.MONDAY,
                    is_complete=True,
                )
            ]
        )

        result = format_persons(persons, days)

        assert result["pptyp"][0] == PersonType.CHILD_UNDER_5.value
        assert result["pagey"][0] == 3

    def test_format_persons_with_day_completeness(self):
        """Test person formatting with day completeness indicators."""
        persons = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    person_type=PersonType.FULL_TIME_WORKER,
                )
            ]
        )

        days = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_day(
                    day_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    travel_dow=TravelDow.MONDAY,
                    is_complete=True,
                ),
                DaysimTestDataBuilder.create_day(
                    day_id=2,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=2,
                    travel_dow=TravelDow.TUESDAY,
                    is_complete=True,
                ),
                DaysimTestDataBuilder.create_day(
                    day_id=3,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=3,
                    travel_dow=TravelDow.WEDNESDAY,
                    is_complete=True,
                ),
                DaysimTestDataBuilder.create_day(
                    day_id=4,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=4,
                    travel_dow=TravelDow.THURSDAY,
                    is_complete=False,
                ),
                DaysimTestDataBuilder.create_day(
                    day_id=5,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=5,
                    travel_dow=TravelDow.FRIDAY,
                    is_complete=False,
                ),
            ]
        )

        result = format_persons(persons, days)

        assert "mon_complete" in result.columns
        assert result["mon_complete"][0] == 1
        assert result["tue_complete"][0] == 1
        assert result["wed_complete"][0] == 1
        # Because Thu is incomplete, only 2 complete days in Tue-Wed-Thu
        assert result["num_days_complete_3dayweekday"][0] == 2
        # Because Thu and Fri are incomplete, only 3 complete days in Mon-Thu
        assert result["num_days_complete_4dayweekday"][0] == 3
        # Because Thu and Fri are incomplete, only 3 complete days in Mon-Fri
        assert result["num_days_complete_5dayweekday"][0] == 3

    def test_format_persons_gender_mapping(self):
        """Test gender code mapping."""
        persons = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    person_type=PersonType.FULL_TIME_WORKER,
                    gender=Gender.MALE,
                ),
                DaysimTestDataBuilder.create_person(
                    person_id=102,
                    hh_id=1,
                    person_num=2,
                    person_type=PersonType.FULL_TIME_WORKER,
                    gender=Gender.FEMALE,
                ),
            ]
        )

        days = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_day(
                    day_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    travel_dow=TravelDow.MONDAY,
                    is_complete=True,
                ),
                DaysimTestDataBuilder.create_day(
                    day_id=2,
                    person_id=102,
                    hh_id=1,
                    person_num=2,
                    day_num=1,
                    travel_dow=TravelDow.MONDAY,
                    is_complete=True,
                ),
            ]
        )

        result = format_persons(persons, days)

        assert result.filter(pl.col("pno") == 1)["pgend"][0] == 1  # Male
        assert result.filter(pl.col("pno") == 2)["pgend"][0] == 2  # Female


class TestHouseholdFormatting:
    """Tests for household formatting and composition."""

    def test_format_households_single_person(self):
        """Test household formatting with single person."""
        households = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_household(
                    hh_id=1,
                    home_taz=100,
                    home_maz=1000,
                    vehicles=1,
                    income_detailed=IncomeDetailed.INCOME_75TO100,
                    residence_rent_own=ResidenceRentOwn.OWN,
                    residence_type=ResidenceType.SFH,
                    num_workers=1,
                )
            ]
        )

        # Create persons table, then format to get persons_daysim
        persons = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    person_type=PersonType.FULL_TIME_WORKER,
                )
            ]
        )

        days = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_day(
                    day_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    travel_dow=TravelDow.MONDAY,
                    is_complete=True,
                )
            ]
        )

        persons_daysim = format_persons(persons, days)

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
                DaysimTestDataBuilder.create_household(
                    hh_id=1, home_taz=100, home_maz=1000, size=4, vehicles=2
                )
            ]
        )

        persons_daysim = pl.DataFrame(
            [
                {
                    "hhno": 1,
                    "pno": 1,
                    "pptyp": PersonType.FULL_TIME_WORKER.value,
                    "pwtyp": 1,
                },
                {
                    "hhno": 1,
                    "pno": 2,
                    "pptyp": PersonType.PART_TIME_WORKER.value,
                    "pwtyp": 2,
                },
                {
                    "hhno": 1,
                    "pno": 3,
                    "pptyp": PersonType.HIGH_SCHOOL_STUDENT.value,
                    "pwtyp": 0,
                },
                {
                    "hhno": 1,
                    "pno": 4,
                    "pptyp": PersonType.CHILD_5_15.value,
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

    def test_format_households_income_detailed(self):
        """Test income mapping from detailed income field."""
        households = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_household(
                    hh_id=1,
                    income_detailed=IncomeDetailed.INCOME_50TO75,
                    income_followup=None,
                )
            ]
        )

        persons_daysim = pl.DataFrame(
            [{"hhno": 1, "pno": 1, "pptyp": 1, "pwtyp": 1}]
        )

        result = format_households(households, persons_daysim)

        # Should use income_detailed midpoint (approximately 62500)
        assert result["hhincome"][0] > 50000
        assert result["hhincome"][0] < 75000

    def test_format_households_income_followup(self):
        """Test income mapping from followup income field."""
        households = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_household(
                    hh_id=1,
                    income_detailed=None,
                    income_followup=IncomeFollowup.INCOME_100TO200,
                )
            ]
        )

        persons_daysim = pl.DataFrame(
            [{"hhno": 1, "pno": 1, "pptyp": 1, "pwtyp": 1}]
        )

        result = format_households(households, persons_daysim)

        # Should use income_followup midpoint (150000 for INCOME_100TO200)
        assert result["hhincome"][0] == 150000

    def test_format_households_multiple_households(self):
        """Test formatting multiple households."""
        households = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_household(hh_id=1, home_taz=100),
                DaysimTestDataBuilder.create_household(hh_id=2, home_taz=200),
            ]
        )

        persons_daysim = pl.DataFrame(
            [
                {"hhno": 1, "pno": 1, "pptyp": 1, "pwtyp": 1},
                {"hhno": 2, "pno": 1, "pptyp": 3, "pwtyp": 0},
            ]
        )

        result = format_households(households, persons_daysim)

        assert len(result) == 2
        assert result.filter(pl.col("hhno") == 1)["hhtaz"][0] == 100
        assert result.filter(pl.col("hhno") == 2)["hhtaz"][0] == 200


class TestTripFormatting:
    """Tests for trip mode aggregation and formatting."""

    def test_format_linked_trips_sov(self):
        """Test trip formatting for drive alone (SOV)."""
        persons = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_person(
                    person_id=101, hh_id=1, person_num=1
                )
            ]
        )

        unlinked_trips = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_unlinked_trip(
                    trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    mode=Mode.HOUSEHOLD_VEHICLE_1,
                    mode_type=ModeType.CAR,
                    driver=Driver.DRIVER,
                    num_travelers=1,
                )
            ]
        )

        linked_trips = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_linked_trip(
                    linked_trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    mode=Mode.HOUSEHOLD_VEHICLE_1,
                    mode_type=ModeType.CAR,
                    driver=Driver.DRIVER,
                    num_travelers=1,
                )
            ]
        )

        result = format_linked_trips(persons, unlinked_trips, linked_trips)

        assert len(result) == 1
        assert result["mode"][0] == DaysimMode.SOV.value
        assert result["dorp"][0] == DaysimDriverPassenger.DRIVER.value

    def test_format_linked_trips_hov2(self):
        """Test trip formatting for HOV2."""
        persons = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_person(
                    person_id=101, hh_id=1, person_num=1
                )
            ]
        )

        unlinked_trips = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_unlinked_trip(
                    trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    mode=Mode.HOUSEHOLD_VEHICLE_1,
                    mode_type=ModeType.CAR,
                    driver=Driver.DRIVER,
                    num_travelers=2,
                )
            ]
        )

        linked_trips = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_linked_trip(
                    linked_trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    mode=Mode.HOUSEHOLD_VEHICLE_1,
                    mode_type=ModeType.CAR,
                    driver=Driver.DRIVER,
                    num_travelers=2,
                )
            ]
        )

        result = format_linked_trips(persons, unlinked_trips, linked_trips)

        assert result["mode"][0] == DaysimMode.HOV2.value

    def test_format_linked_trips_hov3(self):
        """Test trip formatting for HOV3+."""
        persons = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_person(
                    person_id=101, hh_id=1, person_num=1
                )
            ]
        )

        unlinked_trips = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_unlinked_trip(
                    trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    mode=Mode.HOUSEHOLD_VEHICLE_1,
                    mode_type=ModeType.CAR,
                    driver=Driver.DRIVER,
                    num_travelers=4,
                )
            ]
        )

        linked_trips = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_linked_trip(
                    linked_trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    mode=Mode.HOUSEHOLD_VEHICLE_1,
                    mode_type=ModeType.CAR,
                    driver=Driver.DRIVER,
                    num_travelers=4,
                )
            ]
        )

        result = format_linked_trips(persons, unlinked_trips, linked_trips)

        assert result["mode"][0] == DaysimMode.HOV3.value

    def test_format_linked_trips_walk(self):
        """Test trip formatting for walk."""
        persons = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_person(
                    person_id=101, hh_id=1, person_num=1
                )
            ]
        )

        unlinked_trips = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_unlinked_trip(
                    trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    mode=Mode.WALK,
                    mode_type=ModeType.WALK,
                    driver=Driver.MISSING,
                    num_travelers=1,
                )
            ]
        )

        linked_trips = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_linked_trip(
                    linked_trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    mode=Mode.WALK,
                    mode_type=ModeType.WALK,
                    driver=Driver.MISSING,
                    num_travelers=1,
                )
            ]
        )

        result = format_linked_trips(persons, unlinked_trips, linked_trips)

        assert result["mode"][0] == DaysimMode.WALK.value
        assert result["dorp"][0] == DaysimDriverPassenger.NA.value

    def test_format_linked_trips_bike(self):
        """Test trip formatting for bike."""
        persons = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_person(
                    person_id=101, hh_id=1, person_num=1
                )
            ]
        )

        unlinked_trips = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_unlinked_trip(
                    trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    mode=Mode.BIKE,
                    mode_type=ModeType.BIKE,
                    driver=Driver.MISSING,
                    num_travelers=1,
                )
            ]
        )

        linked_trips = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_linked_trip(
                    linked_trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    mode=Mode.BIKE,
                    mode_type=ModeType.BIKE,
                    driver=Driver.MISSING,
                    num_travelers=1,
                )
            ]
        )

        result = format_linked_trips(persons, unlinked_trips, linked_trips)

        assert result["mode"][0] == DaysimMode.BIKE.value

    def test_format_linked_trips_purpose_mapping(self):
        """Test purpose code mapping."""
        persons = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_person(
                    person_id=101, hh_id=1, person_num=1
                )
            ]
        )

        unlinked_trips = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_unlinked_trip(
                    trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    origin_purpose=PurposeCategory.HOME,
                    dest_purpose=PurposeCategory.WORK,
                )
            ]
        )

        linked_trips = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_linked_trip(
                    linked_trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    origin_purpose=PurposeCategory.HOME,
                    dest_purpose=PurposeCategory.WORK,
                )
            ]
        )

        result = format_linked_trips(persons, unlinked_trips, linked_trips)

        assert result["opurp"][0] == DaysimPurpose.HOME.value
        assert result["dpurp"][0] == DaysimPurpose.WORK.value

    def test_format_linked_trips_time_conversion(self):
        """Test time conversion to minutes after midnight."""
        persons = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_person(
                    person_id=101, hh_id=1, person_num=1
                )
            ]
        )

        unlinked_trips = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_unlinked_trip(
                    trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    depart_time=datetime(2023, 10, 15, 8, 30),  # 8:30 AM
                    arrive_time=datetime(2023, 10, 15, 9, 15),  # 9:15 AM
                )
            ]
        )

        linked_trips = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_linked_trip(
                    linked_trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    depart_time=datetime(2023, 10, 15, 8, 30),
                    arrive_time=datetime(2023, 10, 15, 9, 15),
                )
            ]
        )

        result = format_linked_trips(persons, unlinked_trips, linked_trips)

        assert result["deptm"][0] == 8 * 60 + 30  # 510 minutes
        assert result["arrtm"][0] == 9 * 60 + 15  # 555 minutes


class TestTourFormatting:
    """Tests for tour formatting."""

    def test_format_tours_basic(self):
        """Test basic tour formatting."""
        persons = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_person(
                    person_id=101, hh_id=1, person_num=1
                )
            ]
        )

        days = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_day(
                    day_id=1, person_id=101, hh_id=1, person_num=1, day_num=1
                )
            ]
        )

        linked_trips = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_linked_trip(
                    linked_trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    tour_id=1,
                    mode=Mode.HOUSEHOLD_VEHICLE_1,
                    distance_miles=10.0,
                )
            ]
        )

        tours = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_tour(
                    tour_id=1,
                    parent_tour_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    tour_num=1,
                    tour_purpose=PurposeCategory.WORK,
                )
            ]
        )

        result = format_tours(persons, days, linked_trips, tours)

        assert len(result) == 1
        assert result["hhno"][0] == 1
        assert result["pno"][0] == 1
        assert result["day"][0] == 1
        assert result["tour"][0] == 1

    def test_format_tours_purpose_mapping(self):
        """Test tour purpose mapping."""
        persons = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_person(
                    person_id=101, hh_id=1, person_num=1
                )
            ]
        )

        days = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_day(
                    day_id=1, person_id=101, hh_id=1, person_num=1, day_num=1
                )
            ]
        )

        linked_trips = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_linked_trip(
                    linked_trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    tour_id=1,
                )
            ]
        )

        tours = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_tour(
                    tour_id=1,
                    parent_tour_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    tour_purpose=PurposeCategory.WORK,
                )
            ]
        )

        result = format_tours(persons, days, linked_trips, tours)

        assert result["pdpurp"][0] == DaysimPurpose.WORK.value

    def test_format_tours_time_conversion(self):
        """Test tour time conversion to minutes after midnight."""
        persons = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_person(
                    person_id=101, hh_id=1, person_num=1
                )
            ]
        )

        days = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_day(
                    day_id=1, person_id=101, hh_id=1, person_num=1, day_num=1
                )
            ]
        )

        linked_trips = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_linked_trip(
                    linked_trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    tour_id=1,
                )
            ]
        )

        tours = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_tour(
                    tour_id=1,
                    parent_tour_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    depart_home_time=datetime(2023, 10, 15, 8, 0),
                    arrive_dest_time=datetime(2023, 10, 15, 8, 30),
                )
            ]
        )

        result = format_tours(persons, days, linked_trips, tours)

        assert result["tlvorig"][0] == 8 * 60  # 480 minutes
        assert result["tardest"][0] == 8 * 60 + 30  # 510 minutes


class TestEndToEndDaysimFormatting:
    """End-to-end integration tests for DaySim formatting."""

    def test_format_daysim_simple_work_tour(self):
        """Test end-to-end formatting with simple work tour scenario."""
        households, persons, days, linked_trips, tours = (
            DaysimScenarioBuilder.simple_work_tour()
        )

        # Need unlinked trips for mode aggregation
        unlinked_trips = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_unlinked_trip(
                    trip_id=1,
                    person_id=1,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    trip_num=1,
                    origin_purpose=PurposeCategory.HOME,
                    dest_purpose=PurposeCategory.WORK,
                    mode=Mode.HOUSEHOLD_VEHICLE_1,
                    driver=Driver.DRIVER,
                    num_travelers=1,
                ),
                DaysimTestDataBuilder.create_unlinked_trip(
                    trip_id=2,
                    person_id=1,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    trip_num=2,
                    origin_purpose=PurposeCategory.WORK,
                    dest_purpose=PurposeCategory.HOME,
                    mode=Mode.HOUSEHOLD_VEHICLE_1,
                    driver=Driver.DRIVER,
                    num_travelers=1,
                ),
            ]
        )

        result = format_daysim(
            persons, households, unlinked_trips, linked_trips, tours, days
        )

        # Verify all expected keys present
        assert "households_daysim" in result
        assert "persons_daysim" in result
        assert "linked_trips_daysim" in result
        assert "tours_daysim" in result

        # Verify record counts
        assert len(result["households_daysim"]) == 1
        assert len(result["persons_daysim"]) == 1
        assert len(result["linked_trips_daysim"]) == 2
        assert len(result["tours_daysim"]) == 1

    def test_format_daysim_transit_commute(self):
        """Test end-to-end formatting with transit commute scenario."""
        households, persons, days, linked_trips, tours = (
            DaysimScenarioBuilder.transit_commute()
        )

        # Create unlinked trips representing walk-BART-walk
        unlinked_trips = pl.DataFrame(
            [
                # Morning: Walk to BART station
                DaysimTestDataBuilder.create_unlinked_trip(
                    trip_id=1,
                    linked_trip_id=1,
                    person_id=1,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    trip_num=1,
                    origin_purpose=PurposeCategory.HOME,
                    dest_purpose=PurposeCategory.CHANGE_MODE,
                    mode=Mode.WALK,
                    mode_type=ModeType.WALK,
                ),
                # Morning: BART ride
                DaysimTestDataBuilder.create_unlinked_trip(
                    trip_id=2,
                    linked_trip_id=1,
                    person_id=1,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    trip_num=2,
                    origin_purpose=PurposeCategory.CHANGE_MODE,
                    dest_purpose=PurposeCategory.CHANGE_MODE,
                    mode=Mode.BART,
                    mode_type=ModeType.TRANSIT,
                ),
                # Morning: Walk from BART to work
                DaysimTestDataBuilder.create_unlinked_trip(
                    trip_id=3,
                    linked_trip_id=1,
                    person_id=1,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    trip_num=3,
                    origin_purpose=PurposeCategory.CHANGE_MODE,
                    dest_purpose=PurposeCategory.WORK,
                    mode=Mode.WALK,
                    mode_type=ModeType.WALK,
                ),
                # Evening: Walk to BART
                DaysimTestDataBuilder.create_unlinked_trip(
                    trip_id=4,
                    linked_trip_id=2,
                    person_id=1,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    trip_num=4,
                    origin_purpose=PurposeCategory.WORK,
                    dest_purpose=PurposeCategory.CHANGE_MODE,
                    mode=Mode.WALK,
                    mode_type=ModeType.WALK,
                ),
                # Evening: BART ride
                DaysimTestDataBuilder.create_unlinked_trip(
                    trip_id=5,
                    linked_trip_id=2,
                    person_id=1,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    trip_num=5,
                    origin_purpose=PurposeCategory.CHANGE_MODE,
                    dest_purpose=PurposeCategory.CHANGE_MODE,
                    mode=Mode.BART,
                    mode_type=ModeType.TRANSIT,
                ),
                # Evening: Walk home
                DaysimTestDataBuilder.create_unlinked_trip(
                    trip_id=6,
                    linked_trip_id=2,
                    person_id=1,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    trip_num=6,
                    origin_purpose=PurposeCategory.CHANGE_MODE,
                    dest_purpose=PurposeCategory.HOME,
                    mode=Mode.WALK,
                    mode_type=ModeType.WALK,
                ),
            ]
        )

        result = format_daysim(
            persons, households, unlinked_trips, linked_trips, tours, days
        )

        # Verify transit mode detected
        trips_result = result["linked_trips_daysim"]
        assert len(trips_result) == 2
        # Both trips should be walk-to-transit
        assert all(trips_result["mode"] == DaysimMode.WALK_TRANSIT.value)
        # Path type should be BART
        assert all(trips_result["pathtype"] == DaysimPathType.BART.value)

    def test_format_daysim_multi_person_household(self):
        """Test end-to-end formatting with multi-person household."""
        households, persons, days, linked_trips, tours = (
            DaysimScenarioBuilder.multi_person_household()
        )

        # Create unlinked trips
        unlinked_trips = pl.DataFrame(
            [
                # Person 1 trips
                DaysimTestDataBuilder.create_unlinked_trip(
                    trip_id=1,
                    person_id=1,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    trip_num=1,
                ),
                DaysimTestDataBuilder.create_unlinked_trip(
                    trip_id=2,
                    person_id=1,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    trip_num=2,
                ),
                # Person 2 trips
                DaysimTestDataBuilder.create_unlinked_trip(
                    trip_id=3,
                    person_id=2,
                    hh_id=1,
                    person_num=2,
                    day_num=1,
                    trip_num=1,
                ),
                DaysimTestDataBuilder.create_unlinked_trip(
                    trip_id=4,
                    person_id=2,
                    hh_id=1,
                    person_num=2,
                    day_num=1,
                    trip_num=2,
                ),
            ]
        )

        result = format_daysim(
            persons, households, unlinked_trips, linked_trips, tours, days
        )

        # Verify household composition
        hh_result = result["households_daysim"]
        assert hh_result["hhsize"][0] == 4
        assert hh_result["hhftw"][0] == 1  # One full-time worker
        assert hh_result["hhptw"][0] == 1  # One part-time worker
        assert hh_result["hhhsc"][0] == 1  # One high school student
        assert hh_result["hh515"][0] == 1  # One child 5-15

        # Verify person types
        persons_result = result["persons_daysim"]
        assert len(persons_result) == 4
        assert (
            PersonType.FULL_TIME_WORKER.value
            in persons_result["pptyp"].to_list()
        )
        assert (
            PersonType.PART_TIME_WORKER.value
            in persons_result["pptyp"].to_list()
        )
        assert (
            PersonType.HIGH_SCHOOL_STUDENT.value
            in persons_result["pptyp"].to_list()
        )
        assert PersonType.CHILD_5_15.value in persons_result["pptyp"].to_list()

    def test_format_daysim_filters_null_taz(self):
        """Test that households without TAZ are filtered out."""
        households = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_household(
                    hh_id=1, home_taz=100, home_maz=1000
                ),
                DaysimTestDataBuilder.create_household(
                    hh_id=2, home_taz=None, home_maz=None
                ),
            ]
        )

        persons = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_person(
                    person_id=101, hh_id=1, person_num=1
                ),
                DaysimTestDataBuilder.create_person(
                    person_id=201, hh_id=2, person_num=1
                ),
            ]
        )

        days = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_day(
                    day_id=1, person_id=101, hh_id=1, person_num=1
                ),
                DaysimTestDataBuilder.create_day(
                    day_id=2, person_id=201, hh_id=2, person_num=1
                ),
            ]
        )

        unlinked_trips = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_unlinked_trip(
                    trip_id=1, person_id=101, hh_id=1, person_num=1
                ),
                DaysimTestDataBuilder.create_unlinked_trip(
                    trip_id=2, person_id=201, hh_id=2, person_num=1
                ),
            ]
        )

        linked_trips = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_linked_trip(
                    linked_trip_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    tour_id=1,
                ),
                DaysimTestDataBuilder.create_linked_trip(
                    linked_trip_id=2,
                    person_id=201,
                    hh_id=2,
                    person_num=1,
                    tour_id=2,
                ),
            ]
        )

        tours = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_tour(
                    tour_id=1, person_id=101, hh_id=1, person_num=1
                ),
                DaysimTestDataBuilder.create_tour(
                    tour_id=2, person_id=201, hh_id=2, person_num=1
                ),
            ]
        )

        result = format_daysim(
            persons, households, unlinked_trips, linked_trips, tours, days
        )

        # Only household 1 should remain
        assert len(result["households_daysim"]) == 1
        assert result["households_daysim"]["hhno"][0] == 1

        # Only person from household 1 should remain
        assert len(result["persons_daysim"]) == 1
        assert result["persons_daysim"]["hhno"][0] == 1

        # Only trips/tours from household 1 should remain
        assert len(result["linked_trips_daysim"]) == 1
        assert result["linked_trips_daysim"]["hhno"][0] == 1

        assert len(result["tours_daysim"]) == 1
        assert result["tours_daysim"]["hhno"][0] == 1

    def test_format_daysim_output_schema(self):
        """Test that output DataFrames have expected DaySim columns."""
        households, persons, days, linked_trips, tours = (
            DaysimScenarioBuilder.simple_work_tour()
        )

        unlinked_trips = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_unlinked_trip(
                    trip_id=1, person_id=1, hh_id=1, person_num=1, day_num=1
                ),
                DaysimTestDataBuilder.create_unlinked_trip(
                    trip_id=2, person_id=1, hh_id=1, person_num=1, day_num=1
                ),
            ]
        )

        result = format_daysim(
            persons, households, unlinked_trips, linked_trips, tours, days
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
