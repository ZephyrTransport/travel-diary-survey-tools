"""Comprehensive test data factory for tour extraction tests.

Ensures all test fixtures include ALL fields required by validation,
preventing silent failures due to missing columns.
"""

from datetime import datetime

import polars as pl

from data_canon.codebook.days import TravelDow
from data_canon.codebook.households import ResidenceRentOwn, ResidenceType
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    PersonType,
    SchoolType,
    Student,
)
from data_canon.codebook.trips import Driver, ModeType, Purpose, PurposeCategory


class TestDataBuilder:
    """Builder for creating complete, valid test datasets."""

    @staticmethod
    def create_minimal_household(
        hh_id: int = 1,
        home_lat: float = 37.70,
        home_lon: float = -122.40,
        **overrides,
    ) -> dict:
        """Create a minimal but complete household record.

        Includes ALL fields required by HouseholdModel validation.

        Args:
            hh_id: Household ID
            home_lat: Home latitude
            home_lon: Home longitude
            **overrides: Override any default values

        Returns:
            Complete household record dict
        """
        return {
            "hh_id": hh_id,
            "home_lat": home_lat,
            "home_lon": home_lon,
            "home_taz": 1,
            "home_maz": 1,
            "size": 2,
            "vehicles": 1,
            "income": 75000,
            "residence_type": ResidenceType.SFH.value,
            "residence_rent_own": ResidenceRentOwn.OWN.value,
            "weight": 1.0,
            **overrides,
        }

    @staticmethod
    def create_minimal_person(
        person_id: int = 1,
        hh_id: int = 1,
        person_type: PersonType = PersonType.FULL_TIME_WORKER,
        home_lat: float = 37.70,
        home_lon: float = -122.40,
        work_lat: float | None = 37.75,
        work_lon: float | None = -122.45,
        **overrides,
    ) -> dict:
        """Create a minimal but complete person record.

        Includes ALL fields required by PersonModel for extract_tours step.
        Automatically sets reasonable defaults based on person_type.

        Args:
            person_id: Person ID
            hh_id: Household ID
            person_type: Person type enum
            home_lat: Home latitude
            home_lon: Home longitude
            work_lat: Work latitude (None if not a worker)
            work_lon: Work longitude (None if not a worker)
            **overrides: Override any default values

        Returns:
            Complete person record dict
        """
        # Set defaults based on person type
        if person_type in [
            PersonType.FULL_TIME_WORKER,
            PersonType.PART_TIME_WORKER,
        ]:
            employment = (
                Employment.EMPLOYED_FULLTIME
                if person_type == PersonType.FULL_TIME_WORKER
                else Employment.EMPLOYED_PARTTIME
            )
            student = Student.NONSTUDENT
            age = AgeCategory.AGE_35_TO_44.value
            school_type = None
        elif person_type in [
            PersonType.UNIVERSITY_STUDENT,
            PersonType.HIGH_SCHOOL_STUDENT,
        ]:
            employment = Employment.UNEMPLOYED_NOT_LOOKING
            student = Student.FULLTIME_INPERSON
            age = (
                AgeCategory.AGE_18_TO_24.value
                if person_type == PersonType.UNIVERSITY_STUDENT
                else AgeCategory.AGE_16_TO_17.value
            )
            school_type = (
                SchoolType.COLLEGE
                if person_type == PersonType.UNIVERSITY_STUDENT
                else SchoolType.HIGH_SCHOOL
            )
        elif person_type == PersonType.CHILD_AGE_5_15:
            employment = Employment.UNEMPLOYED_NOT_LOOKING
            student = Student.NONSTUDENT
            age = AgeCategory.AGE_5_TO_15.value
            school_type = SchoolType.ELEMENTARY
        else:  # RETIREE, NON_WORKING_ADULT, CHILD_UNDER_5
            employment = Employment.UNEMPLOYED_NOT_LOOKING
            student = Student.NONSTUDENT
            age = (
                AgeCategory.AGE_65_TO_74.value
                if person_type == PersonType.RETIREE
                else AgeCategory.AGE_25_TO_34.value
                if person_type == PersonType.NON_WORKING_ADULT
                else AgeCategory.AGE_UNDER_5.value
            )
            school_type = None

        return {
            "person_id": person_id,
            "hh_id": hh_id,
            "person_type": person_type.value,
            "employment": employment.value,
            "student": student.value,
            "age": age,
            "gender": "female",
            "home_lat": home_lat,
            "home_lon": home_lon,
            "work_lat": work_lat,
            "work_lon": work_lon,
            "school_lat": None,
            "school_lon": None,
            "school_type": school_type.value if school_type else None,
            "weight": 1.0,
            **overrides,
        }

    @staticmethod
    def create_minimal_trip(
        trip_id: int,
        person_id: int = 1,
        hh_id: int = 1,
        day_id: int = 2,
        depart_time: datetime = datetime(2024, 1, 1, 8, 0),
        arrive_time: datetime = datetime(2024, 1, 1, 9, 0),
        o_purpose_category: PurposeCategory = PurposeCategory.HOME,
        d_purpose_category: PurposeCategory = PurposeCategory.WORK,
        mode: ModeType = ModeType.CAR,
        o_lat: float = 37.70,
        o_lon: float = -122.40,
        d_lat: float = 37.75,
        d_lon: float = -122.45,
        num_travelers: int = 1,
        driver: int = Driver.DRIVER.value,
        **overrides,
    ) -> dict:
        """Create a minimal but complete trip record.

        Includes ALL fields required by UnlinkedTripModel validation.
        The detailed o_purpose/d_purpose fields are set to generic values
        since tour extraction primarily uses purpose_category.

        Args:
            trip_id: Trip ID
            person_id: Person ID
            hh_id: Household ID
            day_id: Day ID
            depart_time: Departure datetime
            arrive_time: Arrival datetime
            o_purpose_category: Origin purpose category
            d_purpose_category: Destination purpose category
            mode: Mode type
            o_lat: Origin latitude
            o_lon: Origin longitude
            d_lat: Destination latitude
            d_lon: Destination longitude
            num_travelers: Number of travelers on trip
            driver: Driver status code
            **overrides: Override any default values

        Returns:
            Complete trip record dict
        """
        duration_minutes = (arrive_time - depart_time).total_seconds() / 60
        return {
            "trip_id": trip_id,
            "linked_trip_id": trip_id,
            "person_id": person_id,
            "hh_id": hh_id,
            "day_id": day_id,
            "travel_dow": TravelDow.MONDAY.value,
            "depart_time": depart_time,
            "arrive_time": arrive_time,
            "depart_date": depart_time.date(),
            "arrive_date": arrive_time.date(),
            "depart_hour": depart_time.hour,
            "depart_minute": depart_time.minute,
            "depart_seconds": 0,
            "arrive_hour": arrive_time.hour,
            "arrive_minute": arrive_time.minute,
            "arrive_seconds": 0,
            "o_purpose_category": o_purpose_category.value,
            "d_purpose_category": d_purpose_category.value,
            # Detailed purpose - generic values since tests focus on category
            "o_purpose": Purpose.MISSING.value,
            "d_purpose": Purpose.MISSING.value,
            "mode_type": mode.value,
            "o_lat": o_lat,
            "o_lon": o_lon,
            "d_lat": d_lat,
            "d_lon": d_lon,
            "distance_meters": 5.0,
            "duration_minutes": duration_minutes,
            "num_travelers": num_travelers,
            "driver": driver,
            "trip_weight": 1.0,
            **overrides,
        }


class ScenarioBuilder:
    """Builder for creating complete test scenarios."""

    @classmethod
    def simple_work_tour(
        cls,
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """Create simple work tour: Home -> Work -> Home (no subtours).

        Returns:
            Tuple of (households, persons, trips) DataFrames
        """
        home = (37.70, -122.40)
        work = (37.75, -122.45)

        hh = pl.DataFrame(
            [
                TestDataBuilder.create_minimal_household(
                    home_lat=home[0], home_lon=home[1]
                )
            ]
        )

        persons = pl.DataFrame(
            [
                TestDataBuilder.create_minimal_person(
                    person_id=1,
                    work_lat=work[0],
                    work_lon=work[1],
                    home_lat=home[0],
                    home_lon=home[1],
                )
            ]
        )

        trips = pl.DataFrame(
            [
                TestDataBuilder.create_minimal_trip(
                    trip_id=1,
                    depart_time=datetime(2024, 1, 1, 8, 0),
                    arrive_time=datetime(2024, 1, 1, 9, 0),
                    o_purpose_category=PurposeCategory.HOME,
                    d_purpose_category=PurposeCategory.WORK,
                    mode=ModeType.CAR,
                    o_lat=home[0],
                    o_lon=home[1],
                    d_lat=work[0],
                    d_lon=work[1],
                ),
                TestDataBuilder.create_minimal_trip(
                    trip_id=2,
                    linked_trip_id=2,
                    depart_time=datetime(2024, 1, 1, 17, 0),
                    arrive_time=datetime(2024, 1, 1, 17, 30),
                    o_purpose_category=PurposeCategory.WORK,
                    d_purpose_category=PurposeCategory.HOME,
                    mode=ModeType.CAR,
                    o_lat=work[0],
                    o_lon=work[1],
                    d_lat=home[0],
                    d_lon=home[1],
                ),
            ]
        )

        return hh, persons, trips

    @classmethod
    def work_tour_with_subtour(
        cls,
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """Create work tour with lunch subtour.

        Pattern: Home -> Work -> Lunch -> Work -> Home.

        Returns:
            Tuple of (households, persons, trips) DataFrames
        """
        home = (37.70, -122.40)
        work = (37.75, -122.45)
        lunch = (37.76, -122.46)

        hh = pl.DataFrame(
            [
                TestDataBuilder.create_minimal_household(
                    home_lat=home[0], home_lon=home[1]
                )
            ]
        )

        persons = pl.DataFrame(
            [
                TestDataBuilder.create_minimal_person(
                    person_id=1,
                    work_lat=work[0],
                    work_lon=work[1],
                    home_lat=home[0],
                    home_lon=home[1],
                )
            ]
        )

        trips = pl.DataFrame(
            [
                TestDataBuilder.create_minimal_trip(
                    trip_id=1,
                    depart_time=datetime(2024, 1, 1, 8, 0),
                    arrive_time=datetime(2024, 1, 1, 9, 0),
                    o_purpose_category=PurposeCategory.HOME,
                    d_purpose_category=PurposeCategory.WORK,
                    mode=ModeType.CAR,
                    o_lat=home[0],
                    o_lon=home[1],
                    d_lat=work[0],
                    d_lon=work[1],
                ),
                TestDataBuilder.create_minimal_trip(
                    trip_id=2,
                    linked_trip_id=2,
                    depart_time=datetime(2024, 1, 1, 12, 0),
                    arrive_time=datetime(2024, 1, 1, 12, 15),
                    o_purpose_category=PurposeCategory.WORK,
                    d_purpose_category=PurposeCategory.MEAL,
                    mode=ModeType.WALK,
                    o_lat=work[0],
                    o_lon=work[1],
                    d_lat=lunch[0],
                    d_lon=lunch[1],
                ),
                TestDataBuilder.create_minimal_trip(
                    trip_id=3,
                    linked_trip_id=3,
                    depart_time=datetime(2024, 1, 1, 13, 0),
                    arrive_time=datetime(2024, 1, 1, 13, 15),
                    o_purpose_category=PurposeCategory.MEAL,
                    d_purpose_category=PurposeCategory.WORK,
                    mode=ModeType.WALK,
                    o_lat=lunch[0],
                    o_lon=lunch[1],
                    d_lat=work[0],
                    d_lon=work[1],
                ),
                TestDataBuilder.create_minimal_trip(
                    trip_id=4,
                    linked_trip_id=4,
                    depart_time=datetime(2024, 1, 1, 17, 0),
                    arrive_time=datetime(2024, 1, 1, 17, 30),
                    o_purpose_category=PurposeCategory.WORK,
                    d_purpose_category=PurposeCategory.HOME,
                    mode=ModeType.CAR,
                    o_lat=work[0],
                    o_lon=work[1],
                    d_lat=home[0],
                    d_lon=home[1],
                ),
            ]
        )

        return hh, persons, trips

    @classmethod
    def multiple_tours_same_day(
        cls,
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """Create multiple tours: Home -> Work -> Home -> Shop -> Home.

        Returns:
            Tuple of (households, persons, trips) DataFrames
        """
        home = (37.70, -122.40)
        work = (37.75, -122.45)
        shop = (37.71, -122.41)

        hh = pl.DataFrame(
            [
                TestDataBuilder.create_minimal_household(
                    home_lat=home[0], home_lon=home[1]
                )
            ]
        )

        persons = pl.DataFrame(
            [
                TestDataBuilder.create_minimal_person(
                    person_id=1,
                    work_lat=work[0],
                    work_lon=work[1],
                    home_lat=home[0],
                    home_lon=home[1],
                )
            ]
        )

        trips = pl.DataFrame(
            [
                TestDataBuilder.create_minimal_trip(
                    trip_id=1,
                    depart_time=datetime(2024, 1, 1, 8, 0),
                    arrive_time=datetime(2024, 1, 1, 9, 0),
                    o_purpose_category=PurposeCategory.HOME,
                    d_purpose_category=PurposeCategory.WORK,
                    mode=ModeType.CAR,
                    o_lat=home[0],
                    o_lon=home[1],
                    d_lat=work[0],
                    d_lon=work[1],
                ),
                TestDataBuilder.create_minimal_trip(
                    trip_id=2,
                    linked_trip_id=2,
                    depart_time=datetime(2024, 1, 1, 17, 0),
                    arrive_time=datetime(2024, 1, 1, 17, 30),
                    o_purpose_category=PurposeCategory.WORK,
                    d_purpose_category=PurposeCategory.HOME,
                    mode=ModeType.CAR,
                    o_lat=work[0],
                    o_lon=work[1],
                    d_lat=home[0],
                    d_lon=home[1],
                ),
                TestDataBuilder.create_minimal_trip(
                    trip_id=3,
                    linked_trip_id=3,
                    depart_time=datetime(2024, 1, 1, 18, 0),
                    arrive_time=datetime(2024, 1, 1, 18, 15),
                    o_purpose_category=PurposeCategory.HOME,
                    d_purpose_category=PurposeCategory.SHOP,
                    mode=ModeType.WALK,
                    o_lat=home[0],
                    o_lon=home[1],
                    d_lat=shop[0],
                    d_lon=shop[1],
                ),
                TestDataBuilder.create_minimal_trip(
                    trip_id=4,
                    linked_trip_id=4,
                    depart_time=datetime(2024, 1, 1, 19, 0),
                    arrive_time=datetime(2024, 1, 1, 19, 15),
                    o_purpose_category=PurposeCategory.SHOP,
                    d_purpose_category=PurposeCategory.HOME,
                    mode=ModeType.WALK,
                    o_lat=shop[0],
                    o_lon=shop[1],
                    d_lat=home[0],
                    d_lon=home[1],
                ),
            ]
        )

        return hh, persons, trips

    @classmethod
    def no_work_location(
        cls,
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """Create work tour without defined work location.

        Returns:
            Tuple of (households, persons, trips) DataFrames
        """
        home = (37.70, -122.40)
        work_dest = (37.75, -122.45)

        hh = pl.DataFrame(
            [
                TestDataBuilder.create_minimal_household(
                    home_lat=home[0], home_lon=home[1]
                )
            ]
        )

        persons = pl.DataFrame(
            [
                TestDataBuilder.create_minimal_person(
                    person_id=1,
                    work_lat=None,  # No defined work location
                    work_lon=None,
                    home_lat=home[0],
                    home_lon=home[1],
                )
            ]
        )

        trips = pl.DataFrame(
            [
                TestDataBuilder.create_minimal_trip(
                    trip_id=1,
                    depart_time=datetime(2024, 1, 1, 8, 0),
                    arrive_time=datetime(2024, 1, 1, 9, 0),
                    o_purpose_category=PurposeCategory.HOME,
                    d_purpose_category=PurposeCategory.WORK,
                    mode=ModeType.CAR,
                    o_lat=home[0],
                    o_lon=home[1],
                    d_lat=work_dest[0],
                    d_lon=work_dest[1],
                ),
                TestDataBuilder.create_minimal_trip(
                    trip_id=2,
                    linked_trip_id=2,
                    depart_time=datetime(2024, 1, 1, 17, 0),
                    arrive_time=datetime(2024, 1, 1, 17, 30),
                    o_purpose_category=PurposeCategory.WORK,
                    d_purpose_category=PurposeCategory.HOME,
                    mode=ModeType.CAR,
                    o_lat=work_dest[0],
                    o_lon=work_dest[1],
                    d_lat=home[0],
                    d_lon=home[1],
                ),
            ]
        )

        return hh, persons, trips
