"""Test fixtures for DaySim formatter tests.

Provides factory methods for creating complete, valid canonical survey data
that can be used to test DaySim formatting functions.
"""

from datetime import datetime

import polars as pl

from data_canon.codebook.days import TravelDow
from data_canon.codebook.generic import LocationType
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
    WorkParking,
)
from data_canon.codebook.tours import TourCategory, TourDataQuality
from data_canon.codebook.trips import (
    AccessEgressMode,
    Driver,
    Mode,
    ModeType,
    PurposeCategory,
)


class DaysimTestDataBuilder:
    """Factory for creating canonical survey data for DaySim formatter tests."""

    @staticmethod
    def create_household(
        hh_id: int = 1,
        home_lat: float = 37.70,
        home_lon: float = -122.40,
        home_taz: int = 100,
        home_maz: int = 1000,
        size: int = 1,
        vehicles: int = 1,
        income_detailed: IncomeDetailed | None = IncomeDetailed.INCOME_75TO100,
        income_followup: IncomeFollowup | None = None,
        residence_type: ResidenceType = ResidenceType.SFH,
        residence_rent_own: ResidenceRentOwn = ResidenceRentOwn.OWN,
        **overrides,
    ) -> dict:
        """Create a complete canonical household record.

        Args:
            hh_id: Household ID
            home_lat: Home latitude
            home_lon: Home longitude
            home_taz: Home TAZ
            home_maz: Home MAZ
            size: Household size
            vehicles: Number of vehicles
            income_detailed: Detailed income category
            income_followup: Followup income category (if detailed is null)
            residence_type: Residence type
            residence_rent_own: Residence rent/own status
            **overrides: Override any default values

        Returns:
            Complete household record dict
        """
        return {
            "hh_id": hh_id,
            "home_lat": home_lat,
            "home_lon": home_lon,
            "home_taz": home_taz,
            "home_maz": home_maz,
            "size": size,
            "num_people": size,  # Add this field for DaySim formatting
            "vehicles": vehicles,
            "num_vehicles": vehicles,  # Add this field for DaySim formatting
            "num_workers": 1,  # Default to 1 worker
            "income": 87500,  # Approximate midpoint for 75-100k
            "income_detailed": income_detailed.value
            if income_detailed
            else None,
            "income_followup": income_followup.value
            if income_followup
            else None,
            "residence_type": residence_type.value,
            "residence_rent_own": residence_rent_own.value,
            "hh_weight": 1.0,
            **overrides,
        }

    @staticmethod
    def create_person(
        person_id: int = 1,
        hh_id: int = 1,
        person_num: int = 1,
        person_type: PersonType = PersonType.FULL_TIME_WORKER,
        employment: Employment | None = None,
        student: Student | None = None,
        age: AgeCategory | None = None,
        age_years: int | None = None,
        gender: Gender = Gender.FEMALE,
        home_lat: float = 37.70,
        home_lon: float = -122.40,
        work_lat: float | None = 37.75,
        work_lon: float | None = -122.45,
        work_taz: int | None = 200,
        work_maz: int | None = 2000,
        school_lat: float | None = None,
        school_lon: float | None = None,
        school_taz: int | None = None,
        school_maz: int | None = None,
        school_type: SchoolType | None = None,
        transit_pass: bool = False,
        work_mode: Mode = Mode.MISSING,
        is_proxy: bool = False,
        num_complete_days: int = 1,
        **overrides,
    ) -> dict:
        """Create a complete canonical person record.

        Args:
            person_id: Person ID
            hh_id: Household ID
            person_num: Person number within household
            person_type: Person type enum
            employment: Employment status (auto-derived if None)
            student: Student status (auto-derived if None)
            age: Age category (auto-derived if None)
            age_years: Exact age in years (auto-derived if None)
            gender: Gender enum
            home_lat: Home latitude
            home_lon: Home longitude
            work_lat: Work latitude
            work_lon: Work longitude
            work_taz: Work TAZ
            work_maz: Work MAZ
            school_lat: School latitude
            school_lon: School longitude
            school_taz: School TAZ
            school_maz: School MAZ
            school_type: School type
            transit_pass: Has transit pass
            work_mode: Usual work mode
            is_proxy: Is proxy respondent
            num_complete_days: Number of complete diary days
            **overrides: Override any default values

        Returns:
            Complete person record dict
        """
        # Auto-derive employment, student, age based on person_type
        # if not provided
        if employment is None:
            if person_type in [PersonType.FULL_TIME_WORKER]:
                employment = Employment.EMPLOYED_FULLTIME
            elif person_type == PersonType.PART_TIME_WORKER:
                employment = Employment.EMPLOYED_PARTTIME
            else:
                employment = Employment.UNEMPLOYED_NOT_LOOKING

        if student is None:
            if person_type in [
                PersonType.UNIVERSITY_STUDENT,
                PersonType.HIGH_SCHOOL_STUDENT,
            ]:
                student = Student.FULLTIME_INPERSON
            else:
                student = Student.NONSTUDENT

        if age is None:
            age_map = {
                PersonType.CHILD_UNDER_5: AgeCategory.AGE_UNDER_5,
                PersonType.CHILD_5_15: AgeCategory.AGE_5_TO_15,
                PersonType.HIGH_SCHOOL_STUDENT: AgeCategory.AGE_16_TO_17,
                PersonType.UNIVERSITY_STUDENT: AgeCategory.AGE_18_TO_24,
                PersonType.FULL_TIME_WORKER: AgeCategory.AGE_35_TO_44,
                PersonType.PART_TIME_WORKER: AgeCategory.AGE_35_TO_44,
                PersonType.NON_WORKER: AgeCategory.AGE_25_TO_34,
                PersonType.RETIRED: AgeCategory.AGE_65_TO_74,
            }
            age = age_map.get(person_type, AgeCategory.AGE_35_TO_44)

        if age_years is None:
            age_years_map = {
                AgeCategory.AGE_UNDER_5: 3,
                AgeCategory.AGE_5_TO_15: 10,
                AgeCategory.AGE_16_TO_17: 16,
                AgeCategory.AGE_18_TO_24: 21,
                AgeCategory.AGE_25_TO_34: 30,
                AgeCategory.AGE_35_TO_44: 40,
                AgeCategory.AGE_45_TO_54: 50,
                AgeCategory.AGE_55_TO_64: 60,
                AgeCategory.AGE_65_TO_74: 70,
                AgeCategory.AGE_75_TO_84: 80,
                AgeCategory.AGE_85_AND_UP: 90,
            }
            age_years = age_years_map[age]

        # Determine work_park based on whether person has work location
        if work_taz is not None:
            work_park = WorkParking.FREE
        else:
            work_park = WorkParking.NOT_APPLICABLE

        return {
            "person_id": person_id,
            "hh_id": hh_id,
            "person_num": person_num,
            "person_type": person_type.value,
            "employment": employment.value,
            "student": student.value,
            "age": age.value,
            "age_years": age_years,
            "gender": gender.value,
            "home_lat": home_lat,
            "home_lon": home_lon,
            "work_lat": work_lat,
            "work_lon": work_lon,
            "work_taz": work_taz,
            "work_maz": work_maz,
            "school_lat": school_lat,
            "school_lon": school_lon,
            "school_taz": school_taz,
            "school_maz": school_maz,
            "school_type": school_type.value if school_type else None,
            "work_park": work_park.value,
            "transit_pass": transit_pass,
            "work_mode": work_mode.value,
            "is_proxy": is_proxy,
            "num_days_complete": num_complete_days,
            "person_weight": 1.0,
            **overrides,
        }

    @staticmethod
    def create_day(
        day_id: int = 1,
        person_id: int = 1,
        hh_id: int = 1,
        person_num: int = 1,
        day_num: int = 1,
        travel_date: datetime = datetime(2023, 10, 15),
        travel_dow: TravelDow = TravelDow.MONDAY,
        is_complete: bool = True,
        **overrides,
    ) -> dict:
        """Create a complete canonical day record.

        Args:
            day_id: Day ID
            person_id: Person ID
            hh_id: Household ID
            person_num: Person number
            day_num: Day number
            travel_date: Travel date
            travel_dow: Day of week
            is_complete: Day completeness flag
            **overrides: Override any default values

        Returns:
            Complete day record dict
        """
        return {
            "day_id": day_id,
            "person_id": person_id,
            "hh_id": hh_id,
            "person_num": person_num,
            "day_num": day_num,
            "travel_date": travel_date,
            "travel_dow": travel_dow.value,
            "is_complete": is_complete,
            "day_weight": 1.0,
            **overrides,
        }

    @staticmethod
    def create_unlinked_trip(
        trip_id: int = 1,
        tour_id: int = 1,
        linked_trip_id: int = 1,
        person_id: int = 1,
        hh_id: int = 1,
        person_num: int = 1,
        day_num: int = 1,
        trip_num: int = 1,
        depart_time: datetime = datetime(2023, 10, 15, 8, 0),
        arrive_time: datetime = datetime(2023, 10, 15, 8, 30),
        origin_lat: float = 37.70,
        origin_lon: float = -122.40,
        origin_purpose: PurposeCategory = PurposeCategory.HOME,
        dest_lat: float = 37.75,
        dest_lon: float = -122.45,
        dest_purpose: PurposeCategory = PurposeCategory.WORK,
        mode: Mode = Mode.WALK,
        mode_type: ModeType = ModeType.WALK,
        driver: Driver = Driver.MISSING,
        transit_access: AccessEgressMode | None = None,
        transit_egress: AccessEgressMode | None = None,
        num_travelers: int = 1,
        distance_meters: float = 5.0,
        **overrides,
    ) -> dict:
        """Create a complete canonical unlinked trip record.

        Args:
            trip_id: Trip ID
            tour_id: Tour ID
            linked_trip_id: Linked trip ID this unlinked trip belongs to
            person_id: Person ID
            hh_id: Household ID
            person_num: Person number
            day_num: Day number
            trip_num: Trip number
            depart_time: Departure datetime
            arrive_time: Arrival datetime
            origin_lat: Origin latitude
            origin_lon: Origin longitude
            origin_purpose: Origin purpose
            dest_lat: Destination latitude
            dest_lon: Destination longitude
            dest_purpose: Destination purpose
            mode: Mode enum
            mode_type: Mode type enum
            driver: Driver status
            transit_access: Transit access mode (for transit trips)
            transit_egress: Transit egress mode (for transit trips)
            num_travelers: Number of travelers in vehicle
            distance_meters: Trip distance in meters
            **overrides: Override any default values

        Returns:
            Complete unlinked trip record dict
        """
        return {
            "trip_id": trip_id,
            "linked_trip_id": linked_trip_id,
            "person_id": person_id,
            "hh_id": hh_id,
            "person_num": person_num,
            "day_num": day_num,
            "trip_num": trip_num,
            "tour_id": tour_id,
            "depart_time": depart_time,
            "arrive_time": arrive_time,
            "origin_lat": origin_lat,
            "origin_lon": origin_lon,
            "origin_purpose": origin_purpose.value,
            "dest_lat": dest_lat,
            "dest_lon": dest_lon,
            "dest_purpose": dest_purpose.value,
            "mode": mode.value,
            "mode_type": mode_type.value,
            "driver": driver.value,
            "transit_access": transit_access.value if transit_access else None,
            "transit_egress": transit_egress.value if transit_egress else None,
            "num_travelers": num_travelers,
            "distance_meters": distance_meters,
            "trip_weight": 1.0,
            **overrides,
        }

    @staticmethod
    def create_linked_trip(
        linked_trip_id: int = 1,
        person_id: int = 1,
        hh_id: int = 1,
        person_num: int = 1,
        day_id: int = 1,
        day_num: int = 1,
        travel_dow: TravelDow = TravelDow.SUNDAY,
        linked_trip_num: int = 1,
        tour_id: int = 1,
        depart_time: datetime = datetime(2023, 10, 15, 8, 0),
        arrive_time: datetime = datetime(2023, 10, 15, 8, 30),
        origin_lat: float = 37.70,
        origin_lon: float = -122.40,
        origin_taz: int = 100,
        origin_maz: int = 1000,
        origin_purpose: PurposeCategory = PurposeCategory.HOME,
        dest_lat: float = 37.75,
        dest_lon: float = -122.45,
        dest_taz: int = 200,
        dest_maz: int = 2000,
        dest_purpose: PurposeCategory = PurposeCategory.WORK,
        mode: Mode = Mode.HOUSEHOLD_VEHICLE_1,
        mode_type: ModeType = ModeType.CAR,
        driver: Driver = Driver.DRIVER,
        num_travelers: int = 1,
        distance_meters: float = 8046.72,
        num_unlinked_trips: int = 1,
        tour_direction: int = 1,  # 1=OUTBOUND, 2=INBOUND
        access_mode: AccessEgressMode | None = None,
        egress_mode: AccessEgressMode | None = None,
        **overrides,
    ) -> dict:
        """Create a complete canonical linked trip record.

        Args:
            linked_trip_id: Linked trip ID
            person_id: Person ID
            hh_id: Household ID
            person_num: Person number
            day_id: Day ID
            day_num: Day number
            travel_dow: Day of week
            linked_trip_num: Linked trip number
            tour_id: Parent tour ID
            depart_time: Departure datetime
            arrive_time: Arrival datetime
            origin_lat: Origin latitude
            origin_lon: Origin longitude
            origin_taz: Origin TAZ
            origin_maz: Origin MAZ
            origin_purpose: Origin purpose
            dest_lat: Destination latitude
            dest_lon: Destination longitude
            dest_taz: Destination TAZ
            dest_maz: Destination MAZ
            dest_purpose: Destination purpose
            mode: Aggregated mode enum
            mode_type: Aggregated mode type enum
            driver: Driver status
            num_travelers: Number of travelers
            distance_meters: Trip distance
            num_unlinked_trips: Number of component unlinked trips
            tour_direction: Tour direction (1=OUTBOUND, 2=INBOUND)
            access_mode: Transit access mode (for transit trips)
            egress_mode: Transit egress mode (for transit trips)
            **overrides: Override any default values

        Returns:
            Complete linked trip record dict
        """
        return {
            "linked_trip_id": linked_trip_id,
            "person_id": person_id,
            "hh_id": hh_id,
            "person_num": person_num,
            "day_id": day_id,
            "day_num": day_num,
            "travel_dow": travel_dow.value,
            "linked_trip_num": linked_trip_num,
            "tour_id": tour_id,
            "depart_time": depart_time,
            "arrive_time": arrive_time,
            "duration_minutes": int(
                (arrive_time - depart_time).total_seconds() / 60
            ),
            "o_lat": origin_lat,
            "o_lon": origin_lon,
            "o_taz": origin_taz,
            "o_maz": origin_maz,
            "o_purpose_category": origin_purpose.value,
            "d_lat": dest_lat,
            "d_lon": dest_lon,
            "d_taz": dest_taz,
            "d_maz": dest_maz,
            "d_purpose_category": dest_purpose.value,
            "mode": mode.value,
            "mode_type": mode_type.value,
            "driver": driver.value,
            "num_travelers": num_travelers,
            "distance_meters": distance_meters,
            "num_unlinked_trips": num_unlinked_trips,
            "tour_direction": tour_direction,
            "access_mode": access_mode.value if access_mode else None,
            "egress_mode": egress_mode.value if egress_mode else None,
            "linked_trip_weight": 1.0,
            **overrides,
        }

    @staticmethod
    def create_tour(
        tour_id: int = 1,
        person_id: int = 1,
        hh_id: int = 1,
        person_num: int = 1,
        day_id: int = 1,
        day_num: int = 1,
        tour_num: int = 1,
        tour_purpose: PurposeCategory = PurposeCategory.WORK,
        origin_lat: float = 37.70,
        origin_lon: float = -122.40,
        o_location_type: str = LocationType.HOME.value,
        primary_dest_lat: float = 37.75,
        primary_dest_lon: float = -122.45,
        d_location_type: str = LocationType.WORK.value,
        origin_depart_time: datetime = datetime(2023, 10, 15, 8, 0),
        dest_arrive_time: datetime = datetime(2023, 10, 15, 8, 30),
        dest_depart_time: datetime = datetime(2023, 10, 15, 17, 0),
        origin_arrive_time: datetime = datetime(2023, 10, 15, 17, 30),
        num_trips: int = 2,
        origin_linked_trip_id: int | None = None,
        dest_linked_trip_id: int | None = None,
        parent_tour_id: int = 1,
        **overrides,
    ) -> dict:
        """Create a complete canonical tour record.

        Args:
            tour_id: Tour ID
            person_id: Person ID
            hh_id: Household ID
            person_num: Person number
            day_id: Day ID
            day_num: Day number
            tour_num: Tour number
            tour_purpose: Tour purpose
            origin_lat: Tour origin latitude
            origin_lon: Tour origin longitude
            origin_purpose: Tour origin purpose
            o_location_type: Origin location type code (1=home, 2=work, etc.)
            primary_dest_lat: Primary destination latitude
            primary_dest_lon: Primary destination longitude
            primary_dest_purpose: Primary destination purpose
            d_location_type: Destination location type code
            origin_depart_time: Departure from origin
            dest_arrive_time: Arrival at destination
            dest_depart_time: Departure from destination
            origin_arrive_time: Arrival back at origin
            num_trips: Number of linked trips
            origin_linked_trip_id: ID of first trip in tour
            dest_linked_trip_id: ID of trip arriving at primary destination
            parent_tour_id: Parent tour ID (for subtours)
            **overrides: Override any default values

        Returns:
            Complete tour record dict
        """
        # Default trip IDs if not specified
        if origin_linked_trip_id is None:
            origin_linked_trip_id = tour_id * 100 + 1
        if dest_linked_trip_id is None:
            dest_linked_trip_id = tour_id * 100 + (
                num_trips // 2 if num_trips > 2 else 2
            )

        return {
            "tour_id": tour_id,
            "person_id": person_id,
            "hh_id": hh_id,
            "person_num": person_num,
            "day_id": day_id,
            "day_num": day_num,
            "tour_num": tour_num,
            "tour_purpose": tour_purpose.value,
            "o_lat": origin_lat,
            "o_lon": origin_lon,
            "d_lat": primary_dest_lat,
            "d_lon": primary_dest_lon,
            "o_location_type": o_location_type,
            "d_location_type": d_location_type,
            "tour_mode": 1,
            "origin_depart_time": origin_depart_time,
            "origin_arrive_time": origin_arrive_time,
            "dest_arrive_time": dest_arrive_time,
            "dest_depart_time": dest_depart_time,
            "origin_linked_trip_id": origin_linked_trip_id,
            "dest_linked_trip_id": dest_linked_trip_id,
            "parent_tour_id": parent_tour_id,
            "tour_category": TourCategory.COMPLETE.value,
            "tour_data_quality": TourDataQuality.VALID.value,
            "tour_weight": 1.0,
            **overrides,
        }


class DaysimScenarioBuilder:
    """Factory for creating complete multi-table test scenarios."""

    @staticmethod
    def simple_work_tour() -> tuple[
        pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame
    ]:
        """Create a simple work tour scenario: Home -> Work -> Home.

        Returns:
            Tuple of (households, persons, days, linked_trips, tours) DataFrames
        """
        households = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_household(
                    hh_id=1, home_taz=100, home_maz=1000
                )
            ]
        )

        persons = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_person(
                    person_id=1,
                    hh_id=1,
                    person_num=1,
                    person_type=PersonType.FULL_TIME_WORKER,
                    work_taz=200,
                    work_maz=2000,
                )
            ]
        )

        days = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_day(
                    day_id=1, person_id=1, hh_id=1, person_num=1, day_num=1
                )
            ]
        )

        linked_trips = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_linked_trip(
                    linked_trip_id=1,
                    person_id=1,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    linked_trip_num=1,
                    tour_id=1,
                    origin_purpose=PurposeCategory.HOME,
                    dest_purpose=PurposeCategory.WORK,
                    depart_time=datetime(2023, 10, 15, 8, 0),
                    arrive_time=datetime(2023, 10, 15, 8, 30),
                    mode=Mode.HOUSEHOLD_VEHICLE_1,
                    driver=Driver.DRIVER,
                    num_travelers=1,
                    distance_meters=1000.0,
                ),
                DaysimTestDataBuilder.create_linked_trip(
                    linked_trip_id=2,
                    person_id=1,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    linked_trip_num=2,
                    tour_id=1,
                    origin_purpose=PurposeCategory.WORK,
                    dest_purpose=PurposeCategory.HOME,
                    depart_time=datetime(2023, 10, 15, 17, 0),
                    arrive_time=datetime(2023, 10, 15, 17, 30),
                    mode=Mode.HOUSEHOLD_VEHICLE_1,
                    driver=Driver.DRIVER,
                    num_travelers=1,
                    distance_meters=1000.0,
                ),
            ]
        )

        tours = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_tour(
                    tour_id=1,
                    person_id=1,
                    hh_id=1,
                    person_num=1,
                    day_id=1,
                    day_num=1,
                    tour_num=1,
                    tour_purpose=PurposeCategory.WORK,
                    origin_depart_time=datetime(2023, 10, 15, 8, 0),
                    dest_arrive_time=datetime(2023, 10, 15, 8, 30),
                    dest_depart_time=datetime(2023, 10, 15, 17, 0),
                    origin_arrive_time=datetime(2023, 10, 15, 17, 30),
                    num_trips=2,
                )
            ]
        )

        return households, persons, days, linked_trips, tours

    @staticmethod
    def transit_commute() -> tuple[
        pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame
    ]:
        """Create a transit commute scenario with walk-to-BART.

        Returns:
            Tuple of (households, persons, days, linked_trips, tours) DataFrames
        """
        households = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_household(
                    hh_id=1, home_taz=100, home_maz=1000
                )
            ]
        )

        persons = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_person(
                    person_id=1,
                    hh_id=1,
                    person_num=1,
                    person_type=PersonType.FULL_TIME_WORKER,
                    work_taz=200,
                    work_maz=2000,
                    transit_pass=True,
                )
            ]
        )

        days = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_day(
                    day_id=1, person_id=1, hh_id=1, person_num=1, day_num=1
                )
            ]
        )

        linked_trips = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_linked_trip(
                    linked_trip_id=1,
                    person_id=1,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    linked_trip_num=1,
                    tour_id=1,
                    origin_purpose=PurposeCategory.HOME,
                    dest_purpose=PurposeCategory.WORK,
                    depart_time=datetime(2023, 10, 15, 7, 30),
                    arrive_time=datetime(2023, 10, 15, 8, 30),
                    mode=Mode.BART,
                    mode_type=ModeType.TRANSIT,
                    driver=Driver.MISSING,
                    num_travelers=1,
                    distance_meters=15000.0,
                ),
                DaysimTestDataBuilder.create_linked_trip(
                    linked_trip_id=2,
                    person_id=1,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    linked_trip_num=2,
                    tour_id=1,
                    origin_purpose=PurposeCategory.WORK,
                    dest_purpose=PurposeCategory.HOME,
                    depart_time=datetime(2023, 10, 15, 17, 30),
                    arrive_time=datetime(2023, 10, 15, 18, 30),
                    mode=Mode.BART,
                    mode_type=ModeType.TRANSIT,
                    driver=Driver.MISSING,
                    num_travelers=1,
                    distance_meters=15000.0,
                ),
            ]
        )

        tours = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_tour(
                    tour_id=1,
                    person_id=1,
                    hh_id=1,
                    person_num=1,
                    day_id=1,
                    day_num=1,
                    tour_num=1,
                    tour_purpose=PurposeCategory.WORK,
                    origin_depart_time=datetime(2023, 10, 15, 7, 30),
                    dest_arrive_time=datetime(2023, 10, 15, 8, 30),
                    dest_depart_time=datetime(2023, 10, 15, 17, 30),
                    origin_arrive_time=datetime(2023, 10, 15, 18, 30),
                    num_trips=2,
                )
            ]
        )

        return households, persons, days, linked_trips, tours

    @staticmethod
    def multi_person_household() -> tuple[
        pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame
    ]:
        """Create a multi-person household with various person types.

        Returns:
            Tuple of (households, persons, days, linked_trips, tours) DataFrames
        """
        households = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_household(
                    hh_id=1, home_taz=100, home_maz=1000, size=4, vehicles=2
                )
            ]
        )

        persons = pl.DataFrame(
            [
                # Adult full-time worker
                DaysimTestDataBuilder.create_person(
                    person_id=1,
                    hh_id=1,
                    person_num=1,
                    person_type=PersonType.FULL_TIME_WORKER,
                    age_years=42,
                    gender=Gender.MALE,
                    work_taz=200,
                    work_maz=2000,
                ),
                # Adult part-time worker
                DaysimTestDataBuilder.create_person(
                    person_id=2,
                    hh_id=1,
                    person_num=2,
                    person_type=PersonType.PART_TIME_WORKER,
                    age_years=40,
                    gender=Gender.FEMALE,
                    work_taz=300,
                    work_maz=3000,
                ),
                # High school student
                DaysimTestDataBuilder.create_person(
                    person_id=3,
                    hh_id=1,
                    person_num=3,
                    person_type=PersonType.HIGH_SCHOOL_STUDENT,
                    age_years=16,
                    gender=Gender.FEMALE,
                    work_lat=None,
                    work_lon=None,
                    work_taz=None,
                    work_maz=None,
                    school_lat=37.72,
                    school_lon=-122.42,
                    school_taz=150,
                    school_maz=1500,
                    school_type=SchoolType.HIGH_SCHOOL,
                ),
                # Child
                DaysimTestDataBuilder.create_person(
                    person_id=4,
                    hh_id=1,
                    person_num=4,
                    person_type=PersonType.CHILD_5_15,
                    age_years=8,
                    gender=Gender.MALE,
                    work_lat=None,
                    work_lon=None,
                    work_taz=None,
                    work_maz=None,
                    school_lat=37.71,
                    school_lon=-122.41,
                    school_taz=120,
                    school_maz=1200,
                    school_type=SchoolType.ELEMENTARY,
                ),
            ]
        )

        days = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_day(
                    day_id=i, person_id=i, hh_id=1, person_num=i, day_num=1
                )
                for i in range(1, 5)
            ]
        )

        # Create minimal trips/tours for persons 1 and 2 only
        linked_trips = pl.DataFrame(
            [
                # Person 1: Work tour
                DaysimTestDataBuilder.create_linked_trip(
                    linked_trip_id=1,
                    person_id=1,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    linked_trip_num=1,
                    tour_id=1,
                    origin_purpose=PurposeCategory.HOME,
                    dest_purpose=PurposeCategory.WORK,
                ),
                DaysimTestDataBuilder.create_linked_trip(
                    linked_trip_id=2,
                    person_id=1,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    linked_trip_num=2,
                    tour_id=1,
                    origin_purpose=PurposeCategory.WORK,
                    dest_purpose=PurposeCategory.HOME,
                ),
                # Person 2: Work tour
                DaysimTestDataBuilder.create_linked_trip(
                    linked_trip_id=3,
                    person_id=2,
                    hh_id=1,
                    person_num=2,
                    day_num=1,
                    linked_trip_num=1,
                    tour_id=2,
                    origin_purpose=PurposeCategory.HOME,
                    dest_purpose=PurposeCategory.WORK,
                ),
                DaysimTestDataBuilder.create_linked_trip(
                    linked_trip_id=4,
                    person_id=2,
                    hh_id=1,
                    person_num=2,
                    day_num=1,
                    linked_trip_num=2,
                    tour_id=2,
                    origin_purpose=PurposeCategory.WORK,
                    dest_purpose=PurposeCategory.HOME,
                ),
            ]
        )

        tours = pl.DataFrame(
            [
                DaysimTestDataBuilder.create_tour(
                    tour_id=1,
                    person_id=1,
                    hh_id=1,
                    person_num=1,
                    day_id=1,
                    day_num=1,
                    tour_num=1,
                    tour_purpose=PurposeCategory.WORK,
                ),
                DaysimTestDataBuilder.create_tour(
                    tour_id=2,
                    person_id=2,
                    hh_id=1,
                    person_num=2,
                    day_id=2,
                    day_num=1,
                    tour_num=1,
                    tour_purpose=PurposeCategory.WORK,
                ),
            ]
        )

        return households, persons, days, linked_trips, tours
