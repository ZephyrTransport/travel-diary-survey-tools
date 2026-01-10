"""Base record builders for households, persons, and days.

This module provides simplified builders for core survey entities.
Person types are derived automatically using the pipeline's
derive_person_type() function.
"""

from datetime import UTC, date, datetime

import polars as pl

from data_canon.codebook.days import TravelDow
from data_canon.codebook.generic import BooleanYesNo
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
    JobType,
    SchoolType,
    Student,
    WorkParking,
)
from data_canon.codebook.trips import Mode
from processing.tours.person_type import derive_person_type


def create_household(
    hh_id: int = 1,
    home_lat: float | None = 37.70,
    home_lon: float | None = -122.40,
    home_taz: int | None = None,
    home_maz: int | None = None,
    home_walk_subzone: int | None = None,
    num_people: int = 1,
    num_vehicles: int = 1,
    num_workers: int = 1,
    income_detailed: IncomeDetailed | None = IncomeDetailed.INCOME_75TO100,
    income_followup: IncomeFollowup | None = None,
    residence_type: ResidenceType | None = ResidenceType.SFH,
    residence_rent_own: ResidenceRentOwn | None = ResidenceRentOwn.OWN,
    hh_weight: float = 1.0,
    **overrides,
) -> dict:
    """Create a complete canonical household record.

    Includes all fields that may be required by various formatters.
    All fields are always included with sensible defaults for formatter
    compatibility.

    Args:
        hh_id: Household ID
        home_lat: Home latitude (optional, for MAZ-based models)
        home_lon: Home longitude (optional, for MAZ-based models)
        home_taz: Home TAZ (required - assigned via spatial join in tests)
        home_maz: Home MAZ (optional, for Daysim)
        home_walk_subzone: Walk-to-transit subzone 0/1/2 (CTRAMP)
        num_people: Household size
        num_vehicles: Number of vehicles
        num_workers: Number of workers
        income_detailed: Detailed income category enum
        income_followup: Followup income category enum (if detailed is null)
        residence_type: Residence type enum (default SFH, for Daysim)
        residence_rent_own: Residence rent/own status enum (default OWN, for
            Daysim)
        hh_weight: Household expansion factor
        **overrides: Override any default values

    Returns:
        Complete household record dict
    """
    record = {
        "hh_id": hh_id,
        "home_taz": home_taz,
        "num_people": num_people,
        "num_vehicles": num_vehicles,
        "num_workers": num_workers,
        "income_detailed": income_detailed.value if income_detailed else None,
        "income_followup": income_followup.value if income_followup else None,
        "hh_weight": hh_weight,
    }

    # Always include optional fields (formatters may require them)
    record["home_lat"] = home_lat
    record["home_lon"] = home_lon
    record["home_maz"] = home_maz
    record["home_walk_subzone"] = home_walk_subzone
    record["residence_type"] = residence_type.value if residence_type else None
    record["residence_rent_own"] = residence_rent_own.value if residence_rent_own else None

    return {**record, **overrides}


def create_person(
    person_id: int = 101,
    hh_id: int = 1,
    person_num: int = 1,
    age: AgeCategory = AgeCategory.AGE_35_TO_44,
    gender: Gender = Gender.MALE,
    employment: Employment = Employment.EMPLOYED_FULLTIME,
    student: Student = Student.NONSTUDENT,
    school_type: SchoolType = SchoolType.MISSING,
    job_type: int | None = None,
    commute_subsidy_use_3: BooleanYesNo = BooleanYesNo.NO,
    commute_subsidy_use_4: BooleanYesNo = BooleanYesNo.NO,
    value_of_time: float = 15.0,
    # Optional location fields
    home_lat: float | None = None,
    home_lon: float | None = None,
    work_lat: float | None = None,
    work_lon: float | None = None,
    work_taz: int | None = None,
    work_maz: int | None = None,
    school_lat: float | None = None,
    school_lon: float | None = None,
    school_taz: int | None = None,
    school_maz: int | None = None,
    # Optional Daysim-specific fields
    work_park: WorkParking | None = None,
    transit_pass: bool | None = None,
    work_mode: Mode | None = None,
    is_proxy: bool | None = None,
    num_complete_days: int = 0,
    days: list[dict] | None = None,
    **overrides,
) -> dict:
    """Create a complete canonical person record.

    Person type is automatically derived from age/employment/student using the
    pipeline's derive_person_type() function.

    Args:
        person_id: Person ID
        hh_id: Household ID
        person_num: Person number within household
        age: Person age category (AgeCategory enum or int)
        gender: Gender enumeration
        employment: Employment status enum
        student: Student status enum
        school_type: Type of school enum (if student)
        job_type: Job type enum (if employed).
        commute_subsidy_use_3: Free parking used (BooleanYesNo)
        commute_subsidy_use_4: Discounted parking used (BooleanYesNo)
        value_of_time: Value of time in $/hour
        home_lat: Home latitude (optional)
        home_lon: Home longitude (optional)
        work_lat: Work latitude (optional)
        work_lon: Work longitude (optional)
        work_taz: Work TAZ (optional - assigned via spatial join in tests)
        work_maz: Work MAZ (optional, for Daysim)
        school_lat: School latitude (optional)
        school_lon: School longitude (optional)
        school_taz: School TAZ (optional - assigned via spatial join in tests)
        school_maz: School MAZ (optional, for Daysim)
        work_park: Work parking type enum (optional, for Daysim)
        transit_pass: Has transit pass (optional, for Daysim)
        work_mode: Usual work mode enum (optional, for Daysim)
        is_proxy: Is proxy interview (optional, for Daysim)
        num_complete_days: Number of complete days (default 0, for Daysim)
        days: Day records to compute num_complete_days from (optional)
        **overrides: Override any default values

    Returns:
        Complete person record dict with person_type auto-derived
    """
    record = {
        "person_id": person_id,
        "hh_id": hh_id,
        "person_num": person_num,
        "age": age.value,
        "gender": gender.value,
        "employment": employment.value,
        "student": student.value,
        "school_type": school_type.value,
        "commute_subsidy_use_3": commute_subsidy_use_3.value,
        "commute_subsidy_use_4": commute_subsidy_use_4.value,
        "value_of_time": value_of_time,
    }

    # Set job_type - default to FIXED (1) for employed, MISSING (995) for non-workers
    if job_type is None:
        if employment.value in [
            Employment.EMPLOYED_FULLTIME.value,
            Employment.EMPLOYED_PARTTIME.value,
            Employment.EMPLOYED_SELF.value,
        ]:
            job_type = JobType.FIXED.value
        else:
            job_type = JobType.MISSING.value
    record["job_type"] = job_type

    # Always include all location fields (tour extraction requires them
    # even if None)
    record["home_lat"] = home_lat
    record["home_lon"] = home_lon
    record["work_lat"] = work_lat
    record["work_lon"] = work_lon
    record["work_taz"] = work_taz
    record["work_maz"] = work_maz
    record["school_lat"] = school_lat
    record["school_lon"] = school_lon
    record["school_taz"] = school_taz
    record["school_maz"] = school_maz

    # Compute num_days_complete from days if provided
    if days is not None:
        num_complete_days = sum(1 for day in days if day.get("is_complete", False))

    # Always include DaySim-specific fields with sensible defaults

    # Work parking
    if work_park is None:
        work_park = WorkParking.FREE if work_taz is not None else WorkParking.NOT_APPLICABLE
    record["work_park"] = work_park.value

    # Transit pass
    if transit_pass is None:
        transit_pass = BooleanYesNo.NO.value
    elif isinstance(transit_pass, bool):
        transit_pass = BooleanYesNo.YES.value if transit_pass else BooleanYesNo.NO.value
    record["transit_pass"] = transit_pass

    # Usual work mode
    if work_mode is None:
        work_mode = Mode.MISSING
    record["work_mode"] = work_mode.value

    # Proxy interview
    if is_proxy is None:
        is_proxy = BooleanYesNo.NO.value
    elif isinstance(is_proxy, bool):
        is_proxy = BooleanYesNo.YES.value if is_proxy else BooleanYesNo.NO.value
    record["is_proxy"] = is_proxy

    # Complete days
    record["num_days_complete"] = num_complete_days

    # Apply overrides before deriving person_type
    record = {**record, **overrides}

    # Derive person_type using pipeline function (single source of truth)
    person_df = pl.DataFrame([record])
    person_df = derive_person_type(person_df)
    record["person_type"] = person_df["person_type"][0]

    return record


def create_day(
    day_id: int = 1,
    person_id: int = 101,
    hh_id: int = 1,
    person_num: int = 1,
    day_num: int = 1,
    travel_date: date | None = None,
    travel_dow: TravelDow = TravelDow.MONDAY,
    is_complete: bool = True,
    num_trips: int = 0,
    day_weight: float = 1.0,
    **overrides,
) -> dict:
    """Create a day record for multi-day scenarios.

    Day records track which days each person provided diary data for,
    including completeness and basic trip counts. Used primarily for
    DaySim formatting which requires day-level data.

    Args:
        day_id: Day ID
        person_id: Person ID
        hh_id: Household ID
        person_num: Person number within household (for DaySim)
        day_num: Day number in survey period (for DaySim)
        travel_date: Travel date (defaults to today)
        travel_dow: Day of week enum
        is_complete: Day complete (person at home at start/end)
        num_trips: Number of trips on this day
        day_weight: Day expansion factor (for DaySim)
        **overrides: Override any default values

    Returns:
        Complete day record dict
    """
    if travel_date is None:
        travel_date = datetime.now(tz=UTC).date()

    record = {
        "day_id": day_id,
        "person_id": person_id,
        "hh_id": hh_id,
        "person_num": person_num,
        "day_num": day_num,
        "travel_date": travel_date,
        "travel_dow": travel_dow.value,
        "is_complete": is_complete,
        "num_trips": num_trips,
        "day_weight": day_weight,
    }

    return {**record, **overrides}
