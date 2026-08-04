"""Data models for CT-RAMP file formats.

Based on [https://github.com/BayAreaMetro/modeling-website/wiki/DataDictionary](https://github.com/BayAreaMetro/modeling-website/wiki/DataDictionary)

See [Pipeline Steps > Format Output > CT-RAMP Format](../pipeline_steps/format_output/ctramp.md)
"""
# ruff: noqa: E501 N815

from pydantic import BaseModel, Field, field_validator

from data_canon.codebook.ctramp import (
    CTRAMPActivityPattern,
    CTRAMPEmploymentCategory,
    CTRAMPIndustry,
    CTRAMPModeType,
    CTRAMPPersonType,
    CTRAMPStudentCategory,
    CTRAMPTourCategory,
    CTRAMPTourPurpose,
    CTRAMPTripPurpose,
    FreeParkingChoice,
    IMFChoice,
    TourComposition,
    WalkToTransitSubZone,
    WFHChoice,
)


class HouseholdCTRAMPModel(BaseModel):
    """Household results from Travel Model (CT-RAMP format)."""

    hh_id: int = Field(description="Unique household ID number")
    taz: int = Field(
        ge=1,
        le=1454,
        description="Transportation analysis zone of home location",
    )
    income: int | None = Field(default=None, ge=0, description="Annual household income ($2000)")
    autos: int = Field(ge=0, description="Household automobiles")
    jtf_choice: int = Field(ge=-4, le=21, description="Number and type of household joint tours")
    size: int = Field(ge=1, description="Number of persons in the household")
    workers: int = Field(ge=0, description="Number of full- or part-time workers in the household")
    # NOTE: Model output only, not derivable from survey data.
    humanVehicles: int | None = Field(
        default=None, ge=0, description="Household automobiles, human-driven (New in TM1.5)"
    )
    autonomousVehicles: int | None = Field(
        default=None, ge=0, description="Household automobiles, autonomous (New in TM1.5)"
    )
    walk_subzone: WalkToTransitSubZone | None = Field(
        default=None,
        description="Walk to transit sub-zone (0=cannot walk to transit, 1=short-walk, 2=long-walk)",
    )
    auto_suff: int | None = Field(default=None, description="Incorrectly coded; ignore")
    wfh_choice: WFHChoice | None = Field(
        default=None,
        description="Work-from-home choice (0=non-worker or workers who don't work from home, 1=workers who work from home)",
    )
    # NOTE: Model output only, not derivable from survey data.
    ao_rn: int | None = Field(
        default=None, description="Random number for automobile ownership model"
    )
    fp_rn: int | None = Field(default=None, description="Random number for free parking model")
    cdap_rn: int | None = Field(
        default=None, description="Random number for coordinated daily activity pattern model"
    )
    imtf_rn: int | None = Field(
        default=None, description="Random number for individual mandatory tour frequency model"
    )
    imtod_rn: int | None = Field(
        default=None, description="Random number for individual mandatory tour time-of-day model"
    )
    immc_rn: int | None = Field(
        default=None, description="Random number for individual mandatory mode choice model"
    )
    jtf_rn: int | None = Field(
        default=None, description="Random number for joint tour frequency model"
    )
    jtl_rn: int | None = Field(
        default=None, description="Random number for joint tour location choice model"
    )
    jtod_rn: int | None = Field(
        default=None, description="Random number for joint tour time-of-day model"
    )
    jmc_rn: int | None = Field(
        default=None, description="Random number for joint tour mode choice model"
    )
    inmtf_rn: int | None = Field(
        default=None, description="Random number for individual non-mandatory tour frequency model"
    )
    inmtl_rn: int | None = Field(
        default=None, description="Random number for individual non-mandatory location choice model"
    )
    inmtod_rn: int | None = Field(
        default=None, description="Random number for individual non-mandatory time-of-day model"
    )
    inmmc_rn: int | None = Field(
        default=None, description="Random number for individual non-mandatory mode choice model"
    )
    awf_rn: int | None = Field(
        default=None, description="Random number for at-work frequency model"
    )
    awl_rn: int | None = Field(
        default=None, description="Random number for at-work location choice model"
    )
    awtod_rn: int | None = Field(
        default=None, description="Random number for at-work time-of-day model"
    )
    awmc_rn: int | None = Field(
        default=None, description="Random number for at-work mode choice model"
    )
    stf_rn: int | None = Field(default=None, description="Random number for stop frequency model")
    stl_rn: int | None = Field(
        default=None, description="Random number for stop location choice model"
    )

    # NOTE: Not part of CT-RAMP spec; for survey - model comparative analysis.
    sampleRate: float | None = Field(
        default=None,
        ge=0,
        description="This household represents 1/sampleRate households, calculate from weights as 1/weight",
    )  # For some reason this wasn't in the original CTRAMP model spec
    hh_weight: float | None = Field(
        default=None, ge=0, description="Survey weight for the household (not part of CT-RAMP spec)"
    )


class PersonCTRAMPModel(BaseModel):
    """Person results from Travel Model (CT-RAMP format)."""

    hh_id: int = Field(description="Unique household ID number")
    person_id: int = Field(description="Unique person ID number")
    person_num: int = Field(ge=1, description="Person number unique to the household")
    age: int = Field(ge=0, description="Person age")
    gender: str = Field(description="Person gender (m=male, f=female)")
    type: CTRAMPPersonType = Field(
        description=(
            "Person lifestage/employment type (Full-time worker, Part-time worker, University student, Nonworker, "
            "Retired, Student of non-driving age, Student of driving age, Child too young for school)"
        )
    )
    value_of_time: float | None = Field(
        default=None, ge=0, description="Value of time ($2000 per hour)"
    )
    fp_choice: FreeParkingChoice = Field(
        description="Free parking eligibility choice (1=park for free, 2=pay to park)"
    )
    activity_pattern: CTRAMPActivityPattern = Field(
        description="Primary daily activity pattern (M=mandatory, N=non-mandatory, H=home)"
    )
    imf_choice: IMFChoice = Field(
        description=(
            "Individual mandatory tour frequency (1=one work tour, 2=two work tours, 3=one school tour, "
            "4=two school tours, 5=one work tour and one school tour)"
        ),
    )
    inmf_choice: int = Field(ge=0, le=96, description="Individual non-mandatory tour frequency")
    wfh_choice: WFHChoice = Field(
        description="Works from home choice (0=non-worker or workers who don't work from home, 1=workers who work from home) - Added in TM1.6",
    )
    industry_empsix: CTRAMPIndustry | None = Field(
        default=None,
        description="Six-category employment sector (empsix), derived from the canonical industry code",
    )

    # NOTE: Not part of CT-RAMP spec; for survey - model comparative analysis.
    sampleRate: float | None = Field(
        default=None,
        ge=0,
        description="This person represents 1/sampleRate persons, calculate from weights as 1/weight",
    )  # For some reason this wasn't in the original CTRAMP model spec
    person_weight: float | None = Field(
        default=None, ge=0, description="Survey weight for the person (not part of CT-RAMP spec)"
    )

    # For some reason the person type in this model is string not int unlike the other CT-RAMP models
    @field_validator("type", mode="before")
    @classmethod
    def validate_person_type_from_label(cls, v: str | CTRAMPPersonType) -> CTRAMPPersonType:
        """Convert string label to enum member."""
        if isinstance(v, str):
            result = CTRAMPPersonType.from_label(v)
            if result is None:
                msg = f"Invalid person type label: {v}"
                raise ValueError(msg)
            return CTRAMPPersonType(result.value)
        return v


class MandatoryLocationCTRAMPModel(BaseModel):
    """Mandatory tour locations from Travel Model (CT-RAMP format)."""

    HHID: int = Field(description="Unique household ID number")
    HomeTAZ: int = Field(ge=1, le=1454, description="Home transportation analysis zone")
    Income: int | None = Field(default=None, ge=0, description="Annual household income ($2000)")
    PersonID: int = Field(description="Unique person ID number")
    PersonNum: int = Field(ge=1, description="Person number unique to the household")
    PersonType: CTRAMPPersonType = Field(
        description=(
            "Person lifestage/employment type (1=Full-time worker; 2=Part-time worker; 3=University student; "
            "4=Nonworker; 5=Retired; 6=Student of non-driving age; 7=Student of driving age; "
            "8=Child too young for school)"
        ),
    )
    PersonAge: int = Field(ge=0, description="Person age")
    EmploymentCategory: CTRAMPEmploymentCategory = Field(
        description=(
            "Employment category derived from the BATS `employment` field. "
            "See [`format_persons`][processing.formatting.ctramp.format_persons.format_persons] "
            "for the full mapping from BATS employment values to CT-RAMP categories."
        )
    )
    StudentCategory: CTRAMPStudentCategory = Field(
        description='Student category ("College or higher", "Grade or high school", "Not student")'
    )
    WorkLocation: int = Field(
        ge=0,
        le=1454,
        description="Work location transportation analysis zone (1-1454, or 0 if no workplace is selected)",
    )
    SchoolLocation: int = Field(
        ge=0,
        le=1454,
        description="School location transportation analysis zone (1-1454, or 0 if no school is selected)",
    )
    # NOTE: Model output only, not derivable from survey data.
    HomeSubZone: WalkToTransitSubZone | None = Field(
        default=None,
        description="Walk to transit home sub-zone (0=cannot walk to transit; 1=short-walk; 2=long-walk)",
    )
    SchoolSubZone: WalkToTransitSubZone | None = Field(
        default=None,
        description="Walk to transit school sub-zone (0=cannot walk to transit; 1=short-walk; 2=long-walk)",
    )
    WorkSubZone: WalkToTransitSubZone | None = Field(
        default=None,
        description="Walk to transit work sub-zone (0=cannot walk to transit; 1=short-walk; 2=long-walk)",
    )
    work_distance_survey: float | None = Field(
        default=None,
        ge=0,
        description="""
        Survey-reported home to work distance in miles calculated as average traveled distance to work
        (for validation, not part of CT-RAMP spec)""",
    )
    school_distance_survey: float | None = Field(
        default=None,
        ge=0,
        description="""
        Survey-reported home to school distance in miles calculated as average traveled distance to school
        (for validation, not part of CT-RAMP spec)""",
    )


class IndividualTourCTRAMPModel(BaseModel):
    """Individual tours from Travel Model (CT-RAMP format)."""

    hh_id: int = Field(description="Unique household ID number")
    person_id: int = Field(description="Unique person ID number")
    person_num: int = Field(ge=1, description="Person number unique to the household")
    person_type: CTRAMPPersonType = Field(
        description=(
            "Person lifestage/employment type (1=Full-time worker; 2=Part-time worker; 3=University student; "
            "4=Nonworker; 5=Retired; 6=Student of non-driving age; 7=Student of driving age; "
            "8=Child too young for school)"
        ),
    )
    tour_id: int = Field(ge=0, description="Individual tour number unique to the person")
    tour_category: CTRAMPTourCategory = Field(
        description='Type of tour ("MANDATORY", "INDIVIDUAL_NON_MANDATORY", "AT_WORK")'
    )
    tour_purpose: CTRAMPTourPurpose = Field(
        description=(
            'Tour purpose, given the type of tour ("work_low", "work_med", "work_high", "work_very_high", '
            '"university", "school_high", "school_grade", "atwork_business", "atwork_eat", "atwork_maint", '
            '"eatout", "escort_kids", "escort_no_kids", "othdiscr", "othmaint", "shopping", "social")'
        )
    )
    orig_taz: int = Field(ge=1, le=1454, description="Origin transportation analysis zone")
    dest_taz: int = Field(ge=1, le=1454, description="Destination transportation analysis zone")
    start_hour: int = Field(
        ge=0,  # Model is limited to 5-23, but data may have 0-4 values,
        le=23,
        description="Start time of the tour (5=5am-6am, ..., 23=11pm-midnight)",
    )
    end_hour: int = Field(
        ge=0,  # Model is limited to 5-23, but data may have 0-4 values,
        le=23,
        description="End time of the tour (5=5am-6am, ..., 23=11pm-midnight)",
    )
    tour_mode: CTRAMPModeType = Field(
        description="Primary travel mode for the tour (see TravelModes#tour-and-trip-modes)"
    )
    atWork_freq: int = Field(
        ge=0,
        description="Number of at work sub-tours (non-zero only for work tours)",
    )
    num_ob_stops: int = Field(description="Number of out-bound stops on the tour")
    num_ib_stops: int = Field(description="Number of in-bound stops on the tour")
    # NOTE: Derivable from survey weights when available.
    sampleRate: float | None = Field(
        default=None,
        ge=0,
        description="This tour represents 1/sampleRate tours, calculate from weights as 1/weight",
    )
    # NOTE: Model output only, not derivable from survey data.
    avAvailable: int | None = Field(
        default=None, description="Autonomous vehicle available, added in Travel Model 1.5"
    )
    dest_walk_segment: WalkToTransitSubZone | None = Field(
        default=None,
        description="Walk to transit destination sub-zone (0=cannot walk to transit; 1=short-walk; 2=long-walk)",
    )
    orig_walk_segment: WalkToTransitSubZone | None = Field(
        default=None,
        description="Walk to transit origin sub-zone (0=cannot walk to transit; 1=short-walk; 2=long-walk)",
    )
    dcLogsum: float | None = Field(
        default=None, description="To document, added in Travel Model 1.5"
    )
    origTaxiWait: float | None = Field(
        default=None, description="To document, added in Travel Model 1.5"
    )
    destTaxiWait: float | None = Field(
        default=None, description="To document, added in Travel Model 1.5"
    )
    origSingleTNCWait: float | None = Field(
        default=None, description="To document, added in Travel Model 1.5"
    )
    destSingleTNCWait: float | None = Field(
        default=None, description="To document, added in Travel Model 1.5"
    )
    origSharedTNCWait: float | None = Field(
        default=None, description="To document, added in Travel Model 1.5"
    )
    destSharedTNCWait: float | None = Field(
        default=None, description="To document, added in Travel Model 1.5"
    )

    # NOTE: Not part of CT-RAMP spec; for survey - model comparative analysis.
    tour_weight: float | None = Field(
        default=None, ge=0, description="Survey weight for the tour (not part of CT-RAMP spec)"
    )


class IndividualTripCTRAMPModel(BaseModel):
    """Individual trips from Travel Model (CT-RAMP format)."""

    hh_id: int = Field(description="Unique household ID number")
    person_id: int = Field(description="Unique person ID number")
    person_num: int = Field(ge=1, description="Person number unique to the household")
    tour_id: int = Field(
        description=(
            "Individual tour number unique to the person. "
            "0 (1 individual tour), 1, 2, 3, or 4 (5 individual tours), or, for at work sub-tours, "
            "a two-digit integer where the first digit is the individual tour number (1-based) and the second digit is the sub-tour number (e.g. 12, is second sub-tour made on tour number 1, which has a tour_id of 0)."
        )
    )
    stop_id: int = Field(
        description=(
            "Trip number within the half tour, 0-based in order of trips; "
            "-1 if the half-tour leg has no intermediate stop (a single trip)"
        )
    )
    inbound: int = Field(
        ge=0,
        le=1,
        description="Inbound stop indicator (1 if the trip is on the inbound leg of the tour, 0 otherwise)",
    )
    tour_purpose: CTRAMPTourPurpose = Field(
        description=(
            'Tour purpose, given the type of tour ("work_low", "work_med", "work_high", "work_very high", '
            '"university", "school_high", "school_grade", "atwork_business", "atwork_eat", "atwork_maint", '
            '"eatout", "escort_kids", "escort_no kids", "othdiscr", "othmaint", "shopping", "social")'
        )
    )
    orig_purpose: CTRAMPTripPurpose = Field(
        description=(
            'Purpose at the origin end of the trip ("Home", "work", "university", "school", "atwork", '
            '"eatout", "escort", "othdiscr", "othmaint", "shopping", "social")'
        )
    )
    dest_purpose: CTRAMPTripPurpose = Field(
        description=(
            'Purpose at the destination end of the trip ("Home", "work", "university", "school", "atwork", '
            '"eatout", "escort", "othdiscr", "othmaint", "shopping", "social")'
        )
    )
    orig_taz: int = Field(ge=1, le=1454, description="Origin transportation analysis zone")
    dest_taz: int = Field(ge=1, le=1454, description="Destination transportation analysis zone")
    parking_taz: int = Field(
        ge=0,
        le=1454,
        description="Transportation analysis zone in which the trip maker(s) park (0 if no parking zone is selected)",
    )
    depart_hour: int = Field(
        ge=0,  # Model is limited to 5-23, but data may have 0-4 values,
        le=23,
        description="Time of departure for the trip (5=5am-6am, ..., 23=11pm-midnight)",
    )
    trip_mode: CTRAMPModeType = Field(
        description="Travel mode for the trip (see TravelModes#tour-and-trip-modes)"
    )
    tour_mode: CTRAMPModeType = Field(
        description="Primary travel mode for the tour (see TravelModes#tour-and-trip-modes)"
    )
    tour_category: CTRAMPTourCategory = Field(
        description='The type of tour for which this trip is a part ("AT_WORK", "INDIVIDUAL_NON_MANDATORY", "MANDATORY")'
    )
    # NOTE: Derivable from survey weights when available.
    sampleRate: float | None = Field(
        default=None,
        ge=0,
        description="This trip represents 1/sampleRate trips, calculate from weights as 1/weight",
    )
    # NOTE: Model output only, not derivable from survey data.
    avAvailable: int | None = Field(
        default=None,
        description="Does the household have an autonomous vehicle available for this tour?",
    )
    taxiWait: float | None = Field(default=None, description="TBD")
    singleTNCWait: float | None = Field(default=None, description="TBD")
    sharedTNCWait: float | None = Field(default=None, description="TBD")
    orig_walk_segment: WalkToTransitSubZone | None = Field(
        default=None,
        description="Walk to transit origin sub-zone (0=cannot walk to transit; 1=short walk; 2=long walk)",
    )
    dest_walk_segment: WalkToTransitSubZone | None = Field(
        default=None,
        description="Walk to transit destination sub-zone (0=cannot walk to transit; 1=short walk; 2=long walk)",
    )

    # NOTE: Not part of CT-RAMP spec; for survey - model comparative analysis.
    trip_weight: float | None = Field(
        default=None,
        ge=0,
        description="Survey weight for the trip, aka linked_trip_weight (not part of CT-RAMP spec)",
    )
    distance_survey: float | None = Field(
        default=None,
        ge=0,
        description="Survey-reported trip distance in miles (for validation, not part of CT-RAMP spec)",
    )


class JointTourCTRAMPModel(BaseModel):
    """Joint tours from Travel Model (CT-RAMP format)."""

    hh_id: int = Field(description="Unique household ID number")
    tour_id: int = Field(
        ge=0,
        description="Joint tour number unique to the household (0=first joint tour, 1=second, etc.)",
    )
    tour_category: CTRAMPTourCategory = Field(
        description='Type of joint tour ("JOINT_NON_MANDATORY")'
    )
    tour_purpose: CTRAMPTourPurpose = Field(
        description='Purpose of the joint tour ("eatout", "othdiscr", "othmaint", "shopping", "social")'
    )
    tour_composition: TourComposition = Field(
        description="Type of tour composition (1=adults only; 2=children only; 3=adults and children)",
    )
    tour_participants: str = Field(
        description="Household members participating in the tour (space-separated person_num values)"
    )
    orig_taz: int = Field(ge=1, le=1454, description="Origin transportation analysis zone")
    dest_taz: int = Field(ge=1, le=1454, description="Destination transportation analysis zone")
    start_hour: int = Field(
        ge=0,  # Model is limited to 5-23, but data may have 0-4 values,
        le=23,
        description="Start time of the tour (5=5am-6am, ..., 23=11pm-midnight)",
    )
    end_hour: int = Field(
        ge=0,  # Model is limited to 5-23, but data may have 0-4 values,
        le=23,
        description="End time of the tour (5=5am-6am, ..., 23=11pm-midnight)",
    )
    tour_mode: CTRAMPModeType = Field(
        description="Primary travel mode for the tour (see TravelModes#tour-and-trip-modes)"
    )
    num_ob_stops: int = Field(ge=0, description="Number of out-bound (from home) stops on the tour")
    num_ib_stops: int = Field(ge=0, description="Number of in-bound (to home) stops on the tour")
    # NOTE: Derivable from survey weights when available.
    sampleRate: float | None = Field(
        default=None,
        gt=0,
        description="This tour represents 1/sampleRate joint tours, calculate from weights as 1/weight",
    )
    joint_tour_weight: float | None = Field(
        default=None,
        ge=0,
        description="Survey weight for the joint tour (not part of CT-RAMP spec)",
    )
    # NOTE: Model output only, not derivable from survey data.
    orig_walk_segment: WalkToTransitSubZone | None = Field(
        default=None,
        description="Walk to transit origin sub-zone (0=cannot walk to transit; 1=short-walk; 2=long-walk)",
    )
    dest_walk_segment: WalkToTransitSubZone | None = Field(
        default=None,
        description="Walk to transit destination sub-zone (0=cannot walk to transit; 1=short-walk; 2=long-walk)",
    )


class JointTripCTRAMPModel(BaseModel):
    """Joint trip results from Travel Model (CT-RAMP format)."""

    hh_id: int = Field(description="Unique household ID number")
    tour_id: int = Field(
        ge=0,
        description="Joint tour number unique to the household (0=first joint tour, 1=second, etc.)",
    )
    stop_id: int = Field(
        description=(
            "Trip number within the half tour, 0-based in order of trips; "
            "-1 if the half-tour leg has no intermediate stop (a single trip)"
        )
    )
    inbound: int = Field(
        ge=0,
        le=1,
        description="Inbound stop indicator (1 if the stop is on the inbound leg of the tour, 0 otherwise)",
    )
    tour_purpose: CTRAMPTourPurpose = Field(
        description='Purpose of the joint tour ("eatout", "othdiscr", "othmaint", "shopping", "social")'
    )
    orig_purpose: CTRAMPTripPurpose = Field(
        description='Purpose at the origin end of the trip ("Home", "eatout", "othdiscr", "othmaint", "shopping", "social")'
    )
    dest_purpose: CTRAMPTripPurpose = Field(
        description='Purpose at the destination end of the trip ("Home", "eatout", "othdiscr", "othmaint", "shopping", "social")'
    )
    orig_taz: int = Field(ge=1, le=1454, description="Origin transportation analysis zone")
    dest_taz: int = Field(ge=1, le=1454, description="Destination transportation analysis zone")
    parking_taz: int = Field(
        ge=0,
        le=1454,
        description="Transportation analysis zone in which the trip maker(s) park (0 if no parking zone is selected)",
    )
    depart_hour: int = Field(
        ge=0,  # Model is limited to 5-23, but data may have 0-4 values,
        le=23,
        description="Time of departure for the trip (5=5am-6am, ..., 23=11pm-midnight)",
    )
    trip_mode: CTRAMPModeType = Field(
        description="Travel mode for the trip (see TravelModes#tour-and-trip-modes)"
    )
    num_participants: int = Field(ge=2, description="Number of participants on the tour")
    tour_mode: CTRAMPModeType = Field(
        description="Primary travel mode for the tour (see TravelModes#tour-and-trip-modes)"
    )
    tour_category: CTRAMPTourCategory = Field(description='Tour category ("JOINT_NON_MANDATORY")')
    # NOTE: Derivable from survey weights when available.
    sampleRate: float | None = Field(
        default=None,
        gt=0,
        description="This trip represents 1/sampleRate joint trips, calculate from weights as 1/weight",
    )
    joint_trip_weight: float | None = Field(
        default=None,
        ge=0,
        description="Survey weight for the joint trip (not part of CT-RAMP spec)",
    )
    # NOTE: Model output only, not derivable from survey data.
    orig_walk_segment: WalkToTransitSubZone | None = Field(
        default=None,
        description="Walk to transit origin sub-zone (0=cannot walk to transit; 1=short-walk; 2=long-walk)",
    )
    dest_walk_segment: WalkToTransitSubZone | None = Field(
        default=None,
        description="Walk to transit destination sub-zone (0=cannot walk to transit; 1=short-walk; 2=long-walk)",
    )


class CDAPResultsCTRAMPModel(BaseModel):
    """CDAP (Coordinated Daily Activity Pattern) results in CT-RAMP format.

    Matches the schema of ``cdapResults.csv`` written by the CT-RAMP Java model.
    """

    HHID: int = Field(description="Unique household ID number")
    PersonID: int = Field(description="Unique person ID number")
    PersonNum: int = Field(ge=1, description="Person number unique to the household")
    PersonType: int = Field(ge=1, le=8, description="Person type code (1-8)")
    ActivityString: CTRAMPActivityPattern = Field(
        description="Daily activity pattern (M=mandatory, N=non-mandatory, H=home)"
    )
    person_weight: float | None = Field(
        default=None, ge=0, description="Survey weight for the person (not part of CT-RAMP spec)"
    )


class AOResultsCTRAMPModel(BaseModel):
    """Auto Ownership results in CT-RAMP format.

    Matches the schema of ``aoResults.csv`` written by the CT-RAMP Java model.
    """

    HHID: int = Field(description="Unique household ID number")
    AO: int = Field(ge=0, description="Number of vehicles owned by the household")
