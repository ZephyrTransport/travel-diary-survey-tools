"""Data models for DaySim file formats.

Based on https://github.com/RSGInc/DaySim/wiki/docs/Daysim%20Input%20Data%20File%20Documentation.docx
"""

from pydantic import BaseModel, Field

from data_canon.codebook.daysim import (
    DaysimDriverPassenger,
    DaysimGender,
    DaysimMode,
    DaysimPathType,
    DaysimPersonType,
    DaysimPurpose,
    DaysimResidenceOwnership,
    DaysimResidenceType,
    DaysimWorkerType,
)


class HouseholdDaysimModel(BaseModel):
    """Household File Format for DaySim."""

    hhno: int = Field(ge=1, description="The household ID number")
    hhsize: int = Field(ge=1, le=99, description="The number of persons in the household")
    hhvehs: int = Field(ge=0, le=99, description="The number of autos in the household")
    hhwkrs: int = Field(ge=0, le=99, description="The number of workers in the household")
    hhftw: int = Field(
        ge=0,
        le=99,
        description="The number of HH members with person type=full-time worker",
    )
    hhptw: int = Field(
        ge=0,
        le=99,
        description="The number of HH members with person type=part-time worker",
    )
    hhret: int = Field(
        ge=0,
        le=99,
        description="The number of HH members with person type=retired adult",
    )
    hhoad: int = Field(
        ge=0,
        le=99,
        description="The number of HH members with person type=other non-working adult",
    )
    hhuni: int = Field(
        ge=0,
        le=99,
        description="The number of HH members with person type=university student",
    )
    hhhsc: int = Field(
        ge=0,
        le=99,
        description="The number of HH members with person type=grade school student age 16+",
    )
    hh515: int = Field(
        ge=0,
        le=99,
        description="The number of HH members with person type=child age 5-15",
    )
    hhcu5: int = Field(
        ge=0,
        le=99,
        description="The number of HH members with person type=child age 0-4",
    )
    hhincome: int = Field(
        ge=-1,
        le=9999999,
        description="The household annual income, in integer dollars",
    )
    hownrent: DaysimResidenceOwnership = Field(description="Household own versus rent status")
    hrestype: DaysimResidenceType = Field(description="Household residence building type")
    hhxco: float = Field(ge=-9999, le=9999, description="Household residence X coordinate")
    hhyco: float = Field(ge=-9999, le=9999, description="Household residence Y coordinate")
    hhparcel: int = Field(
        ge=1,
        le=9999999,
        description="The ID of the parcel on which the household lives",
    )
    hhtaz: int = Field(
        ge=1,
        le=9999999,
        description="The ID of the zone in which the household lives",
    )
    hhexpfac: float = Field(ge=0, description="The expansion factor for the household")
    samptype: int = Field(ge=0, le=99, description="The type of sample used")


class PersonDaysimModel(BaseModel):
    """Person File Format for DaySim."""

    hhno: int = Field(ge=1, description="The household ID number")
    pno: int = Field(
        ge=1,
        le=99,
        description="The person sequence number within the household",
    )
    pptyp: DaysimPersonType = Field(description="Person type")
    pagey: int = Field(ge=0, le=99, description="Age in years")
    pgend: DaysimGender = Field(description="Gender")
    pwtyp: DaysimWorkerType = Field(description="Worker type")
    pwpcl: int = Field(ge=-1, description="Usual work location parcel ID")
    pwtaz: int = Field(ge=-1, description="Usual work location zone ID")
    pwautime: float = Field(
        ge=-1,
        description="The 1-way peak auto travel time between residence and work",
    )
    pwaudist: float = Field(
        ge=-1,
        description="The 1-way peak auto travel distance between residence and work",
    )
    pstyp: int = Field(ge=-1, le=2, description="Student type")
    pspcl: int = Field(ge=-1, description="Usual school location parcel ID")
    pstaz: int = Field(ge=-1, description="Usual school location zone ID")
    psautime: float = Field(
        ge=-1,
        description="The 1-way peak auto travel time between residence and school",
    )
    psaudist: float = Field(
        ge=-1,
        description="The 1-way peak auto travel distance between residence and school",
    )
    puwmode: DaysimMode = Field(description="The usual mode used to work")
    puwarrp: int = Field(ge=-1, le=9, description="The usual arrival period at work")
    puwdepp: int = Field(ge=-1, le=9, description="The usual departure period from work")
    ptpass: int = Field(ge=0, le=1, description="Transit pass ownership")
    ppaidprk: int = Field(ge=-1, le=1, description="Worker has to pay to park at work")
    pdiary: int = Field(ge=0, le=1, description="Survey respondent used their diary")
    pproxy: int = Field(ge=0, le=1, description="Survey responses by proxy")
    psxco: float | None = Field(ge=-9999, le=9999, description="Person's school X coordinate")
    psyco: float | None = Field(ge=-9999, le=9999, description="Person's school Y coordinate")
    pwxco: float | None = Field(ge=-9999, le=9999, description="Person's work X coordinate")
    pwyco: float | None = Field(ge=-9999, le=9999, description="Person's work Y coordinate")
    psexpfac: float = Field(ge=0, description="The expansion factor for the person")


class HouseholdDayDaysimModel(BaseModel):
    """HouseholdDay File Format for DaySim."""

    hhno: int = Field(ge=1, description="The household ID number")
    day: int = Field(ge=1, le=99, description="The survey day sequence")
    dow: int = Field(ge=1, le=7, description="The day of the week")
    jttours: int = Field(
        ge=0,
        le=99,
        description="The number of fully joint tour records output for the household",
    )
    phtours: int = Field(
        ge=0,
        le=99,
        description="The number of partially joint half tour records output",
    )
    fhtours: int = Field(
        ge=0,
        le=99,
        description="The number of fully joint half tour records output",
    )
    hdexpfac: float = Field(ge=0, description="The expansion factor for the household-day")


class PersonDayDaysimModel(BaseModel):
    """PersonDay File Format for DaySim."""

    hhno: int = Field(ge=1, description="The household ID number")
    pno: int = Field(
        ge=1,
        le=99,
        description="The person sequence number within the household",
    )
    day: int = Field(ge=1, le=99, description="The survey day sequence")
    beghom: int = Field(ge=0, le=1, description="A flag if the survey diary day begins at home")
    endhom: int = Field(ge=0, le=1, description="A flag if the survey diary day ends at home")
    hbtours: int = Field(
        ge=0,
        le=99,
        description="The total number of home-based tour records predicted",
    )
    wbtours: int = Field(
        ge=0,
        le=99,
        description="The total number of work-based subtour records predicted",
    )
    uwtours: int = Field(
        ge=0,
        le=99,
        description="The total number of home-based work tours to usual workplace",
    )
    wktours: int = Field(ge=0, le=99, description="The number of home-based work tours predicted")
    sctours: int = Field(
        ge=0,
        le=99,
        description="The number of home-based school tours predicted",
    )
    estours: int = Field(
        ge=0,
        le=99,
        description="The number of home-based escort tours predicted",
    )
    pbtours: int = Field(
        ge=0,
        le=99,
        description="The number of home-based personal business tours predicted",
    )
    shtours: int = Field(
        ge=0,
        le=99,
        description="The number of home-based shopping tours predicted",
    )
    mltours: int = Field(ge=0, le=99, description="The number of home-based meal tours predicted")
    sotours: int = Field(
        ge=0,
        le=99,
        description="The number of home-based social tours predicted",
    )
    retours: int = Field(
        ge=0,
        le=99,
        description="The number of home-based recreation tours predicted",
    )
    metours: int = Field(
        ge=0,
        le=99,
        description="The number of home-based medical tours predicted",
    )
    wkstops: int = Field(ge=0, le=99, description="The number of home-based work stops predicted")
    scstops: int = Field(
        ge=0,
        le=99,
        description="The number of home-based school stops predicted",
    )
    esstops: int = Field(
        ge=0,
        le=99,
        description="The number of home-based escort stops predicted",
    )
    pbstops: int = Field(
        ge=0,
        le=99,
        description="The number of home-based personal business stops predicted",
    )
    shstops: int = Field(
        ge=0,
        le=99,
        description="The number of home-based shopping stops predicted",
    )
    mlstops: int = Field(ge=0, le=99, description="The number of home-based meal stops predicted")
    sostops: int = Field(
        ge=0,
        le=99,
        description="The number of home-based social stops predicted",
    )
    restops: int = Field(
        ge=0,
        le=99,
        description="The number of home-based recreation stops predicted",
    )
    mestops: int = Field(
        ge=0,
        le=99,
        description="The number of home-based medical stops predicted",
    )
    wkathome: int = Field(
        ge=0,
        le=1439,
        description="The number of minutes spent working at home during the day",
    )
    pwxco: float | None = Field(
        ge=-9999, le=9999, description="Person's work location X coordinate"
    )
    pwyco: float | None = Field(
        ge=-9999, le=9999, description="Person's work location Y coordinate"
    )
    psxco: float | None = Field(
        ge=-9999, le=9999, description="Person's school location X coordinate"
    )
    psyco: float | None = Field(
        ge=-9999, le=9999, description="Person's school location Y coordinate"
    )
    pdexpfac: float = Field(ge=0, description="The expansion factor for the person-day")


class TourDaysimModel(BaseModel):
    """Tour File Format for DaySim."""

    hhno: int = Field(ge=1, description="The household ID number")
    pno: int = Field(
        ge=1,
        le=99,
        description="The person sequence number within the household",
    )
    day: int = Field(ge=1, le=99, description="The survey day sequence")
    tour: int = Field(ge=1, le=99, description="The tour sequence within the person-day")
    jtindex: int = Field(
        ge=0,
        le=99,
        description="Links to the sequence number of the tour in the JointTour file",
    )
    parent: int = Field(
        ge=0,
        le=99,
        description="The tour sequence number of the parent work tour",
    )
    subtrs: int = Field(
        ge=0,
        le=99,
        description="The number of work-based subtours made from the work activity",
    )
    pdpurp: DaysimPurpose = Field(description="The tour primary destination purpose")
    tlvorig: int = Field(
        ge=0,
        le=1439,
        description="The time leaving the tour origin, in minutes after midnight",
    )
    tardest: int = Field(
        ge=0,
        le=1439,
        description="The time arriving at the tour destination, in minutes after midnight",
    )
    tlvdest: int = Field(
        ge=0,
        le=1439,
        description="The time leaving the tour destination, in minutes after midnight",
    )
    tarorig: int = Field(
        ge=0,
        le=1439,
        description="The time arriving back at the tour origin, in minutes after midnight",
    )
    toadtyp: int = Field(ge=1, le=5, description="Tour origin address type")
    tdadtyp: int = Field(ge=1, le=5, description="Tour destination address type")
    topcl: int | None = Field(ge=-1, description="Tour origin parcel ID")
    totaz: int | None = Field(ge=-1, description="Tour origin zone ID")
    tdpcl: int | None = Field(ge=-1, description="Tour destination parcel ID")
    tdtaz: int | None = Field(ge=-1, description="Tour destination zone ID")
    tmodetp: DaysimMode = Field(description="Tour main mode type")
    tpathtp: DaysimPathType = Field(description="Tour main mode path type")
    tautotime: float = Field(ge=-1, description="The one-way auto travel time")
    tautocost: float = Field(ge=-1, description="The one-way auto toll cost")
    tautodist: float = Field(ge=-1, description="The one-way auto travel distance")
    tripsh1: int = Field(
        ge=1,
        le=99,
        description="The number of trips segments on the half tour to the destination",
    )
    tripsh2: int = Field(
        ge=1,
        le=99,
        description="The number of trips segments on the half tour from the destination",
    )
    phtindx1: int = Field(
        ge=0,
        le=99,
        description="Links to the sequence number of the first half tour in partial half tour",
    )
    phtindx2: int = Field(
        ge=0,
        le=99,
        description="Links to the sequence number of the second half tour in partial half tour",
    )
    fhtindx1: int = Field(
        ge=0,
        le=99,
        description="Links to the sequence number of the first half tour in full half tour",
    )
    fhtindx2: int = Field(
        ge=0,
        le=99,
        description="Links to the sequence number of the second half tour in full half tour",
    )
    toexpfac: float = Field(ge=0, description="The expansion factor for the tour")


class LinkedTripDaysimModel(BaseModel):
    """Trip File Format for DaySim."""

    hhno: int = Field(ge=1, description="The household ID number")
    pno: int = Field(
        ge=1,
        le=99,
        description="The person sequence number within the household",
    )
    day: int = Field(ge=1, le=99, description="The survey day sequence")
    tour: int = Field(ge=1, le=99, description="The tour sequence within the person-day")
    half: int = Field(ge=1, le=2, description="The half tour")
    tseg: int = Field(ge=1, le=99, description="The trip sequence number within the half tour")
    tsvid: int = Field(ge=1, le=99, description="Links to a travel survey trip ID")
    opurp: DaysimPurpose = Field(description="The purpose at the trip origin")
    dpurp: DaysimPurpose = Field(description="The purpose at the trip destination")
    oadtyp: int = Field(ge=1, le=6, description="Trip origin address type")
    dadtyp: int = Field(ge=1, le=6, description="Trip destination address type")
    opcl: int | None = Field(ge=-1, description="Trip origin parcel ID")
    otaz: int | None = Field(ge=-1, description="Trip origin zone ID")
    oxco: float = Field(ge=-9999, le=9999, description="Trip origin X coordinate")
    oyco: float = Field(ge=-9999, le=9999, description="Trip origin Y coordinate")
    dpcl: int | None = Field(ge=-1, description="Trip destination parcel ID")
    dtaz: int | None = Field(ge=-1, description="Trip destination zone ID")
    dxco: float = Field(ge=-9999, le=9999, description="Trip destination X coordinate")
    dyco: float = Field(ge=-9999, le=9999, description="Trip destination Y coordinate")
    mode: DaysimMode = Field(description="Trip mode")
    pathtype: DaysimPathType = Field(description="Trip path type")
    dorp: DaysimDriverPassenger = Field(
        description="Driver/passenger for auto trips, walk time for transit trips"
    )
    deptm: int = Field(
        ge=0,
        le=1439,
        description="The trip departure time, in minutes after midnight",
    )
    arrtm: int = Field(
        ge=0,
        le=1439,
        description="The trip arrival time, in minutes after midnight",
    )
    endacttm: int = Field(
        ge=0,
        le=1439,
        description="The end time of the destination activity, in minutes after midnight",
    )
    travtime: float = Field(ge=-1, description="The travel time by the trip mode and path type")
    travcost: float = Field(ge=-1, description="The travel cost by the trip mode and path type")
    travdist: float = Field(
        ge=-1,
        description="The network distance between the trip origin and destination",
    )
    trexpfac: float = Field(ge=0, description="The expansion factor for the trip")
