"""Synthetic survey for the e2e run: 33 households, each built to exercise particular outputs.

Returns Polars DataFrames and does no file I/O. All coordinates are synthetic. The
``# HH n`` comments in ``_build_records`` say what each household is for.
"""

import math
from datetime import date, datetime, time, timedelta

import polars as pl

from data_canon.codebook.households import IncomeBroad, ResidenceRentOwn, ResidenceType
from data_canon.codebook.persons import (
    AgeCategory,
    CommuteFreq,
    Employment,
    Ethnicity,
    Gender,
    JobType,
    Race,
    SchoolType,
    Student,
    SurveyTech,
    WorkParking,
)
from data_canon.codebook.trips import Driver, Mode, ModeType, Purpose, PurposeCategory
from processing.habitual_locations import reported_habitual_locations
from utils.create_ids import create_household_days

COORDS = {
    "home_a": (37.7750, -122.4180),
    "home_b": (37.7620, -122.4350),
    "home_c": (37.7480, -122.4100),
    "home_d": (37.7830, -122.3950),
    "home_e": (37.7390, -122.4000),
    "home_f": (37.7910, -122.3870),
    "home_g": (37.7150, -122.4500),
    "home_h": (37.7500, -122.4420),
    "home_i": (37.7680, -122.3800),
    "home_j": (37.7250, -122.4700),
    "home_k": (37.7550, -122.4050),
    "home_l": (37.7350, -122.4250),
    "home_m": (37.7450, -122.4300),
    "home_n": (37.7780, -122.4350),
    "home_o": (37.7300, -122.4150),
    # Far outside the zone polygon, so the zone join leaves these without a zone.
    "home_remote": (38.5800, -121.4900),
    "work_remote": (38.5650, -121.4700),
    "work_1": (37.7900, -122.3960),
    "work_2": (37.7850, -122.4010),
    "work_3": (37.7600, -122.3890),
    "school_1": (37.7700, -122.4200),
    "shop_1": (37.7650, -122.4260),
    "shop_2": (37.7710, -122.4150),
    "meal_1": (37.7680, -122.4300),
    "errand_1": (37.7720, -122.4130),
    "social_1": (37.7580, -122.4400),
    "bart_station": (37.7840, -122.4080),
    "bart_dest": (37.7950, -122.3930),
    "lunch_1": (37.7870, -122.3980),
    "univ_1": (37.7620, -122.4180),
    "coffee_1": (37.7760, -122.4090),
    "gym_1": (37.7690, -122.4230),
    "pr_1": (37.7810, -122.4110),
    "home_p": (37.7420, -122.4120),
    "home_q": (37.7660, -122.4480),
    "home_r": (37.7190, -122.4600),
    "home_s": (37.7520, -122.4560),
    "home_t": (37.7390, -122.4380),
}

# One zone polygon covering every location except the remote two.
ZONE_GEOJSON = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {"fipco": "001", "TAZ_NODE": 1, "MAZ_NODE": 1},
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-122.55, 37.70],
                        [-122.55, 37.82],
                        [-122.35, 37.82],
                        [-122.35, 37.70],
                        [-122.55, 37.70],
                    ]
                ],
            },
        }
    ],
}

# A week diary runs Monday to Sunday, so a profile limiting the days of the week
# drops some of each household's days rather than whole households.
WEEK = [date(2024, 3, 11) + timedelta(days=n) for n in range(7)]
# For the households that test two particular dates; both are inside Tuesday-Thursday.
DAY_DATE_1 = date(2024, 3, 12)  # Tuesday
DAY_DATE_2 = date(2024, 3, 13)  # Wednesday

# Codebook values under short names, read from the enums so a renumbered code shows.
FEMALE, MALE, GENDER_MISSING = Gender.FEMALE.value, Gender.MALE.value, Gender.MISSING.value
AGE_UNDER_5, AGE_5_TO_15 = AgeCategory.AGE_UNDER_5.value, AgeCategory.AGE_5_TO_15.value
AGE_16_TO_17, AGE_18_TO_24 = AgeCategory.AGE_16_TO_17.value, AgeCategory.AGE_18_TO_24.value
AGE_25_TO_34, AGE_35_TO_44 = AgeCategory.AGE_25_TO_34.value, AgeCategory.AGE_35_TO_44.value
AGE_45_TO_54, AGE_55_TO_64 = AgeCategory.AGE_45_TO_54.value, AgeCategory.AGE_55_TO_64.value
AGE_65_TO_74, AGE_75_TO_84 = AgeCategory.AGE_65_TO_74.value, AgeCategory.AGE_75_TO_84.value
EMP_FULLTIME, EMP_PARTTIME = Employment.EMPLOYED_FULLTIME.value, Employment.EMPLOYED_PARTTIME.value
EMP_SELF, EMP_NOT_LOOKING = Employment.EMPLOYED_SELF.value, Employment.UNEMPLOYED_NOT_LOOKING.value
STU_FULLTIME, STU_PARTTIME = Student.FULLTIME_INPERSON.value, Student.PARTTIME_INPERSON.value
STU_NONSTUDENT = Student.NONSTUDENT.value
INC_UNDER_25K, INC_25_50K = IncomeBroad.INCOME_UNDER25.value, IncomeBroad.INCOME_25TO50.value
INC_50_75K, INC_75_100K = IncomeBroad.INCOME_50TO75.value, IncomeBroad.INCOME_75TO100.value
INC_100_200K, INC_200_PLUS = IncomeBroad.INCOME_100TO200.value, IncomeBroad.INCOME_200_OR_MORE.value
INC_MISSING = IncomeBroad.MISSING.value
RES_SFH, RES_TOWNHOUSE = ResidenceType.SFH.value, ResidenceType.TOWNHOUSE.value
RES_MULTIFAMILY, RES_CONDO_5_50 = (
    ResidenceType.MULTIFAMILY.value,
    ResidenceType.CONDO_5TO50_UNITS.value,
)
RES_MISSING = ResidenceType.MISSING.value
OWN, RENT = ResidenceRentOwn.OWN.value, ResidenceRentOwn.RENT.value
RENT_OWN_MISSING = ResidenceRentOwn.MISSING.value
PC_HOME, PC_WORK = PurposeCategory.HOME.value, PurposeCategory.WORK.value
PC_SCHOOL, PC_ESCORT = PurposeCategory.SCHOOL.value, PurposeCategory.ESCORT.value
PC_SHOP, PC_MEAL = PurposeCategory.SHOP.value, PurposeCategory.MEAL.value
PC_SOCIALREC, PC_ERRAND = PurposeCategory.SOCIALREC.value, PurposeCategory.ERRAND.value
PC_CHANGE_MODE = PurposeCategory.CHANGE_MODE.value
PC_WORK_RELATED = PurposeCategory.WORK_RELATED.value
PURP_HOME, PURP_WORK = Purpose.HOME.value, Purpose.WORK_VOLUNTEER.value
MT_WALK, MT_BIKE = ModeType.WALK.value, ModeType.BIKE.value
MT_BIKESHARE, MT_TAXI = ModeType.BIKESHARE.value, ModeType.TAXI.value
MT_TNC, MT_CAR = ModeType.TNC.value, ModeType.CAR.value
MT_SCHOOL_BUS, MT_TRANSIT = ModeType.SCHOOL_BUS.value, ModeType.TRANSIT.value
MODE_WALK, MODE_BIKE_RENTED, MODE_BART = Mode.WALK.value, Mode.BIKE_RENTED.value, Mode.BART.value
WP_FREE, WP_NOT_APPLICABLE = WorkParking.FREE.value, WorkParking.NOT_APPLICABLE.value
WP_MISSING = WorkParking.MISSING.value
YES, NO = 1, 0
TECH_GPS, TECH_BROWSER = SurveyTech.GPS.value, SurveyTech.BROWSER.value
DRV_DRIVER, DRV_PASSENGER = Driver.DRIVER.value, Driver.PASSENGER.value
DRV_MISSING = Driver.MISSING.value
JOB_FIXED, JOB_VARIES = JobType.FIXED.value, JobType.VARIES.value
SCH_PRESCHOOL, SCH_ELEMENTARY = SchoolType.PRESCHOOL.value, SchoolType.ELEMENTARY.value
SCH_HIGH_SCHOOL, SCH_4YEAR = SchoolType.HIGH_SCHOOL.value, SchoolType.COLLEGE_4YEAR.value
RACE_AFAM, RACE_ASIAN, RACE_WHITE = Race.AFAM.value, Race.ASIAN.value, Race.WHITE.value
RACE_OTHER, RACE_PNTA = Race.OTHER.value, Race.PNTA.value
ETH_NOT_HISPANIC, ETH_MEXICAN = Ethnicity.NOT_HISPANIC.value, Ethnicity.MEXICAN.value
ETH_MISSING = Ethnicity.MISSING.value
CF_5_DAYS, CF_NEVER = CommuteFreq.DAYS_5.value, CommuteFreq.NEVER.value

# Personas: the person fields a kind of person shares; keywords to `person` override them.
EMPLOYED = {"emp": EMP_FULLTIME, "stu": STU_NONSTUDENT}
WORKER = {**EMPLOYED, "jt": JOB_FIXED, "cf": CF_5_DAYS}
CAR_COMMUTER = {**WORKER, "wp": WP_FREE, "wm": MT_CAR}
NONWORKER = {"emp": EMP_NOT_LOOKING, "stu": STU_NONSTUDENT, "cf": CF_NEVER}
STUDENT = {"emp": EMP_NOT_LOOKING, "stu": STU_FULLTIME}
CHILD = {**STUDENT, "sloc": "school_1", "st": SCH_ELEMENTARY, "proxy": YES}


def _meters(lat1, lon1, lat2, lon2):
    earth_radius_m = 6_371_000
    dlat, dlon = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    return 2 * earth_radius_m * math.asin(math.sqrt(a))


def _span(d, span):
    """Resolve ``"8:00-8:30"`` on date ``d`` to (depart, arrive) datetimes."""
    return [datetime.combine(d, time.fromisoformat(t.zfill(5))) for t in span.split("-")]


class _Day:
    """Trip context bound to a single person-day."""

    def __init__(self, survey, person_day_id, pid, hid, dd):
        self._s = survey
        self.person_day_id = person_day_id
        self.pid = pid
        self.hid = hid
        self.dd = dd

    def trip(self, o, d, opc, dpc, mode, span, *, m1=None, ntrav=1, drv=None):
        """Append one unlinked trip over ``span`` (``"8:00-8:30"``)."""
        self._s._tc += 1
        olat, olon = COORDS[o]
        dlat, dlon = COORDS[d]
        if drv is None:
            drv = DRV_DRIVER if mode == MT_CAR else DRV_PASSENGER if mode == MT_TNC else DRV_MISSING
        dep_dt, arr_dt = _span(self.dd, span)
        self._s.trips.append(
            {
                "unlinked_trip_id": self._s._tc,
                "person_day_id": self.person_day_id,
                "person_id": self.pid,
                "hh_id": self.hid,
                "linked_trip_id": self._s._tc,
                "o_lat": olat,
                "o_lon": olon,
                "d_lat": dlat,
                "d_lon": dlon,
                "o_purpose": PURP_HOME,
                "d_purpose": PURP_WORK,
                "o_purpose_category": opc,
                "d_purpose_category": dpc,
                "mode_type": mode,
                "mode_1": m1 if m1 is not None else mode,
                "mode_2": None,
                "mode_3": None,
                "mode_4": None,
                "transit_access": None,
                "transit_egress": None,
                "duration_minutes": round((arr_dt - dep_dt).total_seconds() / 60, 1),
                "distance_meters": round(_meters(olat, olon, dlat, dlon), 1),
                "depart_time": dep_dt,
                "arrive_time": arr_dt,
                "travel_dow": dep_dt.weekday() + 1,  # 1=Mon … 7=Sun
                "driver": drv,
                "num_travelers": ntrav,
                "survey_complete": True,
                "unlinked_trip_weight": None,
            }
        )
        return self

    def round_trip(self, home, b, bpc, mode, out, back, **kw):
        """Home to ``b`` over span ``out``, and back over span ``back``."""
        self.trip(home, b, PC_HOME, bpc, mode, out, **kw)
        self.trip(b, home, bpc, PC_HOME, mode, back, **kw)
        return self


class _Week:
    """Trip context for a week diary: each trip goes on every day of it."""

    def __init__(self, days):
        self.days = days

    def trip(self, *args, **kw):
        for day in self.days:
            day.trip(*args, **kw)
        return self

    def round_trip(self, *args, **kw):
        for day in self.days:
            day.round_trip(*args, **kw)
        return self


class _Survey:
    """Accumulates households, persons, days, and trips as lists of dicts."""

    def __init__(self):
        self.hhs, self.pers, self.person_days, self.trips = [], [], [], []
        self._tc = 0

    def household(
        self,
        hid,
        home,
        *,
        rent=RENT,
        res=RES_MULTIFAMILY,
        income=INC_50_75K,
        veh=1,
        npeople=1,
        nworkers=1,
        survey_complete=True,
    ):
        self.hhs.append(
            {
                "hh_id": hid,
                "home_lat": COORDS[home][0],
                "home_lon": COORDS[home][1],
                "residence_rent_own": rent,
                "residence_type": res,
                "income_bin": income,
                "num_vehicles": veh,
                "num_people": npeople,
                "num_workers": nworkers,
                "survey_complete": survey_complete,
                "hh_weight": None,
            }
        )

    def person(self, pid, hid, pn, age, gen, persona, **fields):
        """Append a person from a persona; keywords override its fields."""
        self._person(pid, hid, pn, age, gen, **{**persona, **fields})

    def _person(
        self,
        pid,
        hid,
        pn,
        age,
        gen,
        *,
        emp,
        stu,
        wloc=None,
        sloc=None,
        jt=None,
        st=None,
        wp=WP_NOT_APPLICABLE,
        wm=None,
        race=RACE_WHITE,
        eth=ETH_NOT_HISPANIC,
        cf=None,
        tp=NO,
        proxy=NO,
        ndays=1,
        comp=True,
        tech=TECH_GPS,
        excl=None,
    ):
        self.pers.append(
            {
                "person_id": pid,
                "hh_id": hid,
                "person_num": pn,
                "age": age,
                "gender": gen,
                "employment": emp,
                "student": stu,
                "work_lat": COORDS[wloc][0] if wloc else None,
                "work_lon": COORDS[wloc][1] if wloc else None,
                "school_lat": COORDS[sloc][0] if sloc else None,
                "school_lon": COORDS[sloc][1] if sloc else None,
                "job_type": jt,
                "school_type": st,
                "work_park": wp,
                "work_mode": wm,
                "race": race,
                "ethnicity": eth,
                "telework_freq": None,
                "commute_freq": cf,
                "education": None,
                "commute_subsidy_provide_free_parking": NO,
                "commute_subsidy_provide_discounted_parking": NO,
                "commute_subsidy_use_free_parking": NO,
                "commute_subsidy_use_discounted_parking": NO,
                "transit_pass": tp,
                "is_proxy": proxy,
                "survey_tech": tech,
                "exclusion_reason": excl,
                "num_days_complete": ndays,
                "survey_complete": comp,
                "person_weight": None,
            }
        )

    def day(self, pid, hid, dd, *, pnum=1, dnum=1, comp=True) -> _Day:
        """Append a person-day (person_day_id = pid*100 + dnum) and return its trip context."""
        person_day_id = pid * 100 + dnum
        self.person_days.append(
            {
                "person_day_id": person_day_id,
                "person_id": pid,
                "hh_id": hid,
                "travel_date": datetime(dd.year, dd.month, dd.day),
                "travel_dow": dd.isoweekday(),  # 1=Mon … 7=Sun
                "person_num": pnum,
                "day_num": dnum,
                "survey_complete": comp,
                # Not asked in the toy survey.
                "begin_day": None,
                "end_day": None,
                "person_day_weight": None,
            }
        )
        return _Day(self, person_day_id, pid, hid, dd)

    def week(self, pid, hid, *, pnum=1, comp=True) -> _Week:
        """Append a person-day on each day of ``WEEK`` and return them as one trip context."""
        days = [
            self.day(pid, hid, dd, pnum=pnum, dnum=dnum, comp=comp)
            for dnum, dd in enumerate(WEEK, start=1)
        ]
        if comp:
            person = next(p for p in self.pers if p["person_id"] == pid)
            person["num_days_complete"] = len(days)
        return _Week(days)


def _build_records():
    """Build all canonical records. Returns (hh, per, day, trip) as lists of dicts."""
    s = _Survey()

    # HH 1 - simple car commuter
    s.household(1, "home_a", rent=OWN, res=RES_SFH, income=INC_75_100K)
    s.person(101, 1, 1, AGE_35_TO_44, MALE, CAR_COMMUTER, wloc="work_1")
    s.week(101, 1).round_trip("home_a", "work_1", PC_WORK, MT_CAR, "8:00-8:30", "17:00-17:30")

    # HH 2 - transit with mode change (4 unlinked -> 2 linked)
    s.household(2, "home_b", res=RES_CONDO_5_50, income=INC_100_200K, veh=0)
    s.person(
        201,
        2,
        1,
        AGE_25_TO_34,
        FEMALE,
        WORKER,
        wloc="bart_dest",
        wm=MT_TRANSIT,
        race=RACE_ASIAN,
        tp=YES,
    )
    d = s.week(201, 2)
    d.trip("home_b", "bart_station", PC_HOME, PC_CHANGE_MODE, MT_WALK, "7:30-7:45", m1=MODE_WALK)
    d.trip(
        "bart_station", "bart_dest", PC_CHANGE_MODE, PC_WORK, MT_TRANSIT, "7:45-8:15", m1=MODE_BART
    )
    d.trip(
        "bart_dest",
        "bart_station",
        PC_WORK,
        PC_CHANGE_MODE,
        MT_TRANSIT,
        "17:00-17:30",
        m1=MODE_BART,
    )
    d.trip("bart_station", "home_b", PC_CHANGE_MODE, PC_HOME, MT_WALK, "17:30-17:45", m1=MODE_WALK)

    # HH 3 - joint trip household (2 persons)
    s.household(
        3, "home_c", rent=OWN, res=RES_SFH, income=INC_100_200K, veh=2, npeople=2, nworkers=2
    )
    for pn, pid, gen in [(1, 301, MALE), (2, 302, FEMALE)]:
        s.person(pid, 3, pn, AGE_35_TO_44, gen, CAR_COMMUTER, wloc="work_2")
        s.week(pid, 3, pnum=pn).round_trip(
            "home_c", "work_2", PC_WORK, MT_CAR, "8:00-8:25", "17:30-17:55", ntrav=2
        )

    # HH 4 - multi-stop errands
    s.household(4, "home_d", res=RES_TOWNHOUSE, income=INC_50_75K, nworkers=0)
    s.person(401, 4, 1, AGE_45_TO_54, MALE, NONWORKER, race=RACE_OTHER, eth=ETH_MEXICAN)
    d = s.week(401, 4)
    d.trip("home_d", "shop_1", PC_HOME, PC_SHOP, MT_CAR, "9:00-9:15")
    d.trip("shop_1", "meal_1", PC_SHOP, PC_MEAL, MT_CAR, "9:45-10:00")
    d.trip("meal_1", "errand_1", PC_MEAL, PC_ERRAND, MT_CAR, "10:45-11:00")
    d.trip("errand_1", "shop_2", PC_ERRAND, PC_SHOP, MT_CAR, "11:30-11:45")
    d.trip("shop_2", "home_d", PC_SHOP, PC_HOME, MT_CAR, "12:15-12:30")

    # HH 5 - escort + school (adult + child)
    s.household(5, "home_e", rent=OWN, res=RES_SFH, income=INC_25_50K, npeople=2)
    s.person(501, 5, 1, AGE_35_TO_44, FEMALE, CAR_COMMUTER, wloc="work_3", race=RACE_AFAM)
    s.person(502, 5, 2, AGE_5_TO_15, FEMALE, CHILD, race=RACE_AFAM)
    d = s.week(501, 5, pnum=1)
    d.trip("home_e", "school_1", PC_HOME, PC_ESCORT, MT_CAR, "7:30-7:45")
    d.trip("school_1", "work_3", PC_ESCORT, PC_WORK, MT_CAR, "7:45-8:10")
    d.trip("work_3", "home_e", PC_WORK, PC_HOME, MT_CAR, "17:00-17:25")
    s.week(502, 5, pnum=2).round_trip(
        "home_e", "school_1", PC_SCHOOL, MT_CAR, "7:30-7:45", "15:00-15:15", drv=DRV_PASSENGER
    )

    # HH 6 - work subtour (lunch)
    s.household(6, "home_f", res=RES_CONDO_5_50, income=INC_200_PLUS, veh=0)
    s.person(601, 6, 1, AGE_25_TO_34, FEMALE, WORKER, wloc="work_1", wm=MT_WALK)
    d = s.week(601, 6)
    d.trip("home_f", "work_1", PC_HOME, PC_WORK, MT_WALK, "8:00-8:20")
    d.trip("work_1", "lunch_1", PC_WORK, PC_MEAL, MT_WALK, "12:00-12:10")
    d.trip("lunch_1", "work_1", PC_MEAL, PC_WORK, MT_WALK, "12:45-12:55")
    d.trip("work_1", "home_f", PC_WORK, PC_HOME, MT_WALK, "17:30-17:50")

    # HH 7 - single-trip tour (didn't return home)
    s.household(7, "home_g", res=RES_MULTIFAMILY, income=INC_UNDER_25K, nworkers=0)
    s.person(701, 7, 1, AGE_18_TO_24, MALE, NONWORKER)
    s.week(701, 7).trip("home_g", "social_1", PC_HOME, PC_SOCIALREC, MT_CAR, "19:00-19:20")

    # HH 8 - recreation (2 retirees, potential joint trips)
    s.household(
        8, "home_h", rent=OWN, res=RES_SFH, income=INC_200_PLUS, veh=2, npeople=2, nworkers=0
    )
    for pn, pid, gen, age in [(1, 801, MALE, AGE_55_TO_64), (2, 802, FEMALE, AGE_45_TO_54)]:
        s.person(pid, 8, pn, age, gen, NONWORKER)
        d = s.week(pid, 8, pnum=pn)
        d.trip("home_h", "shop_1", PC_HOME, PC_SHOP, MT_CAR, "10:00-10:15")
        d.trip("shop_1", "meal_1", PC_SHOP, PC_MEAL, MT_CAR, "11:00-11:10")
        d.trip("meal_1", "home_h", PC_MEAL, PC_HOME, MT_CAR, "12:30-12:45")

    # HH 9 - TNC user
    s.household(9, "home_i", res=RES_CONDO_5_50, income=INC_100_200K, veh=0)
    s.person(901, 9, 1, AGE_25_TO_34, MALE, WORKER, wloc="work_2", wp=WP_FREE, wm=MT_TNC)
    s.week(901, 9).round_trip("home_i", "work_2", PC_WORK, MT_TNC, "8:30-8:50", "18:00-18:20")

    # HH 10 - bikeshare + part-time student
    s.household(10, "home_j", res=RES_TOWNHOUSE, income=INC_50_75K, veh=0)
    s.person(
        1001,
        10,
        1,
        AGE_18_TO_24,
        FEMALE,
        WORKER,
        emp=EMP_PARTTIME,
        stu=STU_PARTTIME,
        wloc="work_3",
        sloc="school_1",
        st=SCH_4YEAR,
        wp=WP_FREE,
        wm=MT_BIKESHARE,
        race=RACE_OTHER,
        tp=YES,
    )
    s.week(1001, 10).round_trip(
        "home_j", "work_3", PC_WORK, MT_BIKESHARE, "9:00-9:25", "16:00-16:25", m1=MODE_BIKE_RENTED
    )

    # HH 11 - multi-day traveler (2 travel days)
    s.household(11, "home_k", rent=OWN, res=RES_SFH, income=INC_100_200K)
    s.person(1101, 11, 1, AGE_45_TO_54, MALE, CAR_COMMUTER, wloc="work_1", ndays=2)
    s.day(1101, 11, DAY_DATE_1, dnum=1).round_trip(
        "home_k", "work_1", PC_WORK, MT_CAR, "8:00-8:20", "17:00-17:20"
    )
    s.day(1101, 11, DAY_DATE_2, dnum=2).round_trip(
        "home_k", "work_1", PC_WORK, MT_CAR, "8:15-8:35", "17:30-17:50"
    )

    # HH 12 - incomplete household (missing data)
    s.household(
        12,
        "home_l",
        rent=RENT_OWN_MISSING,
        res=RES_MISSING,
        income=INC_MISSING,
        survey_complete=False,
    )
    s.person(
        1201,
        12,
        1,
        AGE_55_TO_64,
        GENDER_MISSING,
        CAR_COMMUTER,
        wloc="work_2",
        wp=WP_MISSING,
        race=RACE_PNTA,
        eth=ETH_MISSING,
        comp=False,
    )
    s.week(1201, 12, comp=False).round_trip(
        "home_l", "work_2", PC_WORK, MT_CAR, "8:00-8:25", "17:00-17:25"
    )

    # HH 13 - senior retiree couple (65+). Also restores the under-25k income bin
    # and exercises the taxi mode.
    s.household(13, "home_m", rent=OWN, res=RES_SFH, income=INC_UNDER_25K, npeople=2, nworkers=0)
    s.person(1301, 13, 1, AGE_65_TO_74, MALE, NONWORKER)
    s.person(1302, 13, 2, AGE_75_TO_84, FEMALE, NONWORKER)
    s.week(1301, 13, pnum=1).round_trip(
        "home_m", "shop_2", PC_SHOP, MT_WALK, "10:00-10:20", "11:00-11:20", m1=MODE_WALK
    )
    s.week(1302, 13, pnum=2).round_trip(
        "home_m", "meal_1", PC_MEAL, MT_TAXI, "12:00-12:20", "13:30-13:50"
    )

    # HH 14 - large family (4 persons): 2 workers, a high-school teen (school bus),
    # and a preschool-age child (proxy). Exercises 3+ person households, the 16-17
    # and under-5 age bands, self-employment, and the school-bus mode.
    s.household(
        14, "home_n", rent=OWN, res=RES_SFH, income=INC_100_200K, veh=2, npeople=4, nworkers=2
    )
    s.person(1401, 14, 1, AGE_35_TO_44, MALE, CAR_COMMUTER, wloc="work_1", race=RACE_ASIAN)
    s.person(
        1402,
        14,
        2,
        AGE_35_TO_44,
        FEMALE,
        CAR_COMMUTER,
        emp=EMP_SELF,
        wloc="work_2",
        race=RACE_ASIAN,
    )
    s.person(
        1403, 14, 3, AGE_16_TO_17, FEMALE, CHILD, st=SCH_HIGH_SCHOOL, proxy=NO, race=RACE_ASIAN
    )
    s.person(1404, 14, 4, AGE_UNDER_5, MALE, CHILD, st=SCH_PRESCHOOL, race=RACE_ASIAN)
    s.week(1401, 14, pnum=1).round_trip(
        "home_n", "work_1", PC_WORK, MT_CAR, "8:00-8:25", "17:00-17:25"
    )
    s.week(1402, 14, pnum=2).round_trip(
        "home_n", "work_2", PC_WORK, MT_CAR, "9:00-9:20", "16:00-16:20"
    )
    s.week(1403, 14, pnum=3).round_trip(
        "home_n",
        "school_1",
        PC_SCHOOL,
        MT_SCHOOL_BUS,
        "7:30-7:50",
        "15:00-15:20",
        drv=DRV_PASSENGER,
    )
    s.week(1404, 14, pnum=4).round_trip(
        "home_n", "school_1", PC_SCHOOL, MT_CAR, "8:30-8:45", "14:00-14:15", drv=DRV_PASSENGER
    )

    # HH 15 - car-free bike commuter; exercises the BIKE mode.
    s.household(15, "home_o", res=RES_MULTIFAMILY, income=INC_50_75K, veh=0)
    s.person(1501, 15, 1, AGE_25_TO_34, MALE, WORKER, wloc="work_3", wm=MT_BIKE, race=RACE_OTHER)
    s.week(1501, 15).round_trip("home_o", "work_3", PC_WORK, MT_BIKE, "8:30-8:50", "17:30-17:50")

    # HH 16 - university student (person_type UNIVERSITY_STUDENT, student_category
    # COLLEGE, ctramp purpose 'university', activity_pattern M). Person 1602 has a
    # day record but NO trips -> activity_pattern H (no-travel day).
    s.household(16, "home_p", income=INC_25_50K, npeople=2, nworkers=0)
    s.person(
        1601, 16, 1, AGE_18_TO_24, FEMALE, STUDENT, sloc="univ_1", st=SCH_4YEAR, race=RACE_ASIAN
    )
    s.person(1602, 16, 2, AGE_55_TO_64, MALE, NONWORKER, cf=None, race=RACE_ASIAN)
    s.week(1601, 16, pnum=1).round_trip(
        "home_p", "univ_1", PC_SCHOOL, MT_TRANSIT, "9:00-9:30", "15:00-15:30"
    )
    s.week(1602, 16, pnum=2)  # no trips -> "H" pattern

    # HH 17 - part-time worker with an OUTBOUND stop (coffee before work) and an
    # INBOUND stop (grocery after work). tour_purpose stays WORK (higher priority).
    s.household(17, "home_q", income=INC_75_100K)
    s.person(1701, 17, 1, AGE_45_TO_54, MALE, CAR_COMMUTER, emp=EMP_PARTTIME, wloc="work_1")
    d = s.week(1701, 17)
    d.trip("home_q", "coffee_1", PC_HOME, PC_SHOP, MT_CAR, "8:00-8:10")
    d.trip("coffee_1", "work_1", PC_SHOP, PC_WORK, MT_CAR, "8:20-9:00")
    d.trip("work_1", "shop_1", PC_WORK, PC_SHOP, MT_CAR, "17:00-17:15")
    d.trip("shop_1", "home_q", PC_SHOP, PC_HOME, MT_CAR, "17:45-18:00")

    # HH 18 - loop trip (home -> home single trip) -> tour_data_quality LOOP_TRIP.
    s.household(18, "home_r", income=INC_UNDER_25K, veh=0, nworkers=0)
    s.person(1801, 18, 1, AGE_25_TO_34, FEMALE, NONWORKER)
    s.week(1801, 18).trip(
        "home_r", "home_r", PC_HOME, PC_HOME, MT_WALK, "10:00-10:30", m1=MODE_WALK
    )

    # HH 19 - change-mode-as-primary-purpose (tour_data_quality CHANGE_MODE +
    # tour_purpose CHANGE_MODE). The >180 min gap prevents the two segments from
    # linking, so the CHANGE_MODE stop survives to tour level (a linking failure).
    s.household(19, "home_a", income=INC_100_200K)
    s.person(1901, 19, 1, AGE_35_TO_44, MALE, CAR_COMMUTER, wloc="work_3")
    s.week(1901, 19).round_trip(
        "home_a", "pr_1", PC_CHANGE_MODE, MT_CAR, "8:00-8:20", "12:00-12:20"
    )

    # HH 20 - day that never touches home (work -> meal -> gym) ->
    # tour_data_quality PARTIAL_BOTH, tour_category PARTIAL_BOTH.
    s.household(20, "home_b", income=INC_50_75K)
    s.person(2001, 20, 1, AGE_25_TO_34, FEMALE, CAR_COMMUTER, wloc="work_1")
    d = s.week(2001, 20)
    d.trip("work_1", "meal_1", PC_WORK, PC_MEAL, MT_CAR, "12:00-12:20")
    d.trip("meal_1", "gym_1", PC_MEAL, PC_SOCIALREC, MT_CAR, "13:30-13:50")

    # HH 21 - partial-start tour: the day's first trip starts away from home (an
    # overnight worker heading out, then home via an errand) and ends home. Needs
    # 2+ trips (a single trip would be forced to PARTIAL_BOTH) -> PARTIAL_START.
    s.household(21, "home_c", income=INC_50_75K)
    s.person(2101, 21, 1, AGE_35_TO_44, MALE, CAR_COMMUTER, wloc="work_2")
    d = s.week(2101, 21)
    d.trip("work_2", "errand_1", PC_WORK, PC_ERRAND, MT_CAR, "0:30-0:50")
    d.trip("errand_1", "home_c", PC_ERRAND, PC_HOME, MT_CAR, "1:00-1:20")

    # HH 22 - partial-end tour (home -> work -> social; ends away from home) ->
    # tour_category PARTIAL_END.
    s.household(22, "home_d", income=INC_100_200K)
    s.person(2201, 22, 1, AGE_45_TO_54, FEMALE, CAR_COMMUTER, wloc="work_1")
    d = s.week(2201, 22)
    d.trip("home_d", "work_1", PC_HOME, PC_WORK, MT_CAR, "8:00-8:30")
    d.trip("work_1", "social_1", PC_WORK, PC_SOCIALREC, MT_CAR, "17:00-17:30")

    # HH 23 - 3-person joint trip with a child -> joint composition
    # ADULTS_AND_CHILDREN. All three travel home->shop->home together.
    s.household(23, "home_e", income=INC_100_200K, veh=2, npeople=3, nworkers=2)
    s.person(2301, 23, 1, AGE_35_TO_44, MALE, EMPLOYED, race=RACE_AFAM)
    s.person(2302, 23, 2, AGE_35_TO_44, FEMALE, EMPLOYED, race=RACE_AFAM)
    s.person(2303, 23, 3, AGE_5_TO_15, MALE, CHILD, sloc=None, race=RACE_AFAM)
    for pn, pid in [(1, 2301), (2, 2302), (3, 2303)]:
        s.week(pid, 23, pnum=pn).round_trip(
            "home_e", "shop_1", PC_SHOP, MT_CAR, "11:00-11:20", "12:30-12:50", ntrav=3
        )

    # HH 24 - two children travel together with no adult. The morning school run is
    # a *mandatory* joint tour, which CT-RAMP cannot represent: under a fully_joint
    # profile the cascade keeps it off any joint tour (negative control). The
    # afternoon shop tour is non-mandatory and admissible, so it is what actually
    # exercises joint composition CHILDREN_ONLY.
    s.household(24, "home_f", income=INC_75_100K, npeople=2, nworkers=0)
    together = {"ntrav": 2, "m1": MODE_WALK}
    for pn, pid, gen in [(1, 2401, MALE), (2, 2402, FEMALE)]:
        s.person(pid, 24, pn, AGE_5_TO_15, gen, CHILD)
        w = s.week(pid, 24, pnum=pn)
        w.round_trip(
            "home_f", "school_1", PC_SCHOOL, MT_WALK, "8:00-8:20", "15:00-15:20", **together
        )
        w.round_trip("home_f", "shop_1", PC_SHOP, MT_WALK, "16:00-16:20", "17:00-17:20", **together)

    # HH 25 - two household members make the same home->shop->home trip but SIX
    # HOURS APART -> must NOT be detected as joint (temporal-overlap negative control).
    s.household(25, "home_g", income=INC_50_75K, npeople=2)
    for pn, pid, gen, h in [(1, 2501, MALE, 8), (2, 2502, FEMALE, 14)]:
        s.person(pid, 25, pn, AGE_45_TO_54, gen, NONWORKER)
        out, back = f"{h}:00-{h}:20", f"{h + 1}:00-{h + 1}:20"
        s.week(pid, 25, pnum=pn).round_trip(
            "home_g", "shop_2", PC_SHOP, MT_WALK, out, back, m1=MODE_WALK
        )

    # HH 26 - the weight-propagation household. Each member travels on BOTH
    # diary dates, so the persons -> days split divides by the days that survive
    # the usability gate, and the divisor changes when a day is dropped.
    # Person 2601 has one clean day and one whose tour never reaches home
    # (PARTIAL_DIARY_EDGE): a strict profile keeps one day, a loose one both.
    # Person 2602 has two clean days.
    s.household(26, "home_a", income=INC_100_200K, npeople=2, veh=2, nworkers=2)
    for pn, pid, gen in [(1, 2601, MALE), (2, 2602, FEMALE)]:
        s.person(pid, 26, pn, AGE_35_TO_44, gen, EMPLOYED, wloc="work_1", cf=CF_NEVER)
    # Day one: both members commute home -> work -> home.
    for pn, pid in [(1, 2601), (2, 2602)]:
        s.day(pid, 26, DAY_DATE_1, pnum=pn, dnum=1).round_trip(
            "home_a", "work_1", PC_WORK, MT_CAR, "8:00-8:30", "17:00-17:30"
        )
    # Day two: 2602 commutes again; 2601 leaves home and the diary stops there, so
    # a primary_home profile drops that day and its weight must land on day one.
    s.day(2602, 26, DAY_DATE_2, pnum=2, dnum=2).round_trip(
        "home_a", "work_1", PC_WORK, MT_CAR, "8:00-8:30", "17:00-17:30"
    )
    s.day(2601, 26, DAY_DATE_2, dnum=2).trip(
        "home_a", "work_1", PC_HOME, PC_WORK, MT_CAR, "8:00-8:30"
    )

    # HH 27 and 28 - out of region, which a profile naming a zone_coverage gates
    # on. Both diaries are complete and their tours close at home, so geography is
    # the only thing that can exclude them.
    #
    # 27 lives outside the zone file, so the household has no home zone. That has
    # to reach its tours and days: the cascade reduces upward, so a household-level
    # fact reaches them only because it is joined into their verdicts.
    s.household(27, "home_remote", income=INC_100_200K)
    s.person(2701, 27, 1, AGE_35_TO_44, FEMALE, EMPLOYED, cf=CF_NEVER)
    s.week(2701, 27).round_trip(
        "home_remote", "work_remote", PC_WORK, MT_CAR, "8:00-8:30", "17:00-17:30"
    )

    # 28 lives in region and commutes out of it on the second date only, which
    # isolates the tour-level gate: one day is dropped, the person keeps the other.
    s.household(28, "home_b", income=INC_100_200K)
    s.person(2801, 28, 1, AGE_35_TO_44, MALE, EMPLOYED, wloc="work_1", cf=CF_NEVER)
    s.day(2801, 28, DAY_DATE_1, dnum=1).round_trip(
        "home_b", "work_1", PC_WORK, MT_CAR, "8:00-8:30", "17:00-17:30"
    )
    s.day(2801, 28, DAY_DATE_2, dnum=2).round_trip(
        "home_b", "work_remote", PC_WORK, MT_CAR, "8:00-9:30", "17:00-18:30"
    )

    # HH 29 - members usable on different dates: 2901 travels cleanly on Tuesday
    # and leaves an open tour on Wednesday, 2902 the reverse. Under
    # household_day_needs: all_members the household is usable on no date while
    # each person passes on one, and a formatter must not keep a person whose
    # household it excluded (that person has no home to report).
    s.household(29, "home_c", income=INC_75_100K, npeople=2, veh=1, nworkers=2)
    for pn, pid, gen in [(1, 2901, FEMALE), (2, 2902, MALE)]:
        s.person(pid, 29, pn, AGE_35_TO_44, gen, EMPLOYED, wloc="work_2", cf=CF_NEVER)
    s.day(2901, 29, DAY_DATE_1, dnum=1).round_trip(
        "home_c", "work_2", PC_WORK, MT_CAR, "8:00-8:25", "17:00-17:25"
    )
    s.day(2902, 29, DAY_DATE_1, pnum=2, dnum=1).trip(
        "home_c", "work_2", PC_HOME, PC_WORK, MT_CAR, "9:00-9:25"
    )
    s.day(2901, 29, DAY_DATE_2, dnum=2).trip(
        "home_c", "work_2", PC_HOME, PC_WORK, MT_CAR, "9:00-9:25"
    )
    s.day(2902, 29, DAY_DATE_2, pnum=2, dnum=2).round_trip(
        "home_c", "work_2", PC_WORK, MT_CAR, "8:00-8:25", "17:00-17:25"
    )

    # HH 30 - the joint group that loses a member. Both leave together, so the
    # clique detection groups that leg; only 3001 comes home. 3002's tour never
    # closes, so a primary_home profile excludes it while 3001 stays usable. A
    # group down to one member stops being joint, and every table carrying its id
    # must clear it, or the trip claims a joint tour that has no joint trip.
    s.household(30, "home_d", income=INC_100_200K, npeople=2, veh=1, nworkers=2)
    for pn, pid, gen in [(1, 3001, FEMALE), (2, 3002, MALE)]:
        s.person(pid, 30, pn, AGE_25_TO_34, gen, EMPLOYED, wloc="work_3", cf=CF_NEVER)
    s.week(3001, 30).round_trip(
        "home_d", "work_3", PC_WORK, MT_CAR, "7:30-8:00", "16:30-17:00", ntrav=2
    )
    s.week(3002, 30, pnum=2).trip(
        "home_d", "work_3", PC_HOME, PC_WORK, MT_CAR, "7:30-8:00", drv=DRV_PASSENGER, ntrav=2
    )

    # HH 31 - work-related stops, and a workplace the survey never asked for.
    # 3101's job location varies, so no workplace was reported; a full working day
    # at work_2 makes it an observed one. Their work-related tour is a work tour,
    # and the observed workplace stands in as the model's usual workplace.
    # 3102 is retired: the same kind of stop is volunteering, and stays
    # non-mandatory.
    s.household(31, "home_s", income=INC_75_100K, npeople=2, veh=1, nworkers=1)
    s.person(3101, 31, 1, AGE_35_TO_44, FEMALE, EMPLOYED, jt=JOB_VARIES)
    s.person(3102, 31, 2, AGE_65_TO_74, MALE, NONWORKER, cf=None)
    s.week(3101, 31).round_trip(
        "home_s", "work_2", PC_WORK_RELATED, MT_CAR, "8:00-8:30", "17:00-17:30"
    )
    s.week(3102, 31, pnum=2).round_trip(
        "home_s", "social_1", PC_WORK_RELATED, MT_WALK, "10:00-10:20", "12:00-12:20"
    )

    # HH 32 - a work tour with no usual workplace. 3201's location varies and the
    # one work-related stop is a 45-minute meeting, too short to be a workplace,
    # so the tour is work but the model has no usual workplace to send it to.
    s.household(32, "home_t", income=INC_100_200K, npeople=1, veh=1, nworkers=1)
    s.person(3201, 32, 1, AGE_45_TO_54, MALE, EMPLOYED, emp=EMP_PARTTIME, jt=JOB_VARIES)
    s.week(3201, 32).round_trip(
        "home_t", "errand_1", PC_WORK_RELATED, MT_CAR, "9:00-9:20", "10:05-10:25"
    )

    # HH 33 - reported by form and excluded by the cleaner for it, so it is
    # unusable under every profile though the survey calls it complete.
    s.household(33, "home_l", income=INC_75_100K, npeople=1, veh=1, nworkers=0)
    s.person(
        3301,
        33,
        1,
        AGE_35_TO_44,
        FEMALE,
        NONWORKER,
        cf=None,
        tech=TECH_BROWSER,
        excl="survey_tech excluded: browser",
    )
    s.week(3301, 33).round_trip("home_l", "shop_1", PC_SHOP, MT_CAR, "10:00-10:15", "11:00-11:15")

    return s.hhs, s.pers, s.person_days, s.trips


def build_survey_dataframes():
    """Return canonical survey data as a dict of Polars DataFrames.

    Keys: households, persons, household_days, person_days, unlinked_trips, habitual_locations
    """
    hh, per, day, trip = _build_records()
    households = pl.DataFrame(hh)

    # More MISSING income bins (HH 12 already has one) so the RF imputation has
    # several values to fill and enough complete rows to train on.
    _income_missing_hh = {4, 7, 8, 12}
    households = households.with_columns(
        income_bin=pl.when(pl.col("hh_id").is_in(_income_missing_hh))
        .then(pl.lit(INC_MISSING))
        .otherwise(pl.col("income_bin"))
    )

    # The formatters read a continuous `income` as well as income_bin; null when MISSING.
    _bin_to_income = {
        INC_UNDER_25K: 15_000,
        INC_25_50K: 37_500,
        INC_50_75K: 62_500,
        INC_75_100K: 87_500,
        INC_100_200K: 150_000,
        INC_200_PLUS: 250_000,
    }
    households = households.with_columns(
        income=pl.col("income_bin").replace_strict(_bin_to_income, default=None)
    )

    persons = pl.DataFrame(per)
    # The household-days a project cleaner would build.
    person_days, household_days = create_household_days(
        pl.DataFrame(day, schema_overrides={"begin_day": pl.Int64, "end_day": pl.Int64})
    )
    tables = {
        "households": households,
        "persons": persons,
        "household_days": household_days,
        "person_days": person_days,
        "unlinked_trips": pl.DataFrame(trip),
        # Delivered with the survey: the reported home, work and school only.
        "habitual_locations": reported_habitual_locations(households, persons),
    }

    return tables
