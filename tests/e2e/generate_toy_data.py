"""Synthetic toy data builders for E2E integration tests.

All functions return Polars DataFrames (or dicts for JSON) with correct dtypes.
No file I/O -- callers decide where to materialize the data.

All coordinates and data are fully synthetic -- no real PII.

Each household in ``_build_records`` is authored to exercise specific pipeline
classification outputs (person_type, tour_data_quality, tour_category,
activity_pattern, joint composition, linking patterns). See tests/e2e/COVERAGE.md
for the scenario -> edge-case map. The record fields are produced through a small
``_Survey`` builder so each scenario reads as a few compact lines.
"""

import math
from datetime import date, datetime

import polars as pl

# ---------------------------------------------------------------------------
# Coordinates (synthetic SF Bay Area -- no real addresses)
# ---------------------------------------------------------------------------
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
}

DAY_DATE_1 = date(2024, 3, 11)  # Monday
DAY_DATE_2 = date(2024, 3, 12)  # Tuesday
DAY_DATE_SAT = date(2024, 3, 16)  # Saturday

# ---------------------------------------------------------------------------
# Canonical enum integer values (from data_canon.codebook)
# ---------------------------------------------------------------------------
FEMALE, MALE, GENDER_MISSING = 1, 2, 995
AGE_UNDER_5, AGE_5_TO_15, AGE_16_TO_17 = 1, 2, 3
AGE_18_TO_24, AGE_25_TO_34 = 4, 5
AGE_35_TO_44, AGE_45_TO_54, AGE_55_TO_64 = 6, 7, 8
AGE_65_TO_74, AGE_75_TO_84 = 9, 10
EMP_FULLTIME, EMP_PARTTIME, EMP_SELF, EMP_NOT_LOOKING = 1, 2, 3, 5
STU_FULLTIME, STU_PARTTIME, STU_NONSTUDENT = 0, 1, 2
INC_UNDER_25K, INC_25_50K, INC_50_75K = 1, 2, 3
INC_75_100K, INC_100_200K, INC_200_PLUS, INC_MISSING = 4, 5, 6, 995
RES_SFH, RES_TOWNHOUSE, RES_MULTIFAMILY, RES_CONDO_5_50, RES_MISSING = 1, 2, 3, 4, 995
OWN, RENT, RENT_OWN_MISSING = 1, 2, 995
PC_HOME, PC_WORK, PC_SCHOOL = 1, 2, 4
PC_ESCORT, PC_SHOP, PC_MEAL = 6, 7, 8
PC_SOCIALREC, PC_ERRAND, PC_CHANGE_MODE = 9, 10, 11
PURP_HOME, PURP_WORK = 1, 2
MT_WALK, MT_BIKE, MT_BIKESHARE, MT_TAXI = 1, 2, 3, 5
MT_TNC, MT_CAR, MT_SCHOOL_BUS, MT_TRANSIT = 6, 8, 10, 13
MODE_WALK, MODE_BIKE_RENTED, MODE_BART = 1, 4, 30
WP_FREE, WP_NOT_APPLICABLE, WP_MISSING = 1, 996, 995
YES, NO = 1, 0
DOW_MONDAY, DOW_TUESDAY, DOW_SATURDAY = 1, 2, 6
DRV_DRIVER, DRV_PASSENGER, DRV_MISSING = 1, 2, 995
JOB_FIXED = 1
SCH_PRESCHOOL, SCH_ELEMENTARY, SCH_HIGH_SCHOOL, SCH_4YEAR = 3, 5, 7, 12
RACE_AFAM, RACE_ASIAN, RACE_WHITE, RACE_OTHER, RACE_PNTA = 1, 3, 5, 6, 999
ETH_NOT_HISPANIC, ETH_MEXICAN, ETH_MISSING = 1, 2, 995
CF_5_DAYS, CF_NEVER = 2, 996


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _meters(lat1, lon1, lat2, lon2):
    earth_radius_m = 6_371_000
    dlat, dlon = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    return 2 * earth_radius_m * math.asin(math.sqrt(a))


def _dt(d, hm):
    """Resolve a ``(hour, minute)`` tuple against date ``d`` to a datetime."""
    return datetime(d.year, d.month, d.day, hm[0], hm[1], 0)


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------


class _Day:
    """Trip context bound to a single person-day (day_id, person, hh, date)."""

    def __init__(self, survey, day_id, pid, hid, dd):
        self._s = survey
        self.day_id = day_id
        self.pid = pid
        self.hid = hid
        self.dd = dd

    def trip(self, o, d, opc, dpc, mode, dep, arr, *, m1=None, ntrav=1, drv=None):
        """Append one unlinked trip. Times are ``(hour, minute)`` tuples."""
        self._s._tc += 1
        olat, olon = COORDS[o]
        dlat, dlon = COORDS[d]
        if drv is None:
            drv = DRV_DRIVER if mode == MT_CAR else DRV_PASSENGER if mode == MT_TNC else DRV_MISSING
        dep_dt, arr_dt = _dt(self.dd, dep), _dt(self.dd, arr)
        self._s.trips.append(
            {
                "unlinked_trip_id": self._s._tc,
                "day_id": self.day_id,
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
                "complete": True,
                "unlinked_trip_weight": None,
            }
        )
        return self

    def round_trip(self, a, b, apc, bpc, mode, out, back, **kw):
        """Two mirror trips a->b then b->a, with purposes swapped.

        ``out``/``back`` are ``((dep_h, dep_m), (arr_h, arr_m))`` pairs.
        """
        self.trip(a, b, apc, bpc, mode, out[0], out[1], **kw)
        self.trip(b, a, bpc, apc, mode, back[0], back[1], **kw)
        return self


class _Survey:
    """Accumulates households, persons, days, and trips as lists of dicts."""

    def __init__(self):
        self.hhs, self.pers, self.days, self.trips = [], [], [], []
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
        complete=True,
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
                "complete": complete,
                "hh_weight": None,
            }
        )

    def person(
        self,
        pid,
        hid,
        pn,
        age,
        gen,
        emp,
        stu,
        *,
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
                "commute_subsidy_provide_free_parking": NO,
                "commute_subsidy_provide_discounted_parking": NO,
                "commute_subsidy_use_free_parking": NO,
                "commute_subsidy_use_discounted_parking": NO,
                "transit_pass": tp,
                "is_proxy": proxy,
                "num_days_complete": ndays,
                "complete": comp,
                "person_weight": None,
            }
        )

    def day(self, pid, hid, dd, dow, *, pnum=1, dnum=1, comp=True) -> _Day:
        """Append a person-day (day_id = pid*100 + dnum) and return its trip context."""
        day_id = pid * 100 + dnum
        self.days.append(
            {
                "day_id": day_id,
                "person_id": pid,
                "hh_id": hid,
                "travel_date": datetime(dd.year, dd.month, dd.day),
                "travel_dow": dow,
                "person_num": pnum,
                "day_num": dnum,
                "complete": comp,
                "day_weight": None,
            }
        )
        return _Day(self, day_id, pid, hid, dd)


# ---------------------------------------------------------------------------
# Scenario households
# ---------------------------------------------------------------------------


def _build_records():
    """Build all canonical records. Returns (hh, per, day, trip) as lists of dicts."""
    s = _Survey()

    # HH 1 - simple car commuter
    s.household(1, "home_a", rent=OWN, res=RES_SFH, income=INC_75_100K)
    s.person(
        101,
        1,
        1,
        AGE_35_TO_44,
        MALE,
        EMP_FULLTIME,
        STU_NONSTUDENT,
        wloc="work_1",
        jt=JOB_FIXED,
        wp=WP_FREE,
        wm=MT_CAR,
        cf=CF_5_DAYS,
    )
    s.day(101, 1, DAY_DATE_1, DOW_MONDAY).round_trip(
        "home_a", "work_1", PC_HOME, PC_WORK, MT_CAR, ((8, 0), (8, 30)), ((17, 0), (17, 30))
    )

    # HH 2 - transit with mode change (4 unlinked -> 2 linked)
    s.household(2, "home_b", res=RES_CONDO_5_50, income=INC_100_200K, veh=0)
    s.person(
        201,
        2,
        1,
        AGE_25_TO_34,
        FEMALE,
        EMP_FULLTIME,
        STU_NONSTUDENT,
        wloc="bart_dest",
        jt=JOB_FIXED,
        wm=MT_TRANSIT,
        race=RACE_ASIAN,
        cf=CF_5_DAYS,
        tp=YES,
    )
    d = s.day(201, 2, DAY_DATE_1, DOW_MONDAY)
    d.trip(
        "home_b", "bart_station", PC_HOME, PC_CHANGE_MODE, MT_WALK, (7, 30), (7, 45), m1=MODE_WALK
    )
    d.trip(
        "bart_station",
        "bart_dest",
        PC_CHANGE_MODE,
        PC_WORK,
        MT_TRANSIT,
        (7, 45),
        (8, 15),
        m1=MODE_BART,
    )
    d.trip(
        "bart_dest",
        "bart_station",
        PC_WORK,
        PC_CHANGE_MODE,
        MT_TRANSIT,
        (17, 0),
        (17, 30),
        m1=MODE_BART,
    )
    d.trip(
        "bart_station", "home_b", PC_CHANGE_MODE, PC_HOME, MT_WALK, (17, 30), (17, 45), m1=MODE_WALK
    )

    # HH 3 - joint trip household (2 persons)
    s.household(
        3, "home_c", rent=OWN, res=RES_SFH, income=INC_100_200K, veh=2, npeople=2, nworkers=2
    )
    for pn, pid, gen in [(1, 301, MALE), (2, 302, FEMALE)]:
        s.person(
            pid,
            3,
            pn,
            AGE_35_TO_44,
            gen,
            EMP_FULLTIME,
            STU_NONSTUDENT,
            wloc="work_2",
            jt=JOB_FIXED,
            wp=WP_FREE,
            wm=MT_CAR,
            cf=CF_5_DAYS,
        )
        s.day(pid, 3, DAY_DATE_1, DOW_MONDAY, pnum=pn).round_trip(
            "home_c",
            "work_2",
            PC_HOME,
            PC_WORK,
            MT_CAR,
            ((8, 0), (8, 25)),
            ((17, 30), (17, 55)),
            ntrav=2,
        )

    # HH 4 - multi-stop errands
    s.household(4, "home_d", res=RES_TOWNHOUSE, income=INC_50_75K, nworkers=0)
    s.person(
        401,
        4,
        1,
        AGE_45_TO_54,
        MALE,
        EMP_NOT_LOOKING,
        STU_NONSTUDENT,
        race=RACE_OTHER,
        eth=ETH_MEXICAN,
        cf=CF_NEVER,
    )
    d = s.day(401, 4, DAY_DATE_1, DOW_MONDAY)
    d.trip("home_d", "shop_1", PC_HOME, PC_SHOP, MT_CAR, (9, 0), (9, 15))
    d.trip("shop_1", "meal_1", PC_SHOP, PC_MEAL, MT_CAR, (9, 45), (10, 0))
    d.trip("meal_1", "errand_1", PC_MEAL, PC_ERRAND, MT_CAR, (10, 45), (11, 0))
    d.trip("errand_1", "shop_2", PC_ERRAND, PC_SHOP, MT_CAR, (11, 30), (11, 45))
    d.trip("shop_2", "home_d", PC_SHOP, PC_HOME, MT_CAR, (12, 15), (12, 30))

    # HH 5 - escort + school (parent + child)
    s.household(5, "home_e", rent=OWN, res=RES_SFH, income=INC_25_50K, npeople=2)
    s.person(
        501,
        5,
        1,
        AGE_35_TO_44,
        FEMALE,
        EMP_FULLTIME,
        STU_NONSTUDENT,
        wloc="work_3",
        jt=JOB_FIXED,
        wp=WP_FREE,
        wm=MT_CAR,
        race=RACE_AFAM,
        cf=CF_5_DAYS,
    )
    s.person(
        502,
        5,
        2,
        AGE_5_TO_15,
        FEMALE,
        EMP_NOT_LOOKING,
        STU_FULLTIME,
        sloc="school_1",
        st=SCH_ELEMENTARY,
        race=RACE_AFAM,
        proxy=YES,
    )
    d = s.day(501, 5, DAY_DATE_1, DOW_MONDAY, pnum=1)
    d.trip("home_e", "school_1", PC_HOME, PC_ESCORT, MT_CAR, (7, 30), (7, 45))
    d.trip("school_1", "work_3", PC_ESCORT, PC_WORK, MT_CAR, (7, 45), (8, 10))
    d.trip("work_3", "home_e", PC_WORK, PC_HOME, MT_CAR, (17, 0), (17, 25))
    s.day(502, 5, DAY_DATE_1, DOW_MONDAY, pnum=2).round_trip(
        "home_e",
        "school_1",
        PC_HOME,
        PC_SCHOOL,
        MT_CAR,
        ((7, 30), (7, 45)),
        ((15, 0), (15, 15)),
        drv=DRV_PASSENGER,
    )

    # HH 6 - work subtour (lunch)
    s.household(6, "home_f", res=RES_CONDO_5_50, income=INC_200_PLUS, veh=0)
    s.person(
        601,
        6,
        1,
        AGE_25_TO_34,
        FEMALE,
        EMP_FULLTIME,
        STU_NONSTUDENT,
        wloc="work_1",
        jt=JOB_FIXED,
        wm=MT_WALK,
        cf=CF_5_DAYS,
    )
    d = s.day(601, 6, DAY_DATE_1, DOW_MONDAY)
    d.trip("home_f", "work_1", PC_HOME, PC_WORK, MT_WALK, (8, 0), (8, 20))
    d.trip("work_1", "lunch_1", PC_WORK, PC_MEAL, MT_WALK, (12, 0), (12, 10))
    d.trip("lunch_1", "work_1", PC_MEAL, PC_WORK, MT_WALK, (12, 45), (12, 55))
    d.trip("work_1", "home_f", PC_WORK, PC_HOME, MT_WALK, (17, 30), (17, 50))

    # HH 7 - single-trip tour (didn't return home)
    s.household(7, "home_g", res=RES_MULTIFAMILY, income=INC_UNDER_25K, nworkers=0)
    s.person(701, 7, 1, AGE_18_TO_24, MALE, EMP_NOT_LOOKING, STU_NONSTUDENT, cf=CF_NEVER)
    s.day(701, 7, DAY_DATE_1, DOW_MONDAY).trip(
        "home_g", "social_1", PC_HOME, PC_SOCIALREC, MT_CAR, (19, 0), (19, 20)
    )

    # HH 8 - weekend recreation (2 retirees, potential joint trips)
    s.household(
        8, "home_h", rent=OWN, res=RES_SFH, income=INC_200_PLUS, veh=2, npeople=2, nworkers=0
    )
    for pn, pid, gen, age in [(1, 801, MALE, AGE_55_TO_64), (2, 802, FEMALE, AGE_45_TO_54)]:
        s.person(pid, 8, pn, age, gen, EMP_NOT_LOOKING, STU_NONSTUDENT, cf=CF_NEVER)
        d = s.day(pid, 8, DAY_DATE_SAT, DOW_SATURDAY, pnum=pn)
        d.trip("home_h", "shop_1", PC_HOME, PC_SHOP, MT_CAR, (10, 0), (10, 15))
        d.trip("shop_1", "meal_1", PC_SHOP, PC_MEAL, MT_CAR, (11, 0), (11, 10))
        d.trip("meal_1", "home_h", PC_MEAL, PC_HOME, MT_CAR, (12, 30), (12, 45))

    # HH 9 - TNC user
    s.household(9, "home_i", res=RES_CONDO_5_50, income=INC_100_200K, veh=0)
    s.person(
        901,
        9,
        1,
        AGE_25_TO_34,
        MALE,
        EMP_FULLTIME,
        STU_NONSTUDENT,
        wloc="work_2",
        jt=JOB_FIXED,
        wp=WP_FREE,
        wm=MT_TNC,
        cf=CF_5_DAYS,
    )
    s.day(901, 9, DAY_DATE_1, DOW_MONDAY).round_trip(
        "home_i", "work_2", PC_HOME, PC_WORK, MT_TNC, ((8, 30), (8, 50)), ((18, 0), (18, 20))
    )

    # HH 10 - bikeshare + part-time student
    s.household(10, "home_j", res=RES_TOWNHOUSE, income=INC_50_75K, veh=0)
    s.person(
        1001,
        10,
        1,
        AGE_18_TO_24,
        FEMALE,
        EMP_PARTTIME,
        STU_PARTTIME,
        wloc="work_3",
        sloc="school_1",
        jt=JOB_FIXED,
        st=SCH_4YEAR,
        wp=WP_FREE,
        wm=MT_BIKESHARE,
        race=RACE_OTHER,
        cf=CF_5_DAYS,
        tp=YES,
    )
    s.day(1001, 10, DAY_DATE_1, DOW_MONDAY).round_trip(
        "home_j",
        "work_3",
        PC_HOME,
        PC_WORK,
        MT_BIKESHARE,
        ((9, 0), (9, 25)),
        ((16, 0), (16, 25)),
        m1=MODE_BIKE_RENTED,
    )

    # HH 11 - multi-day traveler (2 travel days)
    s.household(11, "home_k", rent=OWN, res=RES_SFH, income=INC_100_200K)
    s.person(
        1101,
        11,
        1,
        AGE_45_TO_54,
        MALE,
        EMP_FULLTIME,
        STU_NONSTUDENT,
        wloc="work_1",
        jt=JOB_FIXED,
        wp=WP_FREE,
        wm=MT_CAR,
        cf=CF_5_DAYS,
        ndays=2,
    )
    s.day(1101, 11, DAY_DATE_1, DOW_MONDAY, dnum=1).round_trip(
        "home_k", "work_1", PC_HOME, PC_WORK, MT_CAR, ((8, 0), (8, 20)), ((17, 0), (17, 20))
    )
    s.day(1101, 11, DAY_DATE_2, DOW_TUESDAY, dnum=2).round_trip(
        "home_k", "work_1", PC_HOME, PC_WORK, MT_CAR, ((8, 15), (8, 35)), ((17, 30), (17, 50))
    )

    # HH 12 - incomplete household (missing data)
    s.household(
        12, "home_l", rent=RENT_OWN_MISSING, res=RES_MISSING, income=INC_MISSING, complete=False
    )
    s.person(
        1201,
        12,
        1,
        AGE_55_TO_64,
        GENDER_MISSING,
        EMP_FULLTIME,
        STU_NONSTUDENT,
        wloc="work_2",
        jt=JOB_FIXED,
        wp=WP_MISSING,
        wm=MT_CAR,
        race=RACE_PNTA,
        eth=ETH_MISSING,
        cf=CF_5_DAYS,
        comp=False,
    )
    s.day(1201, 12, DAY_DATE_1, DOW_MONDAY, comp=False).round_trip(
        "home_l", "work_2", PC_HOME, PC_WORK, MT_CAR, ((8, 0), (8, 25)), ((17, 0), (17, 25))
    )

    # HH 13 - senior retiree couple (65+). Also restores the under-25k income bin
    # and exercises the taxi mode.
    s.household(13, "home_m", rent=OWN, res=RES_SFH, income=INC_UNDER_25K, npeople=2, nworkers=0)
    s.person(1301, 13, 1, AGE_65_TO_74, MALE, EMP_NOT_LOOKING, STU_NONSTUDENT, cf=CF_NEVER)
    s.person(1302, 13, 2, AGE_75_TO_84, FEMALE, EMP_NOT_LOOKING, STU_NONSTUDENT, cf=CF_NEVER)
    s.day(1301, 13, DAY_DATE_1, DOW_MONDAY, pnum=1).round_trip(
        "home_m",
        "shop_2",
        PC_HOME,
        PC_SHOP,
        MT_WALK,
        ((10, 0), (10, 20)),
        ((11, 0), (11, 20)),
        m1=MODE_WALK,
    )
    s.day(1302, 13, DAY_DATE_1, DOW_MONDAY, pnum=2).round_trip(
        "home_m", "meal_1", PC_HOME, PC_MEAL, MT_TAXI, ((12, 0), (12, 20)), ((13, 30), (13, 50))
    )

    # HH 14 - large family (4 persons): 2 workers, a high-school teen (school bus),
    # and a preschool-age child (proxy). Exercises 3+ person households, the 16-17
    # and under-5 age bands, self-employment, and the school-bus mode.
    s.household(
        14, "home_n", rent=OWN, res=RES_SFH, income=INC_100_200K, veh=2, npeople=4, nworkers=2
    )
    s.person(
        1401,
        14,
        1,
        AGE_35_TO_44,
        MALE,
        EMP_FULLTIME,
        STU_NONSTUDENT,
        wloc="work_1",
        jt=JOB_FIXED,
        wp=WP_FREE,
        wm=MT_CAR,
        race=RACE_ASIAN,
        cf=CF_5_DAYS,
    )
    s.person(
        1402,
        14,
        2,
        AGE_35_TO_44,
        FEMALE,
        EMP_SELF,
        STU_NONSTUDENT,
        wloc="work_2",
        jt=JOB_FIXED,
        wp=WP_FREE,
        wm=MT_CAR,
        race=RACE_ASIAN,
        cf=CF_5_DAYS,
    )
    s.person(
        1403,
        14,
        3,
        AGE_16_TO_17,
        FEMALE,
        EMP_NOT_LOOKING,
        STU_FULLTIME,
        sloc="school_1",
        st=SCH_HIGH_SCHOOL,
        race=RACE_ASIAN,
    )
    s.person(
        1404,
        14,
        4,
        AGE_UNDER_5,
        MALE,
        EMP_NOT_LOOKING,
        STU_FULLTIME,
        sloc="school_1",
        st=SCH_PRESCHOOL,
        race=RACE_ASIAN,
        proxy=YES,
    )
    s.day(1401, 14, DAY_DATE_1, DOW_MONDAY, pnum=1).round_trip(
        "home_n", "work_1", PC_HOME, PC_WORK, MT_CAR, ((8, 0), (8, 25)), ((17, 0), (17, 25))
    )
    s.day(1402, 14, DAY_DATE_1, DOW_MONDAY, pnum=2).round_trip(
        "home_n", "work_2", PC_HOME, PC_WORK, MT_CAR, ((9, 0), (9, 20)), ((16, 0), (16, 20))
    )
    s.day(1403, 14, DAY_DATE_1, DOW_MONDAY, pnum=3).round_trip(
        "home_n",
        "school_1",
        PC_HOME,
        PC_SCHOOL,
        MT_SCHOOL_BUS,
        ((7, 30), (7, 50)),
        ((15, 0), (15, 20)),
        drv=DRV_PASSENGER,
    )
    s.day(1404, 14, DAY_DATE_1, DOW_MONDAY, pnum=4).round_trip(
        "home_n",
        "school_1",
        PC_HOME,
        PC_SCHOOL,
        MT_CAR,
        ((8, 30), (8, 45)),
        ((14, 0), (14, 15)),
        drv=DRV_PASSENGER,
    )

    # HH 15 - car-free bike commuter; exercises the BIKE mode.
    s.household(15, "home_o", res=RES_MULTIFAMILY, income=INC_50_75K, veh=0)
    s.person(
        1501,
        15,
        1,
        AGE_25_TO_34,
        MALE,
        EMP_FULLTIME,
        STU_NONSTUDENT,
        wloc="work_3",
        jt=JOB_FIXED,
        wm=MT_BIKE,
        race=RACE_OTHER,
        cf=CF_5_DAYS,
    )
    s.day(1501, 15, DAY_DATE_1, DOW_MONDAY).round_trip(
        "home_o", "work_3", PC_HOME, PC_WORK, MT_BIKE, ((8, 30), (8, 50)), ((17, 30), (17, 50))
    )

    # HH 16 - university student (person_type UNIVERSITY_STUDENT, student_category
    # COLLEGE, ctramp purpose 'university', activity_pattern M). Person 1602 has a
    # day record but NO trips -> activity_pattern H (no-travel day).
    s.household(16, "home_p", income=INC_25_50K, npeople=2, nworkers=0)
    s.person(
        1601,
        16,
        1,
        AGE_18_TO_24,
        FEMALE,
        EMP_NOT_LOOKING,
        STU_FULLTIME,
        sloc="univ_1",
        st=SCH_4YEAR,
        race=RACE_ASIAN,
    )
    s.person(1602, 16, 2, AGE_55_TO_64, MALE, EMP_NOT_LOOKING, STU_NONSTUDENT, race=RACE_ASIAN)
    s.day(1601, 16, DAY_DATE_1, DOW_MONDAY, pnum=1).round_trip(
        "home_p", "univ_1", PC_HOME, PC_SCHOOL, MT_TRANSIT, ((9, 0), (9, 30)), ((15, 0), (15, 30))
    )
    s.day(1602, 16, DAY_DATE_1, DOW_MONDAY, pnum=2)  # no trips -> "H" pattern

    # HH 17 - part-time worker with an OUTBOUND stop (coffee before work) and an
    # INBOUND stop (grocery after work). tour_purpose stays WORK (higher priority).
    s.household(17, "home_q", income=INC_75_100K)
    s.person(
        1701,
        17,
        1,
        AGE_45_TO_54,
        MALE,
        EMP_PARTTIME,
        STU_NONSTUDENT,
        wloc="work_1",
        jt=JOB_FIXED,
        wp=WP_FREE,
        wm=MT_CAR,
        cf=CF_5_DAYS,
    )
    d = s.day(1701, 17, DAY_DATE_1, DOW_MONDAY)
    d.trip("home_q", "coffee_1", PC_HOME, PC_SHOP, MT_CAR, (8, 0), (8, 10))
    d.trip("coffee_1", "work_1", PC_SHOP, PC_WORK, MT_CAR, (8, 20), (9, 0))
    d.trip("work_1", "shop_1", PC_WORK, PC_SHOP, MT_CAR, (17, 0), (17, 15))
    d.trip("shop_1", "home_q", PC_SHOP, PC_HOME, MT_CAR, (17, 45), (18, 0))

    # HH 18 - loop trip (home -> home single trip) -> tour_data_quality LOOP_TRIP.
    s.household(18, "home_r", income=INC_UNDER_25K, veh=0, nworkers=0)
    s.person(1801, 18, 1, AGE_25_TO_34, FEMALE, EMP_NOT_LOOKING, STU_NONSTUDENT, cf=CF_NEVER)
    s.day(1801, 18, DAY_DATE_1, DOW_MONDAY).trip(
        "home_r", "home_r", PC_HOME, PC_HOME, MT_WALK, (10, 0), (10, 30), m1=MODE_WALK
    )

    # HH 19 - change-mode-as-primary-purpose (tour_data_quality CHANGE_MODE +
    # tour_purpose CHANGE_MODE). The >180 min gap prevents the two segments from
    # linking, so the CHANGE_MODE stop survives to tour level (a linking failure).
    s.household(19, "home_a", income=INC_100_200K)
    s.person(
        1901,
        19,
        1,
        AGE_35_TO_44,
        MALE,
        EMP_FULLTIME,
        STU_NONSTUDENT,
        wloc="work_3",
        jt=JOB_FIXED,
        wp=WP_FREE,
        wm=MT_CAR,
        cf=CF_5_DAYS,
    )
    s.day(1901, 19, DAY_DATE_1, DOW_MONDAY).round_trip(
        "home_a", "pr_1", PC_HOME, PC_CHANGE_MODE, MT_CAR, ((8, 0), (8, 20)), ((12, 0), (12, 20))
    )

    # HH 20 - day that never touches home (work -> meal -> gym) ->
    # tour_data_quality MISSING_ANCHOR, tour_category PARTIAL_BOTH.
    s.household(20, "home_b", income=INC_50_75K)
    s.person(
        2001,
        20,
        1,
        AGE_25_TO_34,
        FEMALE,
        EMP_FULLTIME,
        STU_NONSTUDENT,
        wloc="work_1",
        jt=JOB_FIXED,
        wp=WP_FREE,
        wm=MT_CAR,
        cf=CF_5_DAYS,
    )
    d = s.day(2001, 20, DAY_DATE_1, DOW_MONDAY)
    d.trip("work_1", "meal_1", PC_WORK, PC_MEAL, MT_CAR, (12, 0), (12, 20))
    d.trip("meal_1", "gym_1", PC_MEAL, PC_SOCIALREC, MT_CAR, (13, 30), (13, 50))

    # HH 21 - partial-start tour: the day's first trip starts away from home (an
    # overnight worker heading out, then home via an errand) and ends home. Needs
    # 2+ trips (a single trip would be forced to PARTIAL_BOTH) -> PARTIAL_START.
    s.household(21, "home_c", income=INC_50_75K)
    s.person(
        2101,
        21,
        1,
        AGE_35_TO_44,
        MALE,
        EMP_FULLTIME,
        STU_NONSTUDENT,
        wloc="work_2",
        jt=JOB_FIXED,
        wp=WP_FREE,
        wm=MT_CAR,
        cf=CF_5_DAYS,
    )
    d = s.day(2101, 21, DAY_DATE_1, DOW_MONDAY)
    d.trip("work_2", "errand_1", PC_WORK, PC_ERRAND, MT_CAR, (0, 30), (0, 50))
    d.trip("errand_1", "home_c", PC_ERRAND, PC_HOME, MT_CAR, (1, 0), (1, 20))

    # HH 22 - partial-end tour (home -> work -> social; ends away from home) ->
    # tour_category PARTIAL_END.
    s.household(22, "home_d", income=INC_100_200K)
    s.person(
        2201,
        22,
        1,
        AGE_45_TO_54,
        FEMALE,
        EMP_FULLTIME,
        STU_NONSTUDENT,
        wloc="work_1",
        jt=JOB_FIXED,
        wp=WP_FREE,
        wm=MT_CAR,
        cf=CF_5_DAYS,
    )
    d = s.day(2201, 22, DAY_DATE_1, DOW_MONDAY)
    d.trip("home_d", "work_1", PC_HOME, PC_WORK, MT_CAR, (8, 0), (8, 30))
    d.trip("work_1", "social_1", PC_WORK, PC_SOCIALREC, MT_CAR, (17, 0), (17, 30))

    # HH 23 - 3-person joint trip with a child -> joint composition
    # ADULTS_AND_CHILDREN. All three travel home->shop->home together.
    s.household(23, "home_e", income=INC_100_200K, veh=2, npeople=3, nworkers=2)
    for pn, pid, age in [(1, 2301, AGE_35_TO_44), (2, 2302, AGE_35_TO_44), (3, 2303, AGE_5_TO_15)]:
        child = age == AGE_5_TO_15
        s.person(
            pid,
            23,
            pn,
            age,
            FEMALE if pn == 2 else MALE,
            EMP_NOT_LOOKING if child else EMP_FULLTIME,
            STU_FULLTIME if child else STU_NONSTUDENT,
            st=SCH_ELEMENTARY if child else None,
            proxy=YES if child else NO,
            race=RACE_AFAM,
        )
        s.day(pid, 23, DAY_DATE_SAT, DOW_SATURDAY, pnum=pn).round_trip(
            "home_e",
            "shop_1",
            PC_HOME,
            PC_SHOP,
            MT_CAR,
            ((11, 0), (11, 20)),
            ((12, 30), (12, 50)),
            ntrav=3,
        )

    # HH 24 - two children travel together with no adult. The morning school run is
    # a *mandatory* joint tour, which CT-RAMP cannot represent: it is reclassified to
    # individual tours by identify_misclassified_joint_tours (negative control). The
    # afternoon shop tour is non-mandatory and admissible, so it is what actually
    # exercises joint composition CHILDREN_ONLY.
    s.household(24, "home_f", income=INC_75_100K, npeople=2, nworkers=0)
    for pn, pid in [(1, 2401), (2, 2402)]:
        s.person(
            pid,
            24,
            pn,
            AGE_5_TO_15,
            MALE if pn == 1 else FEMALE,
            EMP_NOT_LOOKING,
            STU_FULLTIME,
            sloc="school_1",
            st=SCH_ELEMENTARY,
            proxy=YES,
        )
        s.day(pid, 24, DAY_DATE_1, DOW_MONDAY, pnum=pn).round_trip(
            "home_f",
            "school_1",
            PC_HOME,
            PC_SCHOOL,
            MT_WALK,
            ((8, 0), (8, 20)),
            ((15, 0), (15, 20)),
            ntrav=2,
            m1=MODE_WALK,
        ).round_trip(
            "home_f",
            "shop_1",
            PC_HOME,
            PC_SHOP,
            MT_WALK,
            ((16, 0), (16, 20)),
            ((17, 0), (17, 20)),
            ntrav=2,
            m1=MODE_WALK,
        )

    # HH 25 - two household members make the same home->shop->home trip but SIX
    # HOURS APART -> must NOT be detected as joint (temporal-overlap negative control).
    s.household(25, "home_g", income=INC_50_75K, npeople=2)
    for pn, pid, hour in [(1, 2501, 8), (2, 2502, 14)]:
        s.person(
            pid,
            25,
            pn,
            AGE_45_TO_54,
            MALE if pn == 1 else FEMALE,
            EMP_NOT_LOOKING,
            STU_NONSTUDENT,
            cf=CF_NEVER,
        )
        s.day(pid, 25, DAY_DATE_1, DOW_MONDAY, pnum=pn).round_trip(
            "home_g",
            "shop_2",
            PC_HOME,
            PC_SHOP,
            MT_WALK,
            ((hour, 0), (hour, 20)),
            ((hour + 1, 0), (hour + 1, 20)),
            m1=MODE_WALK,
        )

    return s.hhs, s.pers, s.days, s.trips


def build_survey_dataframes():
    """Return canonical survey data as a dict of Polars DataFrames.

    Keys: households, persons, days, unlinked_trips
    """
    hh, per, day, trip = _build_records()
    households = pl.DataFrame(hh)

    # Inject a few more MISSING income bins (HH12 is already missing) so the
    # imputation step has several values to fill and enough complete rows to
    # train on. This gives the e2e a non-degenerate RF imputation to exercise
    # the stash / feature-importance code path (quality is unit-tested elsewhere).
    _income_missing_hh = {4, 7, 8, 12}
    households = households.with_columns(
        income_bin=pl.when(pl.col("hh_id").is_in(_income_missing_hh))
        .then(pl.lit(INC_MISSING))
        .otherwise(pl.col("income_bin"))
    )

    # Representative continuous income from the income bin (null when bin is MISSING).
    # The daysim/ctramp formatters read a raw `income` column in addition to income_bin.
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

    tables = {
        "households": households,
        "persons": pl.DataFrame(per),
        "days": pl.DataFrame(day),
        "unlinked_trips": pl.DataFrame(trip),
    }

    # The e2e excludes compute_weights/add_existing_weights, but the daysim/ctramp
    # formatters derive expansion factors from the weight columns (e.g. 1 / hh_weight).
    # Populate them with a neutral 1.0 ("unweighted" → expansion factor 1) so the
    # formatters run without needing the weighting step.
    for tbl, wcol in (
        ("households", "hh_weight"),
        ("persons", "person_weight"),
        ("days", "day_weight"),
        ("unlinked_trips", "unlinked_trip_weight"),
    ):
        tables[tbl] = tables[tbl].with_columns(pl.lit(1.0).alias(wcol))

    return tables


# ---------------------------------------------------------------------------
# Zone GeoJSON
# ---------------------------------------------------------------------------

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
