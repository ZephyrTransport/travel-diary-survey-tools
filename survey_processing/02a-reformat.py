"""Map survey results CSVs to Daysim format"""

import argparse
from pathlib import Path

import numpy as np
import polars as pl
import tomllib

COUNTY_FIPS = 6 * 1000 + np.array([1, 13, 41, 55, 75, 81, 85, 95, 97])  # 6 = CA


def reformat(config):
    taz_spatial_join_dir = Path(config["01-taz_spatial_join"]["dir"])
    reformat_dir = Path(config["02a-reformat"]["dir"])
    reformat_dir.mkdir(exist_ok=True)

    person = reformat_person(
        taz_spatial_join_dir / config["person_filename"],
        load_day_completeness(Path(config["raw"]["dir"]) / config["day_filename"]),
        config["weighted"],
    )
    person.write_csv(reformat_dir / config["person_filename"])
    reformat_hh(
        taz_spatial_join_dir / config["hh_filename"],
        person,
        config["weighted"],
    ).write_csv(reformat_dir / config["hh_filename"])
    reformat_trip(
        taz_spatial_join_dir / config["trip_filename"],
        config["weighted"],
    ).write_csv(reformat_dir / config["trip_filename"])
    return


def load_day_completeness(in_day_filepath):
    # For 2022: even though it looks like we're calculating the person-day completeness,
    # a person is only 'complete' on a day if the entire hh is complete on that day.
    # so we should be looking at the `hh_day_complete` col, not the `is_complete` col
    return (
        pl.read_csv(in_day_filepath, columns=["person_id", "is_complete", "travel_dow"])
        .pivot(index="person_id", on="travel_dow", values="is_complete")
        .fill_null(0)
        .with_columns(
            # person_id in day file is actually hhno*100 + pno
            hhno=(pl.col("person_id") // 100),
            pno=(pl.col("person_id") % 100),
            num_days_complete_3dayweekday=pl.sum_horizontal(["2", "3", "4"]),
            num_days_complete_4dayweekday=pl.sum_horizontal(["1", "2", "3", "4"]),
            num_days_complete_5dayweekday=pl.sum_horizontal(["1", "2", "3", "4", "5"]),
            # num_days_complete_weekend=pl.sum_horizontal(["6", "7"]),
        )
        # .with_columns(
        #     num_days_complete=pl.sum_horizontal(
        #         "num_days_complete_5dayweekday"#, "num_days_complete_weekend"
        #     )
        # )
        .select(
            ["hhno", "pno"]
            + ["1", "2", "3", "4", "5", "6", "7"]
            + [
                "num_days_complete_3dayweekday",
                "num_days_complete_4dayweekday",
                "num_days_complete_5dayweekday",
                # "num_days_complete",
            ]
        )
        .rename(
            {
                "1": "mon_complete",
                "2": "tue_complete",
                "3": "wed_complete",
                "4": "thu_complete",
                "5": "fri_complete",
                "6": "sat_complete",
                "7": "sun_complete",
            }
        )
    )


def reformat_person(in_person_filepath, day_with_completeness, weighted: bool):
    age_dict = {
        1: 3,
        2: 10,
        3: 16,  # 16-17
        4: 21,
        5: 30,
        6: 40,
        7: 50,
        8: 60,
        9: 70,
        10: 80,
        11: 90,  # 85+
    }
    gender_dict = {
        # currently, imputation codes non-binary (gender) into male/female
        # (gender_imputed), so let's keep using (non-imputed) gender for now
        1: 2,  # female
        2: 1,  # male
        4: 3,  # non-binary
        997: 3,  # other/self-describe
        995: 9,  # missing
        999: 9,  # prefer not to answer
    }
    student_dict = {
        0: 1,  # full-time, some/all in-person
        1: 2,  # part-time, some/all in-person
        2: 0,  # not student
        3: 2,  # part-time, remote only
        4: 1,  # full-time, remote only
        995: -1,  # missing
    }
    work_park_dict = {
        1: 0,  # free parking at work
        # ppaidpark = 1: paid parking at work
        2: 0, # employer pays all costs -> free parking at work # UPDATED DC 1/23/2025
        3: 1,
        4: 1,
        995: -1,  # missing
        996: -1,  # UPDATED DC 1/23/2025
        997: -1,  # never drive to work
        998: -1,  # don't know
    }
    residence_rent_own_dict = {
        1: 1,  # own
        2: 2,  # rent
        3: 3,  # provided by military -> other
        4: 3,  # provided by family/relative/freind rent-free -> other
        997: 3,  # other
        995: 9,  # missing
        999: 9,  # prefer not to answer -> missing
    }
    residence_type_dict = {
        1: 1,  # detached house
        2: 2,  # rowhouse/townhouse -> duplex/triplex/rowhouse
        3: 3,  # duplex/triplex/quads 2-4 units -> apt/condo
        4: 3,  # apt/condos 5-49 units
        5: 3,  # apt/condos 50+ units
        6: 3,  # senior/age-restricted apt/condos
        7: 4,  # manufactured/mobile home, trailer -> mobile home, trailer
        9: 5,  # dorm, group qarters, inst housing -> dorm/rented room
        995: 9,  # missing
        997: 6,  # other
    }
    person_out_cols = [
        "hhno",
        "pno",
        "pptyp",
        "pagey",
        "pgend",
        "pwtyp",
        "pwpcl",
        "pwtaz",
        "pstyp",
        "pspcl",
        "pstaz",
        "ppaidprk",
        "pwxcord",
        "pwycord",
        "psxcord",
        "psycord",
        "pownrent",  # for joining to hh table later (usu in hh for Daysim)
        "prestype",  # for joining to hh table later (usu in hh for Daysim)
        "mon_complete",
        "tue_complete",
        "wed_complete",
        "thu_complete",
        "fri_complete",
        "sat_complete",
        "sun_complete",
        "num_days_complete_3dayweekday",  # depends on hh completeness (in 2022)
        "num_days_complete_4dayweekday",  # depends on hh completeness (in 2022)
        "num_days_complete_5dayweekday",  # depends on hh completeness (in 2022)
        "num_days_complete",
    ]
    if weighted:
        person_out_cols.append("person_weight")
    person = (
        pl.read_csv(
            in_person_filepath,
            schema_overrides={
                "hh_id": int,
                "person_num": int,
                "person_id": int,
                "pgend": int,
            },
        )
        .cast(
            {
                "work_taz": int,
                "work_maz": int,
                "school_taz": int,
                "school_maz": int,
            }
        )
        .rename(
            {
                "hh_id": "hhno",
                "person_num": "pno",
                "work_lon": "pwxcord",
                "work_lat": "pwycord",
                "school_lon": "psxcord",
                "school_lat": "psycord",
                "work_taz": "pwtaz",
                "work_maz": "pwpcl",
                "school_taz": "pstaz",
                "school_maz": "pspcl",
            }
        )
        .with_columns(
            pl.col(["pwxcord", "pwycord", "psxcord", "psycord"]).fill_null(-1),
            pagey=pl.col("age").replace(age_dict),
            # currently, imputation codes non-binary (gender) into male/female
            # (gender_imputed), so let's keep using (non-imputed) gender for now
            pgend=pl.col("gender").replace(gender_dict),
            # NOTE pstyp/student: bad logic for the 0 as default! (copied from 2019)
            # since the student var only applies to people above 16
            pstyp=pl.col("student").replace(student_dict).fill_null(0),
            # ppaidprk: paid parking at workplace?
            ppaidprk=pl.col("work_park").replace(work_park_dict),
            # pownrent & prestype: for joining to hh table (to conform to Daysim)
            pownrent=pl.col("residence_rent_own").replace(residence_rent_own_dict),
            prestype=pl.col("residence_type").replace(residence_type_dict),
            num_days_complete=pl.col("num_days_complete").replace(
                {995: 0}  # missing -> 0
            ),
        )
        .with_columns(
            pptyp=pl.when(pl.col("pagey") < 5)
            .then(pl.lit(8))  # child 0-4
            .when(pl.col("pagey") < 16)
            .then(pl.lit(7))  # child 5-15
            # only if age >= 16:
            .when(pl.col("employment").is_in([1, 3, 8]))  # employed full-time, primarily self employed, employed, but not currently working (e.g., on leave, furloughed 100%) # UPDATED DC 1/23/2025
            .then(pl.lit(1))  # full-time worker
            # all cases below: if not full-time employed and age >= 16:
            .when(
                (pl.col("pagey") < 18)  # and age >= 16
                # full/part-time student, in-person & remote
                & (pl.col("student").is_in([0, 1, 3, 4]))
            )
            .then(pl.lit(6))  # high school 16+
            .when(
                (pl.col("pagey") < 25)  # and age >= 16
                & (pl.col("school_type")).is_in([4, 7])  # home school, high school
                # full/part-time student, in-person & remote
                & (pl.col("student").is_in([0, 1, 3, 4]))
            )
            .then(pl.lit(6))  # high school 16+
            # logic below is for age 18-65
            .when(
                # for age >= 18:
                # full/part-time student, in-person & remote
                pl.col("student").is_in([0, 1, 3, 4])
            )
            .then(pl.lit(5))  # university student
            # part-time / self employed / unpaid volunteer or intern /
            # NOTE note the categories we're counting as part-time workers here
            .when(pl.col("employment").is_in([2, 3, 7])) # UPDATED DC 1/23/2025
            .then(pl.lit(2))  # part-time worker
            .when(pl.col("pagey") < 65)
            .then(pl.lit(4))  # non-working age < 65
            .otherwise(pl.lit(3)),  # non-working age 65+
        )
        .with_columns(
            pwtyp=pl.when(pl.col("pptyp").is_in([1, 2]))
            .then(pl.col("pptyp"))  # direct mapping pptyp -> pwtyp for 1 or 2
            # categorize student workers as paid part-time workers
            .when(
                pl.col("pptyp").is_in([5, 6])  # student: uni or high school 16+
                # employed: full-time, part-time, self-
                & pl.col("employment").is_in([1, 2, 3])
            )
            .then(pl.lit(2))  # paid part time
            .otherwise(pl.lit(0))
        )
        .with_columns(
            # pwtaz, pstaz, pwpcl, pspcl: only keep those within Bay Area
            # p{w, s}{taz, pcl, xcord, ycord}: in previous surveys,
            # some persons are not workers/students but have school loc:
            # account for that by setting school loc to null/missing
            pwtaz=pl.when(
                pl.col(["work_county"]).is_in(COUNTY_FIPS) & (pl.col("pwtyp") != 0)
            )
            .then(pl.col("pwtaz"))
            .otherwise(pl.lit(-1)),
            pwpcl=pl.when(
                pl.col(["work_county"]).is_in(COUNTY_FIPS) & (pl.col("pwtyp") != 0)
            )
            .then(pl.col("pwpcl"))
            .otherwise(pl.lit(-1)),
            pwxcord=pl.when(pl.col("pwtyp") != 0)
            .then(pl.col("pwxcord"))
            .otherwise(pl.lit(-1)),
            pwycord=pl.when(pl.col("pwtyp") != 0)
            .then(pl.col("pwycord"))
            .otherwise(pl.lit(-1)),
            pstaz=pl.when(
                pl.col(["school_county"]).is_in(COUNTY_FIPS) & (pl.col("pstyp") != 0)
            )
            .then(pl.col("pstaz"))
            .otherwise(pl.lit(-1)),
            pspcl=pl.when(
                pl.col(["school_county"]).is_in(COUNTY_FIPS) & (pl.col("pstyp") != 0)
            )
            .then(pl.col("pspcl"))
            .otherwise(pl.lit(-1)),
            psxcord=pl.when(pl.col("pstyp") != 0)
            .then(pl.col("psxcord"))
            .otherwise(pl.lit(-1)),
            psycord=pl.when(pl.col("pstyp") != 0)
            .then(pl.col("psycord"))
            .otherwise(pl.lit(-1)),
        )
        .join(day_with_completeness, on=["hhno", "pno"], how="left")
        .select(person_out_cols)
        .sort(by=["hhno", "pno"])
    )
    return person


def reformat_hh(in_hh_filepath, person, weighted: bool):
    # TODO or should we just use imputed income?
    income_detailed_dict = {
        999: -1,
        1: 7500,
        2: 20000,
        3: 30000,
        4: 42500,
        5: 62500,
        6: 87500,
        7: 125000,
        8: 175000,
        9: 225000,
        10: 350000,  # 250k+
    }
    income_followup_dict = {
        999: -1,
        1: 12500,
        2: 37500,
        3: 62500,
        4: 87500,
        5: 150000,  # in 2019, this was 175000
        6: 250000,  # 200k+; in 2019, this was 350000
    }
    hh_out_cols = [
        "hhno",
        "hhsize",
        "hhvehs",
        "hhincome",
        "hownrent",
        "hrestype",
        "hhparcel",
        "hhtaz",
        "hxcord",
        "hycord",
    ]
    if weighted:
        hh_out_cols.append("hh_weight")
    person = (
        person.select("hhno", "pownrent", "prestype")
        .group_by("hhno")
        # from spot checking the data, the first person in the houehold has the values
        # for the ownrent and restype columns; the remaining members of the houeholds
        # has the value 995 (missing)
        .first()
        .rename({"pownrent": "hownrent", "prestype": "hrestype"})
    )
    hh = (
        pl.read_csv(in_hh_filepath)
        .rename(
            {
                "hh_id": "hhno",
                "home_maz": "hhparcel",
                "home_taz": "hhtaz",
                "home_lon": "hxcord",
                "home_lat": "hycord",
                "num_people": "hhsize",
                "num_vehicles": "hhvehs",
            }
        )
        .with_columns(
            pl.col("income_detailed").replace(income_detailed_dict),
            pl.col("income_followup").replace(income_followup_dict),
        )
        .with_columns(
            # replace income_detailed with income_followup if income_detailed == -1
            # note though that income_followup could also be -1
            pl.when(pl.col("income_detailed") > 0)
            .then(pl.col("income_detailed"))
            .otherwise(pl.col("income_followup"))
            .alias("hhincome"),
        )
        .join(person, on="hhno", how="left")
        .select(hh_out_cols)
        .sort(by="hhno")
    )
    return hh


def reformat_trip(in_trip_filepath, weighted: bool):
    purpose_dict = {
        -1: -1,  # not imputable -> missing
        995: -1,  # missing -> missing
        1: 0,  # home -> home
        2: 1,  # work -> work
        3: 4,  # work-related -> personal business (for tour-building reasons) # UPDATED DC 1/23/2025
        4: 2,  # school -> school
        5: 2,  # school related -> school
        6: 3,  # escort -> escort
        7: 5,  # shop -> shop
        8: 6,  # meal -> meal
        9: 7,  # socrec -> socrec
        10: 4,  # errand -> pers.bus
        11: 10,  # change mode -> change mode
        12: 11,  # overnight non-home -> other
        13: 11,  # other -> other
    }
    trip_out_cols = [
        "hhno",
        "pno",
        "tripno",
        "dow",
        "opurp",
        "dpurp",
        "opcl",
        "otaz",
        "dpcl",
        "dtaz",
        "mode",
        "path",
        "dorp",
        "deptm",
        "arrtm",
        "oxcord",
        "oycord",
        "dxcord",
        "dycord",
        "mode_type",
    ]
    if weighted:
        trip_out_cols.append("trip_weight")
    trip = (
        pl.read_csv(
            in_trip_filepath,
            schema_overrides={"person_id": int, "opurp": int, "dpurp": int},
        )
        .cast(
            {
                "o_taz": int,
                "o_maz": int,
                "d_taz": int,
                "d_maz": int,
            }
        )
        .rename(
            {
                "hh_id": "hhno",
                "person_num": "pno",
                "o_taz": "otaz",
                "o_maz": "opcl",
                "d_taz": "dtaz",
                "d_maz": "dpcl",
                "o_lon": "oxcord",
                "o_lat": "oycord",
                "d_lon": "dxcord",
                "d_lat": "dycord",
                "trip_num": "tripno",
                "travel_dow": "dow",
            }
        )
        # retain only trips in complete person-days
        .filter(pl.col("day_is_complete") == 1)
        .with_columns(
            pl.col(["oxcord", "oycord", "dxcord", "dycord"]).fill_null(-1),
            # NOTE deptm/arrtm are NOT using standard Daysim definitions
            deptm=(pl.col("depart_hour") * 100 + pl.col("depart_minute")),
            arrtm=(pl.col("arrive_hour") * 100 + pl.col("arrive_minute")),
            # {o, d}{taz, pcl}: only keep those within Bay Area
            otaz=pl.when(pl.col(["o_county"]).is_in(COUNTY_FIPS))
            .then(pl.col("otaz"))
            .otherwise(pl.lit(-1)),
            opcl=pl.when(pl.col(["o_county"]).is_in(COUNTY_FIPS))
            .then(pl.col("opcl"))
            .otherwise(pl.lit(-1)),
            dtaz=pl.when(pl.col(["d_county"]).is_in(COUNTY_FIPS))
            .then(pl.col("dtaz"))
            .otherwise(pl.lit(-1)),
            dpcl=pl.when(pl.col(["d_county"]).is_in(COUNTY_FIPS))
            .then(pl.col("dpcl"))
            .otherwise(pl.lit(-1)),
            opurp=pl.col("o_purpose_category").replace(purpose_dict),
            dpurp=pl.col("d_purpose_category").replace(purpose_dict),
            # Daysim mode:
            # 0-other 1-walk 2-bike 3-DA 4-hov2 5-hov3
            # 6-walktran 7-drivetran 8-schbus 9-tnc
            mode=pl.when(pl.col("mode_type") == 1)  # walk
            .then(pl.lit(1))
            .when(pl.col("mode_type").is_in([2, 3, 4]))  # bike, bike/scooter-share
            .then(pl.lit(2))
            .when(pl.col("mode_type").is_in([8, 9]))  # car, carshare
            .then(
                pl.when(pl.col("num_travelers") == 1)
                .then(pl.lit(3))
                .when(pl.col("num_travelers") == 2)
                .then(pl.lit(4))
                .when(pl.col("num_travelers") > 2)
                .then(pl.lit(5))
                # do we need to check to see num_travelers always > 0?
            )
            .when(pl.col("mode_type").is_in([5, 6]))  # taxi, tnc
            .then(pl.lit(9))
            .when(pl.col("mode_type") == 10)  # school bus
            .then(pl.lit(8))
            .when(pl.col("mode_type") == 11)  # shuttle/vanpool
            .then(pl.lit(5))  # make shuttle/vanpool HOV3+
            .when(
                pl.col("mode_type").is_in([12, 13])  # ferry, transit
                | (
                    (pl.col("mode_type") == 14)  # long distance pax
                    & (pl.col("mode_1") == 41)  # intercity/commuter rail
                )
            )
            .then(
                pl.when(
                    # 6: uber/lyft, taxi, car service;
                    # 7: drove / 8: parked / 9, 10: dropped off in vehicle/motorcycle
                    pl.col("transit_access").is_in([6, 7, 8, 9, 10])
                    | pl.col("transit_egress").is_in([6, 7, 8, 9, 10])
                )
                .then(pl.lit(7))  # drivetran
                .otherwise(pl.lit(6))  # walktran
            )
            .otherwise(pl.lit(0)),
        )
        .with_columns(
            # Daysim pathtype
            # 0-none 1-fullnetwork 2-no-toll network 3-bus 4-lrt
            # 5-premium 6-BART 7-ferry
            # (NOTE Daysim userguide says 5 = premium bus and 6 = commuter rail, but I
            # kept the 2019 ligic that kept 6 as only BART and grouped commuter rail
            # under 5)
            path=pl.when(pl.col("mode_type") == 8)  # car
            .then(pl.lit(1))  # full network
            .when(pl.col("mode").is_in([6, 7]))  # Daysim mode: transit
            .then(
                pl.when(
                    pl.any_horizontal(
                        # ferry / water taxi
                        pl.col("mode_1", "mode_2", "mode_3", "mode_4") == 78
                    )
                )
                .then(pl.lit(7))  # ferry
                .when(
                    pl.any_horizontal(
                        pl.col("mode_1", "mode_2", "mode_3", "mode_4") == 30  # BART
                    )
                )
                .then(pl.lit(6))  # see notes above
                .when(
                    pl.any_horizontal(
                        pl.col("mode_1", "mode_2", "mode_3", "mode_4").is_in(
                            # intercity rail, other rail, express/transbay bus
                            [41, 42, 55]
                        )
                    )
                )
                .then(pl.lit(5))  # see notes above
                .when(
                    pl.any_horizontal(
                        pl.col("mode_1", "mode_2", "mode_3", "mode_4").is_in(
                            [
                                53,  # MUNI Metro
                                # Rail (e.g. train, light rail, trolley, BART, MUNI
                                # Metro) [even though BART has its own mode code 30]
                                105,
                                # cable car, streetcar
                                68,
                            ]
                        )
                    )
                )
                .then(pl.lit(4))  # LRT
                .otherwise(pl.lit(3))  # bus
            )
            # NOTE `otherwise` includes taxi, shuttle/vanpool, TNC, carshare, schoolbus
            .otherwise(pl.lit(0)),  # none
            dorp=pl.when(pl.col("mode").is_in([3, 4, 5]))  # sov, hov 2/3+
            .then(
                pl.when(pl.col("driver").is_in([1,3]))  # driver, switched driver/pax  # UPDATED DC 1/23/2025
                .then(pl.lit(1))  # driver
                .when(pl.col("driver") == 2)  # passenger
                .then(pl.lit(2))  # passenger
                # driver == 3: both driver and pax (switched drivers during trip)
                # -> NOTE also code as missing
                .otherwise(pl.lit(9))  # missing (for all remaining car trips)
            )
            .when(pl.col("mode") == 9)  # TNC
            .then(
                pl.when(pl.col("num_travelers") == 1)
                .then(pl.lit(11))
                .when(pl.col("num_travelers") == 2)
                .then(pl.lit(12))
                .when(pl.col("num_travelers") > 2)
                .then(pl.lit(13))
            )
            .otherwise(pl.lit(3)),  # N/A
        )
        .select(trip_out_cols)
        .sort(by=["hhno", "pno", "tripno"])
    )
    return trip


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config_filepath")
    args = parser.parse_args()
    with open(args.config_filepath, "rb") as f:
        config = tomllib.load(f)
    reformat(config)
