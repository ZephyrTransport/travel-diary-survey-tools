import argparse
import tomllib
from pathlib import Path

import numpy as np
import pandas as pd

# TODO update the pd _append logic to not do appends (deprecated)


def link_trips_week(config):
    trip = pd.read_csv(Path(config["02a-reformat"]["dir"]) / config["trip_filename"])
    link_trips_week_dir = Path(config["02b-link_trips_week"]["dir"])
    link_trips_week_dir.mkdir(exist_ok=True)

    ORIG_COLS = trip.columns

    # flag the first trip of each day.
    min_trips = (
        trip[["hhno", "pno", "dow", "tripno"]]
        .groupby(["hhno", "pno", "dow"])
        .min()
        .reset_index()
    )
    min_trips = min_trips.rename(columns={"tripno": "tripno_min"})
    trip = trip.merge(min_trips, how="left", on=["hhno", "pno", "dow"])
    trip["first_ofday"] = 0
    trip.loc[trip["tripno"] == trip["tripno_min"], "first_ofday"] = 1

    # flag the last trip of each day
    max_trips = (
        trip[["hhno", "pno", "dow", "tripno"]]
        .groupby(["hhno", "pno", "dow"])
        .max()
        .reset_index()
    )
    max_trips = max_trips.rename(columns={"tripno": "tripno_max"})
    trip = trip.merge(max_trips, how="left", on=["hhno", "pno", "dow"])
    trip["last_ofday"] = 0
    trip.loc[trip["tripno"] == trip["tripno_max"], "last_ofday"] = 1

    # flag the last trip of the person
    max_trips = (
        trip[["hhno", "pno", "tripno"]].groupby(["hhno", "pno"]).max().reset_index()
    )
    max_trips = max_trips.rename(columns={"tripno": "tripno_max_per"})
    trip = trip.merge(max_trips, how="left", on=["hhno", "pno"])
    trip["last_ofper"] = 0
    trip.loc[trip["tripno"] == trip["tripno_max_per"], "last_ofper"] = 1

    # append the next trips' attributes to this record
    trips_nxt = trip.copy()
    trips_nxt["tripno"] = trips_nxt["tripno"] - 1
    NXT_COLS = [
        "dpurp",
        "dpcl",
        "dtaz",
        "mode",
        "path",
        "deptm",
        "arrtm",
        "dxcord",
        "dycord",
        "first_ofday",
        "mode_type",
    ]
    trips_nxt = trips_nxt[["hhno", "pno", "tripno"] + NXT_COLS]

    col_dict = {}
    for col in NXT_COLS:
        col_dict[col] = col + "_nxt"
    trips_nxt = trips_nxt.rename(columns=col_dict)
    trip = trip.merge(trips_nxt, how="left", on=["hhno", "pno", "tripno"])

    # calculate activity duration in minutes
    trip.loc[pd.isna(trip["deptm_nxt"]), "deptm_nxt"] = 0
    trip["act_dur"] = (
        (trip["deptm_nxt"] / 100).astype(int) * 60
        + trip["deptm_nxt"]
        - (trip["deptm_nxt"] / 100).astype(int) * 100
        - (trip["arrtm"] / 100).astype(int) * 60
        - trip["arrtm"]
        + (trip["arrtm"] / 100).astype(int) * 100
    )

    trip.loc[
        (trip["last_ofday"] == 1)
        & (trip["first_ofday_nxt"] == 1)
        & (trip["act_dur"] < 0),
        "act_dur",
    ] += 1440

    # print(trip.loc[(trip['dpurp']==10) & ((trip['last_ofday']==0) | ((trip['last_ofday']==1) & (trip['first_ofday_nxt']==1))), 'act_dur'].describe())
    # trip linking parameters
    ACT_DUR_LIMIT = 35
    ACT_DUR_LIMIT2 = 15
    WALK_MODES = [0, 1, 2]
    DRIVE_MODES = [3, 4, 5, 9]

    delete_list = []
    accegr_df = pd.DataFrame(
        columns=[
            "hhno",
            "pno",
            "dow",
            "tripno",
            "mode",
            "otaz",
            "dtaz",
            "acc_mode",
            "egr_mode",
        ]
    )
    
    tmp_dict = {}
    tmp_flag = False

    def merge_trips(rownum, skip, mode, tmp_flag):
        #global tmp_flag
        if skip == 1 and mode in [6, 7]:
            tmp_dict["hhno"] = [trip.loc[rownum, "hhno"]]
            tmp_dict["pno"] = [trip.loc[rownum, "pno"]]
            tmp_dict["dow"] = [trip.loc[rownum, "dow"]]
            tmp_dict["tripno"] = [trip.loc[rownum, "tripno"]]
            tmp_dict["mode"] = [mode]
            tmp_dict["otaz"] = [trip.loc[rownum, "otaz"]]
            tmp_dict["dtaz"] = [trip.loc[rownum, "dtaz_nxt"]]
            tmp_dict["acc_mode"] = [trip.loc[rownum, "mode_type"]]
            tmp_dict["egr_mode"] = [trip.loc[rownum, "mode_type_nxt"]]
            tmp_flag = True
        elif tmp_flag:
            tmp_dict["mode"] = [mode]
            tmp_dict["dtaz"] = [trip.loc[rownum, "dtaz_nxt"]]
            tmp_dict["egr_mode"] = [trip.loc[rownum, "mode_type_nxt"]]

        trip.loc[rownum, "dpurp"] = trip.loc[rownum, "dpurp_nxt"]
        trip.loc[rownum, "dpcl"] = trip.loc[rownum, "dpcl_nxt"]
        trip.loc[rownum, "dtaz"] = trip.loc[rownum, "dtaz_nxt"]
        trip.loc[rownum, "arrtm"] = trip.loc[rownum, "arrtm_nxt"]
        trip.loc[rownum, "dxcord"] = trip.loc[rownum, "dxcord_nxt"]
        trip.loc[rownum, "dycord"] = trip.loc[rownum, "dycord_nxt"]

        trip.loc[rownum, "mode"] = mode
        trip.loc[rownum, "path"] = max(
            trip.loc[rownum, "path"], trip.loc[rownum, "path_nxt"]
        )

        trip.loc[rownum, "last_ofper"] = trip.loc[rownum + skip, "last_ofper"]
        trip.loc[rownum, "dpurp_nxt"] = trip.loc[rownum + skip, "dpurp_nxt"]
        trip.loc[rownum, "dpcl_nxt"] = trip.loc[rownum + skip, "dpcl_nxt"]
        trip.loc[rownum, "dtaz_nxt"] = trip.loc[rownum + skip, "dtaz_nxt"]
        trip.loc[rownum, "deptm_nxt"] = trip.loc[rownum + skip, "deptm_nxt"]
        trip.loc[rownum, "arrtm_nxt"] = trip.loc[rownum + skip, "arrtm_nxt"]
        trip.loc[rownum, "act_dur"] = trip.loc[rownum + skip, "act_dur"]
        trip.loc[rownum, "mode_nxt"] = trip.loc[rownum + skip, "mode_nxt"]
        trip.loc[rownum, "path_nxt"] = trip.loc[rownum + skip, "path_nxt"]
        trip.loc[rownum, "dxcord_nxt"] = trip.loc[rownum + skip, "dxcord_nxt"]
        trip.loc[rownum, "dycord_nxt"] = trip.loc[rownum + skip, "dycord_nxt"]
        trip.loc[rownum, "mode_type_nxt"] = trip.loc[rownum + skip, "mode_type_nxt"]

        delete_list.append(
            [
                trip.loc[rownum + skip, "hhno"],
                trip.loc[rownum + skip, "pno"],
                trip.loc[rownum + skip, "dow"],
                trip.loc[rownum + skip, "tripno"],
            ]
        )
        return tmp_flag

    # loop through trips 
    i = 0
    while i < len(trip):
        if i % 1000 == 0:
            print(i)

        if i > 0:
            prev_hhno = trip.loc[i, "hhno"]
            prev_pno = trip.loc[i, "pno"]
            prev_dow = trip.loc[i, "dow"]
            dow_diff = trip.loc[i, "dow"] - trip.loc[i - 1, "dow"]
        else:
            prev_hhno = 0
            prev_pno = 0
            prev_dow = 0
            dow_diff = 0

        hhno = trip.loc[i, "hhno"]
        pno = trip.loc[i, "pno"]
        dow = trip.loc[i, "dow"]
        tripno = trip.loc[i, "tripno"]

        #     if hhno==181076628:
        #         print('hello')

        act_dur = trip.loc[i, "act_dur"]
        mode = trip.loc[i, "mode"]
        path = trip.loc[i, "path"]
        mode_nxt = trip.loc[i, "mode_nxt"]
        path_nxt = trip.loc[i, "path_nxt"]

        dpurp = trip.loc[i, "dpurp"]
        
        # Handle change-mode exceptions.  
        # Not a change mode.  Move on.  
        if dpurp != 10:
            if (
                hhno == prev_hhno
                and pno == prev_pno
                and dow_diff <= 1
                and trip.loc[i, "opurp"] == 10
            ):
                trip.loc[i, "opurp"] = 4
            i += 1
            continue
        # Last trip of the person, it can't be a change mode. Recode it. 
        elif trip.loc[i, "last_ofper"] == 1 and dpurp == 10:
            trip.loc[i, "dpurp"] = 4  # just assume this is personal business
            if tmp_flag:
                accegr_df = pd.concat([accegr_df, pd.DataFrame(tmp_dict)])
                tmp_flag = False
            i += 1
            continue
        # Last trip of the day can't be change mode.  Recode it.  
        elif trip.loc[i, "last_ofday"] == 1 and np.isnan(trip.loc[i, "dpurp_nxt"]):
            trip.loc[i, "dpurp"] = 4  # just assume this is personal business
            if tmp_flag:
                accegr_df = pd.concat([accegr_df, pd.DataFrame(tmp_dict)])
                tmp_flag = False
            i += 1
            continue

        #     if hhno==181076628 and tripno==1:
        #         print('hello')

        # Now, we have a hit a record for which destination purpose is change_mode (dpurp = 10)

        j = 1
        # Merge access, egress, and sequential transit trips where the activity duration < ACT_DUR_LIMIT
        # walk + walk-transit
        if (
            (mode in WALK_MODES and mode_nxt == 6)    
            or (mode == 6 and mode_nxt in WALK_MODES) 
        ) and act_dur <= ACT_DUR_LIMIT:
            tmp_flag = merge_trips(i, j, 6, tmp_flag)
            j += 1
        # walk, drive, and drive-transit (drive-transit may have a walk acc/egr on one end).
        elif (
            (mode in WALK_MODES + DRIVE_MODES and mode_nxt in [6, 7])
            or (mode in [6, 7] and mode_nxt in WALK_MODES + DRIVE_MODES)
        ) and act_dur <= ACT_DUR_LIMIT:
            tmp_flag = merge_trips(i, j, 7, tmp_flag)
            j += 1
        # merge sequential transit trips
        elif mode in [6, 7] and mode_nxt in [6, 7] and act_dur <= ACT_DUR_LIMIT:
            tmp_flag = merge_trips(i, j, max(mode, mode_nxt), tmp_flag)
            j += 1
        # merge remaining trips less than ACT_DUR_LIMIT2
        elif act_dur <= ACT_DUR_LIMIT2:
            tmp_flag = merge_trips(i, j, max(mode, mode_nxt), tmp_flag)
            j += 1
        else:
            #       print('check this case in initial: %s, %s' %(hhno, tripno))
            trip.loc[i, "dpurp"] = 4  # just assume this is personal business
            if tmp_flag:
                accegr_df = pd.concat([accegr_df, pd.DataFrame(tmp_dict)])
                tmp_flag = False
            i += 1
            continue

        # we've merged 2 trips... keep going until we run out of change-mode in this trip sequence
        final_dpurp = trip.loc[i, "dpurp"]
        while (
            final_dpurp == 10
            and trip.loc[i, "last_ofper"] == 0
            and pd.notnull(trip.loc[i, "dpurp_nxt"])
        ):
            act_dur = trip.loc[i, "act_dur"]
            mode = trip.loc[i, "mode"]
            path = trip.loc[i, "path"]
            mode_nxt = trip.loc[i, "mode_nxt"]
            path_nxt = trip.loc[i, "path_nxt"]

            if (
                (mode in WALK_MODES and mode_nxt == 6)
                or (mode == 6 and mode_nxt in WALK_MODES)
            ) and act_dur <= ACT_DUR_LIMIT:
                tmp_flag = merge_trips(i, j, 6, tmp_flag)
                j += 1
            elif (
                (mode in WALK_MODES + DRIVE_MODES and mode_nxt in [6, 7])
                or (mode in [6, 7] and mode_nxt in WALK_MODES + DRIVE_MODES)
            ) and act_dur <= ACT_DUR_LIMIT:
                tmp_flag = merge_trips(i, j, 7, tmp_flag)
                j += 1
            elif mode in [6, 7] and mode_nxt in [6, 7] and act_dur <= ACT_DUR_LIMIT:
                tmp_flag = merge_trips(i, j, max(mode, mode_nxt), tmp_flag)
                j += 1
            elif act_dur <= ACT_DUR_LIMIT2:
                tmp_flag = merge_trips(i, j, max(mode, mode_nxt), tmp_flag)
                j += 1
            else:
                #           print('check this case in loop: %s, %s' %(hhno, tripno))
                trip.loc[i, "dpurp"] = 4  # just assume this is personal business
                if tmp_flag:
                    accegr_df = pd.concat([accegr_df, pd.DataFrame(tmp_dict)])
                    tmp_flag = False
                break

            final_dpurp = trip.loc[i, "dpurp"]

        if tmp_flag:
            accegr_df = pd.concat([accegr_df, pd.DataFrame(tmp_dict)])
            tmp_flag = False
        i = i + j
        continue

    print("num_linked: %d" % len(delete_list))

    del_df = pd.DataFrame(delete_list)
    del_df.columns = ["hhno", "pno", "dow", "tripno"]
    del_df["del_flag"] = 1

    print(len(trip))
    trip = trip.merge(del_df, how="left")
    trip.loc[pd.isna(trip["del_flag"]), "del_flag"] = 0

    print(len(trip))
    trip.to_csv(
        link_trips_week_dir
        / config["02b-link_trips_week"]["trip_linked_detail_week_filename"],
        index=False,
    )

    trip = trip.loc[trip["del_flag"] == 0, ORIG_COLS]
    print(len(trip))

    # create a new continous tripno called lintripno
    trip = trip.sort_values(["hhno", "pno", "tripno"])
    df_count = (
        trip[["hhno", "pno", "tripno"]].groupby(["hhno", "pno"]).count().reset_index()
    )
    trip["lintripno"] = np.concatenate(
        df_count["tripno"].apply(lambda x: range(1, x + 1))
    )

    trip.to_csv(link_trips_week_dir / config["trip_filename"], index=False)
    accegr_df.to_csv(
        link_trips_week_dir / config["02b-link_trips_week"]["accegr_filename"],
        index=False,
    )
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config_filepath")
    args = parser.parse_args()
    with open(args.config_filepath, "rb") as f:
        config = tomllib.load(f)
    link_trips_week(config)
