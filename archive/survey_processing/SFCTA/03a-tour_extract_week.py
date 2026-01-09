import argparse
import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import tomllib

"""
Daysim Pipeline - Step 3a: Tour Extraction and Pattern Recognition

This script extracts travel patterns from linked trip data and organizes them
into tours - round trips that start and end at the same location (typically home).
This is a critical step for activity-based travel demand modeling.

Key concepts:
- Tour: A sequence of trips starting and ending at the same location
- Home-based tours: Tours that start and end at home
- Work-based tours: Subtours that start and end at work during a work day
- Primary tour: The most important tour of the day (usually work or school)
- Stops: Intermediate destinations within a tour

Processing steps:
1. Identify tour boundaries using origin/destination patterns
2. Classify tours by primary purpose (work, school, personal business, etc.)
3. Extract tour timing, mode hierarchy, and stop patterns
4. Create person-day patterns summarizing daily activity
5. Generate weighted outputs for model estimation

Input: Linked trip data from step 02b
Output: Tour, trip, person-day, person, and household files in Daysim format
"""

# Algorithm parameters and constants
MAXSTOP = 21  # Maximum stops per half-tour
PMAX = 15     # Maximum persons per household
DMAX = 1      # Maximum days per person (Drew tested DMAX=7 in Fall 2024, didn't work)
TMAX = 200    # Maximum trips per person
NPTYPES = 9   # Number of trip purpose types
delim = ","   # CSV delimiter
MAXTOUR = 75  # Maximum tours per person-day

# according to the weighting memo, the weights are only good for weekdays only,
# but seems like this script is recalculating the trip weights from scratch anyways.
# 3/4/5/7 days:
WT_COMPLETE_COL = "num_days_complete_3dayweekday"
# WT_COMPLETE_COL = "num_days_complete_4dayweekday"
# WT_COMPLETE_COL = "num_days_complete_5dayweekday"
# WT_COMPLETE_COL = "num_days_complete"


def isclose(a, b, rel_tol=1e-09, abs_tol=0.000001):
    """
    Compare floating point numbers for approximate equality.

    Used for comparing coordinates to identify when trip origins/destinations
    match known locations (home, work, school).

    Args:
        a, b (float): Numbers to compare
        rel_tol (float): Relative tolerance for comparison
        abs_tol (float): Absolute tolerance for comparison

    Returns:
        bool: True if numbers are approximately equal
    """
    """compares floating point numbers"""
    # TODO just swap out and use np.isclose()
    return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)


def clock(mins):
    """
    Convert minutes past midnight to HHMM clock time format.

    Handles time values that exceed 24 hours by wrapping to next day.
    Used for formatting departure and arrival times in tour output.

    Args:
        mins (int): Minutes past midnight

    Returns:
        str: Time in HHMM format (e.g., '1430' for 2:30 PM)
    """
    """converts minutes past midnight to clock time on 24 hour clock"""
    while mins >= 1440:
        mins -= 1440
    mins2 = 100 * (int(mins / 60)) + (mins % 60)
    return str(mins2)


def tour_extract_week(config):
    """
    Extract tours from weekly trip data and generate Daysim model inputs.

    This is the main function that processes trip data through the tour
    extraction algorithm, identifying travel patterns and generating
    comprehensive outputs for activity-based travel demand modeling.

    The algorithm:
    1. Loads and processes household, person, and trip data
    2. Identifies tour boundaries based on location patterns
    3. Determines primary purpose and timing for each tour
    4. Extracts work-based subtours from home-based work tours
    5. Organizes trips into tour stops and calculates tour modes
    6. Generates person-day activity patterns
    7. Outputs weighted data for model estimation

    Args:
        config (dict): Configuration dictionary with file paths, weighting options,
                      and model parameters

    Returns:
        None: Outputs multiple CSV files (tour, trip, personday, person, hh)
              to 03a-tour_extract_week directory
    """
    # Initiate log file
    logfilename = "03a-tour_extract_week.log"
    logfile = open(logfilename, "w")
    logfile.write(f"Tour extract survey program started: {datetime.datetime.now()}\n")

    reformat_dir = Path(config["02a-reformat"]["dir"])
    # HOTFIX pandas now support nullable int columns, but numpy doesn't, so we're
    # filling the null taz values (i.e. outside the Bay Area) with -1 for now.
    # TODO remove the fillna's once the logic below is replaced to not construct
    # each column one by one with numpy
    hh = pd.read_csv(reformat_dir / config["hh_filename"]).fillna({"hhtaz": -1})
    persons = pd.read_csv(reformat_dir / config["person_filename"]).fillna(
        {"pwtaz": -1, "pstaz": -1}
    )
    trip = pd.read_csv(
        Path(config["02b-link_trips_week"]["dir"]) / config["trip_filename"]
    ).fillna({"otaz": -1, "dtaz": -1})

    tour_extract_week_dir = Path(config["03a-tour_extract_week"]["dir"])
    tour_extract_week_dir.mkdir(exist_ok=True)
    outhhfilename = tour_extract_week_dir / config["hh_filename"]
    outperfilename = tour_extract_week_dir / config["person_filename"]
    outpdayfilename = (
        tour_extract_week_dir / config["03a-tour_extract_week"]["personday_filename"]
    )
    outtourfilename = (
        tour_extract_week_dir / config["03a-tour_extract_week"]["tour_filename"]
    )
    outtripfilename = tour_extract_week_dir / config["trip_filename"]

    weighted = config["weighted"]

    if weighted:
        hh["hhexpfac"] = hh[config["03a-tour_extract_week"]["hh_weight_col"]]
        persons["psexpfac"] = persons[
            config["03a-tour_extract_week"]["person_weight_col"]
        ]
        # NOTE TODO trexpfac: doesn't seem to be actually used;
        # seems like the `wt` var is just (over)written to this column.
        # But why calculate it ourselves if the vendor already does it for us?
        # trip["trexpfac"] = trip[config["03a-tour_extract_week"]["trip_weight_col"]]

        # remove some bad records
        r1 = trip.shape[0]
        trip = trip[(trip["opurp"] >= 0) | (trip["dpurp"] >= 0)]
        r2 = trip.shape[0]
        s = f"Removed {r1 - r2} bad records with missing opurp and dpurp"
        print(s)
        logfile.write(s + "\n")

        # tour level variables
        tindex = np.empty((PMAX, DMAX, MAXTOUR), dtype=int)
        tnewid = np.empty((PMAX, DMAX, MAXTOUR), dtype=int)
        tmodetp = np.empty((PMAX, DMAX, MAXTOUR), dtype=int)
        tpathtp = np.empty((PMAX, DMAX, MAXTOUR), dtype=int)
        trtype = np.empty((PMAX, DMAX, MAXTOUR), dtype=int)
        trprio = np.empty((PMAX, DMAX, MAXTOUR), dtype=int)
        subtrs = np.empty((PMAX, DMAX, MAXTOUR), dtype=int)
        parent = np.empty((PMAX, DMAX, MAXTOUR), dtype=int)
        pdtrip = np.empty((PMAX, DMAX, MAXTOUR), dtype=int)
        pdpurp = np.empty((PMAX, DMAX, MAXTOUR), dtype=int)
        pddura = np.empty((PMAX, DMAX, MAXTOUR), dtype=int)
        pddist = np.empty((PMAX, DMAX, MAXTOUR), dtype=int)
        pdprio = np.empty((PMAX, DMAX, MAXTOUR), dtype=int)
        pnmand = np.empty((PMAX, DMAX, MAXTOUR), dtype=int)
        pnmact = np.empty((PMAX, DMAX, MAXTOUR), dtype=int)
        leaveorig = np.empty((PMAX, DMAX, MAXTOUR), dtype=int)
        leavedest = np.empty((PMAX, DMAX, MAXTOUR), dtype=int)
        arrivorig = np.empty((PMAX, DMAX, MAXTOUR), dtype=int)
        arrivdest = np.empty((PMAX, DMAX, MAXTOUR), dtype=int)

        htrips = np.empty((PMAX, DMAX, MAXTOUR, 3), dtype=int)
        htourmode = np.empty((PMAX, DMAX, MAXTOUR, 3), dtype=int)
        htourpath = np.empty((PMAX, DMAX, MAXTOUR, 3), dtype=int)

        # stop level variables
        strip = np.empty((PMAX, DMAX, MAXTOUR, 3, MAXSTOP), dtype=int)

        ntours = np.empty((PMAX, DMAX, 12), dtype=int)
        nstops = np.empty((PMAX, DMAX, 12), dtype=int)
        beghom = np.empty((PMAX, DMAX), dtype=int)
        endhom = np.empty((PMAX, DMAX), dtype=int)
        hbtour = np.empty((PMAX, DMAX), dtype=int)
        wbtour = np.empty((PMAX, DMAX), dtype=int)
        uwtour = np.empty((PMAX, DMAX), dtype=int)
        primtour = np.empty((PMAX, DMAX), dtype=int)

        precs = np.empty((PMAX, DMAX, 2), dtype=int)
        # new person/trip variables to store key data items
        psvid = np.empty(PMAX, dtype=int)
        precs_w = np.empty(PMAX, dtype=int)
        pptyp = np.empty(PMAX, dtype=int)
        pwtyp = np.empty(PMAX, dtype=int)
        pstyp = np.empty(PMAX, dtype=int)
        pagey = np.empty(PMAX, dtype=int)
        pgend = np.empty(PMAX, dtype=int)
        pwtaz = np.empty(PMAX)
        pstaz = np.empty(PMAX)
        if weighted:
            pexpwt = np.empty(PMAX)
        else:
            pexpwt = np.ones(PMAX)  # assign weights of 1 if dataset unweighted
        pwxco = np.empty(PMAX)
        pwyco = np.empty(PMAX)
        psxco = np.empty(PMAX)
        psyco = np.empty(PMAX)
        pwpcl = np.empty(PMAX)
        pspcl = np.empty(PMAX)

        num_wkdays = np.empty(PMAX)

        tsvid = np.empty((PMAX, TMAX), dtype=int)
        totyp = np.empty((PMAX, TMAX), dtype=int)
        toact = np.empty((PMAX, TMAX), dtype=int)
        toatm = np.empty((PMAX, TMAX), dtype=int)
        todtm = np.empty((PMAX, TMAX), dtype=int)
        toprp = np.empty((PMAX, TMAX), dtype=int)
        todur = np.empty((PMAX, TMAX), dtype=int)
        tdtyp = np.empty((PMAX, TMAX), dtype=int)
        tdact = np.empty((PMAX, TMAX), dtype=int)
        tdatm = np.empty((PMAX, TMAX), dtype=int)
        tddtm = np.empty((PMAX, TMAX), dtype=int)
        tdprp = np.empty((PMAX, TMAX), dtype=int)
        tddur = np.empty((PMAX, TMAX), dtype=int)
        tdura = np.empty((PMAX, TMAX), dtype=int)
        tmode = np.empty((PMAX, TMAX), dtype=int)
        tpath = np.empty((PMAX, TMAX), dtype=int)
        xtour = np.empty((PMAX, TMAX), dtype=int)
        xhalf = np.empty((PMAX, TMAX), dtype=int)
        tdorp = np.empty((PMAX, TMAX), dtype=int)
        totaz = np.empty((PMAX, TMAX), dtype=int)
        tdtaz = np.empty((PMAX, TMAX), dtype=int)
        topcl = np.empty((PMAX, TMAX))
        tdpcl = np.empty((PMAX, TMAX))
        toxco = np.empty((PMAX, TMAX))
        toyco = np.empty((PMAX, TMAX))
        tdxco = np.empty((PMAX, TMAX))
        tdyco = np.empty((PMAX, TMAX))
        extrastop = np.empty((3), dtype=int)
        tdow = np.empty((PMAX, TMAX), dtype=int)

        hfheader = 0
        pfheader = 0
        pdfheader = 0
        tfheader = 0
        sfheader = 0
        outhhfile = open(outhhfilename, "w")
        outperfile = open(outperfilename, "w")
        outpdayfile = open(outpdayfilename, "w")
        outtourfile = open(outtourfilename, "w")
        outtripfile = open(outtripfilename, "w")

        # TODO rewrite logic to use dataframe operations, not loop through row by row
        for h in range(len(hh)):
            #         for h in range(1791,1792):
            hhno = hh["hhno"][h]
            hhsize = hh["hhsize"][h]
            hhxco = hh["hxcord"][h]
            hhyco = hh["hycord"][h]
            hpers = persons.loc[persons["hhno"] == hhno,].reset_index()

            #             # For debugging
            #             if hhno == 181000211:
            #                 print('hi')

            pdays = {}
            # loop through household members
            for p in range(1, len(hpers) + 1):
                pno = hpers["pno"][p - 1]
                # calculate household level variables from person attributes
                if p == 1:
                    hhwkrs = 0
                    hhftw = 0
                    hhptw = 0
                    hhret = 0
                    hhoad = 0
                    hhuni = 0
                    hhhsc = 0
                    hh515 = 0
                    hhcu5 = 0
                if hpers["pwtyp"][p - 1] > 0:
                    hhwkrs = hhwkrs + 1
                if hpers["pptyp"][p - 1] == 1:
                    hhftw = hhftw + 1
                if hpers["pptyp"][p - 1] == 2:
                    hhptw = hhptw + 1
                if hpers["pptyp"][p - 1] == 3:
                    hhret = hhret + 1
                if hpers["pptyp"][p - 1] == 4:
                    hhoad = hhoad + 1
                if hpers["pptyp"][p - 1] == 5:
                    hhuni = hhuni + 1
                if hpers["pptyp"][p - 1] == 6:
                    hhhsc = hhhsc + 1
                if hpers["pptyp"][p - 1] == 7:
                    hh515 = hh515 + 1
                if hpers["pptyp"][p - 1] == 8:
                    hhcu5 = hhcu5 + 1

                # store the person data
                psvid[p] = pno
                pptyp[p] = hpers["pptyp"][p - 1]
                pwtyp[p] = hpers["pwtyp"][p - 1]
                pwtaz[p] = hpers["pwtaz"][p - 1]
                pstyp[p] = hpers["pstyp"][p - 1]
                pstaz[p] = hpers["pstaz"][p - 1]
                pwxco[p] = hpers["pwxcord"][p - 1]
                pwyco[p] = hpers["pwycord"][p - 1]
                psxco[p] = hpers["psxcord"][p - 1]
                psyco[p] = hpers["psycord"][p - 1]
                pagey[p] = hpers["pagey"][p - 1]
                pgend[p] = hpers["pgend"][p - 1]
                if weighted:
                    pexpwt[p] = hpers["psexpfac"][p - 1]
                pwpcl[p] = hpers["pwpcl"][p - 1]
                pspcl[p] = hpers["pspcl"][p - 1]

                num_wkdays[p] = hpers[WT_COMPLETE_COL][p - 1]

                hpertrips = trip.loc[
                    (trip["hhno"] == hhno) & (trip["pno"] == pno),
                ].reset_index()
                precs_w[p] = int(len(hpertrips))
                pdays[p] = []
                st_trip_nos = []
                # initialize.  If DMAX==1 nothing happens
                for k in range(1, DMAX):
                    precs[p, k, 0] = 0
                    precs[p, k, 1] = 0

                if len(hpertrips) > 0:
                    pdays[p] = hpertrips["dow"].unique()
                    tripnum_col = (
                        "lintripno" if "lintripno" in hpertrips.columns else "tripno"
                    )
                    # list of first tripnum by dow
                    t1 = sorted(
                        hpertrips[["dow", tripnum_col]]
                        .groupby("dow")
                        .min()[tripnum_col]
                        .tolist()
                    )
                    st_trip_nos = list(t1)
                    # list of last tripnum by dow
                    t2 = sorted(
                        hpertrips[["dow", tripnum_col]]
                        .groupby("dow")
                        .max()[tripnum_col]
                        .tolist()
                    )
                #                     for d,t1,t2 in zip(pdays[p], t1, t2):
                #                         precs[p,d,0] = t1
                #                         precs[p,d,1] = t2

                # store the trip attributes
                for t in range(1, len(hpertrips) + 1):
                    tsvid[p, t] = hpertrips["tripno"][t - 1]

                    #                     # For debugging
                    #                     if tsvid[p,t] == 16:
                    #                         print('hi')

                    tmode[p, t] = hpertrips["mode"][t - 1]
                    tpath[p, t] = hpertrips["path"][t - 1]

                    tdprp[p, t] = hpertrips["dpurp"][t - 1]
                    #                     if tdprp[p,t] not in range(1,NPTYPES+1):
                    #                         tdprp[p,t] = 4

                    topcl[p, t] = hpertrips["opcl"][t - 1]
                    totaz[p, t] = hpertrips["otaz"][t - 1]
                    tdpcl[p, t] = hpertrips["dpcl"][t - 1]
                    tdtaz[p, t] = hpertrips["dtaz"][t - 1]
                    tdxco[p, t] = hpertrips["dxcord"][t - 1]
                    tdyco[p, t] = hpertrips["dycord"][t - 1]
                    toxco[p, t] = hpertrips["oxcord"][t - 1]
                    toyco[p, t] = hpertrips["oycord"][t - 1]
                    tdorp[p, t] = hpertrips["dorp"][t - 1]

                    tdow[p, t] = hpertrips["dow"][t - 1]

                    # get the destination type by checking against known home and work locations
                    # TODO: what about school?
                    # TODO: What about overnight / secondary home?
                    if tdprp[p, t] == 0 or (
                        isclose(tdxco[p, t], hhxco) and isclose(tdyco[p, t], hhyco)
                    ):  # if dest is home
                        tdtyp[p, t] = 1
                        tdpcl[p, t] = hh["hhparcel"][h]
                        tdtaz[p, t] = hh["hhtaz"][h]
                    elif tdprp[p, t] == 1 or (
                        isclose(tdxco[p, t], pwxco[p])
                        and isclose(tdyco[p, t], pwyco[p])
                    ):  # if dest is work
                        tdtyp[p, t] = 2
                        # tdpcl[p,t] = pwpcl[p]
                        # tdtaz[p,t] = pwtaz[p]
                    elif not isclose(tdxco[p, t], -1.0) and not isclose(
                        tdyco[p, t], -1.0
                    ):
                        tdtyp[p, t] = 4
                    else:
                        tdtyp[p, t] = 5

                    if (tdow[p, t] - tdow[p, t - 1] > 1) or t == 1:
                        # If there is a day missing between trips, reset the trip origin based on known locations.
                        reset_flag = True
                    else:
                        # Otherwise, set the trip origin equal to the previous trip destination.
                        reset_flag = False

                    if t > 1 and not reset_flag:
                        toxco[p, t] = tdxco[p, t - 1]
                        toyco[p, t] = tdyco[p, t - 1]
                        totyp[p, t] = tdtyp[p, t - 1]
                        topcl[p, t] = tdpcl[p, t - 1]
                        totaz[p, t] = tdtaz[p, t - 1]
                        toprp[p, t] = tdprp[p, t - 1]
                    elif hpertrips["opurp"][t - 1] == 0 or (
                        isclose(hhxco, toxco[p, t])
                        and isclose(hhyco, toyco[p, t])
                        and not isclose(toxco[p, t], -1.0)
                        and not isclose(toyco[p, t], -1.0)
                    ):  # if origin is home
                        toxco[p, t] = hhxco
                        toyco[p, t] = hhyco
                        totyp[p, t] = 1
                        topcl[p, t] = hh["hhparcel"][h]
                        totaz[p, t] = hh["hhtaz"][h]
                        toprp[p, t] = 0
                    elif hpertrips["opurp"][t - 1] == 1:
                        # if origin is work; CH: though why are we not checking if it's
                        # close to the work coords here, yet we check for home coords above?
                        toxco[p, t] = pwxco[p]
                        toyco[p, t] = pwyco[p]
                        totyp[p, t] = 2
                        topcl[p, t] = pwpcl[p]
                        totaz[p, t] = pwtaz[p]
                        toprp[p, t] = 1
                    else:
                        toxco[p, t] = -1
                        toyco[p, t] = -1
                        totyp[p, t] = 5
                        topcl[p, t] = 0
                        totaz[p, t] = 0
                        toprp[p, t] = 4

                    if t > 1 and not reset_flag:
                        toatm[p, t] = tdatm[p, t - 1]
                    else:
                        toatm[p, t] = 0

                    # convert time from int(hhmm) into minutes-past-midnight
                    strthr = int(hpertrips["deptm"][t - 1] / 100)
                    strtmin = int(hpertrips["deptm"][t - 1]) - int(strthr * 100)
                    todtm[p, t] = 60 * strthr + strtmin
                    if todtm[p, t] < toatm[p, t]:
                        todtm[p, t] = todtm[p, t] + 1440

                    endhour = int(hpertrips["arrtm"][t - 1] / 100)
                    endminte = int(hpertrips["arrtm"][t - 1]) - int(endhour * 100)
                    tdatm[p, t] = 60 * endhour + endminte
                    if tdatm[p, t] < todtm[p, t]:
                        tdatm[p, t] = tdatm[p, t] + 1440

                    # initialize duration to 4 hours (240 minutes)
                    tddtm[p, t] = 1680
                    tddur[p, t] = tddtm[p, t] - tdatm[p, t]
                    if t > 1 and not reset_flag:
                        # calculate actual duration if it's not the first trip, and not a gap in days.
                        tddtm[p, t - 1] = todtm[p, t]
                        tddur[p, t - 1] = tddtm[p, t - 1] - tdatm[p, t - 1]

            # loop on persons again to set up trip for tour formation
            for p in range(1, len(hpers) + 1):
                # tour formation logic - loop on days
                d = 0
                # TODO should `day` be d instead? 2019 also had d stuck at 0 though
                # though Drew says this seems like it should be a single iteration loop
                for day in range(1):
                    # for d in range(DMAX):
                    # d += 1  # Drew tried uncommenting this in Fall 2024
                    # empty counters for diary day
                    hbtour[p, d] = 0
                    wbtour[p, d] = 0
                    uwtour[p, d] = 0
                    primtour[p, d] = 0
                    for q in range(1, NPTYPES + 1):
                        ntours[p, d, q] = 0
                    for q in range(1, NPTYPES + 1):
                        nstops[p, d, q] = 0

                    t1 = 1
                    t2 = precs_w[p]
                    if (t2 == 0) or (totyp[p, t1] == 1):
                        beghom[p, d] = 1
                    else:
                        beghom[p, d] = 0
                    if (t2 == 0) or (tdtyp[p, t2] == 1):
                        endhom[p, d] = 1
                    else:
                        endhom[p, d] = 0

                    # for diary days with trip, form tours
                    if t2 > 0:
                        # Identify starts and ends of any home-based tours
                        fact = 0
                        for t in range(1, t2 + 1):
                            # first trip of tour leaving home
                            if (
                                (fact == 0)
                                and (totyp[p, t] == 1)
                                and (tdtyp[p, t] != 1)
                            ):
                                fact = t
                                fdow = tdow[p, t]
                            elif (totyp[p, t] == 1) and (tdtyp[p, t] != 1) and fact > 0:
                                fact = t
                                fdow = tdow[p, t]
                            # last trip of tour returning home
                            else:
                                if (
                                    (fact > 0)
                                    and (totyp[p, t] != 1)
                                    and (tdtyp[p, t] == 1)
                                ):
                                    if tdow[p, t] - fdow > 1:
                                        fact = 0
                                        continue
                                    # initialize home-based tour attributes
                                    hbtour[p, d] = hbtour[p, d] + 1
                                    subtrs[p, d, hbtour[p, d]] = 0
                                    parent[p, d, hbtour[p, d]] = 0
                                    leaveorig[p, d, hbtour[p, d]] = fact
                                    arrivorig[p, d, hbtour[p, d]] = t
                                    fact = 0

                    # Loop on home-based tours and figure out main activity and purpose
                    for tour in range(1, hbtour[p, d] + 1):
                        pdtrip[p, d, tour] = 0
                        pddura[p, d, tour] = 0
                        pdpurp[p, d, tour] = 0
                        pdprio[p, d, tour] = 10

                        for t in range(leaveorig[p, d, tour], arrivorig[p, d, tour]):
                            if tdprp[p, t] == 1:
                                # work
                                if pptyp[p] > 4:
                                    tprior = 2
                                else:
                                    tprior = 1
                            elif tdprp[p, t] == 2:
                                # school
                                if pptyp[p] > 4:
                                    tprior = 1
                                else:
                                    tprior = 2
                            elif tdprp[p, t] == 3:
                                # escort
                                tprior = 3
                            else:
                                tprior = 4

                            # update the primary destination if this one is a higher priority, or same priority and longer duration
                            if (tprior < pdprio[p, d, tour]) or (
                                tprior == pdprio[p, d, tour]
                                and tddur[p, t] > pddura[p, d, tour]
                            ):
                                pdtrip[p, d, tour] = t
                                pddura[p, d, tour] = tddur[p, t]
                                pdprio[p, d, tour] = tprior
                                pdpurp[p, d, tour] = tdprp[p, t]
                                arrivdest[p, d, tour] = t
                                leavedest[p, d, tour] = t + 1

                        # update the primary tour if this one is a higher priority, or same priority and longer duration
                        if tour == 1:
                            primtour[p, d] = 1
                        elif (pdprio[p, d, tour] < pdprio[p, d, primtour[p, d]]) or (
                            pdprio[p, d, tour] == pdprio[p, d, primtour[p, d]]
                            and pddura[p, d, tour] > pddura[p, d, primtour[p, d]]
                        ):
                            primtour[p, d] = tour

                        # if a work tour to usual workplace, find first and last visits to  dest
                        if (
                            pdpurp[p, d, tour] == 1
                            and tdtyp[p, arrivdest[p, d, tour]] == 2
                        ):
                            uwtour[p, d] = uwtour[p, d] + 1
                            fact = arrivdest[p, d, tour] - 1
                            for t in range(fact, leaveorig[p, d, tour] - 1, -1):
                                if tdtyp[p, t] == 2:
                                    arrivdest[p, d, tour] = t
                            fact = leavedest[p, d, tour] + 1
                            for t in range(fact, arrivorig[p, d, tour] + 1):
                                if totyp[p, t] == 2:
                                    leavedest[p, d, tour] = t

                    # For any work tour to usual workplace, find any subtours
                    for tour in range(1, hbtour[p, d] + 1):
                        if (
                            pdpurp[p, d, tour] == 1
                            and tdtyp[p, arrivdest[p, d, tour]] == 2
                            and (leavedest[p, d, tour] > arrivdest[p, d, tour] + 1)
                        ):
                            fact = 0
                            for t in range(
                                arrivdest[p, d, tour] + 1, leavedest[p, d, tour] + 1
                            ):
                                # first trip of subtour leaving work
                                if totyp[p, t] == 2 and tdtyp[p, t] != 2:
                                    fact = t
                                # last trip of subtour returning to work
                                elif totyp[p, t] != 2 and tdtyp[p, t] == 2:
                                    wbtour[p, d] = wbtour[p, d] + 1
                                    subtrs[p, d, tour] = subtrs[p, d, tour] + 1
                                    subtrs[p, d, hbtour[p, d] + wbtour[p, d]] = subtrs[
                                        p, d, tour
                                    ]
                                    parent[p, d, hbtour[p, d] + wbtour[p, d]] = tour
                                    leaveorig[p, d, hbtour[p, d] + wbtour[p, d]] = fact
                                    arrivorig[p, d, hbtour[p, d] + wbtour[p, d]] = t

                    # Loop on work-based tours and figure out main activity and purpose
                    for tour in range(
                        hbtour[p, d] + 1, hbtour[p, d] + wbtour[p, d] + 1
                    ):
                        pdtrip[p, d, tour] = 0
                        pddura[p, d, tour] = 0
                        pdpurp[p, d, tour] = 0
                        pdprio[p, d, tour] = 10

                        for t in range(leaveorig[p, d, tour], arrivorig[p, d, tour]):
                            if tdprp[p, t] == 1:
                                if pptyp[p] > 4:
                                    tprior = 2
                                else:
                                    tprior = 1
                            elif tdprp[p, t] == 2:
                                if pptyp[p] > 4:
                                    tprior = 1
                                else:
                                    tprior = 2
                            elif tdprp[p, t] == 3:
                                tprior = 3
                            else:
                                tprior = 4

                            if (tprior < pdprio[p, d, tour]) or (
                                (tprior == pdprio[p, d, tour])
                                and (tddur[p, t] > pddura[p, d, tour])
                            ):
                                pdtrip[p, d, tour] = t
                                pddura[p, d, tour] = tddur[p, t]
                                pdprio[p, d, tour] = tprior
                                pdpurp[p, d, tour] = tdprp[p, t]
                                arrivdest[p, d, tour] = t
                                leavedest[p, d, tour] = t + 1

                    # Count tours by purpose, break all tours into stops and set modes
                    for tour in range(1, hbtour[p, d] + wbtour[p, d] + 1):
                        # also determine longest non-mandatory activity on the tour
                        tprior = 0
                        pnmand[p, d, tour] = 0
                        pnmact[p, d, tour] = 0

                        if pdpurp[p, d, tour] < 1 or pdpurp[p, d, tour] > NPTYPES:
                            print(
                                "Tour purpose out of range HH P D T Purp: {},{},{},{},{}".format(
                                    hhno, p, d + 1, tour, pdpurp[p, d, tour]
                                )
                            )

                        if tour <= hbtour[p, d]:
                            ntours[p, d, pdpurp[p, d, tour]] = (
                                ntours[p, d, pdpurp[p, d, tour]] + 1
                            )
                        else:
                            nstops[p, d, pdpurp[p, d, tour]] = (
                                nstops[p, d, pdpurp[p, d, tour]] + 1
                            )

                        # set stops
                        for half in range(1, 3):
                            htourmode[p, d, tour, half] = 0
                            htourpath[p, d, tour, half] = 0
                            stop = 0
                            if half == 1:
                                t = leaveorig[p, d, tour]
                            else:
                                t = leavedest[p, d, tour]
                            while stop < MAXSTOP - 1 and (
                                (half == 1 and t <= arrivdest[p, d, tour])
                                or (half == 2 and t <= arrivorig[p, d, tour])
                            ):
                                stop = stop + 1
                                if tmode[p, t] > htourmode[p, d, tour, half]:
                                    htourmode[p, d, tour, half] = tmode[p, t]
                                    htourpath[p, d, tour, half] = tpath[p, t]
                                strip[p, d, tour, half, stop] = t
                                xtour[p, t] = tour  # index from trip list to tour
                                xhalf[p, t] = half  # index from trip list to half tour
                                # also determine longest out ofhome non-mandatory activity on the tour
                                if (
                                    tdtyp[p, t] > 1
                                    and tdprp[p, t] > 3
                                    and tddur[p, t] > tprior
                                ):
                                    pnmand[p, d, tour] = tdprp[p, t]
                                    pnmact[p, d, tour] = t
                                    tprior = tddur[p, t]
                                if (
                                    t >= leaveorig[p, d, tour]
                                    and t < arrivdest[p, d, tour]
                                ) or (
                                    t >= leavedest[p, d, tour]
                                    and t < arrivorig[p, d, tour]
                                ):
                                    if stop < MAXSTOP:
                                        nstops[p, d, tdprp[p, t]] = (
                                            nstops[p, d, tdprp[p, t]] + 1
                                        )
                                t = t + 1
                            htrips[p, d, tour, half] = stop

            # reorder tours in purpose order
            for p in range(1, len(hpers) + 1): # UPDATE: replaced hhsize w/ len(hpers)
                for d in range(1):
                    torder = 0
                    for tselect in range(12):
                        for tour in range(1, hbtour[p, d] + wbtour[p, d] + 1):
                            # hbw on first round, wb subtour on second round, hb others by purpose after that
                            if (
                                (
                                    tselect == 0
                                    and parent[p, d, tour] == 0
                                    and pdpurp[p, d, tour] == 1
                                )
                                or (tselect == 0 and parent[p, d, tour] > 0)
                                or (
                                    tselect > 1
                                    and parent[p, d, tour] == 0
                                    and pdpurp[p, d, tour] == tselect
                                )
                            ):
                                torder = torder + 1
                                tnewid[p, d, tour] = torder
                                tindex[p, d, torder] = tour

            # write household record
            if hfheader == 0:
                header = (
                    "hhno"
                    + delim
                    + "hhsize"
                    + delim
                    + "hhvehs"
                    + delim
                    + "hhwkrs"
                    + delim
                    + "hhftw"
                    + delim
                    + "hhptw"
                    + delim
                    + "hhret"
                    + delim
                    + "hhoad"
                    + delim
                    + "hhuni"
                    + delim
                    + "hhhsc"
                    + delim
                    + "hh515"
                    + delim
                    + "hhcu5"
                    + delim
                    + "hhincome"
                    + delim
                    + "hownrent"
                    + delim
                    + "hrestype"
                    + delim
                    + "hhparcel"
                    + delim
                    + "hhtaz"
                    + delim
                    + "hhxco"
                    + delim
                    + "hhyco"
                )
                if weighted:
                    header += delim + "hhexpfac"
                outhhfile.write(header + "\n")
                hfheader = 1
            outrec = (
                str(hhno)
                + delim
                + str(hhsize)
                + delim
                + str(hh["hhvehs"][h])
                + delim
                + str(hhwkrs)
                + delim
                + str(hhftw)
                + delim
                + str(hhptw)
                + delim
                + str(hhret)
                + delim
                + str(hhoad)
                + delim
                + str(hhuni)
                + delim
                + str(hhhsc)
                + delim
                + str(hh515)
                + delim
                + str(hhcu5)
                + delim
                + str(hh["hhincome"][h])
                + delim
                + str(hh["hownrent"][h])
                + delim
                + str(hh["hrestype"][h])
                + delim
                + str(hh["hhparcel"][h])
                + delim
                + str(hh["hhtaz"][h])
                + delim
                + str(hhxco)
                + delim
                + str(hhyco)
            )
            if weighted:
                outrec += delim + str(hh["hhexpfac"][h])
            outhhfile.write(outrec + "\n")
            outhhfile.flush()

            # write person record
            for p in range(1, len(hpers) + 1):
                if pfheader == 0:
                    header = (
                        "hhno"
                        + delim
                        + "pno"
                        + delim
                        + "pptyp"
                        + delim
                        + "pagey"
                        + delim
                        + "pgend"
                        + delim
                        + "pwtyp"
                        + delim
                        + "pwpcl"
                        + delim
                        + "pwtaz"
                        + delim
                        + "pwxco"
                        + delim
                        + "pwyco"
                        + delim
                        + "pstyp"
                        + delim
                        + "pspcl"
                        + delim
                        + "pstaz"
                        + delim
                        + "psxco"
                        + delim
                        + "psyco"
                        + delim
                        + "puwmode"
                        + delim
                        + "puwarrp"
                        + delim
                        + "puwdepp"
                        + delim
                        + "ptpass"
                        + delim
                        + "ppaidprk"
                        + delim
                        + "pdiary"
                        + delim
                        + "pproxy"
                        + delim
                        + "psexpfac"  # write this even if not weighted
                    )
                    outperfile.write(header + "\n")
                    pfheader = 1
                outrec = (
                    str(hhno)
                    + delim
                    + str(p)
                    + delim
                    + str(pptyp[p])
                    + delim
                    + str(pagey[p])
                    + delim
                    + str(pgend[p])
                    + delim
                    + str(pwtyp[p])
                    + delim
                    + str(pwpcl[p])
                    + delim
                    + str(pwtaz[p])
                    + delim
                    + str(hpers["pwxcord"][p - 1])
                    + delim
                    + str(hpers["pwycord"][p - 1])
                    + delim
                    + str(pstyp[p])
                    + delim
                    + str(pspcl[p])
                    + delim
                    + str(pstaz[p])
                    + delim
                    + str(hpers["psxcord"][p - 1])
                    + delim
                    + str(hpers["psycord"][p - 1])
                    + delim
                    + str(-1)
                    + delim
                    + str(-1)
                    + delim
                    + str(-1)
                    + delim
                    + str(-1)
                    + delim
                    + str(-1)
                    + delim
                    + str(-1)
                    + delim
                    + str(-1)
                    + delim
                    + str(pexpwt[p])  # write this even if not weighted
                )
                outperfile.write(outrec + "\n")
            outperfile.flush()

            for p in range(1, len(hpers) + 1):
                # write person-day, tour and trip records
                header = (
                    "hhno"
                    + delim
                    + "pno"
                    + delim
                    + "day"
                    + delim
                    + "beghom"
                    + delim
                    + "endhom"
                    + delim
                    + "hbtours"
                    + delim
                    + "wbtours"
                    + delim
                    + "uwtours"
                    + delim
                    + "wktours"
                    + delim
                    + "sctours"
                    + delim
                    + "estours"
                    + delim
                    + "pbtours"
                    + delim
                    + "shtours"
                    + delim
                    + "mltours"
                    + delim
                    + "sotours"
                    + delim
                    + "retours"
                    + delim
                    + "metours"
                    + delim
                    + "wkstops"
                    + delim
                    + "scstops"
                    + delim
                    + "esstops"
                    + delim
                    + "pbstops"
                    + delim
                    + "shstops"
                    + delim
                    + "mlstops"
                    + delim
                    + "sostops"
                    + delim
                    + "restops"
                    + delim
                    + "mestops"
                    + delim
                    + "pdexpfac"
                )

                for d in range(1):
                    # for d in range(DMAX):
                    # write person-day pattern record
                    if pdfheader == 0:
                        outpdayfile.write(header + "\n")
                        pdfheader = 1

                    outrec = (
                        str(hhno)
                        + delim
                        + str(p)
                        + delim
                        + str(d)
                        + delim
                        + str(beghom[p, d])
                        + delim
                        + str(endhom[p, d])
                        + delim
                        + str(hbtour[p, d])
                        + delim
                        + str(wbtour[p, d])
                        + delim
                        + str(uwtour[p, d])
                    )
                    for t in range(1, NPTYPES + 1):
                        outrec = outrec + delim + str(ntours[p, d, t])
                    for t in range(1, NPTYPES + 1):
                        outrec = outrec + delim + str(nstops[p, d, t])

                    if num_wkdays[p] == 0:
                        wt = 0
                    else:
                        wt = pexpwt[p] / num_wkdays[p]
                    outrec = outrec + delim + str(wt)
                    outpdayfile.write(outrec + "\n")

                    # process tours
                    for torder in range(1, hbtour[p, d] + wbtour[p, d] + 1):
                        tour = tindex[p, d, torder]
                        # set additional tour variables
                        if htourmode[p, d, tour, 1] >= htourmode[p, d, tour, 2]:
                            tmodetp[p, d, tour] = htourmode[p, d, tour, 1]
                            tpathtp[p, d, tour] = htourpath[p, d, tour, 1]
                        else:
                            tmodetp[p, d, tour] = htourmode[p, d, tour, 2]
                            tpathtp[p, d, tour] = htourpath[p, d, tour, 2]
                        # write tour record
                        if tfheader == 0:
                            header = (
                                "hhno"
                                + delim
                                + "pno"
                                + delim
                                + "day"
                                + delim
                                + "tour"
                                + delim
                                + "parent"
                                + delim
                                + "subtrs"
                                + delim
                                + "pdpurp"
                                + delim
                                + "tlvorig"
                                + delim
                                + "tardest"
                                + delim
                                + "tlvdest"
                                + delim
                                + "tarorig"
                                + delim
                                + "toadtyp"
                                + delim
                                + "tdadtyp"
                                + delim
                                + "topcl"
                                + delim
                                + "totaz"
                                + delim
                                + "tdpcl"
                                + delim
                                + "tdtaz"
                                + delim
                                + "toxco"
                                + delim
                                + "toyco"
                                + delim
                                + "tdxco"
                                + delim
                                + "tdyco"
                                + delim
                                + "tmodetp"
                                + delim
                                + "tpathtp"
                                + delim
                                + "tripsh1"
                                + delim
                                + "tripsh2"
                                + delim
                                + "toexpfac"
                            )
                            outtourfile.write(header + "\n")
                            tfheader = 1
                        # insert an extra stop for park and ride - easier and safer to do it here than before
                        for half in range(1, 3):
                            if tmodetp[p, d, tour] in range(11, 16) and htourmode[
                                p, d, tour, half
                            ] in range(11, 16):
                                extrastop[half] = 1
                            else:
                                extrastop[half] = 0

                        if parent[p, d, tour] > 0:
                            parentt = tnewid[p, d, parent[p, d, tour]]
                        else:
                            parentt = 0

                        if num_wkdays[p] == 0:
                            wt = 0
                        else:
                            wt = pexpwt[p] / num_wkdays[p]
                        outrec = (
                            str(hhno)
                            + delim
                            + str(p)
                            + delim
                            + str(d)
                            + delim
                            + str(torder)
                            + delim
                            + str(parentt)
                            + delim
                            + str(subtrs[p, d, tour])
                            + delim
                            + str(pdpurp[p, d, tour])
                            + delim
                            + clock(todtm[p, leaveorig[p, d, tour]])
                            + delim
                            + clock(tdatm[p, arrivdest[p, d, tour]])
                            + delim
                            + clock(todtm[p, leavedest[p, d, tour]])
                            + delim
                            + clock(tdatm[p, arrivorig[p, d, tour]])
                            + delim
                            + str(totyp[p, leaveorig[p, d, tour]])
                            + delim
                            + str(tdtyp[p, arrivdest[p, d, tour]])
                            + delim
                            + str(topcl[p, leaveorig[p, d, tour]])
                            + delim
                            + str(totaz[p, leaveorig[p, d, tour]])
                            + delim
                            + str(tdpcl[p, arrivdest[p, d, tour]])
                            + delim
                            + str(tdtaz[p, arrivdest[p, d, tour]])
                            + delim
                            + str(toxco[p, leaveorig[p, d, tour]])
                            + delim
                            + str(toyco[p, leaveorig[p, d, tour]])
                            + delim
                            + str(tdxco[p, arrivdest[p, d, tour]])
                            + delim
                            + str(tdyco[p, arrivdest[p, d, tour]])
                            + delim
                            + str(tmodetp[p, d, tour])
                            + delim
                            + str(tpathtp[p, d, tour])
                            + delim
                            + str(htrips[p, d, tour, 1] + extrastop[1])
                            + delim
                            + str(htrips[p, d, tour, 2] + extrastop[2])
                            + delim
                            + str(wt)
                        )
                        outtourfile.write(outrec + "\n")

                        # process trip
                        for half in range(1, 3):
                            extradone = 0
                            for tt in range(1, htrips[p, d, tour, half] + 1):
                                # write trip record
                                if sfheader == 0:
                                    header = (
                                        "hhno"
                                        + delim
                                        + "pno"
                                        + delim
                                        + "day"
                                        + delim
                                        + "tour"
                                        + delim
                                        + "half"
                                        + delim
                                        + "tseg"
                                        + delim
                                        + "tsvid"
                                        + delim
                                        + "opurp"
                                        + delim
                                        + "dpurp"
                                        + delim
                                        + "oadtyp"
                                        + delim
                                        + "dadtyp"
                                        + delim
                                        + "opcl"
                                        + delim
                                        + "otaz"
                                        + delim
                                        + "dpcl"
                                        + delim
                                        + "dtaz"
                                        + delim
                                        + "oxco"
                                        + delim
                                        + "oyco"
                                        + delim
                                        + "dxco"
                                        + delim
                                        + "dyco"
                                        + delim
                                        + "mode"
                                        + delim
                                        + "pathtype"
                                        + delim
                                        + "dorp"
                                        + delim
                                        + "deptm"
                                        + delim
                                        + "arrtm"
                                        + delim
                                        + "endacttm"
                                        + delim
                                        + "trexpfac"
                                    )
                                    outtripfile.write(header + "\n")
                                    sfheader = 1
                                t = strip[p, d, tour, half, tt]

                                if num_wkdays[p] == 0:
                                    wt = 0
                                else:
                                    wt = pexpwt[p] / num_wkdays[p]

                                # code for splitting drive transit trip here for ABM Transfer effort.
                                if tmode[p, t] == 7 and extradone == 0:
                                    # drive to transit, split to 2 trip
                                    if half == 1:
                                        extmode1 = 3  # sov
                                        extpath1 = 0
                                        extmode2 = 6  # walk-transit
                                        extpath2 = tpathtp[p, d, tour]
                                    else:
                                        extmode1 = 6  # walk-transit
                                        extpath1 = tpathtp[p, d, tour]
                                        extmode2 = 3  # sov
                                        extpath2 = 0
                                    extatim = (
                                        todtm[p, t] + (tdatm[p, t] - todtm[p, t]) // 2
                                    )  # midpoint of trip time
                                    if extatim + 1 < tdatm[p, t]:
                                        extdtim = (
                                            extatim + 1
                                        )  # one minute later, if possible
                                    else:
                                        extdtim = extatim

                                    # write first trip from trip origin to change mode destination
                                    outrec = (
                                        str(hhno)
                                        + delim
                                        + str(p)
                                        + delim
                                        + str(d)
                                        + delim
                                        + str(torder)
                                        + delim
                                        + str(half)
                                        + delim
                                        + str(tt + extradone)
                                        + delim
                                        + str(tsvid[p, t])
                                        + delim
                                        + str(toprp[p, t])
                                        + delim
                                        + str(10)  # new change mode purpose
                                        + delim
                                        + str(totyp[p, t])
                                        + delim
                                        + str(6)  # new destination type
                                        + delim
                                        + str(topcl[p, t])
                                        + delim
                                        + str(totaz[p, t])
                                        + delim
                                        + str(-1)
                                        + delim
                                        + str(
                                            -1
                                        )  # destination parcel and taz is not known yet
                                        + delim
                                        + str(toxco[p, t])
                                        + delim
                                        + str(toyco[p, t])
                                        + delim
                                        + str(tdxco[p, t])
                                        + delim
                                        + str(tdyco[p, t])
                                        + delim
                                        + str(extmode1)
                                        + delim
                                        + str(extpath1)  # new mode and path 1
                                        + delim
                                        + str(1)  # assumed as driver
                                        + delim
                                        + clock(todtm[p, t])
                                        + delim
                                        + clock(extatim)
                                        + delim
                                        + clock(
                                            extdtim
                                        )  # assumed arrival and departure times at destination
                                        + delim
                                        + str(wt)
                                    )
                                    outtripfile.write(outrec + "\n")

                                    # write second trip from change mode origin to trip destination
                                    extradone = 1
                                    outrec = (
                                        str(hhno)
                                        + delim
                                        + str(p)
                                        + delim
                                        + str(d)
                                        + delim
                                        + str(torder)
                                        + delim
                                        + str(half)
                                        + delim
                                        + str(tt + extradone)
                                        + delim
                                        + str(tsvid[p, t])
                                        + delim
                                        + str(10)
                                        + delim
                                        + str(tdprp[p, t])  # new change mode purpose
                                        + delim
                                        + str(6)
                                        + delim
                                        + str(tdtyp[p, t])  # new destination type
                                        + delim
                                        + str(-1)
                                        + delim
                                        + str(-1)
                                        + delim
                                        + str(tdpcl[p, t])
                                        + delim
                                        + str(
                                            tdtaz[p, t]
                                        )  # origin parcel/taz is not known yet
                                        + delim
                                        + str(toxco[p, t])
                                        + delim
                                        + str(toyco[p, t])
                                        + delim
                                        + str(tdxco[p, t])
                                        + delim
                                        + str(tdyco[p, t])
                                        + delim
                                        + str(extmode2)
                                        + delim
                                        + str(extpath2)  # new mode and path 2
                                        + delim
                                        + str(1)  # assumed as driver
                                        + delim
                                        + clock(extdtim)
                                        + delim  # assumed departure time at origin
                                        + clock(tdatm[p, t])
                                        + delim
                                        + clock(tddtm[p, t])
                                        + delim
                                        + str(wt)
                                    )
                                    outtripfile.write(outrec + "\n")
                                else:
                                    # regular trip - as before, but index tt may be higher
                                    outrec = (
                                        str(hhno)
                                        + delim
                                        + str(p)
                                        + delim
                                        + str(d)
                                        + delim
                                        + str(torder)
                                        + delim
                                        + str(half)
                                        + delim
                                        + str(tt + extradone)
                                        + delim
                                        + str(tsvid[p, t])
                                        + delim
                                        + str(toprp[p, t])
                                        + delim
                                        + str(tdprp[p, t])
                                        + delim
                                        + str(totyp[p, t])
                                        + delim
                                        + str(tdtyp[p, t])
                                        + delim
                                        + str(topcl[p, t])
                                        + delim
                                        + str(totaz[p, t])
                                        + delim
                                        + str(tdpcl[p, t])
                                        + delim
                                        + str(tdtaz[p, t])
                                        + delim
                                        + str(toxco[p, t])
                                        + delim
                                        + str(toyco[p, t])
                                        + delim
                                        + str(tdxco[p, t])
                                        + delim
                                        + str(tdyco[p, t])
                                        + delim
                                        + str(tmode[p, t])
                                        + delim
                                        + str(tpath[p, t])
                                        + delim
                                        + str(tdorp[p, t])
                                        + delim
                                        + clock(todtm[p, t])
                                        + delim
                                        + clock(tdatm[p, t])
                                        + delim
                                        + clock(tddtm[p, t])
                                        + delim
                                        + str(wt)
                                    )
                                    outtripfile.write(outrec + "\n")

                outpdayfile.flush()
                outtourfile.flush()
                outtripfile.flush()

        outperfile.close()
        outhhfile.close()
        outpdayfile.flush()
        outtourfile.flush()
        outtripfile.flush()

        logfile.write(
            f"\nTour extract survey week program finished: {datetime.datetime.now()}\n"
        )
        logfile.close()
        print(f"Tour extract survey week program finished: {datetime.datetime.now()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config_filepath")
    args = parser.parse_args()
    with open(args.config_filepath, "rb") as f:
        config = tomllib.load(f)
    tour_extract_week(config)
