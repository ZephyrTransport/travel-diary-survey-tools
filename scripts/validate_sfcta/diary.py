"""Disaggregate diary traces: follow one person-day through both pipelines.

The legacy pipeline destroys the survey's globally-unique trip IDs, so these
traces lean on the crosswalk built in helpers.py to show, stage by stage, how
the same raw diary is transformed by each pipeline — and where they diverge.

Exemplar selection is fully deterministic (filter + sort + first) so the
report is reproducible.
"""

import logging

import polars as pl
import spec
from helpers import (
    WEEKDAYS,
    build_crosswalk,
    clock_label,
    load_legacy,
    load_new_daysim,
    load_new_survey,
    load_raw,
)

# DaySim value labels come from the codebook enums, not a hand-kept copy.
PURPOSE_NAMES = spec.labels("trip", "dpurp")
MODE_NAMES = spec.labels("trip", "mode")

logger = logging.getLogger(__name__)

# Label dictionaries for raw-survey coded values (data_canon codebook codes)
RAW_PURPOSE_CATEGORY = {
    -1: "Missing",
    995: "Missing",
    1: "Home",
    2: "Work",
    3: "Work related",
    4: "School",
    5: "School related",
    6: "Escort",
    7: "Shop",
    8: "Meal",
    9: "Social/Recreation",
    10: "Errand",
    11: "Change mode",
    12: "Overnight",
    13: "Other",
}
RAW_MODE_TYPE = {
    1: "Walk",
    2: "Bike",
    3: "Bikeshare",
    4: "Scootershare",
    5: "Taxi",
    6: "TNC",
    7: "Other",
    8: "Car",
    9: "Carshare",
    10: "School bus",
    11: "Shuttle/vanpool",
    12: "Ferry",
    13: "Transit",
    14: "Long distance",
    995: "Missing",
}

# Both pipelines now use the spec DaySim purpose codes (change mode=10, other=11)
LEGACY_PURPOSE_NAMES = PURPOSE_NAMES


def _label(col: str, names: dict[int, str]) -> pl.Expr:
    return pl.col(col).replace_strict(names, default="?", return_dtype=pl.String).alias(col)


def _iso_hm(col: str) -> pl.Expr:
    """HH:MM from an ISO datetime string column."""
    return pl.col(col).str.slice(11, 5).alias(col)


# ---------------------------------------------------------------------
# Exemplar selection
# ---------------------------------------------------------------------


def select_exemplars(basis: str = "vendor") -> dict[str, dict]:
    """Pick one deterministic person-day per comparison scenario.

    Scenarios:
        clean_match:        both pipelines agree end-to-end
        linking_divergence: pipelines partition raw trips into different
                            linked trips
        tour_divergence:    same linked trips, different tour structure
        drive_transit_split: legacy splits a drive-to-transit trip into two
                            DaySim trip records sharing one tsvid
    """
    xwalk = build_crosswalk(basis).filter(pl.col("dow").is_in(WEEKDAYS))

    per_day = (
        xwalk.group_by(["hhno", "pno", "dow"])
        .agg(
            pl.len().alias("n_trips"),
            pl.col("new_in_final").all().alias("all_new_final"),
            pl.col("leg_in_final").any().alias("any_leg_final"),
            pl.col("leg_deleted").is_not_null().all().alias("all_in_02b"),
            pl.col("linking_agrees").first(),
            pl.col("leg_tour").drop_nulls().n_unique().alias("n_leg_tours"),
            pl.col("new_tour").drop_nulls().n_unique().alias("n_new_tours"),
            pl.col("leg_final_rows").max().alias("max_leg_rows"),
        )
        .sort(["hhno", "pno", "dow"])
    )

    def _first(df: pl.DataFrame, scenario: str) -> dict:
        if df.is_empty():
            logger.warning("No exemplar found for scenario %s", scenario)
            return {}
        row = df.row(0, named=True)
        return {"hhno": row["hhno"], "pno": row["pno"], "dow": row["dow"]}

    in_both = per_day.filter(
        pl.col("all_new_final") & pl.col("any_leg_final") & pl.col("all_in_02b")
    )
    return {
        "clean_match": _first(
            in_both.filter(
                pl.col("linking_agrees")
                & (pl.col("n_leg_tours") == pl.col("n_new_tours"))
                & pl.col("n_trips").is_between(4, 5)
                & (pl.col("max_leg_rows") == 1)
            ),
            "clean_match",
        ),
        "linking_divergence": _first(
            in_both.filter(
                ~pl.col("linking_agrees").fill_null(value=True) & (pl.col("n_trips") <= 6)
            ),
            "linking_divergence",
        ),
        "tour_divergence": _first(
            in_both.filter(
                pl.col("linking_agrees")
                & (pl.col("n_leg_tours") != pl.col("n_new_tours"))
                & (pl.col("n_trips") <= 6)
                & (pl.col("max_leg_rows") == 1)
            ),
            "tour_divergence",
        ),
        "drive_transit_split": _first(
            in_both.filter((pl.col("max_leg_rows") == 2) & (pl.col("n_trips") <= 6)),
            "drive_transit_split",
        ),
    }


# ---------------------------------------------------------------------
# Stage traces
# ---------------------------------------------------------------------


def trace_legacy(hhno: int, pno: int, dow: int) -> list[tuple[str, pl.DataFrame]]:
    """Legacy pipeline stages for one person-day, as display-ready frames."""
    stages: list[tuple[str, pl.DataFrame]] = []

    raw = (
        load_raw("trip")
        .filter(
            (pl.col("hh_id") == hhno)
            & (pl.col("person_num") == pno)
            & (pl.col("travel_dow") == dow)
        )
        .sort("trip_num")
        .select(
            pl.col("trip_num").alias("trip"),
            (
                pl.col("depart_hour").cast(pl.String).str.zfill(2)
                + ":"
                + pl.col("depart_minute").cast(pl.String).str.zfill(2)
            ).alias("depart"),
            (
                pl.col("arrive_hour").cast(pl.String).str.zfill(2)
                + ":"
                + pl.col("arrive_minute").cast(pl.String).str.zfill(2)
            ).alias("arrive"),
            _label("o_purpose_category", RAW_PURPOSE_CATEGORY).alias("o_purpose"),
            _label("d_purpose_category", RAW_PURPOSE_CATEGORY).alias("d_purpose"),
            _label("mode_type", RAW_MODE_TYPE).alias("mode"),
            pl.col("distance_miles").round(1).alias("miles"),
            pl.col("trip_weight").round(1).alias("weight"),
        )
    )
    stages.append(("Raw survey trips", raw))

    reformat = (
        load_legacy("02a", "trip")
        .filter((pl.col("hhno") == hhno) & (pl.col("pno") == pno) & (pl.col("dow") == dow))
        .sort("tripno")
        .select(
            pl.col("tripno").alias("trip"),
            clock_label("deptm", hhmm=True).alias("depart"),
            clock_label("arrtm", hhmm=True).alias("arrive"),
            _label("opurp", LEGACY_PURPOSE_NAMES).alias("o_purpose"),
            _label("dpurp", LEGACY_PURPOSE_NAMES).alias("d_purpose"),
            _label("mode", MODE_NAMES).alias("mode"),
            pl.col("otaz"),
            pl.col("dtaz"),
            pl.col("trip_weight").round(1).alias("weight"),
        )
    )
    stages.append(
        ("02a-reformat: DaySim recoding (survey trip_id/person_id dropped here)", reformat)
    )

    survivors = load_legacy("02b", "trip").select("hhno", "pno", "dow", "tripno", "lintripno")
    linked = (
        load_legacy("02b", "trip-detail")
        .filter((pl.col("hhno") == hhno) & (pl.col("pno") == pno) & (pl.col("dow") == dow))
        .join(survivors, on=["hhno", "pno", "dow", "tripno"], how="left")
        .sort("tripno")
        .select(
            pl.col("tripno").alias("trip"),
            pl.when(pl.col("del_flag") == 1)
            .then(pl.lit("absorbed"))
            .otherwise(pl.col("lintripno").cast(pl.String))
            .alias("linked_trip"),
            clock_label("deptm", hhmm=True).alias("depart"),
            clock_label("arrtm", hhmm=True).alias("arrive"),
            _label("dpurp", LEGACY_PURPOSE_NAMES).alias("d_purpose"),
            _label("mode", MODE_NAMES).alias("mode"),
        )
    )
    stages.append(("02b-link_trips: change-mode segments merged", linked))

    final_trips = (
        load_legacy("03b", "trip")
        .filter((pl.col("hhno") == hhno) & (pl.col("pno") == pno) & (pl.col("day") == dow))
        .sort(["tour", "half", "tseg"])
        .select(
            "tour",
            "half",
            "tseg",
            "tsvid",
            clock_label("deptm", hhmm=True).alias("depart"),
            clock_label("arrtm", hhmm=True).alias("arrive"),
            _label("opurp", LEGACY_PURPOSE_NAMES).alias("o_purpose"),
            _label("dpurp", LEGACY_PURPOSE_NAMES).alias("d_purpose"),
            _label("mode", MODE_NAMES).alias("mode"),
            pl.col("trexpfac").round(1).alias("weight"),
        )
    )
    stages.append(("03a/03b: final DaySim trips (tours extracted, Tue-Thu weighted)", final_trips))

    final_tours = (
        load_legacy("03b", "tour")
        .filter((pl.col("hhno") == hhno) & (pl.col("pno") == pno) & (pl.col("day") == dow))
        .sort("tour")
        .select(
            "tour",
            pl.col("parent"),
            _label("pdpurp", LEGACY_PURPOSE_NAMES).alias("purpose"),
            clock_label("tlvorig", hhmm=True).alias("leave_origin"),
            clock_label("tardest", hhmm=True).alias("arrive_dest"),
            clock_label("tarorig", hhmm=True).alias("return_origin"),
            _label("tmodetp", MODE_NAMES).alias("mode"),
            pl.col("toexpfac").round(1).alias("weight"),
        )
    )
    stages.append(("03b: final DaySim tours", final_tours))
    return stages


def trace_new(
    hhno: int, pno: int, dow: int, basis: str = "vendor"
) -> list[tuple[str, pl.DataFrame]]:
    """New pipeline stages for the same person-day."""
    stages: list[tuple[str, pl.DataFrame]] = []

    unlinked = (
        load_new_survey("unlinked_trips", basis)
        .filter(
            (pl.col("hh_id") == hhno)
            & (pl.col("person_num") == pno)
            & (pl.col("travel_dow") == dow)
        )
        .sort("trip_num")
    )
    person_ids = unlinked["person_id"].unique().to_list()
    tour_ids = [t for t in unlinked["tour_id"].unique().to_list() if t is not None]

    stages.append(
        (
            "Cleaned unlinked trips (canonical IDs preserved)",
            unlinked.select(
                pl.col("trip_num").alias("trip"),
                _iso_hm("depart_time").alias("depart"),
                _iso_hm("arrive_time").alias("arrive"),
                _label("o_purpose_category", RAW_PURPOSE_CATEGORY).alias("o_purpose"),
                _label("d_purpose_category", RAW_PURPOSE_CATEGORY).alias("d_purpose"),
                _label("mode_type", RAW_MODE_TYPE).alias("mode"),
                pl.col("linked_trip_num").alias("linked_trip"),
                pl.col("unlinked_trip_weight").round(1).alias("weight"),
            ),
        )
    )

    linked = (
        load_new_survey("linked_trips", basis)
        .filter(pl.col("person_id").is_in(person_ids) & (pl.col("travel_dow") == dow))
        .sort("linked_trip_num")
        .select(
            pl.col("linked_trip_num").alias("linked_trip"),
            _iso_hm("depart_time").alias("depart"),
            _iso_hm("arrive_time").alias("arrive"),
            _label("d_purpose_category", RAW_PURPOSE_CATEGORY).alias("d_purpose"),
            _label("mode_type", RAW_MODE_TYPE).alias("mode"),
            pl.col("num_segments").alias("segments"),
            pl.col("tour_num").alias("tour"),
            pl.col("tour_direction").alias("direction"),
            pl.col("linked_trip_weight").round(1).alias("weight"),
        )
    )
    stages.append(("link_trips: segments merged (dwell-time + buffer rules)", linked))

    tours = (
        load_new_survey("tours", basis)
        .filter(pl.col("tour_id").is_in(tour_ids))
        .sort("tour_num")
        .select(
            pl.col("tour_num").alias("tour"),
            pl.col("subtour_num").alias("subtour"),
            _label("tour_purpose", RAW_PURPOSE_CATEGORY).alias("purpose"),
            _iso_hm("origin_depart_time").alias("leave_origin"),
            _iso_hm("dest_arrive_time").alias("arrive_dest"),
            _iso_hm("origin_arrive_time").alias("return_origin"),
            pl.col("trip_count").alias("trips"),
            pl.col("tour_weight").round(1).alias("weight"),
        )
    )
    stages.append(("extract_tours: canonical tours", tours))

    daysim = load_new_daysim(basis)
    trips = (
        daysim["trip"]
        .filter((pl.col("hhno") == hhno) & (pl.col("pno") == pno) & (pl.col("day") == dow))
        .sort(["tour", "half", "tseg"])
        .select(
            "tour",
            "half",
            "tseg",
            "tsvid",
            clock_label("deptm", hhmm=False).alias("depart"),
            clock_label("arrtm", hhmm=False).alias("arrive"),
            _label("opurp", PURPOSE_NAMES).alias("o_purpose"),
            _label("dpurp", PURPOSE_NAMES).alias("d_purpose"),
            _label("mode", MODE_NAMES).alias("mode"),
            pl.col("trexpfac").round(1).alias("weight"),
        )
    )
    stages.append(("format_daysim: final DaySim trips", trips))

    dtours = (
        daysim["tour"]
        .filter((pl.col("hhno") == hhno) & (pl.col("pno") == pno) & (pl.col("day") == dow))
        .sort("tour")
        .select(
            "tour",
            pl.col("parent"),
            _label("pdpurp", PURPOSE_NAMES).alias("purpose"),
            clock_label("tlvorig", hhmm=False).alias("leave_origin"),
            clock_label("tardest", hhmm=False).alias("arrive_dest"),
            clock_label("tarorig", hhmm=False).alias("return_origin"),
            _label("tmodetp", MODE_NAMES).alias("mode"),
            pl.col("toexpfac").round(1).alias("weight"),
        )
    )
    stages.append(("format_daysim: final DaySim tours", dtours))
    return stages


# ---------------------------------------------------------------------
# Side-by-side rendering
# ---------------------------------------------------------------------

# Align the two pipelines' stages into comparable pairs. Indices reference the
# ordered lists returned by trace_legacy / trace_new. Legacy's 02a-reformat and
# the new pipeline's canonical-tours stage are omitted so each row compares a
# genuinely equivalent step.
PAIRED_STAGES = [
    ("Input trips", 0, 0),
    ("After trip linking", 2, 1),
    ("Final DaySim trips", 3, 3),
    ("Final DaySim tours", 4, 4),
]


def paired_stages(
    hhno: int, pno: int, dow: int, basis: str = "vendor"
) -> list[tuple[str, pl.DataFrame, pl.DataFrame]]:
    """Return (stage_label, legacy_frame, new_frame) tuples for side-by-side view."""
    legacy = trace_legacy(hhno, pno, dow)
    new = trace_new(hhno, pno, dow, basis)
    return [(label, legacy[li][1], new[ni][1]) for label, li, ni in PAIRED_STAGES]


def _table_html(df: pl.DataFrame) -> str:
    return df.to_pandas().to_html(index=False, border=0, classes="diary-table")


def side_by_side_html(hhno: int, pno: int, dow: int, basis: str = "vendor") -> str:
    """HTML with legacy (left) and new (right) stages side by side per row."""
    css = """
    <style>
    .diary-sbs { margin: 0.5rem 0 1.5rem; }
    .diary-sbs .stage-label { font-weight: 600; margin: 1rem 0 0.4rem; }
    .diary-sbs .cols { display: flex; gap: 1.25rem; align-items: flex-start;
                       flex-wrap: wrap; }
    .diary-sbs .col { flex: 1 1 360px; min-width: 0; overflow-x: auto; }
    .diary-sbs .col h5 { margin: 0 0 0.3rem; font-size: 0.85rem;
                         text-transform: uppercase; letter-spacing: 0.04em;
                         opacity: 0.7; }
    .diary-sbs table.diary-table { border-collapse: collapse; font-size: 0.8rem;
                                   width: 100%; }
    .diary-sbs table.diary-table th, .diary-sbs table.diary-table td {
        border: 1px solid rgba(128,128,128,0.3); padding: 2px 6px;
        text-align: right; white-space: nowrap; }
    </style>
    """
    parts = ['<div class="diary-sbs">', css]
    for label, leg, new in paired_stages(hhno, pno, dow, basis):
        parts.append(f'<div class="stage-label">{label}</div>')
        parts.append('<div class="cols">')
        parts.append(f'<div class="col"><h5>Legacy</h5>{_table_html(leg)}</div>')
        parts.append(f'<div class="col"><h5>New pipeline</h5>{_table_html(new)}</div>')
        parts.append("</div>")
    parts.append("</div>")
    return "".join(parts)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    exemplars = select_exemplars()
    for scenario, who in exemplars.items():
        print(f"\n===== {scenario}: {who} =====")
        if not who:
            continue
        for side, tracer in (("LEGACY", trace_legacy), ("NEW", trace_new)):
            print(f"--- {side} ---")
            for stage_label, frame in tracer(who["hhno"], who["pno"], who["dow"]):
                print(f"[{stage_label}]")
                print(frame)
