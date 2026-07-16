"""Shared helpers for the SFCTA legacy vs new pipeline validation report.

Loads both pipelines' outputs (mirroring slow network CSVs to a local parquet
cache), normalizes semantic differences (purpose codes, time encodings),
derives comparable weights, and builds the raw-trip-level crosswalk that makes
disaggregate comparison possible despite the legacy pipeline dropping the
globally-unique survey IDs.

Run standalone to execute the sanity checks that gate the report render:

    uv run python scripts/validate_sfcta/helpers.py
"""

import logging
import os
import sys
from collections.abc import Callable
from pathlib import Path

import polars as pl

# Make the installed package importable when run as a loose script
REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import spec  # noqa: E402  (pure spec layer; never imports helpers, so no cycle)

from utils.helpers import expr_haversine  # noqa: E402

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------

HERE = Path(__file__).resolve().parent
CACHE = HERE / ".cache"

_MTC_UNC = Path(r"\\models.ad.mtc.ca.gov\data\models")
_MTC_DRIVE = Path("M:/")


def _mtc(relative: str) -> Path:
    """Resolve a path on the MTC data share, preferring the mapped drive."""
    for root in (_MTC_DRIVE, _MTC_UNC):
        candidate = root / relative
        if candidate.exists():
            return candidate
    msg = (
        f"Cannot reach '{relative}' on the MTC share. Map the M: drive first: "
        r"net use M: \\models.ad.mtc.ca.gov\data\models"
    )
    raise FileNotFoundError(msg)


_BATS_2023 = "Data/HomeInterview/Bay Area Travel Study 2023/Data"
RAW_DIR = f"{_BATS_2023}/Full Weighted 2023 Dataset/WeightedDataset_02212025"
LEGACY_DIR = f"{_BATS_2023}/Processed/test"

# Legacy pipeline stage subdirectories, in execution order
LEGACY_STAGES = {
    "00": "00-preprocess",
    "01": "01-taz_spatial_join",
    "02a": "02a-reformat",
    "02b": "02b-link_trips_week",
    "03a": "03a-tour_extract_week",
    "03b": "03b-assign_day/wt-wkday_3day",
}

# New pipeline outputs: existing raked-weights run and the vendor-weights
# variant produced by run_vendor_weights.py
NEW_OUT = {
    "raked": REPO / "bats_2023" / "output",
    "vendor": CACHE / "vendor_output",
}

# Legacy "wt-wkday_3day" scheme: Tuesday(2), Wednesday(3), Thursday(4)
WEEKDAYS = [2, 3, 4]

# DaySim table name -> file name (same in both pipelines' final outputs)
DAYSIM_TABLES = {
    "hh": "households.csv",
    "person": "persons.csv",
    "personday": "days.csv",
    "tour": "tours.csv",
    "trip": "linked_trips.csv",
}
LEGACY_TABLES = {
    "hh": "hh.csv",
    "person": "person.csv",
    "personday": "personday.csv",
    "tour": "tour.csv",
    "trip": "trip.csv",
}

# ---------------------------------------------------------------------
# Cached data loading
# ---------------------------------------------------------------------

FORCE_REFRESH = os.environ.get("VALIDATE_SFCTA_REFRESH", "") == "1"


def cached_read(csv_path: Path, cache_name: str) -> pl.DataFrame:
    """Read a CSV, mirroring it to a local parquet cache for fast re-reads.

    Network reads from the M: share take minutes for the larger files; the
    parquet mirror makes report renders take seconds instead.
    """
    CACHE.mkdir(parents=True, exist_ok=True)
    parquet_path = CACHE / f"{cache_name}.parquet"
    if parquet_path.exists() and not FORCE_REFRESH:
        return pl.read_parquet(parquet_path)

    logger.info("Reading %s (mirroring to %s)", csv_path, parquet_path.name)
    df = pl.read_csv(csv_path, infer_schema_length=50_000)
    df.write_parquet(parquet_path)
    return df


def load_raw(table: str) -> pl.DataFrame:
    """Load a raw survey table: hh, person, day, trip, location, vehicle."""
    return cached_read(_mtc(f"{RAW_DIR}/{table}.csv"), f"raw_{table}")


def load_legacy(stage: str, table: str) -> pl.DataFrame:
    """Load a legacy pipeline output.

    Args:
        stage: One of "00", "01", "02a", "02b", "03a", "03b".
        table: File stem, e.g. "trip", "hh", "accegr_week", "trip-detail";
            for "03b" the DaySim names hh/person/personday/tour/trip apply.
    """
    subdir = LEGACY_STAGES[stage]
    path = _mtc(f"{LEGACY_DIR}/{subdir}/{table}.csv")
    return cached_read(path, f"legacy_{stage}_{table.replace('-', '_')}")


def load_legacy_daysim() -> dict[str, pl.DataFrame]:
    """Load the legacy final DaySim tables (03b, wt-wkday_3day), spec-normalized."""
    return {
        name: normalize_legacy(name, load_legacy("03b", stem.removesuffix(".csv")))
        for name, stem in LEGACY_TABLES.items()
    }


def load_new_daysim(basis: str = "vendor") -> dict[str, pl.DataFrame]:
    """Load the new pipeline's DaySim tables for a weight basis.

    Note: both bases contain identical rows; only the ``*expfac`` columns
    differ (raked vs vendor weights).
    """
    out_dir = NEW_OUT[basis] / "daysim_2023"
    if not out_dir.exists():
        hint = " Run run_vendor_weights.py first." if basis == "vendor" else ""
        msg = f"New pipeline DaySim outputs not found at {out_dir}.{hint}"
        raise FileNotFoundError(msg)
    return {name: pl.read_csv(out_dir / fname) for name, fname in DAYSIM_TABLES.items()}


def load_new_survey(table: str, basis: str = "vendor") -> pl.DataFrame:
    """Load a new-pipeline canonical survey output (e.g. "unlinked_trips")."""
    path = NEW_OUT[basis] / "survey" / f"{table}_2023.csv"
    return cached_read(path, f"new_{basis}_survey_{table}")


def new_days_with_dow(basis: str = "vendor") -> pl.DataFrame:
    """New daysim person-day table with a proper day-of-week column.

    The new pipeline's daysim days file keys ``day`` by *diary day number*
    (1..n of participation) while its trips/tours files use *day of week* —
    the legacy pipeline uses day-of-week everywhere. Bridge via the survey
    days table so person-day comparisons align on day-of-week.
    """
    days = load_new_daysim(basis)["personday"]
    survey_days = load_new_survey("days", basis).select(
        pl.col("hh_id").alias("hhno"),
        pl.col("person_num").alias("pno"),
        pl.col("day_num").alias("day"),
        "travel_dow",
    )
    return days.join(survey_days, on=["hhno", "pno", "day"], how="left").with_columns(
        pl.col("travel_dow").alias("dow")
    )


# ---------------------------------------------------------------------
# Semantics: code labels, remaps, time conversion, distance
# ---------------------------------------------------------------------

# Value labels for coded columns are no longer hand-maintained here: they come
# from the DaySim codebook enums via spec.labels(table, column), so the report and
# the codebook can never drift apart. See spec.py.

# Legacy codes that must be remapped to the spec's convention before comparison.
# Legacy writes 9 for "missing" in the ownership/residence fields where the spec
# and the new pipeline use -1; without this remap, pct_match scores 9 != -1 as a
# disagreement and understates agreement on both columns.
LEGACY_CODE_REMAP: dict[str, dict[str, dict[int, int]]] = {
    "hh": {"hownrent": {9: -1}, "hrestype": {9: -1}},
}


def normalize_legacy(table: str, df: pl.DataFrame) -> pl.DataFrame:
    """Remap legacy code conventions to the spec's, so comparisons are apples-to-apples."""
    remap = LEGACY_CODE_REMAP.get(table, {})
    if not remap:
        return df
    return df.with_columns(
        pl.col(col).replace(mapping).alias(col)
        for col, mapping in remap.items()
        if col in df.columns
    )


# The survey's own detailed income brackets (IncomeDetailed). Both pipelines
# derive hhincome as a representative dollar value within one of these brackets,
# but pick slightly different midpoints (e.g. 62,500 vs 62,000), so exact-dollar
# comparison is meaningless. Binning back to the source brackets is the
# apples-to-apples test of whether they agree on a household's income.
INCOME_BIN_EDGES = [15_000, 25_000, 35_000, 50_000, 75_000, 100_000, 150_000, 200_000, 250_000]
INCOME_BIN_LABELS = [
    "<15k",
    "15-25k",
    "25-35k",
    "35-50k",
    "50-75k",
    "75-100k",
    "100-150k",
    "150-200k",
    "200-250k",
    "250k+",
]


INCOME_BIN_NAMES = dict(enumerate(INCOME_BIN_LABELS))


def income_bin(col: str = "hhincome") -> pl.Expr:
    """Coarse income bin from representative-dollar income (drops missing <=0)."""
    return (
        pl.when(pl.col(col) > 0)
        .then(pl.col(col).cut(INCOME_BIN_EDGES, labels=INCOME_BIN_LABELS))
        .otherwise(None)
        .alias("income_bin")
    )


def income_bin_index(col: str = "hhincome") -> pl.Expr:
    """Integer coarse-income-bin index (0..5), for plotting via INCOME_BIN_NAMES."""
    idx_labels = [str(i) for i in range(len(INCOME_BIN_LABELS))]
    return (
        pl.when(pl.col(col) > 0)
        .then(pl.col(col).cut(INCOME_BIN_EDGES, labels=idx_labels).cast(pl.String).cast(pl.Int32))
        .otherwise(None)
        .alias("inc_bin")
    )


def hhmm_to_minutes(col: str) -> pl.Expr:
    """Legacy HHMM clock int (e.g. 1244) -> minutes since midnight (764)."""
    return (
        pl.when(pl.col(col) < 0)
        .then(pl.col(col))
        .otherwise((pl.col(col) // 100) * 60 + (pl.col(col) % 100))
    )


def minutes_to_hhmm(col: str) -> pl.Expr:
    """Minutes since midnight -> HHMM clock int (mod 24h for spillover)."""
    mins = pl.col(col) % 1440
    return pl.when(pl.col(col) < 0).then(pl.col(col)).otherwise((mins // 60) * 100 + mins % 60)


def clock_label(col: str, *, hhmm: bool) -> pl.Expr:
    """Human-readable HH:MM string from either encoding (for diary displays)."""
    mins = hhmm_to_minutes(col) if hhmm else pl.col(col) % 1440
    return (
        pl.when(pl.col(col) < 0)
        .then(pl.lit("--:--"))
        .otherwise(
            (mins // 60).cast(pl.Int64).cast(pl.String).str.zfill(2)
            + ":"
            + (mins % 60).cast(pl.Int64).cast(pl.String).str.zfill(2)
        )
    )


def trip_distance_miles(x_o: str, y_o: str, x_d: str, y_d: str) -> pl.Expr:
    """Haversine miles from lon/lat coordinate columns (nulls/-1 -> null).

    Legacy final trips carry no travdist, so distances are computed from
    coordinates identically on both sides to keep the comparison fair.
    """
    valid = (pl.col(x_o).abs() > 1) & (pl.col(x_d).abs() > 1)
    return pl.when(valid).then(
        expr_haversine(pl.col(y_o), pl.col(x_o), pl.col(y_d), pl.col(x_d), units="miles")
    )


def normalize_times(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    """Convert legacy HHMM clock columns to minutes-past-midnight in place.

    The new pipeline already writes minutes; normalizing legacy to minutes (rather
    than the other way) keeps the encoding monotone and arithmetic-safe, so time
    differences and tolerance checks are meaningful.
    """
    return df.with_columns(hhmm_to_minutes(c).alias(c) for c in cols if c in df.columns)


# ---------------------------------------------------------------------
# Zone bridge: legacy TAZ1454 vs new TM2 zones
# ---------------------------------------------------------------------

# The DaySim taz fields carry TM2 TAZ_NODE by default (config: taz_field), while the
# legacy pipeline carries TM1 TAZ1454. Same geography in a different id space, so a
# raw comparison of the taz columns is meaningless. The new pipeline's *survey*
# outputs additionally carry the TAZ1454 value, which IS comparable to legacy. Only
# the columns that join cleanly by surviving IDs are bridged here; trip and tour zone
# geography is validated through the coordinate columns instead (both pipelines emit
# o/d and tour-end coordinates directly).
NEW_TAZ1454: dict[tuple[str, str], tuple[str, str, list[str], list[str]]] = {
    # (table, daysim_col): (survey_table, survey_taz1454_col, daysim_keys, survey_keys)
    ("hh", "hhtaz"): ("households", "home_TAZ1454", ["hhno"], ["hh_id"]),
    ("person", "pwtaz"): ("persons", "work_TAZ1454", ["hhno", "pno"], ["hh_id", "person_num"]),
    ("person", "pstaz"): ("persons", "school_TAZ1454", ["hhno", "pno"], ["hh_id", "person_num"]),
}


def taz1454(table: str, col: str, basis: str = "vendor") -> pl.DataFrame | None:
    """New-pipeline TAZ1454 for one taz column, keyed by the DaySim join keys.

    Returns a frame [<daysim_keys>, "<col>_taz1454"] to compare against the legacy
    taz column, or None if the column has no clean TAZ1454 bridge (trip/tour taz).
    """
    spec = NEW_TAZ1454.get((table, col))
    if spec is None:
        return None
    survey_table, survey_col, daysim_keys, survey_keys = spec
    survey = load_new_survey(survey_table, basis).select(
        *[pl.col(s).alias(d) for s, d in zip(survey_keys, daysim_keys, strict=True)],
        pl.col(survey_col).alias(f"{col}_taz1454"),
    )
    return survey.unique(subset=daysim_keys)


# ---------------------------------------------------------------------
# Common-universe filters
# ---------------------------------------------------------------------


def common_households(legacy_hh: pl.DataFrame, new_hh: pl.DataFrame) -> list[int]:
    """Household IDs present in both pipelines' final outputs."""
    return sorted(set(legacy_hh["hhno"].to_list()) & set(new_hh["hhno"].to_list()))


def common_persons(
    legacy_personday: pl.DataFrame,
    new_personday_dow: pl.DataFrame,
    hhnos: list[int],
) -> pl.DataFrame:
    """Persons reporting at least one Tue-Thu day in BOTH pipelines.

    Excludes the legacy pipeline's Monday-travelers that 03b re-assigned onto
    weekday records for the wt-wkday_3day scheme (they have no matching
    day-of-week rows in the new pipeline).
    """

    def _persons(df: pl.DataFrame, day_col: str) -> pl.DataFrame:
        return (
            df.filter(pl.col("hhno").is_in(hhnos) & pl.col(day_col).is_in(WEEKDAYS))
            .select(["hhno", "pno"])
            .unique()
        )

    return _persons(legacy_personday, "day").join(
        _persons(new_personday_dow, "dow"), on=["hhno", "pno"], how="inner"
    )


def filter_universe(
    df: pl.DataFrame,
    persons: pl.DataFrame,
    day_col: str | None = "day",
) -> pl.DataFrame:
    """Restrict a table to the common persons (and Tue-Thu days if applicable)."""
    out = df.join(persons, on=["hhno", "pno"], how="semi")
    if day_col is not None and day_col in out.columns:
        out = out.filter(pl.col(day_col).is_in(WEEKDAYS))
    return out


# ---------------------------------------------------------------------
# Weights
# ---------------------------------------------------------------------


def legacy_equivalent_day_weight() -> pl.DataFrame:
    """Reconstruct the legacy person-day weight from raw survey columns.

    The legacy 03b step computes ``pdexpfac = person_weight / n_complete_TueThu
    _days`` (zero outside Tue-Thu). Because it uses only raw inputs, the same
    weight can be attached to BOTH pipelines' person-days, giving an exact
    like-for-like weighted comparison. Returns one row per person:
    [hhno, pno, wt_wkday3].
    """
    person = load_raw("person").select(
        pl.col("hh_id").alias("hhno"),
        pl.col("person_num").alias("pno"),
        pl.col("person_id"),
        "person_weight",
    )
    n_complete = (
        load_raw("day")
        .filter(pl.col("travel_dow").is_in(WEEKDAYS) & (pl.col("is_complete") == 1))
        .group_by("person_id")
        .agg(pl.len().alias("n_wkdays"))
    )
    return (
        person.join(n_complete, on="person_id", how="left")
        .with_columns(
            pl.when(pl.col("n_wkdays") > 0)
            .then(pl.col("person_weight") / pl.col("n_wkdays"))
            .otherwise(0.0)
            .alias("wt_wkday3")
        )
        .select(["hhno", "pno", "wt_wkday3"])
    )


def check_pdexpfac_reconstruction() -> dict:
    """Verify legacy pdexpfac == person_weight / n_complete_TueThu_days."""
    legacy_pd = load_legacy("03b", "personday").filter(
        pl.col("day").is_in(WEEKDAYS) & (pl.col("pdexpfac") > 0)
    )
    merged = legacy_pd.join(legacy_equivalent_day_weight(), on=["hhno", "pno"], how="inner")
    n_match = merged.filter((pl.col("wt_wkday3") - pl.col("pdexpfac")).abs() < 1e-6).height
    return {"rows": merged.height, "matching": n_match, "pct": 100 * n_match / merged.height}


# ---------------------------------------------------------------------
# Crosswalk: raw trip_id <-> legacy <-> new pipeline
# ---------------------------------------------------------------------


def build_crosswalk(basis: str = "vendor") -> pl.DataFrame:
    """Build the unlinked-trip-level crosswalk between the three ID spaces.

    The legacy pipeline drops the survey's globally-unique ``trip_id`` and
    ``person_id`` in 02a-reformat, but preserves ``hhno == hh_id``,
    ``pno == person_num`` and (for the first segment of each linked trip)
    ``tsvid == trip_num``. The new pipeline preserves all canonical IDs. One
    row per raw trip, cached to .cache/crosswalk.parquet:

    Columns (grouped):
        trip_id, hhno, pno, day_num, dow, tripno   -- raw identity
        new_linked_id, new_group, new_tour_id,      -- new pipeline lineage
        new_in_final, new_tour, new_half, new_tseg
        leg_lintripno, leg_group, leg_deleted,      -- legacy lineage
        leg_in_final, leg_tour, leg_half, leg_tseg, leg_final_rows
    """
    parquet_path = CACHE / "crosswalk.parquet"
    if parquet_path.exists() and not FORCE_REFRESH:
        return pl.read_parquet(parquet_path)

    spine = load_raw("trip").select(
        "trip_id",
        pl.col("hh_id").alias("hhno"),
        pl.col("person_num").alias("pno"),
        "day_num",
        pl.col("travel_dow").alias("dow"),
        pl.col("trip_num").alias("tripno"),
    )

    # --- New pipeline lineage (canonical IDs survive) -------------------
    unlinked = load_new_survey("unlinked_trips", basis).select(
        pl.col("unlinked_trip_id").alias("trip_id"),
        pl.col("linked_trip_id").alias("new_linked_id"),
        pl.col("linked_trip_num").alias("new_tsvid"),
        pl.col("tour_id").alias("new_tour_id"),
    )
    daysim_trips = (
        load_new_daysim(basis)["trip"]
        .select(
            "hhno",
            "pno",
            pl.col("day").alias("dow"),
            pl.col("tsvid").alias("new_tsvid"),
            pl.col("tour").alias("new_tour"),
            pl.col("half").alias("new_half"),
            pl.col("tseg").alias("new_tseg"),
        )
        .unique(subset=["hhno", "pno", "dow", "new_tsvid"])
        .with_columns(pl.lit(value=True).alias("new_in_final"))
    )

    xwalk = (
        spine.join(unlinked, on="trip_id", how="left")
        .join(daysim_trips, on=["hhno", "pno", "dow", "new_tsvid"], how="left")
        .with_columns(pl.col("new_in_final").fill_null(value=False))
    )

    # --- Legacy lineage (reconstructed) ---------------------------------
    # 02b surviving rows carry lintripno; absorbed rows appear in trip-detail
    # with del_flag=1 and were merged INTO the nearest preceding survivor.
    detail = load_legacy("02b", "trip-detail").select(
        "hhno", "pno", "dow", "tripno", pl.col("del_flag").alias("leg_deleted")
    )
    survivors = load_legacy("02b", "trip").select(
        "hhno", "pno", "dow", "tripno", pl.col("lintripno").alias("leg_lintripno")
    )
    legacy_linked = (
        detail.join(survivors, on=["hhno", "pno", "dow", "tripno"], how="left")
        .sort(["hhno", "pno", "dow", "tripno"])
        .with_columns(
            # Absorbed rows take the group of the preceding survivor. A tiny
            # residue (~0.05%) has a deleted first-of-day row; backward-fill
            # attaches those to the following survivor.
            pl.col("leg_lintripno")
            .fill_null(strategy="forward")
            .fill_null(strategy="backward")
            .over(["hhno", "pno", "dow"])
            .alias("leg_group")
        )
    )

    final_trips = (
        load_legacy("03b", "trip")
        .select(
            "hhno",
            "pno",
            pl.col("day").alias("dow"),
            pl.col("tsvid").alias("tripno"),
            pl.col("tour").alias("leg_tour"),
            pl.col("half").alias("leg_half"),
            pl.col("tseg").alias("leg_tseg"),
        )
        .group_by(["hhno", "pno", "dow", "tripno"])
        .agg(
            pl.len().alias("leg_final_rows"),  # 2 = legacy drive-transit split
            pl.col("leg_tour").first(),
            pl.col("leg_half").first(),
            pl.col("leg_tseg").first(),
        )
        .with_columns(pl.lit(value=True).alias("leg_in_final"))
    )

    xwalk = (
        xwalk.join(legacy_linked, on=["hhno", "pno", "dow", "tripno"], how="left")
        .join(final_trips, on=["hhno", "pno", "dow", "tripno"], how="left")
        .with_columns(pl.col("leg_in_final").fill_null(value=False))
    )

    # --- Agreement flags per person-day ----------------------------------
    # Trip-linking partitions agree when the mapping between legacy groups and
    # new groups is one-to-one over the trips present in both pipelines.
    both = xwalk.filter(pl.col("leg_group").is_not_null() & pl.col("new_linked_id").is_not_null())
    agreement = (
        both.group_by(["hhno", "pno", "dow"])
        .agg(
            pl.struct(["leg_group", "new_linked_id"]).n_unique().alias("n_pairs"),
            pl.col("leg_group").n_unique().alias("n_leg_groups"),
            pl.col("new_linked_id").n_unique().alias("n_new_groups"),
        )
        .with_columns(
            (
                (pl.col("n_pairs") == pl.col("n_leg_groups"))
                & (pl.col("n_pairs") == pl.col("n_new_groups"))
            ).alias("linking_agrees")
        )
        .select(["hhno", "pno", "dow", "n_leg_groups", "n_new_groups", "linking_agrees"])
    )
    xwalk = xwalk.join(agreement, on=["hhno", "pno", "dow"], how="left")

    CACHE.mkdir(parents=True, exist_ok=True)
    xwalk.write_parquet(parquet_path)
    return xwalk


# ---------------------------------------------------------------------
# Record-level trip comparison: collapse, bridge, match
# ---------------------------------------------------------------------

# Legacy final-trip fields, split by which end of a collapsed drive-transit pair
# they take. The legacy pipeline emits a drive-to-transit trip as two rows sharing
# one tsvid (the auto access leg, then the walk-transit leg); the new pipeline emits
# one linked mode=7 row. Collapsing legacy to one row makes the two comparable.
_TRIP_ORIGIN_FIELDS = ["opurp", "oadtyp", "opcl", "otaz", "oxco", "oyco", "deptm"]
_TRIP_DEST_FIELDS = ["dpurp", "dadtyp", "dpcl", "dtaz", "dxco", "dyco", "arrtm", "endacttm"]


def collapse_legacy_split(leg_trips: pl.DataFrame) -> tuple[pl.DataFrame, int]:
    """Collapse legacy drive-to-transit 2-row trips into one linked-trip row.

    Per (hhno, pno, day, tsvid), ordered by (half, tseg): origin fields come from the
    first leg, destination fields from the last, mode becomes 7 (drive-to-transit),
    pathtype the max over the pair (the transit leg carries it; the auto leg writes
    the lower code), and dorp the driver/passenger of the auto leg. All other columns
    (tour, half, tseg, trexpfac, ...) take the first row -- trexpfac is verified
    identical within the pair by the split-invariant sanity check.

    Returns (collapsed_trips, n_collapsed).
    """
    key = ["hhno", "pno", "day", "tsvid"]
    counts = leg_trips.group_by(key).agg(pl.len().alias("_n"))
    n_split = counts.filter(pl.col("_n") > 1).height
    if n_split == 0:
        return leg_trips, 0

    ordered = leg_trips.sort([*key, "half", "tseg"])
    others = [
        c
        for c in leg_trips.columns
        if c not in {*key, *_TRIP_ORIGIN_FIELDS, *_TRIP_DEST_FIELDS, "mode", "pathtype", "dorp"}
    ]
    collapsed = ordered.group_by(key, maintain_order=True).agg(
        *[pl.col(c).first() for c in _TRIP_ORIGIN_FIELDS],
        *[pl.col(c).last() for c in _TRIP_DEST_FIELDS],
        *[pl.col(c).first() for c in others],
        # A collapsed pair is drive-to-transit; a lone row keeps its own mode.
        pl.when(pl.len() > 1).then(pl.lit(7)).otherwise(pl.col("mode").first()).alias("mode"),
        pl.col("pathtype").max().alias("pathtype"),
        # dorp of the auto (non-transit) leg; falls back to the single row's value.
        pl.col("dorp")
        .filter(pl.col("mode") != 6)
        .first()
        .fill_null(pl.col("dorp").first())
        .alias("dorp"),
    )
    return collapsed.select(leg_trips.columns), n_split


def trip_key_bridge(basis: str = "vendor") -> pl.DataFrame:
    """Map each legacy linked trip to its new-pipeline counterpart, via shared raw trips.

    Legacy tsvid (raw trip_num of the first surviving segment) and new tsvid
    (linked_trip_num) live in different id spaces, so trips cannot be joined on tsvid.
    But build_crosswalk() carries, per raw trip, both the legacy linked-trip group
    (leg_group) and the new linked-trip id (new_linked_id). Grouping by
    (hhno, pno, dow, leg_group, new_linked_id) gives the raw-trip overlap of every
    candidate legacy<->new pair; keeping the MUTUAL-BEST pair for each side (arg-max
    overlap, ties broken on lowest tsvid for determinism) yields one row per matched
    linked trip.

    match_status partitions every legacy final trip exactly once:
        matched_exact   mutual-best, identical raw-trip sets (linkers agree)
        matched_partial mutual-best, overlapping but unequal sets (linking differs)
        legacy_only     a legacy linked trip with no new counterpart
    (new_only trips are recovered separately by matched_trips via an anti-join.)

    Columns: hhno, pno, dow, leg_group, leg_tsvid, new_linked_id, new_tsvid,
             n_shared, n_leg, n_new, match_status. Cached to .cache/trip_bridge.parquet.
    """
    parquet_path = CACHE / "trip_bridge.parquet"
    if parquet_path.exists() and not FORCE_REFRESH:
        return pl.read_parquet(parquet_path)

    xwalk = build_crosswalk(basis)

    # leg_tsvid: the tsvid legacy assigns the group == raw trip_num of its first
    # surviving (in-final) segment. Groups with no in-final segment never reached the
    # legacy final table and are not bridge rows.
    leg_final = (
        xwalk.filter(pl.col("leg_in_final"))
        .group_by(["hhno", "pno", "dow", "leg_group"])
        .agg(pl.col("tripno").min().alias("leg_tsvid"), pl.len().cast(pl.Int64).alias("n_leg"))
    )
    new_size = (
        xwalk.filter(pl.col("new_in_final"))
        .group_by(["hhno", "pno", "dow", "new_linked_id"])
        .agg(pl.len().cast(pl.Int64).alias("n_new"), pl.col("new_tsvid").first())
    )

    # Raw-trip overlap of every candidate pair present in both finals.
    pairs = (
        xwalk.filter(pl.col("leg_in_final") & pl.col("new_in_final"))
        .group_by(["hhno", "pno", "dow", "leg_group", "new_linked_id"])
        .agg(pl.len().cast(pl.Int64).alias("n_shared"))
        .join(leg_final, on=["hhno", "pno", "dow", "leg_group"], how="left")
        .join(new_size, on=["hhno", "pno", "dow", "new_linked_id"], how="left")
    )

    # Mutual-best: the pair must be the top-overlap choice for BOTH its legacy group
    # and its new group. Deterministic tie-break on the counterpart's tsvid.
    best_leg = (
        pairs.sort(["n_shared", "new_tsvid"], descending=[True, False])
        .group_by(["hhno", "pno", "dow", "leg_group"], maintain_order=True)
        .agg(pl.col("new_linked_id").first().alias("_best_new"))
    )
    best_new = (
        pairs.sort(["n_shared", "leg_tsvid"], descending=[True, False])
        .group_by(["hhno", "pno", "dow", "new_linked_id"], maintain_order=True)
        .agg(pl.col("leg_group").first().alias("_best_leg"))
    )

    mutual = (
        pairs.join(best_leg, on=["hhno", "pno", "dow", "leg_group"], how="left")
        .join(best_new, on=["hhno", "pno", "dow", "new_linked_id"], how="left")
        .filter(
            (pl.col("new_linked_id") == pl.col("_best_new"))
            & (pl.col("leg_group") == pl.col("_best_leg"))
        )
        .with_columns(
            pl.when(
                (pl.col("n_shared") == pl.col("n_leg")) & (pl.col("n_shared") == pl.col("n_new"))
            )
            .then(pl.lit("matched_exact"))
            .otherwise(pl.lit("matched_partial"))
            .alias("match_status")
        )
        .select(
            "hhno",
            "pno",
            "dow",
            "leg_group",
            "leg_tsvid",
            "new_linked_id",
            "new_tsvid",
            "n_shared",
            "n_leg",
            "n_new",
            "match_status",
        )
    )

    # Legacy final linked trips that got no mutual-best partner.
    leg_only = (
        leg_final.join(
            mutual.select(["hhno", "pno", "dow", "leg_group"]),
            on=["hhno", "pno", "dow", "leg_group"],
            how="anti",
        )
        .with_columns(
            pl.lit(None, dtype=pl.Int64).alias("new_linked_id"),
            pl.lit(None, dtype=pl.Int64).alias("new_tsvid"),
            pl.lit(0, dtype=pl.Int64).alias("n_shared"),
            pl.lit(None, dtype=pl.Int64).alias("n_new"),
            pl.lit("legacy_only").alias("match_status"),
        )
        .select(mutual.columns)
    )

    bridge = pl.concat([mutual, leg_only])
    CACHE.mkdir(parents=True, exist_ok=True)
    bridge.write_parquet(parquet_path)
    return bridge


def matched_trips(
    legacy_trips: pl.DataFrame,
    new_trips: pl.DataFrame,
    bridge: pl.DataFrame | None = None,
    basis: str = "vendor",
) -> dict[str, object]:
    """One row per matched linked trip carrying BOTH pipelines' full DaySim records.

    Collapses the legacy drive-transit split, normalizes legacy times to minutes, then
    joins each pipeline's trips through trip_key_bridge on (hhno, pno, day, tsvid). New
    columns are suffixed _new. Returns the matched frame plus the match taxonomy so
    every trip on each side is accounted for exactly once.
    """
    bridge = bridge if bridge is not None else trip_key_bridge(basis)
    bridge = bridge.rename({"dow": "day"})  # both DaySim trip tables key day-of-week as `day`

    leg, _ = collapse_legacy_split(legacy_trips)
    leg = normalize_times(leg, ["deptm", "arrtm", "endacttm"])

    matched_pairs = bridge.filter(pl.col("match_status").str.starts_with("matched"))
    leg_keyed = matched_pairs.select(
        ["hhno", "pno", "day", "leg_tsvid", "new_tsvid", "match_status", "n_shared"]
    )
    matched = leg_keyed.join(
        leg,
        left_on=["hhno", "pno", "day", "leg_tsvid"],
        right_on=["hhno", "pno", "day", "tsvid"],
        how="inner",
    ).join(
        new_trips,
        left_on=["hhno", "pno", "day", "new_tsvid"],
        right_on=["hhno", "pno", "day", "tsvid"],
        how="inner",
        suffix="_new",
    )
    # Taxonomy on the comparison universe (the matched frame), not the global bridge.
    n_exact = matched.filter(pl.col("match_status") == "matched_exact").height
    n_partial = matched.filter(pl.col("match_status") == "matched_partial").height
    n_leg_only = max(leg.height - matched.height, 0)
    n_new_only = max(new_trips.height - matched.height, 0)
    _, n_split = collapse_legacy_split(legacy_trips)
    return {
        "matched": matched,
        "n_legacy": leg.height,
        "n_new": new_trips.height,
        "n_matched": matched.height,
        "n_exact": n_exact,
        "n_partial": n_partial,
        "n_legacy_only": n_leg_only,
        "n_new_only": n_new_only,
        "n_split_collapsed": n_split,
        "match_rate_pct": 100 * matched.height / leg.height if leg.height else 0.0,
    }


# ---------------------------------------------------------------------
# Household & person attribute comparison (exact joins on surviving IDs)
# ---------------------------------------------------------------------


FLAG_THRESHOLD = 5.0  # flag any variable that disagrees on more than this %


def pct_match(legacy_df: pl.DataFrame, new_df: pl.DataFrame, keys: list[str], col: str) -> float:
    """Exact-match rate (%) for one column on records joined by surviving IDs."""
    joined = legacy_df.select([*keys, col]).join(
        new_df.select([*keys, col]), on=keys, how="inner", suffix="_new"
    )
    n = joined.height
    if not n:
        return 0.0
    return round(100 * joined.filter(pl.col(col) == pl.col(f"{col}_new")).height / n, 2)


def variable_agreement(
    legacy_df: pl.DataFrame,
    new_df: pl.DataFrame,
    keys: list[str],
    cols: list[str],
    extra_notes: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Per-variable exact-match display table (formatted, with flags).

    The legacy pipeline preserves hhno/pno, so household and person attributes
    join one-to-one with the new pipeline — no crosswalk needed. Variables that
    disagree by more than ``FLAG_THRESHOLD`` are marked with a warning flag.
    """
    extra_notes = extra_notes or {}
    joined = legacy_df.select([*keys, *cols]).join(
        new_df.select([*keys, *cols]), on=keys, how="inner", suffix="_new"
    )
    n = joined.height
    rows = []
    for c in cols:
        pct = pct_match(legacy_df, new_df, keys, c)
        legacy_stub = joined.select((pl.col(c) == -1).all()).item()
        flagged = pct < 100 - FLAG_THRESHOLD
        note = extra_notes.get(c, "")
        if not note and legacy_stub:
            note = "legacy never populates this (constant -1); matches only where new is also -1"
        rows.append(
            {
                "variable": f"⚠ {c}" if flagged else c,
                "n": n,
                "% agree": (f"**{pct:.1f}**" if flagged else f"{pct:.1f}"),
                "note": note,
            }
        )
    return pl.DataFrame(rows)


def trip_structuring_table(
    xwalk_universe: pl.DataFrame,
    legacy_trips: pl.DataFrame,
    new_trips: pl.DataFrame,
    legacy_tours: pl.DataFrame,
    new_tours: pl.DataFrame,
) -> pl.DataFrame:
    """How raw trips become structured travel, stage by stage, in each pipeline.

    Four stages, the last a different unit (tours, not trips):
        Unlinked trips    raw trips both pipelines carried into linking
        Linked trips      after trip linking (change-mode segments merged)
        Trips in tours    trips surviving tour extraction into the DaySim trip file
                          (legacy counted AFTER collapsing its drive-transit 2-row
                          split, so the unit is one linked trip on both sides)
        Tours             the DaySim tour records those trips form

    ``xwalk_universe`` is the crosswalk on the comparison universe (has ``dow``,
    ``leg_group``, ``new_linked_id``); the trip/tour frames are each pipeline's
    DaySim outputs on that universe.
    """
    both = xwalk_universe.filter(
        pl.col("leg_group").is_not_null() & pl.col("new_linked_id").is_not_null()
    )
    unlinked = both.height
    leg_linked = both.select(pl.struct(["hhno", "pno", "dow", "leg_group"])).n_unique()
    new_linked = both["new_linked_id"].n_unique()
    leg_in_tours = collapse_legacy_split(legacy_trips)[0].height
    return pl.DataFrame(
        [
            {"stage": "Unlinked trips", "unit": "trips", "legacy": unlinked, "new": unlinked},
            {"stage": "Linked trips", "unit": "trips", "legacy": leg_linked, "new": new_linked},
            {
                "stage": "Trips in tours",
                "unit": "trips",
                "legacy": leg_in_tours,
                "new": new_trips.height,
            },
            {
                "stage": "Tours",
                "unit": "tours",
                "legacy": legacy_tours.height,
                "new": new_tours.height,
            },
        ]
    ).with_columns(
        (100 * pl.col("legacy") / unlinked).round(1).alias("retention_legacy_pct"),
        (100 * pl.col("new") / unlinked).round(1).alias("retention_new_pct"),
        pl.when(pl.col("stage") == "Trips in tours")
        .then(pl.lit(f"legacy raw rows before drive-transit collapse: {legacy_trips.height:,}"))
        .otherwise(pl.lit(""))
        .alias("note"),
    )


# ---------------------------------------------------------------------
# Aggregate comparison builders
# ---------------------------------------------------------------------


# Why each table's legacy-vs-new record count differs. Keyed by table name; consumed
# by row_count_table so the reader sees the cause beside the delta.
ROW_COUNT_NOTES = {
    "hh": "same universe by construction",
    "person": "same universe by construction",
    "personday": "new keeps zero-weight / incomplete days as rows; legacy drops them",
    "tour": "tour-detection differences (coordinate buffer vs TAZ equality)",
    "trip": "tour-formatting drops partial/invalid tours + drive-transit 2-row collapse",
}


def row_count_table(tables: dict[str, dict[str, pl.DataFrame]]) -> pl.DataFrame:
    """Row counts per table across pipelines. tables[label][table_name] = df."""
    names = ["hh", "person", "personday", "tour", "trip"]
    labels = list(tables)
    rows = []
    for name in names:
        row: dict[str, object] = {"table": name}
        for label in labels:
            row[label] = tables[label][name].height
        rows.append(row)
    df = pl.DataFrame(rows)
    if len(labels) == 2:
        a, b = labels
        df = df.with_columns(
            (pl.col(b) - pl.col(a)).alias("diff"),
            ((pl.col(b) - pl.col(a)) / pl.col(a) * 100)
            .map_elements(lambda v: f"{v:+.2f}%", return_dtype=pl.String)
            .alias("pct_diff"),
            pl.when(pl.col(b) == pl.col(a))
            .then(pl.lit(""))
            .otherwise(pl.col("table").replace_strict(ROW_COUNT_NOTES, default=""))
            .alias("note"),
        )
    return df


def distribution_table(
    legacy_df: pl.DataFrame,
    new_df: pl.DataFrame,
    col: str,
    names: dict[int, str],
    weight_cols: tuple[str, str] | None = None,
) -> pl.DataFrame:
    """Category distribution comparison (counts or weighted sums + shares)."""

    def _dist(df: pl.DataFrame, weight: str | None, label: str) -> pl.DataFrame:
        agg = pl.col(weight).sum() if weight else pl.len().cast(pl.Float64)
        out = df.group_by(col).agg(agg.alias(label))
        return out.with_columns((pl.col(label) / pl.col(label).sum() * 100).alias(f"{label}_pct"))

    w_leg, w_new = weight_cols or (None, None)
    comparison = (
        _dist(legacy_df, w_leg, "legacy")
        .join(_dist(new_df, w_new, "new"), on=col, how="full", coalesce=True)
        .fill_null(0.0)
        .with_columns(
            pl.col(col).replace_strict(names, default="?").alias("label"),
            (pl.col("new_pct") - pl.col("legacy_pct")).round(2).alias("share_delta"),
        )
        .sort(col)
        .select([col, "label", "legacy", "legacy_pct", "new", "new_pct", "share_delta"])
    )
    return comparison


def rate_table(
    legacy: dict[str, pl.DataFrame],
    new: dict[str, pl.DataFrame],
    weight_cols: dict[str, tuple[str, str]] | None = None,
) -> pl.DataFrame:
    """Behavioral rates: trips & tours per person-day (optionally weighted).

    weight_cols maps table -> (legacy_weight_col, new_weight_col).
    """

    def _total(df: pl.DataFrame, weight: str | None) -> float:
        return float(df[weight].sum()) if weight else float(df.height)

    rows = []
    for label, data in (("legacy", legacy), ("new", new)):
        idx = 0 if label == "legacy" else 1
        w = {t: weight_cols[t][idx] for t in weight_cols} if weight_cols else {}
        pd_total = _total(data["personday"], w.get("personday"))
        rows.append(
            {
                "pipeline": label,
                "person_days": pd_total,
                "trips": _total(data["trip"], w.get("trip")),
                "tours": _total(data["tour"], w.get("tour")),
                "trips_per_day": _total(data["trip"], w.get("trip")) / pd_total,
                "tours_per_day": _total(data["tour"], w.get("tour")) / pd_total,
            }
        )
    return pl.DataFrame(rows)


def time_of_day_profile(
    legacy_trips: pl.DataFrame,
    new_trips: pl.DataFrame,
    weight_cols: tuple[str, str] | None = None,
) -> pl.DataFrame:
    """Departure-hour profile. Legacy deptm is HHMM; new deptm is minutes."""
    leg = legacy_trips.with_columns((pl.col("deptm") // 100).alias("hour"))
    new = new_trips.with_columns((pl.col("deptm") % 1440 // 60).alias("hour"))
    return distribution_table(
        leg.filter(pl.col("hour").is_between(0, 23)),
        new.filter(pl.col("hour").is_between(0, 23)),
        "hour",
        {h: f"{h:02d}:00" for h in range(24)},
        weight_cols,
    )


DISTANCE_BINS = [0.25, 0.5, 1, 2, 5, 10, 20, 50]


def distance_distribution(
    legacy_trips: pl.DataFrame,
    new_trips: pl.DataFrame,
    weight_cols: tuple[str, str] | None = None,
) -> pl.DataFrame:
    """Trip-distance distribution, computed identically (haversine) both sides."""

    def _binned(df: pl.DataFrame) -> pl.DataFrame:
        return (
            df.with_columns(trip_distance_miles("oxco", "oyco", "dxco", "dyco").alias("dist_mi"))
            .drop_nulls("dist_mi")
            .with_columns(
                pl.col("dist_mi").cut(DISTANCE_BINS).cast(pl.String).alias("dist_bin"),
                pl.col("dist_mi")
                .cut(DISTANCE_BINS, labels=[str(i) for i in range(len(DISTANCE_BINS) + 1)])
                .cast(pl.String)
                .cast(pl.Int32)
                .alias("bin_order"),
            )
        )

    leg, new = _binned(legacy_trips), _binned(new_trips)
    labels = dict(
        leg.select(["bin_order", "dist_bin"]).unique().sort("bin_order").iter_rows()
    ) | dict(new.select(["bin_order", "dist_bin"]).unique().sort("bin_order").iter_rows())
    return distribution_table(leg, new, "bin_order", labels, weight_cols)


# ---------------------------------------------------------------------
# Matched-tour analysis (ported from projects/bats_2023/compare/daysim)
# ---------------------------------------------------------------------


def matched_tours(legacy_tours: pl.DataFrame, new_tours: pl.DataFrame) -> dict[str, object]:
    """Match tours across pipelines on person-day + anchor times, carrying all columns.

    Tour numbers are invented independently by each pipeline, so tours match on
    (hhno, pno, day) plus the three anchor times (leave origin, arrive destination,
    arrive back at origin). Legacy HHMM is normalized to minutes (monotone and
    arithmetic-safe) to match the new pipeline's encoding. Every tour column is
    carried through, suffixed _new on the new side, so any column can be compared on
    matched tours. Person-days where two tours share all three anchor times are
    dropped (they would join many-to-many) and counted.

    Returns the matched frame and summary stats (purpose/mode agreement retained for
    the executive summary).
    """
    anchors = ["tlvorig", "tardest", "tarorig"]  # the match key (all three anchor times)
    keys = ["hhno", "pno", "day", *anchors]

    # Normalize EVERY legacy tour clock column to minutes -- not just the anchors --
    # so non-anchor times (tlvdest) compare correctly too.
    all_times = ["tlvorig", "tardest", "tlvdest", "tarorig"]
    leg = normalize_times(legacy_tours, all_times)  # HHMM -> minutes
    new = new_tours  # already minutes; matches legacy after normalization

    def _unambiguous(df: pl.DataFrame) -> tuple[pl.DataFrame, int]:
        dup = df.group_by(keys).agg(pl.len().alias("_n")).filter(pl.col("_n") > 1).drop("_n")
        clean = df.join(dup, on=keys, how="anti")
        return clean, df.height - clean.height

    leg, n_leg_amb = _unambiguous(leg)
    new, n_new_amb = _unambiguous(new)

    new_suffixed = new.rename({c: f"{c}_new" for c in new.columns if c not in keys})
    matched = leg.join(new_suffixed, on=keys, how="inner").with_columns(
        # The anchor times are the match key, hence equal by construction; expose _new
        # copies so the coverage table reports them uniformly (100% within tolerance).
        *[pl.col(a).alias(f"{a}_new") for a in anchors]
    )
    n_leg, n_new, n_match = legacy_tours.height, new_tours.height, matched.height
    return {
        "matched": matched,
        "n_legacy": n_leg,
        "n_new": n_new,
        "n_matched": n_match,
        "n_ambiguous": n_leg_amb + n_new_amb,
        "match_rate_pct": 100 * n_match / n_leg if n_leg else 0.0,
        "purpose_agree_pct": 100
        * matched.filter(pl.col("pdpurp") == pl.col("pdpurp_new")).height
        / max(n_match, 1),
        "mode_agree_pct": 100
        * matched.filter(pl.col("tmodetp") == pl.col("tmodetp_new")).height
        / max(n_match, 1),
    }


# ---------------------------------------------------------------------
# Sanity checks
# ---------------------------------------------------------------------


#: Records one check's outcome: (name, ok, detail). Each ``_check_*`` below takes one
#: and reports through it, so the checks stay independent of how results are collected.
CheckFn = Callable[..., None]


def _check_inputs(check: CheckFn, basis: str) -> bool:
    """Inputs reachable. Returns False if nothing downstream can run."""
    try:
        _mtc(LEGACY_DIR)
    except FileNotFoundError as exc:
        check("legacy outputs reachable", ok=False, detail=str(exc))
        return False
    check("legacy outputs reachable", ok=True, detail=str(_mtc(LEGACY_DIR)))

    for b in ("raked", basis):
        ok = (NEW_OUT[b] / "daysim_2023").exists()
        if ok:
            suffix = ""
        else:
            suffix = " missing — run run_vendor_weights.py" if b == "vendor" else " missing"
        check(f"new pipeline outputs ({b})", ok=ok, detail=str(NEW_OUT[b]) + suffix)
        if not ok:
            return False
    return True


def _check_tables_nonempty(check: CheckFn, legacy: dict, new: dict) -> None:
    """Every DaySim table has rows on both sides."""
    for name in DAYSIM_TABLES:
        check(
            f"tables non-empty: {name}",
            ok=legacy[name].height > 0 and new[name].height > 0,
            detail=f"legacy={legacy[name].height:,}, new={new[name].height:,}",
        )


def _check_time_encodings(check: CheckFn, legacy: dict) -> None:
    """Legacy clock columns are HHMM (the new pipeline's are minutes).

    Across every clock column -- not just deptm -- since matched_trips / matched_tours
    normalize all of them.
    """
    time_cols = {
        "trip": ["deptm", "arrtm", "endacttm"],
        "tour": ["tlvorig", "tardest", "tlvdest", "tarorig"],
    }
    bad = []
    for tbl, cols in time_cols.items():
        for col in cols:
            leg_col = legacy[tbl].filter(pl.col(col) >= 0)
            if leg_col.height and (
                leg_col[col].max() > 2359 or leg_col.filter(pl.col(col) % 100 >= 60).height
            ):
                bad.append(f"{tbl}.{col}")
    check(
        "time encodings (legacy HHMM / new minutes)",
        ok=not bad,
        detail="all clock columns are HHMM on legacy" if not bad else f"non-HHMM: {bad}",
    )


def _check_pdexpfac(check: CheckFn) -> None:
    """Legacy pdexpfac reconstructs from raw person_weight / n weekdays."""
    recon = check_pdexpfac_reconstruction()
    check(
        "legacy pdexpfac reconstruction",
        ok=recon["pct"] > 99.9,
        detail=f"{recon['matching']:,}/{recon['rows']:,} rows exact ({recon['pct']:.2f}%)",
    )


def _check_crosswalk(check: CheckFn, basis: str) -> None:
    """Raw-trip crosswalk reaches both pipelines and assigns legacy linking groups."""
    xwalk = build_crosswalk(basis)
    n = xwalk.height
    new_cov = 100 * xwalk.filter(pl.col("new_linked_id").is_not_null()).height / n
    in_02b = xwalk.filter(pl.col("leg_deleted").is_not_null())
    leg_cov = 100 * in_02b.height / n
    check("crosswalk: raw->new coverage", ok=new_cov > 99.0, detail=f"{new_cov:.2f}% of raw trips")
    check(
        "crosswalk: raw->legacy(02b) coverage",
        ok=leg_cov > 50.0,  # legacy drops incomplete days/out-of-region up front
        detail=f"{leg_cov:.2f}% of raw trips (legacy filters incomplete days first)",
    )
    grouped = in_02b.filter(pl.col("leg_group").is_not_null()).height
    check(
        "crosswalk: legacy linking groups assigned",
        ok=grouped >= 0.999 * in_02b.height,
        detail=f"{grouped:,}/{in_02b.height:,} 02b rows have a linked-trip group",
    )


def _check_tour_match(check: CheckFn, legacy: dict, new: dict, persons: pl.DataFrame) -> dict:
    """Matched-tour regression anchor (prior analysis found ~83.8%). Returns the match."""
    result = matched_tours(
        filter_universe(legacy["tour"], persons),
        filter_universe(new["tour"], persons),
    )
    check(
        "matched-tour rate near prior anchor (83.8%)",
        ok=abs(result["match_rate_pct"] - 83.8) < 2.0,
        detail=f"{result['match_rate_pct']:.1f}% matched, "
        f"purpose agree {result['purpose_agree_pct']:.1f}%, "
        f"mode agree {result['mode_agree_pct']:.1f}%",
    )
    return result


def _check_legacy_split(check: CheckFn, legacy: dict) -> None:
    """Legacy drive-transit split invariants, which guard collapse_legacy_split.

    Every duplicated (hhno,pno,day,tsvid) has exactly 2 rows, modes {3 SOV, 6 walk-
    transit}, and identical trexpfac within the pair.
    """
    dup = (
        legacy["trip"]
        .group_by(["hhno", "pno", "day", "tsvid"])
        .agg(
            pl.len().alias("n"),
            pl.col("mode").sort().alias("modes"),
            pl.col("trexpfac").n_unique().alias("nw"),
        )
        .filter(pl.col("n") > 1)
    )
    split_ok = (
        dup.filter(pl.col("n") != 2).height == 0
        and dup.filter(pl.col("modes") != [3, 6]).height == 0
        and dup.filter(pl.col("nw") != 1).height == 0
    )
    check(
        "legacy drive-transit split invariants",
        ok=bool(split_ok),
        detail=f"{dup.height:,} split trips, all 2 rows / modes {{3,6}} / equal weight",
    )


def _check_trip_bridge(
    check: CheckFn, legacy: dict, new: dict, persons: pl.DataFrame, basis: str
) -> None:
    """Trip bridge coverage and the falsifiable guard.

    If the mutual-best bridge were pairing unrelated trips, the departure times of
    "exact" matches would not line up -- so >95% must agree on deptm within 5 minutes.
    """
    mt = matched_trips(
        filter_universe(legacy["trip"], persons),
        filter_universe(new["trip"], persons),
        basis=basis,
    )
    check(
        "trip bridge coverage",
        ok=mt["match_rate_pct"] > 90.0,
        detail=f"{mt['n_matched']:,}/{mt['n_legacy']:,} legacy trips matched "
        f"({mt['match_rate_pct']:.1f}%); {mt['n_exact']:,} exact, {mt['n_partial']:,} partial, "
        f"{mt['n_legacy_only']:,} legacy-only, {mt['n_new_only']:,} new-only",
    )
    exact = mt["matched"].filter(pl.col("match_status") == "matched_exact")
    within = exact.filter((pl.col("deptm") - pl.col("deptm_new")).abs() <= 5).height
    dep_pct = 100 * within / max(exact.height, 1)
    check(
        "trip bridge: exact matches agree on departure time (±5 min)",
        ok=dep_pct > 95.0,
        detail=f"{dep_pct:.1f}% of {exact.height:,} exact matches within 5 min",
    )


def _check_tour_anchors(check: CheckFn, result: dict) -> None:
    """Tour anchor-match is nearly unambiguous.

    Few person-days have two tours sharing all three anchor times, which would
    otherwise join many-to-many.
    """
    check(
        "tour anchor-match unambiguous",
        ok=result["n_ambiguous"] < 0.02 * result["n_legacy"],
        detail=f"{result['n_ambiguous']:,} ambiguous tours dropped of {result['n_legacy']:,}",
    )


def _check_zone_bridge(check: CheckFn, basis: str) -> None:
    """Zone bridge resolves for every TAZ column that claims a clean bridge."""
    zone_ok = all(taz1454(tbl, col, basis) is not None for (tbl, col) in NEW_TAZ1454)
    check(
        "zone bridge (TAZ1454) available",
        ok=zone_ok,
        detail=f"{len(NEW_TAZ1454)} taz columns bridged to TAZ1454",
    )


def _check_spec_drift(check: CheckFn, legacy: dict, new: dict) -> None:
    """No spec column may silently stop being emitted, or the report goes incomplete.

    Undeclared emitted columns are reported but not fatal.
    """
    dropped, leaked = [], []
    for tbl in spec.MODELS:
        lc, nc = set(legacy[tbl].columns), set(new[tbl].columns)
        dropped += [f"{tbl}.{c}" for c in spec.missing_from_both(tbl, lc, nc)]
        leaked += [f"{tbl}.{c}" for c in spec.undeclared(tbl, nc)]
    check(
        "spec coverage: no column silently dropped",
        ok=not dropped,
        detail=(
            f"all {spec.spec_column_count()} spec columns emitted"
            + (f"; new leaks {len(leaked)} non-spec columns" if leaked else "")
        )
        if not dropped
        else f"columns emitted by neither pipeline: {dropped}",
    )


def _check_offspec_values(check: CheckFn, legacy: dict, new: dict) -> None:
    """Every observed coded value is in its codebook domain.

    Columns with a registered divergence (e.g. legacy's -1 stubs) are exempt -- their
    off-domain values are already documented; this guards the rest.
    """
    offspec = []
    for tbl in spec.MODELS:
        for cs in spec.columns_of_kind(tbl, spec.Kind.CATEGORICAL):
            if not cs.labels or spec.divergence(tbl, cs.name):
                continue
            for side, df in (("legacy", legacy[tbl]), ("new", new[tbl])):
                if cs.name in df.columns:
                    vals = set(df[cs.name].drop_nulls().unique().to_list())
                    bad = spec.offspec_values(cs, vals)
                    if bad:
                        offspec.append(f"{side} {tbl}.{cs.name}={bad}")
    check(
        "off-spec coded values",
        ok=not offspec,
        detail="all coded values in domain (documented divergences exempt)"
        if not offspec
        else "; ".join(offspec),
    )


def _check_mapping_integrity(
    check: CheckFn, legacy: dict, new: dict, hhnos: list, persons: pl.DataFrame
) -> None:
    """Directly-mapped scalar attributes should agree 1:1 on non-missing legacy values.

    This is a DATA finding, not an environment gate -- it reports (does not fail the
    render) so discrepancies surface for follow-up.
    """
    leg_uni = {
        "hh": legacy["hh"].filter(pl.col("hhno").is_in(hhnos)),
        "person": filter_universe(legacy["person"], persons),
    }
    new_uni = {
        "hh": new["hh"].filter(pl.col("hhno").is_in(hhnos)),
        "person": filter_universe(new["person"], persons),
    }
    dirty: list[str] = []
    for tbl in ("hh", "person"):
        mi = spec.mapping_integrity(tbl, leg_uni[tbl], new_uni[tbl])
        dirty.extend(
            f"{tbl}.{r['column']}={r['% agree']}% ({r['n disagree']:,} disagree)"
            for r in mi.iter_rows(named=True)
            if not r["clean"]
        )
    check(
        "mapping integrity (directly-mapped attributes)",
        ok=True,  # informational: reported in the report's integrity section
        detail="all mapped attributes 1:1 on non-missing legacy values"
        if not dirty
        else "discrepancies to investigate: " + "; ".join(dirty),
    )


def run_all_checks(basis: str = "vendor") -> pl.DataFrame:
    """Run the fail-fast sanity checks; returns a summary table.

    Called at the top of validation_report.qmd so a broken environment fails
    the render rather than producing a silently wrong document.
    """
    results: list[dict[str, object]] = []

    def check(name: str, ok: bool, detail: str) -> None:
        results.append({"check": name, "ok": ok, "detail": detail})
        logger.info("%s %s: %s", "PASS" if ok else "FAIL", name, detail)

    if not _check_inputs(check, basis):
        return pl.DataFrame(results)

    legacy = load_legacy_daysim()
    new = load_new_daysim(basis)
    hhnos = common_households(legacy["hh"], new["hh"])
    persons = common_persons(legacy["personday"], new_days_with_dow(basis), hhnos)

    _check_tables_nonempty(check, legacy, new)
    _check_time_encodings(check, legacy)
    _check_pdexpfac(check)
    _check_crosswalk(check, basis)
    tours = _check_tour_match(check, legacy, new, persons)
    _check_legacy_split(check, legacy)
    _check_trip_bridge(check, legacy, new, persons, basis)
    _check_tour_anchors(check, tours)
    _check_zone_bridge(check, basis)
    _check_spec_drift(check, legacy, new)
    _check_offspec_values(check, legacy, new)
    _check_mapping_integrity(check, legacy, new, hhnos, persons)
    return pl.DataFrame(results)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    summary = run_all_checks()
    for check in summary.to_dicts():  # ASCII-safe on Windows consoles
        print(f"[{'PASS' if check['ok'] else 'FAIL'}] {check['check']}: {check['detail']}")
    if not summary["ok"].all():
        sys.exit(1)
