"""The DaySim spec as data: column enumeration, classification and labels.

The report compares two independent implementations of the same contract, so the
contract itself — the DaySim pydantic models in ``src/data_canon/models/daysim.py``
— drives which columns get compared. Nothing here hardcodes a column list: every
column, its allowed values and its comparison strategy are read off the model's
``FieldInfo`` (type annotation, ge/le bounds, description).

That is what keeps the report exhaustive. Add a field to a model, or change a
codebook enum, and the column appears in the coverage table on the next render
(and ``check_spec_drift`` in helpers.py fails the render if a pipeline starts or
stops emitting a column behind the spec's back).

This module is **pure**: no data, no network, no I/O. It is imported by helpers.py
(which does touch data), so it must never import helpers.

Run standalone to print the classification of every spec column:

    uv run python scripts/validate_sfcta/spec.py
"""

import inspect
import sys
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import get_args, get_origin

import polars as pl
from pydantic.fields import FieldInfo

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))

from data_canon.codebook.daysim import (  # noqa: E402
    DaysimPaidParking,
    DaysimStudentType,
)
from data_canon.core.labeled_enum import LabeledEnum  # noqa: E402
from data_canon.models.daysim import (  # noqa: E402
    HouseholdDayDaysimModel,
    HouseholdDaysimModel,
    LinkedTripDaysimModel,
    PersonDayDaysimModel,
    PersonDaysimModel,
    TourDaysimModel,
)

# ---------------------------------------------------------------------
# The spec: which models back which comparison table
# ---------------------------------------------------------------------

# Report table name -> DaySim model. These are the five tables BOTH pipelines emit.
MODELS: dict[str, type] = {
    "hh": HouseholdDaysimModel,
    "person": PersonDaysimModel,
    "personday": PersonDayDaysimModel,
    "tour": TourDaysimModel,
    "trip": LinkedTripDaysimModel,
}

# In the spec but emitted by neither pipeline. Reported as such rather than silently
# omitted -- "we didn't look" and "nobody writes it" are different statements.
UNEMITTED_MODELS: dict[str, type] = {"householdday": HouseholdDayDaysimModel}

# Join keys. Households, persons and person-days join directly on these; tours and
# trips do not (their `tour` / `tsvid` sequence numbers are invented independently by
# each pipeline), so they are matched on content -- see helpers.matched_tours /
# helpers.matched_trips -- and their id columns are classified ID, not KEY.
KEYS: dict[str, list[str]] = {
    "hh": ["hhno"],
    "person": ["hhno", "pno"],
    "personday": ["hhno", "pno", "day"],
    "tour": ["hhno", "pno", "day"],
    "trip": ["hhno", "pno", "day"],
    "householdday": ["hhno", "day"],
}

# Sequence numbers each pipeline invents for itself: comparable in distribution but
# never joinable, and never an agreement number.
ID_COLUMNS: set[str] = {"tour", "tsvid", "half", "tseg", "parent", "jtindex"}

# Columns both pipelines emit that the spec puts on a different table (or not at all).
# Declared here so check_spec_drift can tell "undeclared column appeared" (a real
# finding) from "known implementation-vs-spec mismatch" (documented in DIVERGENCES).
EXTRA_COLUMNS: dict[str, dict[str, str]] = {
    # The model declares these on PersonDayDaysimModel; both pipelines write them on
    # the person table instead. See DIVERGENCES[("person", "pwxco")].
    "person": {
        "pwxco": "coord",
        "pwyco": "coord",
        "psxco": "coord",
        "psyco": "coord",
    },
    # Tour end coordinates: absent from TourDaysimModel, emitted by both pipelines,
    # and genuinely comparable -- so they are compared.
    "tour": {
        "toxco": "coord",
        "toyco": "coord",
        "tdxco": "coord",
        "tdyco": "coord",
    },
    # New-pipeline bookkeeping column with no legacy counterpart.
    "trip": {"tripno": "id"},
}

# Spec fields known to be absent from an output. Anything absent and NOT listed here
# fails check_spec_drift -- so a pipeline silently dropping a column is caught.
KNOWN_ABSENT: dict[str, set[str]] = {
    "person": {"pwautime", "pwaudist", "psautime", "psaudist"},  # legacy: not emitted
    "personday": {"pwxco", "pwyco", "psxco", "psyco"},  # neither emits (see EXTRA_COLUMNS)
}


# ---------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------


class Kind(StrEnum):
    """How a column is compared and plotted."""

    KEY = "key"  # joins the two pipelines; not itself compared
    ID = "id"  # independently-invented sequence number; distribution only
    CATEGORICAL = "categorical"  # coded value with a labelled domain
    COUNT = "count"  # bounded non-negative integer (0..99)
    CONTINUOUS = "continuous"  # unbounded numeric
    TIME = "time"  # clock time; legacy writes HHMM, new writes minutes
    COORD = "coord"  # longitude / latitude degrees
    ZONE = "zone"  # TAZ or parcel id
    WEIGHT = "weight"  # expansion factor


# Agreement tolerances, by kind. Exact equality is the wrong test for floats, and for
# clock times a sub-minute difference is an encoding artefact, not a disagreement.
TOLERANCE: dict[Kind, float] = {
    Kind.WEIGHT: 1e-6,  # relative
    Kind.COORD: 1e-3,  # degrees, ~100 m: the two pipelines round coordinates to
    #                    different precisions, so a metre-scale tolerance flags pure
    #                    rounding as disagreement. 100 m still catches real relocations.
    Kind.CONTINUOUS: 1e-6,  # absolute
    Kind.TIME: 5.0,  # minutes
}

# Zone columns with a clean TAZ1454 bridge (helpers.NEW_TAZ1454). Every other taz/parcel
# column is an independent id space (legacy TM1 TAZ1454 vs new TM2 TAZ_NODE, or the
# parcel field carrying a TAZ id) and is not comparable by value -- the geography is
# validated through the coordinate columns instead.
BRIDGED_ZONES: frozenset[tuple[str, str]] = frozenset(
    {("hh", "hhtaz"), ("person", "pwtaz"), ("person", "pstaz")}
)

#: Value -> label for a coded column. Keys are `int | str` because that is what
#: `LabeledEnum.to_dict()` returns -- every DaySim code is an int in practice, but the
#: codebook's contract permits string-valued members, so the map is typed to match it.
LabelMap = dict[int | str, str]

#: The model types these as plain bounded ints, but a codebook enum for them already
#: exists. The only column->enum hardcoding permitted here, and it points at the
#: codebook rather than restating its members.
LABEL_OVERRIDES: dict[str, type[LabeledEnum]] = {
    "pstyp": DaysimStudentType,
    "ppaidprk": DaysimPaidParking,
}

#: Plain 0/1 flags. Not worth a codebook enum, but "No"/"Yes" beats "0"/"1" on an axis.
FLAG_COLUMNS: frozenset[str] = frozenset({"beghom", "endhom", "ptpass", "pdiary", "pproxy"})
FLAG_LABELS: LabelMap = {0: "No", 1: "Yes"}

# Deliberately NOT labelled: oadtyp/dadtyp/toadtyp/tdadtyp. The two pipelines disagree
# on what the codes mean (the new one hardcodes 3 = "other"; legacy's meaning is not
# recoverable from its source), and inventing a shared codebook would paper over that.
# They are classified CATEGORICAL with raw integer labels and carry a divergence note.


def _enum_of(annotation: object) -> type[LabeledEnum] | None:
    """The LabeledEnum behind a field annotation, unwrapping `X | None` unions.

    Mirrors utils.enum_helpers._extract_enum_from_annotation, which is private and
    bound to the canonical survey models; this keeps the report decoupled from it.
    """
    if get_origin(annotation) is not None:
        for arg in get_args(annotation):
            if arg is not type(None) and inspect.isclass(arg) and issubclass(arg, LabeledEnum):
                return arg
        return None
    if inspect.isclass(annotation) and issubclass(annotation, LabeledEnum):
        return annotation
    return None


def _bound(field: FieldInfo, attr: str) -> float | None:
    """Read a ge/le constraint off a pydantic FieldInfo."""
    for meta in field.metadata:
        if (value := getattr(meta, attr, None)) is not None:
            return float(value)
    return None


# Scalar attributes copied straight from a single survey field with no modeling: they
# must map 1:1, so agreement below 100% on non-missing values is a suspected bug, not
# expected imputation noise. Coordinates and zones are also mapped (geocoded) but are
# handled via tolerance / the zone bridge, so they carry the "mapped" tag for display
# without entering the strict integrity check (see mapping_integrity). Everything else
# (person/worker types, tour & stop counters, weights, hhincome's representative dollar)
# is derived and legitimately may differ.
MAPPED_ATTRIBUTES: frozenset[str] = frozenset(
    {"hhsize", "hhvehs", "hownrent", "hrestype", "pagey", "pgend"}
)

# Legacy values the new pipeline legitimately overwrites (imputes), keyed by column.
# Excluded from the mapping-integrity check so documented imputation is not mistaken for
# a mapping bug. Default (any column) is the -1 / null missing sentinel; pgend also has
# the new pipeline force non-binary (3) and missing (9) gender to M/F for ACS weighting.
IMPUTED_SENTINELS: dict[str, set[int]] = {"pgend": {3, 9}}


@dataclass(frozen=True)
class ColumnSpec:
    """One column of the DaySim spec, with everything needed to compare and plot it."""

    table: str
    name: str
    kind: Kind
    labels: LabelMap | None
    ge: float | None
    le: float | None
    description: str
    in_spec: bool  # False for EXTRA_COLUMNS

    @property
    def tolerance(self) -> float | None:
        """How close two values must be to count as agreeing, or None for exact match."""
        return TOLERANCE.get(self.kind)

    @property
    def plottable(self) -> bool:
        """Keys carry no information; everything else is worth a panel if it varies."""
        return self.kind is not Kind.KEY

    @property
    def derivation(self) -> str:
        """Whether the column is "mapped" (1:1 survey passthrough) or "derived" (computed)."""
        if self.kind in (Kind.KEY, Kind.ID):
            return ""
        if self.kind in (Kind.COORD, Kind.ZONE) or self.name in MAPPED_ATTRIBUTES:
            return "mapped"
        return "derived"


def _categorical_labels(name: str, field: FieldInfo) -> LabelMap | None:
    """Value labels if the column is a *labelled* categorical, else None.

    Three label sources, in precedence order: the codebook enum on the annotation, the
    two override columns the model types as plain ints, and the 0/1 flag columns.
    """
    if (enum_cls := _enum_of(field.annotation)) is not None:
        return enum_cls.to_dict()
    if name in LABEL_OVERRIDES:
        return LABEL_OVERRIDES[name].to_dict()
    if name in FLAG_COLUMNS:
        return dict(FLAG_LABELS)
    return None


def _kind_by_suffix(name: str) -> Kind | None:
    """Kind implied by the column's name suffix, or None if the name says nothing."""
    if name.endswith("expfac"):
        return Kind.WEIGHT
    if name.endswith(("xco", "yco")):
        return Kind.COORD
    if name.endswith(("taz", "pcl", "parcel")):
        return Kind.ZONE
    return None


def _kind_by_bounds(description: str, ge: float | None, le: float | None) -> Kind:
    """Fallback kind, read from the field's description and numeric bounds."""
    # Clock times, per the spec's own words. "minutes after midnight" appears in the
    # description of exactly deptm/arrtm/endacttm and the four tour anchors; wkathome's
    # description ("minutes spent working at home") correctly does not match.
    if "minutes after midnight" in description:
        return Kind.TIME
    # Small bounded ints with no codebook: address types, usual arrival/departure
    # periods. Labelled with their raw values -- see the note above FLAG_COLUMNS.
    if le is not None and le <= 9:
        return Kind.CATEGORICAL
    # Bounded non-negative integers: the household composition counts, the person-day
    # tour/stop counters, and the tour half-tour trip counts (which start at 1, not 0).
    if ge is not None and ge >= 0 and le == 99:
        return Kind.COUNT
    return Kind.CONTINUOUS


def classify(table: str, name: str, field: FieldInfo | None = None) -> ColumnSpec:
    """Assign a column its comparison strategy, reading only the pydantic FieldInfo.

    Rules are ordered; first match wins. The ordering matters: `parent` and `jtindex`
    are ints in 0..99 and would otherwise land in COUNT, and `wkathome` is a *duration*
    in minutes that must not be mistaken for a clock TIME.
    """
    ge = _bound(field, "ge") if field else None
    le = _bound(field, "le") if field else None
    description = (field.description or "") if field else ""
    in_spec = field is not None

    def built(kind: Kind, labels: LabelMap | None = None) -> ColumnSpec:
        return ColumnSpec(table, name, kind, labels, ge, le, description, in_spec)

    # Declared extras carry their kind in EXTRA_COLUMNS rather than a FieldInfo.
    if field is None:
        return built(Kind(EXTRA_COLUMNS.get(table, {}).get(name, "continuous")))
    if name in KEYS.get(table, []):
        return built(Kind.KEY)
    if name in ID_COLUMNS:
        return built(Kind.ID)
    if (labels := _categorical_labels(name, field)) is not None:
        return built(Kind.CATEGORICAL, labels)
    if (kind := _kind_by_suffix(name)) is not None:
        return built(kind)
    return built(_kind_by_bounds(description, ge, le))


def columns(table: str) -> list[ColumnSpec]:
    """Every column of a table: spec fields in spec order, then declared extras."""
    model = MODELS.get(table) or UNEMITTED_MODELS[table]
    out = [classify(table, name, field) for name, field in model.model_fields.items()]
    out += [classify(table, name) for name in EXTRA_COLUMNS.get(table, {})]
    return out


def column(table: str, name: str) -> ColumnSpec:
    """One column by name."""
    for cs in columns(table):
        if cs.name == name:
            return cs
    msg = f"'{name}' is not a column of the '{table}' spec"
    raise KeyError(msg)


def labels(table: str, name: str) -> LabelMap | None:
    """Value -> label for a categorical column, straight from the codebook enum."""
    return column(table, name).labels


def columns_of_kind(table: str, kind: Kind) -> list[ColumnSpec]:
    """Every column of a table classified as the given kind, in spec order."""
    return [cs for cs in columns(table) if cs.kind is kind]


def time_columns(table: str) -> list[str]:
    """Clock-time columns, which legacy writes as HHMM and the new pipeline as minutes."""
    return [cs.name for cs in columns_of_kind(table, Kind.TIME)]


def spec_column_count() -> int:
    """Total spec fields across the five emitted tables -- the report's coverage target."""
    return sum(len(model.model_fields) for model in MODELS.values())


# ---------------------------------------------------------------------
# Divergence registry
# ---------------------------------------------------------------------

#: Why a column disagrees, keyed by (table, column). Consumed by BOTH the coverage
#: table's note column and the Divergences narrative, so a flagged or non-comparable
#: column can never appear in the report without a written cause. Each entry says
#: whether the cause is a config switch, a spec mismatch, an encoding convention, or
#: an unimplemented field.
DIVERGENCES: dict[tuple[str, str], str] = {
    # --- encoding conventions (no effect on any distribution) ---
    ("hh", "hownrent"): (
        "encoding: legacy codes missing as 9, the spec and the new pipeline use -1. "
        "Remapped before comparison (helpers.LEGACY_CODE_REMAP)."
    ),
    ("hh", "hrestype"): (
        "encoding: legacy codes missing as 9, the spec and the new pipeline use -1. "
        "Remapped before comparison (helpers.LEGACY_CODE_REMAP)."
    ),
    ("hh", "hhtaz"): (
        "config: the DaySim taz field carries TM2 TAZ_NODE by default; legacy carries "
        "TM1 TAZ1454. Same geography -- compared on TAZ1454 via the zone bridge. "
        "Set by config (taz_field)."
    ),
    ("hh", "hhincome"): (
        "both derive a representative dollar value from the survey's detailed income "
        "bracket but pick different midpoints within it, so exact-dollar match is "
        "meaningless. Compared on the source brackets in the Households section."
    ),
    ("person", "pgend"): (
        "the new pipeline imputes missing / non-binary / prefer-not-to-answer gender to "
        "M/F (required for ACS weighting); legacy leaves it as reported. No disagreement "
        "for anyone who reported M or F."
    ),
    # --- unimplemented fields ---
    ("person", "puwmode"): "unimplemented in legacy (constant -1); the new pipeline derives it.",
    ("person", "ptpass"): "unimplemented in legacy (constant -1); the new pipeline derives it.",
    ("person", "pproxy"): "unimplemented in legacy (constant -1); the new pipeline derives it.",
    ("person", "ppaidprk"): "unimplemented in legacy (constant -1); the new pipeline derives it.",
    ("person", "pdiary"): "unimplemented in legacy (constant -1); the new pipeline derives it.",
    ("person", "puwarrp"): "unimplemented in both pipelines (constant -1).",
    ("person", "puwdepp"): "unimplemented in both pipelines (constant -1).",
    ("person", "pwautime"): "unimplemented: skim-derived, needs a network. Not emitted by legacy.",
    ("person", "pwaudist"): "unimplemented: skim-derived, needs a network. Not emitted by legacy.",
    ("person", "psautime"): "unimplemented: skim-derived, needs a network. Not emitted by legacy.",
    ("person", "psaudist"): "unimplemented: skim-derived, needs a network. Not emitted by legacy.",
    ("trip", "travtime"): "unimplemented in the new pipeline (constant -1): skim-derived.",
    ("trip", "travcost"): "unimplemented in the new pipeline (constant -1): skim-derived.",
    ("trip", "travdist"): "unimplemented in the new pipeline (constant -1): skim-derived.",
    ("tour", "tautotime"): "unimplemented in the new pipeline (constant -1): skim-derived.",
    ("tour", "tautocost"): "unimplemented in the new pipeline (constant -1): skim-derived.",
    ("tour", "tautodist"): "unimplemented in the new pipeline (constant -1): skim-derived.",
    ("hh", "samptype"): "unimplemented in the new pipeline (constant 0); a single sample.",
    ("personday", "wkathome"): "unimplemented in the new pipeline (constant 0).",
    ("tour", "jtindex"): "unimplemented in the new pipeline (constant 0): joint tours not linked.",
    ("tour", "phtindx1"): "unimplemented in the new pipeline (constant 0): partial half tours.",
    ("tour", "phtindx2"): "unimplemented in the new pipeline (constant 0): partial half tours.",
    ("tour", "fhtindx1"): "unimplemented in the new pipeline (constant 0): full half tours.",
    ("tour", "fhtindx2"): "unimplemented in the new pipeline (constant 0): full half tours.",
    ("tour", "tpathtp"): "unimplemented in the new pipeline (constant 1): needs a network.",
    ("tour", "subtrs"): (
        "the new pipeline does not currently detect work-based subtours, so subtrs is 0 "
        "for every tour; legacy detects a small number. (This is the residual after "
        "fixing the earlier formatter bug that set subtrs=1 for all tours.)"
    ),
    ("personday", "wbtours"): (
        "the new pipeline does not currently detect work-based subtours, so wbtours is 0 "
        "for every person-day; legacy finds ~1,600. (Residual after fixing the earlier "
        "formatter bug that made wbtours equal hbtours.)"
    ),
    # --- spec mismatches ---
    ("person", "pwxco"): (
        "spec mismatch: the model declares pwxco/pwyco/psxco/psyco on PersonDay, but "
        "BOTH pipelines write them on the person table. Compared here; the model should "
        "move them."
    ),
    ("tour", "toadtyp"): (
        "the two pipelines do not share an address-type codebook: the new one hardcodes "
        "3 = other on trips and 1 on tours; legacy's coding is not recoverable from its "
        "source. Left unlabelled rather than papering over the disagreement."
    ),
    # --- zone / parcel id spaces ---
    ("hh", "hhparcel"): (
        "incomparable id space: legacy writes the TAZ id into the parcel field. No "
        "parcel geography exists on either side."
    ),
    ("trip", "otaz"): (
        "id space: legacy carries TM1 TAZ1454, the new pipeline TM2 TAZ_NODE. Trip "
        "geography is validated through the o/d coordinate columns instead."
    ),
    ("tour", "totaz"): (
        "id space: legacy carries TM1 TAZ1454, the new pipeline TM2 TAZ_NODE. Tour "
        "geography is validated through the tour-end coordinate columns instead."
    ),
    # --- genuine value divergences (documented, not defects) ---
    ("trip", "endacttm"): (
        "the two pipelines compute the destination activity-end time differently "
        "(a ~15-minute median difference); it is not a match key."
    ),
    ("trip", "trexpfac"): (
        "weight-source difference: each trip takes its person-day weight (see "
        "personday.pdexpfac). Because the two pipelines emit slightly different trip "
        "SETS (tour-formatting drops), the weighted trip total differs by ~0.2%."
    ),
    ("tour", "toexpfac"): (
        "the new pipeline derives the tour weight as the MEAN of its linked-trip weights, "
        "whereas legacy assigns the person-day weight directly; combined with different "
        "tour sets this shifts the weighted tour total by ~1.3%."
    ),
    ("personday", "pdexpfac"): (
        "weight source: legacy spreads each person's single survey weight evenly across "
        "their complete Tue/Wed/Thu diary days (personday_weight = person_weight / number "
        "of complete weekday days) and assigns that to each weekday person-day; the new "
        "pipeline uses the vendor's per-day weight directly. Weighted person-day totals "
        "still agree exactly. Pointing add_existing_weights at person_weight, divided the "
        "same way, reproduces legacy."
    ),
}
DIVERGENCES[("trip", "dtaz")] = DIVERGENCES[("trip", "otaz")]
DIVERGENCES[("tour", "tdtaz")] = DIVERGENCES[("tour", "totaz")]

# Tour destination differs even on anchor-time-matched tours (origin/home agrees):
# the two pipelines select a tour's primary destination by different rules (legacy by
# TAZ equality, the new pipeline by a coordinate buffer that also supports school/other
# anchors), so ~a quarter of matched tours land on a different primary destination.
_DEST_SELECT = (
    "primary-destination selection differs: legacy anchors on TAZ equality, the new "
    "pipeline on a coordinate buffer, so anchor-time-matched tours can resolve to a "
    "different primary destination (median ~3 km apart)."
)
DIVERGENCES[("tour", "tdxco")] = _DEST_SELECT
DIVERGENCES[("tour", "tdyco")] = _DEST_SELECT

# Person-day tour counts that ARE populated on both sides still differ, because tour
# detection differs (the same cause as the primary-destination and matched-tour gaps).
_TOUR_DETECT = (
    "tour detection differs (legacy TAZ-equality anchoring vs the new pipeline's "
    "coordinate buffer with school/other anchors), so the per-day tour counts differ."
)
# (wbtours is NOT here: its former divergence was a formatter bug, now fixed.)
for _col in ("hbtours", "uwtours", "endhom", "beghom"):
    DIVERGENCES[("personday", _col)] = _TOUR_DETECT

# The address-type and parcel notes apply verbatim to their siblings.
for _tbl, _col in (
    ("tour", "tdadtyp"),
    ("trip", "oadtyp"),
    ("trip", "dadtyp"),
):
    DIVERGENCES[(_tbl, _col)] = DIVERGENCES[("tour", "toadtyp")]
for _tbl, _col in (
    ("person", "pwpcl"),
    ("person", "pspcl"),
    ("trip", "opcl"),
    ("trip", "dpcl"),
    ("tour", "topcl"),
    ("tour", "tdpcl"),
):
    DIVERGENCES[(_tbl, _col)] = DIVERGENCES[("hh", "hhparcel")]
for _col in ("pwyco", "psxco", "psyco"):
    DIVERGENCES[("person", _col)] = DIVERGENCES[("person", "pwxco")]

# Every purpose-specific person-day counter: the new pipeline writes 0 for all of them.
for _col in (
    "wktours",
    "sctours",
    "estours",
    "pbtours",
    "shtours",
    "mltours",
    "sotours",
    "retours",
    "metours",
    "wkstops",
    "scstops",
    "esstops",
    "pbstops",
    "shstops",
    "mlstops",
    "sostops",
    "restops",
    "mestops",
):
    DIVERGENCES[("personday", _col)] = (
        "unimplemented in the new pipeline (constant 0): the purpose-specific tour and "
        "stop counters are not populated. Legacy populates them. Only hbtours, wbtours "
        "and uwtours are populated on both sides."
    )


def divergence(table: str, name: str) -> str:
    """The documented cause for a column's divergence, or "" if none is registered."""
    return DIVERGENCES.get((table, name), "")


def declared_columns(table: str) -> set[str]:
    """Spec fields plus declared extras -- the columns the report expects to see."""
    model = MODELS.get(table) or UNEMITTED_MODELS[table]
    return set(model.model_fields) | set(EXTRA_COLUMNS.get(table, {}))


def undeclared(table: str, emitted: set[str]) -> list[str]:
    """Emitted columns that are neither a spec field nor a declared extra."""
    return sorted(emitted - declared_columns(table))


def missing_from_both(table: str, leg_cols: set[str], new_cols: set[str]) -> list[str]:
    """Spec columns emitted by neither pipeline and not marked KNOWN_ABSENT.

    This is the drift guard: a spec column that silently stops being emitted -- which
    would make the report quietly incomplete -- fails the check.
    """
    return sorted(declared_columns(table) - leg_cols - new_cols - KNOWN_ABSENT.get(table, set()))


def offspec_values(cs: ColumnSpec, values: set[int]) -> list[int]:
    """Observed values of a coded column that are not in its codebook domain."""
    if not cs.labels:
        return []
    return sorted(v for v in values if v is not None and int(v) not in cs.labels)


# ---------------------------------------------------------------------
# Comparability and the coverage/agreement table
# ---------------------------------------------------------------------

FLAG_THRESHOLD = 5.0  # flag a comparable column disagreeing on more than this %

# Comparability verdicts, and whether each admits a plot.
COMPARABLE = "comparable"
_STATUS_NOTE = {
    "legacy_absent": "not emitted by the legacy pipeline",
    "new_absent": "not emitted by the new pipeline",
    "both_absent": "emitted by neither pipeline",
    "legacy_constant": "constant in the legacy pipeline (nothing to compare)",
    "new_constant": "constant in the new pipeline (nothing to compare)",
    "both_constant": "constant in both pipelines",
    "id_space": "independent id spaces; not comparable by value",
    "key": "join key",
}


def _constant(df: pl.DataFrame, col: str) -> bool:
    return col in df.columns and df.select(pl.col(col).n_unique()).item() <= 1


def _incomparable_by_kind(cs: ColumnSpec) -> str | None:
    """Non-comparability the column's kind alone decides, else None."""
    if cs.kind is Kind.KEY:
        return "key"
    # Invented sequence numbers never share a value space; parcel ids and unbridged
    # taz columns (TM1 vs TM2) are independent id spaces -- geography is compared via
    # the coordinate columns instead.
    if cs.kind is Kind.ID:
        return "id_space"
    if cs.kind is Kind.ZONE and (cs.table, cs.name) not in BRIDGED_ZONES:
        return "id_space"
    return None


def _incomparable_by_presence(
    cs: ColumnSpec, leg_df: pl.DataFrame, new_df: pl.DataFrame
) -> str | None:
    """Non-comparability from a side not emitting the column at all, else None."""
    in_leg, in_new = cs.name in leg_df.columns, cs.name in new_df.columns
    if not in_leg and not in_new:
        return "both_absent"
    if not in_leg:
        return "legacy_absent"
    if not in_new:
        return "new_absent"
    return None


def _incomparable_by_constancy(
    cs: ColumnSpec, leg_df: pl.DataFrame, new_df: pl.DataFrame
) -> str | None:
    """Non-comparability from a side stubbing the column to one value, else None."""
    leg_const, new_const = _constant(leg_df, cs.name), _constant(new_df, cs.name)
    if leg_const and new_const:
        return "both_constant"
    if leg_const:
        return "legacy_constant"
    if new_const:
        return "new_constant"
    return None


def comparability(cs: ColumnSpec, leg_df: pl.DataFrame, new_df: pl.DataFrame) -> str:
    """Whether a column can be compared by value, and if not, why.

    This is what mechanically decides which columns get a plot: only ``comparable``
    columns do. Stubs (constant on a side), absent columns and id-space columns route
    themselves into the coverage table with an explanation and no panel -- no hardcoded
    exclusion list.

    The three tests are ordered: a column absent from a side cannot be tested for
    constancy, so presence must be settled first.
    """
    return (
        _incomparable_by_kind(cs)
        or _incomparable_by_presence(cs, leg_df, new_df)
        or _incomparable_by_constancy(cs, leg_df, new_df)
        or COMPARABLE
    )


def agreement(cs: ColumnSpec, joined: pl.DataFrame, leg_col: str, new_col: str) -> dict:
    """Agreement statistics for one comparable column on a joined frame.

    ``joined`` carries both pipelines' values as ``leg_col`` and ``new_col``. The
    statistic depends on the kind: exact-match rate for coded/count/zone columns,
    within-tolerance rate (plus median absolute difference) for numeric ones, and
    within-5-minutes for clock times.
    """
    valid = joined.filter(pl.col(leg_col).is_not_null() & pl.col(new_col).is_not_null())
    n = valid.height
    if n == 0:
        return {"n": 0, "stat": "-", "pct": None, "median_abs_diff": None}

    tol = cs.tolerance
    if cs.kind in (Kind.CATEGORICAL, Kind.COUNT, Kind.ZONE, Kind.ID):
        agree = valid.filter(pl.col(leg_col) == pl.col(new_col)).height
        pct = 100 * agree / n
        return {"n": n, "stat": "% exact", "pct": pct, "median_abs_diff": None}

    diff = (pl.col(new_col).cast(pl.Float64) - pl.col(leg_col).cast(pl.Float64)).abs()
    if cs.kind is Kind.WEIGHT:  # relative tolerance
        rel = diff / pl.max_horizontal(pl.col(leg_col).cast(pl.Float64).abs(), pl.lit(1e-9))
        within = valid.filter(rel <= tol).height
        stat = "% within 1e-6 rel"
    elif cs.kind is Kind.TIME:
        within = valid.filter(diff <= tol).height
        stat = "% within 5 min"
    else:  # CONTINUOUS, COORD
        within = valid.filter(diff <= tol).height
        stat = "% within tol"
    med = valid.select(diff.median()).item()
    return {"n": n, "stat": stat, "pct": 100 * within / n, "median_abs_diff": med}


def coverage_table(
    table: str,
    leg_df: pl.DataFrame,
    new_df: pl.DataFrame,
    matched: pl.DataFrame | None = None,
    taz_bridges: dict[str, pl.DataFrame] | None = None,
) -> pl.DataFrame:
    """One row per spec column: presence, comparability, agreement, flag and cause.

    For households / persons / person-days the two pipelines join directly on the
    surviving keys, built here. For tours and trips they do not (independent sequence
    numbers), so ``matched`` -- a frame carrying each column as ``col`` and ``col_new``
    -- must be supplied by helpers.matched_tours / helpers.matched_trips.

    ``taz_bridges`` maps a zone column to its new-pipeline TAZ1454 frame (from
    helpers.taz1454), letting TM1-vs-TM2 zone columns be compared on TAZ1454.
    """
    keys = KEYS.get(table, [])
    direct = matched is None
    if direct:
        joined = leg_df.join(new_df, on=keys, how="inner", suffix="_new")

    rows: list[dict] = []
    for cs in columns(table):
        status = comparability(cs, leg_df, new_df)
        row = {
            "column": cs.name,
            "definition": cs.description
            or ("(not in spec; emitted by both)" if not cs.in_spec else ""),
            "kind": str(cs.kind),
            "map": cs.derivation,
            "legacy": "yes" if cs.name in leg_df.columns else "—",
            "new": "yes" if cs.name in new_df.columns else "—",
            "n": 0,
            "% agree": "-",
            "median |Δ|": "-",
            "note": divergence(table, cs.name) or _STATUS_NOTE.get(status, ""),
        }

        # Zone columns with a TAZ1454 bridge become comparable on that value.
        bridge = (taz_bridges or {}).get(cs.name)
        if status == COMPARABLE or bridge is not None:
            if bridge is not None and direct:
                jb = joined.join(bridge, on=keys, how="left")
                stats = agreement(cs, jb, cs.name, f"{cs.name}_taz1454")
                row["note"] = row["note"] or "compared on TAZ1454 via the zone bridge"
            elif direct:
                stats = agreement(cs, joined, cs.name, f"{cs.name}_new")
            else:
                stats = agreement(cs, matched, cs.name, f"{cs.name}_new")
            row["n"] = stats["n"]
            if stats["pct"] is not None:
                flagged = stats["pct"] < 100 - FLAG_THRESHOLD
                row["column"] = f"⚠ {cs.name}" if flagged else cs.name
                row["% agree"] = f"**{stats['pct']:.1f}**" if flagged else f"{stats['pct']:.1f}"
                if stats["median_abs_diff"] is not None:
                    row["median |Δ|"] = f"{stats['median_abs_diff']:.4g}"

        rows.append(row)
    return pl.DataFrame(rows)


def mapping_integrity(table: str, leg_df: pl.DataFrame, new_df: pl.DataFrame) -> pl.DataFrame:
    """Fidelity of directly-mapped scalar columns, on NON-MISSING legacy values.

    A "mapped" column (a survey passthrough like hhsize or hownrent) must agree 1:1
    where the legacy value is a real value -- not a missing sentinel the new pipeline
    legitimately imputes. Any shortfall is a suspected mapping bug, not expected noise.
    Zones/coords are excluded (compared via the bridge / a tolerance elsewhere); this
    check is the strict integrity test for the scalar attributes.

    Returns one row per checked column: column, n (non-missing), % agree, n_disagree,
    flag. Households/persons only (the directly-joined tables).
    """
    keys = KEYS[table]
    joined = leg_df.join(new_df, on=keys, how="inner", suffix="_new")
    rows: list[dict] = []
    for cs in columns(table):
        if cs.name not in MAPPED_ATTRIBUTES:  # scalar passthroughs only
            continue
        if comparability(cs, leg_df, new_df) != COMPARABLE:
            continue
        col, ncol = cs.name, f"{cs.name}_new"
        # Legacy "has a real, non-imputed value": not null, not -1, and not a value the
        # new pipeline is documented to overwrite (e.g. gender 3/9). Any remaining
        # disagreement is a genuine mapping discrepancy, not expected imputation.
        excluded = {-1, *IMPUTED_SENTINELS.get(cs.name, set())}
        valid = joined.filter(pl.col(col).is_not_null() & ~pl.col(col).is_in(list(excluded)))
        n = valid.height
        if n == 0:
            continue
        n_disagree = valid.filter(pl.col(col) != pl.col(ncol)).height
        pct = 100 * (n - n_disagree) / n
        rows.append(
            {
                "column": cs.name,
                "definition": cs.description,
                "n (legacy real value)": n,
                "% agree": round(pct, 2),
                "n disagree": n_disagree,
                "clean": n_disagree == 0,
                "note": divergence(table, cs.name),
            }
        )
    return pl.DataFrame(rows)


if __name__ == "__main__":
    total = 0
    for table in [*MODELS, *UNEMITTED_MODELS]:
        cols = columns(table)
        total += sum(1 for cs in cols if cs.in_spec)
        print(f"\n=== {table} ({len(cols)} columns) ===")
        for cs in cols:
            note = "" if cs.in_spec else "  [extra: not in spec]"
            dom = f"  {sorted(cs.labels)}" if cs.labels else ""
            print(f"  {cs.name:<12} {cs.kind:<12}{dom}{note}")
    print(f"\n{total} spec fields; {spec_column_count()} across the five emitted tables.")
    print(f"{len(DIVERGENCES)} divergences registered.")
