"""Polars schemas from the canonical models, and the empty or derived tables a test leaves out."""

from datetime import datetime
from enum import Enum
from typing import get_args

import polars as pl
from pydantic import BaseModel

import processing  # noqa: F401 - registers every step's contract
from data_canon.core.dataclass import CanonicalData
from pipeline.step_registry import STEP_REGISTRY, is_config_named
from processing.habitual_locations import reported_habitual_locations
from processing.weighting.core.hierarchy import LEVELS
from utils.create_ids import create_household_days

# Each canonical table's model.
_MODELS = CanonicalData().models
# The joint ids each table carries once joint detection and tour extraction ran.
_JOINT_IDS = {"tours": "joint_tour_id", "linked_trips": "joint_trip_id"}
_POLARS_TYPES = {
    int: pl.Int64,
    float: pl.Float64,
    str: pl.String,
    bool: pl.Boolean,
    datetime: pl.Datetime,
}


def model_to_polars_schema(model: type[BaseModel]) -> dict[str, type]:
    """The polars type of each of *model*'s fields; optional fields take their inner type."""
    schema = {}
    for name, field in model.model_fields.items():
        args = get_args(field.annotation)
        if args and type(None) in args:
            schema[name] = _python_type_to_polars(next(t for t in args if t is not type(None)))
        else:
            schema[name] = _python_type_to_polars(field.annotation)
    return schema


def _python_type_to_polars(python_type) -> type:
    """Int64 for ints and enums with int values, String for anything unrecognised."""
    if python_type in _POLARS_TYPES:
        return _POLARS_TYPES[python_type]
    if getattr(python_type, "__origin__", None) in _POLARS_TYPES:
        return _POLARS_TYPES[python_type.__origin__]
    if isinstance(python_type, type) and issubclass(python_type, Enum):
        first = next(iter(python_type), None)
        if first is not None and isinstance(first.value, int):
            return pl.Int64
    if int in getattr(python_type, "__bases__", ()):
        return pl.Int64
    return pl.String


def _generated(table: str) -> dict[str, type]:
    """The usability flag and the weight: named by config, so not model fields.

    Every frame reaching a formatter has been through the steps that write them.
    The weight takes its base name, which is what a consumer reads once it has
    resolved its own profile.
    """
    columns = {"usable_test": pl.Boolean}
    if table in LEVELS:
        columns[LEVELS[table].weight_col] = pl.Float64
    return columns


def empty(table: str) -> pl.DataFrame:
    """An empty *table* frame: its model's columns, the usability flag and the weight."""
    return pl.DataFrame(schema={**model_to_polars_schema(_MODELS[table]), **_generated(table)})


def get_tour_schema() -> dict[str, type]:
    """The schema of ``create_tour`` rows: the model's columns and the extra ones it fills."""
    return {
        **model_to_polars_schema(_MODELS["tours"]),
        "person_num": pl.Int64,
        "day_num": pl.Int64,
        "o_taz": pl.Int64,
        "d_taz": pl.Int64,
        "o_TAZ1454": pl.Int64,
        "d_TAZ1454": pl.Int64,
        "travel_dow": pl.Int64,
        "student_category": pl.String,
        **_generated("tours"),
    }


def days_for_persons(
    persons: pl.DataFrame, day_num: int = 1, usability_flag_col: str = "usable_test"
) -> pl.DataFrame:
    """One day per person, keyed ``person_id * 100 + day_num`` as the pipeline does.

    ``format_ctramp`` inner-joins persons to ``person_days``, so a person with no
    row here gets no CT-RAMP output.
    """
    if persons.is_empty():
        return empty("person_days")
    return (
        persons.select(
            (pl.col("person_id") * 100 + day_num).cast(pl.Int64).alias("person_day_id"),
            pl.col("person_id"),
            pl.col("hh_id"),
            (pl.col("hh_id") * 100 + day_num).cast(pl.Int64).alias("hh_day_id"),
            # One shared date per day number, so a household's members share it.
            pl.lit(datetime(2024, 1, day_num).date()).alias("travel_date"),
        )
        .unique()
        .with_columns(pl.lit(value=True).alias(usability_flag_col))
    )


def add_household_days(tables: dict) -> dict:
    """Build ``household_days`` and ``hh_day_id`` from ``tables["person_days"]``, in place.

    The pipeline gets them from the project cleaner; hand-built test tables need
    them before the cascade, which requires both. A fixture without ``day_num``
    has its household's dates numbered in order. Returns *tables*, so a call
    can wrap the argument it prepares.
    """
    person_days = tables.get("person_days")
    if person_days is None or "household_days" in tables:
        return tables
    numbered = "day_num" not in person_days.columns
    if numbered:
        person_days = person_days.with_columns(
            pl.col("travel_date").rank("dense").over("hh_id").cast(pl.Int64).alias("day_num")
        )
    person_days, tables["household_days"] = create_household_days(person_days)
    tables["person_days"] = person_days.drop("day_num") if numbered else person_days
    return tables


def step_tables(step: str, tables: dict) -> dict:
    """The tables *step* takes, from *tables*, with an empty frame for each one left out.

    A household with no unlinked trips, say, is a valid input, but a step that
    reads the table needs the empty table rather than none. Each empty frame has
    the table's model columns and the columns the step requires of it. An
    optional table left out stays out.
    """
    out = {}
    for name, contract in STEP_REGISTRY[step].taken.items():
        if tables.get(name) is not None:
            out[name] = tables[name]
        elif not contract.optional:
            types = model_to_polars_schema(_MODELS[name]) if name in _MODELS else {}
            required = sorted(c for c in contract.requires if not is_config_named(c))
            columns = list(dict.fromkeys([*types, *required]))
            out[name] = pl.DataFrame(schema={c: types.get(c, pl.Int64) for c in columns})
    return out


def reported_locations(
    households: pl.DataFrame, persons: pl.DataFrame, zones: list[str]
) -> dict[str, pl.DataFrame]:
    """The habitual tables survey cleaning delivers for *households* and *persons*.

    Every location is a reported one, so none has days spent at it. Each zone
    in *zones* gets its column, null: the formatters read the zones config names.
    """
    located = reported_habitual_locations(households, persons)
    return {
        "habitual_locations": located.with_columns(
            pl.lit(None, dtype=pl.Int64).alias(zone) for zone in zones
        ),
        "habitual_location_days": pl.DataFrame(
            schema=model_to_polars_schema(_MODELS["habitual_location_days"])
        ),
    }


def add_missing_tables(tables: dict) -> dict:
    """Give *tables* every table the cascade reads, an empty one where the test left it out.

    Builds ``household_days`` from the person-days first (see
    :func:`add_household_days`). A tour or trip table without its joint id gets
    one, null: joint detection ran and found no joint travel. Returns *tables*,
    in place.
    """
    add_household_days(tables)
    for name, column in _JOINT_IDS.items():
        df = tables.get(name)
        if df is not None and column not in df.columns:
            tables[name] = df.with_columns(pl.lit(None, dtype=pl.Int64).alias(column))
    tables.update(step_tables("cascade_usability", tables))
    return tables
