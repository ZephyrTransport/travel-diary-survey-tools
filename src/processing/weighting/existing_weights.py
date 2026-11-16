"""Attach pre-computed weights to survey data tables.

This module provides the ``add_existing_weights`` pipeline step, which loads
weight CSV files and joins them to the corresponding canonical tables.
Optionally, it can derive missing weights by propagating values through the
survey hierarchy.

# Core algorithm

## Phase 1 -- Load and Join Weights

1. For each provided weight config:

    1. Validate the config key against allowed table types.
    2. Load the weight CSV from ``weight_path``.
    3. Validate required ID and weight columns exist.
    4. Handle ID column name mismatches (rename if needed).
    5. Left-join weights to the table on the ID column.

## Phase 2 -- Move weight off dropped records, per profile

Households, then persons within their household, then each person's days, then
each day's trips. See the Existing Weights docs page for the rules and worked
examples.

## Phase 3 -- Derive Missing Weights (when ``derive_missing_weights=True``)

1. *Down the hierarchy* for household → person → day → unlinked_trip:
   validate parent has weights, then share the parent's weight among its usable
   records (a person's usable days split their weight, rather than each
   carrying all of it).
2. *Aggregated weights* for linked_trip and tour: the mean of their members'
   final weights (excluding nulls and zeros). The joint levels **sum** instead
   -- see [`Agg`][processing.weighting.core.hierarchy.Agg].
"""

import logging

import polars as pl

from pipeline.decoration import Tables, step
from processing.usability import usable_col_for
from processing.weighting.core.hierarchy import (
    LEVELS,
    Flow,
    describe_weight_columns,
)
from processing.weighting.core.propagation import (
    collect_tables,
    drop_unsuffixed_weights,
    non_null_tables,
    propagate_weights,
)
from processing.weighting.core.specs import resolve_fitted_profiles
from processing.weighting.existing.config import (
    ExistingWeightConfig,
    ExistingWeightsConfig,
    attach_supplied,
)
from processing.weighting.existing.population import (
    POPULATION_TABLES,
    apply_usability,
    follow_parent,
    gate_households_by_size,
    persons_follow_household,
)
from processing.weighting.existing.travel import (
    JOINT_TABLES,
    TRAVEL_TABLES,
    check_usable_were_supplied,
    check_weight_factors,
    gate_travel,
    trips_follow_their_day,
    warn_null_weights,
)
from processing.weighting.validation.weight_checks import (
    _check_hierarchy,
    check_days_carry_their_person,
    check_persons_carry_their_household,
    check_weight_placement,
)

logger = logging.getLogger(__name__)


def weight_columns(existing: ExistingWeightsConfig) -> dict[str, dict]:
    """The suffixed weight columns `add_existing_weights` writes, described, per table."""
    return describe_weight_columns(resolve_fitted_profiles(existing.weight_profiles))


@step(
    # A weight goes on each table present; none has to be.
    optional={
        "households": {"hh_id", "usable_<profile>"},
        "persons": {"person_id", "hh_id"},
        "person_days": {"person_day_id", "person_id"},
        "unlinked_trips": {"unlinked_trip_id", "person_day_id", "linked_trip_id"},
        # The trip and tour joint ids are read only to weight the joint tables.
        "linked_trips": {"linked_trip_id", "tour_id"},
        "tours": {"tour_id"},
        "joint_trips": {"joint_trip_id"},
        "joint_tours": {"joint_tour_id"},
    },
    produces={
        "households": {"hh_weight_<profile>"},
        "persons": {"person_weight_<profile>"},
        "person_days": {"person_day_weight_<profile>"},
        "unlinked_trips": {"unlinked_trip_weight_<profile>"},
        "linked_trips": {"linked_trip_weight_<profile>"},
        "tours": {"tour_weight_<profile>"},
        "joint_trips": {"joint_trip_weight_<profile>"},
        "joint_tours": {"joint_tour_weight_<profile>"},
    },
    generated_columns=weight_columns,
)
def add_existing_weights(
    tables: Tables,
    weights: dict[str, dict],
    existing: ExistingWeightsConfig,
) -> dict[str, pl.DataFrame]:
    """Attach existing weights to the data.

    Loads weights from provided files and attaches them to the data.

    For any tables that exist, and do not have weights provided, we can optionally
    derive missing weights from the next logical upstream table.

    For example, if only household weights exist, persons share their household's
    weight and each person's days split that person's weight. If trip (unlinked)
    weights exist but not linked trips or tours, those are the mean of their trips.

    # Weight hierarchy logic

        hh_weight
          └─ person_weight        (carry forward via hh_id)
              └─ person_day_weight        (split: person_weight / n_usable_days)
                  └─ unlinked_trip_weight  (carry forward via person_day_id)
                      ├─ linked_trip_weight   (mean agg via linked_trip_id)
                      │   └─ joint_trip_weight (SUM agg via joint_trip_id)
                      └─ tour_weight          (mean agg via tour_id)
                          └─ joint_tour_weight (SUM agg via joint_tour_id)

    The shape is declared once in
    [`HIERARCHY`][processing.weighting.core.hierarchy.HIERARCHY]; this
    step only supplies the weights it is given and derives the rest from it.

    Supplied weights move household -> person -> day -> trip, each level only
    within the level above it; the rules, the arithmetic and worked examples
    are on the Existing Weights docs page. In short: households scale by one
    factor per household size; persons copy their household, a dropped
    person's share staying inside it; a person's days carry exactly that
    person's weight; a trip is scaled by its day's factor; linked trips and
    tours are the mean of their final trips.

    Checked before the step returns, per profile, raising on failure:

    - the household total equals the supplied total;
    - ``hh_weight x num_persons == sum(person_weight)`` for every household;
    - each person's days sum to that person's weight, when both are supplied;
    - each derived level against the identity it was derived by;
    - no record grew past its ``max_weight_factor``;
    - no dropped record carries weight, and the formatter's own record
      selection keeps every weight.

    The person total is logged against the supplied total rather than held:
    size groups hold it exactly, except where the top group joins sizes. Not
    held, by design: the trip, linked trip and tour totals.

    Args:
        weights: A dict mapping config keys to weight file settings.
            Each entry specifies a weight CSV to load.  Supported config
            keys: ``hh_weight``, ``person_weight``,
            ``person_day_weight``, ``unlinked_trip_weight``,
            ``linked_trip_weight``, ``joint_trip_weight``,
            ``tour_weight``, ``joint_tour_weight``.

            Config options per entry:

            - ``weight_path``: Path to CSV file containing weights (required).
            - ``weight_id_col``: ID column name in the weight file
              (optional, defaults to canonical table ID).
            - ``weight_col``: Weight column name in the weight file
              (optional, defaults to canonical weight column).

        existing: How supplied weights move onto usable records, per
            profile. See ``ExistingWeightsConfig``.
        tables: Any of the canonical tables in the contract; each one present
            gets weights.

    Returns:
        Dict of tables with attached weights.

    # Example config

    ```yaml
        - name: add_existing_weights
          params:
            weight_profiles: [ctramp]
            preserve_travel_totals: true
            household_size_top_group: 6
            derive_missing_weights: true
            weights:
              hh_weight:
                weight_path: "weights/hh_weights.csv"
                # defaults: id_col='hh_id', weight_col='hh_weight'
              person_weight:
                weight_path: "weights/person_weights.csv"
              unlinked_trip_weight:
                weight_path: "weights/trip_weights.csv"
                weight_id_col: "trip_id"
                weight_col: "trip_weight"
    ```
    """
    tables = collect_tables(**tables)
    profiles = resolve_fitted_profiles(existing.weight_profiles)
    configs, supplied = _read_supplied(tables, weights)

    # A limit compares a record's final weight to its supplied one, so it needs one.
    limits = existing.max_weight_factor or {}
    for table, limit in limits.items():
        if table not in supplied:
            msg = (
                f"max_weight_factor names {table}, which has no supplied weight to compare "
                f"a final weight against. Tables with one: {sorted(supplied)}."
            )
            raise ValueError(msg)
        if limit <= 0:
            msg = f"max_weight_factor for {table} must be positive, got {limit}"
            raise ValueError(msg)

    for profile in profiles:
        _redistribute(tables, supplied, configs, existing, profile, limits=limits)

    drop_unsuffixed_weights(tables)

    logger.info(
        "Weight attachment complete for %s. Tables with weights: %s",
        ", ".join(profiles),
        sorted(supplied),
    )

    return non_null_tables(tables)


def _read_supplied(
    tables: dict[str, pl.DataFrame | None],
    weights: dict[str, dict],
) -> tuple[dict[str, ExistingWeightConfig], dict[str, pl.DataFrame]]:
    """Read each supplied weight file once, under the canonical column names.

    The names are made canonical here so what was read is uniform whatever the
    file called them; `attach_supplied` renames the weight to the column each
    profile writes.

    Returns:
        The validated config per key, and table -> its supplied
        ``(id, weight)`` frame for each table present in the data.
    """
    configs = {key: ExistingWeightConfig(config_key=key, **value) for key, value in weights.items()}
    supplied: dict[str, pl.DataFrame] = {}
    for cfg in configs.values():
        table_name = cfg.table_name
        if tables.get(table_name) is None:
            logger.warning("Weight file provided for %s but table not found in data", table_name)
            continue

        logger.info("Loading weights from %s for %s", cfg.weight_path, table_name)
        weight_df = pl.read_csv(cfg.weight_path)
        cfg.validate_columns(weight_df)
        weight_df = weight_df.select(
            pl.col(cfg.weight_id_col).alias(cfg.table_id_col),  # type: ignore[arg-type]
            pl.col(cfg.weight_col).alias(cfg.canonical_weight_col),  # type: ignore[arg-type]
        )
        logger.info("Loaded %d %s weights from %s", weight_df.height, table_name, cfg.weight_path)
        supplied[table_name] = weight_df
    return configs, supplied


def _redistribute(
    tables: dict[str, pl.DataFrame | None],
    supplied: dict[str, pl.DataFrame],
    configs: dict[str, ExistingWeightConfig],
    existing: ExistingWeightsConfig,
    profile: str,
    *,
    limits: dict[str, float],
) -> None:
    """Write one profile's weight columns from the supplied weights, in place.

    Each profile starts from the supplied numbers as read, not from the previous
    profile's redistributed ones, so every column set holds the same supplied
    totals.

    Usability is stamped upstream by ``cascade_usability``. Populations come
    first (`_place_population`), then travel, then the joint tables. Each level is
    then checked against the rule that set it: persons against their household,
    supplied days against their person, derived levels against the identity they
    were derived by.
    """
    flag = usable_col_for(profile)
    # Every configured table, present or not: none of them is derived.
    provided = {cfg.table_name for cfg in configs.values()}

    has_weight = attach_supplied(tables, supplied, configs, profile)
    check_usable_were_supplied(tables, has_weight, usability_flag_col=flag)

    supplied_totals, by_size, days_follow = _place_population(
        tables, has_weight, flag, existing.household_size_top_group
    )

    if existing.derive_missing_weights:
        propagate_weights(
            tables,
            has_weight,
            skip=provided | TRAVEL_TABLES | JOINT_TABLES,
            usability_flag_col=flag,
            profile=profile,
        )

    if {"unlinked_trips", "person_days"} <= supplied.keys():
        # Unlinked trips move with their day. Their total is not held: a dropped
        # day's weight lands on the person's other days, whose trip counts differ.
        trips_follow_their_day(
            tables,
            has_weight,
            supplied["person_days"],
            usability_flag_col=flag,
            preserve_travel_totals=existing.preserve_travel_totals,
        )
        if existing.derive_missing_weights:
            # Linked trips and tours: the mean of their final trips, so each
            # agrees with its own trips.
            propagate_weights(
                tables,
                has_weight,
                skip=provided | POPULATION_TABLES | JOINT_TABLES,
                usability_flag_col=flag,
                profile=profile,
            )
        # A linked trip or tour weight supplied as a file is gated on its own.
        to_gate = {
            t: c
            for t, c in has_weight.items()
            if t in supplied.keys() & TRAVEL_TABLES - {"unlinked_trips"}
        }
    else:
        if existing.derive_missing_weights:
            # Travel claims: derived from the gated days, but not gated themselves.
            propagate_weights(
                tables,
                has_weight,
                skip=provided | POPULATION_TABLES | JOINT_TABLES,
                usability_flag_col=None,
                profile=profile,
            )
        to_gate = has_weight

    supplied_totals |= gate_travel(
        tables,
        to_gate,
        usability_flag_col=flag,
        preserve_travel_totals=existing.preserve_travel_totals,
    )

    if existing.derive_missing_weights:
        # Joint tables last: the sum of the members that kept their weight.
        propagate_weights(
            tables,
            has_weight,
            skip=provided | POPULATION_TABLES | TRAVEL_TABLES,
            usability_flag_col=flag,
            profile=profile,
        )

    warn_null_weights(tables, profile)
    if by_size and "persons" in has_weight:
        check_persons_carry_their_household(non_null_tables(tables), profile)
    if days_follow:
        check_days_carry_their_person(non_null_tables(tables), profile)
    derived = {t for t in has_weight if t not in supplied and LEVELS[t].flow is Flow.DOWN}
    if derived:
        _check_hierarchy(non_null_tables(tables), flag, profile, only=derived)
    check_weight_factors(tables, has_weight, supplied, limits)
    # Every supplied total still there, and all of it on records the
    # formatter will keep -- or the run stops here, not in the model.
    check_weight_placement(non_null_tables(tables), flag, profile, supplied_totals=supplied_totals)


def _place_population(
    tables: dict[str, pl.DataFrame | None],
    has_weight: dict[str, str],
    flag: str,
    top_group: int,
) -> tuple[dict[str, float], bool, bool]:
    """Move the supplied population weights onto usable records, in place.

    Households scale by size; persons copy their household; days take their
    person's weight.

    Returns:
        The supplied totals held, whether households were scaled by size, and
        whether days follow their person.
    """
    population = {t: c for t, c in has_weight.items() if t in POPULATION_TABLES}
    supplied_totals: dict[str, float] = {}
    households_df = tables.get("households")
    by_size = (
        "households" in population and households_df is not None and flag in households_df.columns
    )
    days_follow = "person_days" in population and "persons" in population
    if by_size:
        population.pop("households")
        supplied_totals["households"] = gate_households_by_size(
            tables,
            has_weight,
            usability_flag_col=flag,
            top_group=top_group,
        )
        if "persons" in population:
            population.pop("persons")
            persons_follow_household(tables, has_weight, usability_flag_col=flag)
    if days_follow:
        # Held person by person below, which holds the day total to the
        # person total rather than to the supplier's day total.
        population.pop("person_days")
        if "persons" in population:
            # Persons supplied without households: the older rule, within the
            # household then region-wide.
            supplied_totals |= apply_usability(
                tables, {"persons": population.pop("persons")}, usability_flag_col=flag
            )
    supplied_totals |= apply_usability(tables, population, usability_flag_col=flag)
    if days_follow:
        follow_parent(tables, has_weight, "person_days", usability_flag_col=flag)
    return supplied_totals, by_size, days_follow
