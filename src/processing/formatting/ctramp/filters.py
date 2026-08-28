"""Pre-formatting record filters for the CT-RAMP step.

These drop records that must not reach the CT-RAMP output, cascading each drop
through the tour → linked-trip → joint-trip hierarchy so referential integrity is
preserved, and log what was removed:

* [`_drop_invalid_tours`][processing.formatting.ctramp.filters._drop_invalid_tours]:
  removes tours that are not VALID or not COMPLETE (mirrors the DaySim formatter),
  reporting a two-grid drop summary split by already-incomplete vs. newly-unusable
  loss.
* [`_drop_missing_taz`][processing.formatting.ctramp.filters._drop_missing_taz]:
  removes households (and their descendants) without a valid home TAZ, and
  tours/trips whose origin or destination TAZ is missing.
* [`_drop_zero_weight`][processing.formatting.ctramp.filters._drop_zero_weight]:
  removes households with no positive survey weight and their descendants.

They are separated from the orchestrator ([`format_ctramp`]
[processing.formatting.ctramp.format_ctramp.format_ctramp]) so that file reads as
a conductor, matching the one-concern-per-file layout of the other formatters.
"""

import logging

import polars as pl

from data_canon.codebook.tours import TourDataQuality
from processing.completeness import MIN_JOINT_PARTICIPANTS, suggest_usability_columns

from .ctramp_config import CTRAMPConfig

logger = logging.getLogger(__name__)


def _valid_taz(field: str) -> pl.Expr:
    """A TAZ column is valid when present and not the -1 missing sentinel."""
    col = pl.col(field)
    return col.is_not_null() & (col != -1)


def _drop_by_tour_ids(
    tours: pl.DataFrame,
    linked_trips: pl.DataFrame,
    joint_trips: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Cascade a tour filter down to linked trips and joint trips.

    Keeps only linked trips whose ``tour_id`` still exists in ``tours``, then
    keeps only joint trips whose ``joint_trip_id`` still appears in the surviving
    linked trips. This preserves referential integrity after tours are removed.

    Args:
        tours: Tours that survived a filter (defines the surviving tour_ids)
        linked_trips: Linked trips to prune to the surviving tours
        joint_trips: Joint trips to prune to the surviving linked trips

    Returns:
        Tuple of (tours, linked_trips, joint_trips) with integrity maintained
    """
    linked_trips = linked_trips.filter(pl.col("tour_id").is_in(tours["tour_id"].implode()))

    if len(joint_trips) > 0 and "joint_trip_id" in linked_trips.columns:
        valid_joint_trip_ids = linked_trips.filter(pl.col("joint_trip_id").is_not_null())[
            "joint_trip_id"
        ].unique()
        joint_trips = joint_trips.filter(
            pl.col("joint_trip_id").is_in(valid_joint_trip_ids.implode())
        )

    tours, linked_trips, joint_trips = _drop_lone_joint_participants(
        tours, linked_trips, joint_trips
    )
    return tours, linked_trips, joint_trips


def _drop_lone_joint_participants(
    tours: pl.DataFrame,
    linked_trips: pl.DataFrame,
    joint_trips: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Un-joint any joint group left with a single surviving participant.

    Dropping tours can strip a joint tour down to one member, and CT-RAMP's
    ``num_participants >= 2`` makes a group of one meaningless. Rather than
    delete the survivor's travel -- it is still a real tour -- the joint ids are
    nulled so it is emitted as an individual tour instead.

    Args:
        tours: Tours surviving the filter, carrying ``joint_tour_id``
        linked_trips: Linked trips already pruned to the surviving tours
        joint_trips: Joint trips already pruned to the surviving linked trips

    Returns:
        Tuple of (tours, linked_trips, joint_trips) with singleton joint groups
        demoted to individual travel.
    """
    if "joint_tour_id" not in tours.columns:
        return tours, linked_trips, joint_trips

    survivors = tours.filter(pl.col("joint_tour_id").is_not_null())
    if survivors.is_empty():
        return tours, linked_trips, joint_trips

    person_col = "person_id" if "person_id" in survivors.columns else "tour_id"
    lone = (
        survivors.group_by("joint_tour_id")
        .agg(pl.col(person_col).n_unique().alias("_n"))
        .filter(pl.col("_n") < MIN_JOINT_PARTICIPANTS)
        .select("joint_tour_id")
    )
    if lone.is_empty():
        return tours, linked_trips, joint_trips

    lone_ids = lone["joint_tour_id"]
    logger.info(
        "Demoted %d joint tour(s) to individual travel: the tour drop left them "
        "with a single participant",
        lone_ids.len(),
    )

    def _unjoint(df: pl.DataFrame) -> pl.DataFrame:
        if "joint_tour_id" not in df.columns:
            return df
        is_lone = pl.col("joint_tour_id").is_in(lone_ids.implode())
        exprs = [
            pl.when(is_lone).then(None).otherwise(pl.col("joint_tour_id")).alias("joint_tour_id")
        ]
        if "joint_trip_id" in df.columns:
            exprs.append(
                pl.when(is_lone)
                .then(None)
                .otherwise(pl.col("joint_trip_id"))
                .alias("joint_trip_id")
            )
        return df.with_columns(exprs)

    tours, linked_trips = _unjoint(tours), _unjoint(linked_trips)

    # Joint trips that lost their group are no longer joint travel at all.
    if len(joint_trips) > 0 and "joint_trip_id" in linked_trips.columns:
        still_joint = linked_trips.filter(pl.col("joint_trip_id").is_not_null())[
            "joint_trip_id"
        ].unique()
        joint_trips = joint_trips.filter(pl.col("joint_trip_id").is_in(still_joint.implode()))

    return tours, linked_trips, joint_trips


def _completeness_split(df: pl.DataFrame) -> tuple[int, int]:
    """Return (already_incomplete, newly_unusable) counts from the ``complete`` flag.

    ``already_incomplete`` are records flagged incomplete (household / person /
    day cascade) — they were already excluded from the weighted model.
    ``newly_unusable`` are otherwise-complete records now being dropped. When the
    ``complete`` column is absent (e.g. tests), everything is treated as newly
    unusable.
    """
    total = df.height
    if "complete" not in df.columns:
        return 0, total
    incomplete = df.filter(~pl.col("complete").fill_null(value=False)).height
    return incomplete, total - incomplete


def _drop_grid(title: str, df: pl.DataFrame, denom: int) -> list[str]:
    """Build the lines of one drop grid (tours or member trips).

    ``new %`` is the newly-unusable share of all records — the genuinely new loss.
    """
    header = f"  {title:<14}{'dropped':>9}{'incomplete':>13}{'newly unusable':>16}{'new %':>8}"

    def row(name: str, incomplete: int, newly_unusable: int) -> str:
        total = incomplete + newly_unusable
        pct = newly_unusable * 100 / denom if denom > 0 else 0
        return f"  {name:<14}{total:>9,}{incomplete:>13,}{newly_unusable:>16,}{pct:>7.1f}%"

    lines = [header]
    for label in ("invalid", "partial/open"):
        incomplete, newly_unusable = _completeness_split(df.filter(pl.col("_reason") == label))
        if incomplete + newly_unusable == 0:
            continue
        lines.append(row(label, incomplete, newly_unusable))
    incomplete, newly_unusable = _completeness_split(df)
    lines.append(row("total", incomplete, newly_unusable))
    return lines


def _log_drop_summary(dropped: pl.DataFrame, linked_trips: pl.DataFrame, n_og_tours: int) -> None:
    """Log a two-grid table of dropped tours and member trips.

    Splits each count into records that were already incomplete (household /
    person / day flagged incomplete — already out of the weighted model) versus
    newly-unusable removal of otherwise-complete records, so the log makes clear
    how much is genuinely new loss.
    """
    if dropped.height == 0:
        return

    dropped_trips = linked_trips.join(
        dropped.select("tour_id", "_reason"), on="tour_id", how="inner"
    )
    lines = [
        "Dropped tours/member-trips to match DaySim (DaySim already drops these).",
        '  "incomplete" = flagged incomplete via the household>person>day>trip cascade; '
        '"newly unusable" = otherwise-complete records.',
        "",
        *_drop_grid("TOURS", dropped, n_og_tours),
        "",
        *_drop_grid("MEMBER TRIPS", dropped_trips, len(linked_trips)),
    ]
    logger.info("\n".join(lines))


def _drop_invalid_tours(
    tours: pl.DataFrame,
    linked_trips: pl.DataFrame,
    joint_trips: pl.DataFrame,
    usability_flag_col: str,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Remove tours not admissible to the model, cascading to linked and joint trips.

    Keeps only the tours *usability_flag_col* admits. That column is one of the
    usability profiles stamped upstream by the ``cascade_completeness`` step (see
    [`processing.completeness`][processing.completeness]), so this formatter, the
    DaySim formatter and the weighting can be pointed at the same profile and
    agree on the tour universe. Without the filter, single-trip/loop tours (which
    carry a null ``tour_purpose``) survive into CT-RAMP output and are silently
    coerced to the ``OTHDISCR`` catch-all purpose.

    The verdict is read, never re-derived: if the named column is absent this
    raises rather than inventing a criterion of its own, which is how the three
    consumers used to drift apart. The error offers the frame's boolean columns
    as candidates, since a profile's name comes from config and so cannot be
    recognised by its shape.

    Args:
        tours: Canonical tour data carrying *usability_flag_col*
        linked_trips: Canonical linked trip data
        joint_trips: Aggregated joint trip data
        usability_flag_col: Which usability profile decides the tour universe.
            Naming a looser profile admits more, at the cost of no longer
            matching whichever profile the weighting and the DaySim formatter
            were given.

    Returns:
        Tuple of (tours, linked_trips, joint_trips) with inadmissible tours and
        their orphaned trips removed.
    """
    if usability_flag_col not in tours.columns:
        msg = (
            f"Tours carry no '{usability_flag_col}' column, so there is nothing to "
            f"gate on. Declare it in cascade_completeness's usability_profiles, or "
            f"set drop_invalid_tours: false to keep every tour. "
            f"{suggest_usability_columns(tours)}"
        )
        raise ValueError(msg)

    # Name the profile in the log: several may be stamped, and two formatters
    # reading different ones is legitimate but must not be invisible.
    logger.info("CT-RAMP tour universe gated on %s", usability_flag_col)

    has_quality = "tour_data_quality" in tours.columns

    # The gate is the only criterion. A null means cascade_completeness never
    # reached this row, which is a broken frame rather than licence to guess:
    # re-deriving one here would be a second definition of admissibility that
    # cannot know which profile was asked for.
    keep = pl.col(usability_flag_col).fill_null(value=False)

    # Tag each dropped tour's reason so the log can distinguish structurally
    # invalid tours from partial/open ones (valid but not home-based). This
    # only labels what the gate already decided; it never decides anything.
    if has_quality:
        is_valid = pl.col("tour_data_quality") == TourDataQuality.VALID.value
        reason = pl.when(~is_valid).then(pl.lit("invalid")).otherwise(pl.lit("partial/open"))
    else:
        reason = pl.lit("partial/open")

    n_og_tours = len(tours)
    dropped = tours.filter(~keep).with_columns(reason.alias("_reason"))
    _log_drop_summary(dropped, linked_trips, n_og_tours)

    tours = tours.filter(keep)
    tours, linked_trips, joint_trips = _drop_by_tour_ids(tours, linked_trips, joint_trips)
    return tours, linked_trips, joint_trips


def _drop_zero_weight(
    households: pl.DataFrame,
    persons: pl.DataFrame,
    tours: pl.DataFrame,
    linked_trips: pl.DataFrame,
    joint_trips: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Remove records with null or zero household weight and cascade.

    Households with null or zero hh_weight are excluded from CT-RAMP output
    (they have no representation in the travel model). Removal cascades to
    persons, tours, and trips to maintain referential integrity.

    Args:
        households: Canonical household data with hh_weight column
        persons: Canonical person data
        tours: Canonical tour data
        linked_trips: Canonical linked trip data
        joint_trips: Canonical joint trip data

    Returns:
        Tuple of filtered DataFrames maintaining referential integrity
    """
    if "hh_weight" not in households.columns:
        logger.warning("hh_weight column not found; skipping zero-weight filter.")
        return households, persons, tours, linked_trips, joint_trips

    n_before = len(households)
    households = households.filter(pl.col("hh_weight").is_not_null() & (pl.col("hh_weight") > 0))
    n_dropped = n_before - len(households)
    if n_dropped == 0:
        return households, persons, tours, linked_trips, joint_trips

    logger.info(
        "Dropped %d household(s) with null or zero hh_weight (keeping %d)",
        n_dropped,
        len(households),
    )

    valid_hh_ids = households["hh_id"]
    persons = persons.filter(pl.col("hh_id").is_in(valid_hh_ids.implode()))
    if len(tours) > 0:
        tours = tours.filter(pl.col("hh_id").is_in(valid_hh_ids.implode()))
    if len(linked_trips) > 0:
        linked_trips = linked_trips.filter(pl.col("hh_id").is_in(valid_hh_ids.implode()))
    if len(joint_trips) > 0:
        joint_trips = joint_trips.filter(pl.col("hh_id").is_in(valid_hh_ids.implode()))

    logger.info(
        "After zero-weight filter: %d persons, %d tours, %d linked trips, %d joint trips",
        len(persons),
        len(tours),
        len(linked_trips),
        len(joint_trips),
    )

    return households, persons, tours, linked_trips, joint_trips


def _drop_missing_taz(
    households: pl.DataFrame,
    persons: pl.DataFrame,
    tours: pl.DataFrame,
    linked_trips: pl.DataFrame,
    joint_trips: pl.DataFrame,
    config: CTRAMPConfig,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Remove records with missing TAZ fields and ensure referential integrity.

    Performs cascading deletions to maintain data consistency:
    1. Remove households without valid home TAZ
    2. Remove persons from dropped households
    3. Remove tours/trips with missing origin or destination TAZ
    4. Remove tours that have no trips remaining
    5. Remove joint trips that have no linked trips remaining

    Args:
        households: Canonical household data
        persons: Canonical person data
        tours: Canonical tour data
        linked_trips: Canonical linked trip data
        joint_trips: Canonical joint trip data
        config: CTRAMP configuration with taz_field name

    Returns:
        Tuple of filtered DataFrames maintaining referential integrity
    """
    taz = config.taz_field
    o_taz, d_taz = f"o_{taz}", f"d_{taz}"

    # Track original counts for logging
    counts = {
        "households": len(households),
        "persons": len(persons),
        "tours": max(0, len(tours)),
        "linked_trips": max(0, len(linked_trips)),
        "joint_trips": max(0, len(joint_trips)),
    }

    # Step 1: Filter households by home TAZ
    households = households.filter(_valid_taz(f"home_{taz}"))
    valid_hh_ids = households["hh_id"]

    # Step 2: Remove orphaned persons
    persons = persons.filter(pl.col("hh_id").is_in(valid_hh_ids.implode()))

    logger.info(
        "Dropped %d households without valid home TAZ (keeping %d households, %d persons)",
        counts["households"] - len(households),
        len(households),
        len(persons),
    )

    # Step 3: Filter tours by household and TAZ fields
    if len(tours) > 0:
        tours = tours.filter(
            pl.col("hh_id").is_in(valid_hh_ids.implode()) & _valid_taz(o_taz) & _valid_taz(d_taz)
        )
        valid_tour_ids = tours["tour_id"]
    else:
        valid_tour_ids = pl.Series("tour_id", [], dtype=pl.Int64)

    # Step 4: Filter linked trips by tour and TAZ fields
    if len(linked_trips) > 0:
        linked_trips = linked_trips.filter(
            pl.col("tour_id").is_in(valid_tour_ids.implode())
            & _valid_taz(o_taz)
            & _valid_taz(d_taz)
        )
        # Get tours that still have trips
        tours_with_trips = linked_trips["tour_id"].unique()

        # Remove tours that lost all their trips
        if len(tours) > 0:
            tours_before = len(tours)
            tours = tours.filter(pl.col("tour_id").is_in(tours_with_trips.implode()))
            if tours_before != len(tours):
                logger.info(
                    "Removed %d tours that had no valid trips remaining",
                    tours_before - len(tours),
                )
    # No trips means no tours should remain
    elif len(tours) > 0:
        logger.info("Removed all %d tours because no valid trips exist", len(tours))
        tours = tours.head(0)

    # Step 5: Filter joint trips by household and by their surviving members.
    # A joint trip carries no location of its own -- it is a linking table -- so
    # it is admissible exactly while its member trips are, which the TAZ filter
    # above has already decided.
    if len(joint_trips) > 0:
        # First filter by household
        joint_trips = joint_trips.filter(pl.col("hh_id").is_in(valid_hh_ids))

        # Keep only joint trips that have corresponding linked trips
        if len(linked_trips) > 0 and "joint_trip_id" in linked_trips.columns:
            valid_joint_trip_ids = linked_trips.filter(pl.col("joint_trip_id").is_not_null())[
                "joint_trip_id"
            ].unique()
            joint_trips = joint_trips.filter(pl.col("joint_trip_id").is_in(valid_joint_trip_ids))
        else:
            # No linked trips with joint_trip_id means no joint trips should remain
            logger.info(
                "Removed all %d joint trips because no valid linked trips exist", len(joint_trips)
            )
            joint_trips = joint_trips.head(0)

    # Final logging
    logger.info(
        "TAZ filtering complete:\n"
        "  Tours: %d → %d (dropped %d)\n"
        "  Linked trips: %d → %d (dropped %d)\n"
        "  Joint trips: %d → %d (dropped %d)",
        counts["tours"],
        len(tours),
        counts["tours"] - len(tours),
        counts["linked_trips"],
        len(linked_trips),
        counts["linked_trips"] - len(linked_trips),
        counts["joint_trips"],
        len(joint_trips),
        counts["joint_trips"] - len(joint_trips),
    )

    return households, persons, tours, linked_trips, joint_trips
