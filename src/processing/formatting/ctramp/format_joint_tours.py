"""Format canonical joint tours for CT-RAMP."""

import logging
from collections.abc import Collection

import polars as pl

from data_canon.codebook.ctramp import (
    CTRAMPTourCategory,
    TourComposition,
)
from data_canon.codebook.persons import SchoolType
from data_canon.codebook.tours import TourDirection

from .ctramp_config import CTRAMPConfig
from .joint_representative import representative_person_per_tour
from .mode_mappings import (
    aggregate_transit_submode,
    ctramp_mode_expression,
)
from .purpose_mappings import NO_EMPLOYMENT, ctramp_purpose_category_expression

logger = logging.getLogger(__name__)


def format_joint_tour(
    tours_canonical: pl.DataFrame,
    linked_trips_canonical: pl.DataFrame,
    unlinked_trips_canonical: pl.DataFrame,
    joint_tours_canonical: pl.DataFrame,
    persons_canonical: pl.DataFrame,
    households_ctramp: pl.DataFrame,
    config: CTRAMPConfig,
) -> pl.DataFrame:
    """Format canonical joint tours to CT-RAMP's household-level schema."""
    logger.info("Formatting joint tour data for CT-RAMP")

    if tours_canonical.is_empty():
        logger.info("No tours provided")
        return pl.DataFrame()

    joint_tours = tours_canonical.filter(pl.col("joint_tour_id").is_not_null())
    if joint_tours.is_empty():
        logger.info("No joint tours found")
        return pl.DataFrame()

    joint_tours = joint_tours.join(
        persons_canonical.select(["person_id", "person_num", "age"]),
        on="person_id",
        how="left",
    )
    group_agg = joint_tours.group_by("joint_tour_id").agg(
        [
            pl.col("hh_id").first(),
            pl.col("person_num").sort().cast(pl.Utf8).str.join(" ").alias("Participants"),
            # Member tours behind the weight -- the divisor back to a per-tour rate.
            pl.len().cast(pl.Int64).alias("_n_members"),
        ]
    )

    # Everything tour-shaped comes from the member the joint trip file also uses,
    # so the two describe one member's outing rather than two members' halves.
    participants_agg = group_agg.join(
        _representative_tour(joint_tours, linked_trips_canonical, config),
        on="joint_tour_id",
        how="left",
    )

    joint_tours_formatted = participants_agg.join(
        _composition(joint_tours, config.age_adult), on="joint_tour_id", how="left"
    )

    # Income only: a joint record's weight and rate come from its own columns, never
    # from hh_weight, which the cascade has since split and redistributed.
    joint_tours_formatted = joint_tours_formatted.join(
        households_ctramp.select(["hh_id", "income"]),
        on="hh_id",
        how="left",
    )

    if not linked_trips_canonical.is_empty():
        joint_trips = linked_trips_canonical.filter(pl.col("joint_tour_id").is_not_null()).sort(
            "joint_tour_id", "depart_time", "arrive_time"
        )
        party = joint_trips.group_by("joint_tour_id").agg(
            pl.col("person_id").n_unique().alias("num_travelers"),
            pl.col("access_mode").drop_nulls().first().alias("tour_access_mode"),
            pl.col("egress_mode").drop_nulls().first().alias("tour_egress_mode"),
        )
        # Stops are one member's: every member makes the same trips, so counting
        # them all multiplies each leg by the party size.
        stops = (
            joint_trips.join(
                joint_tours_formatted.select("joint_tour_id", "person_id"),
                on=["joint_tour_id", "person_id"],
                how="semi",
            )
            .group_by("joint_tour_id")
            .agg(
                _stops(TourDirection.OUTBOUND).alias("num_ob_stops"),
                _stops(TourDirection.INBOUND).alias("num_ib_stops"),
            )
        )
        joint_tours_formatted = (
            joint_tours_formatted.join(party, on="joint_tour_id", how="left")
            .join(stops, on="joint_tour_id", how="left")
            .with_columns(pl.col("num_ob_stops", "num_ib_stops").fill_null(0))
        )
    else:
        joint_tours_formatted = joint_tours_formatted.with_columns(
            pl.lit(None).cast(pl.Int64).alias("num_travelers"),
            pl.lit(0).alias("num_ob_stops"),
            pl.lit(0).alias("num_ib_stops"),
            pl.lit(None).cast(pl.Int64).alias("tour_access_mode"),
            pl.lit(None).cast(pl.Int64).alias("tour_egress_mode"),
        )

    if not unlinked_trips_canonical.is_empty():
        submode_by_joint_tour = aggregate_transit_submode(unlinked_trips_canonical, "joint_tour_id")
        joint_tours_formatted = joint_tours_formatted.join(
            submode_by_joint_tour, on="joint_tour_id", how="left"
        )
        joint_submode_expr = pl.col("transit_submode")
        tnc_type = pl.col("tnc_type")
    else:
        joint_submode_expr = None
        tnc_type = None

    joint_tours_formatted = joint_tours_formatted.with_columns(
        [
            ctramp_purpose_category_expression(
                pl.col("tour_purpose"),
                pl.col("income"),
                pl.lit(SchoolType.MISSING.value),
                NO_EMPLOYMENT,
                config,
            ).alias("tour_purpose_ctramp"),
            ctramp_mode_expression(
                pl.col("tour_mode"),
                pl.col("num_travelers"),
                pl.col("tour_access_mode"),
                pl.col("tour_egress_mode"),
                joint_submode_expr,
                tnc_type,
            ).alias("tour_mode_ctramp"),
        ]
    ).with_columns(
        [
            pl.col("origin_depart_time").dt.hour().alias("start_hour"),
            pl.col("origin_arrive_time").dt.hour().alias("end_hour"),
            pl.lit(CTRAMPTourCategory.JOINT_NON_MANDATORY.value).alias("tour_category"),
        ]
    )

    _validate_joint_tours_have_trips(joint_tours_formatted, linked_trips_canonical)
    _validate_joint_tours_are_wholly_joint(linked_trips_canonical)

    joint_tours_formatted = joint_tours_formatted.join(
        ctramp_joint_tour_numbers(tours_canonical), on="joint_tour_id", how="left"
    )

    weight_cols: list[pl.Expr] = []
    if "joint_tour_weight" in joint_tours_canonical.columns:
        joint_tours_formatted = joint_tours_formatted.join(
            joint_tours_canonical.select("joint_tour_id", "joint_tour_weight"),
            on="joint_tour_id",
            how="left",
        )
        weight_cols = joint_weight_columns(
            joint_tours_formatted.columns, "joint_tour_weight", pl.col("_n_members")
        )

    return joint_tours_formatted.select(
        [
            pl.col("hh_id"),
            # CT-RAMP calls the household-scoped joint-tour number tour_id; renamed
            # only here so it is never confused with the canonical joint_tour_id.
            pl.col("_ctramp_joint_tour_id").alias("tour_id"),
            pl.col("tour_category"),
            pl.col("tour_purpose_ctramp").alias("tour_purpose"),
            pl.col("Composition").alias("tour_composition"),
            pl.col("Participants").alias("tour_participants"),
            pl.col(f"o_{config.taz_field}").cast(pl.Int64).alias("orig_taz"),
            pl.col(f"d_{config.taz_field}").cast(pl.Int64).alias("dest_taz"),
            pl.col("start_hour").cast(pl.Int64),
            pl.col("end_hour").cast(pl.Int64),
            pl.col("tour_mode_ctramp").alias("tour_mode"),
            pl.col("num_ob_stops").cast(pl.Int64),
            pl.col("num_ib_stops").cast(pl.Int64),
            *weight_cols,
        ]
    )


def _stops(direction: TourDirection) -> pl.Expr:
    """Stops on one half of a tour: its trips in *direction* less the one reaching the end."""
    return ((pl.col("tour_direction") == direction.value).sum() - 1).clip(lower_bound=0)


def _composition(joint_tours: pl.DataFrame, age_adult: int) -> pl.DataFrame:
    """Each joint tour's party: adults only, children only, or both.

    Args:
        joint_tours: Member tours carrying each member's ``age`` category.
        age_adult: The first ``AgeCategory`` code counted as an adult.
    """
    return (
        joint_tours.group_by("joint_tour_id")
        .agg(
            (pl.col("age") < age_adult).any().alias("has_children"),
            (pl.col("age") >= age_adult).any().alias("has_adults"),
        )
        .select(
            "joint_tour_id",
            pl.when(pl.col("has_children") & pl.col("has_adults"))
            .then(pl.lit(TourComposition.ADULTS_AND_CHILDREN.value))
            .when(pl.col("has_children"))
            .then(pl.lit(TourComposition.CHILDREN_ONLY.value))
            .otherwise(pl.lit(TourComposition.ADULTS_ONLY.value))
            .alias("Composition"),
        )
    )


def _representative_tour(
    joint_tours: pl.DataFrame, linked_trips_canonical: pl.DataFrame, config: CTRAMPConfig
) -> pl.DataFrame:
    """Reduce each joint tour's member tours to the representative member's, with their person_id.

    Falls back to the lowest person number when the member trips needed to choose
    a representative are absent, so a caller passing tours alone still gets one
    member's tour rather than whichever row happened to sort first.
    """
    columns = [
        "tour_category",
        "tour_purpose",
        f"o_{config.taz_field}",
        f"d_{config.taz_field}",
        "origin_depart_time",
        "origin_arrive_time",
        "tour_mode",
        "subtour_num",
    ]

    joint_members = (
        linked_trips_canonical.filter(
            pl.col("joint_trip_id").is_not_null() & pl.col("joint_tour_id").is_not_null()
        )
        if not linked_trips_canonical.is_empty()
        and {"joint_trip_id", "joint_tour_id"}.issubset(linked_trips_canonical.columns)
        else linked_trips_canonical.clear()
    )

    chosen = representative_person_per_tour(joint_members)
    if chosen.is_empty():
        return (
            joint_tours.sort(["joint_tour_id", "person_id"])
            .unique(subset="joint_tour_id", keep="first", maintain_order=True)
            .select(["joint_tour_id", "person_id", *columns])
        )

    representative = joint_tours.join(chosen, on=["joint_tour_id", "person_id"], how="semi")
    # Any joint tour whose representative is missing here keeps a member anyway.
    remainder = joint_tours.join(representative, on="joint_tour_id", how="anti")
    return (
        pl.concat([representative, remainder])
        .sort(["joint_tour_id", "person_id"])
        .unique(subset="joint_tour_id", keep="first", maintain_order=True)
        .select(["joint_tour_id", "person_id", *columns])
    )


def ctramp_joint_tour_numbers(tours_canonical: pl.DataFrame) -> pl.DataFrame:
    """Number each household's joint tours from zero, as CT-RAMP identifies them.

    Both joint files carry this number, so it is derived once from the tours --
    the only table holding every admitted joint tour. Ranking it separately over
    each file's own rows agrees only while the two files cover the same joint
    tours; let one lose a tour and the rest silently shift down onto their
    neighbours' numbers, which no referential check can see because the number
    landed on does exist.

    Args:
        tours_canonical: Canonical tours.

    Returns:
        One row per joint tour: ``joint_tour_id`` and its 0-based
        ``_ctramp_joint_tour_id`` within the household.
    """
    return (
        tours_canonical.filter(pl.col("joint_tour_id").is_not_null())
        .select("hh_id", "joint_tour_id")
        .unique()
        .with_columns(
            (pl.col("joint_tour_id").rank("dense").over("hh_id") - 1)
            .cast(pl.Int64)
            .alias("_ctramp_joint_tour_id")
        )
        .select("joint_tour_id", "_ctramp_joint_tour_id")
    )


def joint_weight_columns(
    available: Collection[str], weight_col: str, member_count: pl.Expr
) -> list[pl.Expr]:
    """Carry a joint weight through, expressed as a per-member sample rate.

    A joint weight is the SUM over its members, so its inverse is *not* a sample
    rate: CT-RAMP re-applies the party multiplier itself
    (``num_participants/sampleRate``), and inverting the sum would count the party
    twice. The per-member rate is the inverse of the member *mean*, i.e.
    ``n_members / weight``.

    The count comes from the member table the caller already grouped, not from the
    joint record. It matches what the weighting summed over because both read the
    same usability profile: the cascade voids the joint ids of any grouping the
    profile rejects and dissolves any left with fewer than two members, and
    ``keep_usable`` keeps exactly the records the profile admits.

    Args:
        available: Columns present on the frame the expressions will run against.
        weight_col: The joint weight column to carry through.
        member_count: Expression giving the number of members behind the weight.

    Returns:
        The weight and ``sampleRate`` expressions, or empty when there is no
        weight to carry.
    """
    if weight_col not in available:
        return []
    return [
        pl.col(weight_col),
        pl.when(pl.col(weight_col) > 0)
        .then(member_count / pl.col(weight_col))
        .otherwise(None)
        .alias("sampleRate"),
    ]


def _validate_joint_tours_are_wholly_joint(linked_trips: pl.DataFrame) -> None:
    """Raise when a joint tour has a leg that is not itself a joint trip.

    CT-RAMP has no way to express a half-shared tour: a tour is joint or it is
    not. The survey does -- a parent who drops a child and drives on to work
    made one tour, part of it together -- so this is a constraint of the output
    rather than of the data, and is enforced here rather than upstream.

    A profile declaring ``joint_tours: fully_joint`` keeps no such tour, so this
    fires only when the config points CT-RAMP at a ``partial`` profile. Failing
    loudly beats the alternative: the individual trip file excludes joint tours
    and the joint trip file needs a ``joint_trip_id``, so the unshared leg would
    reach neither.

    Raises:
        ValueError: If a trip on a joint tour carries no ``joint_trip_id``.
    """
    required = {"joint_tour_id", "joint_trip_id"}
    if linked_trips.is_empty() or not required.issubset(linked_trips.columns):
        return
    # A frame carrying no joint trip at all is not asserting that these tours are
    # partly shared -- it simply never recorded trip-level sharing. Judging it
    # would fail every caller that supplies tours without the trips behind them.
    if linked_trips["joint_trip_id"].null_count() == len(linked_trips):
        return

    stray = linked_trips.filter(
        pl.col("joint_tour_id").is_not_null() & pl.col("joint_trip_id").is_null()
    )
    if stray.is_empty():
        return

    tour_ids = stray["joint_tour_id"].unique().to_list()
    preview = tour_ids[:10]
    msg = (
        f"{len(stray)} trip(s) across {len(tour_ids)} joint tour(s) are not joint trips. "
        f"CT-RAMP cannot represent a partly-shared tour: point it at a usability profile "
        f"declaring joint_tours: fully_joint, under which the cascade keeps no such tour. "
        f"Joint tour IDs: {preview}"
        f"{'...' if len(tour_ids) > len(preview) else ''}"
    )
    raise ValueError(msg)


def _validate_joint_tours_have_trips(joint_tours: pl.DataFrame, linked_trips: pl.DataFrame) -> None:
    """Raise when a joint tour has no corresponding linked trip."""
    if linked_trips.is_empty():
        return
    joint_tours_with_trips = (
        linked_trips.filter(pl.col("joint_tour_id").is_not_null()).select("joint_tour_id").unique()
    )
    if joint_tours_with_trips.is_empty():
        return
    zero_trip_tours = joint_tours.filter(
        ~pl.col("joint_tour_id").is_in(joint_tours_with_trips["joint_tour_id"].implode())
    )
    if zero_trip_tours.is_empty():
        return
    tour_ids = zero_trip_tours["joint_tour_id"].to_list()
    msg = (
        f"Found {len(zero_trip_tours)} joint tours with zero trips. "
        f"Joint tour IDs: {tour_ids[:10]}"
        f"{'...' if len(tour_ids) > 10 else ''}"  # noqa: PLR2004
    )
    raise ValueError(msg)
