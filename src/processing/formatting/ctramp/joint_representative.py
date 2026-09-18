"""Pick one member to stand for a joint group's location.

CT-RAMP writes a joint tour and its joint trips as single rows, so each needs
one origin and one destination. The canonical tables deliberately do not supply
them: members of a joint trip were in the same place, and collapsing their
coordinates means choosing a rule, which belongs to the consumer rather than the
survey.

The rule here is to use a **member's own reported location** rather than an
average of them. An average names a point none of them visited, which then has
to be placed in a zone system from scratch -- and near a boundary it can land on
the wrong side, or (where zones do not tile the world) nowhere at all. A member's
location is already zoned, in every zone system at once, and is somewhere a
person actually went.

Which member is settled once per **joint tour**, not per leg. Choosing per leg
lets a household take its outbound leg from one member and its inbound from
another; where those two reported home a few metres apart across a boundary, the
tour then departs one zone and returns to a different one -- a tour that never
gets home. One member for the whole tour cannot produce that.

This lives under the CT-RAMP formatter because it is one consumer's convention,
not a fact about the survey: another output may reasonably collapse a group a
different way. Should a second consumer want the same rule, move the selection
to a shared location rather than importing it from here -- and design it for
that at the time, since the part worth sharing is the choice of member (the
medoid, with its tie-break), not the columns each format happens to need.
"""

import logging

import polars as pl

logger = logging.getLogger(__name__)

COORD_COLUMNS = ("o_lat", "o_lon", "d_lat", "d_lon")


def _offset_from_centre(end: str) -> pl.Expr:
    """How far a member's trip end sits from its own leg's centre, in degrees.

    Longitude is scaled by the cosine of the latitude so the two axes are
    comparable. The unit does not matter: this only ranks the members of one leg
    against each other.
    """
    lat, lon = f"{end}_lat", f"{end}_lon"
    centre_lat, centre_lon = f"_c_{end}_lat", f"_c_{end}_lon"
    return (
        (pl.col(lat) - pl.col(centre_lat)).pow(2)
        + ((pl.col(lon) - pl.col(centre_lon)) * pl.col(centre_lat).radians().cos()).pow(2)
    ).sqrt()


def select_representative_members(joint_members: pl.DataFrame) -> pl.DataFrame:
    """Reduce each joint trip to the member trip that stands for it.

    The representative is the member sitting closest to the middle of the group,
    totalled over every leg of the joint tour and over both ends of each leg, so
    that one person supplies the whole itinerary. Two members are always
    equidistant from their own midpoint, so a pair -- the majority of joint
    trips -- is settled entirely by the tie-break, ``person_id``. Members of a
    joint tour share a household and ``person_id`` embeds the person number, so
    that is the lowest person number in the group.

    The member's whole row is returned rather than just its coordinates, so the
    caller can take the zone that was assigned to that real location instead of
    re-deriving one.

    Args:
        joint_members: Member linked trips carrying ``joint_trip_id``,
            ``joint_tour_id``, ``person_id`` and both ends' coordinates.

    Returns:
        One row per ``joint_trip_id``: the representative member's trip.
    """
    if joint_members.is_empty():
        return joint_members

    centres = joint_members.group_by("joint_trip_id").agg(
        [pl.col(c).mean().alias(f"_c_{c}") for c in COORD_COLUMNS]
    )
    offsets = joint_members.join(centres, on="joint_trip_id", how="left").with_columns(
        # Rounded so that a tie is one: two members are equidistant from their own
        # midpoint in exact arithmetic but not in floating point, and unrounded the
        # last bit of noise would decide the pair -- the commonest case -- instead
        # of the tie-break. 1e-9 degrees is a fraction of a millimetre, far below
        # any distance that separates two members for real.
        (_offset_from_centre("o") + _offset_from_centre("d")).round(9).alias("_offset")
    )

    # One representative per tour: sum each member's offset across the legs they
    # travelled, then take the most central. Legs with no joint tour fall back to
    # being their own group, which is the same rule over a single leg.
    group = pl.coalesce(
        pl.col("joint_tour_id").cast(pl.Utf8), pl.col("joint_trip_id").cast(pl.Utf8)
    )
    totals = (
        offsets.with_columns(group.alias("_group"))
        .group_by("_group", "person_id")
        .agg(pl.col("_offset").sum())
    )
    representative = (
        totals.sort(["_offset", "person_id"])
        .unique(subset="_group", keep="first", maintain_order=True)
        .select("_group", "person_id")
    )

    return (
        offsets.with_columns(group.alias("_group"))
        .join(representative, on=["_group", "person_id"], how="semi")
        .unique(subset="joint_trip_id", keep="first", maintain_order=True)
        .drop("_group", "_offset", *[f"_c_{c}" for c in COORD_COLUMNS])
    )


def representative_person_per_tour(joint_members: pl.DataFrame) -> pl.DataFrame:
    """The person standing for each joint tour.

    The joint tour and its joint trips are two views of one outing, so both take
    their location and clock from the same member. Without this the tour file
    would describe whichever member happened to sort first, and could place the
    tour in a different zone from its own outbound trip.

    Args:
        joint_members: Member linked trips, as for
            :func:`select_representative_members`.

    Returns:
        One row per ``joint_tour_id`` with the representative's ``person_id``.
    """
    empty = pl.DataFrame(schema={"joint_tour_id": pl.Int64, "person_id": pl.Int64})
    if joint_members.is_empty() or "joint_tour_id" not in joint_members.columns:
        return empty

    return (
        select_representative_members(joint_members)
        .filter(pl.col("joint_tour_id").is_not_null())
        .select("joint_tour_id", "person_id")
        .unique(subset="joint_tour_id", keep="first", maintain_order=True)
    )


def log_members_spanning_zones(joint_members: pl.DataFrame, taz_field: str) -> None:
    """Report joint trips whose members were not all in the same zone.

    Two members standing a few metres apart across a boundary really were in
    different zones, so this is the survey being accurate rather than a fault to
    repair. It is logged because the joint record reports only its
    representative's zone, and an analyst comparing that against the member trips
    should be able to see how often the two can differ instead of discovering it.
    """
    o_col, d_col = f"o_{taz_field}", f"d_{taz_field}"
    if not {o_col, d_col}.issubset(joint_members.columns) or joint_members.is_empty():
        return

    spans = joint_members.group_by("joint_trip_id").agg(
        pl.col(o_col).n_unique().alias("_n_o"), pl.col(d_col).n_unique().alias("_n_d")
    )
    spanning = spans.filter((pl.col("_n_o") > 1) | (pl.col("_n_d") > 1)).height
    if spanning:
        logger.info(
            "%d of %d joint trips (%.2f%%) have members in more than one zone; each is "
            "written with its representative member's zone",
            spanning,
            spans.height,
            100 * spanning / spans.height,
        )
