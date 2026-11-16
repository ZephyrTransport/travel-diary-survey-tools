"""Which tours and trips are on a joint tour or joint trip, per profile.

``joint_tour_id`` and ``joint_trip_id`` record the groups the survey detected,
once, for every profile. Whether a record is *on* its group is a profile's
call, so each profile gets its own copy of the id -- ``joint_tour_id_<profile>``
on tours and both trip tables, ``joint_trip_id_<profile>`` on linked trips --
holding the same value where the record is on it and null where it is not.
Nothing here edits the bare id.

Runs after the up and down walks in
[`usable`][processing.usability.usable], since it reads
every table's verdict. The functions below are in the order they run.
"""

import polars as pl

from data_canon.codebook.trips import WORK_PURPOSE_CATEGORIES, PurposeCategory

from .profiles import UsabilityProfile
from .reduce import and_reduced

# A joint entity is only joint while two of its members survive.
MIN_JOINT_PARTICIPANTS = 2

# Joint groupings and the member table they are formed from:
# (joint_table, member_table, shared key). Tours first: rejecting a joint tour
# can empty a joint trip, never the other way round.
_JOINT_GROUPINGS = (
    ("joint_tours", "tours", "joint_tour_id"),
    ("joint_trips", "linked_trips", "joint_trip_id"),
)

# Tables that carry their tour's joint_tour_id, and so its answer.
_TOUR_MEMBER_TABLES = ("linked_trips", "unlinked_trips")

# Purposes a model never draws jointly, so a tour with one is never on a joint tour.
# Work includes work-related: a model treats a meeting or jobsite as work.
_NEVER_JOINT_PURPOSES = (
    *(p.value for p in WORK_PURPOSE_CATEGORIES),
    PurposeCategory.SCHOOL.value,
    PurposeCategory.ESCORT.value,
)


def flag_joint(tables: dict[str, pl.DataFrame | None], profile: UsabilityProfile) -> None:
    """Decide the joint tours and joint trips for *profile*, in place.

    First which tour is on which joint tour, and which trip on which joint
    trip. Then the groupings are judged and the ids of any rejected grouping
    voided -- twice, in the only order the chain runs: rejecting a joint tour
    takes its tours' trips off their joint trips, which can empty a joint
    trip, so joint trips are judged once more. Nothing flows back up, which
    the final check states rather than assumes.

    Raises:
        RuntimeError: If the second void changed anything, meaning a rule now
            flows from joint trips back to joint tours.
    """
    _flag_joint_ids(tables, profile)

    _flag_joint_groupings(tables, profile)
    _void_ids_of_rejected_groupings(tables, profile)

    _flag_joint_groupings(tables, profile)
    if _void_ids_of_rejected_groupings(tables, profile):
        msg = (
            "Rejecting a joint trip changed a joint tour, but the chain only runs "
            "from joint tours to joint trips; a rule flowing the other way was added."
        )
        raise RuntimeError(msg)


# -- Which record is on which grouping -----------------------------------------


def _flag_joint_ids(
    tables: dict[str, pl.DataFrame | None],
    profile: UsabilityProfile,
) -> None:
    """Stamp ``joint_tour_id_<profile>`` and ``joint_trip_id_<profile>``, in place.

    Under ``partial`` a usable tour in a group is on it. Under ``fully_joint``
    the model draws one set of trips for everyone on the tour, so a tour is on
    it only if it is usable, not work, work-related, school or escort, and made the same
    joint trips as the others (see :func:`_fully_joint_tours`). A linked trip
    is on its joint trip only while its tour is on its joint tour, and both
    trip tables carry the tour's answer too.
    """
    tours = tables["tours"]
    tour_col = profile.joint_tour_id
    usable = pl.col(profile.flag).fill_null(value=False)

    if not profile.requires_fully_joint:
        tables["tours"] = tours.with_columns(
            pl.when(usable).then(pl.col("joint_tour_id")).otherwise(None).alias(tour_col)
        )
    else:
        tables["tours"] = _fully_joint_tours(tours, tables["linked_trips"], profile)

    # A joint tour with one tour left on it is nobody's joint tour.
    tables["tours"] = _dissolve_undersized(tables["tours"], tour_col)
    _copy_tour_answer(tables, profile)
    _flag_joint_trip_ids(tables, profile)


def _fully_joint_tours(
    tours: pl.DataFrame,
    trips: pl.DataFrame,
    profile: UsabilityProfile,
) -> pl.DataFrame:
    """Return *tours* with ``joint_tour_id_<profile>`` under the fully-joint rule.

    Tours are matched by their **leg signature**: the joint trips a tour's
    usable legs belong to, in order. Tours that made identical trips have the
    same signature, and that is what the model means by a joint tour. Within a
    survey group, the largest class of identical signatures is the joint tour
    (ties go to the class holding the lowest tour id, so the answer is stable);
    everyone outside it travels alone. A tour with any leg that is not a joint
    trip went somewhere alone and is in no class. A group then needs two tours
    and one purpose, or it is no group and everyone on it travels alone.

    A tour is also never work, work-related, school or escort.
    """
    tour_col = profile.joint_tour_id
    on_tour = pl.col(profile.flag).fill_null(value=False) & pl.col("joint_tour_id").is_not_null()
    if "tour_purpose" in tours.columns:
        on_tour = on_tour & ~pl.col("tour_purpose").is_in(_NEVER_JOINT_PURPOSES)

    signatures = (
        trips.filter(pl.col(profile.flag).fill_null(value=False))
        .group_by("tour_id")
        .agg(
            pl.col("joint_trip_id").is_null().any().alias("_solo_leg"),
            pl.col("joint_trip_id").sort().alias("_signature"),
        )
    )
    classes = (
        tours.filter(on_tour)
        .select("tour_id", "joint_tour_id")
        .join(signatures, on="tour_id", how="inner")
        .filter(~pl.col("_solo_leg"))
        .group_by("joint_tour_id", "_signature")
        .agg(pl.col("tour_id").alias("_tours"), pl.len().alias("_n_tours"))
        .with_columns(pl.col("_tours").list.min().alias("_first"))
        .sort("joint_tour_id", "_n_tours", "_first", descending=[False, True, False])
        .unique("joint_tour_id", keep="first", maintain_order=True)
        .filter(pl.col("_n_tours") >= MIN_JOINT_PARTICIPANTS)
    )
    in_class = classes.select(pl.col("_tours").explode().alias("tour_id"))["tour_id"]
    on_tour = on_tour & pl.col("tour_id").is_in(in_class.implode())

    tours = tours.with_columns(
        pl.when(on_tour).then(pl.col("joint_tour_id")).otherwise(None).alias(tour_col)
    )

    # No group of one, and no group of mixed purpose.
    aggs = [pl.len().alias("_n_tours")]
    if "tour_purpose" in tours.columns:
        aggs.append(pl.col("tour_purpose").n_unique().alias("_n_purposes"))
    groups = tours.filter(pl.col(tour_col).is_not_null()).group_by(tour_col).agg(aggs)
    dissolve = pl.col("_n_tours") < MIN_JOINT_PARTICIPANTS
    if "_n_purposes" in groups.columns:
        dissolve = dissolve | (pl.col("_n_purposes") > 1)
    dissolved = groups.filter(dissolve)[tour_col]
    return tours.with_columns(
        pl.when(pl.col(tour_col).is_in(dissolved.implode()))
        .then(None)
        .otherwise(pl.col(tour_col))
        .alias(tour_col)
    )


def _flag_joint_trip_ids(
    tables: dict[str, pl.DataFrame | None],
    profile: UsabilityProfile,
) -> None:
    """Stamp ``joint_trip_id_<profile>`` on linked trips, in place.

    A usable trip in a joint trip is on it; under ``fully_joint`` only while its
    tour is on its joint tour, which ``joint_tour_id_<profile>`` already says.
    """
    trips = tables["linked_trips"]
    trip_col = profile.joint_trip_id
    on_trip = pl.col(profile.flag).fill_null(value=False)
    if profile.requires_fully_joint:
        on_trip = on_trip & pl.col(profile.joint_tour_id).is_not_null()
    trips = trips.with_columns(
        pl.when(on_trip).then(pl.col("joint_trip_id")).otherwise(None).alias(trip_col)
    )
    tables["linked_trips"] = _dissolve_undersized(trips, trip_col)


# -- Judging the groupings ---------------------------------------------------------


def _flag_joint_groupings(
    tables: dict[str, pl.DataFrame | None],
    profile: UsabilityProfile,
) -> None:
    """Stamp the usable flag on the joint tour and joint trip tables, in place.

    A joint entity is a grouping, so it is usable only while it is still
    *joint*: at least :data:`MIN_JOINT_PARTICIPANTS` of its records must be on
    it under this profile. It also belongs to one household, and the consumer
    reads it through that, so it is no more usable than the household is.

    The grouping's own ``survey_complete`` (every traveller complete) is no veto.
    A member is on it only while usable, which already needs that member's
    record complete, so an incomplete traveller is pruned from the grouping
    rather than rejecting it for everyone else.
    """
    for joint_table, member_table, key in _JOINT_GROUPINGS:
        df = tables[joint_table]
        # The profile's own copy of the id is the stricter count: a usable tour
        # that left its group for a leg is not on it.
        own = profile.joint_tour_id if key == "joint_tour_id" else profile.joint_trip_id
        df = and_reduced(
            df.drop(profile.flag, strict=False),
            tables[member_table].filter(pl.col(own).is_not_null()),
            key,
            pl.len() >= MIN_JOINT_PARTICIPANTS,
            fill=False,
            into=profile.flag,
        )
        # The household is the only record a grouping belongs to. The person_day_id
        # a joint table carries is one traveller's, picked as a representative, so
        # it is not inherited: the count of usable members already decides.
        tables[joint_table] = and_reduced(
            df,
            tables["households"],
            "hh_id",
            pl.col(profile.flag).first(),
            fill=False,
            into=profile.flag,
        )


# -- Voiding what a rejected grouping left behind -----------------------------------


def _void_ids_of_rejected_groupings(
    tables: dict[str, pl.DataFrame | None],
    profile: UsabilityProfile,
) -> bool:
    """Null the profile's joint ids wherever they name a grouping it rejects.

    A joint tour the profile does not admit -- its household unusable, say -- is
    nobody's joint tour, so every tour and trip still carrying its id under
    this profile has that id voided. Under ``fully_joint`` a trip is on its
    joint trip only while its tour is on its joint tour, so those follow; and
    a grouping left with one record then dissolves. Returns whether anything
    changed.
    """
    changed = False
    for joint_table, _member_table, key in _JOINT_GROUPINGS:
        groups = tables[joint_table]
        rejected = groups.filter(~pl.col(profile.flag).fill_null(value=False))[key]
        own = profile.joint_tour_id if key == "joint_tour_id" else profile.joint_trip_id
        changed = _void_on_tables(tables, own, rejected) or changed
    if not changed:
        return False

    tables["tours"] = _dissolve_undersized(tables["tours"], profile.joint_tour_id)
    _copy_tour_answer(tables, profile)
    trips = tables["linked_trips"]
    if profile.requires_fully_joint:
        trips = trips.with_columns(
            pl.when(pl.col(profile.joint_tour_id).is_null())
            .then(None)
            .otherwise(pl.col(profile.joint_trip_id))
            .alias(profile.joint_trip_id)
        )
    tables["linked_trips"] = _dissolve_undersized(trips, profile.joint_trip_id)
    return True


def _void_on_tables(
    tables: dict[str, pl.DataFrame | None], id_col: str, rejected: pl.Series
) -> bool:
    """Null *id_col* wherever it names one of *rejected*, on every table carrying it."""
    if rejected.is_empty():
        return False
    changed = False
    # Only linked trips carry the joint trip ids.
    for name in ("tours", *_TOUR_MEMBER_TABLES):
        df = tables[name]
        if id_col not in df.columns:
            continue
        names_it = pl.col(id_col).is_in(rejected.implode())
        if df.filter(names_it).is_empty():
            continue
        changed = True
        tables[name] = df.with_columns(
            pl.when(names_it).then(None).otherwise(pl.col(id_col)).alias(id_col)
        )
    return changed


# -- Shared helpers ------------------------------------------------------------------


def _copy_tour_answer(tables: dict[str, pl.DataFrame | None], profile: UsabilityProfile) -> None:
    """Give both trip tables their tour's ``joint_tour_id_<profile>``, in place."""
    on_tour = tables["tours"].select("tour_id", pl.col(profile.joint_tour_id).alias("_on_tour"))
    for name in _TOUR_MEMBER_TABLES:
        # Written in place, so a second pass leaves the columns where they were.
        tables[name] = (
            tables[name]
            .join(on_tour, on="tour_id", how="left")
            .with_columns(pl.col("_on_tour").alias(profile.joint_tour_id))
            .drop("_on_tour")
        )


def _dissolve_undersized(df: pl.DataFrame, id_col: str) -> pl.DataFrame:
    """Null *id_col* wherever fewer than two records share its value.

    A group the profile has reduced to one record is no group. Nulling the id
    here, for every profile, is what lets a consumer trust that any record still
    carrying the id has company -- so the formatter checks that rather than
    repairing it.
    """
    counts = (
        df.filter(pl.col(id_col).is_not_null())
        .group_by(id_col)
        .len(name="_n_on_it")
        .filter(pl.col("_n_on_it") < MIN_JOINT_PARTICIPANTS)
    )
    if counts.is_empty():
        return df
    alone = counts[id_col]
    return df.with_columns(
        pl.when(pl.col(id_col).is_in(alone.implode()))
        .then(None)
        .otherwise(pl.col(id_col))
        .alias(id_col)
    )
