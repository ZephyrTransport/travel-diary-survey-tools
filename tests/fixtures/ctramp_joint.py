"""Two-person households with joint tours, run through the CT-RAMP tour and trip formatters."""

from collections.abc import Sequence

import polars as pl

from data_canon.codebook.tours import TourDirection
from data_canon.codebook.trips import PurposeCategory
from processing.formatting.ctramp.format_households import format_households
from processing.formatting.ctramp.format_joint_tours import format_joint_tour
from processing.formatting.ctramp.format_joint_trips import format_joint_trip
from processing.formatting.ctramp.format_tours import format_individual_tour
from processing.formatting.ctramp.format_trips import format_individual_trip

from .records import create_household, create_linked_trip, create_person, create_tour
from .schema_utils import empty, get_tour_schema

BOTH_LEGS = (TourDirection.OUTBOUND, TourDirection.INBOUND)
# Unequal, so the sum, the mean and twice the first member's weight all differ.
MEMBER_WEIGHTS = (10.0, 30.0)


def two_person_household(**second_person):
    """Household 1 with persons 101 and 102; keyword arguments override person 102's fields."""
    households = pl.DataFrame([create_household(hh_id=1)])
    persons = pl.DataFrame(
        [
            create_person(person_id=101, hh_id=1, person_num=1),
            create_person(person_id=102, hh_id=1, person_num=2, **second_person),
        ]
    )
    return households, persons


def tours_frame(specs: Sequence[dict]) -> pl.DataFrame:
    """Canonical tours from dicts of tour_id, person_id, person_num, purpose and weight."""
    rows = [
        create_tour(
            tour_id=spec["tour_id"],
            person_id=spec["person_id"],
            person_num=spec["person_num"],
            tour_num=spec.get("tour_num", 1),
            joint_tour_id=spec.get("joint_tour_id"),
            tour_purpose=spec["purpose"],
            num_travelers=spec.get("num_travelers", 2),
            tour_weight=spec["weight"],
        )
        for spec in specs
    ]
    return pl.DataFrame(rows, schema=get_tour_schema()).with_columns(
        pl.col("joint_tour_id").cast(pl.Int64)
    )


def shared_tour(
    purpose: PurposeCategory,
    *,
    joint_tour_id: int | None = 5001,
    weights: tuple[float, float] = MEMBER_WEIGHTS,
    num_travelers: int = 2,
) -> pl.DataFrame:
    """Persons 101 and 102 on tours of the same purpose, grouped as joint unless the id is None."""
    return tours_frame(
        [
            {
                "tour_id": 1001 + i,
                "person_id": 101 + i,
                "person_num": 1 + i,
                "joint_tour_id": joint_tour_id,
                "purpose": purpose,
                "weight": weight,
                "num_travelers": num_travelers,
            }
            for i, weight in enumerate(weights)
        ]
    )


def mixed_tours() -> pl.DataFrame:
    """Persons 101 and 102 share joint tour 5001; person 101 also makes an individual work tour."""
    return pl.concat(
        [
            shared_tour(PurposeCategory.SOCIALREC),
            tours_frame(
                [
                    {
                        "tour_id": 1003,
                        "person_id": 101,
                        "person_num": 1,
                        "tour_num": 2,
                        "purpose": PurposeCategory.WORK,
                        "weight": MEMBER_WEIGHTS[0],
                        "num_travelers": 1,
                    }
                ]
            ),
        ]
    )


def trips_for(
    tours: pl.DataFrame,
    *,
    joint_trip_ids: tuple[int | None, int | None] | None = None,
) -> pl.DataFrame:
    """One outbound and one inbound trip per tour, carrying the tour's weight.

    Every leg of a joint tour is a joint trip, with an id derived from the joint
    tour. Pass ``joint_trip_ids`` to set the ids directly, which is the only way
    to share trips with no joint tour behind them.
    """
    rows = []
    for tour in tours.iter_rows(named=True):
        joint_tour_id = tour["joint_tour_id"]
        for leg, direction in enumerate(BOTH_LEGS):
            if joint_trip_ids is not None:
                joint_trip_id = joint_trip_ids[leg]
            elif joint_tour_id is not None:
                joint_trip_id = joint_tour_id * 10 + leg
            else:
                joint_trip_id = None
            rows.append(
                create_linked_trip(
                    linked_trip_id=len(rows) + 1,
                    person_id=tour["person_id"],
                    person_num=tour["person_num"],
                    tour_id=tour["tour_id"],
                    joint_tour_id=joint_tour_id,
                    joint_trip_id=joint_trip_id,
                    tour_direction=direction,
                    num_travelers=tour["num_travelers"],
                    linked_trip_weight=tour["tour_weight"],
                )
            )
    return pl.DataFrame(rows).with_columns(
        pl.col("joint_tour_id").cast(pl.Int64),
        pl.col("joint_trip_id").cast(pl.Int64),
    )


def joint_trips_table(trips: pl.DataFrame) -> pl.DataFrame:
    """Member trips grouped into the canonical joint_trips shape; the weight is the members' sum."""
    return (
        trips.filter(pl.col("joint_trip_id").is_not_null())
        .group_by("joint_trip_id")
        .agg(
            pl.col(
                "hh_id",
                "tour_id",
                "o_purpose_category",
                "d_purpose_category",
                "o_taz",
                "d_taz",
                "mode_type",
                "depart_time",
                "arrive_time",
                "tour_direction",
            ).first(),
            pl.len().alias("num_joint_travelers"),
            pl.col("linked_trip_weight").sum().alias("joint_trip_weight"),
        )
    )


def format_all(
    tours,
    trips,
    persons,
    households,
    config,
    *,
    joint_tours_canonical=None,
    joint_trips_canonical=None,
    include_joint=False,
) -> dict[str, pl.DataFrame]:
    """Run the household, individual and joint tour and trip formatters over one scenario."""
    households_ctramp = format_households(households, persons, tours, config)
    individual_tours = format_individual_tour(
        tours_canonical=tours,
        linked_trips_canonical=trips,
        unlinked_trips_canonical=pl.DataFrame(),
        persons_canonical=persons,
        households_ctramp=households_ctramp,
        config=config,
        include_joint=include_joint,
    )
    individual_trips = format_individual_trip(
        linked_trips_canonical=trips,
        unlinked_trips_canonical=pl.DataFrame(),
        tours_ctramp=individual_tours,
        persons_canonical=persons,
        households_ctramp=households_ctramp,
        config=config,
    )
    joint_tours = format_joint_tour(
        tours_canonical=tours,
        linked_trips_canonical=trips,
        unlinked_trips_canonical=pl.DataFrame(),
        joint_tours_canonical=(
            empty("joint_tours") if joint_tours_canonical is None else joint_tours_canonical
        ),
        persons_canonical=persons,
        households_ctramp=households_ctramp,
        config=config,
    )
    joint_trips = format_joint_trip(
        joint_trips_canonical=(
            joint_trips_table(trips) if joint_trips_canonical is None else joint_trips_canonical
        ),
        linked_trips_canonical=trips,
        unlinked_trips_canonical=pl.DataFrame(),
        tours_canonical=tours,
        households_ctramp=households_ctramp,
        config=config,
    )
    return {
        "households": households_ctramp,
        "individual_tours": individual_tours,
        "individual_trips": individual_trips,
        "joint_tours": joint_tours,
        "joint_trips": joint_trips,
    }
