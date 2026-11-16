"""At-work subtours survive the model-usability gate and reach the formatters.

``tour_type`` says what a tour is anchored on and ``tour_category`` how
completely it reaches that anchor, whichever anchor that is, so a work-based
subtour is COMPLETE when it returns to the workplace.
"""

from datetime import datetime

import polars as pl
import pytest

from data_canon.codebook.ctramp import AtWorkFreq, CTRAMPTourCategory
from data_canon.codebook.tours import (
    TourCategory,
    TourDataQuality,
    TourDirection,
    TourType,
)
from data_canon.codebook.trips import Purpose, PurposeCategory
from processing.formatting.ctramp.format_households import format_households
from processing.formatting.ctramp.format_tours import format_individual_tour
from processing.formatting.daysim.format_days import format_days as format_days_daysim
from processing.formatting.daysim.format_tours import format_tours as format_tours_daysim
from processing.formatting.usable_records import keep_usable
from tests.fixtures.cascade import STRICT, stamp
from tests.fixtures.fixtures import process_scenario_through_pipeline
from tests.fixtures.records import create_household, create_linked_trip, create_person, create_tour
from tests.fixtures.scenario_builders import multi_stop_tour
from tests.fixtures.schema_utils import get_tour_schema
from tests.fixtures.tour_pipeline import (
    HOME,
    HOUSEHOLDS,
    LUNCH,
    WORK,
    at,
    leg,
    link_and_extract_tours,
    person,
)

H, W, M = Purpose.HOME, Purpose.PRIMARY_WORKPLACE, Purpose.DINING

# Home -> work -> lunch -> work -> home: one work tour, one at-work subtour.
LUNCH_SUBTOUR_DAY = [
    leg(HOME, WORK, H, W, at(8), at(8, 30)),
    leg(WORK, LUNCH, W, M, at(12), at(12, 15)),
    leg(LUNCH, WORK, M, W, at(13), at(13, 15)),
    leg(WORK, HOME, W, H, at(17), at(17, 30)),
]


@pytest.fixture
def extracted():
    """Extract tours for the lunch-subtour day."""
    return link_and_extract_tours(person(), HOUSEHOLDS, LUNCH_SUBTOUR_DAY)


@pytest.fixture
def subtour_and_parent(extracted):
    """The (parent tour, subtour) pair from the lunch-subtour day."""
    tours = extracted["tours"]
    parent = tours.filter(pl.col("subtour_num") == 0)
    subtour = tours.filter(pl.col("subtour_num") > 0)
    assert len(parent) == 1, "expected exactly one home-based parent tour"
    assert len(subtour) == 1, "expected exactly one at-work subtour"
    return parent.row(0, named=True), subtour.row(0, named=True)


class TestSubtourClassification:
    """Extraction records the anchor and the boundary as two separate facts."""

    def test_subtour_is_work_based_complete_and_valid(self, subtour_and_parent):
        """A WORK_BASED subtour of a HOME_BASED tour; both COMPLETE against their own anchor.

        A subtour has no home anchor and must not be penalised for it.
        """
        parent, subtour = subtour_and_parent
        assert parent["tour_type"] == TourType.HOME_BASED.value
        assert subtour["tour_type"] == TourType.WORK_BASED.value
        assert subtour["tour_category"] == TourCategory.COMPLETE.value
        assert parent["tour_category"] == TourCategory.COMPLETE.value
        assert subtour["tour_data_quality"] == TourDataQuality.VALID.value

    def test_subtour_trips_get_a_real_direction(self, extracted):
        """Out to the subtour activity is OUTBOUND; back to the workplace is INBOUND.

        Direction is relative to the tour's own anchor; DaySim (``half``) and
        CT-RAMP (``inbound``) need it.
        """
        linked_trips = extracted["linked_trips"]
        subtour_trips = linked_trips.filter(pl.col("subtour_num") > 0).sort("depart_time")
        assert subtour_trips["tour_direction"].to_list() == [
            TourDirection.OUTBOUND.value,
            TourDirection.INBOUND.value,
        ]


def _gate(tours: pl.DataFrame) -> dict[int, bool]:
    """Run the real usability cascade over extracted *tours*, keyed by tour_id."""
    tables = {
        "households": pl.DataFrame({"hh_id": [1]}),
        "persons": pl.DataFrame({"person_id": [1], "hh_id": [1]}),
        "person_days": pl.DataFrame(
            {
                "person_day_id": tours["person_day_id"].unique().sort(),
                "person_id": [1],
                "hh_id": [1],
                "travel_date": [datetime(2024, 1, 17)],
                "survey_complete": [True],
            }
        ),
        "tours": tours.with_columns(pl.lit(value=True).alias("survey_complete")),
    }
    out = stamp(tables, STRICT)
    return dict(zip(*out["tours"].select("tour_id", "usable_test"), strict=True))


class TestSubtourModelUsability:
    """The usability gate admits subtours, but not orphaned ones."""

    def test_subtour_is_usable(self, extracted, subtour_and_parent):
        """A well-formed subtour on a coherent household-day is admissible."""
        parent, subtour = subtour_and_parent
        usable = _gate(extracted["tours"])
        assert usable[parent["tour_id"]] is True
        assert usable[subtour["tour_id"]] is True, "a VALID at-work subtour must reach the model"

    def test_subtour_dies_with_its_parent(self, extracted, subtour_and_parent):
        """Dropping the parent tour drops its subtour too.

        Otherwise CT-RAMP gets an AT_WORK tour hanging off a tour not in the output.
        """
        parent, subtour = subtour_and_parent
        # Spoil only the parent; the subtour stays structurally perfect.
        tours = extracted["tours"].with_columns(
            pl.when(pl.col("tour_id") == parent["tour_id"])
            .then(pl.lit(TourDataQuality.SPATIAL_GAP.value))
            .otherwise(pl.col("tour_data_quality"))
            .alias("tour_data_quality")
        )
        usable = _gate(tours)
        assert usable[parent["tour_id"]] is False
        assert usable[subtour["tour_id"]] is False, "a subtour must not outlive its parent tour"


class TestSubtourReachesCtramp:
    """The CT-RAMP formatter keeps subtours and gives them their own identity."""

    def test_the_gate_keeps_the_subtour(self, extracted, subtour_and_parent):
        """The formatter's gate keeps an extracted subtour and its trips."""
        parent, subtour = subtour_and_parent
        usable = _gate(extracted["tours"])
        tours = extracted["tours"].with_columns(
            pl.col("tour_id").replace_strict(usable).alias("usable_test")
        )
        linked_trips = extracted["linked_trips"].with_columns(
            pl.col("tour_id").replace_strict(usable).alias("usable_test")
        )

        kept = keep_usable({"tours": tours, "linked_trips": linked_trips}, "test")

        assert sorted(kept["tours"]["tour_id"].to_list()) == sorted(
            [parent["tour_id"], subtour["tour_id"]]
        ), "the at-work subtour must survive the gate"
        assert len(kept["linked_trips"]) == len(linked_trips), (
            "the subtour's trips must survive with it"
        )

    def test_subtour_gets_its_own_id_category_and_frequency(self, standard_config):
        """A work tour is CT-RAMP tour 0 and its subtour 11, AT_WORK, counted in ``atWork_freq``.

        A subtour shares its parent's ``tour_num``, so the formatter numbers base
        tours 0-based per person and subtours as <1-based parent tour #><subtour #>.
        """
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [
                create_tour(
                    tour_id=11000,
                    person_id=101,
                    hh_id=1,
                    tour_num=1,
                    parent_tour_id=11000,
                    tour_purpose=PurposeCategory.WORK,
                ),
                create_tour(
                    tour_id=11010,
                    person_id=101,
                    hh_id=1,
                    tour_num=1,
                    subtour_num=1,
                    parent_tour_id=11000,
                    tour_purpose=PurposeCategory.MEAL,
                ),
            ],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=i, tour_id=tour_id, person_id=101, tour_direction=direction
                )
                for i, (tour_id, direction) in enumerate(
                    [
                        (11000, TourDirection.OUTBOUND),
                        (11000, TourDirection.INBOUND),
                        (11010, TourDirection.OUTBOUND),
                        (11010, TourDirection.INBOUND),
                    ],
                    start=1,
                )
            ]
        )
        result = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=format_households(households, persons, tours, standard_config),
            config=standard_config,
        )

        assert result["tour_id"].n_unique() == len(result)
        by_id = {row["_tour_id_canonical"]: row for row in result.iter_rows(named=True)}
        assert (by_id[11000]["tour_id"], by_id[11010]["tour_id"]) == (0, 11)
        assert by_id[11000]["tour_category"] == CTRAMPTourCategory.MANDATORY.value
        assert by_id[11010]["tour_category"] == CTRAMPTourCategory.AT_WORK.value
        assert by_id[11000]["atWork_freq"] == AtWorkFreq.ONE_EAT.value
        assert by_id[11010]["atWork_freq"] == AtWorkFreq.NONE_NOT_WORK.value


class TestSubtourReachesDaysim:
    """DaySim numbers subtours distinctly and counts them as work-based."""

    @pytest.fixture
    def daysim_tours(self):
        """Run the lunch-subtour scenario through link -> extract -> DaySim tours."""
        households, persons, person_days, unlinked_trips = multi_stop_tour()
        data = process_scenario_through_pipeline(households, persons, person_days, unlinked_trips)
        return format_tours_daysim(
            data["persons"], data["person_days"], data["linked_trips"], data["tours"]
        ), data

    def test_subtour_gets_its_own_daysim_tour_number(self, daysim_tours):
        """DaySim keys tours on (hhno, pno, day, tour); a subtour shares its parent's tour_num."""
        result, _data = daysim_tours
        keys = result.select("hhno", "pno", "day", "tour")
        assert keys.n_unique() == len(result), (
            "a work tour and its at-work subtour must not share a DaySim tour number"
        )

    def test_subtour_points_at_its_parent(self, daysim_tours):
        """``parent`` names the parent's DaySim tour number; 0 for home-based tours."""
        result, data = daysim_tours
        n_subtours = data["tours"].filter(pl.col("subtour_num") > 0).height
        assert n_subtours == 1, "precondition: the scenario produces one at-work subtour"

        subtours = result.filter(pl.col("parent") != 0)
        assert subtours.height == 1, "exactly one DaySim row should declare a parent tour"
        assert result.filter(pl.col("parent") == 0).height == result.height - 1, (
            "home-based tours must report parent=0, not point at themselves"
        )
        assert subtours["parent"][0] in result["tour"].to_list(), (
            "a subtour's parent must name a tour that is actually in the output"
        )
        assert subtours["parent"][0] != subtours["tour"][0], "a subtour must not be its own parent"

    def test_day_counts_split_home_based_from_work_based(self, daysim_tours):
        """``hbtours`` counts only home-anchored tours; ``wbtours`` the subtours."""
        _result, data = daysim_tours
        days_daysim = format_days_daysim(data["persons"], data["person_days"], data["tours"])
        assert days_daysim["hbtours"].sum() == 1, "one home-based work tour"
        assert days_daysim["wbtours"].sum() == 1, "one at-work subtour"
