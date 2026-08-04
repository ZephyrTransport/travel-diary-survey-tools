"""At-work subtours must survive the model-usability gate and reach the formatters.

Regression cover for issue #85. A work-based subtour anchors at the workplace,
not at home, so it never starts or ends at home. Tour classification used to
write ``TourType.WORK_BASED`` into the ``tour_category`` column -- and those
enums collide, ``WORK_BASED == 2 == TourCategory.PARTIAL_END`` -- which left
every subtour permanently non-COMPLETE. Each ``tour_category == COMPLETE`` gate
(the DaySim drop since the first commit, the CT-RAMP drop since #79, the
``model_usable`` fuse since #83) then discarded it, and ``atWork_freq`` could
only ever be zero.

The fix splits the two facts: ``tour_type`` says what a tour is anchored on and
``tour_category`` says how completely it reaches that anchor, whichever anchor
that is. These tests pin both halves plus the downstream consequences.
"""

from datetime import datetime

import polars as pl
import pytest

from data_canon.codebook.ctramp import CTRAMPTourCategory
from data_canon.codebook.days import TravelDow
from data_canon.codebook.persons import AgeCategory, Employment, Student
from data_canon.codebook.tours import (
    TourCategory,
    TourDataQuality,
    TourDirection,
    TourType,
)
from data_canon.codebook.trips import Driver, ModeType, Purpose, PurposeCategory
from processing import link_trips
from processing.completeness import compute_model_usable
from processing.formatting.ctramp.ctramp_config import CTRAMPConfig
from processing.formatting.ctramp.filters import _drop_invalid_tours
from processing.formatting.ctramp.format_households import format_households
from processing.formatting.ctramp.format_tours import format_individual_tour
from processing.formatting.daysim.format_days import format_days as format_days_daysim
from processing.formatting.daysim.format_tours import format_tours as format_tours_daysim
from processing.tours.extraction import extract_tours
from tests.fixtures import (
    create_household,
    create_linked_trip,
    create_person,
    create_tour,
)
from tests.fixtures.fixtures import process_scenario_through_pipeline
from tests.fixtures.scenario_builders import multi_stop_tour
from tests.fixtures.tour_records import get_tour_schema

HOME = (37.80, -122.40)
WORK = (37.85, -122.45)
LUNCH = (37.86, -122.46)


def _persons_and_households():
    """One full-time worker with a reported workplace."""
    persons = pl.DataFrame(
        {
            "person_id": [1],
            "hh_id": [1],
            "age": [AgeCategory.AGE_35_TO_44.value],
            "employment": [Employment.EMPLOYED_FULLTIME.value],
            "student": [Student.NONSTUDENT.value],
            "school_type": [None],
            "work_lat": [WORK[0]],
            "work_lon": [WORK[1]],
            "school_lat": [None],
            "school_lon": [None],
        }
    )
    households = pl.DataFrame({"hh_id": [1], "home_lat": [HOME[0]], "home_lon": [HOME[1]]})
    return persons, households


def _trips(legs: list[tuple]) -> pl.DataFrame:
    """Build an unlinked_trips frame from (depart, arrive, o, d, o_cat, d_cat, o_p, d_p)."""
    return pl.DataFrame(
        {
            "unlinked_trip_id": list(range(1, len(legs) + 1)),
            "day_id": [1] * len(legs),
            "person_id": [1] * len(legs),
            "hh_id": [1] * len(legs),
            "travel_dow": [TravelDow.WEDNESDAY.value] * len(legs),
            "depart_time": [x[0] for x in legs],
            "arrive_time": [x[1] for x in legs],
            "o_lat": [x[2][0] for x in legs],
            "o_lon": [x[2][1] for x in legs],
            "d_lat": [x[3][0] for x in legs],
            "d_lon": [x[3][1] for x in legs],
            "o_purpose_category": [x[4] for x in legs],
            "d_purpose_category": [x[5] for x in legs],
            "o_purpose": [x[6] for x in legs],
            "d_purpose": [x[7] for x in legs],
            "mode_type": [ModeType.CAR.value] * len(legs),
            "unlinked_trip_weight": [1.0] * len(legs),
            "distance_meters": [5000.0] * len(legs),
            "duration_minutes": [30.0] * len(legs),
            "num_travelers": [1] * len(legs),
            "driver": [Driver.DRIVER.value] * len(legs),
        }
    )


def _lunch_subtour_day() -> pl.DataFrame:
    """Home -> work -> lunch -> work -> home: one work tour, one at-work subtour."""
    return _trips(
        [
            (
                datetime(2024, 1, 17, 8, 0),
                datetime(2024, 1, 17, 8, 30),
                HOME,
                WORK,
                PurposeCategory.HOME.value,
                PurposeCategory.WORK.value,
                Purpose.HOME.value,
                Purpose.PRIMARY_WORKPLACE.value,
            ),
            (
                datetime(2024, 1, 17, 12, 0),
                datetime(2024, 1, 17, 12, 15),
                WORK,
                LUNCH,
                PurposeCategory.WORK.value,
                PurposeCategory.MEAL.value,
                Purpose.PRIMARY_WORKPLACE.value,
                Purpose.DINING.value,
            ),
            (
                datetime(2024, 1, 17, 13, 0),
                datetime(2024, 1, 17, 13, 15),
                LUNCH,
                WORK,
                PurposeCategory.MEAL.value,
                PurposeCategory.WORK.value,
                Purpose.DINING.value,
                Purpose.PRIMARY_WORKPLACE.value,
            ),
            (
                datetime(2024, 1, 17, 17, 0),
                datetime(2024, 1, 17, 17, 30),
                WORK,
                HOME,
                PurposeCategory.WORK.value,
                PurposeCategory.HOME.value,
                Purpose.PRIMARY_WORKPLACE.value,
                Purpose.HOME.value,
            ),
        ]
    )


@pytest.fixture
def standard_config():
    """CT-RAMP config matching the other formatter tests."""
    return CTRAMPConfig(
        income_low_threshold=60000,
        income_med_threshold=150000,
        income_high_threshold=240000,
        income_base_year_dollars=2023,
        age_adult=4,
    )


@pytest.fixture
def extracted():
    """Extract tours for the lunch-subtour day."""
    persons, households = _persons_and_households()
    link_result = link_trips(
        unlinked_trips=_lunch_subtour_day(),
        change_mode_enum=PurposeCategory.CHANGE_MODE.value,
        transit_mode_enums=[ModeType.TRANSIT.value],
    )
    linked_trips = link_result["linked_trips"].with_columns(
        pl.lit(None).cast(pl.Int64).alias("joint_trip_id")
    )
    result = extract_tours(persons, households, link_result["unlinked_trips"], linked_trips)
    return result, persons, households


@pytest.fixture
def subtour_and_parent(extracted):
    """The (parent tour, subtour) pair from the lunch-subtour day."""
    tours = extracted[0]["tours"]
    parent = tours.filter(pl.col("subtour_num") == 0)
    subtour = tours.filter(pl.col("subtour_num") > 0)
    assert len(parent) == 1, "expected exactly one home-based parent tour"
    assert len(subtour) == 1, "expected exactly one at-work subtour"
    return parent.row(0, named=True), subtour.row(0, named=True)


class TestSubtourClassification:
    """Extraction records the anchor and the boundary as two separate facts."""

    def test_subtour_is_typed_work_based(self, subtour_and_parent):
        """The subtour is WORK_BASED and its parent HOME_BASED."""
        parent, subtour = subtour_and_parent
        assert parent["tour_type"] == TourType.HOME_BASED.value
        assert subtour["tour_type"] == TourType.WORK_BASED.value

    def test_subtour_is_complete_against_its_own_anchor(self, subtour_and_parent):
        """Work -> lunch -> work departs from and returns to the workplace.

        This is the fix for #85. Before it, the subtour carried
        ``TourType.WORK_BASED`` (2) in ``tour_category``, which decodes as
        ``PARTIAL_END``, so it could never pass a COMPLETE gate.
        """
        _parent, subtour = subtour_and_parent
        assert subtour["tour_category"] == TourCategory.COMPLETE.value

    def test_subtour_is_structurally_valid(self, subtour_and_parent):
        """A subtour has no home anchor, and must not be penalised for it."""
        _parent, subtour = subtour_and_parent
        assert subtour["tour_data_quality"] == TourDataQuality.VALID.value

    def test_tour_category_never_carries_a_tour_type_code(self, extracted):
        """Guard the enum collision that caused #85.

        ``TourType`` and ``TourCategory`` share the integer space, so a stray
        type code in ``tour_category`` is undetectable by value alone. Pinning
        subtours to COMPLETE is what makes the confusion visible: WORK_BASED
        would read as PARTIAL_END here.
        """
        tours = extracted[0]["tours"]
        assert set(tours["tour_category"].to_list()) == {TourCategory.COMPLETE.value}
        assert set(tours["tour_type"].to_list()) == {
            TourType.HOME_BASED.value,
            TourType.WORK_BASED.value,
        }

    def test_subtour_trips_get_a_real_direction(self, extracted):
        """A subtour is split into outbound/inbound like any other tour.

        Direction is relative to the tour's *own* anchor and primary
        destination, so the leg out to the subtour activity is OUTBOUND and the
        leg back to the workplace is INBOUND. Stamping both "SUBTOUR" instead
        discarded the direction DaySim (``half``) and CT-RAMP (``inbound``)
        need; subtour membership is carried by ``subtour_num`` /
        ``parent_tour_id`` / ``tour_type``.
        """
        linked_trips = extracted[0]["linked_trips"]
        subtour_trips = linked_trips.filter(pl.col("subtour_num") > 0).sort("depart_time")
        assert len(subtour_trips) == 2
        assert subtour_trips["tour_direction"].to_list() == [
            TourDirection.OUTBOUND.value,
            TourDirection.INBOUND.value,
        ]


def _gate(tours: pl.DataFrame) -> dict[int, bool]:
    """Run the real ``model_usable`` gate over *tours*, keyed by canonical tour_id.

    Feeds the gate genuinely extracted tours rather than hand-written ones, so a
    misclassification upstream shows up here as a dropped tour -- which is
    exactly how #85 reached the formatters.
    """
    tables = {
        "days": pl.DataFrame(
            {
                "day_id": tours["day_id"].unique().sort(),
                "person_id": [1],
                "hh_id": [1],
                "travel_date": [datetime(2024, 1, 17)],
                "complete": [True],
            }
        ),
        "tours": tours.with_columns(pl.lit(value=True).alias("complete")),
    }
    compute_model_usable(tables)
    return dict(zip(*tables["tours"].select("tour_id", "model_usable"), strict=True))


class TestSubtourModelUsability:
    """The ``model_usable`` gate admits subtours, but not orphaned ones."""

    def test_subtour_is_model_usable(self, extracted, subtour_and_parent):
        """A well-formed subtour on a coherent household-day is admissible."""
        parent, subtour = subtour_and_parent
        usable = _gate(extracted[0]["tours"])
        assert usable[parent["tour_id"]] is True
        assert usable[subtour["tour_id"]] is True, "a VALID at-work subtour must reach the model"

    def test_subtour_dies_with_its_parent(self, extracted, subtour_and_parent):
        """Dropping the parent tour must drop its subtour too.

        Otherwise CT-RAMP gets an AT_WORK tour hanging off a tour that is not in
        the output, and the parent's ``atWork_freq`` is stranded.
        """
        parent, subtour = subtour_and_parent
        # Spoil only the parent; the subtour stays structurally perfect.
        tours = extracted[0]["tours"].with_columns(
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

    def test_drop_invalid_tours_keeps_the_subtour(self, extracted, subtour_and_parent):
        """The CT-RAMP pre-format drop must not remove at-work subtours.

        Runs on genuinely extracted tours: the drop re-derives its criterion
        from ``tour_data_quality`` / ``tour_category``, so a subtour misclassified
        upstream is discarded right here -- the mechanism behind #85.
        """
        parent, subtour = subtour_and_parent
        tours = extracted[0]["tours"]
        linked_trips = extracted[0]["linked_trips"]
        joint_trips = pl.DataFrame({"joint_trip_id": [], "hh_id": []})

        kept, kept_trips, _ = _drop_invalid_tours(tours, linked_trips, joint_trips)

        assert sorted(kept["tour_id"].to_list()) == sorted(
            [parent["tour_id"], subtour["tour_id"]]
        ), "the at-work subtour must survive the CT-RAMP drop"
        assert len(kept_trips) == len(linked_trips), "the subtour's trips must survive with it"

    def _formatted(self, config):
        """Run the real CT-RAMP tour formatter over a work tour + its subtour."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        # Canonical ids pack day / tour_num / subtour_num, and a subtour shares
        # its parent's tour_num -- the collision the formatter has to resolve.
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
                    trip_id=1, tour_id=11000, person_id=101, tour_direction=TourDirection.OUTBOUND
                ),
                create_linked_trip(
                    trip_id=2, tour_id=11000, person_id=101, tour_direction=TourDirection.INBOUND
                ),
                create_linked_trip(
                    trip_id=3, tour_id=11010, person_id=101, tour_direction=TourDirection.OUTBOUND
                ),
                create_linked_trip(
                    trip_id=4, tour_id=11010, person_id=101, tour_direction=TourDirection.INBOUND
                ),
            ]
        )
        households_ctramp = format_households(households, persons, tours, config)
        return format_individual_tour(tours, trips, persons, households_ctramp, config)

    def test_subtour_gets_its_own_ctramp_tour_id(self, standard_config):
        """CT-RAMP tour_id is unique within the person.

        ``tour_num`` is not: it restarts each day and a subtour shares its
        parent's value, so emitting it directly collided a work tour with its
        own at-work subtour. Base tours are numbered 0-based per person and
        subtours as <1-based parent tour #><subtour #>, so the person's first
        tour is 0 and its first subtour is 11.
        """
        result = self._formatted(standard_config)
        assert result["tour_id"].n_unique() == len(result), (
            "a work tour and its at-work subtour must not share a CT-RAMP tour_id"
        )
        by_id = dict(zip(*result.select("_tour_id_canonical", "tour_id"), strict=True))
        assert by_id[11000] == 0, "the person's first base tour is 0"
        assert by_id[11010] == 11, "its first subtour is <parent 1><subtour 1>"

    def test_subtour_maps_to_the_at_work_category(self, standard_config):
        """A subtour is AT_WORK to CT-RAMP; its home-based parent is MANDATORY."""
        result = self._formatted(standard_config)
        by_id = dict(zip(*result.select("_tour_id_canonical", "tour_category"), strict=True))
        assert by_id[11000] == CTRAMPTourCategory.MANDATORY.value
        assert by_id[11010] == CTRAMPTourCategory.AT_WORK.value

    def test_parent_tour_reports_its_subtour_frequency(self, standard_config):
        """``atWork_freq`` counts the parent's subtours -- it was always 0 before."""
        result = self._formatted(standard_config)
        by_id = dict(zip(*result.select("_tour_id_canonical", "atWork_freq"), strict=True))
        assert by_id[11000] == 1, "the work tour has one at-work subtour"
        assert by_id[11010] == 0, "a subtour has no subtours of its own"


class TestSubtourReachesDaysim:
    """DaySim numbers subtours distinctly and counts them as work-based."""

    @pytest.fixture
    def daysim_tours(self):
        """Run the lunch-subtour scenario through link -> extract -> DaySim tours."""
        households, persons, days, unlinked_trips = multi_stop_tour()
        data = process_scenario_through_pipeline(households, persons, days, unlinked_trips)
        return format_tours_daysim(
            data["persons"], data["days"], data["linked_trips"], data["tours"]
        ), data

    def test_subtour_gets_its_own_daysim_tour_number(self, daysim_tours):
        """DaySim keys tours on (hhno, pno, day, tour), so the numbers must differ.

        A subtour carries its parent's ``tour_num``, so emitting that directly
        put a work tour and its at-work subtour on the same key.
        """
        result, _data = daysim_tours
        keys = result.select("hhno", "pno", "day", "tour")
        assert keys.n_unique() == len(result), (
            "a work tour and its at-work subtour must not share a DaySim tour number"
        )

    def test_subtour_points_at_its_parent(self, daysim_tours):
        """``parent`` names the parent's DaySim tour number; 0 for home-based tours.

        DaySim's tour file drops the canonical id, so the parent link has to be
        expressed in DaySim's own numbering.
        """
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
        """``hbtours`` counts only home-anchored tours; ``wbtours`` the subtours.

        Both are COMPLETE now -- each against its own anchor -- so COMPLETE alone
        can no longer tell them apart.
        """
        _result, data = daysim_tours
        days_daysim = format_days_daysim(data["persons"], data["days"], data["tours"])
        assert days_daysim["hbtours"].sum() == 1, "one home-based work tour"
        assert days_daysim["wbtours"].sum() == 1, "one at-work subtour"
