"""Tests for tour priority calculation utilities."""

import datetime

import polars as pl
import pytest

from data_canon.codebook.tours import PersonCategory
from data_canon.codebook.trips import ModeType, PurposeCategory
from processing.tours.priority_utils import (
    add_activity_duration_column,
    add_mode_priority_column,
    add_purpose_priority_column,
    add_purpose_score_column,
)
from processing.tours.tour_configs import TourConfig


@pytest.fixture
def default_config():
    """Get default tour configuration."""
    return TourConfig()


class TestPersonCategory:
    """Test PersonCategory constants."""

    def test_worker_constant(self):
        """Test that WORKER constant has expected value."""
        assert PersonCategory.WORKER == "worker"

    def test_student_constant(self):
        """Test that STUDENT constant has expected value."""
        assert PersonCategory.STUDENT == "student"

    def test_other_constant(self):
        """Test that OTHER constant has expected value."""
        assert PersonCategory.OTHER == "other"


class TestAddPurposePriorityColumn:
    """Test add_purpose_priority_column function."""

    def test_adds_priority_column(self, default_config):
        """Test that priority column is added correctly."""
        df = pl.DataFrame(
            {
                "person_category": [PersonCategory.WORKER] * 3,
                "d_purpose_category": [
                    PurposeCategory.WORK.value,
                    PurposeCategory.SHOP.value,
                    PurposeCategory.HOME.value,
                ],
            }
        )

        result = add_purpose_priority_column(df, default_config)

        assert "purpose_priority" in result.columns
        # HOME should have priority 999
        assert (
            result.filter(pl.col("d_purpose_category") == PurposeCategory.HOME.value)[
                "purpose_priority"
            ][0]
            == 999
        )

    def test_custom_alias(self, default_config):
        """Test that custom column alias works."""
        df = pl.DataFrame(
            {
                "person_category": [PersonCategory.WORKER],
                "d_purpose_category": [PurposeCategory.WORK.value],
            }
        )

        result = add_purpose_priority_column(df, default_config, alias="custom_priority")

        assert "custom_priority" in result.columns
        assert "purpose_priority" not in result.columns


class TestAddModePriorityColumn:
    """Test add_mode_priority_column function."""

    def test_adds_mode_priority_column(self):
        """Test that mode priority column is added."""
        mode_hierarchy = [ModeType.WALK, ModeType.BIKE, ModeType.CAR, ModeType.TRANSIT]

        df = pl.DataFrame(
            {
                "mode_type": [
                    ModeType.TRANSIT.value,
                    ModeType.CAR.value,
                    ModeType.WALK.value,
                ],
            }
        )

        result = add_mode_priority_column(df, mode_hierarchy)

        assert "mode_priority" in result.columns
        assert len(result) == 3
        # Transit should have highest priority (last in list)
        transit_priority = result.filter(pl.col("mode_type") == ModeType.TRANSIT.value)[
            "mode_priority"
        ][0]
        walk_priority = result.filter(pl.col("mode_type") == ModeType.WALK.value)["mode_priority"][
            0
        ]
        assert transit_priority > walk_priority

    def test_custom_mode_alias(self):
        """Test custom alias for mode priority."""
        mode_hierarchy = [ModeType.WALK, ModeType.CAR]

        df = pl.DataFrame(
            {
                "mode_type": [ModeType.CAR.value],
            }
        )

        result = add_mode_priority_column(df, mode_hierarchy, alias="custom_mode")

        assert "custom_mode" in result.columns
        assert "mode_priority" not in result.columns


class TestAddActivityDurationColumn:
    """Test add_activity_duration_column function."""

    def test_adds_duration_column(self):
        """Test that activity duration column is calculated correctly."""
        df = pl.DataFrame(
            {
                "person_id": [1, 1, 1],
                "day_id": [1, 1, 1],
                "arrive_time": [
                    datetime.datetime(2023, 1, 1, 8, 0),
                    datetime.datetime(2023, 1, 1, 12, 0),
                    datetime.datetime(2023, 1, 1, 17, 0),
                ],
                "depart_time": [
                    datetime.datetime(2023, 1, 1, 8, 30),
                    datetime.datetime(2023, 1, 1, 13, 0),
                    datetime.datetime(2023, 1, 1, 18, 0),
                ],
            }
        )

        result = add_activity_duration_column(df)

        assert "activity_duration" in result.columns
        # Activity duration = next_trip.depart_time - current_trip.arrive_time
        # Trip 0: arrive 08:00, next departs 13:00 → 13:00 - 08:00 = 300 min
        # Trip 1: arrive 12:00, next departs 18:00 → 18:00 - 12:00 = 360 min
        # Trip 2: last trip, uses default = 240 min
        assert result["activity_duration"][0] == 300.0
        assert result["activity_duration"][1] == 360.0
        assert result["activity_duration"][2] == 240.0

    def test_custom_default_duration(self):
        """Test custom default duration for last trip."""
        df = pl.DataFrame(
            {
                "person_id": [1],
                "day_id": [1],
                "arrive_time": [datetime.datetime(2023, 1, 1, 8, 0)],
                "depart_time": [datetime.datetime(2023, 1, 1, 9, 0)],
            }
        )

        result = add_activity_duration_column(df, default_minutes=120.0)

        # Should use custom default
        assert result["activity_duration"][0] == 120.0

    def test_custom_alias(self):
        """Test custom column alias."""
        df = pl.DataFrame(
            {
                "person_id": [1],
                "day_id": [1],
                "arrive_time": [datetime.datetime(2023, 1, 1, 8, 0)],
                "depart_time": [datetime.datetime(2023, 1, 1, 9, 0)],
            }
        )

        result = add_activity_duration_column(df, alias="custom_duration")

        assert "custom_duration" in result.columns
        assert "activity_duration" not in result.columns


def _score_trips(person_category, purpose_category, durations):
    """One trip per duration, all same person category and purpose."""
    n = len(durations)
    return pl.DataFrame(
        {
            "person_category": [person_category] * n,
            "_d_purpose_effective": [purpose_category] * n,
            "_activity_duration": [float(d) for d in durations],
        }
    )


class TestAddPurposeScoreColumn:
    """Test the duration-weighted purpose score used to pick tour purpose."""

    def test_score_increases_with_duration(self, default_config):
        """A longer activity of the same purpose always scores higher."""
        df = _score_trips(PersonCategory.WORKER, PurposeCategory.SHOP.value, [10, 60, 240])
        scored = add_purpose_score_column(df, default_config, alias="_s")["_s"].to_list()
        assert scored[0] < scored[1] < scored[2]

    def test_score_is_half_of_ceiling_at_halfmax(self, default_config):
        """At duration == h the score is exactly W / 2."""
        h = default_config.purpose_score_halfmax[PurposeCategory.SHOP]
        w = default_config.purpose_score_weights[PersonCategory.WORKER][PurposeCategory.SHOP]
        df = _score_trips(PersonCategory.WORKER, PurposeCategory.SHOP.value, [h])
        score = add_purpose_score_column(df, default_config, alias="_s")["_s"][0]
        assert score == pytest.approx(w / 2)

    def test_trivial_mandatory_driveby_is_overridden(self, default_config):
        """A sub-threshold work drive-by (5 min) loses to a long shopping stop.

        Mandatory purposes are sticky (low h), but only above the establishment
        threshold; a trivial pass-by falls below it and a real activity wins.
        """
        df = pl.DataFrame(
            {
                "person_category": [PersonCategory.WORKER, PersonCategory.WORKER],
                "_d_purpose_effective": [
                    PurposeCategory.WORK.value,
                    PurposeCategory.SHOP.value,
                ],
                "_activity_duration": [5.0, 240.0],
            }
        )
        scored = add_purpose_score_column(df, default_config, alias="_s")
        work = scored.filter(pl.col("_d_purpose_effective") == PurposeCategory.WORK.value)["_s"][0]
        shop = scored.filter(pl.col("_d_purpose_effective") == PurposeCategory.SHOP.value)["_s"][0]
        assert shop > work

    def test_modest_mandatory_is_sticky_over_long_discretionary(self, default_config):
        """A genuine but short work visit (30 min) still beats a long shop.

        This is the stickiness the scoring is calibrated for: any real mandatory
        visit wins, so short work stops are not reclassified as discretionary.
        """
        df = pl.DataFrame(
            {
                "person_category": [PersonCategory.WORKER, PersonCategory.WORKER],
                "_d_purpose_effective": [
                    PurposeCategory.WORK.value,
                    PurposeCategory.SHOP.value,
                ],
                "_activity_duration": [30.0, 240.0],
            }
        )
        scored = add_purpose_score_column(df, default_config, alias="_s")
        work = scored.filter(pl.col("_d_purpose_effective") == PurposeCategory.WORK.value)["_s"][0]
        shop = scored.filter(pl.col("_d_purpose_effective") == PurposeCategory.SHOP.value)["_s"][0]
        assert work > shop

    def test_pure_escort_wins_but_escort_with_activity_does_not(self, default_config):
        """Escort scores below any real activity, so escort+shop is a shop tour."""
        df = pl.DataFrame(
            {
                "person_category": [PersonCategory.WORKER, PersonCategory.WORKER],
                "_d_purpose_effective": [
                    PurposeCategory.ESCORT.value,
                    PurposeCategory.SHOP.value,
                ],
                # a long escort vs a modest shop: shop still wins
                "_activity_duration": [120.0, 30.0],
            }
        )
        scored = add_purpose_score_column(df, default_config, alias="_s")
        escort = scored.filter(pl.col("_d_purpose_effective") == PurposeCategory.ESCORT.value)[
            "_s"
        ][0]
        shop = scored.filter(pl.col("_d_purpose_effective") == PurposeCategory.SHOP.value)["_s"][0]
        assert shop > escort

    def test_typical_mandatory_outscores_long_discretionary(self, default_config):
        """A normal work day still beats even a long social visit."""
        df = pl.DataFrame(
            {
                "person_category": [PersonCategory.WORKER, PersonCategory.WORKER],
                "_d_purpose_effective": [
                    PurposeCategory.WORK.value,
                    PurposeCategory.SOCIALREC.value,
                ],
                "_activity_duration": [304.0, 300.0],
            }
        )
        scored = add_purpose_score_column(df, default_config, alias="_s")
        work = scored.filter(pl.col("_d_purpose_effective") == PurposeCategory.WORK.value)["_s"][0]
        social = scored.filter(pl.col("_d_purpose_effective") == PurposeCategory.SOCIALREC.value)[
            "_s"
        ][0]
        assert work > social

    def test_person_category_changes_the_ranking(self, default_config):
        """A worker ranks work over school; a student ranks school over work."""
        df = pl.DataFrame(
            {
                "person_category": [
                    PersonCategory.WORKER,
                    PersonCategory.WORKER,
                    PersonCategory.STUDENT,
                    PersonCategory.STUDENT,
                ],
                "_d_purpose_effective": [
                    PurposeCategory.WORK.value,
                    PurposeCategory.SCHOOL.value,
                    PurposeCategory.WORK.value,
                    PurposeCategory.SCHOOL.value,
                ],
                # equal, typical duration so only the ceiling W decides
                "_activity_duration": [200.0, 200.0, 200.0, 200.0],
            }
        )
        scored = add_purpose_score_column(df, default_config, alias="_s")
        worker = scored.filter(pl.col("person_category") == PersonCategory.WORKER)
        student = scored.filter(pl.col("person_category") == PersonCategory.STUDENT)
        w_work = worker.filter(pl.col("_d_purpose_effective") == PurposeCategory.WORK.value)["_s"][
            0
        ]
        w_school = worker.filter(pl.col("_d_purpose_effective") == PurposeCategory.SCHOOL.value)[
            "_s"
        ][0]
        s_work = student.filter(pl.col("_d_purpose_effective") == PurposeCategory.WORK.value)["_s"][
            0
        ]
        s_school = student.filter(pl.col("_d_purpose_effective") == PurposeCategory.SCHOOL.value)[
            "_s"
        ][0]
        assert w_work > w_school
        assert s_school > s_work

    def test_overnight_scores_zero(self, default_config):
        """OVERNIGHT has ceiling 0, so it never outscores a real purpose."""
        df = pl.DataFrame(
            {
                "person_category": [PersonCategory.WORKER, PersonCategory.WORKER],
                "_d_purpose_effective": [
                    PurposeCategory.OVERNIGHT.value,
                    PurposeCategory.SHOP.value,
                ],
                "_activity_duration": [600.0, 20.0],
            }
        )
        scored = add_purpose_score_column(df, default_config, alias="_s")
        overnight = scored.filter(
            pl.col("_d_purpose_effective") == PurposeCategory.OVERNIGHT.value
        )["_s"][0]
        shop = scored.filter(pl.col("_d_purpose_effective") == PurposeCategory.SHOP.value)["_s"][0]
        assert overnight == 0.0
        assert shop > overnight

    def test_unmapped_purpose_scores_null(self, default_config):
        """A purpose with no weight (HOME) gets a null score and never wins."""
        df = _score_trips(PersonCategory.WORKER, PurposeCategory.HOME.value, [120])
        score = add_purpose_score_column(df, default_config, alias="_s")["_s"][0]
        assert score is None
