"""Tests for tour priority calculation utilities."""

import polars as pl
import pytest

from data_canon.codebook.tours import PersonCategory
from data_canon.codebook.trips import ModeType, PurposeCategory
from processing.tours.priority_utils import (
    add_mode_priority_column,
    add_purpose_priority_column,
    add_purpose_score_column,
)
from tests.fixtures.tour_pipeline import tour_config


@pytest.fixture
def default_config():
    """Tour rules as the BATS configs state them."""
    return tour_config()


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
        # HOME never needs a priority, so it sorts last
        assert (
            result.filter(pl.col("d_purpose_category") == PurposeCategory.HOME.value)[
                "purpose_priority"
            ][0]
            == 999
        )


class TestAddModePriorityColumn:
    """Test add_mode_priority_column function."""

    def test_adds_mode_priority_column(self):
        """Later in the hierarchy means higher priority."""
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

        # Priority is the mode's index in the hierarchy
        assert result["mode_priority"].to_list() == [3, 2, 0]


def _score_trips(person_category, purpose_category, durations):
    """One trip per duration, all same person category and purpose."""
    n = len(durations)
    return pl.DataFrame(
        {
            "person_category": [person_category] * n,
            "d_purpose_category": [purpose_category] * n,
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

    @pytest.mark.parametrize(
        ("purpose_a", "duration_a", "purpose_b", "duration_b", "winner"),
        [
            # Mandatory purposes are sticky (low h), but only above the
            # establishment threshold; a trivial pass-by falls below it.
            pytest.param(
                PurposeCategory.WORK,
                5.0,
                PurposeCategory.SHOP,
                240.0,
                PurposeCategory.SHOP,
                id="trivial_mandatory_driveby_is_overridden",
            ),
            # The stickiness the scoring is calibrated for: any real mandatory
            # visit wins, so short work stops stay work rather than becoming
            # discretionary.
            pytest.param(
                PurposeCategory.WORK,
                30.0,
                PurposeCategory.SHOP,
                240.0,
                PurposeCategory.WORK,
                id="modest_mandatory_is_sticky_over_long_discretionary",
            ),
            # Escort scores below any real activity, so escort + shop is a shop
            # tour even when the escorting took four times as long.
            pytest.param(
                PurposeCategory.ESCORT,
                120.0,
                PurposeCategory.SHOP,
                30.0,
                PurposeCategory.SHOP,
                id="pure_escort_wins_but_escort_with_activity_does_not",
            ),
            pytest.param(
                PurposeCategory.WORK,
                304.0,
                PurposeCategory.SOCIALREC,
                300.0,
                PurposeCategory.WORK,
                id="typical_mandatory_outscores_long_discretionary",
            ),
            pytest.param(
                PurposeCategory.OVERNIGHT,
                600.0,
                PurposeCategory.SHOP,
                20.0,
                PurposeCategory.SHOP,
                id="overnight_has_ceiling_zero_and_never_wins",
            ),
        ],
    )
    def test_the_calibrated_ranking(
        self, default_config, purpose_a, duration_a, purpose_b, duration_b, winner
    ):
        """The calibration contract: which of two competing stops names the tour."""
        df = pl.DataFrame(
            {
                "person_category": [PersonCategory.WORKER, PersonCategory.WORKER],
                "d_purpose_category": [purpose_a.value, purpose_b.value],
                "_activity_duration": [duration_a, duration_b],
            }
        )

        scored = add_purpose_score_column(df, default_config, alias="_s")
        scores = dict(zip(scored["d_purpose_category"], scored["_s"], strict=True))

        loser = purpose_b if winner is purpose_a else purpose_a
        assert scores[winner.value] > scores[loser.value]

    def test_overnight_scores_zero(self, default_config):
        """OVERNIGHT has ceiling 0, so its score is zero at any duration."""
        df = _score_trips(PersonCategory.WORKER, PurposeCategory.OVERNIGHT.value, [600])
        assert add_purpose_score_column(df, default_config, alias="_s")["_s"][0] == 0.0

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
                "d_purpose_category": [
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
        w_work, w_school, s_work, s_school = scored["_s"].to_list()

        assert w_work > w_school
        assert s_school > s_work

    def test_unmapped_purpose_scores_null(self, default_config):
        """A purpose with no weight (HOME) gets a null score and never wins."""
        df = _score_trips(PersonCategory.WORKER, PurposeCategory.HOME.value, [120])
        score = add_purpose_score_column(df, default_config, alias="_s")["_s"][0]
        assert score is None
