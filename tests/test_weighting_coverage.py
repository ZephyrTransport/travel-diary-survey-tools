"""The bound on what the weighting can answer: households it cannot place.

Two region tests exist and are allowed to disagree. The model zones a usability
profile reads are assigned with a snap tolerance; the control geography is a
strict point-in-polygon. A household that passes the first and fails the second is
admitted by the profile and belongs to no balancing zone, so no fit can weight it.

Left alone that produces a column of nulls and no explanation. These tests pin
that it is counted, and that a share large enough to mean a misconfigured
geography stops the run rather than being absorbed.
"""

import polars as pl
import pytest

from processing.weighting.validation.coverage import check_control_geography_coverage


def _seed(placed: int, unplaceable: int) -> pl.DataFrame:
    """A seed with the given split of placed and unplaceable households."""
    n = placed + unplaceable
    return pl.DataFrame(
        {
            "hh_id": list(range(1, n + 1)),
            "ctrl_geoid": ["06001"] * placed + [None] * unplaceable,
        }
    )


class TestCounting:
    """The counts are what turn a column of nulls into a statement."""

    def test_a_fully_placed_seed_reports_none_unplaceable(self):
        """The ordinary case, and the one measured on both real surveys."""
        coverage = check_control_geography_coverage(
            _seed(10, 0), profile="p", max_unplaceable_share=0.01
        )
        assert coverage.n_universe == 10
        assert coverage.n_placed == 10
        assert coverage.n_unplaceable == 0
        assert coverage.unplaceable_share == 0.0

    def test_unplaceable_households_are_counted_not_dropped_silently(self):
        """The count is the report; the caller is what removes them from the seed."""
        coverage = check_control_geography_coverage(
            _seed(9, 1), profile="p", max_unplaceable_share=0.5
        )
        assert coverage.n_unplaceable == 1
        assert coverage.unplaceable_share == pytest.approx(0.1)

    def test_the_profile_is_carried_on_the_result(self):
        """A report covering several fits has to say which one each row is."""
        assert (
            check_control_geography_coverage(
                _seed(1, 0), profile="analysis_usable", max_unplaceable_share=0.01
            ).profile
            == "analysis_usable"
        )

    def test_an_empty_seed_has_no_share_rather_than_dividing_by_zero(self):
        """A profile that admits nothing is a different complaint, made elsewhere."""
        coverage = check_control_geography_coverage(
            _seed(0, 0), profile="p", max_unplaceable_share=0.0
        )
        assert coverage.unplaceable_share == 0.0

    def test_a_seed_with_no_geography_column_places_nothing(self):
        """Rather than reading as fully placed, which would hide the whole problem."""
        seed = pl.DataFrame({"hh_id": [1, 2]})
        coverage = check_control_geography_coverage(seed, profile="p", max_unplaceable_share=1.0)
        assert coverage.n_placed == 0
        assert coverage.n_unplaceable == 2


class TestTheTolerance:
    """A boundary effect is expected; a geography that does not cover the survey is not."""

    def test_a_share_above_the_tolerance_raises(self):
        """Half the survey outside the region is a geography, not a boundary."""
        with pytest.raises(ValueError, match="have no control geography"):
            check_control_geography_coverage(_seed(5, 5), profile="p", max_unplaceable_share=0.01)

    def test_the_message_names_both_geographies(self):
        """So the reader knows which of the two layers to go and look at."""
        with pytest.raises(ValueError, match="no control geography") as excinfo:
            check_control_geography_coverage(_seed(1, 9), profile="p", max_unplaceable_share=0.01)
        message = str(excinfo.value)
        assert "point-in-polygon" in message
        assert "snap tolerance" in message
        assert "max_unplaceable_share" in message

    def test_a_share_at_the_tolerance_is_allowed(self):
        """The tolerance is what is tolerated, not the first value rejected."""
        coverage = check_control_geography_coverage(
            _seed(99, 1), profile="p", max_unplaceable_share=0.01
        )
        assert coverage.n_unplaceable == 1

    def test_the_classification_follows_the_polygon_not_the_model_zone(self):
        """The two tests disagree by design, and this one is about the control side.

        A household with a model zone but no control geography is unplaceable, and
        one placed by the polygons is placed whatever its model zone says.
        """
        seed = pl.DataFrame(
            {
                "hh_id": [1, 2],
                "ctrl_geoid": [None, "06001"],
                "taz": [42, None],
            }
        )
        coverage = check_control_geography_coverage(seed, profile="p", max_unplaceable_share=0.6)
        assert coverage.n_unplaceable == 1
        assert coverage.n_placed == 1
