"""A column named by config is still constrained.

A declared field carries its bounds in the model and pydantic enforces them row by
row. A generated column has no field to hang them on, so before this it was
delivered unchecked -- and the weight columns are all generated now, since a run
names them after the profile it fitted. A negative weight would have been written
out in silence.
"""

import polars as pl
import pytest

from data_canon.core.dataclass import CanonicalData
from data_canon.core.exceptions import DataValidationError
from data_canon.validation.column import GeneratedColumn, check_generated_constraints
from processing.weighting.core.hierarchy import describe_weight_columns

# Enough of a household to satisfy the declared fields, so the only thing under
# test is the generated column beside them.
HOUSEHOLD = {
    "hh_id": [1],
    "home_lat": [37.8],
    "home_lon": [-122.3],
    "residence_rent_own": [1],
    "residence_type": [1],
    "income_bin": [1],
    "num_vehicles": [1],
    "complete": [True],
}


class TestTheCheck:
    """Bounds are checked over the column, since there is no field to check per row."""

    def test_a_value_below_the_bound_raises(self):
        """The case that matters: a weight that expands a record negatively."""
        df = pl.DataFrame({"w": [1.0, -0.5]})
        with pytest.raises(DataValidationError, match="below the declared minimum"):
            check_generated_constraints("households", df, {"w": GeneratedColumn(ge=0)})

    def test_the_message_says_how_many_and_how_low(self):
        """A count and the worst value, so the size of the problem is visible."""
        df = pl.DataFrame({"w": [-3.0, -1.0, 2.0]})
        with pytest.raises(DataValidationError, match="below the declared minimum") as excinfo:
            check_generated_constraints("households", df, {"w": GeneratedColumn(ge=0)})
        message = str(excinfo.value)
        assert "2 of 3" in message
        assert "-3.0" in message

    def test_values_at_the_bound_pass(self):
        """Zero is a legitimate weight: the record was excluded, not corrupted."""
        check_generated_constraints(
            "households", pl.DataFrame({"w": [0.0, 1.0]}), {"w": GeneratedColumn(ge=0)}
        )

    def test_a_column_with_no_bound_is_not_checked(self):
        """Most generated columns are zone ids and flags, with nothing to bound."""
        check_generated_constraints(
            "households", pl.DataFrame({"w": [-1.0]}), {"w": GeneratedColumn("a description")}
        )

    def test_an_absent_column_is_not_a_violation(self):
        """A step registers what it can produce; a run may produce fewer."""
        check_generated_constraints(
            "households", pl.DataFrame({"other": [1]}), {"w": GeneratedColumn(ge=0)}
        )

    def test_nulls_are_not_below_the_bound(self):
        """Null means no weight was estimated, which the bound has no opinion on."""
        check_generated_constraints(
            "households", pl.DataFrame({"w": [None, 1.0]}), {"w": GeneratedColumn(ge=0)}
        )


class TestThroughValidate:
    """The check has to be wired into validation, not merely available."""

    def test_a_negative_registered_weight_fails_validation(self):
        """End to end through validate(), not just the helper in isolation."""
        canon = CanonicalData(households=pl.DataFrame({**HOUSEHOLD, "hh_weight_p": [-5.0]}))
        canon.register_generated_columns(
            "households", {"hh_weight_p": GeneratedColumn("a weight", ge=0)}
        )
        with pytest.raises(DataValidationError, match="hh_weight_p"):
            canon.validate("households")

    def test_an_unregistered_column_is_nobody_s_promise(self):
        """Which is why the weighting has to register what it writes."""
        canon = CanonicalData(households=pl.DataFrame({**HOUSEHOLD, "hh_weight_p": [-5.0]}))
        canon.validate("households")

    def test_a_description_only_registration_still_works(self):
        """Every existing caller passes names or descriptions, and must keep working."""
        canon = CanonicalData(households=pl.DataFrame({**HOUSEHOLD, "home_taz": [7]}))
        canon.register_generated_columns("households", {"home_taz": "the home zone"})
        canon.validate("households")
        assert canon.describe_generated("households") == {"home_taz": "the home zone"}

    def test_a_bare_iterable_registration_still_works(self):
        """Names with no promise at all, as add_zone_ids passes them."""
        canon = CanonicalData(households=pl.DataFrame({**HOUSEHOLD, "home_taz": [7]}))
        canon.register_generated_columns("households", ["home_taz"])
        assert canon.public_columns("households") >= {"home_taz"}


class TestWhatTheWeightingPromises:
    """The weight columns must arrive with the bound the deleted fields carried."""

    @pytest.mark.parametrize("profile", [None, "ctramp_usable"])
    def test_every_weight_column_is_bounded_at_zero(self, profile):
        """Including the un-suffixed spelling, which is a profile's weights too."""
        described = describe_weight_columns((profile,))
        assert described, "no columns described"
        for table, columns in described.items():
            for column, spec in columns.items():
                assert spec.ge == 0, f"{table}.{column} carries no lower bound"

    def test_every_weight_column_says_what_it_counts(self):
        """The text the declared fields used to carry to the codebook."""
        for columns in describe_weight_columns(("ctramp_usable",)).values():
            for column, spec in columns.items():
                assert spec.description, f"{column} has no description"

    def test_the_joint_overlay_warning_survived(self):
        """The one description a consumer gets wrong without being told.

        Summing a joint table and its member table double counts, because the
        joint levels are an overlay carrying person-trips, not a partition.
        """
        described = describe_weight_columns(("ctramp_usable",))
        joint = described["joint_trips"]["joint_trip_weight_ctramp_usable"].description
        assert "OVERLAYS" in joint
        assert "double counts" in joint
