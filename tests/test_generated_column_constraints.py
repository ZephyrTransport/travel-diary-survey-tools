"""A generated column (named by config, not a model field) is checked against its bounds."""

import polars as pl
import pytest

from data_canon.core.dataclass import CanonicalData
from data_canon.core.exceptions import DataValidationError
from data_canon.validation.column import GeneratedColumn, check_generated_constraints
from processing.weighting.core.hierarchy import describe_weight_columns

# Enough of a household to pass the declared fields.
HOUSEHOLD = {
    "hh_id": [1],
    "home_lat": [37.8],
    "home_lon": [-122.3],
    "residence_rent_own": [1],
    "residence_type": [1],
    "income_bin": [1],
    "num_vehicles": [1],
    "survey_complete": [True],
}


class TestTheCheck:
    """Bounds are checked over the column, since there is no field to check per row."""

    def test_a_value_below_the_bound_raises(self):
        """A value below ``ge`` raises with the count and the worst value."""
        df = pl.DataFrame({"w": [-3.0, -1.0, 2.0]})
        with pytest.raises(DataValidationError, match="below the declared minimum") as excinfo:
            check_generated_constraints("households", df, {"w": GeneratedColumn(ge=0)})

        message = str(excinfo.value)
        assert "2 of 3" in message
        assert "-3.0" in message

    @pytest.mark.parametrize(
        ("df", "spec"),
        [
            pytest.param(pl.DataFrame({"w": [0.0, 1.0]}), GeneratedColumn(ge=0), id="at-the-bound"),
            pytest.param(
                pl.DataFrame({"w": [-1.0]}), GeneratedColumn("a description"), id="no-bound"
            ),
            pytest.param(pl.DataFrame({"other": [1]}), GeneratedColumn(ge=0), id="column-absent"),
            pytest.param(pl.DataFrame({"w": [None, 1.0]}), GeneratedColumn(ge=0), id="nulls"),
        ],
    )
    def test_what_does_not_violate_the_bound(self, df: pl.DataFrame, spec: GeneratedColumn):
        """At the bound, no bound, column absent or null: nothing is raised."""
        check_generated_constraints("households", df, {"w": spec})


class TestThroughValidate:
    """``CanonicalData.validate`` runs the check on registered columns."""

    def test_a_negative_registered_weight_fails_validation(self):
        """A registered weight below its bound fails validation."""
        canon = CanonicalData(households=pl.DataFrame({**HOUSEHOLD, "hh_weight_p": [-5.0]}))
        canon.register_generated_columns(
            "households", {"hh_weight_p": GeneratedColumn("a weight", ge=0)}
        )
        with pytest.raises(DataValidationError, match="hh_weight_p"):
            canon.validate("households")

    def test_an_unregistered_column_is_nobody_s_promise(self):
        """An unregistered column is not checked."""
        canon = CanonicalData(households=pl.DataFrame({**HOUSEHOLD, "hh_weight_p": [-5.0]}))
        canon.validate("households")

    def test_a_description_only_registration_still_works(self):
        """A description string registers a column with that description and no bound."""
        canon = CanonicalData(households=pl.DataFrame({**HOUSEHOLD, "home_taz": [7]}))
        canon.register_generated_columns("households", {"home_taz": "the home zone"})
        canon.validate("households")
        assert canon.generated_columns["households"]["home_taz"].description == "the home zone"

    def test_a_bare_iterable_registration_still_works(self):
        """A list of names registers the columns as public."""
        canon = CanonicalData(households=pl.DataFrame({**HOUSEHOLD, "home_taz": [7]}))
        canon.register_generated_columns("households", ["home_taz"])
        assert canon.public_columns("households") >= {"home_taz"}


class TestWhatTheWeightingPromises:
    """``describe_weight_columns`` bounds and describes every weight column."""

    @pytest.mark.parametrize("profile", [None, "ctramp"])
    def test_every_weight_column_is_bounded_at_zero(self, profile):
        """Every weight column, suffixed or not, has ``ge=0``."""
        described = describe_weight_columns((profile,))
        assert described, "no columns described"
        for table, columns in described.items():
            for column, spec in columns.items():
                assert spec.ge == 0, f"{table}.{column} carries no lower bound"

    def test_every_weight_column_says_what_it_counts(self):
        """Every weight column has a description; the joint ones warn against double counting."""
        described = describe_weight_columns(("ctramp",))
        for columns in described.values():
            for column, spec in columns.items():
                assert spec.description, f"{column} has no description"

        joint = described["joint_trips"]["joint_trip_weight_ctramp"].description
        assert "OVERLAYS" in joint
        assert "double counts" in joint
