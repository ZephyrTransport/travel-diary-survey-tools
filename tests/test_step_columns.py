"""Columns named from a step's settings, declared with ``@step(generated_columns=...)``."""

import polars as pl

from data_canon.core.dataclass import CanonicalData
from pipeline.decoration import Tables, step
from processing.add_zone_ids.add_zone_ids import zone_columns
from processing.imputation.impute import preimputed_columns
from processing.usability.descriptions import profile_columns
from processing.weighting.compute_weights import weight_columns

ALL_DAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
PROFILE = {
    "tour_closes_at": "primary_home",
    "household_day_needs": "all_members",
    "zone_coverage": "none",
    "joint_tours": "partial",
    "travel_days": ALL_DAYS,
}


def _zone_step(calls: list[str]):
    """A step that adds one zone column, recording what was registered when its body ran."""

    def columns(zone_name: str) -> dict[str, list[str]]:
        return {"households": [f"home_{zone_name}"]}

    @step(validate_input=False, requires={"households": {"hh_id"}}, generated_columns=columns)
    def zone_step(
        tables: Tables, zone_name: str, canonical_data: CanonicalData
    ) -> dict[str, pl.DataFrame]:
        calls.append(sorted(canonical_data.generated_columns.get("households", {})))
        households = tables["households"]
        return {"households": households.with_columns(pl.lit(1).alias(f"home_{zone_name}"))}

    return zone_step


class TestDecoratorRegisters:
    """The decorator registers a step's columns from its settings."""

    HOUSEHOLDS = pl.DataFrame({"hh_id": [1, 2]})

    def test_registered_before_the_body_runs(self):
        """The body already sees its columns registered."""
        calls: list[list[str]] = []
        data = CanonicalData()
        _zone_step(calls)(households=self.HOUSEHOLDS, zone_name="taz", canonical_data=data)
        assert calls == [["home_taz"]]


class TestEachStepListsItsColumns:
    """Each step's function names what the step writes, from its own settings."""

    def test_zone_columns(self):
        """One column per zone geography, at every location the step zones."""
        got = zone_columns([{"zone_name": "taz"}, {"zone_name": "maz"}])
        assert got["households"] == ["home_taz", "home_maz"]
        assert got["persons"] == ["work_taz", "school_taz", "work_maz", "school_maz"]
        assert got["habitual_locations"] == ["taz", "maz"]

    def test_profile_columns(self):
        """Each profile's flag and joint ids, described; household_days gets only the flag."""
        got = profile_columns({"ctramp": PROFILE, "analysis": PROFILE})
        assert {"usable_ctramp", "usable_analysis", "joint_tour_id_ctramp"} <= set(got["tours"])
        assert set(got["household_days"]) == {"usable_ctramp", "usable_analysis"}
        assert all(got["households"].values())  # every column carries a description

    def test_profile_columns_without_profiles(self):
        """Nothing to declare; the step itself stops on the missing setting."""
        assert profile_columns(None) == {}

    def test_preimputed_columns(self):
        """One stash per imputed column, MICE groups included; none when not stashing."""
        impute = {
            "persons": [{"column": "gender"}, {"columns": ["age", "employment"]}],
            "households": [{"column": "income_bin"}],
        }
        assert preimputed_columns(impute) == {
            "persons": ["gender_preimputed", "age_preimputed", "employment_preimputed"],
            "households": ["income_bin_preimputed"],
        }
        assert preimputed_columns(impute, stash_preimputed=False) == {}

    def test_weight_columns(self):
        """Each profile's suffixed weights, at every level."""
        got = weight_columns(["ctramp", "analysis"])
        assert {"hh_weight_ctramp", "hh_weight_analysis"} <= set(got["households"])
        assert "tour_weight_ctramp" in got["tours"]
