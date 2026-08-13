"""Tests for add_existing_weights function."""

import polars as pl
import pytest
from pydantic import ValidationError

from data_canon.codebook.persons import AgeCategory
from processing.weighting.existing_weights import add_existing_weights


class TestAddExistingWeights:
    """Test add_existing_weights function."""

    def test_load_single_weight_file(self, tmp_path):
        """Test loading weights from a single file."""
        # Create test household data
        households = pl.DataFrame(
            {
                "hh_id": [1, 2, 3],
                "hh_size": [2, 3, 1],
            }
        )

        # Create test weight file
        weight_file = tmp_path / "hh_weights.csv"
        weights_df = pl.DataFrame(
            {
                "hh_id": [1, 2, 3],
                "hh_weight": [1.5, 2.0, 1.0],
            }
        )
        weights_df.write_csv(weight_file)

        # Load weights
        weights_config = {
            "hh_weight": {
                "weight_path": str(weight_file),
            }
        }

        result = add_existing_weights(
            weights=weights_config,
            households=households,
        )

        assert "households" in result
        assert "hh_weight" in result["households"].columns
        assert result["households"]["hh_weight"].to_list() == [1.5, 2.0, 1.0]

    def test_custom_id_column_names(self, tmp_path):
        """Test using custom ID column names in table and weight file."""
        # Create test data with custom ID column
        persons = pl.DataFrame(
            {
                "person_id": [1, 2, 3],
                "age": [
                    AgeCategory.AGE_25_TO_34.value,
                    AgeCategory.AGE_35_TO_44.value,
                    AgeCategory.AGE_45_TO_54.value,
                ],
            }
        )

        # Weight file uses different ID column name
        weight_file = tmp_path / "person_weights.csv"
        weights_df = pl.DataFrame(
            {
                "pid": [1, 2, 3],  # Different column name
                "person_weight": [1.2, 1.5, 1.8],
            }
        )
        weights_df.write_csv(weight_file)

        weights_config = {
            "person_weight": {
                "weight_path": str(weight_file),
                "weight_id_col": "pid",
            }
        }

        result = add_existing_weights(
            weights=weights_config,
            persons=persons,
        )

        assert "persons" in result
        assert "person_weight" in result["persons"].columns
        assert result["persons"]["person_weight"].to_list() == [1.2, 1.5, 1.8]

    def test_custom_weight_column_name(self, tmp_path):
        """Test using custom weight column name."""
        trips = pl.DataFrame(
            {
                "unlinked_trip_id": [1, 2, 3],
                "mode": ["car", "walk", "transit"],
            }
        )

        weight_file = tmp_path / "trip_weights.csv"
        weights_df = pl.DataFrame(
            {
                "unlinked_trip_id": [1, 2, 3],
                "wt": [2.0, 1.5, 1.8],  # Custom weight column name
            }
        )
        weights_df.write_csv(weight_file)

        weights_config = {
            "unlinked_trip_weight": {
                "weight_path": str(weight_file),
                "weight_col": "wt",
                "keep_name": True,
            }
        }

        result = add_existing_weights(
            weights=weights_config,
            unlinked_trips=trips,
        )

        assert "unlinked_trips" in result
        assert "wt" in result["unlinked_trips"].columns
        assert result["unlinked_trips"]["wt"].to_list() == [2.0, 1.5, 1.8]

    def test_derive_person_weights_from_household(self, tmp_path):
        """Test deriving person weights from household weights."""
        households = pl.DataFrame(
            {
                "hh_id": [1, 2],
                "hh_size": [2, 1],
            }
        )

        persons = pl.DataFrame(
            {
                "person_id": [1, 2, 3],
                "hh_id": [1, 1, 2],
                "age": [
                    AgeCategory.AGE_25_TO_34.value,
                    AgeCategory.AGE_25_TO_34.value,
                    AgeCategory.AGE_45_TO_54.value,
                ],
            }
        )

        # Only provide household weights
        hh_weight_file = tmp_path / "hh_weights.csv"
        hh_weights_df = pl.DataFrame(
            {
                "hh_id": [1, 2],
                "hh_weight": [1.5, 2.0],
            }
        )
        hh_weights_df.write_csv(hh_weight_file)

        weights_config = {
            "hh_weight": {
                "weight_path": str(hh_weight_file),
            }
        }

        result = add_existing_weights(
            weights=weights_config,
            households=households,
            persons=persons,
            derive_missing_weights=True,
        )

        # Check households have weights
        assert "hh_weight" in result["households"].columns
        assert result["households"]["hh_weight"].to_list() == [1.5, 2.0]

        # Check persons derived weights from households
        assert "person_weight" in result["persons"].columns
        assert result["persons"]["person_weight"].to_list() == [1.5, 1.5, 2.0]

    def test_derive_aggregated_weights(self, tmp_path):
        """Test deriving linked trip weights from unlinked trips."""
        unlinked_trips = pl.DataFrame(
            {
                "unlinked_trip_id": [1, 2, 3, 4],
                "linked_trip_id": [1, 1, 2, 2],
                "unlinked_trip_weight": [1.0, 2.0, 3.0, 1.0],
            }
        )

        linked_trips = pl.DataFrame(
            {
                "linked_trip_id": [1, 2],
                "mode": ["car", "transit"],
            }
        )

        # Provide unlinked trip weights (already in data)
        # We need to create a dummy weight file to trigger the weight loading
        weight_file = tmp_path / "trip_weights.csv"
        weights_df = pl.DataFrame(
            {
                "unlinked_trip_id": [1, 2, 3, 4],
                "unlinked_trip_weight": [1.0, 2.0, 3.0, 1.0],
            }
        )
        weights_df.write_csv(weight_file)

        weights_config = {
            "unlinked_trip_weight": {
                "weight_path": str(weight_file),
            }
        }

        result = add_existing_weights(
            weights=weights_config,
            unlinked_trips=unlinked_trips,
            linked_trips=linked_trips,
            derive_missing_weights=True,
        )

        # Check linked trips have derived weights (mean of component trips, excluding zeros)
        assert "linked_trip_weight" in result["linked_trips"].columns
        # Linked trip 1: mean(1.0, 2.0) = 1.5
        # Linked trip 2: mean(3.0, 1.0) = 2.0
        assert result["linked_trips"]["linked_trip_weight"].to_list() == [1.5, 2.0]

    def test_exclude_zeros_and_nulls_from_aggregation(self, tmp_path):
        """Test that zeros and nulls are excluded from mean aggregation."""
        unlinked_trips = pl.DataFrame(
            {
                "unlinked_trip_id": [1, 2, 3, 4],
                "linked_trip_id": [1, 1, 2, 2],
                "unlinked_trip_weight": [1.0, 0.0, 3.0, None],  # Zero and null
            }
        )

        linked_trips = pl.DataFrame(
            {
                "linked_trip_id": [1, 2],
            }
        )

        weight_file = tmp_path / "trip_weights.csv"
        weights_df = pl.DataFrame(
            {
                "unlinked_trip_id": [1, 2, 3, 4],
                "unlinked_trip_weight": [1.0, 0.0, 3.0, None],
            }
        )
        weights_df.write_csv(weight_file)

        weights_config = {
            "unlinked_trip_weight": {
                "weight_path": str(weight_file),
            }
        }

        result = add_existing_weights(
            weights=weights_config,
            unlinked_trips=unlinked_trips,
            linked_trips=linked_trips,
            derive_missing_weights=True,
        )

        # Linked trip 1: mean(1.0) = 1.0 (zero excluded)
        # Linked trip 2: mean(3.0) = 3.0 (null excluded)
        assert result["linked_trips"]["linked_trip_weight"].to_list() == [1.0, 3.0]

    def test_error_on_invalid_config_key(self, tmp_path):
        """Test that invalid config keys raise an error."""
        households = pl.DataFrame(
            {
                "hh_id": [1, 2],
            }
        )

        weight_file = tmp_path / "weights.csv"
        pl.DataFrame({"hh_id": [1, 2], "weight": [1.0, 2.0]}).write_csv(weight_file)

        weights_config = {
            "invalid_key": {  # Invalid key
                "weight_path": str(weight_file),
            }
        }

        with pytest.raises(ValueError, match="Invalid weight config key"):
            add_existing_weights(
                weights=weights_config,
                households=households,
            )

    def test_error_on_missing_weight_path(self):
        """Test that missing weight_path raises an error."""
        households = pl.DataFrame(
            {
                "hh_id": [1, 2],
            }
        )

        weights_config = {
            "hh_weight": {
                # Missing weight_path - Pydantic will catch this
            }
        }

        with pytest.raises(ValidationError):
            add_existing_weights(
                weights=weights_config,
                households=households,
            )

    def test_error_on_nonexistent_file(self):
        """Test that nonexistent weight file raises FileNotFoundError."""
        households = pl.DataFrame(
            {
                "hh_id": [1, 2],
            }
        )

        weights_config = {
            "hh_weight": {
                "weight_path": "/nonexistent/path/to/weights.csv",
            }
        }

        with pytest.raises(FileNotFoundError, match="Weight file does not exist"):
            add_existing_weights(
                weights=weights_config,
                households=households,
            )

    def test_error_on_missing_id_column_in_weight_file(self, tmp_path):
        """Test error when weight file is missing the ID column."""
        households = pl.DataFrame(
            {
                "hh_id": [1, 2],
            }
        )

        weight_file = tmp_path / "weights.csv"
        weights_df = pl.DataFrame(
            {
                "wrong_id": [1, 2],  # Wrong column name
                "hh_weight": [1.0, 2.0],
            }
        )
        weights_df.write_csv(weight_file)

        weights_config = {
            "hh_weight": {
                "weight_path": str(weight_file),
            }
        }

        with pytest.raises(ValueError, match="missing required ID column"):
            add_existing_weights(
                weights=weights_config,
                households=households,
            )

    def test_error_on_missing_weight_column(self, tmp_path):
        """Test error when weight file is missing the weight column."""
        households = pl.DataFrame(
            {
                "hh_id": [1, 2],
            }
        )

        weight_file = tmp_path / "weights.csv"
        weights_df = pl.DataFrame(
            {
                "hh_id": [1, 2],
                "wrong_weight": [1.0, 2.0],  # Wrong column name
            }
        )
        weights_df.write_csv(weight_file)

        weights_config = {
            "hh_weight": {
                "weight_path": str(weight_file),
            }
        }

        with pytest.raises(ValueError, match="missing required weight column"):
            add_existing_weights(
                weights=weights_config,
                households=households,
            )

    def test_multiple_tables_with_weights(self, tmp_path):
        """Test loading weights for multiple tables."""
        households = pl.DataFrame(
            {
                "hh_id": [1, 2],
            }
        )

        persons = pl.DataFrame(
            {
                "person_id": [1, 2, 3],
                "hh_id": [1, 1, 2],
            }
        )

        # Create weight files
        hh_weight_file = tmp_path / "hh_weights.csv"
        pl.DataFrame(
            {
                "hh_id": [1, 2],
                "hh_weight": [1.5, 2.0],
            }
        ).write_csv(hh_weight_file)

        person_weight_file = tmp_path / "person_weights.csv"
        pl.DataFrame(
            {
                "person_id": [1, 2, 3],
                "person_weight": [1.2, 1.3, 2.1],
            }
        ).write_csv(person_weight_file)

        weights_config = {
            "hh_weight": {
                "weight_path": str(hh_weight_file),
            },
            "person_weight": {
                "weight_path": str(person_weight_file),
            },
        }

        result = add_existing_weights(
            weights=weights_config,
            households=households,
            persons=persons,
        )

        assert "hh_weight" in result["households"].columns
        assert "person_weight" in result["persons"].columns
        assert result["households"]["hh_weight"].to_list() == [1.5, 2.0]
        assert result["persons"]["person_weight"].to_list() == [1.2, 1.3, 2.1]

    def test_warning_when_table_not_found(self, tmp_path, caplog):
        """Test that a warning is logged when weight file provided but table doesn't exist."""
        # No households provided
        weight_file = tmp_path / "hh_weights.csv"
        pl.DataFrame(
            {
                "hh_id": [1, 2],
                "hh_weight": [1.0, 2.0],
            }
        ).write_csv(weight_file)

        weights_config = {
            "hh_weight": {
                "weight_path": str(weight_file),
            }
        }

        result = add_existing_weights(
            weights=weights_config,
            households=None,  # Table not provided
        )

        # Should not raise error, just log warning
        assert "households" not in result
        assert "Weight file provided for households but table not found" in caplog.text

    def test_config_key_auto_inference(self, tmp_path):
        """Test that config_key is automatically inferred from dict key."""
        households = pl.DataFrame(
            {
                "hh_id": [1, 2],
            }
        )

        # Create weight file
        weight_file = tmp_path / "hh_weights.csv"
        pl.DataFrame(
            {
                "hh_id": [1, 2],
                "hh_weight": [1.5, 2.0],
            }
        ).write_csv(weight_file)

        # config_key will be inferred from the dict key "hh_weight"
        weights_config = {
            "hh_weight": {
                "weight_path": str(weight_file),
            }
        }

        result = add_existing_weights(
            weights=weights_config,
            households=households,
        )

        assert "hh_weight" in result["households"].columns
        assert result["households"]["hh_weight"].to_list() == [1.5, 2.0]

    def test_defaults_applied_correctly(self, tmp_path):
        """Test that default weight_id_col and weight_col are applied correctly."""
        persons = pl.DataFrame(
            {
                "person_id": [1, 2, 3],
            }
        )

        # Weight file with canonical column names
        weight_file = tmp_path / "person_weights.csv"
        pl.DataFrame(
            {
                "person_id": [1, 2, 3],  # Canonical ID column
                "person_weight": [1.2, 1.5, 1.8],  # Canonical weight column
            }
        ).write_csv(weight_file)

        # Only provide weight_path, let defaults fill in the rest
        weights_config = {
            "person_weight": {
                "weight_path": str(weight_file),
                # weight_id_col should default to "person_id"
                # weight_col should default to "person_weight"
            }
        }

        result = add_existing_weights(
            weights=weights_config,
            persons=persons,
        )

        assert "person_weight" in result["persons"].columns
        assert result["persons"]["person_weight"].to_list() == [1.2, 1.5, 1.8]


class TestSuppliedTotalPreserved:
    """Supplied weights are redistributed onto usable records, never shrunk.

    The vendor's anchor cannot be re-balanced from here -- their weights already
    sum to their population estimate -- so dropping records must leave each
    table's supplied total intact. These use ``complete`` rather than the
    default ``model_usable``, since the fixtures carry no tour structure.
    """

    def _households(self) -> pl.DataFrame:
        """Four households, one incomplete (hh 3)."""
        return pl.DataFrame(
            {
                "hh_id": [1, 2, 3, 4],
                "hh_size": [2, 3, 1, 2],
                "complete": [True, True, False, True],
            }
        )

    def _weights_config(self, tmp_path) -> dict:
        weight_file = tmp_path / "hh_weights.csv"
        pl.DataFrame({"hh_id": [1, 2, 3, 4], "hh_weight": [10.0, 20.0, 30.0, 40.0]}).write_csv(
            weight_file
        )
        return {"hh_weight": {"weight_path": str(weight_file)}}

    def test_supplied_household_total_is_preserved(self, tmp_path):
        """Households have no parent, so the supplied total is held by rescaling."""
        result = add_existing_weights(
            weights=self._weights_config(tmp_path),
            households=self._households(),
            usability_flag_col="complete",
        )
        weights = result["households"].sort("hh_id")["hh_weight"].to_list()
        # hh 3 (incomplete) stays 0; the supplied total of 100 is retained
        assert weights[2] == 0.0
        assert sum(weights) == pytest.approx(100.0)
        # Survivors scaled by 100/70, keeping their relative proportions
        scale = 100.0 / 70.0
        assert weights[0] == pytest.approx(10.0 * scale)
        assert weights[1] == pytest.approx(20.0 * scale)
        assert weights[3] == pytest.approx(40.0 * scale)

    def test_supplied_day_weight_is_conserved_within_the_person(self, tmp_path):
        """A supplied day weight moves to the *same person's* usable days.

        Day weights are conserved within the person -- never pooled across a
        household. Person 1's unusable day moves onto their own remaining day;
        person 2's days are untouched even though they share the household.
        """
        days = pl.DataFrame(
            {
                "day_id": [10, 20, 30, 40],
                "person_id": [1, 1, 2, 2],
                "hh_id": [1, 1, 1, 1],
                "complete": [True, False, True, True],
            }
        )
        weight_file = tmp_path / "day_weights.csv"
        pl.DataFrame({"day_id": [10, 20, 30, 40], "day_weight": [10.0, 10.0, 5.0, 5.0]}).write_csv(
            weight_file
        )

        result = add_existing_weights(
            weights={"day_weight": {"weight_path": str(weight_file)}},
            days=days,
            usability_flag_col="complete",
        )
        weights = result["days"].sort("day_id")["day_weight"].to_list()
        # Person 1: 20 supplied over one usable day; person 2: unchanged.
        assert weights == pytest.approx([20.0, 0.0, 5.0, 5.0])
        assert sum(weights) == pytest.approx(30.0)

    def test_missing_scope_column_raises(self, tmp_path):
        """Days without person_id cannot be conserved as declared, so this fails loudly."""
        days = pl.DataFrame({"day_id": [10, 20], "hh_id": [1, 1], "complete": [True, False]})
        weight_file = tmp_path / "day_weights.csv"
        pl.DataFrame({"day_id": [10, 20], "day_weight": [10.0, 10.0]}).write_csv(weight_file)

        with pytest.raises(ValueError, match="missing its scope column"):
            add_existing_weights(
                weights={"day_weight": {"weight_path": str(weight_file)}},
                days=days,
                usability_flag_col="complete",
            )

    def test_no_usable_record_is_safe(self, tmp_path):
        """If nothing is usable there is nowhere to put the weight; no error."""
        households = pl.DataFrame({"hh_id": [1, 2], "hh_size": [2, 3], "complete": [False, False]})
        weight_file = tmp_path / "hh_weights.csv"
        pl.DataFrame({"hh_id": [1, 2], "hh_weight": [10.0, 20.0]}).write_csv(weight_file)
        result = add_existing_weights(
            weights={"hh_weight": {"weight_path": str(weight_file)}},
            households=households,
            usability_flag_col="complete",
        )
        assert result["households"]["hh_weight"].to_list() == [0.0, 0.0]
