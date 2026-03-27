"""Test enum helpers utility."""

import polars as pl
import pytest

from data_canon.codebook.households import IncomeBroad, ResidenceRentOwn
from data_canon.codebook.persons import Gender
from processing.imputation.impute_utils import prepare_column_for_imputation
from utils.enum_helpers import (
    get_enum_class_for_field,
    resolve_enum_labels,
)


class TestGetEnumClassForField:
    """Tests for discovering the enum class attached to a model field."""

    def test_direct_enum_field(self):
        """Enum field without Optional wrapper returns the enum class."""
        enum_class = get_enum_class_for_field("persons", "gender")
        assert enum_class is Gender

    def test_optional_enum_field(self):
        """Optional[Enum] field still returns the underlying enum class."""
        enum_class = get_enum_class_for_field("households", "residence_rent_own")
        assert enum_class is ResidenceRentOwn

    def test_income_broad_field(self):
        """IncomeBroad enum is resolved for income_bin field."""
        enum_class = get_enum_class_for_field("households", "income_bin")
        assert enum_class is IncomeBroad

    def test_non_enum_field_raises(self):
        """Non-enum fields (e.g., float, int) raise ValueError."""
        with pytest.raises(ValueError, match="No enum class found"):
            get_enum_class_for_field("households", "home_lat")

    def test_unknown_table_raises(self):
        """Unknown table names raise ValueError."""
        with pytest.raises(ValueError, match="Unknown table name"):
            get_enum_class_for_field("nonexistent_table", "some_field")

    def test_unknown_field_raises(self):
        """Unknown field names raise ValueError."""
        with pytest.raises(ValueError, match="not found in model"):
            get_enum_class_for_field("persons", "nonexistent_field")


class TestResolveEnumLabels:
    """Tests for resolving enum member names to their integer values."""

    def test_resolve_single_label(self):
        """Resolving a single enum label returns its value."""
        values = resolve_enum_labels("persons", "gender", ["FEMALE"])
        assert values == [Gender.FEMALE.value]

    def test_resolve_multiple_labels(self):
        """Resolving multiple labels returns all values in order."""
        values = resolve_enum_labels("persons", "gender", ["MISSING", "PNTA"])
        assert values == [995, 999]

    def test_resolve_income_broad_labels(self):
        """Resolving IncomeBroad labels returns correct values."""
        values = resolve_enum_labels("households", "income_bin", ["MISSING", "PNTA"])
        assert values == [IncomeBroad.MISSING.value, IncomeBroad.PNTA.value]

    def test_resolve_household_enum(self):
        """Resolving works across different table/model types."""
        values = resolve_enum_labels("households", "residence_rent_own", ["OWN", "RENT"])
        assert values == [
            ResidenceRentOwn.OWN.value,
            ResidenceRentOwn.RENT.value,
        ]

    def test_bad_label_raises(self):
        """An invalid enum label raises ValueError."""
        with pytest.raises(ValueError, match="not found in enum"):
            resolve_enum_labels("persons", "gender", ["INVALID_LABEL"])

    def test_no_enum_raises(self):
        """Resolving labels for a non-enum field raises ValueError."""
        with pytest.raises(ValueError, match="No enum class found"):
            resolve_enum_labels("households", "home_lat", ["SOMETHING"])


class TestPrepareColumnForImputation:
    """Tests for preparing columns for imputation."""

    def test_prepare_income_column(self):
        """Test preparing income_bin column replaces MISSING/PNTA with null."""
        df = pl.DataFrame(
            {
                "hh_id": [1, 2, 3, 4, 5],
                "income_bin": [1, 2, 995, 999, 3],  # 995=MISSING, 999=PNTA
            }
        )

        df_prepared, resolved_values = prepare_column_for_imputation(
            df, "households", "income_bin", ["MISSING", "PNTA"]
        )

        assert resolved_values == [
            IncomeBroad.MISSING.value,
            IncomeBroad.PNTA.value,
        ]

        income_col = df_prepared["income_bin"]
        assert income_col[0] == 1
        assert income_col[1] == 2
        assert income_col[2] is None  # Was 995 (MISSING)
        assert income_col[3] is None  # Was 999 (PNTA)
        assert income_col[4] == 3
