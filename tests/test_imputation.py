"""Tests for imputation module."""

import numpy as np
import polars as pl
import pytest

from processing.imputation.comparison import compare_imputation_methods
from processing.imputation.flags import stash_preimputed_column, stash_preimputed_columns
from processing.imputation.impute_utils import (
    build_feature_matrix,
    decode_dense_to_integer,
    encode_integer_categoricals,
    is_categorical,
)
from processing.imputation.knn import impute_knn
from processing.imputation.mice import impute_mice
from processing.imputation.random_forest import impute_random_forest
from processing.imputation.validation import (
    validate_knn_imputation,
    validate_mice_imputation,
    validate_rf_imputation,
)


class TestKNNImputation:
    """Tests for KNN imputation."""

    def test_basic_knn_imputation(self):
        """Should impute missing values using KNN."""
        df = pl.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "feature1": [1.0, 2.0, 3.0, 4.0, 5.0],
                "feature2": [10.0, 20.0, 30.0, 40.0, 50.0],
                "target": [100.0, None, 300.0, None, 500.0],
            }
        )

        result_df, stats = impute_knn(
            df,
            "target",
            n_neighbors=2,
            neighbor_weights="uniform",
            numeric_features=["feature1", "feature2"],
        )

        # Should have imputed 2 values
        assert stats["n_missing"] == 2
        assert stats["n_imputed"] == 2
        assert stats["pct_imputed"] == pytest.approx(40.0)

        # No nulls should remain
        assert result_df["target"].null_count() == 0

    def test_no_missing_values(self):
        """Should skip imputation when no missing values."""
        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "value": [1.0, 2.0, 3.0],
            }
        )

        result_df, stats = impute_knn(df, "value", n_neighbors=2, numeric_features=["value"])

        assert stats["n_missing"] == 0
        assert stats["n_imputed"] == 0
        assert result_df.equals(df)

    def test_all_missing_values(self):
        """Should handle all missing values gracefully."""
        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "feature": [1.0, 2.0, 3.0],
                "target": [None, None, None],
            }
        )

        _, stats = impute_knn(df, "target", n_neighbors=2, numeric_features=["feature"])

        assert stats["n_missing"] == 3
        assert stats["n_imputed"] == 0
        assert stats["pct_imputed"] == 100.0

    def test_categorical_imputation(self):
        """Should impute categorical values (integer codes)."""
        df = pl.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "feature": [1.0, 2.0, 3.0, 4.0, 5.0],
                "mode": [1, None, 1, None, 2],
            }
        )

        result_df, stats = impute_knn(df, "mode", n_neighbors=2, numeric_features=["feature"])

        assert stats["n_imputed"] == 2
        assert result_df["mode"].null_count() == 0
        # Values should be reasonable (between 1 and 2)
        assert result_df["mode"].min() >= 1  # pyright: ignore[reportOperatorIssue]
        assert result_df["mode"].max() <= 2  # pyright: ignore[reportOperatorIssue]

    def test_knn_with_non_contiguous_integer_codes(self):
        """KNN should produce valid category codes for non-contiguous integers."""
        rng = np.random.default_rng(42)
        n = 40
        feature = rng.normal(size=n)

        # Use non-contiguous codes: 10, 20, 30
        codes = np.array([10, 20, 30])
        target = codes[np.digitize(feature, bins=[-0.5, 0.5]) % 3]
        target_list = target.tolist()
        # Introduce nulls
        target_list[0] = None
        target_list[5] = None

        df = pl.DataFrame(
            {
                "feature": feature.tolist(),
                "cat": pl.Series("cat", target_list, dtype=pl.Int64),
            }
        )
        result, stats = impute_knn(df, "cat", n_neighbors=3, numeric_features=["feature"])

        assert stats["n_imputed"] == 2
        assert result["cat"].null_count() == 0
        # ALL values must be one of the valid original codes
        assert set(result["cat"].to_list()).issubset({10, 20, 30})


class TestMICEImputation:
    """Tests for MICE imputation."""

    def test_basic_mice_imputation(self):
        """Should impute correlated columns using MICE."""
        df = pl.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "col1": [1.0, None, 3.0, 4.0, 5.0],
                "col2": [10.0, 20.0, None, 40.0, 50.0],
                "col3": [100.0, 200.0, 300.0, 400.0, 500.0],
            }
        )

        result_df, stats = impute_mice(
            df,
            columns=["col1", "col2"],
            max_iter=5,
            random_state=42,
            numeric_features=["col1", "col2", "col3"],
        )

        # Should have imputed values
        assert stats["col1"]["n_imputed"] == 1
        assert stats["col2"]["n_imputed"] == 1
        assert result_df["col1"].null_count() == 0
        assert result_df["col2"].null_count() == 0

    def test_no_missing_in_any_column(self):
        """Should skip imputation when no missing values."""
        df = pl.DataFrame(
            {
                "col1": [1.0, 2.0, 3.0],
                "col2": [10.0, 20.0, 30.0],
            }
        )

        result_df, stats = impute_mice(
            df, columns=["col1", "col2"], numeric_features=["col1", "col2"]
        )

        assert stats["col1"]["n_imputed"] == 0
        assert stats["col2"]["n_imputed"] == 0
        assert result_df.equals(df)


class TestRandomForestImputation:
    """Tests for Random Forest imputation."""

    def test_basic_rf_categorical_imputation(self):
        """Should impute categorical values using Random Forest classifier."""
        rng = np.random.default_rng(42)
        n = 50
        feature = rng.normal(size=n).tolist()
        # Deterministic categories based on feature
        target = [1 if f > 0 else 2 for f in feature]
        # Null out some
        target[0] = None
        target[5] = None
        target[10] = None

        df = pl.DataFrame(
            {
                "feature": feature,
                "mode": pl.Series("mode", target, dtype=pl.Int64),
            }
        )

        result_df, stats = impute_random_forest(
            df, "mode", n_estimators=50, random_state=42, numeric_features=["feature"]
        )

        assert stats["n_missing"] == 3
        assert stats["n_imputed"] == 3
        assert result_df["mode"].null_count() == 0
        # Values should be valid categories
        assert set(result_df["mode"].to_list()).issubset({1, 2})

    def test_basic_rf_continuous_imputation(self):
        """Should impute continuous values using Random Forest regressor."""
        df = pl.DataFrame(
            {
                "x": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
                "y": [2.0, 4.0, None, 8.0, 10.0, 12.0, None, 16.0, 18.0, 20.0],
            }
        )

        result_df, stats = impute_random_forest(
            df, "y", n_estimators=50, random_state=42, numeric_features=["x"]
        )

        assert stats["n_missing"] == 2
        assert stats["n_imputed"] == 2
        assert result_df["y"].null_count() == 0

    def test_rf_no_missing_values(self):
        """Should skip imputation when no missing values."""
        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "value": [1.0, 2.0, 3.0],
            }
        )

        result_df, stats = impute_random_forest(df, "value", numeric_features=["value"])

        assert stats["n_missing"] == 0
        assert stats["n_imputed"] == 0
        assert result_df.equals(df)

    def test_rf_all_missing_values(self):
        """Should handle all missing values gracefully."""
        df = pl.DataFrame(
            {
                "feature": [1.0, 2.0, 3.0],
                "target": [None, None, None],
            }
        )

        _, stats = impute_random_forest(df, "target", numeric_features=["feature"])

        assert stats["n_missing"] == 3
        assert stats["n_imputed"] == 0
        assert stats["pct_imputed"] == 100.0

    def test_rf_with_non_contiguous_integer_codes(self):
        """RF should produce valid category codes for non-contiguous integers."""
        rng = np.random.default_rng(42)
        n = 60
        feature = rng.normal(size=n)

        # Use non-contiguous codes: 10, 20, 30
        codes = np.array([10, 20, 30])
        target = codes[np.digitize(feature, bins=[-0.5, 0.5]) % 3]
        target_list = target.tolist()
        # Introduce nulls
        target_list[0] = None
        target_list[5] = None
        target_list[10] = None

        df = pl.DataFrame(
            {
                "feature": feature.tolist(),
                "cat": pl.Series("cat", target_list, dtype=pl.Int64),
            }
        )
        result, stats = impute_random_forest(
            df, "cat", n_estimators=50, random_state=42, numeric_features=["feature"]
        )

        assert stats["n_imputed"] == 3
        assert result["cat"].null_count() == 0
        # ALL values must be one of the valid original codes
        assert set(result["cat"].to_list()).issubset({10, 20, 30})

    def test_rf_with_categorical_features(self):
        """RF should handle one-hot encoded categorical features."""
        rng = np.random.default_rng(42)
        n = 50
        df = pl.DataFrame(
            {
                "age": rng.normal(40, 10, size=n).tolist(),
                "gender": rng.choice([1, 2], size=n).tolist(),
                "income": pl.Series(
                    "income",
                    [*rng.choice([1, 2, 3], size=n - 3).tolist(), None, None, None],
                    dtype=pl.Int64,
                ),
            }
        )

        result_df, stats = impute_random_forest(
            df,
            "income",
            n_estimators=50,
            random_state=42,
            numeric_features=["age"],
            categorical_features=["gender"],
        )

        assert stats["n_imputed"] == 3
        assert result_df["income"].null_count() == 0
        assert set(result_df["income"].to_list()).issubset({1, 2, 3})

    def test_rf_missing_column_raises(self):
        """Should raise error for missing column."""
        df = pl.DataFrame({"a": [1, 2, 3]})

        with pytest.raises(ValueError, match="Column 'missing' not found"):
            impute_random_forest(df, "missing", numeric_features=["a"])

    def test_rf_returns_feature_importance(self):
        """RF stats should include feature_importance dict."""
        rng = np.random.default_rng(42)
        n = 50
        df = pl.DataFrame(
            {
                "age": rng.normal(40, 10, size=n).tolist(),
                "gender": rng.choice([1, 2], size=n).tolist(),
                "income": pl.Series(
                    "income",
                    [*rng.choice([1, 2, 3], size=n - 3).tolist(), None, None, None],
                    dtype=pl.Int64,
                ),
            }
        )

        _, stats = impute_random_forest(
            df,
            "income",
            n_estimators=50,
            random_state=42,
            numeric_features=["age"],
            categorical_features=["gender"],
        )

        assert "feature_importance" in stats
        fi = stats["feature_importance"]
        assert isinstance(fi, dict)
        # Should have age + gender (one-hot aggregated back)
        assert "age" in fi
        assert "gender" in fi
        # Importances should sum to ~1.0
        assert pytest.approx(sum(fi.values()), abs=0.01) == 1.0
        # Should be sorted descending
        values = list(fi.values())
        assert values == sorted(values, reverse=True)


class TestPreimputedStash:
    """Tests for pre-imputation value stashing."""

    def test_stash_single_column(self):
        """Should stash original values including nulls."""
        original_df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "value": [1.0, None, 3.0],
            }
        )

        imputed_df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "value": [1.0, 2.0, 3.0],
            }
        )

        result_df = stash_preimputed_column(imputed_df, original_df, "value")

        assert "value_preimputed" in result_df.columns
        assert result_df["value_preimputed"].to_list() == [1.0, None, 3.0]

    def test_stash_multiple_columns(self):
        """Should stash original values for multiple columns."""
        original_df = pl.DataFrame(
            {
                "col1": [1.0, None, 3.0],
                "col2": [10.0, 20.0, None],
            }
        )

        imputed_df = pl.DataFrame(
            {
                "col1": [1.0, 2.0, 3.0],
                "col2": [10.0, 20.0, 30.0],
            }
        )

        result_df = stash_preimputed_columns(imputed_df, original_df, ["col1", "col2"])

        assert "col1_preimputed" in result_df.columns
        assert "col2_preimputed" in result_df.columns
        assert result_df["col1_preimputed"].to_list() == [1.0, None, 3.0]
        assert result_df["col2_preimputed"].to_list() == [10.0, 20.0, None]

    def test_preimputed_recovers_boolean_flag(self):
        """Boolean imputed flag should be derivable from preimputed column."""
        original_df = pl.DataFrame(
            {
                "value": [1.0, None, 3.0, None],
            }
        )

        imputed_df = pl.DataFrame(
            {
                "value": [1.0, 2.0, 3.0, 4.0],
            }
        )

        result_df = stash_preimputed_column(imputed_df, original_df, "value")

        # Derive the old boolean flag from the preimputed column
        was_imputed = result_df["value_preimputed"].is_null() & result_df["value"].is_not_null()
        assert was_imputed.to_list() == [False, True, False, True]

    def test_preimputed_preserves_pnta_vs_null(self):
        """Should distinguish PNTA (999) from genuine null."""
        original_df = pl.DataFrame(
            {
                "income": [1, 999, None, 3, 995],
            }
        )

        imputed_df = pl.DataFrame(
            {
                "income": [1, 2, 2, 3, 2],
            }
        )

        result_df = stash_preimputed_column(imputed_df, original_df, "income")

        stashed = result_df["income_preimputed"].to_list()
        assert stashed[0] == 1  # unchanged
        assert stashed[1] == 999  # PNTA preserved
        assert stashed[2] is None  # genuine null preserved
        assert stashed[3] == 3  # unchanged
        assert stashed[4] == 995  # MISSING preserved


class TestValidation:
    """Tests for imputation validation."""

    def test_is_categorical(self):
        """Should correctly identify categorical columns."""
        df = pl.DataFrame(
            {
                "int_col": [1, 2, 3],
                "float_col": [1.5, 2.5, 3.5],
                "str_col": ["a", "b", "c"],
            }
        )

        assert is_categorical(df, "int_col") is True
        assert is_categorical(df, "float_col") is False
        assert is_categorical(df, "str_col") is True

    def test_knn_validation_categorical(self):
        """Should validate KNN imputation on categorical data."""
        df = pl.DataFrame(
            {
                "feature": [1.0, 2.0, 3.0, 4.0, 5.0] * 20,  # 100 rows
                "mode": [1, 1, 2, 2, 1] * 20,
            }
        )

        metrics = validate_knn_imputation(
            df,
            column="mode",
            n_folds=3,
            sample_pct=10.0,
            n_neighbors=3,
            neighbor_weights="uniform",
            random_state=42,
            numeric_features=["feature"],
        )

        assert metrics["type"] == "categorical"
        assert "accuracy" in metrics
        assert "precision" in metrics
        assert "recall" in metrics
        assert "f1" in metrics
        assert 0 <= metrics["accuracy"] <= 1

    def test_knn_validation_continuous(self):
        """Should validate KNN imputation on continuous data."""
        df = pl.DataFrame(
            {
                "feature": [1.0, 2.0, 3.0, 4.0, 5.0] * 20,
                "distance": [10.5, 20.3, 15.7, 25.1, 18.9] * 20,
            }
        )

        metrics = validate_knn_imputation(
            df,
            column="distance",
            n_folds=3,
            sample_pct=10.0,
            n_neighbors=3,
            neighbor_weights="distance",
            random_state=42,
            numeric_features=["feature"],
        )

        assert metrics["type"] == "continuous"
        assert "rmse" in metrics
        assert "mae" in metrics
        assert "r2" in metrics
        assert metrics["rmse"] >= 0

    def test_mice_validation(self):
        """Should validate MICE imputation on multiple columns."""
        df = pl.DataFrame(
            {
                "col1": [1.0, 2.0, 3.0, 4.0, 5.0] * 20,
                "col2": [10.0, 20.0, 30.0, 40.0, 50.0] * 20,
            }
        )

        metrics = validate_mice_imputation(
            df,
            columns=["col1", "col2"],
            n_folds=3,
            sample_pct=10.0,
            max_iter=5,
            random_state=42,
            numeric_features=["col1", "col2"],
        )

        assert "col1" in metrics
        assert "col2" in metrics
        assert metrics["col1"]["type"] == "continuous"
        assert "rmse" in metrics["col1"]

    def test_rf_validation_categorical(self):
        """Should validate RF imputation on categorical data."""
        df = pl.DataFrame(
            {
                "feature": [1.0, 2.0, 3.0, 4.0, 5.0] * 20,
                "mode": [1, 1, 2, 2, 1] * 20,
            }
        )

        metrics = validate_rf_imputation(
            df,
            column="mode",
            n_folds=3,
            sample_pct=10.0,
            n_estimators=50,
            random_state=42,
            numeric_features=["feature"],
        )

        assert metrics["type"] == "categorical"
        assert "accuracy" in metrics
        assert 0 <= metrics["accuracy"] <= 1

    def test_rf_validation_continuous(self):
        """Should validate RF imputation on continuous data."""
        df = pl.DataFrame(
            {
                "feature": [1.0, 2.0, 3.0, 4.0, 5.0] * 20,
                "distance": [10.5, 20.3, 15.7, 25.1, 18.9] * 20,
            }
        )

        metrics = validate_rf_imputation(
            df,
            column="distance",
            n_folds=3,
            sample_pct=10.0,
            n_estimators=50,
            random_state=42,
            numeric_features=["feature"],
        )

        assert metrics["type"] == "continuous"
        assert "rmse" in metrics
        assert metrics["rmse"] >= 0


class TestEdgeCases:
    """Tests for edge cases."""

    def test_knn_with_single_row(self):
        """Should handle single row gracefully."""
        df = pl.DataFrame(
            {
                "feature": [1.0],
                "target": [None],
            }
        )

        _, stats = impute_knn(df, "target", n_neighbors=1, numeric_features=["feature"])
        # Cannot impute with only one row
        assert stats["n_imputed"] == 0

    def test_mice_with_insufficient_data(self):
        """Should handle insufficient data gracefully."""
        df = pl.DataFrame(
            {
                "col1": [1.0, None],
                "col2": [None, 2.0],
            }
        )

        result_df, _ = impute_mice(df, columns=["col1", "col2"], numeric_features=["col1", "col2"])
        # Should attempt imputation but may not be accurate
        assert result_df is not None

    def test_knn_missing_column(self):
        """Should raise error for missing column."""
        df = pl.DataFrame({"a": [1, 2, 3]})

        with pytest.raises(ValueError, match="Column 'missing' not found"):
            impute_knn(df, "missing", n_neighbors=2)

    def test_mice_missing_columns(self):
        """Should raise error for missing columns."""
        df = pl.DataFrame({"a": [1, 2, 3]})

        with pytest.raises(ValueError, match="Columns not found"):
            impute_mice(df, columns=["missing1", "missing2"])


class TestDenseIntegerEncoding:
    """Tests for integer categorical dense encoding / decoding."""

    def test_encodes_non_contiguous_codes(self):
        """Non-contiguous integer codes should be mapped to dense 0..N."""
        df = pl.DataFrame({"income_bin": [1, 2, 5, 6, None]})
        encoded, encodings = encode_integer_categoricals(df, ["income_bin"])

        assert "income_bin" in encodings
        # Dense codes should be 0..3 for the four unique values
        assert encodings["income_bin"] == {0: 1, 1: 2, 2: 5, 3: 6}
        vals = encoded["income_bin"].drop_nulls().to_list()
        assert sorted(vals) == [0.0, 1.0, 2.0, 3.0]
        # Null should be preserved
        assert encoded["income_bin"].null_count() == 1

    def test_skips_already_dense_columns(self):
        """Columns coded 0..N-1 should not be re-encoded."""
        df = pl.DataFrame({"status": [0, 1, 2, 3]})
        encoded, encodings = encode_integer_categoricals(df, ["status"])

        assert "status" not in encodings
        assert encoded["status"].to_list() == [0, 1, 2, 3]

    def test_skips_non_integer_columns(self):
        """Float/string columns should be ignored entirely."""
        df = pl.DataFrame({"val": [1.5, 2.5, 3.5]})
        _, encodings = encode_integer_categoricals(df, ["val"])

        assert encodings == {}

    def test_decode_dense_to_integer_basic(self):
        """Dense float predictions should round and decode to original codes."""
        mapping = {0: 1, 1: 2, 2: 5, 3: 6}
        values = np.array([0.3, 0.7, 2.1, 2.9])
        decoded = decode_dense_to_integer(values, mapping)

        assert decoded == [1, 2, 5, 6]

    def test_decode_dense_clamps_out_of_range(self):
        """Values outside [0, N-1] should be clamped to the nearest valid key."""
        mapping = {0: 10, 1: 20, 2: 30}
        values = np.array([-1.0, 5.0])
        decoded = decode_dense_to_integer(values, mapping)

        assert decoded == [10, 30]

    def test_mice_with_non_contiguous_integer_codes(self):
        """MICE should produce valid category codes for non-contiguous integers."""
        rng = np.random.default_rng(42)
        n = 60
        feature = rng.normal(size=n)

        # Use non-contiguous codes: 10, 20, 30
        codes = np.array([10, 20, 30])
        target = codes[np.digitize(feature, bins=[-0.5, 0.5]) % 3]
        target_list = target.tolist()
        # Introduce nulls
        target_list[0] = None
        target_list[5] = None
        target_list[10] = None

        df = pl.DataFrame(
            {
                "feature": feature.tolist(),
                "cat": pl.Series("cat", target_list, dtype=pl.Int64),
            }
        )
        result, _ = impute_mice(
            df,
            columns=["cat"],
            max_iter=5,
            random_state=42,
            numeric_features=["feature"],
        )

        assert result["cat"].null_count() == 0
        # ALL imputed values must be one of the valid original codes
        imputed_vals = set(result["cat"].to_list())
        assert imputed_vals.issubset({10, 20, 30})


class TestBuildFeatureMatrix:
    """Tests for build_feature_matrix feature name tracking."""

    def test_returns_feature_names(self):
        """Feature names should include continuous and one-hot encoded columns."""
        df = pl.DataFrame(
            {
                "target": [1.0, 2.0, 3.0, 4.0],
                "num_feat": [10.0, 20.0, 30.0, 40.0],
                "cat_feat": [1, 2, 1, 2],
            }
        )

        matrix, _indices, names = build_feature_matrix(
            df,
            target_columns=["target"],
            numeric_features=["num_feat"],
            categorical_features=["cat_feat"],
        )

        assert "num_feat" in names
        assert "target" in names
        assert "cat_feat=1" in names
        assert "cat_feat=2" in names
        assert len(names) == matrix.shape[1]

    def test_feature_names_empty_categoricals(self):
        """Should work with only numeric features."""
        df = pl.DataFrame(
            {
                "target": [1.0, None, 3.0],
                "feat": [10.0, 20.0, 30.0],
            }
        )

        _, _, names = build_feature_matrix(df, ["target"], ["feat"], [])

        assert names == ["feat", "target"]


class TestMethodComparison:
    """Tests for head-to-head method comparison."""

    def _make_comparison_data(self) -> tuple[dict[str, pl.DataFrame], dict]:
        """Build a small dataset plus config for comparison tests."""
        df = pl.DataFrame(
            {
                "feature": [1.0, 2.0, 3.0, 4.0, 5.0] * 20,
                "mode": [1, 1, 2, 2, 1] * 20,
            }
        )
        tables = {"persons": df}
        config: dict = {
            "persons": [
                {
                    "method": "knn",
                    "column": "mode",
                    "numeric_features": ["feature"],
                    "n_neighbors": 3,
                }
            ]
        }
        return tables, config

    def test_comparison_returns_all_methods(self):
        """Comparison should produce rows for KNN, RF, and MICE."""
        tables, config = self._make_comparison_data()
        result = compare_imputation_methods(
            config,
            tables,
            n_folds=3,
            sample_pct=10.0,
            random_state=42,
        )

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 3
        methods = set(result["method"].to_list())
        assert methods == {"knn", "rf", "mice"}

        # All rows should reference the same column
        assert result["variable"].unique().to_list() == ["mode"]
        assert result["table"].unique().to_list() == ["persons"]

    def test_comparison_deduplicates_columns(self):
        """Same column configured twice should only produce one set of rows."""
        tables, config = self._make_comparison_data()
        # Add a duplicate config block with RF method
        config["persons"].append(
            {
                "method": "rf",
                "column": "mode",
                "numeric_features": ["feature"],
            }
        )

        result = compare_imputation_methods(
            config,
            tables,
            n_folds=3,
            sample_pct=10.0,
            random_state=42,
        )

        # Still 3 rows (one per method), not 6
        assert len(result) == 3

    def test_comparison_splits_mice_columns(self):
        """MICE columns:[a,b] should produce two sets of 3 method rows."""
        df = pl.DataFrame(
            {
                "feature": [1.0, 2.0, 3.0, 4.0, 5.0] * 20,
                "col_a": [1, 2, 1, 2, 1] * 20,
                "col_b": [10, 20, 30, 10, 20] * 20,
            }
        )
        config = {
            "persons": [
                {
                    "method": "mice",
                    "columns": ["col_a", "col_b"],
                    "numeric_features": ["feature"],
                }
            ]
        }
        result = compare_imputation_methods(
            config,
            {"persons": df},
            n_folds=3,
            sample_pct=10.0,
            random_state=42,
        )

        # 2 columns x 3 methods = 6 rows
        assert len(result) == 6
        assert set(result["variable"].to_list()) == {"col_a", "col_b"}
        for var in ("col_a", "col_b"):
            subset = result.filter(pl.col("variable") == var)
            assert set(subset["method"].to_list()) == {"knn", "rf", "mice"}

    def test_comparison_has_expected_columns(self):
        """Result DataFrame should contain the expected metric columns."""
        tables, config = self._make_comparison_data()
        result = compare_imputation_methods(
            config,
            tables,
            n_folds=3,
            sample_pct=10.0,
            random_state=42,
        )

        expected = {
            "table",
            "variable",
            "method",
            "type",
            "n_samples",
            "n_folds",
            "accuracy",
            "precision",
            "recall",
            "f1",
        }
        assert expected.issubset(set(result.columns))

    def test_comparison_saves_csv(self, tmp_path):
        """Should write CSV when output_path is provided."""
        tables, config = self._make_comparison_data()
        csv_path = str(tmp_path / "comparison.csv")
        compare_imputation_methods(
            config,
            tables,
            n_folds=3,
            sample_pct=10.0,
            random_state=42,
            output_path=csv_path,
        )

        saved = pl.read_csv(csv_path)
        assert len(saved) == 3
        assert "method" in saved.columns
