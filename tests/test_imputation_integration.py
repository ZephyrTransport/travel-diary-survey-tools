"""Simple integration test for imputation step."""

import polars as pl

from processing.imputation.flags import stash_preimputed_columns
from processing.imputation.knn import impute_knn
from processing.imputation.mice import impute_mice
from processing.imputation.validation import validate_knn_imputation, validate_mice_imputation


def test_imputation_step_knn_no_validation():
    """Test full imputation step with KNN (no validation)."""
    # Create sample data
    unlinked_trips = pl.DataFrame(
        {
            "trip_id": [1, 2, 3, 4, 5],
            "person_id": [1, 1, 1, 2, 2],
            "mode": [1, None, 1, 2, None],
            "distance": [5.0, 10.0, 15.0, 20.0, 25.0],
            "duration": [30.0, 45.0, 60.0, 90.0, 120.0],
        }
    )

    original_df = unlinked_trips.clone()
    result_df, stats = impute_knn(
        unlinked_trips,
        "mode",
        n_neighbors=2,
        neighbor_weights="uniform",
        numeric_features=["distance", "duration"],
    )

    # Check results
    assert result_df["mode"].null_count() == 0
    assert stats["n_imputed"] == 2

    # Stash pre-imputation values
    result_df = stash_preimputed_columns(result_df, original_df, ["mode"])
    assert "mode_preimputed" in result_df.columns
    # Original had 2 nulls — those should be None in the stashed column
    assert result_df["mode_preimputed"].null_count() == 2


def test_imputation_step_mice_no_validation():
    """Test full imputation step with MICE (no validation)."""
    data = pl.DataFrame(
        {
            "person_id": [1, 2, 3, 4, 5],
            "col1": [25.0, None, 35.0, 45.0, None],
            "col2": [50000.0, 60000.0, None, 80000.0, 90000.0],
            "col3": [2, 3, 2, 4, 3],
        }
    )

    original_df = data.clone()
    result_df, stats = impute_mice(
        data,
        columns=["col1", "col2"],
        max_iter=5,
        random_state=42,
        numeric_features=["col1", "col2"],
        categorical_features=["col3"],
    )

    # Check results
    assert result_df["col1"].null_count() == 0
    assert result_df["col2"].null_count() == 0
    assert stats["col1"]["n_imputed"] == 2
    assert stats["col2"]["n_imputed"] == 1

    # Stash pre-imputation values
    result_df = stash_preimputed_columns(result_df, original_df, ["col1", "col2"])
    assert "col1_preimputed" in result_df.columns
    assert "col2_preimputed" in result_df.columns


def test_knn_with_validation_quality():
    """Test KNN imputation with k-fold validation."""
    # Create data with clear pattern (easier to impute accurately)
    df = pl.DataFrame(
        {
            "feature": [1.0, 2.0, 3.0, 4.0, 5.0] * 20,  # 100 rows
            "mode": [1, 1, 2, 2, 1] * 20,
        }
    )

    # Validate imputation quality
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

    # Should have reasonable accuracy
    assert metrics["type"] == "categorical"
    assert "accuracy" in metrics
    assert metrics["accuracy"] >= 0.5  # At least as good as random (or better)


def test_mice_with_validation_quality():
    """Test MICE imputation with k-fold validation."""
    # Create correlated data
    df = pl.DataFrame(
        {
            "col1": [1.0, 2.0, 3.0, 4.0, 5.0] * 20,
            "col2": [10.0, 20.0, 30.0, 40.0, 50.0] * 20,  # col2 = col1 * 10
        }
    )

    # Validate imputation quality
    metrics = validate_mice_imputation(
        df,
        columns=["col1", "col2"],
        n_folds=3,
        sample_pct=10.0,
        max_iter=5,
        random_state=42,
        numeric_features=["col1", "col2"],
    )

    # Should have good metrics for highly correlated data
    assert "col1" in metrics
    assert "col2" in metrics
    assert metrics["col1"]["type"] == "continuous"
    assert "rmse" in metrics["col1"]


if __name__ == "__main__":
    test_imputation_step_knn_no_validation()
    test_imputation_step_mice_no_validation()
    test_knn_with_validation_quality()
    test_mice_with_validation_quality()
