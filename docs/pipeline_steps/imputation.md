::: processing.imputation
    options:
      show_root_heading: true
      show_root_toc_entry: false
      show_if_no_docstring: false
      filters:
        - "!^_"

## Feature Importance

When using the **Random Forest** method, the imputation module automatically
reports which features contributed most to the model's predictions.  This
helps you verify that the model is learning from sensible predictors and
catch potential issues like data leakage or uninformative features.

### How it works (Mean Decrease in Impurity)

Each tree in a Random Forest repeatedly splits data on the feature that best
reduces **impurity** — Gini impurity for classifiers, variance for
regressors.  The total impurity reduction attributable to a feature, summed
across all splits in all trees and normalised so that importances sum to 1.0,
is called **Mean Decrease in Impurity (MDI)**.

MDI is computed for free during `model.fit()` — no extra passes over the data
are required.  The imputation module extracts it from
`model.feature_importances_`, maps column indices back to human-readable
names, and aggregates one-hot encoded dummies back to their parent categorical
feature (e.g. `gender=1` + `gender=2` → `gender`).

### Interpreting the output

The returned `feature_importance` dict maps feature names to their normalised
importance score.  Example:

| Feature            | Importance |
|--------------------|-----------|
| employment         | 0.312     |
| education          | 0.241     |
| age                | 0.189     |
| occupation         | 0.127     |
| hh_mode_income_bin | 0.065     |
| …                  | …         |

An importance of 0.31 means ~31% of the model's total impurity reduction came
from splits on that feature.  Higher values indicate features the model relies
on the most.

**What to look for:**

- **Sensible top features** — age and employment should matter for income;
  trip_id should not.
- **Low-importance features** — candidates for removal to speed up
  imputation without affecting quality.
- **Surprising features** — may indicate data leakage (e.g. a downstream
  field that shouldn't be available at imputation time).

### MDI caveats

MDI is **biased toward high-cardinality features**: a feature with 50 unique
values gets more splitting opportunities than one with 2, inflating its
apparent importance.  The module mitigates this by aggregating one-hot columns
back to their parent feature, but the bias can still affect comparisons
between features of very different cardinality.

For more robust ranking, **permutation importance** (shuffle a feature, measure
accuracy drop) is planned as a future Tier 2 enhancement gated behind a config
flag.

### Comparison to statistical significance

| Aspect | Statistical significance (p-values) | Feature importance (MDI) |
|--------|--------------------------------------|--------------------------|
| Question answered | "Is this feature's effect distinguishable from zero?" | "How much does this feature contribute to prediction accuracy?" |
| Sample size sensitivity | Very sensitive — large N makes trivially small effects "significant" | Less sensitive — a useless feature stays near zero regardless of N |
| Nonlinearity | Only captured if you specify the right model form | Inherently captures nonlinear and interaction effects |
| Collinearity | Inflates standard errors, masks individual predictors | Spreads importance across correlated features |
| Threshold | Arbitrary α (usually 0.05) | No hard threshold — it's a ranking, not a binary yes/no |

A feature can be statistically significant (p < 0.001) yet have near-zero
predictive importance (tiny real effect + large sample).  Conversely, a
feature can have high importance with no p-value because there's no parametric
model to test against.  For imputation, importance is the better lens — you
want to know "does including this feature actually improve the imputed
values?"

::: processing.imputation.impute
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - imputation
      filters:
        - "!^logger$"
        - "!^_"

::: processing.imputation.knn
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - impute_knn
      filters:
        - "!^_"

::: processing.imputation.random_forest
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - impute_random_forest
      filters:
        - "!^_"

::: processing.imputation.mice
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - impute_mice
      filters:
        - "!^_"

::: processing.imputation.comparison
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - compare_imputation_methods
      filters:
        - "!^_"

::: processing.imputation.flags
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - stash_preimputed_columns
        - stash_preimputed_column
      filters:
        - "!^_"

::: processing.imputation.validation
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - validate_knn_imputation
        - validate_mice_imputation
        - validate_rf_imputation
        - log_validation_results
      filters:
        - "!^_"
