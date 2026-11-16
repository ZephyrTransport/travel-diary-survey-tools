# Imputation

::: processing.imputation
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

## The step

::: processing.imputation.impute.imputation
    options:
      show_root_heading: false
      show_root_toc_entry: false
      show_signature: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_raises: false

## Example

Each table lists its own rules; a rule names a column, a method (`knn`, `rf` or
`mice`), the values to treat as missing, and the features to predict it from.

```yaml title="Example"
- name: imputation
  params:
    impute_columns:
      households:
        - column: income_bin
          method: rf
          missing_values: [MISSING, PNTA]
          categorical_features: [residence_type, residence_rent_own]
          aggregate_from:
            persons:
              pivot_count: [employment, education, student]
      persons:
        - column: gender
          method: rf
          missing_values: [MISSING, PNTA]
          join_tables: [households]
          numeric_features: [num_trips]
          categorical_features: [age, employment, education, income_bin]
    stash_preimputed: true
    random_state: 42
    validate_imputation:
      enabled: true
      n_folds: 5
      sample_pct: 5.0
```

## Settings

<!-- step-settings: imputation -->

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

| Feature         | Importance |
|-----------------|-----------|
| employment      | 0.312     |
| education       | 0.241     |
| age             | 0.189     |
| occupation      | 0.127     |
| n_hh_income_bin | 0.065     |
| …               | …         |

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

Code: [processing.imputation](../reference/processing/imputation.md)
