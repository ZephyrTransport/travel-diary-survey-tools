"""Data imputation for missing values in travel diary survey data.

This module provides configurable imputation using established statistical
methods.  It is designed as a pipeline step that operates on any combination
of canonical survey tables (households, persons, days, trips, tours).

# Supported methods

* **KNN** -- single-column imputation via K-Nearest Neighbors similarity
  matching.  Best for isolated missing fields where similar records exist.
* **Random Forest** -- single-column imputation using a supervised RF model
  that auto-selects classifier vs. regressor based on the column type.
  Best for complex non-linear relationships or mixed feature types.
* **MICE** -- multi-column imputation via Multiple Imputation by Chained
  Equations.  Best for correlated variables (e.g. depart/arrive/duration).

# Additional capabilities

* **Pre-imputation stash** -- optional ``{column}_preimputed`` columns that
  preserve the original value (null, PNTA, MISSING, etc.) before imputation,
  enabling downstream equity analysis and imputation auditing.
* **Quality validation** -- optional k-fold cross-validation that masks known
  values, re-imputes them, and reports accuracy / RMSE metrics.
* **Method comparison** -- head-to-head benchmark of KNN, RF, and MICE on
  every imputed column using the same folds and feature sets.
* **Cross-table features** -- ``join_tables`` pulls parent-table columns and
  auto-generates within-household mode features; ``aggregate_from`` pivots
  child rows up to a parent.

# Typical pipeline position

    steps:
      - name: load_data
      - name: custom_cleaning
      - name: imputation        # ← after cleaning, before linking
      - name: link_trips
      - name: joint_trips
      - name: extract_tours

# Supported relationships for joining tables

These parent → child relationships are supported for ``join_tables``

| Child table    | Parent table                | Join key                   |
|----------------|-----------------------------|----------------------------|
| persons        | households                  | hh_id                      |
| days           | persons / households        | person_id / hh_id          |
| unlinked_trips | days / persons / households | day_id / person_id / hh_id |
| linked_trips   | days / persons / households | day_id / person_id / hh_id |
| tours          | persons / households        | person_id / hh_id          |

# Missing-data assumptions

* **KNN** assumes similar records (by feature distance) share similar values.
* **MICE** assumes Missing At Random (MAR): missingness may depend on observed
  values but not on the missing value itself.
* If data is Missing Not At Random (MNAR), results may be biased.

# Current limitations

* No stratified imputation (no ``group_by`` option for within-group models).
* No support for exogenous data sources (PUMS, land use data).
* High-cardinality one-hot encoding can slow MICE convergence -- move
  ordinal/count variables to ``numeric_features`` to mitigate.

# References

* [van Buuren, S. & Groothuis-Oudshoorn, K. (2011). *mice: Multivariate
  Imputation by Chained Equations in R.* Journal of Statistical Software,
  45(3), 1-67.](https://www.jstatsoft.org/article/view/v045i03)
* [Troyanskaya, O. et al. (2001). *Missing value estimation methods for DNA
  microarrays.* Bioinformatics, 17(6), 520-525.](https://www.researchgate.net/publication/220263062_Missing_Value_Estimation_Methods_for_DNA_Microarrays)
"""

from .impute import imputation

__all__ = ["imputation"]
