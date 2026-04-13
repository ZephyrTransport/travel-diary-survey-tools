"""Fractional imputation for null-deficient incidence rows.

When a survey record has a null control value (e.g. missing gender), the
incidence pivot produces all-zero member columns for that control — the
record effectively vanishes from that constraint.  This module replaces
those zeros with probability distributions predicted by a Random Forest
trained on complete PUMS data, implementing *fractional imputation*
(Kim & Fuller, 2004).

The max-entropy balancer accepts non-negative floats in the incidence
matrix, so fractional entries are valid without modification.

Public API
----------
:func:`fill_null_incidence`
    Top-level orchestrator called from the weighting pipeline.
"""

import hashlib
import json
import logging
import pickle
from pathlib import Path

import numpy as np
import polars as pl
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import f1_score, log_loss
from sklearn.model_selection import cross_val_predict

from processing.weighting.controls.base import ControlLevel
from processing.weighting.controls.registry import resolve_targets
from processing.weighting.data_prep.incidence import IncidenceBundle
from processing.weighting.specs import ImputationSummary

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# RF model caching
# ---------------------------------------------------------------------------


def _rf_cache_key(
    train_pivot: pl.DataFrame,
    feature_cols: list[str],
    member_cols: list[str],
    n_estimators: int,
    random_state: int,
) -> str:
    """Deterministic hash of PUMS training data + hyperparameters."""
    data_hash = str(train_pivot.select(feature_cols + member_cols).hash_rows().sum())
    parts = [
        json.dumps(feature_cols),
        json.dumps(member_cols),
        str(n_estimators),
        str(random_state),
        data_hash,
        str(len(train_pivot)),
    ]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]


def _load_cached_rf(cache_dir: Path, control: str, key: str) -> dict | None:
    """Load a cached RF model + CV metrics.  Returns None on miss."""
    path = cache_dir / "rf_models" / f"{control}_{key}.pkl"
    if not path.exists():
        return None
    with path.open("rb") as f:
        return pickle.load(f)  # noqa: S301


def _save_cached_rf(
    cache_dir: Path,
    control: str,
    key: str,
    clf: RandomForestClassifier,
    cv_ll: float | None,
    cv_f1: float | None,
) -> None:
    """Persist a fitted RF + CV metrics to disk."""
    rf_dir = cache_dir / "rf_models"
    rf_dir.mkdir(parents=True, exist_ok=True)
    with (rf_dir / f"{control}_{key}.pkl").open("wb") as f:
        pickle.dump({"model": clf, "cv_ll": cv_ll, "cv_f1": cv_f1}, f)


# ---------------------------------------------------------------------------
# RF training helpers
# ---------------------------------------------------------------------------


def _cv_diagnostics(
    clf: RandomForestClassifier,
    x_train: np.ndarray,
    y_train: np.ndarray,
    n_features: int,
) -> tuple[float | None, float | None]:
    """Stratified CV on training data → (log_loss, f1_macro).

    Automatically reduces fold count when a class has fewer than 5
    samples, and skips CV entirely when any class has < 2  samples.
    """
    class_counts = np.bincount(y_train)
    min_per_class = int(class_counts[class_counts > 0].min())
    cv_folds = min(5, min_per_class)

    if cv_folds < 2:  # noqa: PLR2004
        logger.info("  Skipping CV: smallest class has only %d sample(s)", min_per_class)
        return None, None

    cv_proba = cross_val_predict(clf, x_train, y_train, cv=cv_folds, method="predict_proba")
    cv_pred = cv_proba.argmax(axis=1)
    cv_ll = float(log_loss(y_train, cv_proba))
    cv_f1 = float(f1_score(y_train, cv_pred, average="macro"))
    logger.info(
        "  CV diagnostics: log_loss=%.4f, f1_macro=%.4f (%d rows, %d features, %d folds)",
        cv_ll,
        cv_f1,
        len(y_train),
        n_features,
        cv_folds,
    )
    return cv_ll, cv_f1


def _predict_null_rows(
    clf: RandomForestClassifier,
    predict_pivot: pl.DataFrame,
    null_mask: pl.Series,
    feature_cols: list[str],
    member_cols: list[str],
    id_col: str,
) -> pl.DataFrame:
    """Run predict_proba on null rows → DataFrame aligned to *member_cols*."""
    null_rows = predict_pivot.filter(null_mask)
    if null_rows.is_empty():
        return pl.DataFrame()

    probas = clf.predict_proba(
        null_rows.select(feature_cols).to_numpy().astype(np.float32),
    )
    # Map RF class indices back to member columns
    proba_df = pl.DataFrame(
        {member_cols[cls]: probas[:, i] for i, cls in enumerate(clf.classes_)},
    )
    # Fill any member columns missing from clf.classes_ with 0
    for col in member_cols:
        if col not in proba_df.columns:
            proba_df = proba_df.with_columns(pl.lit(0.0).alias(col))

    return proba_df.with_columns(null_rows[id_col].alias(id_col))


# ---------------------------------------------------------------------------
# RF training + probabilistic prediction
# ---------------------------------------------------------------------------


def _train_and_predict(
    train_pivot: pl.DataFrame,
    predict_pivot: pl.DataFrame,
    null_mask: pl.Series,
    member_cols: list[str],
    id_col: str,
    *,
    n_estimators: int = 100,
    random_state: int = 42,
    cache_dir: Path | None = None,
) -> tuple[pl.DataFrame, float | None, float | None]:
    """Train an RF on complete rows and predict probabilities for null rows.

    Returns ``(proba_df, log_loss, f1_macro)``.  When *cache_dir* is
    provided the fitted model and CV metrics are persisted so subsequent
    runs with the same PUMS data skip training entirely.
    """
    feature_cols = sorted(c for c in train_pivot.columns if "__" in c and c not in member_cols)
    if not feature_cols:
        logger.warning("No feature columns for %s — skipping", member_cols)
        return pl.DataFrame(), None, None

    x_train = train_pivot.select(feature_cols).to_numpy().astype(np.float32)
    y_train = train_pivot.select(member_cols).to_numpy().argmax(axis=1)

    if len(np.unique(y_train)) < 2:  # noqa: PLR2004
        logger.warning("Only 1 class for %s — skipping", member_cols)
        return pl.DataFrame(), None, None

    # --- cache probe -----------------------------------------------------
    cache_key: str | None = None
    control_prefix = member_cols[0].split("__")[0]
    if cache_dir is not None:
        cache_key = _rf_cache_key(
            train_pivot,
            feature_cols,
            member_cols,
            n_estimators,
            random_state,
        )
        cached = _load_cached_rf(cache_dir, control_prefix, cache_key)
        if cached is not None:
            logger.info("  Loaded cached RF %s (key=%s)", control_prefix, cache_key)
            proba_df = _predict_null_rows(
                cached["model"],
                predict_pivot,
                null_mask,
                feature_cols,
                member_cols,
                id_col,
            )
            return proba_df, cached["cv_ll"], cached["cv_f1"]

    # --- train ---------------------------------------------------------
    clf = RandomForestClassifier(
        n_estimators=n_estimators,
        random_state=random_state,
        n_jobs=-1,
    )
    clf.fit(x_train, y_train)
    cv_ll, cv_f1 = _cv_diagnostics(clf, x_train, y_train, len(feature_cols))

    if cache_dir is not None and cache_key is not None:
        _save_cached_rf(cache_dir, control_prefix, cache_key, clf, cv_ll, cv_f1)

    # --- predict ---------------------------------------------------------
    proba_df = _predict_null_rows(
        clf,
        predict_pivot,
        null_mask,
        feature_cols,
        member_cols,
        id_col,
    )
    return proba_df, cv_ll, cv_f1


# ---------------------------------------------------------------------------
# Overlay + unified impute loop
# ---------------------------------------------------------------------------


def _overlay_columns(
    result: pl.DataFrame,
    fractions: pl.DataFrame,
    member_cols: list[str],
    hh_id_col: str,
    *,
    additive: bool,
) -> pl.DataFrame:
    """Single-join overlay of fractional predictions onto the incidence table."""
    aliases = {c: f"_frac_{c}" for c in member_cols}

    joined = result.join(
        fractions.select(
            hh_id_col,
            *[pl.col(c).alias(aliases[c]) for c in member_cols],
        ),
        on=hh_id_col,
        how="left",
    )

    if additive:
        exprs = [
            (pl.col(c).cast(pl.Float64) + pl.col(aliases[c]).fill_null(0.0)).alias(c)
            for c in member_cols
        ]
    else:
        exprs = [
            pl.when(pl.col(aliases[c]).is_not_null())
            .then(pl.col(aliases[c]))
            .otherwise(pl.col(c))
            .alias(c)
            for c in member_cols
        ]

    return joined.with_columns(exprs).drop(list(aliases.values()))


def _impute_controls(
    result: pl.DataFrame,
    level: ControlLevel,
    train_pivot: pl.DataFrame,
    predict_pivot: pl.DataFrame,
    targets: list[str],
    id_col: str,
    hh_id_col: str,
    summaries: list[ImputationSummary],
    *,
    additive: bool,
    person_to_hh: pl.DataFrame | None = None,
    cache_dir: Path | None = None,
) -> pl.DataFrame:
    """Impute all non-structural controls for one level (person or HH).

    For person-level controls, *person_to_hh* provides the (person_id →
    hh_id) lookup used to aggregate per-person probabilities to the
    household incidence row before overlay.
    """
    level_label = "person" if level == ControlLevel.PERSON else "household"
    controls = [c for c in resolve_targets(targets, level) if not c.structural]

    for ctrl in controls:
        member_cols = [f"{ctrl.name}__{m.lower()}" for _, m in ctrl.valid_members]
        if any(c not in predict_pivot.columns for c in member_cols):
            continue

        null_mask = predict_pivot.select(
            pl.sum_horizontal(member_cols).eq(0),
        ).to_series()
        n_null, n_total = int(null_mask.sum()), len(predict_pivot)

        if n_null == 0:
            summaries.append(ImputationSummary(ctrl.name, level_label, n_total, 0))
            continue

        null_pct = 100.0 * n_null / n_total
        logger.info(
            "%s control '%s': %d/%d null (%.1f%%)",
            level_label.title(),
            ctrl.name,
            n_null,
            n_total,
            null_pct,
        )
        if null_pct > 25:  # noqa: PLR2004
            logger.warning(
                "%s control '%s' has %.0f%% null — exceeds 25%%. "
                "Consider removing '%s' as a target.",
                level_label.title(),
                ctrl.name,
                null_pct,
                ctrl.name,
            )

        proba_df, cv_ll, cv_f1 = _train_and_predict(
            train_pivot,
            predict_pivot,
            null_mask,
            member_cols,
            id_col=id_col,
            cache_dir=cache_dir,
        )
        summaries.append(
            ImputationSummary(ctrl.name, level_label, n_total, n_null, cv_ll, cv_f1),
        )
        if proba_df.is_empty():
            continue

        # Person-level: aggregate per-person fractions to household totals
        if person_to_hh is not None:
            proba_df = proba_df.join(person_to_hh, on=id_col, how="left")
            proba_df = proba_df.group_by(hh_id_col).agg(
                [pl.col(c).sum() for c in member_cols],
            )

        result = _overlay_columns(
            result,
            proba_df,
            member_cols,
            hh_id_col,
            additive=additive,
        )

    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fill_null_incidence(
    survey_bundle: IncidenceBundle,
    pums_bundle: IncidenceBundle,
    targets: list[str],
    *,
    cache_dir: Path | None = None,
) -> tuple[pl.DataFrame, list[ImputationSummary]]:
    """Replace null-induced zeros in survey incidence with RF-predicted fractions.

    Parameters
    ----------
    survey_bundle : IncidenceBundle
        Survey incidence bundle (may have null-induced zeros).
    pums_bundle : IncidenceBundle
        PUMS incidence bundle (complete, no nulls — used as training data).
    targets : list[str]
        Control registry names.
    cache_dir : Path | None
        When set, fitted RF models and CV metrics are cached here so that
        repeated runs with the same PUMS data skip training entirely.

    Returns:
    -------
    tuple[pl.DataFrame, list[ImputationSummary]]
        Modified survey incidence table with fractional values replacing
        zeros for null-deficient rows, plus per-control imputation metadata.
    """
    result = survey_bundle.incidence
    hh_id_col = "hh_id" if "hh_id" in result.columns else "SERIALNO"
    person_id_col = "person_id" if "person_id" in survey_bundle.person_pivot.columns else "SPORDER"
    summaries: list[ImputationSummary] = []

    person_to_hh = survey_bundle.person_pivot.select(person_id_col, hh_id_col).unique()

    result = _impute_controls(
        result,
        ControlLevel.PERSON,
        train_pivot=pums_bundle.person_pivot,
        predict_pivot=survey_bundle.person_pivot,
        targets=targets,
        id_col=person_id_col,
        hh_id_col=hh_id_col,
        summaries=summaries,
        additive=True,
        person_to_hh=person_to_hh,
        cache_dir=cache_dir,
    )
    result = _impute_controls(
        result,
        ControlLevel.HOUSEHOLD,
        train_pivot=pums_bundle.incidence,
        predict_pivot=result,
        targets=targets,
        id_col=hh_id_col,
        hh_id_col=hh_id_col,
        summaries=summaries,
        additive=False,
        cache_dir=cache_dir,
    )

    return result, summaries
