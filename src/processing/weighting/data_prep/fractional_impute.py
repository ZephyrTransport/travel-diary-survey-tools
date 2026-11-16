"""Fractional imputation for null-deficient incidence rows.

When a survey record has a null control value (e.g. missing gender), the
incidence pivot produces all-zero member columns for that control — the
record effectively vanishes from that constraint.  This module replaces
those zeros with probability distributions predicted by a Random Forest
trained on complete PUMS data, implementing *fractional imputation*
(Kim & Fuller, 2004).

The max-entropy balancer accepts non-negative floats in the incidence
matrix, so fractional entries are valid without modification.

One forest per control, trained on PUMS split across the zone groups by the
crosswalk, each record counting by its PUMS weight. Its inputs are:

* the record's answers to the other controls;
* its zone, so each zone keeps its own mix;
* for a person control, what the person's other household members are for that
  control: their shares by category, how many are known, and how many there
  are. PUMS knows every member, so it is trained on masked copies too, in which
  only some members are known, as in the survey.

A person whose housemates answered is imputed close to them; one with no
known housemate falls back on zone and their own answers.

This is not the survey ``imputation`` step (:mod:`processing.imputation`),
and the two should not be read as versions of each other.  That step decides
a value for a missing field and writes it into canonical data, where every
downstream consumer sees it.  This one decides nothing: it spreads a record
across a control's categories, only inside the seed incidence matrix, only
for this balancing run, and the matrix is discarded once weights are fitted.
Control totals are never touched -- they come from PUMS and stay fixed.
It is a last resort that keeps a record in a constraint it would otherwise
drop out of, not a statement about what that record is.

Public API
----------
:func:`fill_null_incidence`
    Top-level orchestrator called from the weighting pipeline.
"""

import logging
from dataclasses import dataclass

import numpy as np
import polars as pl
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import f1_score, log_loss
from sklearn.model_selection import GroupKFold, cross_val_predict

from processing.weighting.controls.base import ControlLevel
from processing.weighting.controls.registry import resolve_targets
from processing.weighting.core.specs import ImputationSummary, IncidenceBundle

logger = logging.getLogger(__name__)

# Masked copies of each PUMS person trained on beside the original, so the model
# sees households where only some members are known, as the survey has them.
_MASKED_COPIES = 3
_MASK_SEED = 0

# The forest: number of trees, smallest leaf, and seed.
_N_ESTIMATORS = 100
_MIN_SAMPLES_LEAF = 20
_RANDOM_STATE = 42

# The PUMS column naming each record's household, which CV keeps in one fold.
_HOUSEHOLD_COL = "SERIALNO"


@dataclass(frozen=True)
class TrainedModel:
    """A forest trained in this run for one control, with its CV scores."""

    model: RandomForestClassifier
    feature_cols: list[str]
    cv_ll: float | None
    cv_f1: float | None


# ---------------------------------------------------------------------------
# RF training helpers
# ---------------------------------------------------------------------------


def _cv_diagnostics(
    clf: RandomForestClassifier,
    x_train: np.ndarray,
    y_train: np.ndarray,
    w_train: np.ndarray,
    groups: np.ndarray,
) -> tuple[float | None, float | None]:
    """CV on training data → (log_loss, f1_macro), each record by its weight.

    Each household's rows stay in one fold (*groups*), so no member's value
    reaches the scoring of another member of the same household.

    Automatically reduces fold count when a class has fewer than 5
    samples, and skips CV entirely when any class has < 2  samples.
    """
    class_counts = np.bincount(y_train)
    min_per_class = int(class_counts[class_counts > 0].min())
    cv_folds = min(5, min_per_class, len(np.unique(groups)))

    if cv_folds < 2:  # noqa: PLR2004
        logger.info("  Skipping CV: smallest class has only %d sample(s)", min_per_class)
        return None, None

    cv_proba = cross_val_predict(
        clf,
        x_train,
        y_train,
        groups=groups,
        cv=GroupKFold(n_splits=cv_folds),
        method="predict_proba",
        params={"sample_weight": w_train},
    )
    cv_pred = cv_proba.argmax(axis=1)
    cv_ll = float(log_loss(y_train, cv_proba, sample_weight=w_train))
    cv_f1 = float(f1_score(y_train, cv_pred, average="macro", sample_weight=w_train))
    logger.info(
        "  CV diagnostics: log_loss=%.4f, f1_macro=%.4f (%d rows, %d features, %d folds)",
        cv_ll,
        cv_f1,
        len(y_train),
        x_train.shape[1],
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

    # One thread: threads sum the trees' probabilities in finishing order,
    # which changes the last digits between runs.
    clf.set_params(n_jobs=1)
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
    weight_col: str,
    *,
    models: dict[str, TrainedModel] | None = None,
) -> tuple[pl.DataFrame, float | None, float | None]:
    """Train an RF on complete rows and predict probabilities for null rows.

    The forest is ``_N_ESTIMATORS`` decision trees, each grown on a resample of
    the PUMS training rows. A tree splits its rows by yes/no questions about the
    inputs until no split is allowed; each final group (a leaf) answers with the
    weighted category mix of its rows, and a prediction averages the leaves a
    person lands in across all trees.

    * Each training record counts by its *weight_col*, so the forest learns the
      population's mix rather than the PUMS sample's.
    * A split is made only if both sides keep at least ``_MIN_SAMPLES_LEAF`` rows.
      Trees grow deep where data is plentiful and stop early where it is thin,
      so every leaf's answer is a mix of several PUMS persons and no category
      comes out exactly 0 for want of one.
    * ``_HOUSEHOLD_COL`` names each record's household, which the CV scores keep
      in one fold.

    *models* holds the forests already trained in this run, by control. Every
    profile's seed is imputed from the same PUMS, so a later profile reuses the
    first one's forest and its CV scores instead of training it again. Nothing
    outlives the run.

    Returns ``(proba_df, log_loss, f1_macro)``.

    Raises:
        ValueError: If *train_pivot* carries no *weight_col*, or a forest
            already trained for this control used other inputs.
    """
    if weight_col not in train_pivot.columns:
        msg = f"PUMS training records for fractional imputation carry no {weight_col!r}."
        raise ValueError(msg)
    feature_cols = sorted(c for c in train_pivot.columns if "__" in c and c not in member_cols)
    if not feature_cols:
        logger.warning("No feature columns for %s — skipping", member_cols)
        return pl.DataFrame(), None, None

    x_train = train_pivot.select(feature_cols).to_numpy().astype(np.float32)
    y_train = train_pivot.select(member_cols).to_numpy().argmax(axis=1)
    w_train = train_pivot[weight_col].cast(pl.Float64).to_numpy()
    groups = train_pivot[_HOUSEHOLD_COL].to_numpy()
    # The pivot's row order varies run to run, and the forest samples rows by
    # position. Sorting by the values fixes it; identical rows are interchangeable.
    order = np.lexsort(np.column_stack([x_train, y_train, w_train]).T[::-1])
    x_train, y_train, w_train = x_train[order], y_train[order], w_train[order]
    groups = groups[order]

    if len(np.unique(y_train)) < 2:  # noqa: PLR2004
        logger.warning("Only 1 class for %s — skipping", member_cols)
        return pl.DataFrame(), None, None

    control = member_cols[0].split("__")[0]
    trained = (models or {}).get(control)
    if trained is not None and trained.feature_cols != feature_cols:
        msg = f"The forest trained for {control} in this run used other inputs."
        raise ValueError(msg)
    if trained is None:
        clf = RandomForestClassifier(
            n_estimators=_N_ESTIMATORS,
            random_state=_RANDOM_STATE,
            min_samples_leaf=_MIN_SAMPLES_LEAF,
            n_jobs=-1,
        )
        clf.fit(x_train, y_train, sample_weight=w_train)
        cv_ll, cv_f1 = _cv_diagnostics(clf, x_train, y_train, w_train, groups)
        trained = TrainedModel(clf, feature_cols, cv_ll, cv_f1)
        if models is not None:
            models[control] = trained
    clf, cv_ll, cv_f1 = trained.model, trained.cv_ll, trained.cv_f1

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


def _with_other_members(
    df: pl.DataFrame,
    member_cols: list[str],
    control: str,
    household: list[str],
    known: pl.Expr,
) -> pl.DataFrame:
    """Add what each person's other household members are, for one control.

    ``others__<category column>`` is the share of the other members whose value
    is *known* that fall in that category, or 0 when none is known.
    ``others_known__<control>`` counts those members, and
    ``others_total__<control>`` every other member of the household.
    """
    seen = known.cast(pl.Int32)
    n_known = seen.sum().over(household) - seen
    shares = [
        pl.when(n_known > 0)
        .then(((pl.col(c) * seen).sum().over(household) - pl.col(c) * seen) / n_known)
        .otherwise(0.0)
        .alias(f"others__{c}")
        for c in member_cols
    ]
    return df.with_columns(
        *shares,
        n_known.alias(f"others_known__{control}"),
        (pl.len().over(household).cast(pl.Int32) - 1).alias(f"others_total__{control}"),
    )


def _masked_copies(
    pums_persons: pl.DataFrame,
    member_cols: list[str],
    control: str,
    weight_col: str,
) -> pl.DataFrame:
    """PUMS persons for one person control, as the original and masked copies.

    The original row knows every other member of the household. Each of the
    :data:`_MASKED_COPIES` copies keeps a random number of the household's
    members known, from none to all, and treats the rest as unknown. Every row
    carries an equal share of the person's weight, so each person still adds up
    to their own weight. Seeded, so runs repeat.
    """
    household = [_HOUSEHOLD_COL, "zone_group_id"]
    base = pums_persons.sort(*household, "SPORDER")
    sizes = base.group_by(household, maintain_order=True).len()
    rng = np.random.default_rng(_MASK_SEED)
    rows = [_with_other_members(base, member_cols, control, household, pl.lit(value=True))]
    for _ in range(_MASKED_COPIES):
        keep = sizes.select(
            *household, pl.Series("_keep", rng.integers(0, sizes["len"].to_numpy() + 1))
        )
        copy = (
            base.with_columns(pl.Series("_draw", rng.random(base.height)))
            .with_columns(pl.col("_draw").rank("ordinal").over(household).alias("_rank"))
            .join(keep, on=household, how="left")
        )
        known = pl.col("_rank") <= pl.col("_keep")
        rows.append(
            _with_other_members(copy, member_cols, control, household, known).drop(
                "_draw", "_rank", "_keep"
            )
        )
    return pl.concat(rows).with_columns(pl.col(weight_col) / (_MASKED_COPIES + 1))


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
    weight_col: str,
    additive: bool,
    person_to_hh: pl.DataFrame | None = None,
    models: dict[str, TrainedModel] | None = None,
) -> pl.DataFrame:
    """Impute all non-structural controls for one level (person or HH).

    For person-level controls, *person_to_hh* provides the (person_id →
    hh_id) lookup used to aggregate per-person probabilities to the
    household incidence row before overlay. A person control's model also
    takes what the person's other household members are for that control
    (:func:`_with_other_members`), trained on masked PUMS copies
    (:func:`_masked_copies`).
    """
    level_label = "person" if level == ControlLevel.PERSON else "household"
    controls = [c for c in resolve_targets(targets, level) if not c.structural and c.imputable]

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

        # A PUMS row in no category holds a value none of the control's categories
        # takes; training would learn it as the first category.
        uncoded = train_pivot.filter(pl.sum_horizontal(member_cols) != 1).height
        if uncoded:
            msg = (
                f"{uncoded:,} of {train_pivot.height:,} PUMS {level_label} records sit in no "
                f"'{ctrl.name}' category: its PUMS mapping gives them a value no category takes."
            )
            raise ValueError(msg)

        train, predict = train_pivot, predict_pivot
        if level == ControlLevel.PERSON:
            # Built per control, so another control's members never feed this one.
            train = _masked_copies(train_pivot, member_cols, ctrl.name, weight_col)
            known = pl.sum_horizontal(member_cols) == 1
            predict = _with_other_members(predict_pivot, member_cols, ctrl.name, [hh_id_col], known)

        proba_df, cv_ll, cv_f1 = _train_and_predict(
            train,
            predict,
            null_mask,
            member_cols,
            id_col=id_col,
            weight_col=weight_col,
            models=models,
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


def _with_zone_columns(df: pl.DataFrame, zone_ids: list[str]) -> pl.DataFrame:
    """Add one 0/1 ``zone__<id>`` column per zone, read off ``zone_group_id``."""
    return df.with_columns(
        (pl.col("zone_group_id") == z).fill_null(value=False).cast(pl.Int32).alias(f"zone__{z}")
        for z in zone_ids
    )


def fill_null_incidence(
    survey_bundle: IncidenceBundle,
    pums_households: pl.DataFrame,
    pums_persons: pl.DataFrame,
    survey_zones: pl.DataFrame,
    targets: list[str],
    *,
    models: dict[str, TrainedModel] | None = None,
) -> tuple[pl.DataFrame, list[ImputationSummary]]:
    """Replace null-induced zeros in survey incidence with RF-predicted fractions.

    Every control's model also takes the record's zone, as one 0/1
    ``zone__<id>`` column per zone, so it learns each zone's own mix. The
    zone columns exist only while imputing and are not in the result.

    Parameters
    ----------
    survey_bundle : IncidenceBundle
        Survey incidence bundle (may have null-induced zeros).
    pums_households : pl.DataFrame
        PUMS household incidence (complete, no nulls), trained on for the
        household controls. Split across zones (``zone_group_id``), each record
        counting by its ``WGTP``.
    pums_persons : pl.DataFrame
        PUMS person pivot (complete, no nulls), trained on for the person
        controls. Split across zones (``zone_group_id``), each record counting by
        its ``PWGTP``.
    survey_zones : pl.DataFrame
        Each survey household's zone: the household id and ``zone_group_id``.
    targets : list[str]
        Control registry names.
    models : dict[str, TrainedModel] | None
        The forests trained so far in this run, by control. Pass the same dict
        for every profile so each forest is trained once per run.

    Returns:
    -------
    tuple[pl.DataFrame, list[ImputationSummary]]
        Modified survey incidence table with fractional values replacing
        zeros for null-deficient rows, plus per-control imputation metadata.

    Raises:
    -------
    ValueError
        If the PUMS frames carry no ``zone_group_id``, or a survey household has
        no zone the PUMS covers.
    """
    for name, frame in (("pums_households", pums_households), ("pums_persons", pums_persons)):
        if "zone_group_id" not in frame.columns:
            msg = f"{name} carries no zone_group_id; split PUMS across zones before imputing."
            raise ValueError(msg)
    zone_ids = sorted(pums_households["zone_group_id"].drop_nulls().unique().to_list())
    zone_cols = [f"zone__{z}" for z in zone_ids]

    result = survey_bundle.incidence
    hh_id_col = "hh_id" if "hh_id" in result.columns else "SERIALNO"
    person_id_col = "person_id" if "person_id" in survey_bundle.person_pivot.columns else "SPORDER"
    summaries: list[ImputationSummary] = []

    zones = survey_zones.select(hh_id_col, "zone_group_id")
    result = result.join(zones, on=hh_id_col, how="left")
    unzoned = result.filter(~pl.col("zone_group_id").is_in(zone_ids)).height
    if unzoned:
        msg = f"{unzoned} survey household(s) have no zone the PUMS covers, so none to impute by."
        raise ValueError(msg)
    result = _with_zone_columns(result, zone_ids)
    survey_persons = _with_zone_columns(
        survey_bundle.person_pivot.join(zones, on=hh_id_col, how="left"), zone_ids
    )
    pums_households = _with_zone_columns(pums_households, zone_ids)
    pums_persons = _with_zone_columns(pums_persons, zone_ids)

    person_to_hh = survey_bundle.person_pivot.select(person_id_col, hh_id_col).unique()

    result = _impute_controls(
        result,
        ControlLevel.PERSON,
        train_pivot=pums_persons,
        predict_pivot=survey_persons,
        targets=targets,
        id_col=person_id_col,
        hh_id_col=hh_id_col,
        summaries=summaries,
        weight_col="PWGTP",
        additive=True,
        person_to_hh=person_to_hh,
        models=models,
    )
    result = _impute_controls(
        result,
        ControlLevel.HOUSEHOLD,
        train_pivot=pums_households,
        predict_pivot=result,
        targets=targets,
        id_col=hh_id_col,
        hh_id_col=hh_id_col,
        summaries=summaries,
        weight_col="WGTP",
        additive=False,
        models=models,
    )

    return result.drop(*zone_cols, "zone_group_id"), summaries
