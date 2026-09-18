"""Definitions of every statistic the diagnostics report uses.

One source for both places a definition appears: the Definitions section at the
top of the report, and the hover text on a column header or tile that uses the
statistic. A header links to its entry, so the hover is a shortcut to the
reference rather than the only place the definition lives.
"""

import html
from dataclasses import dataclass


@dataclass(frozen=True)
class Term:
    """One defined statistic.

    Attributes:
        key: Anchor id, ``g-<key>`` in the report.
        label: How the statistic is abbreviated in a header.
        name: Its full name.
        formula: The technical definition, as HTML.
        measures: What it tells a reader, as short HTML prose.
        group: The report area it belongs to, which decides whether it is listed.
    """

    key: str
    label: str
    name: str
    formula: str
    measures: str
    group: str


WEIGHTS = "Weights"
FIT = "Target fit"
IMPUTATION = "Imputation model"
CASCADE = "Weight cascade"
COMPARER = "Weight set comparer"

GLOSSARY: tuple[Term, ...] = (
    Term(
        "ef",
        "EF",
        "Expansion factor",
        "EF = w / w<sub>0</sub>, where w<sub>0</sub> is the base weight: the zone target "
        "divided by the number of responding households in the zone.",
        "How far weighting moved a household from its starting weight. Values at the "
        "configured limits mean weighting would have moved them further if allowed.",
        WEIGHTS,
    ),
    Term(
        "cv",
        "CV",
        "Coefficient of variation",
        "CV = sd(w) / mean(w)",
        "How unequal the weights are relative to their average. 0 means every record "
        "carries the same weight.",
        WEIGHTS,
    ),
    Term(
        "ess",
        "ESS %",
        "Effective sample size (Kish)",
        "ESS = (&Sigma;w)<sup>2</sup> / &Sigma;w<sup>2</sup>; &nbsp;ESS % = 100 &times; ESS / n. "
        "The matching design effect is DEFF = n / ESS.",
        "The number of equally weighted records that would give the same precision. "
        "Unequal weights reduce precision, and this shows by how much.",
        WEIGHTS,
    ),
    Term(
        "maxmed",
        "max/med",
        "Max over median weight",
        "max(w) / median(w)",
        "How much heavier the heaviest record is than a typical one. Large values mean a "
        "few records carry a large share of every estimate.",
        WEIGHTS,
    ),
    Term(
        "pcterr",
        "% Error",
        "Percent error",
        "100 &times; (Y&#770; &minus; T) / T, where Y&#770; is the weighted total and T the "
        "control target.",
        "How far a weighted total lands from its target, and in which direction.",
        FIT,
    ),
    Term(
        "mape",
        "MAPE",
        "Mean absolute percent error",
        "mean of |% error| over the control categories in a zone",
        "The average miss across all of a zone's targets.",
        FIT,
    ),
    Term(
        "p90",
        "P90",
        "90th percentile absolute percent error",
        "90th percentile of |% error| over the control categories in a zone",
        "How far the less accurate targets are from their values, ignoring the single worst.",
        FIT,
    ),
    Term(
        "maxerr",
        "Max",
        "Maximum absolute percent error",
        "max of |% error| over the control categories in a zone",
        "The single worst-fitting target in the zone.",
        FIT,
    ),
    Term(
        "se",
        "SE",
        "Standard error of the target",
        "SE = &radic;( 4/80 &middot; &Sigma;<sub>r</sub> (T<sub>r</sub> &minus; T)<sup>2</sup> ), "
        "over the 80 PUMS successive-difference replicate weights.",
        "How uncertain the target itself is. A miss within &plusmn;1 SE is inside the "
        "target's own sampling noise.",
        FIT,
    ),
    Term(
        "logloss",
        "log_loss",
        "Log loss",
        "&minus;(1/N) &Sigma;<sub>i</sub> ln p&#770;<sub>i</sub>(y<sub>i</sub>), "
        "stratified cross-validation on the PUMS training records.",
        "How much probability the model puts on the true category. Lower is better; 0 is perfect.",
        IMPUTATION,
    ),
    Term(
        "f1",
        "F1",
        "Macro F1 score",
        "mean over categories of 2PR / (P + R), P precision and R recall, stratified "
        "cross-validation on the PUMS training records.",
        "How often the model's best guess is the true category, counting every category "
        "equally however rare. 1 is perfect.",
        IMPUTATION,
    ),
    Term(
        "ratio",
        "Weight ratio to level above",
        "Redistribution ratio",
        "w / w<sub>above</sub>, where w<sub>above</sub> is the weight of the record it belongs "
        "to: a person's household, or a trip's day.",
        "How much a record's weight was increased to cover dropped records that belong to the "
        "same higher-level record. 1 means no records were dropped.",
        CASCADE,
    ),
    Term(
        "inherit",
        "Same as level above",
        "Share unchanged from level above",
        "share of records with |w / w<sub>above</sub> &minus; 1| &le; 1%",
        "The share of records with the same weight as the record they belong to.",
        CASCADE,
    ),
    Term(
        "fold",
        "Median / p90 / Max difference",
        "Ratio between weight sets",
        "max(A/B, B/A) for one record, over records weighted in both sets. The tiles give "
        "its median, 90th percentile and maximum.",
        "How many times larger one set's weight is than the other's for the same record. "
        "1&times; means the weights are equal.",
        COMPARER,
    ),
    Term(
        "rho",
        "Rank correlation",
        "Spearman rank correlation",
        "&rho; = corr(rank(A), rank(B))",
        "Whether the two sets rank records in the same order, regardless of the size of "
        "the weights. 1 means the same order.",
        COMPARER,
    ),
    Term(
        "r2",
        "R&sup2;",
        "Coefficient of determination",
        "R&sup2; = corr(ln A, ln B)<sup>2</sup>",
        "The share of variation in one set's log weights explained by the other's.",
        COMPARER,
    ),
    Term(
        "rma",
        "RMA fit",
        "Reduced major axis fit",
        "slope = sign(r) &middot; sd(ln A) / sd(ln B), through (mean ln B, mean ln A), "
        "where r = corr(ln A, ln B).",
        "How widely A's weights vary compared with B's; below 1, A's vary less. Swapping "
        "A and B gives the reciprocal. Unlike an ordinary regression line, it is not "
        "flattened when the two sets are weakly related.",
        COMPARER,
    ),
    Term(
        "iqr",
        "IQR",
        "Interquartile range of ln(A/B)",
        "Q<sub>3</sub> &minus; Q<sub>1</sub> of ln(A/B), within one decile of B",
        "How far apart the two sets are for the middle half of records in that band.",
        COMPARER,
    ),
)

_BY_KEY = {t.key: t for t in GLOSSARY}


def _plain(text: str) -> str:
    """HTML entities and tags reduced to plain text, for a ``title`` attribute."""
    stripped = text.replace("<sub>", "").replace("</sub>", "")
    stripped = stripped.replace("<sup>", "^").replace("</sup>", "")
    return html.escape(html.unescape(stripped), quote=True)


def term(key: str, label: str | None = None) -> str:
    """A header or tile label for a defined statistic.

    Hovering shows the definition; clicking jumps to the Definitions section.

    Args:
        key: The statistic's glossary key.
        label: Text to show, when it differs from the glossary's abbreviation.

    Returns:
        The label as HTML.
    """
    entry = _BY_KEY[key]
    title = f"{_plain(entry.name)}: {_plain(entry.measures)}"
    return (
        f'<a class="term" href="#g-{entry.key}" title="{title}">'
        f"{label if label is not None else entry.label}</a>"
    )


def glossary_groups(groups: set[str]) -> list[tuple[str, list[Term]]]:
    """The definitions to list, grouped, for the report areas actually rendered."""
    order = (WEIGHTS, FIT, IMPUTATION, CASCADE, COMPARER)
    return [
        (group, [t for t in GLOSSARY if t.group == group]) for group in order if group in groups
    ]
