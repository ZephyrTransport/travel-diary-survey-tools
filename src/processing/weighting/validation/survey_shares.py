"""Whether each group has enough survey records to be weighted to its target.

A group that is a much smaller share of its area's survey than of the area's
PUMS needs weights far above the area's average, past the expansion cap, so it
cannot reach its target. ``survey_share_table`` compares the two shares for
every group of every target, and ``check_survey_shares`` stops the run, before
balancing, on any group below ``balancing.min_survey_share_ratio``.
"""

import polars as pl

from processing.weighting.controls.base import ControlLevel
from processing.weighting.controls.registry import CONTROLS
from processing.weighting.core.specs import ControlTotals

_AREA_TOTALS = {ControlLevel.HOUSEHOLD: "h_total", ControlLevel.PERSON: "p_total"}

#: Columns of the table, in the order the stop message and the report print them.
COLUMNS = (
    "area",
    "target",
    "group",
    "survey",
    "pums",
    "survey_share",
    "pums_share",
    "ratio",
)


def survey_share_table(
    seeds: dict[str | None, pl.DataFrame], control_totals: ControlTotals
) -> pl.DataFrame:
    """One row per area and group: survey and PUMS counts, both shares and their ratio.

    Counts are households for household targets and persons for person targets,
    each share taken against its area's total. ``ratio`` is the survey share over
    the PUMS share: 1.0 means the group is in the survey in proportion. Each row
    is the profile where the ratio is lowest, so one table covers the run.
    Groups with no target are left out.
    """
    totals = control_totals.totals.filter(pl.col("target_total") > 0).rename(
        {
            "zone_group_id": "area",
            "control_name": "target",
            "category": "group",
            "target_total": "pums",
        }
    )
    is_total = pl.col("target").is_in(list(_AREA_TOTALS.values()))
    level_total = {t: _AREA_TOTALS[CONTROLS[t].level] for t in totals["target"].unique()}
    pums_area = totals.filter(is_total).select(
        "area", pl.col("target").alias("level_total"), pl.col("pums").alias("pums_area")
    )
    groups = totals.filter(~is_total).with_columns(
        pl.col("target").replace_strict(level_total).alias("level_total")
    )
    # The seed column holding each group: structural totals keep their bare name.
    columns = {
        (t if CONTROLS[t].structural else f"{t}__{g}"): (t, g)
        for t, g in totals.select("target", "group").unique().iter_rows()
    }

    tables = []
    for seed in seeds.values():
        present = [c for c in columns if c in seed.columns]
        survey = (
            seed.group_by(pl.col("zone_group_id").alias("area"))
            .agg(pl.col(c).cast(pl.Float64).sum() for c in present)
            .unpivot(index="area", variable_name="column", value_name="survey")
            .with_columns(
                pl.col("column")
                .replace_strict({c: columns[c][0] for c in present})
                .alias("target"),
                pl.col("column").replace_strict({c: columns[c][1] for c in present}).alias("group"),
            )
            .drop("column")
        )
        survey_area = survey.filter(is_total).select(
            "area", pl.col("target").alias("level_total"), pl.col("survey").alias("survey_area")
        )
        tables.append(
            groups.join(survey, on=["area", "target", "group"], how="left")
            .with_columns(pl.col("survey").fill_null(0.0))
            .join(survey_area, on=["area", "level_total"])
            .join(pums_area, on=["area", "level_total"])
        )

    return (
        pl.concat(tables)
        .with_columns(
            (pl.col("survey") / pl.col("survey_area")).alias("survey_share"),
            (pl.col("pums") / pl.col("pums_area")).alias("pums_share"),
        )
        .with_columns((pl.col("survey_share") / pl.col("pums_share")).alias("ratio"))
        .sort("ratio")
        .group_by("area", "target", "group", maintain_order=True)
        .first()
        .select(COLUMNS)
    )


def check_survey_shares(table: pl.DataFrame, min_ratio: float) -> None:
    """Stop if any group's ratio is below *min_ratio*, printing those groups.

    Raises:
        ValueError: Naming every group below the limit, with its counts and shares.
    """
    low = table.filter(pl.col("ratio") < min_ratio)
    if low.is_empty():
        return
    msg = (
        f"Weighting stopped before balancing: {low.height} group(s) are too small a share "
        "of their area's survey to be weighted to their targets.\n\n"
        f"{format_table(low)}\n\n"
        "ratio = % of area's survey / % of area's PUMS; 1.0 means in proportion. Below "
        f"{min_ratio:g} (balancing.min_survey_share_ratio) a group cannot be weighted up to "
        "its target. Merge it with a neighbor in the controls, or lower "
        "min_survey_share_ratio to accept the miss."
    )
    raise ValueError(msg)


def table_cells(row: dict) -> list[str]:
    """One table row as display text, shared by the stop message and the report."""
    return [
        row["area"],
        row["target"],
        row["group"],
        f"{row['survey']:,.0f}",
        f"{row['pums']:,.0f}",
        f"{row['survey_share'] * 100:.1f}%",
        f"{row['pums_share'] * 100:.1f}%",
        f"{row['ratio']:.2f}",
    ]


def format_table(table: pl.DataFrame) -> str:
    """The table as aligned plain text, for the stop message."""
    header = [
        "area",
        "target",
        "group",
        "survey",
        "PUMS",
        "% of area's survey",
        "% of area's PUMS",
        "ratio",
    ]
    rows = [table_cells(r) for r in table.iter_rows(named=True)]
    widths = [max(len(str(x)) for x in col) for col in zip(header, *rows, strict=True)]
    left = 3  # area, target and group read left to right; numbers line up on the right
    lines = [
        "  ".join(
            str(cell).ljust(w) if i < left else str(cell).rjust(w)
            for i, (cell, w) in enumerate(zip(line, widths, strict=True))
        )
        for line in [header, *rows]
    ]
    return "\n".join(lines)
