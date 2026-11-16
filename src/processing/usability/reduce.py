"""The one join every level of both cascades is built from.

Each level of the cascades takes a verdict from a neighbouring table: a count
over the rows below it (a person with at least one complete day), or the one
row it belongs to (a tour inheriting its day's). Both are a reduction of the
neighbour's rows sharing a key, joined back and filled where there are none.
"""

import polars as pl

# The reduced value, between the join and the AND that consumes it.
_REDUCED = "_reduced"


def and_reduced(
    target: pl.DataFrame,
    source: pl.DataFrame,
    on: str,
    agg: pl.Expr,
    *,
    fill: bool,
    into: str,
) -> pl.DataFrame:
    """Return *target* with ``into`` ANDed with *agg* over the *source* rows sharing *on*.

    Args:
        target: The table being judged.
        source: The neighbouring table the verdict comes from.
        on: Key shared by both tables.
        agg: The reduction over each key's *source* rows, such as
            ``pl.col("usable_x").any()``; ``.first()`` reads the one row a
            record belongs to.
        fill: The answer for a *target* row with no *source* rows.
        into: The column written. One *target* does not carry yet starts from
            True, so the result is the reduction alone.
    """
    reduced = source.group_by(on).agg(agg.alias(_REDUCED))
    current = pl.col(into) if into in target.columns else pl.lit(value=True)
    return (
        target.join(reduced, on=on, how="left")
        .with_columns((current & pl.col(_REDUCED).fill_null(value=fill)).alias(into))
        .drop(_REDUCED)
    )
