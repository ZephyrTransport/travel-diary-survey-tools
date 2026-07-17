"""Auto Ownership Results formatting for CT-RAMP.

Produces an ``ao_results_ctramp`` table matching the schema of the
``aoResults.csv`` file written by the CT-RAMP Java model::

    HHID, AO

``AO`` is the number of vehicles owned by the household (0-4+).
"""

import polars as pl


def format_ao_results(households_ctramp: pl.DataFrame) -> pl.DataFrame:
    """Derive aoResults from the already-formatted households table.

    Args:
        households_ctramp: Formatted CT-RAMP households table, expected to have
            ``hh_id`` and ``autos``.

    Returns:
        DataFrame with columns ``HHID``, ``AO``.
    """
    return households_ctramp.select(
        pl.col("hh_id").alias("HHID"),
        pl.col("autos").alias("AO"),
    )
