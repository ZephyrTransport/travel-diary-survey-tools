"""CDAP Results formatting for CT-RAMP.

Produces a ``cdap_results_ctramp`` table matching the schema of the
``cdapResults.csv`` file written by the CT-RAMP Java model::

    HHID, PersonID, PersonNum, PersonType, ActivityString

``PersonType`` is an integer (1-8) and ``ActivityString`` is H/M/N.
"""

import polars as pl


def format_cdap_results(persons_ctramp: pl.DataFrame) -> pl.DataFrame:
    """Derive cdapResults from the already-formatted persons table.

    Args:
        persons_ctramp: Formatted CT-RAMP persons table, expected to have
            ``hh_id``, ``person_id``, ``person_num``, ``person_type`` (int),
            and ``activity_pattern`` (H/M/N).

    Returns:
        DataFrame with columns ``HHID``, ``PersonID``, ``PersonNum``,
        ``PersonType`` (int), ``ActivityString`` (str).
    """
    cols = [
        pl.col("hh_id").alias("HHID"),
        pl.col("person_id").alias("PersonID"),
        pl.col("person_num").alias("PersonNum"),
        pl.col("person_type").cast(pl.Int32).alias("PersonType"),
        pl.col("activity_pattern").alias("ActivityString"),
    ]
    if "person_weight" in persons_ctramp.columns:
        cols.append(pl.col("person_weight"))
    return persons_ctramp.select(cols)
