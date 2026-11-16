"""Each step's ``requires`` contract names only columns that exist and that the step reads.

The step decorator checks ``requires`` on every call, so the e2e run itself checks the
steps it executes; these cover the steps it does not run, and over-declaration.
"""

import polars as pl
import pytest

import processing  # noqa: F401  # triggers @step registration
from pipeline.decoration import Tables
from pipeline.step_registry import STEP_REGISTRY, is_config_named
from tests.e2e.conftest import STEP_ORDER

_CONTRACTS = [
    (step, table, sorted(tc.requires))
    for step, contract in sorted(STEP_REGISTRY.items())
    if step not in STEP_ORDER
    for table, tc in sorted(contract.tables.items())
    if tc.requires
]


@pytest.mark.parametrize(("step", "table", "columns"), _CONTRACTS, ids=lambda v: str(v)[:40])
def test_required_columns_are_present(full_tables, step, table, columns):
    """Every column a step requires exists on the table the pipeline builds."""
    missing = [c for c in columns if c not in full_tables[table].columns and not is_config_named(c)]
    assert not missing, (
        f"Step '{step}' requires {table} columns that the pipeline never produces: "
        f"{missing}. Either the step no longer reads them (drop from `requires`) "
        f"or an upstream step stopped emitting them."
    )


def _is_required(fn, probe, params, baseline, fingerprint):
    """Whether dropping a column changed the outcome. A raise proves it was required."""
    try:
        return fingerprint(fn(probe, params)) != baseline
    except Exception:  # noqa: BLE001  # any failure means the column was load-bearing
        return True


def test_format_ctramp_requires_nothing_it_does_not_read(full_tables, standard_config):
    """Dropping any required column changes ``format_ctramp``'s output or makes it fail.

    A column that can be removed with no effect is not a requirement, and declaring it
    rejects datasets that legitimately lack it.
    """
    from processing.formatting.ctramp.format_ctramp import format_ctramp

    contract = STEP_REGISTRY["format_ctramp"]

    # The body itself, past the decorator's presence check, so a dropped column
    # shows whether the step reads it.
    def fn(probe, params):
        return format_ctramp.__wrapped__(
            tables=Tables("format_ctramp", set(probe), probe), **params
        )

    # The e2e stamps the ctramp profile, and the step requires the one it gates on.
    params = {"ctramp": standard_config.model_copy(update={"usability_profile": "ctramp"})}
    inputs = {name: full_tables.get(name, pl.DataFrame()) for name in contract.taken}

    def fingerprint(result):
        return {k: (v.height, sorted(v.columns)) for k, v in result.items()}

    baseline = fingerprint(fn(inputs, params))

    unused = []
    for table, tc in sorted(contract.taken.items()):
        for col in sorted(tc.requires):
            if is_config_named(col) or col not in inputs[table].columns:
                continue
            if not _is_required(
                fn, {**inputs, table: inputs[table].drop(col)}, params, baseline, fingerprint
            ):
                unused.append(f"{table}.{col}")

    assert not unused, (
        f"format_ctramp declares columns it does not read: {unused}. "
        f"Remove them from `requires` so datasets lacking them are not rejected."
    )
