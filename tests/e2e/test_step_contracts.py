"""Enforce that each step's declared ``requires`` contract matches reality.

The pipeline only checks ``requires`` when a project config opts in with
``validate_input: true``. No config in this repo did, so the contracts were never
exercised and silently rotted: ``format_ctramp`` declared ``tours.num_travelers``,
a column no step produces, while omitting the ~40 columns it genuinely reads.

These tests check the contracts directly, independent of any config, so a stale
declaration fails here rather than at the first site that turns validation on.
"""

import polars as pl
import pytest

import processing  # noqa: F401  # triggers @step registration
from pipeline.step_registry import STEP_REGISTRY

# Zone columns are named from the `taz_field` param (e.g. `o_TAZ1454`), so they
# cannot appear in a static contract and are excluded from these checks.
_DYNAMIC_SUFFIXES = ("_taz", "_maz", "_TAZ1454")

_CONTRACTS = [
    (step, table, sorted(tc.requires))
    for step, contract in sorted(STEP_REGISTRY.items())
    for table, tc in sorted(contract.tables.items())
    if tc.requires
]


@pytest.mark.parametrize(("step", "table", "columns"), _CONTRACTS, ids=lambda v: str(v)[:40])
def test_required_columns_are_present(full_result, step, table, columns):
    """Every column a step requires must exist on the table the pipeline builds.

    Catches the ``tours.num_travelers`` class of defect: a requirement that no
    step anywhere produces, which makes the step unrunnable under validation.
    """
    tables = full_result.as_dict_non_null()
    if table not in tables:
        pytest.skip(f"{table} not produced by the e2e profile")
    missing = [c for c in columns if c not in tables[table].columns]
    assert not missing, (
        f"Step '{step}' requires {table} columns that the pipeline never produces: "
        f"{missing}. Either the step no longer reads them (drop from `requires`) "
        f"or an upstream step stopped emitting them."
    )


def _is_required(fn, probe, params, baseline, fingerprint):
    """Whether dropping a column changed the outcome. A raise proves it was required."""
    try:
        return fingerprint(fn(**probe, **params)) != baseline
    except Exception:  # noqa: BLE001  # any failure means the column was load-bearing
        return True


def test_format_ctramp_requires_nothing_it_does_not_read(full_result):
    """Dropping any required column must actually break ``format_ctramp``.

    Guards against over-declaration. A column that can be removed with no effect
    on the formatted output is not a requirement, and declaring it makes the step
    fail for datasets that legitimately lack it.
    """
    from processing.formatting.ctramp.format_ctramp import format_ctramp

    fn = getattr(format_ctramp, "__wrapped__", format_ctramp)
    tables = full_result.as_dict_non_null()
    params = {
        "income_low_threshold": 30000,
        "income_med_threshold": 60000,
        "income_high_threshold": 100000,
        "income_survey_year_to_ctramp_year": 0.5319148936,
    }
    inputs = {
        name: tables.get(name, pl.DataFrame())
        for name in (
            "persons",
            "households",
            "unlinked_trips",
            "linked_trips",
            "tours",
            "joint_trips",
            "joint_tours",
            "days",
        )
    }

    def fingerprint(result):
        return {k: (v.height, sorted(v.columns)) for k, v in result.items()}

    baseline = fingerprint(fn(**inputs, **params))

    unused = []
    for table, tc in sorted(STEP_REGISTRY["format_ctramp"].tables.items()):
        for col in sorted(tc.requires):
            if col.endswith(_DYNAMIC_SUFFIXES) or col not in inputs[table].columns:
                continue
            if not _is_required(
                fn, {**inputs, table: inputs[table].drop(col)}, params, baseline, fingerprint
            ):
                unused.append(f"{table}.{col}")

    assert not unused, (
        f"format_ctramp declares columns it does not read: {unused}. "
        f"Remove them from `requires` so datasets lacking them are not rejected."
    )
