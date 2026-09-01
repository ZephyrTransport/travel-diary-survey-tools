"""The weighting's own post-run checks, and the flag they have to read.

``weight_sanity_checks`` is the last thing ``compute_weights`` calls: it
compares the balanced totals against their controls and then asserts the
hierarchy identities the propagation is supposed to maintain. It had no test at
all, and that is how it came to call ``_check_hierarchy(tables)`` after that
function grew a required ``usability_flag_col`` -- a TypeError on the last line
of every weighting run, in a module the suite never entered.

Two things are pinned here. The entry point runs and threads the flag to both
checks, and the checks are reading the *same* universe the propagation weighted:
the identities only hold over records that carried weight, so a check gated on a
different column would fail on correct output. ``TestNoCallerOmitsTheFlag`` then
generalises the miss -- it walks the tree for any call that drops the argument,
so the next required parameter cannot quietly diverge from its callers.
"""

import ast
import pathlib

import polars as pl
import pytest

from processing.weighting.core.specs import ControlTotals
from processing.weighting.validation.weight_checks import (
    _check_hierarchy,
    _check_joint_sums,
    weight_sanity_checks,
)

FLAG = "ctramp_usable"


def _empty_totals() -> ControlTotals:
    """Controls with nothing to compare, so only the hierarchy checks run."""
    return ControlTotals(
        totals=pl.DataFrame(
            schema={
                "ctrl_geoid": pl.String,
                "control_name": pl.String,
                "category": pl.String,
                "target_total": pl.Float64,
            }
        ),
        pums_hh_count=0,
        pums_person_count=0,
        geo_ids=[],
    )


def _nothing_usable() -> dict[str, pl.DataFrame]:
    """A person carrying weight whose every day was dropped.

    This is the one shape where the flag changes the verdict. The split level
    expects a person's days to sum to their weight; here they sum to zero. Read
    through the flag the scope kept nothing, which is a reported shortfall --
    the weight is deliberately unrepresented below, never pooled onto another
    person. Read without it, every day looks usable and the same data is a
    hierarchy failure.
    """
    return {
        "households": pl.DataFrame(
            {"hh_id": [1], "hh_weight": [100.0], "ctrl_geoid": ["a"], "base_weight": [100.0]}
        ),
        "persons": pl.DataFrame(
            {"person_id": [1], "hh_id": [1], "person_weight": [100.0], FLAG: [True]}
        ),
        "days": pl.DataFrame(
            {
                "day_id": [1, 2],
                "person_id": [1, 1],
                "day_weight": [0.0, 0.0],
                FLAG: [False, False],
            }
        ),
    }


def _coherent_tables(*, usable: list[bool] | None = None) -> dict[str, pl.DataFrame]:
    """One household, two persons, one day each -- weights that reconcile.

    persons is a copy level (each person carries the household weight), days is
    a split level (a person's usable days sum back to the person weight).
    """
    usable = [True, True] if usable is None else usable
    n_usable = sum(usable) or 1
    return {
        "households": pl.DataFrame(
            {"hh_id": [1], "hh_weight": [100.0], "ctrl_geoid": ["a"], "base_weight": [100.0]}
        ),
        "persons": pl.DataFrame(
            {
                "person_id": [1, 2],
                "hh_id": [1, 1],
                "person_weight": [100.0, 100.0],
                FLAG: [True, True],
            }
        ),
        "days": pl.DataFrame(
            {
                "day_id": [1, 2],
                "person_id": [1, 1],
                "day_weight": [100.0 / n_usable if u else 0.0 for u in usable],
                FLAG: usable,
            }
        ),
    }


class TestTheEntryPointRuns:
    """It is the last line of every weighting run, and nothing covered it."""

    def test_coherent_weights_pass(self):
        """The baseline: correct output must not raise."""
        weight_sanity_checks(_coherent_tables(), _empty_totals(), [], FLAG)

    def test_the_flag_reaches_the_hierarchy_check(self):
        """The regression: this call raised TypeError before the flag was threaded.

        Uses the one shape whose verdict depends on the flag, so the argument is
        shown to arrive rather than merely to be accepted.
        """
        weight_sanity_checks(_nothing_usable(), _empty_totals(), [], FLAG)

    def test_a_broken_hierarchy_still_raises(self):
        """The check has teeth -- threading the flag did not defang it."""
        tables = _coherent_tables()
        tables["persons"] = tables["persons"].with_columns(pl.Series("person_weight", [100.0, 7.0]))

        with pytest.raises(ValueError, match="Weight cascade broken"):
            weight_sanity_checks(tables, _empty_totals(), [], FLAG)

    def test_missing_tables_are_skipped_not_crashed(self):
        """A partial run is legitimate; the checks log and return."""
        weight_sanity_checks(
            {"households": pl.DataFrame({"hh_id": [1]})}, _empty_totals(), [], FLAG
        )


class TestTheChecksReadTheWeightedUniverse:
    """Gating on a different column than the propagation used fails correct output."""

    def test_a_scope_that_kept_nothing_is_reported_not_failed(self):
        """Its weight is unrepresented on purpose, so there is no denominator."""
        _check_hierarchy(_nothing_usable(), FLAG)

    def test_the_same_data_fails_when_the_flag_is_not_found(self):
        """Every child then looks usable, so the shortfall reads as a broken sum.

        Naming a column no table carries is not itself an error -- a project may
        weight tables that were never gated -- which is exactly why the caller
        has to pass the column the propagation actually used.
        """
        with pytest.raises(ValueError, match="Weight cascade broken"):
            _check_hierarchy(_nothing_usable(), "a_profile_nobody_stamped")

    def test_joint_sums_skip_unusable_groupings(self):
        """_aggregate_up zeroes them regardless of members, so they never reconcile."""
        tables = {
            "linked_trips": pl.DataFrame(
                {
                    "linked_trip_id": [1, 2],
                    "joint_trip_id": [10, 10],
                    "linked_trip_weight": [5.0, 5.0],
                }
            ),
            "joint_trips": pl.DataFrame(
                {"joint_trip_id": [10], "joint_trip_weight": [0.0], FLAG: [False]}
            ),
        }

        _check_joint_sums(tables, FLAG)

    def test_a_usable_grouping_that_does_not_reconcile_raises(self):
        """A joint entity that survived must equal the members it kept."""
        tables = {
            "linked_trips": pl.DataFrame(
                {
                    "linked_trip_id": [1, 2],
                    "joint_trip_id": [10, 10],
                    "linked_trip_weight": [5.0, 5.0],
                }
            ),
            "joint_trips": pl.DataFrame(
                {"joint_trip_id": [10], "joint_trip_weight": [3.0], FLAG: [True]}
            ),
        }

        with pytest.raises(ValueError, match="Joint weight is not its members"):
            _check_joint_sums(tables, FLAG)


class TestNoCallerOmitsTheFlag:
    """No call anywhere may drop a required ``usability_flag_col``.

    The specific bug above was one call site; the shape of it is general. When a
    parameter becomes required, the callers are what has to change, and a caller
    inside a module the suite never enters will not say so until a real run
    dies on it. This walks the source instead of relying on coverage.
    """

    ROOTS = ("src", "tests", "projects", "scripts")
    PARAM = "usability_flag_col"

    def _functions_requiring_the_flag(self) -> dict[str, list[str]]:
        """Map function name -> positional parameter names, for those requiring it."""
        required: dict[str, list[str]] = {}
        for path in pathlib.Path("src").rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                    continue
                args = node.args
                positional = [a.arg for a in args.posonlyargs + args.args]
                n_defaults = len(args.defaults)
                required_positional = positional[: len(positional) - n_defaults or None]
                required_kwonly = [
                    a.arg
                    for a, d in zip(args.kwonlyargs, args.kw_defaults, strict=True)
                    if d is None
                ]
                if self.PARAM in required_positional + required_kwonly:
                    required[node.name] = positional
        return required

    def _calls_missing_the_flag(self, required: dict[str, list[str]]) -> list[str]:
        misses = []
        for root in self.ROOTS:
            for path in pathlib.Path(root).rglob("*.py"):
                tree = ast.parse(path.read_text(encoding="utf-8"))
                for node in ast.walk(tree):
                    if not isinstance(node, ast.Call):
                        continue
                    func = node.func
                    name = (
                        func.id
                        if isinstance(func, ast.Name)
                        else func.attr
                        if isinstance(func, ast.Attribute)
                        else None
                    )
                    if name not in required:
                        continue
                    by_keyword = {kw.arg for kw in node.keywords if kw.arg}
                    forwards_kwargs = any(kw.arg is None for kw in node.keywords)
                    positional = required[name]
                    index = positional.index(self.PARAM) if self.PARAM in positional else None
                    by_position = index is not None and len(node.args) > index
                    if self.PARAM in by_keyword or by_position or forwards_kwargs:
                        continue
                    misses.append(f"{path}:{node.lineno} {name}()")
        return misses

    def test_the_audit_finds_the_functions(self):
        """A guard that finds nothing to guard would pass forever."""
        required = self._functions_requiring_the_flag()

        assert "_check_hierarchy" in required
        assert "propagate_weights" in required

    def test_every_call_passes_it(self):
        """The guard proper: one entry per call site that dropped the argument."""
        misses = self._calls_missing_the_flag(self._functions_requiring_the_flag())

        assert misses == [], "call sites dropping a required usability_flag_col:\n" + "\n".join(
            misses
        )
