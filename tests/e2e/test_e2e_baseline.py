"""Fail when the pipeline's numbers move, and say which ones.

Bit-exact by design: any drift fails, including a wanted change, which is promoted by
regenerating the baseline (see the failure message) so the movement shows in the PR diff.
"""

import copy
from pathlib import Path

import polars as pl
import pytest

from . import baseline


@pytest.fixture(scope="module")
def current(full_tables, full_output_dir) -> dict:
    """Fingerprint of this run's canonical tables and the formatted csvs it wrote.

    The formatted tables are read back from disk: they are the deliverable, and the
    pipeline result does not carry them.
    """
    return baseline.fingerprint({**full_tables, **_written_formatter_tables(full_output_dir)})


def _written_formatter_tables(output_dir) -> dict[str, pl.DataFrame]:
    """Read the CT-RAMP and DaySim csvs the run wrote, keyed by table name."""
    written = {}
    for subdir in ("ctramp", "daysim"):
        for path in sorted(Path(output_dir).glob(f"{subdir}/*.csv")):
            written[path.stem] = pl.read_csv(path)
    return written


class TestOutputIsUnchanged:
    """One assertion, over every table the pipeline produces."""

    def test_matches_the_committed_baseline(self, current):
        """Any difference in any column of any table fails, and is named."""
        if baseline.update_requested():
            path = baseline.save(current)
            pytest.skip(f"baseline rewritten: {path}")

        committed = baseline.load()
        if committed is None:
            # Fail, not skip, so deleting the committed file cannot turn this green.
            path = baseline.save(current)
            pytest.fail(
                f"no committed baseline. Wrote {path} from this run. "
                f"Inspect it, and if it is right, commit it. To rewrite an existing "
                f"baseline deliberately, re-run with {baseline.UPDATE_ENV}=1."
            )

        differences = baseline.diff(committed, current)
        assert not differences, baseline.report(differences)


class TestTheBaselineWouldCatchAChange:
    """The comparison reports each kind of change to a fingerprint, and none on a copy."""

    def test_a_changed_value_is_caught(self, current):
        """The case the hash exists for: same shape, different numbers."""
        tampered = copy.deepcopy(current)
        table, col = _any_column(tampered)
        tampered[table]["columns"][col]["hash"] = "0" * 16

        assert any(f"{table}.{col}" in line for line in baseline.diff(current, tampered))

    def test_a_changed_row_count_is_caught(self, current):
        tampered = copy.deepcopy(current)
        table = next(iter(tampered))
        tampered[table]["rows"] += 1

        assert any("rows" in line for line in baseline.diff(current, tampered))

    def test_a_dropped_column_is_caught(self, current):
        tampered = copy.deepcopy(current)
        table, col = _any_column(tampered)
        del tampered[table]["columns"][col]

        assert any("columns dropped" in line for line in baseline.diff(current, tampered))

    def test_an_identical_fingerprint_reports_nothing(self, current):
        """No false positives."""
        assert baseline.diff(current, copy.deepcopy(current)) == []


class TestTheFingerprintStoresNoRecords:
    """A fingerprint is committed, so it carries hashes and aggregates, never values."""

    def test_identifier_columns_have_no_histogram(self, current):
        offenders = [
            f"{t}.{c}"
            for t, spec in current.items()
            for c, col in spec["columns"].items()
            if c.endswith("_id") and "counts" in col
        ]

        assert offenders == []

    def test_coordinate_columns_have_no_histogram(self, current):
        offenders = [
            f"{t}.{c}"
            for t, spec in current.items()
            for c, col in spec["columns"].items()
            if ("lat" in c or "lon" in c) and "counts" in col
        ]

        assert offenders == []

    def test_histograms_are_bounded_and_aggregate(self, current):
        """A histogram over as many categories as rows would be the data itself."""
        for table, spec in current.items():
            for col, fingerprint in spec["columns"].items():
                counts = fingerprint.get("counts")
                if counts is None:
                    continue
                assert len(counts) <= baseline._MAX_HISTOGRAM_CARDINALITY, f"{table}.{col}"
                assert all(isinstance(v, int) for v in counts.values()), f"{table}.{col}"

    def test_every_table_is_sorted_by_a_unique_key(self, current):
        """Ties in a non-unique sort order would make the hash report false drift."""
        unsorted = [t for t, spec in current.items() if not spec["sorted_by"]]

        assert unsorted == [], f"no primary key registered for: {unsorted}"


def _any_column(fp: dict) -> tuple[str, str]:
    table = next(t for t, s in fp.items() if s["columns"])
    return table, next(iter(fp[table]["columns"]))
