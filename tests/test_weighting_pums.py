"""PUMS: reading the official files, and checking the records belong together.

The official files are read from the census folder, downloaded there when
missing, and written in the Census API's coding. The records are then checked
for misattribution with facts true of any correct Census extract.
"""

import io
import random
import zipfile

import polars as pl
import pytest

from processing.weighting.data_prep import pums_data
from processing.weighting.data_prep.pums_data import (
    _cast_pums_types,
    check_pums_complete,
    check_pums_integrity,
    pums_files,
    read_pums,
)


def _households(rng: random.Random, n: int = 400) -> pl.DataFrame:
    """Households whose NP agrees with the person table built beside them."""
    sizes = [rng.randint(1, 5) for _ in range(n)]
    weights = [float(rng.randint(5, 300)) for _ in range(n)]
    return pl.DataFrame(
        {
            "SERIALNO": [f"2023HU{i:07d}" for i in range(n)],
            "NP": sizes,
            "TYPEHUGQ": [1] * n,
            "WGTP": weights,
            # a replicate is the full weight perturbed, so it tracks it closely
            **{
                f"WGTP{r}": [w * (1 + rng.uniform(-0.15, 0.15)) for w in weights]
                for r in range(1, 6)
            },
        }
    )


def _persons(rng: random.Random, hh: pl.DataFrame) -> pl.DataFrame:
    rows = []
    for serial, size in zip(hh["SERIALNO"], hh["NP"], strict=True):
        rows.extend((serial, order) for order in range(1, size + 1))
    weights = [float(rng.randint(5, 300)) for _ in rows]
    return pl.DataFrame(
        {
            "SERIALNO": [r[0] for r in rows],
            "SPORDER": [r[1] for r in rows],
            "PWGTP": weights,
            **{
                f"PWGTP{r}": [w * (1 + rng.uniform(-0.15, 0.15)) for w in weights]
                for r in range(1, 6)
            },
        }
    )


@pytest.fixture
def sound() -> tuple[pl.DataFrame, pl.DataFrame]:
    """A household table and the person table that agrees with it."""
    rng = random.Random(20260831)  # noqa: S311
    hh = _households(rng)
    return hh, _persons(rng, hh)


class TestSoundRecordsPass:
    """The gate has to be quiet on correct data or it will be switched off."""

    def test_consistent_pums_is_accepted(self, sound):
        """Nothing to report on records that belong together."""
        check_pums_integrity(*sound)

    def test_a_vacant_unit_is_not_a_mismatch(self, sound):
        """NP == 0 marks a vacant unit, which correctly has no person records."""
        hh, persons = sound
        hh = hh.with_columns(
            pl.when(pl.col("SERIALNO") == hh["SERIALNO"][0])
            .then(0)
            .otherwise(pl.col("NP"))
            .alias("NP")
        )
        persons = persons.filter(pl.col("SERIALNO") != hh["SERIALNO"][0])

        check_pums_integrity(hh, persons)

    def test_absent_columns_are_not_failures(self, sound):
        """A query without replicate weights is legitimate, not suspicious."""
        hh, persons = sound
        reps = [c for c in hh.columns if c.startswith("WGTP") and c != "WGTP"]

        check_pums_integrity(hh.drop(reps), persons)


class TestMisattributionIsCaught:
    """Each check corresponds to a way the real corruption showed itself."""

    def test_household_size_disagreeing_with_its_persons(self, sound):
        """The sharpest signal: two tables that only agree when both are right."""
        hh, persons = sound
        hh = hh.with_columns((pl.col("NP") + 1).alias("NP"))

        with pytest.raises(ValueError, match="disagrees with their own number"):
            check_pums_integrity(hh, persons)

    def test_a_type_code_outside_its_domain(self, sound):
        """TYPEHUGQ holding 3213."""
        hh, persons = sound
        hh = hh.with_columns(
            pl.when(pl.col("SERIALNO") == hh["SERIALNO"][0])
            .then(3213)
            .otherwise(pl.col("TYPEHUGQ"))
            .alias("TYPEHUGQ")
        )

        with pytest.raises(ValueError, match="TYPEHUGQ outside"):
            check_pums_integrity(hh, persons)

    @pytest.mark.parametrize(
        ("table", "column", "seed", "match"),
        [
            pytest.param("households", "WGTP3", 7, "do not track WGTP", id="household"),
            pytest.param("persons", "PWGTP2", 11, "do not track PWGTP", id="person"),
        ],
    )
    def test_shuffled_replicate_weights(self, sound, table, column, seed, match):
        """Every value is a real weight, on the wrong record; only the full weight tells."""
        hh, persons = sound
        if table == "households":
            hh = hh.with_columns(hh[column].shuffle(seed=seed).alias(column))
        else:
            persons = persons.with_columns(persons[column].shuffle(seed=seed).alias(column))

        with pytest.raises(ValueError, match=match):
            check_pums_integrity(hh, persons)

    @pytest.mark.parametrize(
        ("table", "match"),
        [
            pytest.param("households", "SERIALNO repeats in households", id="household"),
            pytest.param("persons", r"\(SERIALNO, SPORDER\) repeats", id="person"),
        ],
    )
    def test_a_repeated_key(self, sound, table, match):
        """A key that repeats cannot join the chunks back together.

        SERIALNO alone repeats per member, so persons need SPORDER as well.
        """
        hh, persons = sound
        if table == "households":
            hh = pl.concat([hh, hh.head(1)])
        else:
            persons = pl.concat([persons, persons.head(1)])

        with pytest.raises(ValueError, match=match):
            check_pums_integrity(hh, persons)

    def test_the_error_names_every_problem_at_once(self, sound):
        """One fetch produces several symptoms; fixing them one run at a time is slow."""
        hh, persons = sound
        hh = hh.with_columns(
            (pl.col("NP") + 1).alias("NP"),
            pl.lit(99).alias("TYPEHUGQ"),
            hh["WGTP3"].shuffle(seed=3).alias("WGTP3"),
        )

        with pytest.raises(ValueError, match="not internally consistent") as excinfo:
            check_pums_integrity(hh, persons)

        message = str(excinfo.value)
        assert "TYPEHUGQ outside" in message
        assert "disagrees with their own number" in message
        assert "do not track WGTP" in message


class TestReplicatesMustAgreeWithEachOther:
    """Replicates are compared with each other, since how well they track the weight varies."""

    def test_a_split_population_is_caught_even_above_the_absolute_bound(self, sound):
        """Both groups track their weight well enough to clear 0.5, but not each other."""
        hh, persons = sound
        # one replicate degraded part-way: still correlated, but visibly less so
        weakened = hh["WGTP4"] * 0.5 + hh["WGTP4"].shuffle(seed=5) * 0.5
        hh = hh.with_columns(weakened.alias("WGTP4"))

        with pytest.raises(ValueError, match="disagree with each other"):
            check_pums_integrity(hh, persons)


def _official_zip(rows: list[dict[str, str]]) -> bytes:
    """An official-style zip: one CSV, every value text, blanks left blank."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("psam.csv", pl.DataFrame(rows).write_csv())
    return buf.getvalue()


def _write_official(census_dir, pums_year: int, *, pre2020: bool = False) -> None:
    """Two households (one person, two persons) in the official files' coding."""
    state, kind = ("ST", "TYPE") if pre2020 else ("STATE", "TYPEHUGQ")
    hh_vars = {
        state if v == "STATE" else kind if v == "TYPEHUGQ" else v for v in pums_data._HH_VARS
    }
    per_vars = {state if v == "STATE" else v for v in pums_data._PERSON_VARS}
    households = [
        dict.fromkeys(sorted(hh_vars), "1")
        | {
            "SERIALNO": f"S{i}",
            "PUMA": "00101",
            state: "06",
            "NP": str(np),
            "WGTP": "100",
            kind: "1",
            "ADJINC": "1019518",
            "HINCP": hincp,
        }
        for i, (np, hincp) in enumerate([(1, "50000"), (2, "")])
    ]
    persons = [
        dict.fromkeys(sorted(per_vars), "1")
        | {
            "SERIALNO": f"S{i}",
            "SPORDER": str(k),
            "PUMA": "00101",
            state: "06",
            "PWGTP": "90",
            "ESR": esr,
        }
        for i, n in enumerate([1, 2])
        for k, esr in zip(range(1, n + 1), ["1", ""], strict=False)
    ]
    hh_path, per_path = pums_files(census_dir, "06", pums_year)
    hh_path.parent.mkdir(parents=True)
    hh_path.write_bytes(_official_zip(households))
    per_path.write_bytes(_official_zip(persons))


class TestReadingTheOfficialFiles:
    """The official files read as the API data they were checked against."""

    def test_files_are_named_as_the_census_names_them(self, tmp_path):
        """California 2023 is pums_2023/csv_hca.zip and csv_pca.zip."""
        assert pums_files(tmp_path, "06", 2023) == (
            tmp_path / "pums_2023" / "csv_hca.zip",
            tmp_path / "pums_2023" / "csv_pca.zip",
        )

    def test_values_are_written_as_the_api_sends_them(self, tmp_path):
        """A blank income is -60000, a blank employment status 0, and ADJINC 1.019518.

        The file writes ADJINC as 1019518, with six implied decimals.
        """
        _write_official(tmp_path, 2023)

        hh, per = read_pums(tmp_path, "06", 2023)

        assert hh.sort("SERIALNO")["HINCP"].to_list() == [50000.0, -60000.0]
        assert sorted(per["ESR"].to_list()) == [0, 1, 1]
        assert hh["ADJINC"].unique().to_list() == [1.019518]

    def test_pre_2020_names_are_read_as_the_current_ones(self, tmp_path):
        """2019 files say ST and TYPE where later ones say STATE and TYPEHUGQ."""
        _write_official(tmp_path, 2019, pre2020=True)

        hh, per = read_pums(tmp_path, "06", 2019)

        assert {"STATE", "TYPEHUGQ"} <= set(hh.columns)
        assert "STATE" in per.columns

    def test_a_missing_variable_is_named(self, tmp_path):
        """A file without a variable the controls need stops the read."""
        _write_official(tmp_path, 2023)
        hh_path, _ = pums_files(tmp_path, "06", 2023)
        with zipfile.ZipFile(hh_path) as zf:
            table = pl.read_csv(zf.read("psam.csv"), infer_schema_length=0)
        hh_path.write_bytes(_official_zip(table.drop("VEH").to_dicts()))

        with pytest.raises(ValueError, match=r"lacks \['VEH'\]"):
            read_pums(tmp_path, "06", 2023)


class TestAMissingFileIsDownloaded:
    """A fresh clone runs without fetching anything by hand."""

    def _serve(self, monkeypatch, files: dict[str, bytes]) -> list[str]:
        asked: list[str] = []

        class Response:
            def __init__(self, content):
                self.content = content

            def raise_for_status(self):
                pass

        def fake_get(url, timeout):  # noqa: ARG001
            asked.append(url)
            return Response(files[url.rsplit("/", 1)[1]])

        monkeypatch.setattr(pums_data.requests, "get", fake_get)
        return asked

    def test_it_is_fetched_from_the_official_location_and_kept(self, monkeypatch, tmp_path):
        """Both files come from the year's 1-Year folder; a second read asks for nothing."""
        source = tmp_path / "source"
        _write_official(source, 2023)
        hh_path, per_path = pums_files(source, "06", 2023)
        census_dir = tmp_path / "census"
        asked = self._serve(
            monkeypatch,
            {"csv_hca.zip": hh_path.read_bytes(), "csv_pca.zip": per_path.read_bytes()},
        )

        read_pums(census_dir, "06", 2023)
        read_pums(census_dir, "06", 2023)

        base = "https://www2.census.gov/programs-surveys/acs/data/pums/2023/1-Year/"
        assert asked == [base + "csv_hca.zip", base + "csv_pca.zip"]
        assert not list(census_dir.rglob("*.part"))


class TestConversionIsStrict:
    """A value that is not a number stops the read instead of becoming blank."""

    def test_adjinc_reads_as_the_decimal_the_api_sends(self):
        """The API sends ADJINC as "1.019518"; it stays a decimal."""
        df = pl.DataFrame({"ADJINC": ["1.019518"], "NP": ["2"]})
        out = _cast_pums_types(df, {"ADJINC", "NP"})
        assert out.row(0) == (1.019518, 2)

    def test_a_value_that_is_not_a_number_is_named(self):
        """The variable and a sample of what came back."""
        df = pl.DataFrame({"VEH": ["1", "2.5", "x"]})
        with pytest.raises(ValueError, match=r"PUMS variable VEH: 2 value\(s\).*whole numbers"):
            _cast_pums_types(df, {"VEH"})


class TestPumsHasNoBlanks:
    """After translation "not applicable" is a code, so a blank is a lost value."""

    def test_complete_records_pass(self, sound):
        """Nothing blank, nothing raised."""
        check_pums_complete(*sound)

    def test_a_blank_is_named_with_its_count(self, sound):
        """Every record's ADJINC blank: the variable and the count are named."""
        hh, per = sound
        hh = hh.with_columns(pl.lit(None, dtype=pl.Float64).alias("ADJINC"))
        with pytest.raises(ValueError, match=f"household ADJINC: {hh.height:,} of {hh.height:,}"):
            check_pums_complete(hh, per)
