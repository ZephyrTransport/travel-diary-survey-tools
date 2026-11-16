"""Fractional imputation of incidence rows a null left in no category.

Uses the real control registry with synthetic survey and PUMS data.
"""

import dataclasses
import functools
from typing import ClassVar

import numpy as np
import polars as pl
import pytest

from processing.weighting.core.specs import IncidenceBundle
from processing.weighting.data_prep.control_data import (
    recode_pums_households,
    recode_pums_persons,
)
from processing.weighting.data_prep.fractional_impute import (
    _MASKED_COPIES,
    _masked_copies,
    _train_and_predict,
    _with_other_members,
    fill_null_incidence,
)
from processing.weighting.data_prep.incidence import (
    build_incidence_table,
    pivot_person_controls,
)
from processing.weighting.data_prep.seed_data import (
    recode_survey_households,
    recode_survey_persons,
)

TARGETS = ["h_total", "h_size", "h_income", "p_total", "p_gender", "p_age"]


def _null_mask(df: pl.DataFrame, cols: list[str]) -> pl.Series:
    """Boolean mask: True where all *cols* are zero (null-encoded row)."""
    return df.select(pl.sum_horizontal(cols).eq(0)).to_series()


def _make_pums_hh(n: int = 50) -> pl.DataFrame:
    """Generate synthetic PUMS households (complete, no nulls)."""
    rng = np.random.default_rng(42)
    return pl.DataFrame(
        {
            "SERIALNO": list(range(1, n + 1)),
            "NP": rng.choice([1, 2, 3, 4], n).tolist(),
            "HINCP": rng.choice([10000, 30000, 60000, 100000, 200000], n).tolist(),
            "VEH": rng.choice([0, 1, 2, 3], n).tolist(),
            "WGTP": [10] * n,
            "PUMA": ["00100"] * n,
        }
    )


def _make_pums_per(hh_df: pl.DataFrame) -> pl.DataFrame:
    """Generate synthetic PUMS persons matching household sizes."""
    rng = np.random.default_rng(43)
    rows = []
    for row in hh_df.iter_rows(named=True):
        serialno = row["SERIALNO"]
        n_persons = row["NP"]
        rows.extend(
            {
                "SERIALNO": serialno,
                "SPORDER": sp,
                "SEX": rng.choice([1, 2]),
                "AGEP": int(rng.choice([5, 20, 35, 55, 75])),
                "ESR": int(rng.choice([1, 2, 3, 6])),
                "SCHG": int(rng.choice([0, 1, 15])),
                "SCHL": int(rng.choice([16, 20, 21, 24])),
                "RAC1P": int(rng.choice([1, 2, 6])),
                "HISP": int(rng.choice([1, 2])),
                "JWTRNS": int(rng.choice([1, 2, 10])),
                "PUMA": "00100",
                "PWGTP": 10,
            }
            for sp in range(1, n_persons + 1)
        )
    return pl.DataFrame(rows)


def _make_survey_hh(n: int = 30, null_income_rate: float = 0.0) -> pl.DataFrame:
    """Generate synthetic survey households with optional null income."""
    rng = np.random.default_rng(44)
    income_values = [1, 2, 3, 4, 5, 6]  # IncomeBroad enum values
    incomes = rng.choice(income_values, n).tolist()
    if null_income_rate > 0:
        n_null = max(1, int(n * null_income_rate))
        for i in rng.choice(n, n_null, replace=False):
            incomes[i] = None
    return pl.DataFrame(
        {
            "hh_id": list(range(1, n + 1)),
            "income_bin": incomes,
            "num_vehicles": rng.choice([0, 1, 2, 3], n).tolist(),
            "survey_complete": [True] * n,
        }
    )


def _make_survey_per(hh_df: pl.DataFrame, null_gender_rate: float = 0.0) -> pl.DataFrame:
    """Generate synthetic survey persons with optional null gender."""
    rng = np.random.default_rng(45)
    rows = []
    person_id = 100
    persons_per_hh = rng.choice([1, 2, 3], len(hh_df)).tolist()
    for i, hh_row in enumerate(hh_df.iter_rows(named=True)):
        n_per = persons_per_hh[i]
        for _ in range(n_per):
            person_id += 1
            rows.append(
                {
                    "hh_id": hh_row["hh_id"],
                    "person_id": person_id,
                    "gender": int(rng.choice([1, 2])),
                    "age": int(rng.choice([1, 3, 5, 7, 9])),
                    "employment": int(rng.choice([1, 2, 3, 5])),
                    "student": int(rng.choice([1, 2])),
                    "work_mode": int(rng.choice([1, 3, 5])),
                }
            )

    df = pl.DataFrame(rows)
    if null_gender_rate > 0:
        n_null = max(1, int(len(df) * null_gender_rate))
        null_ids = rng.choice(len(df), n_null, replace=False).tolist()
        mask = pl.Series([i in null_ids for i in range(len(df))])
        df = df.with_columns(pl.when(mask).then(None).otherwise(pl.col("gender")).alias("gender"))
    return df


@functools.cache
def _recode_pums():
    """Build complete PUMS incidence bundle + recoded persons."""
    hh = _make_pums_hh()
    per = _make_pums_per(hh)
    hh_r = recode_pums_households(hh, per, TARGETS)
    per_r = recode_pums_persons(per, TARGETS)
    bundle = build_incidence_table(
        hh_r,
        per_r,
        TARGETS,
        hh_id_col="SERIALNO",
        extra_cols=["WGTP", "PUMA"],
        person_extra_cols=["PWGTP", "PUMA"],
    )
    return bundle, per_r


def _impute(survey_bundle, pums_bundle):
    """Impute with PUMS and the survey all in one zone, as the synthetic data is."""
    one_zone = pl.lit("z1").alias("zone_group_id")
    return fill_null_incidence(
        survey_bundle,
        pums_bundle.incidence.with_columns(one_zone),
        pums_bundle.person_pivot.with_columns(one_zone),
        survey_bundle.incidence.select("hh_id").with_columns(one_zone),
        TARGETS,
    )


@functools.cache
def _recode_survey(null_gender_rate=0.0, null_income_rate=0.0):
    """Build survey incidence bundle + recoded persons with optional nulls."""
    hh = _make_survey_hh(null_income_rate=null_income_rate)
    per = _make_survey_per(hh, null_gender_rate=null_gender_rate)
    hh_r = recode_survey_households(hh, per, TARGETS)
    per_r = recode_survey_persons(per, TARGETS)
    bundle = build_incidence_table(hh_r, per_r, TARGETS)
    return bundle, per_r


@pytest.fixture(scope="module")
def imputed():
    """The survey with half its gender and income answers null, and its imputation."""
    survey_bundle, _ = _recode_survey(null_gender_rate=0.5, null_income_rate=0.5)
    return _impute(survey_bundle, _recode_pums()[0])


class TestPivotHelpers:
    """Pivoting recoded persons into per-person indicators and household sums."""

    def test_person_pivot_is_one_binary_row_per_person(self):
        """One row per person with 0/1 indicators; the household sums are fewer rows."""
        _, pums_per = _recode_pums()
        pp, pa = pivot_person_controls(pums_per, TARGETS, "SERIALNO")
        assert len(pp) == len(pums_per)
        assert len(pa) < len(pp)
        for col in [c for c in pp.columns if "__" in c]:
            vals = set(pp[col].to_list())
            assert vals <= {0, 1}, f"Column {col} has non-binary values: {vals}"

    def test_null_person_yields_zero_indicators(self):
        """A person with null gender should get all-zero gender indicators."""
        _, per_r = _recode_survey(null_gender_rate=0.5)
        pp, _ = pivot_person_controls(per_r, TARGETS, "hh_id")
        gender_cols = [c for c in pp.columns if c.startswith("p_gender__")]
        row_sums = pp.select(pl.sum_horizontal(gender_cols)).to_series()
        null_rows = per_r["p_gender"].is_null()
        assert null_rows.any()
        assert row_sums.filter(null_rows).to_list() == [0] * int(null_rows.sum())


class TestTrainedOncePerRun:
    """Every profile imputes from the same PUMS, so a forest is trained once per run."""

    def _call(self, models, train=None):
        (survey_bundle, _), (pums_bundle, _) = _recode_survey(null_gender_rate=0.3), _recode_pums()
        cols = ["p_gender__male", "p_gender__female"]
        return _train_and_predict(
            train if train is not None else pums_bundle.person_pivot,
            survey_bundle.person_pivot,
            _null_mask(survey_bundle.person_pivot, cols),
            cols,
            id_col="person_id",
            weight_col="PWGTP",
            models=models,
        )

    def test_a_later_profile_reuses_the_forest(self):
        """The second call predicts with the first call's forest and its CV scores."""
        models: dict = {}
        first = self._call(models)
        forest = models["p_gender"].model
        second = self._call(models)
        assert models["p_gender"].model is forest
        assert second[1:] == first[1:]

    def test_a_forest_with_other_inputs_is_refused(self):
        """Reusing a forest trained on other inputs would predict from the wrong columns."""
        models: dict = {}
        self._call(models)
        pums_bundle, _ = _recode_pums()
        other = pums_bundle.person_pivot.with_columns(pl.lit(0).alias("zone__extra"))
        with pytest.raises(ValueError, match="used other inputs"):
            self._call(models, train=other)


class TestTrainingCountsByWeight:
    """A PUMS record counts by its weight, not as one."""

    MEMBERS: ClassVar[list[str]] = ["p_gender__male", "p_gender__female"]

    def _predict(self, male_weight: float, female_weight: float, *, weight_col="PWGTP"):
        """Ten men and ten women alike in every other answer; predict one unknown person."""
        train = pl.DataFrame(
            {
                "SERIALNO": list(range(20)),
                "p_age__age_35_to_44": [1] * 20,
                "p_gender__male": [1] * 10 + [0] * 10,
                "p_gender__female": [0] * 10 + [1] * 10,
                "PWGTP": [male_weight] * 10 + [female_weight] * 10,
            }
        )
        predict = pl.DataFrame(
            {
                "person_id": [1],
                "p_age__age_35_to_44": [1],
                "p_gender__male": [0],
                "p_gender__female": [0],
            }
        )
        proba, _, _ = _train_and_predict(
            train,
            predict,
            pl.Series([True]),
            self.MEMBERS,
            id_col="person_id",
            weight_col=weight_col,
        )
        return proba["p_gender__male"].item()

    def test_equal_weights_give_an_even_split(self):
        """The same records counted alike: half and half."""
        assert self._predict(1.0, 1.0) == pytest.approx(0.5, abs=0.15)

    def test_the_heavier_records_pull_the_prediction(self):
        """Men standing for nine times as many people: about nine in ten."""
        assert self._predict(9.0, 1.0) == pytest.approx(0.9, abs=0.08)

    def test_training_without_a_weight_stops(self):
        """Counting every record as one would be a silent choice, so it is refused."""
        with pytest.raises(ValueError, match="WGTP"):
            self._predict(1.0, 1.0, weight_col="WGTP")


class TestZoneIsAnInput:
    """Each control's model also takes the record's zone."""

    TARGETS: ClassVar[list[str]] = ["p_total", "p_gender"]

    def _impute(self, survey_zones=None, *, pums_zoned=True):
        """Men in PUMS zone A, women in zone B; two unknown survey persons, one in each."""
        pums_persons = pl.DataFrame(
            {
                "SERIALNO": list(range(1, 201)),
                "SPORDER": [1] * 200,
                "p_total": [1] * 200,
                "p_gender__male": [1] * 100 + [0] * 100,
                "p_gender__female": [0] * 100 + [1] * 100,
                "PWGTP": [1.0] * 200,
                "zone_group_id": ["A"] * 100 + ["B"] * 100,
            }
        )
        pums_households = pums_persons.rename({"PWGTP": "WGTP"}).drop("SPORDER")
        if not pums_zoned:
            pums_persons = pums_persons.drop("zone_group_id")
        unknown = {"p_total": [1, 1], "p_gender__male": [0, 0], "p_gender__female": [0, 0]}
        survey = IncidenceBundle(
            incidence=pl.DataFrame({"hh_id": [1, 2], **unknown}),
            person_pivot=pl.DataFrame({"hh_id": [1, 2], "person_id": [1, 2], **unknown}),
            household_pivot=pl.DataFrame({"hh_id": [1, 2]}),
        )
        if survey_zones is None:
            survey_zones = pl.DataFrame({"hh_id": [1, 2], "zone_group_id": ["A", "B"]})
        result, _ = fill_null_incidence(
            survey, pums_households, pums_persons, survey_zones, self.TARGETS
        )
        return result.sort("hh_id")

    def test_zone_alone_moves_the_prediction(self):
        """With nothing else to go on, each person takes their own zone's mix."""
        male = self._impute()["p_gender__male"].to_list()
        assert male[0] == pytest.approx(1.0, abs=0.1)
        assert male[1] == pytest.approx(0.0, abs=0.1)

    def test_the_zone_columns_are_not_in_the_seed(self):
        """They exist only while imputing; the balancer never sees them."""
        columns = self._impute().columns
        assert not [c for c in columns if c.startswith("zone__")]
        assert "zone_group_id" not in columns

    def test_pums_without_zones_stops(self):
        """Training region-wide by mistake would be silent, so it is refused."""
        with pytest.raises(ValueError, match="pums_persons carries no zone_group_id"):
            self._impute(pums_zoned=False)

    def test_a_survey_household_outside_every_zone_stops(self):
        """All-zero zone inputs would be a zone nobody trained on."""
        with pytest.raises(ValueError, match="1 survey household"):
            self._impute(pl.DataFrame({"hh_id": [1, 2], "zone_group_id": ["A", "C"]}))


class TestPumsRecordInNoCategory:
    """A PUMS record in no category would be learned as the first one."""

    def test_it_stops_naming_the_control(self):
        """One PUMS person zeroed out of every gender category stops the run."""
        survey_bundle, _ = _recode_survey(null_gender_rate=0.2)
        pums_bundle, _ = _recode_pums()
        first = pl.int_range(pl.len()) == 0
        uncoded = pums_bundle.person_pivot.with_columns(
            pl.when(first).then(0).otherwise(pl.col(c)).alias(c)
            for c in ("p_gender__male", "p_gender__female")
        )
        bundle = dataclasses.replace(pums_bundle, person_pivot=uncoded)
        with pytest.raises(ValueError, match=r"1 of .* sit in no 'p_gender' category"):
            _impute(survey_bundle, bundle)


class TestOtherHouseholdMembers:
    """What a person's other household members are, as inputs to that control."""

    MEMBERS: ClassVar[list[str]] = ["p_gender__male", "p_gender__female"]

    def test_shares_and_counts_leave_the_person_out(self):
        """A man and a woman known, a third member unknown."""
        df = pl.DataFrame(
            {
                "hh_id": [1, 1, 1],
                "p_gender__male": [1, 0, 0],
                "p_gender__female": [0, 1, 0],
            }
        )
        known = pl.sum_horizontal(self.MEMBERS) == 1
        out = _with_other_members(df, self.MEMBERS, "p_gender", ["hh_id"], known)

        assert out["others__p_gender__male"].to_list() == [0.0, 1.0, 0.5]
        assert out["others__p_gender__female"].to_list() == [1.0, 0.0, 0.5]
        assert out["others_known__p_gender"].to_list() == [1, 1, 2]
        assert out["others_total__p_gender"].to_list() == [2, 2, 2]

    def test_no_known_other_member_gives_zero_shares(self):
        """Living alone and nobody else known both read 0; only the total differs."""
        df = pl.DataFrame(
            {
                "hh_id": [1, 2, 2],
                "p_gender__male": [1, 1, 0],
                "p_gender__female": [0, 0, 0],
            }
        )
        known = pl.sum_horizontal(self.MEMBERS) == 1
        out = _with_other_members(df, self.MEMBERS, "p_gender", ["hh_id"], known)

        assert out["others__p_gender__male"].to_list() == [0.0, 0.0, 1.0]
        assert out["others_known__p_gender"].to_list() == [0, 0, 1]
        assert out["others_total__p_gender"].to_list() == [0, 1, 1]


class TestMaskedCopies:
    """Each PUMS person trains as the original and masked copies."""

    def _pums(self):
        n = 40
        return pl.DataFrame(
            {
                "SERIALNO": [i // 4 for i in range(n)],
                "SPORDER": [i % 4 for i in range(n)],
                "zone_group_id": ["z1"] * n,
                "p_gender__male": [i % 2 for i in range(n)],
                "p_gender__female": [1 - i % 2 for i in range(n)],
                "PWGTP": [8.0] * n,
            }
        )

    def _copies(self):
        return _masked_copies(self._pums(), TestOtherHouseholdMembers.MEMBERS, "p_gender", "PWGTP")

    def test_each_person_still_adds_up_to_their_weight(self):
        """The original and every copy share the person's weight."""
        out = self._copies()
        assert out.height == 40 * (_MASKED_COPIES + 1)
        per_person = out.group_by("SERIALNO", "SPORDER").agg(pl.col("PWGTP").sum())
        assert per_person["PWGTP"].to_list() == [8.0] * 40

    def test_copies_know_fewer_members_than_the_original(self):
        """The original knows everyone; the copies hide some."""
        out = self._copies()
        known, total = out["others_known__p_gender"], out["others_total__p_gender"]
        assert (known <= total).all()
        assert (known < total).sum() > 0

    def test_the_masking_repeats(self):
        """Seeded, so two runs train on the same rows."""
        assert self._copies().equals(self._copies())


class TestTheHouseholdMovesThePrediction:
    """A known member's answer reaches the unknown member beside them."""

    def test_an_unknown_member_leans_towards_the_known_one(self):
        """PUMS couples share a gender; a survey man's unknown partner comes out a man."""
        pums_persons = pl.DataFrame(
            {
                "SERIALNO": [i // 2 for i in range(400)],
                "SPORDER": [i % 2 for i in range(400)],
                "p_total": [1] * 400,
                "p_gender__male": [1] * 200 + [0] * 200,
                "p_gender__female": [0] * 200 + [1] * 200,
                "PWGTP": [1.0] * 400,
                "zone_group_id": ["z1"] * 400,
            }
        )
        pums_households = (
            pums_persons.group_by("SERIALNO", "zone_group_id")
            .agg(pl.col("p_total", "p_gender__male", "p_gender__female").sum())
            .with_columns(pl.lit(1.0).alias("WGTP"))
        )
        survey = IncidenceBundle(
            incidence=pl.DataFrame(
                {
                    "hh_id": [1, 2],
                    "p_total": [2, 2],
                    "p_gender__male": [1, 0],
                    "p_gender__female": [0, 1],
                }
            ),
            person_pivot=pl.DataFrame(
                {
                    "hh_id": [1, 1, 2, 2],
                    "person_id": [1, 2, 3, 4],
                    "p_total": [1] * 4,
                    "p_gender__male": [1, 0, 0, 0],
                    "p_gender__female": [0, 0, 1, 0],
                }
            ),
            household_pivot=pl.DataFrame({"hh_id": [1, 2]}),
        )
        zones = pl.DataFrame({"hh_id": [1, 2], "zone_group_id": ["z1", "z1"]})

        result, _ = fill_null_incidence(
            survey, pums_households, pums_persons, zones, ["p_total", "p_gender"]
        )
        male = result.sort("hh_id")["p_gender__male"].to_list()

        assert male[0] == pytest.approx(2.0, abs=0.2)
        assert male[1] == pytest.approx(0.0, abs=0.2)


class TestFillNullIncidence:
    """The full fill_null_incidence orchestrator."""

    def test_no_nulls_passthrough(self):
        """With no nulls, incidence is unchanged and every summary counts 0 nulls."""
        survey_bundle, _ = _recode_survey()

        result, summaries = _impute(survey_bundle, _recode_pums()[0])

        for col in survey_bundle.incidence.columns:
            assert result[col].to_list() == survey_bundle.incidence[col].to_list(), (
                f"Column {col} changed"
            )
        assert all(s.n_null == 0 for s in summaries)

    def test_person_marginal_preserved(self, imputed):
        """After filling, sum(p_gender__*) equals p_total for each household."""
        result, _ = imputed
        gender_cols = [c for c in result.columns if c.startswith("p_gender__")]
        row_sums = result.select(pl.sum_horizontal(gender_cols)).to_series()
        diff = (row_sums - result["p_total"].cast(pl.Float64)).abs()
        assert diff.max() < 0.01, f"Max marginal deviation: {diff.max()}"

    def test_hh_marginal_preserved(self, imputed):
        """After filling, sum(h_income__*) is 1 for each household."""
        result, _ = imputed
        income_cols = [c for c in result.columns if c.startswith("h_income__")]
        row_sums = result.select(pl.sum_horizontal(income_cols)).to_series()
        for s in row_sums.to_list():
            assert abs(s - 1.0) < 0.01, f"HH income marginal sum {s} != 1.0"

    def test_values_become_fractional(self, imputed):
        """Null rows get fractional values."""
        result, _ = imputed
        gender_cols = [c for c in result.columns if c.startswith("p_gender__")]
        all_vals = [v for col in gender_cols for v in result[col].to_list()]
        assert [v for v in all_vals if v != int(v)], "Expected fractional values"

    def test_summaries_report_nulls(self, imputed):
        """Each imputed control reports its nulls, level and fit; h_size had none."""
        by_ctrl = {s.control: s for s in imputed[1]}
        assert by_ctrl["p_gender"].n_null > 0
        assert by_ctrl["p_gender"].level == "person"
        assert by_ctrl["p_gender"].log_loss is not None
        assert by_ctrl["p_gender"].f1_macro is not None
        assert by_ctrl["h_income"].n_null > 0
        assert by_ctrl["h_income"].level == "household"
        assert by_ctrl["h_size"].n_null == 0
        assert by_ctrl["h_size"].log_loss is None
