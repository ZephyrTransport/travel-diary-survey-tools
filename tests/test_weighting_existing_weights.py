"""Tests for add_existing_weights function."""

import polars as pl
import pytest

from processing.formatting.kept_records import select_profile_columns
from processing.weighting.core.hierarchy import LEVEL_BY_WEIGHT_COL
from processing.weighting.existing.config import ExistingWeightsConfig
from processing.weighting.existing_weights import add_existing_weights


def _weight_file(tmp_path, name: str, **columns) -> str:
    """Write a weight CSV and return its path."""
    path = tmp_path / f"{name}.csv"
    pl.DataFrame(columns).write_csv(path)
    return str(path)


def _supplied(tmp_path, **weights: dict[int, float]) -> dict[str, dict[str, str]]:
    """The ``weights`` config for ``<weight col>={id: weight}``, one file each."""
    return {
        name: {
            "weight_path": _weight_file(
                tmp_path,
                name,
                **{LEVEL_BY_WEIGHT_COL[name].id_col: list(by_id), name: list(by_id.values())},
            )
        }
        for name, by_id in weights.items()
    }


def _weights_for(
    profile: str = "p",
    *,
    preserve_travel_totals: bool = True,
    household_size_top_group: int = 6,
    derive_missing_weights: bool = False,
    max_weight_factor: dict[str, float] | None = None,
    **inputs,
) -> dict[str, pl.DataFrame]:
    """Run ``add_existing_weights`` for one profile, read under the base names.

    Input validation is off: these frames carry only what the arithmetic reads.
    """
    existing = ExistingWeightsConfig(
        weight_profiles=[profile],
        preserve_travel_totals=preserve_travel_totals,
        household_size_top_group=household_size_top_group,
        derive_missing_weights=derive_missing_weights,
        max_weight_factor=max_weight_factor,
    )
    result = add_existing_weights(existing=existing, validate_input=False, **inputs)
    return select_profile_columns(result, profile)


def _sorted(result, table: str, key: str, col: str) -> list[float]:
    return result[table].sort(key)[col].to_list()


class TestAddExistingWeights:
    """Loading and joining the supplied weight files."""

    def test_a_weight_column_already_there_is_replaced_not_duplicated(self, tmp_path):
        """Re-running over a table that already carries the column overwrites it."""
        result = _weights_for(
            weights=_supplied(tmp_path, hh_weight={1: 1.5, 2: 2.0}),
            households=pl.DataFrame({"hh_id": [1, 2], "hh_weight": [99.0, 99.0]}),
        )

        assert result["households"]["hh_weight"].to_list() == [1.5, 2.0]
        assert [c for c in result["households"].columns if c.startswith("hh_weight")] == [
            "hh_weight"
        ]

    def test_a_record_no_supplied_total_covers_is_reported(self, tmp_path, caplog):
        """A household absent from the weight file joins to null, and that is logged."""
        with caplog.at_level("WARNING"):
            result = _weights_for(
                weights=_supplied(tmp_path, hh_weight={1: 1.5, 2: 2.0}),
                households=pl.DataFrame({"hh_id": [1, 2, 3], "hh_size": [2, 3, 1]}),
            )

        assert result["households"]["hh_weight"].to_list() == [1.5, 2.0, None]
        assert "1 / 3 NULL" in caplog.text

    def test_custom_id_column_names(self, tmp_path):
        """The weight file may name its id column differently from the table."""
        path = _weight_file(tmp_path, "p", pid=[1, 2, 3], person_weight=[1.2, 1.5, 1.8])
        result = _weights_for(
            weights={"person_weight": {"weight_path": path, "weight_id_col": "pid"}},
            persons=pl.DataFrame({"person_id": [1, 2, 3], "hh_id": [1, 2, 3]}),
        )

        assert result["persons"]["person_weight"].to_list() == [1.2, 1.5, 1.8]

    def test_custom_weight_column_name(self, tmp_path):
        """The weight file may name its weight column; the table gets the canonical one."""
        path = _weight_file(tmp_path, "t", unlinked_trip_id=[1, 2, 3], wt=[2.0, 1.5, 1.8])
        trips = pl.DataFrame(
            {"unlinked_trip_id": [1, 2, 3], "person_day_id": [1, 1, 1], "linked_trip_id": [1, 2, 3]}
        )
        result = _weights_for(
            weights={"unlinked_trip_weight": {"weight_path": path, "weight_col": "wt"}},
            unlinked_trips=trips,
        )

        assert "wt" not in result["unlinked_trips"].columns
        assert result["unlinked_trips"]["unlinked_trip_weight"].to_list() == [2.0, 1.5, 1.8]

    def test_derive_person_weights_from_household(self, tmp_path):
        """Persons without a supplied file copy their household's weight."""
        result = _weights_for(
            weights=_supplied(tmp_path, hh_weight={1: 1.5, 2: 2.0}),
            households=pl.DataFrame({"hh_id": [1, 2], "hh_size": [2, 1]}),
            persons=pl.DataFrame({"person_id": [1, 2, 3], "hh_id": [1, 1, 2]}),
            derive_missing_weights=True,
        )

        assert result["households"]["hh_weight"].to_list() == [1.5, 2.0]
        assert result["persons"]["person_weight"].to_list() == [1.5, 1.5, 2.0]

    def test_error_on_invalid_config_key(self, tmp_path):
        """A config key that is not a weight column is refused."""
        path = _weight_file(tmp_path, "w", hh_id=[1, 2], weight=[1.0, 2.0])
        with pytest.raises(ValueError, match="Invalid weight config key"):
            _weights_for(
                weights={"invalid_key": {"weight_path": path}},
                households=pl.DataFrame({"hh_id": [1, 2]}),
            )

    def test_error_on_nonexistent_file(self):
        """A weight path that does not exist is refused."""
        with pytest.raises(FileNotFoundError, match="Weight file does not exist"):
            _weights_for(
                weights={"hh_weight": {"weight_path": "/nonexistent/path/to/weights.csv"}},
                households=pl.DataFrame({"hh_id": [1, 2]}),
            )

    @pytest.mark.parametrize(
        ("columns", "match"),
        [
            pytest.param(
                {"wrong_id": [1, 2], "hh_weight": [1.0, 2.0]},
                "missing required ID column",
                id="no_id_column",
            ),
            pytest.param(
                {"hh_id": [1, 2], "wrong_weight": [1.0, 2.0]},
                "missing required weight column",
                id="no_weight_column",
            ),
        ],
    )
    def test_error_on_missing_column_in_weight_file(self, tmp_path, columns, match):
        """A weight file has to carry both the key it joins on and the weight."""
        with pytest.raises(ValueError, match=match):
            _weights_for(
                weights={"hh_weight": {"weight_path": _weight_file(tmp_path, "w", **columns)}},
                households=pl.DataFrame({"hh_id": [1, 2]}),
            )

    def test_multiple_tables_with_weights(self, tmp_path):
        """Each supplied file is joined onto its own table."""
        result = _weights_for(
            weights=_supplied(
                tmp_path, hh_weight={1: 1.5, 2: 2.0}, person_weight={1: 1.2, 2: 1.3, 3: 2.1}
            ),
            households=pl.DataFrame({"hh_id": [1, 2]}),
            persons=pl.DataFrame({"person_id": [1, 2, 3], "hh_id": [1, 1, 2]}),
        )

        assert result["households"]["hh_weight"].to_list() == [1.5, 2.0]
        assert result["persons"]["person_weight"].to_list() == [1.2, 1.3, 2.1]

    def test_table_not_found_is_skipped_not_an_error(self, tmp_path):
        """A weight file for a table that was not passed is skipped."""
        result = _weights_for(
            weights=_supplied(tmp_path, hh_weight={1: 1.0, 2: 2.0}), households=None
        )

        assert "households" not in result


class TestSuppliedTotalPreserved:
    """Supplied weights are redistributed onto usable records, never shrunk."""

    def test_supplied_day_weight_is_conserved_within_the_person(self, tmp_path):
        """A person's unusable day moves onto their own usable day, never across the household."""
        person_days = pl.DataFrame(
            {
                "person_day_id": [10, 20, 30, 40],
                "person_id": [1, 1, 2, 2],
                "hh_id": [1, 1, 1, 1],
                "survey_complete": [True, False, True, True],
            }
        )
        result = _weights_for(
            weights=_supplied(tmp_path, person_day_weight={10: 10.0, 20: 10.0, 30: 5.0, 40: 5.0}),
            person_days=person_days,
            profile="survey_complete",
        )
        weights = _sorted(result, "person_days", "person_day_id", "person_day_weight")
        assert weights == pytest.approx([20.0, 0.0, 5.0, 5.0])

    def test_missing_scope_column_raises(self, tmp_path):
        """Days without person_id cannot be conserved as declared, so the step stops."""
        person_days = pl.DataFrame(
            {"person_day_id": [10, 20], "hh_id": [1, 1], "survey_complete": [True, False]}
        )
        with pytest.raises(ValueError, match=r"person_days: missing columns \['person_id'\]"):
            _weights_for(
                weights=_supplied(tmp_path, person_day_weight={10: 10.0, 20: 10.0}),
                person_days=person_days,
                profile="survey_complete",
            )


class TestHouseholdsScaleBySize:
    """Each household size gets its own factor; persons copy their household.

    Four households. Households 2 and 4 are dropped, with their persons.

    | hh | size | supplied | kept |
    |----|------|----------|------|
    | 1  | 1    | 30       | yes  |
    | 2  | 1    | 10       | no   |
    | 3  | 2    | 20       | yes  |
    | 4  | 2    | 10       | no   |

    Persons are supplied their household's weight; each has one day, supplied
    the person's weight.

    * Size 1: 40 supplied, 30 kept, factor 4/3. Household 1 is 40.
    * Size 2: 30 supplied, 20 kept, factor 3/2. Household 3 is 30.

    Persons copy their household, so person 11 is 40 and persons 31 and 32 are
    30 each. Both totals hold: households 70, persons 100.

    One factor for all sizes would be 70/50 = 1.4: households 42 and 28,
    persons 42 + 28 + 28 = 98. The dropped household 4 is larger than the
    dropped household 2, which is what pulls the person total down.
    """

    def _run(
        self,
        tmp_path,
        *,
        top_group: int = 6,
        limits: dict[str, float] | None = None,
        dropped: tuple[int, ...] = (2, 4),
    ) -> dict[str, pl.DataFrame]:
        hh_ids = [1, 2, 3, 4]
        person_ids, person_hh = [11, 21, 31, 32, 41, 42], [1, 2, 3, 3, 4, 4]
        hh_weight = {1: 30.0, 2: 10.0, 3: 20.0, 4: 10.0}
        kept = [h not in dropped for h in person_hh]
        tables = {
            "households": pl.DataFrame(
                {"hh_id": hh_ids, "usable_p": [h not in dropped for h in hh_ids]}
            ),
            "persons": pl.DataFrame(
                {"person_id": person_ids, "hh_id": person_hh, "usable_p": kept}
            ),
            "person_days": pl.DataFrame(
                {
                    "person_day_id": [p * 10 for p in person_ids],
                    "person_id": person_ids,
                    "hh_id": person_hh,
                    "usable_p": kept,
                }
            ),
        }
        weights = _supplied(
            tmp_path,
            hh_weight=hh_weight,
            person_weight={p: hh_weight[h] for p, h in zip(person_ids, person_hh, strict=True)},
            person_day_weight={
                p * 10: hh_weight[h] for p, h in zip(person_ids, person_hh, strict=True)
            },
        )
        return _weights_for(
            weights=weights,
            household_size_top_group=top_group,
            max_weight_factor=limits,
            **tables,
        )

    def test_each_size_has_its_own_factor(self, tmp_path):
        """Household 1 is 30 x 4/3; household 3 is 20 x 3/2."""
        hh = _sorted(self._run(tmp_path), "households", "hh_id", "hh_weight")
        assert hh == pytest.approx([40.0, 0.0, 30.0, 0.0])

    def test_persons_copy_their_household(self, tmp_path):
        """Persons 11, 31 and 32 carry their household's weight; days carry the persons'."""
        result = self._run(tmp_path)
        persons = _sorted(result, "persons", "person_id", "person_weight")
        assert persons == pytest.approx([40.0, 0.0, 30.0, 30.0, 0.0, 0.0])
        assert result["person_days"]["person_day_weight"].sum() == pytest.approx(100.0)

    def test_sizes_from_the_top_group_up_share_one_factor(self, tmp_path):
        """A top group of 1 puts every size in one group: one factor, 70/50.

        The household total still holds; the person total falls to 98.
        """
        result = self._run(tmp_path, top_group=1)
        hh = _sorted(result, "households", "hh_id", "hh_weight")
        assert hh == pytest.approx([42.0, 0.0, 28.0, 0.0])
        assert result["persons"]["person_weight"].sum() == pytest.approx(98.0)

    def test_a_size_with_weight_but_no_kept_household_raises(self, tmp_path):
        """Drop household 1 too: size 1's 40 has no kept household to go to."""
        with pytest.raises(ValueError, match=r"household size 1: .* no kept household"):
            self._run(tmp_path, dropped=(1, 2, 4))

    def test_household_sizes_need_the_persons_table(self, tmp_path):
        """Size is the number of persons, so without persons there is no size."""
        with pytest.raises(ValueError, match="persons table"):
            _weights_for(
                weights=_supplied(tmp_path, hh_weight={1: 10.0}),
                households=pl.DataFrame({"hh_id": [1], "usable_p": [True]}),
            )


class TestTheWeightFactorLimit:
    """An opt-in limit on how far a record's weight may grow past its supplied weight.

    Same households as ``TestHouseholdsScaleBySize``: household 1 grows 4/3 and
    household 3 grows 3/2, and their persons with them.
    """

    _run = TestHouseholdsScaleBySize._run

    def test_a_limit_above_every_factor_passes(self, tmp_path):
        """The largest factor is 1.5, under 2."""
        result = self._run(tmp_path, limits={"households": 2.0})
        assert result["households"]["hh_weight"].sum() == pytest.approx(70.0)

    def test_a_household_past_the_limit_is_named(self, tmp_path):
        """Household 3 grows 1.5x, past 1.4; household 1's 1.33x is within it."""
        with pytest.raises(ValueError, match=r"1 households record\(s\) .* hh_id: \[3\]"):
            self._run(tmp_path, limits={"households": 1.4})

    def test_persons_can_be_limited_too(self, tmp_path):
        """Persons 31 and 32 grow 1.5x with their household."""
        with pytest.raises(ValueError, match=r"2 persons record\(s\) .* person_id: \[31, 32\]"):
            self._run(tmp_path, limits={"persons": 1.4})

    def test_a_limit_on_a_table_without_a_supplied_weight_raises(self, tmp_path):
        """A factor needs a supplied weight to compare against."""
        with pytest.raises(ValueError, match="max_weight_factor names tours"):
            self._run(tmp_path, limits={"tours": 2.0})


def test_a_dropped_person_stays_in_their_household(tmp_path):
    """A dropped person's weight goes to the household's other persons, never further.

    Every supplied household and person weight is 10.

    | person | hh | size | kept | days, supplied |
    |--------|----|------|------|----------------|
    | 11     | 1  | 2    | yes  | 110: 5 kept, 111: 5 dropped |
    | 12     | 1  | 2    | no   | 120: 10 dropped |
    | 21     | 2  | 1    | yes  | 210: 10 kept |
    | 31     | 3  | 1    | no   | 310: 10 dropped; household 3 dropped |

    * Households, size 1: 20 supplied, 10 kept, factor 2. Household 2 is 20.
      Size 2: household 1 is kept whole, factor 1, so it stays 10.
    * Persons copy their household: household 1 is 10 x 2 = 20 on person 11
      alone, since person 12 is dropped. Person 21 is 20.
    * Days: day 110 is person 11's only kept day, so it carries 20. Day 210
      carries 20.
    """
    result = _weights_for(
        weights=_supplied(
            tmp_path,
            hh_weight={1: 10.0, 2: 10.0, 3: 10.0},
            person_weight={11: 10.0, 12: 10.0, 21: 10.0, 31: 10.0},
            person_day_weight={110: 5.0, 111: 5.0, 120: 10.0, 210: 10.0, 310: 10.0},
        ),
        households=pl.DataFrame({"hh_id": [1, 2, 3], "usable_p": [True, True, False]}),
        persons=pl.DataFrame(
            {
                "person_id": [11, 12, 21, 31],
                "hh_id": [1, 1, 2, 3],
                "usable_p": [True, False, True, False],
            }
        ),
        person_days=pl.DataFrame(
            {
                "person_day_id": [110, 111, 120, 210, 310],
                "person_id": [11, 11, 12, 21, 31],
                "hh_id": [1, 1, 1, 2, 3],
                "usable_p": [True, False, False, True, False],
            }
        ),
    )
    assert result["households"]["hh_weight"].sum() == pytest.approx(30.0)
    persons = _sorted(result, "persons", "person_id", "person_weight")
    assert persons == pytest.approx([20.0, 0.0, 20.0, 0.0])
    days = _sorted(result, "person_days", "person_day_id", "person_day_weight")
    assert days == pytest.approx([20.0, 0.0, 0.0, 20.0, 0.0])


class TestAUsableRecordWithoutASuppliedWeightStopsTheRun:
    """A usable record must carry a weight; one the supplier weighted 0 cannot.

    Every supplied household and person weight is 10.

    | person | hh | days, supplied |
    |--------|----|----------------|
    | 11     | 1  | 110: 10 dropped, 111: 0 kept |
    | 12     | 1  | 120: 10 kept |
    | 21     | 2  | 210: 10 dropped, 211: 0 kept |
    | 31     | 3  | 310: 10 kept |

    Days 111 and 211 are usable but were supplied 0, so persons 11 and 21, and
    household 2, have nothing to carry weight on.
    """

    def _run(
        self,
        tmp_path,
        usable: dict[str, list[bool]] | None = None,
        person_weight: tuple[float, ...] = (10.0, 10.0, 10.0, 10.0),
    ) -> dict[str, pl.DataFrame]:
        tables = {
            "households": pl.DataFrame({"hh_id": [1, 2, 3], "usable_p": [True, True, True]}),
            "persons": pl.DataFrame(
                {
                    "person_id": [11, 12, 21, 31],
                    "hh_id": [1, 1, 2, 3],
                    "usable_p": [True, True, True, True],
                }
            ),
            "person_days": pl.DataFrame(
                {
                    "person_day_id": [110, 111, 120, 210, 211, 310],
                    "person_id": [11, 11, 12, 21, 21, 31],
                    "hh_id": [1, 1, 1, 2, 2, 3],
                    "usable_p": [False, True, True, False, True, True],
                }
            ),
        }
        for table, flags in (usable or {}).items():
            tables[table] = tables[table].with_columns(pl.Series("usable_p", flags))
        weights = _supplied(
            tmp_path,
            hh_weight={1: 10.0, 2: 10.0, 3: 10.0},
            person_weight=dict(zip([11, 12, 21, 31], person_weight, strict=True)),
            person_day_weight={110: 10.0, 111: 0.0, 120: 10.0, 210: 10.0, 211: 0.0, 310: 10.0},
        )
        return _weights_for(weights=weights, **tables)

    def test_the_run_stops_and_names_the_table(self, tmp_path):
        """Days 111 and 211 are usable but were supplied 0: the step names them."""
        message = r"person_days: 2 of 4 record\(s\) admitted by usable_p were supplied no"
        with pytest.raises(ValueError, match=message):
            self._run(tmp_path)

    def test_one_stop_names_every_level(self, tmp_path):
        """Persons 11 and 21 also unweighted: one message names persons and days."""
        with pytest.raises(ValueError, match="supplier did not weight") as stopped:
            self._run(tmp_path, person_weight=(0.0, 10.0, 0.0, 10.0))
        assert "persons: 2 of 4" in str(stopped.value)
        assert "person_days: 2 of 4" in str(stopped.value)

    def test_it_runs_once_the_unweighted_records_are_unusable(self, tmp_path):
        """Household 2's weight goes to household 3 (same size); person 11's to person 12."""
        result = self._run(
            tmp_path,
            usable={
                "households": [True, False, True],
                "persons": [False, True, False, True],
                "person_days": [False, False, True, False, False, True],
            },
        )
        assert _sorted(result, "households", "hh_id", "hh_weight") == pytest.approx(
            [10.0, 0.0, 20.0]
        )
        assert _sorted(result, "persons", "person_id", "person_weight") == pytest.approx(
            [0.0, 20.0, 0.0, 20.0]
        )


def test_the_step_checks_its_own_hierarchy(tmp_path, monkeypatch):
    """Persons and days derived from a household file go through the hierarchy check."""
    seen: list[set[str]] = []

    def spy(*_args, only=None, **_kwargs):
        seen.append(set(only))

    monkeypatch.setattr("processing.weighting.existing_weights._check_hierarchy", spy)
    _weights_for(
        weights=_supplied(tmp_path, hh_weight={1: 10.0}),
        derive_missing_weights=True,
        households=pl.DataFrame({"hh_id": [1], "usable_p": [True]}),
        persons=pl.DataFrame({"person_id": [11], "hh_id": [1], "usable_p": [True]}),
        person_days=pl.DataFrame(
            {"person_day_id": [110], "person_id": [11], "hh_id": [1], "usable_p": [True]}
        ),
    )
    assert seen == [{"persons", "person_days"}]


class TestTripsFollowTheirDay:
    """A trip moves with its day, whatever ``preserve_travel_totals`` says.

    One household, one person (weight 10), two days supplied at 5 each. Day 111
    is unusable, so day 110 takes the person's whole 10: its factor is 2.

    Day 110 has trips 1001 (usable) and 1002 (unusable); day 111 has trip 1101.
    Each trip is supplied its day's weight, 5.

    * ``false``: trip 1001 keeps its own claim times its day's factor, 5 x 2 =
      10. Trip 1002 is dropped, and its weight is gone.
    * ``true``: trip 1002's claim also moves onto 1001, within day 110, so 1001
      is 5 x 2 x (10 / 5) = 20.

    Trip 1101 is on a dropped day, so it is 0 either way. Each trip is its own
    linked trip and its own tour (tours 1, 3, 2), so tour 1 carries exactly
    what trip 1001 ends at.
    """

    def _run(
        self, tmp_path, preserve: bool, person_day_weights: tuple[float, float] = (5.0, 5.0)
    ) -> dict[str, pl.DataFrame]:
        tables = {
            "households": pl.DataFrame({"hh_id": [1], "usable_p": [True]}),
            "persons": pl.DataFrame({"person_id": [11], "hh_id": [1], "usable_p": [True]}),
            "person_days": pl.DataFrame(
                {
                    "person_day_id": [110, 111],
                    "person_id": [11, 11],
                    "hh_id": [1, 1],
                    "usable_p": [True, False],
                }
            ),
            "unlinked_trips": pl.DataFrame(
                {
                    "unlinked_trip_id": [1001, 1002, 1101],
                    "linked_trip_id": [1, 2, 3],
                    "tour_id": [1, 3, 2],
                    "person_day_id": [110, 110, 111],
                    "person_id": [11, 11, 11],
                    "hh_id": [1, 1, 1],
                    "usable_p": [True, False, False],
                }
            ),
            "linked_trips": pl.DataFrame(
                {
                    "linked_trip_id": [1, 2, 3],
                    "tour_id": [1, 3, 2],
                    "person_day_id": [110, 110, 111],
                    "usable_p": [True, False, False],
                }
            ),
            "tours": pl.DataFrame(
                {
                    "tour_id": [1, 2, 3],
                    "person_day_id": [110, 111, 110],
                    "usable_p": [True, False, False],
                }
            ),
        }
        weights = _supplied(
            tmp_path,
            hh_weight={1: 10.0},
            person_weight={11: 10.0},
            person_day_weight=dict(zip([110, 111], person_day_weights, strict=True)),
            unlinked_trip_weight={1001: 5.0, 1002: 5.0, 1101: 5.0},
        )
        return _weights_for(
            weights=weights, derive_missing_weights=True, preserve_travel_totals=preserve, **tables
        )

    @pytest.mark.parametrize(("preserve", "expected"), [(False, 10.0), (True, 20.0)])
    def test_the_tour_carries_its_final_trips_weight(self, tmp_path, preserve, expected):
        """``false``: 5 x the day's factor 2; ``true``: also trip 1002's claim, x 2 again."""
        result = self._run(tmp_path, preserve=preserve)
        for table, key, col in [
            ("unlinked_trips", "unlinked_trip_id", "unlinked_trip_weight"),
            ("linked_trips", "linked_trip_id", "linked_trip_weight"),
            ("tours", "tour_id", "tour_weight"),
        ]:
            assert _sorted(result, table, key, col) == pytest.approx([expected, 0.0, 0.0])

    def test_a_weighted_trip_on_an_unweighted_day_raises(self, tmp_path):
        """Trip 1101 was weighted but its day 111 was not: there is no factor.

        Day 110 keeps the person's weight, so the person can still carry it and
        the trip check is what fails.
        """
        with pytest.raises(
            ValueError, match=r"1 unlinked_trips record\(s\) .* person_day_weight of 0"
        ):
            self._run(tmp_path, preserve=False, person_day_weights=(10.0, 0.0))


@pytest.mark.parametrize(
    ("preserve", "trips", "tour"),
    [
        pytest.param(True, [12.0, 24.0, 0.0], 18.0, id="true_moves_the_dropped_trip"),
        pytest.param(False, [6.0, 12.0, 0.0], 9.0, id="false_keeps_each_trip_its_own"),
    ],
)
def test_preserve_travel_totals(tmp_path, preserve, trips, tour):
    """Whether a dropped trip's weight moves to the kept ones; population totals never move.

    One person, one day. Tour 1 has trips 101 and 102 and is usable; tour 2 has
    trip 103 and is not. Trips are supplied 6, 12, 18 (total 36).

    * ``true``: trip 103's 18 moves onto 101 and 102 in proportion, giving 12
      and 24, so the day's trips still total 36.
    * ``false``: every kept trip keeps exactly its own weight: 6, 12, 0.

    A tour's weight is the mean of its final trips either way.
    """
    usable = {"usable_p": [True]}
    trip_ids = {"linked_trip_id": [1, 2, 3], "tour_id": [1, 1, 2], "person_day_id": [110] * 3}
    result = _weights_for(
        weights=_supplied(
            tmp_path,
            hh_weight={1: 10.0},
            person_weight={11: 10.0},
            person_day_weight={110: 10.0},
            unlinked_trip_weight={101: 6.0, 102: 12.0, 103: 18.0},
        ),
        derive_missing_weights=True,
        preserve_travel_totals=preserve,
        households=pl.DataFrame({"hh_id": [1], **usable}),
        persons=pl.DataFrame({"person_id": [11], "hh_id": [1], **usable}),
        person_days=pl.DataFrame(
            {"person_day_id": [110], "person_id": [11], "hh_id": [1], **usable}
        ),
        unlinked_trips=pl.DataFrame(
            {"unlinked_trip_id": [101, 102, 103], **trip_ids, "usable_p": [True, True, False]}
        ),
        linked_trips=pl.DataFrame({**trip_ids, "usable_p": [True, True, False]}),
        tours=pl.DataFrame(
            {"tour_id": [1, 2], "person_day_id": [110, 110], "usable_p": [True, False]}
        ),
    )

    unlinked = _sorted(result, "unlinked_trips", "unlinked_trip_id", "unlinked_trip_weight")
    assert unlinked == pytest.approx(trips)
    linked = _sorted(result, "linked_trips", "linked_trip_id", "linked_trip_weight")
    assert linked == pytest.approx(trips)
    assert _sorted(result, "tours", "tour_id", "tour_weight") == pytest.approx([tour, 0.0])
    assert result["households"]["hh_weight"].sum() == pytest.approx(10.0)
    assert result["persons"]["person_weight"].sum() == pytest.approx(10.0)
    assert result["person_days"]["person_day_weight"].sum() == pytest.approx(10.0)
