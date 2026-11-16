"""The weight hierarchy: the walk, the gate, and the post-run checks."""

import ast
import pathlib

import polars as pl
import pytest

from processing.weighting.core.hierarchy import HIERARCHY
from processing.weighting.core.propagation import (
    collect_tables,
    is_usable,
    non_null_tables,
    propagate_weights,
    safe_join_weight,
    seed_admits,
)
from processing.weighting.core.specs import ControlTotals
from processing.weighting.validation.weight_checks import (
    _check_hierarchy,
    _check_joint_sums,
    weight_sanity_checks,
)

HH_ONLY = {"households": "hh_weight"}
FLAG = "ctramp"

# Every combination of the two seed-gate inputs, nulls included.
GATE_FRAME = pl.DataFrame(
    {
        "case": ["both", "flag only", "complete only", "neither", "null flag", "null complete"],
        "profile": [True, True, False, False, None, True],
        "survey_complete": [True, False, True, False, True, None],
    }
)


def test_collect_tables_fills_the_rest_with_none():
    """Every level gets a key; the ones not supplied are None, and filterable."""
    assert all(v is None for v in collect_tables().values())

    hh = pl.DataFrame({"hh_id": [1]})
    tables = collect_tables(households=hh)
    assert len(tables) == len(HIERARCHY)
    assert tables["households"] is hh
    assert tables["persons"] is None
    assert list(non_null_tables(tables).keys()) == ["households"]


def test_safe_join_replaces_any_existing_weight():
    """The target's own weight column is dropped first, and rows w lacks go null."""
    df = pl.DataFrame({"hh_id": [1, 2, 3], "hh_weight": [0.0, 0.0, 0.0]})
    w = pl.DataFrame({"hh_id": [1, 2], "hh_weight": [1.5, 2.0]})
    result = safe_join_weight(df, w, "hh_id")
    assert result["hh_weight"].to_list() == [1.5, 2.0, None]


def _make_tables(*, complete: bool = False) -> dict[str, pl.DataFrame | None]:
    """Two households, three persons, one day each, four trips on one tour.

    ``complete`` adds a ``survey_complete`` flag that drops household 2 and its
    person, person 2's day, and the trips on days 20 and 30.
    """
    tables = {
        "households": pl.DataFrame({"hh_id": [1, 2], "hh_weight": [10.0, 20.0]}),
        "persons": pl.DataFrame({"person_id": [1, 2, 3], "hh_id": [1, 1, 2]}),
        "person_days": pl.DataFrame(
            {"person_day_id": [10, 20, 30], "person_id": [1, 2, 3], "hh_id": [1, 1, 2]}
        ),
        "unlinked_trips": pl.DataFrame(
            {
                "unlinked_trip_id": [100, 200, 300, 400],
                "person_day_id": [10, 10, 20, 30],
                "linked_trip_id": [1, 1, 2, 2],
            }
        ),
        "linked_trips": pl.DataFrame({"linked_trip_id": [1, 2], "tour_id": [1, 1]}),
        "joint_trips": None,
        "tours": pl.DataFrame({"tour_id": [1]}),
    }
    if complete:
        flags = {
            "households": [True, False],
            "persons": [True, True, False],
            "person_days": [True, False, False],
            "unlinked_trips": [True, True, False, False],
        }
        for name, values in flags.items():
            tables[name] = tables[name].with_columns(pl.Series("survey_complete", values))
    return tables


class TestPropagateCarryForward:
    """Each level copies the weight of the level above it."""

    def test_full_hierarchy(self):
        """Weights copy down to unlinked trips; each aggregate level is its members' mean."""
        tables = _make_tables()
        has_weight = dict(HH_ONLY)

        propagate_weights(tables, has_weight, usability_flag_col="usable")

        assert tables["persons"].sort("person_id")["person_weight"].to_list() == [10.0, 10.0, 20.0]
        assert tables["person_days"].sort("person_day_id")["person_day_weight"].to_list() == [
            10.0,
            10.0,
            20.0,
        ]
        assert tables["unlinked_trips"].sort("unlinked_trip_id")[
            "unlinked_trip_weight"
        ].to_list() == [10.0, 10.0, 10.0, 20.0]
        # linked trip 2 is the mean of 10 and 20; the tour the mean of 10 and 15
        lt = tables["linked_trips"].sort("linked_trip_id")
        assert lt["linked_trip_weight"].to_list() == [10.0, 15.0]
        assert tables["tours"]["tour_weight"].to_list() == [12.5]
        assert has_weight == {
            "households": "hh_weight",
            "persons": "person_weight",
            "person_days": "person_day_weight",
            "unlinked_trips": "unlinked_trip_weight",
            "linked_trips": "linked_trip_weight",
            "tours": "tour_weight",
        }

    @pytest.mark.parametrize(
        ("mutate", "has_weight", "match"),
        [
            pytest.param(
                lambda t: t,
                {},
                "has no weight column",
                id="parent_has_no_weight",
            ),
            pytest.param(
                lambda t: t | {"households": None},
                HH_ONLY,
                "households, which is not loaded",
                id="parent_frame_is_none",
            ),
            pytest.param(
                lambda t: t | {"persons": pl.DataFrame({"person_id": [1], "age": [25]})},
                HH_ONLY,
                "missing join key hh_id",
                id="child_missing_join_key",
            ),
        ],
    )
    def test_carry_forward_gaps_raise(self, mutate, has_weight, match):
        """Each way the carry-forward edge can be broken names itself."""
        tables = mutate(_make_tables())
        with pytest.raises(ValueError, match=match):
            propagate_weights(tables, dict(has_weight), usability_flag_col="usable")

    def test_skip_prevents_carry_forward(self):
        """Tables in the skip set are not overwritten."""
        tables = _make_tables()
        tables["persons"] = tables["persons"].with_columns(pl.lit(99.0).alias("person_weight"))
        has_weight = dict(HH_ONLY)

        propagate_weights(
            tables,
            has_weight,
            skip={"persons", "person_days", "unlinked_trips", "linked_trips", "tours"},
            usability_flag_col="usable",
        )

        assert tables["persons"]["person_weight"].to_list() == [99.0, 99.0, 99.0]
        assert "persons" not in has_weight

    def test_child_none_is_skipped(self):
        """A level whose table is None is skipped, not an error."""
        tables = _make_tables()
        for name in ("persons", "person_days", "unlinked_trips", "linked_trips", "tours"):
            tables[name] = None
        has_weight = dict(HH_ONLY)

        propagate_weights(tables, has_weight, usability_flag_col="usable")

        assert has_weight == HH_ONLY


class TestPropagateAggregate:
    """Tests for the aggregate (mean weight) branch."""

    def test_aggregate_excludes_zeros_and_nulls(self):
        """Zeros and nulls are excluded from the mean aggregation."""
        tables = _make_tables()
        tables["unlinked_trips"] = tables["unlinked_trips"].with_columns(
            pl.Series("unlinked_trip_weight", [5.0, 0.0, None, 3.0])
        )
        has_weight = {
            "households": "hh_weight",
            "persons": "person_weight",
            "person_days": "person_day_weight",
            "unlinked_trips": "unlinked_trip_weight",
        }

        propagate_weights(
            tables,
            has_weight,
            skip={"persons", "person_days", "unlinked_trips"},
            usability_flag_col="usable",
        )

        lt = tables["linked_trips"].sort("linked_trip_id")
        assert lt["linked_trip_weight"].to_list() == [5.0, 3.0]

    @pytest.mark.parametrize(
        ("mutate", "drop_source_weight", "match"),
        [
            pytest.param(
                lambda t: (
                    t
                    | {
                        "unlinked_trips": pl.DataFrame(
                            {
                                "unlinked_trip_id": [100, 200],
                                "person_day_id": [10, 10],
                                "unlinked_trip_weight": [5.0, 3.0],
                            }
                        )
                    }
                ),
                False,
                "missing linked_trip_id",
                id="source_missing_group_key",
            ),
            pytest.param(
                lambda t: t | {"unlinked_trips": None},
                False,
                "source table unlinked_trips is None",
                id="source_frame_is_none",
            ),
            pytest.param(
                lambda t: t,
                True,
                "source table unlinked_trips has no weight",
                id="source_has_no_weight",
            ),
        ],
    )
    def test_aggregate_gaps_raise(self, mutate, drop_source_weight, match):
        """Each way the aggregate edge can be broken names itself."""
        tables = mutate(_make_tables())
        has_weight = {
            "households": "hh_weight",
            "persons": "person_weight",
            "person_days": "person_day_weight",
            "unlinked_trips": "unlinked_trip_weight",
        }
        if drop_source_weight:
            del has_weight["unlinked_trips"]

        with pytest.raises(ValueError, match=match):
            propagate_weights(
                tables,
                has_weight,
                skip={"persons", "person_days", "unlinked_trips"},
                usability_flag_col="usable",
            )


class TestPropagateUsableColumn:
    """Each record's weight is spread across the usable records below it."""

    def test_unusable_records_get_zero_weight(self):
        """A record the flag excludes gets 0 at every level of the walk.

        Person 2's only day is unusable, so their weight is not represented at the
        day level; it is not pooled onto person 1's day.
        """
        tables = _make_tables(complete=True)

        propagate_weights(tables, dict(HH_ONLY), usability_flag_col="survey_complete")

        assert tables["persons"].sort("person_id")["person_weight"].to_list() == [10.0, 10.0, 0.0]
        person_days = tables["person_days"].sort("person_day_id")
        assert person_days["person_day_weight"].to_list() == [10.0, 0.0, 0.0]
        ut = tables["unlinked_trips"].sort("unlinked_trip_id")
        assert ut["unlinked_trip_weight"].to_list() == [10.0, 10.0, 0.0, 0.0]
        lt = tables["linked_trips"].sort("linked_trip_id")
        assert lt["linked_trip_weight"].to_list() == [10.0, 0.0]

    def test_no_usable_column_keeps_carried_weights(self):
        """With usability_flag_col=None, unusable records keep the weight above them."""
        tables = _make_tables(complete=True)

        propagate_weights(tables, dict(HH_ONLY), usability_flag_col=None)

        persons = tables["persons"].sort("person_id")
        assert persons["person_weight"].to_list() == [10.0, 10.0, 20.0]
        person_days = tables["person_days"].sort("person_day_id")
        assert person_days["person_day_weight"].to_list() == [10.0, 10.0, 20.0]

    def test_unusable_aggregate_target_carries_no_weight(self):
        """A tour flagged unusable gets 0, even if a member trip has weight."""
        tables = _make_tables(complete=True)
        tables["linked_trips"] = tables["linked_trips"].with_columns(
            pl.lit(value=True).alias("survey_complete")
        )
        tables["tours"] = tables["tours"].with_columns(pl.lit(value=False).alias("survey_complete"))

        propagate_weights(tables, dict(HH_ONLY), usability_flag_col="survey_complete")

        assert tables["linked_trips"]["linked_trip_weight"].to_list() == [10.0, 0.0]
        assert tables["tours"]["tour_weight"].to_list() == [0.0]


class TestPropagateRedistribution:
    """Days split their person's weight; trips conserve their day's claim."""

    def test_usable_days_split_and_usable_trips_absorb(self):
        """2 of 4 days kept: each is half the person; 1 of 4 trips kept: it is 4x its day."""
        tables = {
            "households": pl.DataFrame(
                {"hh_id": [1], "hh_weight": [10.0], "survey_complete": [True]}
            ),
            "persons": pl.DataFrame({"person_id": [1], "hh_id": [1], "survey_complete": [True]}),
            "person_days": pl.DataFrame(
                {
                    "person_day_id": [10, 20, 30, 40],
                    "person_id": [1, 1, 1, 1],
                    "hh_id": [1, 1, 1, 1],
                    "survey_complete": [True, True, False, False],
                }
            ),
            "unlinked_trips": pl.DataFrame(
                {
                    "unlinked_trip_id": [100, 200, 300, 400],
                    "person_day_id": [10, 10, 10, 10],
                    "linked_trip_id": [1, 1, 2, 2],
                    "survey_complete": [True, False, False, False],
                }
            ),
            "linked_trips": pl.DataFrame({"linked_trip_id": [1, 2], "tour_id": [1, 1]}),
            "joint_trips": None,
            "tours": pl.DataFrame({"tour_id": [1]}),
        }

        propagate_weights(tables, dict(HH_ONLY), usability_flag_col="survey_complete")

        person_days = tables["person_days"].sort("person_day_id")
        assert person_days["person_day_weight"].to_list() == [5.0, 5.0, 0.0, 0.0]
        ut = tables["unlinked_trips"].sort("unlinked_trip_id")
        assert ut["unlinked_trip_weight"].to_list() == [20.0, 0.0, 0.0, 0.0]

    @pytest.mark.parametrize(
        ("hh_weight", "usable_days", "expected_day_weights", "expected_per_person"),
        [
            pytest.param(
                10.0,
                [True, True, False, False],
                [5.0, 5.0, 0.0, 0.0],
                [10.0, 0.0],
                id="person_2_kept_no_day",
            ),
            pytest.param(
                100.0,
                [True, True, True, False],
                [50.0, 50.0, 100.0, 0.0],
                [100.0, 100.0],
                id="persons_kept_different_day_counts",
            ),
            pytest.param(
                10.0,
                [False, False, False, False],
                [0.0, 0.0, 0.0, 0.0],
                [0.0, 0.0],
                id="nobody_kept_a_day",
            ),
        ],
    )
    def test_person_with_no_usable_day_is_a_shortfall_not_pooled(
        self, hh_weight, usable_days, expected_day_weights, expected_per_person
    ):
        """Each person's usable days split that person's weight, and nobody else's.

        A person with no usable day keeps their person weight, which is then not
        represented at the day level: the other person's days do not grow to
        cover it, and the shortfall is not rescaled away.
        """
        tables = {
            "households": pl.DataFrame(
                {"hh_id": [1], "hh_weight": [hh_weight], "survey_complete": [True]}
            ),
            "persons": pl.DataFrame(
                {"person_id": [1, 2], "hh_id": [1, 1], "survey_complete": [True, True]}
            ),
            "person_days": pl.DataFrame(
                {
                    "person_day_id": [10, 20, 30, 40],
                    "person_id": [1, 1, 2, 2],
                    "hh_id": [1, 1, 1, 1],
                    "survey_complete": usable_days,
                }
            ),
            "unlinked_trips": None,
            "linked_trips": None,
            "joint_trips": None,
            "tours": None,
        }

        propagate_weights(tables, dict(HH_ONLY), usability_flag_col="survey_complete")

        assert tables["persons"]["person_weight"].to_list() == [hh_weight, hh_weight]
        person_days = tables["person_days"].sort("person_day_id")
        assert person_days["person_day_weight"].to_list() == expected_day_weights
        per_person = (
            tables["person_days"]
            .group_by("person_id")
            .agg(pl.col("person_day_weight").sum())
            .sort("person_id")
        )
        assert per_person["person_day_weight"].to_list() == pytest.approx(expected_per_person)


def _make_tables_with_joints() -> dict[str, pl.DataFrame]:
    """Two members of one joint trip and joint tour, carrying different weights.

    Person 1 reports two usable days and person 2 one, so the day split leaves
    their trips on 15 and 30: sum 45, mean 22.5, first 15, max 30 all differ.
    """
    both = [True, True]
    return {
        "households": pl.DataFrame({"hh_id": [1], "hh_weight": [30.0], "usable": [True]}),
        "persons": pl.DataFrame({"person_id": [1, 2], "hh_id": [1, 1], "usable": both}),
        "person_days": pl.DataFrame(
            {
                "person_day_id": [10, 11, 20],
                "person_id": [1, 1, 2],
                "hh_id": [1, 1, 1],
                "usable": [True, True, True],
            }
        ),
        "unlinked_trips": pl.DataFrame(
            {
                "unlinked_trip_id": [100, 200],
                "person_day_id": [10, 20],
                "linked_trip_id": [1, 2],
                "usable": both,
            }
        ),
        "linked_trips": pl.DataFrame(
            {
                "linked_trip_id": [1, 2],
                "person_day_id": [10, 20],
                "tour_id": [1, 2],
                "joint_trip_id": [500, 500],
                "usable": both,
            }
        ),
        "joint_trips": pl.DataFrame({"joint_trip_id": [500], "usable": [True]}),
        "tours": pl.DataFrame(
            {
                "tour_id": [1, 2],
                "person_day_id": [10, 20],
                "joint_tour_id": [900, 900],
                "usable": both,
            }
        ),
        "joint_tours": pl.DataFrame({"joint_tour_id": [900], "usable": [True]}),
    }


class TestJointWeights:
    """A joint trip or tour weighs the sum of its kept members."""

    @pytest.mark.parametrize(
        ("joint_table", "joint_weight", "member_table", "member_id", "member_weight"),
        [
            pytest.param(
                "joint_trips",
                "joint_trip_weight",
                "linked_trips",
                "linked_trip_id",
                "linked_trip_weight",
                id="trips",
            ),
            pytest.param(
                "joint_tours",
                "joint_tour_weight",
                "tours",
                "tour_id",
                "tour_weight",
                id="tours",
            ),
        ],
    )
    def test_joint_weight_is_the_sum_and_nothing_else(
        self, joint_table, joint_weight, member_table, member_id, member_weight
    ):
        """45 is the sum; the mean, the first and the max are 22.5, 15 and 30."""
        tables = _make_tables_with_joints()
        propagate_weights(tables, dict(HH_ONLY), usability_flag_col="usable")

        members = tables[member_table].sort(member_id)[member_weight].to_list()
        assert members == pytest.approx([15.0, 30.0])
        weight = tables[joint_table][joint_weight][0]
        assert weight == pytest.approx(45.0)
        for imitation in (22.5, 15.0, 30.0):
            assert weight != pytest.approx(imitation)

    def test_a_grouping_with_no_weighted_member_is_zero_not_null(self):
        """Zero is a weight; null would silently drop out of downstream sums."""
        tables = _make_tables_with_joints()
        for name in ("unlinked_trips", "linked_trips"):
            tables[name] = tables[name].with_columns(pl.lit(value=False).alias("usable"))
        propagate_weights(tables, dict(HH_ONLY), usability_flag_col="usable")

        weight = tables["joint_trips"]["joint_trip_weight"][0]
        assert weight is not None
        assert weight == 0.0


def _make_tables_with_a_tour_off_its_joint_tour():
    """Three tours the survey grouped; under profile ``p`` only two are on it.

    Every record is usable and the household's 10 reaches every tour unsplit,
    so only summing by ``joint_tour_id_p`` makes the joint weight 20, not 30.
    """
    flag = "usable_p"
    three = [True, True, True]
    return {
        "households": pl.DataFrame({"hh_id": [1], "hh_weight": [10.0], flag: [True]}),
        "persons": pl.DataFrame({"person_id": [1, 2, 3], "hh_id": [1, 1, 1], flag: three}),
        "person_days": pl.DataFrame(
            {"person_day_id": [10, 20, 30], "person_id": [1, 2, 3], "hh_id": [1, 1, 1], flag: three}
        ),
        "unlinked_trips": pl.DataFrame(
            {
                "unlinked_trip_id": [100, 200, 300],
                "person_day_id": [10, 20, 30],
                "linked_trip_id": [1, 2, 3],
                flag: three,
            }
        ),
        "linked_trips": pl.DataFrame(
            {
                "linked_trip_id": [1, 2, 3],
                "person_day_id": [10, 20, 30],
                "tour_id": [1, 2, 3],
                "joint_trip_id": [500, 500, 500],
                "joint_trip_id_p": [500, 500, None],
                flag: three,
            }
        ),
        "tours": pl.DataFrame(
            {
                "tour_id": [1, 2, 3],
                "person_day_id": [10, 20, 30],
                "joint_tour_id": [900, 900, 900],
                "joint_tour_id_p": [900, 900, None],
                flag: three,
            }
        ),
        "joint_trips": pl.DataFrame({"joint_trip_id": [500], flag: [True]}),
        "joint_tours": pl.DataFrame({"joint_tour_id": [900], flag: [True]}),
    }


class TestJointWeightsFollowTheProfilesJointIds:
    """A tour the profile took off its joint tour is not in that tour's weight."""

    def test_the_sum_and_the_check_cover_only_the_tours_on_it(self):
        """Three tours of 10 in the survey's group; the profile keeps two: 20, and it reconciles."""
        tables = _make_tables_with_a_tour_off_its_joint_tour()
        propagate_weights(tables, dict(HH_ONLY), usability_flag_col="usable_p")

        assert tables["tours"]["tour_weight"].to_list() == pytest.approx([10.0, 10.0, 10.0])
        assert tables["joint_tours"]["joint_tour_weight"][0] == pytest.approx(20.0)
        assert tables["joint_trips"]["joint_trip_weight"][0] == pytest.approx(20.0)
        _check_joint_sums(tables, "usable_p")

    def test_without_a_profile_copy_the_bare_id_is_summed(self):
        """A flag outside the family has no copy to read, so the survey's group is the sum."""
        tables = _make_tables_with_a_tour_off_its_joint_tour()
        tables["tours"] = tables["tours"].drop("joint_tour_id_p")
        tables["linked_trips"] = tables["linked_trips"].drop("joint_trip_id_p")
        propagate_weights(tables, dict(HH_ONLY), usability_flag_col="usable_p")
        assert tables["joint_tours"]["joint_tour_weight"][0] == pytest.approx(30.0)


class TestTheSeedGate:
    """``seed_admits``: who the balancer fits. Both conditions must hold; a null is not a yes."""

    @staticmethod
    def _admitted(flag: str = "profile") -> list[str]:
        return GATE_FRAME.filter(seed_admits(flag))["case"].to_list()

    def test_only_the_fully_qualified_case_is_admitted(self):
        """Stated as the whole set, so a new admission cannot slip in unnoticed."""
        assert self._admitted() == ["both"]

    def test_gating_on_survey_complete_seeds_every_complete_household(self):
        """Including ones no profile admits; a null profile flag does not matter here."""
        assert self._admitted("survey_complete") == ["both", "complete only", "null flag"]

    def test_survey_complete_does_not_require_a_profile_column(self):
        """A survey-analysis run may have no profile columns at all."""
        frame = pl.DataFrame({"survey_complete": [True, False]})
        assert frame.filter(seed_admits("survey_complete")).height == 1

    def test_everything_it_admits_is_also_usable(self):
        """So a seeded household is never zeroed by the propagation that follows."""
        usable = GATE_FRAME.filter(is_usable("profile"))["case"].to_list()
        assert set(self._admitted()) <= set(usable)


def _empty_totals() -> ControlTotals:
    """Controls with nothing to compare, so only the hierarchy checks run."""
    return ControlTotals(
        totals=pl.DataFrame(
            schema={
                "zone_group_id": pl.String,
                "control_name": pl.String,
                "category": pl.String,
                "target_total": pl.Float64,
            }
        ),
        pums_hh_count=0,
        pums_person_count=0,
        zone_group_ids=[],
    )


def _weighted(
    person_weight: list[float], day_weight: list[float], usable: list[bool]
) -> dict[str, pl.DataFrame]:
    """One household of 100; its persons carry *person_weight*; person 1 has two days."""
    person_ids = list(range(1, len(person_weight) + 1))
    return {
        "households": pl.DataFrame(
            {"hh_id": [1], "hh_weight": [100.0], "zone_group_id": ["a"], "base_weight": [100.0]}
        ),
        "persons": pl.DataFrame(
            {
                "person_id": person_ids,
                "hh_id": [1] * len(person_ids),
                "person_weight": person_weight,
                FLAG: [True] * len(person_ids),
            }
        ),
        "person_days": pl.DataFrame(
            {
                "person_day_id": [1, 2],
                "person_id": [1, 1],
                "person_day_weight": day_weight,
                FLAG: usable,
            }
        ),
    }


def _nothing_usable() -> dict[str, pl.DataFrame]:
    """A person carrying weight whose every day was dropped.

    Read through the flag this is a reported shortfall; read without it, every
    day looks usable and the same data is a hierarchy failure.
    """
    return _weighted([100.0], [0.0, 0.0], [False, False])


class TestWeightSanityChecks:
    """``weight_sanity_checks`` runs, reads the flag, and still raises on a broken hierarchy."""

    def test_coherent_weights_pass(self):
        """Correct output must not raise."""
        tables = _weighted([100.0, 100.0], [50.0, 50.0], [True, True])
        weight_sanity_checks(tables, _empty_totals(), [], FLAG)

    def test_the_flag_reaches_the_hierarchy_check(self):
        """The one shape whose verdict depends on the flag passes, so the flag arrived."""
        weight_sanity_checks(_nothing_usable(), _empty_totals(), [], FLAG)

    def test_a_broken_hierarchy_still_raises(self):
        """A person who does not carry their household's weight is refused."""
        tables = _weighted([100.0, 7.0], [50.0, 50.0], [True, True])

        with pytest.raises(ValueError, match="Weight cascade broken"):
            weight_sanity_checks(tables, _empty_totals(), [], FLAG)

    def test_missing_tables_are_skipped_not_crashed(self):
        """A partial run is legitimate; the checks log and return."""
        weight_sanity_checks(
            {"households": pl.DataFrame({"hh_id": [1]})}, _empty_totals(), [], FLAG
        )

    def test_the_same_data_fails_when_the_flag_is_not_found(self):
        """Every record then looks usable, so the shortfall reads as a broken sum.

        Naming a column no table carries is not itself an error, which is why the
        caller has to pass the column the propagation actually used.
        """
        with pytest.raises(ValueError, match="Weight cascade broken"):
            _check_hierarchy(_nothing_usable(), "a_profile_nobody_stamped")


def _joint_trip(weight: float, usable: bool) -> dict[str, pl.DataFrame]:
    """Two linked trips of 5 on one joint trip of *weight*."""
    return {
        "linked_trips": pl.DataFrame(
            {"linked_trip_id": [1, 2], "joint_trip_id": [10, 10], "linked_trip_weight": [5.0, 5.0]}
        ),
        "joint_trips": pl.DataFrame(
            {"joint_trip_id": [10], "joint_trip_weight": [weight], FLAG: [usable]}
        ),
    }


def test_joint_sums_skip_unusable_groupings():
    """An unusable joint trip is zeroed regardless of its members, so it is not checked."""
    _check_joint_sums(_joint_trip(0.0, usable=False), FLAG)


def test_a_usable_grouping_that_does_not_reconcile_raises():
    """A usable joint trip must equal the members it kept."""
    with pytest.raises(ValueError, match="Joint weight is not its members"):
        _check_joint_sums(_joint_trip(3.0, usable=True), FLAG)


class TestNoCallerOmitsTheFlag:
    """No call in src, tests, projects or scripts may drop a required ``usability_flag_col``.

    A caller in a module the suite never enters would otherwise only fail on a
    real run, so this walks the source instead of relying on coverage.
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
        """One entry per call site that dropped the argument."""
        misses = self._calls_missing_the_flag(self._functions_requiring_the_flag())

        assert misses == [], "call sites dropping a required usability_flag_col:\n" + "\n".join(
            misses
        )


def test_the_computed_path_holds_its_targets():
    """Propagation keeps the balanced household weights and the persons they represent.

    Household 1 (weight 12) has persons 11 and 12 usable and 13 not, so 13's
    share goes to 11 and 12 and the household still counts 3. Household 2 is
    not usable.
    """
    tables = {
        "households": pl.DataFrame(
            {"hh_id": [1, 2], "hh_weight": [12.0, 0.0], "usable_p": [True, False]}
        ),
        "persons": pl.DataFrame(
            {
                "person_id": [11, 12, 13, 21],
                "hh_id": [1, 1, 1, 2],
                "usable_p": [True, True, False, False],
            }
        ),
        "person_days": pl.DataFrame(
            {
                "person_day_id": [110, 120, 130, 210],
                "person_id": [11, 12, 13, 21],
                "hh_id": [1, 1, 1, 2],
                "usable_p": [True, True, False, False],
            }
        ),
        "unlinked_trips": None,
        "linked_trips": None,
        "joint_trips": None,
        "tours": None,
        "joint_tours": None,
    }
    propagate_weights(tables, dict(HH_ONLY), usability_flag_col="usable_p")

    assert tables["households"]["hh_weight"].sum() == pytest.approx(12.0, rel=1e-6)
    persons = tables["persons"].sort("person_id")
    assert persons["person_weight"].to_list() == pytest.approx([18.0, 18.0, 0.0, 0.0])
