"""Tests for weight_propagation shared helpers."""

import polars as pl
import pytest

from processing.weighting.balancing.weight_propagation import (
    HIERARCHY,
    WEIGHT_COLUMNS,
    WEIGHT_CONFIG_MAPPING,
    collect_tables,
    non_null_tables,
    propagate_weights,
    safe_join_weight,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


class TestConstants:
    """Verify the shared constant dictionaries are consistent."""

    def test_weight_config_mapping_keys(self):
        """Every hierarchy level exposes a config key for supplying its weight."""
        assert len(WEIGHT_CONFIG_MAPPING) == len(HIERARCHY)
        assert "household_weights" in WEIGHT_CONFIG_MAPPING
        assert "tour_weights" in WEIGHT_CONFIG_MAPPING
        assert "joint_tour_weights" in WEIGHT_CONFIG_MAPPING

    def test_weight_columns_derived_from_mapping(self):
        """WEIGHT_COLUMNS should have one entry per table in the mapping."""
        expected = {table: wt for table, _, wt in WEIGHT_CONFIG_MAPPING.values()}
        assert expected == WEIGHT_COLUMNS


# ---------------------------------------------------------------------------
# collect_tables / non_null_tables
# ---------------------------------------------------------------------------


class TestCollectTables:
    """Tests for collect_tables, which bundles provided tables into a dict."""

    def test_collect_all_none(self):
        """If all tables are None, collect_tables returns a dict of all None."""
        tables = collect_tables()
        assert all(v is None for v in tables.values())
        assert len(tables) == len(HIERARCHY)

    def test_collect_partial(self):
        """collect_tables correctly collects provided tables and fills in None."""
        hh = pl.DataFrame({"hh_id": [1]})
        tables = collect_tables(households=hh)
        assert tables["households"] is hh
        assert tables["persons"] is None

    def test_non_null_tables_filters(self):
        """non_null_tables filters out None values from the tables dict."""
        hh = pl.DataFrame({"hh_id": [1]})
        tables = collect_tables(households=hh)
        result = non_null_tables(tables)
        assert list(result.keys()) == ["households"]


# ---------------------------------------------------------------------------
# safe_join_weight
# ---------------------------------------------------------------------------


class TestSafeJoinWeight:
    """Tests for safe_join_weight, which joins a weight column to a table while."""

    def test_basic_join(self):
        """Basic left join of weight column from w to df."""
        df = pl.DataFrame({"hh_id": [1, 2], "size": [3, 4]})
        w = pl.DataFrame({"hh_id": [1, 2], "hh_weight": [1.5, 2.0]})
        result = safe_join_weight(df, w, "hh_id")
        assert "hh_weight" in result.columns
        assert result["hh_weight"].to_list() == [1.5, 2.0]

    def test_drops_existing_weight_col(self):
        """If the target already has the weight column, drop it first."""
        df = pl.DataFrame({"hh_id": [1, 2], "hh_weight": [0.0, 0.0]})
        w = pl.DataFrame({"hh_id": [1, 2], "hh_weight": [1.5, 2.0]})
        result = safe_join_weight(df, w, "hh_id")
        assert result["hh_weight"].to_list() == [1.5, 2.0]

    def test_left_join_null_for_missing(self):
        """If w has fewer rows than df, missing rows get null weights."""
        df = pl.DataFrame({"hh_id": [1, 2, 3]})
        w = pl.DataFrame({"hh_id": [1, 2], "hh_weight": [1.5, 2.0]})
        result = safe_join_weight(df, w, "hh_id")
        assert result["hh_weight"].to_list() == [1.5, 2.0, None]


# ---------------------------------------------------------------------------
# propagate_weights -- carry-forward
# ---------------------------------------------------------------------------


def _make_tables():
    """Build a minimal set of canonical tables for propagation tests."""
    households = pl.DataFrame({"hh_id": [1, 2], "hh_weight": [10.0, 20.0]})
    persons = pl.DataFrame({"person_id": [1, 2, 3], "hh_id": [1, 1, 2]})
    days = pl.DataFrame({"day_id": [10, 20, 30], "person_id": [1, 2, 3]})
    unlinked_trips = pl.DataFrame(
        {
            "unlinked_trip_id": [100, 200, 300, 400],
            "day_id": [10, 10, 20, 30],
            "linked_trip_id": [1, 1, 2, 2],
        }
    )
    linked_trips = pl.DataFrame(
        {
            "linked_trip_id": [1, 2],
            "tour_id": [1, 1],
        }
    )
    tours = pl.DataFrame({"tour_id": [1]})
    return {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips,
        "linked_trips": linked_trips,
        "joint_trips": None,
        "tours": tours,
    }


class TestPropagateCarryForward:
    """Tests for the carry-forward (parent -> child) branch."""

    def test_full_hierarchy(self):
        """Weights propagate from households all the way to unlinked_trips."""
        tables = _make_tables()
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight)

        # persons get hh_weight via hh_id
        assert "person_weight" in tables["persons"].columns
        assert tables["persons"].sort("person_id")["person_weight"].to_list() == [
            10.0,
            10.0,
            20.0,
        ]

        # days get person_weight via person_id
        assert "day_weight" in tables["days"].columns
        assert tables["days"].sort("day_id")["day_weight"].to_list() == [
            10.0,
            10.0,
            20.0,
        ]

        # unlinked_trips get day_weight via day_id
        assert "unlinked_trip_weight" in tables["unlinked_trips"].columns
        assert tables["unlinked_trips"].sort("unlinked_trip_id")[
            "unlinked_trip_weight"
        ].to_list() == [10.0, 10.0, 10.0, 20.0]

        # has_weight tracks all (carry-forward + aggregation)
        assert has_weight == {
            "households": "hh_weight",
            "persons": "person_weight",
            "days": "day_weight",
            "unlinked_trips": "unlinked_trip_weight",
            "linked_trips": "linked_trip_weight",
            "tours": "tour_weight",
        }

    def test_error_parent_no_weight(self):
        """Error when child exists but parent has no weight (hierarchy gap)."""
        tables = _make_tables()
        # households present but NOT in has_weight -> gap
        has_weight: dict[str, str] = {}

        with pytest.raises(ValueError, match="has no weight column"):
            propagate_weights(tables, has_weight)

    def test_error_parent_df_is_none(self):
        """Error when has_weight says parent has weight but its DataFrame is None."""
        tables = _make_tables()
        tables["households"] = None  # remove the actual DataFrame
        has_weight: dict[str, str] = {"households": "hh_weight"}

        with pytest.raises(ValueError, match="parent table households is None"):
            propagate_weights(tables, has_weight)

    def test_error_child_missing_join_key(self):
        """Error when child table is missing the join key column."""
        tables = _make_tables()
        # Replace persons with a frame that lacks hh_id
        tables["persons"] = pl.DataFrame({"person_id": [1, 2, 3], "age": [25, 30, 40]})
        has_weight: dict[str, str] = {"households": "hh_weight"}

        with pytest.raises(ValueError, match="missing join key hh_id"):
            propagate_weights(tables, has_weight)

    def test_skip_prevents_carry_forward(self):
        """Tables in the skip set are not overwritten."""
        tables = _make_tables()
        # Give persons their own weight already
        tables["persons"] = tables["persons"].with_columns(pl.lit(99.0).alias("person_weight"))
        has_weight: dict[str, str] = {"households": "hh_weight"}

        # Skip persons, days, and unlinked_trips so we only test skip on persons
        propagate_weights(
            tables,
            has_weight,
            skip={"persons", "days", "unlinked_trips", "linked_trips", "tours"},
        )

        # persons weight should stay at 99, not be overwritten
        assert tables["persons"]["person_weight"].to_list() == [99.0, 99.0, 99.0]
        # persons not in has_weight because it was skipped
        assert "persons" not in has_weight

    def test_child_none_is_skipped(self):
        """If a child table is None, propagation skips it."""
        tables = _make_tables()
        tables["persons"] = None
        tables["days"] = None
        tables["unlinked_trips"] = None
        tables["linked_trips"] = None
        tables["tours"] = None
        has_weight: dict[str, str] = {"households": "hh_weight"}

        # Should not raise -- all downstream tables are None
        propagate_weights(tables, has_weight)

        assert has_weight == {"households": "hh_weight"}


# ---------------------------------------------------------------------------
# propagate_weights -- aggregation
# ---------------------------------------------------------------------------


class TestPropagateAggregate:
    """Tests for the aggregate (mean weight) branch."""

    def test_aggregate_linked_trips(self):
        """Linked trip weight = mean of component unlinked trip weights."""
        tables = _make_tables()
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight)

        assert "linked_trip_weight" in tables["linked_trips"].columns
        lt = tables["linked_trips"].sort("linked_trip_id")
        # linked_trip 1 has unlinked_trips 100 (wt=10) and 200 (wt=10) -> mean=10
        # linked_trip 2 has unlinked_trips 300 (wt=10) and 400 (wt=20) -> mean=15
        assert lt["linked_trip_weight"].to_list() == [10.0, 15.0]

    def test_aggregate_tours(self):
        """Tour weight = mean of linked trip weights."""
        tables = _make_tables()
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight)

        assert "tour_weight" in tables["tours"].columns
        # tour 1 has linked_trips 1 (wt=10) and 2 (wt=15) -> mean=12.5
        assert tables["tours"]["tour_weight"].to_list() == [12.5]

    def test_aggregate_excludes_zeros_and_nulls(self):
        """Zeros and nulls are excluded from the mean aggregation."""
        tables = _make_tables()
        # Manually set unlinked trip weights (bypass carry-forward)
        tables["unlinked_trips"] = pl.DataFrame(
            {
                "unlinked_trip_id": [100, 200, 300, 400],
                "day_id": [10, 10, 20, 30],
                "linked_trip_id": [1, 1, 2, 2],
                "unlinked_trip_weight": [5.0, 0.0, None, 3.0],
            }
        )
        has_weight: dict[str, str] = {
            "households": "hh_weight",
            "persons": "person_weight",
            "days": "day_weight",
            "unlinked_trips": "unlinked_trip_weight",
        }

        propagate_weights(tables, has_weight, skip={"persons", "days", "unlinked_trips"})

        lt = tables["linked_trips"].sort("linked_trip_id")
        # linked_trip 1: mean(5.0) = 5.0  (zero excluded)
        # linked_trip 2: mean(3.0) = 3.0  (null excluded)
        assert lt["linked_trip_weight"].to_list() == [5.0, 3.0]

    def test_error_aggregate_source_missing_group_key(self):
        """Error when source table for aggregation is missing the group key."""
        tables = _make_tables()
        # Remove linked_trip_id from unlinked_trips
        tables["unlinked_trips"] = pl.DataFrame(
            {
                "unlinked_trip_id": [100, 200],
                "day_id": [10, 10],
                "unlinked_trip_weight": [5.0, 3.0],
            }
        )
        has_weight: dict[str, str] = {
            "households": "hh_weight",
            "persons": "person_weight",
            "days": "day_weight",
            "unlinked_trips": "unlinked_trip_weight",
        }

        with pytest.raises(ValueError, match="missing linked_trip_id"):
            propagate_weights(
                tables,
                has_weight,
                skip={"persons", "days", "unlinked_trips"},
            )

    def test_aggregate_skip_when_target_none(self):
        """Aggregation skips when the target table is None."""
        tables = _make_tables()
        tables["linked_trips"] = None
        tables["tours"] = None
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight)

        assert "linked_trips" not in has_weight
        assert "tours" not in has_weight

    def test_error_aggregate_source_none(self):
        """Error when aggregate target exists but source table is None."""
        tables = _make_tables()
        has_weight: dict[str, str] = {
            "households": "hh_weight",
            "persons": "person_weight",
            "days": "day_weight",
            "unlinked_trips": "unlinked_trip_weight",
        }
        # Source for linked_trips aggregation is None
        tables["unlinked_trips"] = None

        with pytest.raises(ValueError, match="source table unlinked_trips is None"):
            propagate_weights(
                tables,
                has_weight,
                skip={"persons", "days", "unlinked_trips"},
            )

    def test_error_aggregate_source_no_weight(self):
        """Error when aggregate target exists but source has no weight."""
        tables = _make_tables()
        # unlinked_trips exists but has no weight in has_weight
        has_weight: dict[str, str] = {
            "households": "hh_weight",
            "persons": "person_weight",
            "days": "day_weight",
        }

        with pytest.raises(ValueError, match="source table unlinked_trips has no weight"):
            propagate_weights(
                tables,
                has_weight,
                skip={"persons", "days", "unlinked_trips"},
            )


# ---------------------------------------------------------------------------
# propagate_weights -- completion flag
# ---------------------------------------------------------------------------


def _make_tables_with_complete():
    """Build tables where some records are marked incomplete."""
    households = pl.DataFrame(
        {
            "hh_id": [1, 2],
            "hh_weight": [10.0, 20.0],
            "complete": [True, False],
        }
    )
    persons = pl.DataFrame(
        {
            "person_id": [1, 2, 3],
            "hh_id": [1, 1, 2],
            "complete": [True, True, False],
        }
    )
    days = pl.DataFrame(
        {
            "day_id": [10, 20, 30],
            "person_id": [1, 2, 3],
            "hh_id": [1, 1, 2],
            "complete": [True, False, False],
        }
    )
    unlinked_trips = pl.DataFrame(
        {
            "unlinked_trip_id": [100, 200, 300, 400],
            "day_id": [10, 10, 20, 30],
            "linked_trip_id": [1, 1, 2, 2],
            "complete": [True, True, False, False],
        }
    )
    linked_trips = pl.DataFrame(
        {
            "linked_trip_id": [1, 2],
            "tour_id": [1, 1],
        }
    )
    tours = pl.DataFrame({"tour_id": [1]})
    return {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips,
        "linked_trips": linked_trips,
        "joint_trips": None,
        "tours": tours,
    }


def _make_tables_partial_usability():
    """One household whose parents each keep only some of their children.

    Person 1 reports 4 days of which 2 are usable; day 10 holds 4 trips of which
    1 is usable. Every parent keeps at least one child, so the carry-forward
    checksum must hold exactly at every level.
    """
    households = pl.DataFrame({"hh_id": [1], "hh_weight": [10.0], "complete": [True]})
    persons = pl.DataFrame({"person_id": [1], "hh_id": [1], "complete": [True]})
    days = pl.DataFrame(
        {
            "day_id": [10, 20, 30, 40],
            "person_id": [1, 1, 1, 1],
            "hh_id": [1, 1, 1, 1],
            "complete": [True, True, False, False],
        }
    )
    unlinked_trips = pl.DataFrame(
        {
            "unlinked_trip_id": [100, 200, 300, 400],
            "day_id": [10, 10, 10, 10],
            "linked_trip_id": [1, 1, 2, 2],
            "complete": [True, False, False, False],
        }
    )
    return {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips,
        "linked_trips": pl.DataFrame({"linked_trip_id": [1, 2], "tour_id": [1, 1]}),
        "joint_trips": None,
        "tours": pl.DataFrame({"tour_id": [1]}),
    }


class TestPropagateUsableColumn:
    """Tests for spreading each parent's weight across its usable children.

    Usability defaults to ``model_usable``; these exercise it against
    ``complete``, the other supported column.
    """

    def test_unusable_persons_get_zero_weight(self):
        """Persons with complete=False get weight 0 even if parent HH has weight."""
        tables = _make_tables_with_complete()
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight, usability_flag_col="complete")

        persons = tables["persons"].sort("person_id")
        assert persons["person_weight"].to_list() == [10.0, 10.0, 0.0]

    def test_no_usable_column_keeps_carried_weights(self):
        """With usability_flag_col=None, unusable children keep the parent weight."""
        tables = _make_tables_with_complete()
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight, usability_flag_col=None)

        # Person 3 (complete=False, in HH 2 with weight 20) keeps 20 instead of 0
        persons = tables["persons"].sort("person_id")
        assert persons["person_weight"].to_list() == [10.0, 10.0, 20.0]
        days = tables["days"].sort("day_id")
        assert days["day_weight"].to_list() == [10.0, 10.0, 20.0]

    def test_unusable_days_get_zero_weight(self):
        """Days with complete=False get weight 0; the household keeps their claim.

        Person 2's only day is unusable, so within household 1 its claim moves to
        day 10 -- the household's day-weight is conserved, not shrunk.
        """
        tables = _make_tables_with_complete()
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight, usability_flag_col="complete")

        days = tables["days"].sort("day_id")
        assert days["day_weight"].to_list() == [20.0, 0.0, 0.0]
        # Household 1 claimed 2 days at a person weight of 10 apiece.
        assert days["day_weight"].sum() == pytest.approx(20.0)

    def test_unusable_unlinked_trips_get_zero_weight(self):
        """Unlinked trips with complete=False get weight 0."""
        tables = _make_tables_with_complete()
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight, usability_flag_col="complete")

        ut = tables["unlinked_trips"].sort("unlinked_trip_id")
        # Day 10 carries 20.0 and both its trips are usable, so each keeps 20.0.
        assert ut["unlinked_trip_weight"].to_list() == [20.0, 20.0, 0.0, 0.0]

    def test_aggregate_excludes_zero_from_unusable(self):
        """Aggregated weights exclude zeros from unusable records."""
        tables = _make_tables_with_complete()
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight, usability_flag_col="complete")

        lt = tables["linked_trips"].sort("linked_trip_id")
        # linked_trip 1: unlinked 100 (wt=20) + 200 (wt=20) -> mean=20
        # linked_trip 2: unlinked 300 (wt=0, incomplete) + 400 (wt=0, incomplete) -> 0 (all zero)
        assert lt["linked_trip_weight"].to_list() == [20.0, 0.0]

    def test_missing_usable_column_propagates_normally(self):
        """Without the usability column present, weights propagate as before."""
        tables = _make_tables()  # no complete/model_usable column
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight, usability_flag_col="complete")

        persons = tables["persons"].sort("person_id")
        assert persons["person_weight"].to_list() == [10.0, 10.0, 20.0]

    def test_unusable_aggregate_target_carries_no_weight(self):
        """A tour flagged unusable gets 0, even if a member trip has weight."""
        tables = _make_tables_with_complete()
        tables["linked_trips"] = tables["linked_trips"].with_columns(
            pl.lit(value=True).alias("complete")
        )
        tables["tours"] = tables["tours"].with_columns(pl.lit(value=False).alias("complete"))
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight, usability_flag_col="complete")

        assert tables["linked_trips"]["linked_trip_weight"].to_list() == [20.0, 0.0]
        assert tables["tours"]["tour_weight"].to_list() == [0.0]

    def test_all_usable_propagates_normally(self):
        """When every record is usable, weights propagate normally."""
        tables = _make_tables_with_complete()
        # Override all to complete
        tables["persons"] = tables["persons"].with_columns(pl.lit(value=True).alias("complete"))
        tables["days"] = tables["days"].with_columns(pl.lit(value=True).alias("complete"))
        tables["unlinked_trips"] = tables["unlinked_trips"].with_columns(
            pl.lit(value=True).alias("complete")
        )
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight, usability_flag_col="complete")

        persons = tables["persons"].sort("person_id")
        assert persons["person_weight"].to_list() == [10.0, 10.0, 20.0]


class TestPropagateRedistribution:
    """The parent's weight is spread across the children it kept, not shrunk."""

    def test_usable_days_absorb_the_unusable_ones(self):
        """A person keeping 2 of 4 days doubles the weight of each survivor."""
        tables = _make_tables_partial_usability()
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight, usability_flag_col="complete")

        days = tables["days"].sort("day_id")
        assert days["day_weight"].to_list() == [20.0, 20.0, 0.0, 0.0]

    def test_usable_trips_absorb_the_unusable_ones(self):
        """A day keeping 1 of 4 trips gives that trip four times the day weight."""
        tables = _make_tables_partial_usability()
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight, usability_flag_col="complete")

        ut = tables["unlinked_trips"].sort("unlinked_trip_id")
        # day 10 carries 20.0 and kept 1 of its 4 trips -> 20 * 4/1
        assert ut["unlinked_trip_weight"].to_list() == [80.0, 0.0, 0.0, 0.0]

    def test_checksum_holds_at_every_level(self):
        """sum(child) == parent_weight * n_children wherever a child survived."""
        tables = _make_tables_partial_usability()
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight, usability_flag_col="complete")

        # 1 household at 10.0 with 1 person
        assert tables["persons"]["person_weight"].sum() == pytest.approx(10.0)
        # 1 person at 10.0 with 4 days
        assert tables["days"]["day_weight"].sum() == pytest.approx(40.0)
        # day 10 carries 20.0 and holds all 4 trips
        assert tables["unlinked_trips"]["unlinked_trip_weight"].sum() == pytest.approx(80.0)

    def test_unrelated_person_days_are_covered_by_the_household(self):
        """A person with no usable day keeps their weight; their days move to the HH.

        Person 2 is an "unrelated person" who reported no usable travel day. They
        keep a person weight, but their day-weight allocation has no sibling day
        of their own to land on, so the household's remaining usable days stand
        in for it -- leaving sum(day_weight) per household intact.
        """
        tables = {
            "households": pl.DataFrame({"hh_id": [1], "hh_weight": [10.0], "complete": [True]}),
            "persons": pl.DataFrame(
                {"person_id": [1, 2], "hh_id": [1, 1], "complete": [True, True]}
            ),
            "days": pl.DataFrame(
                {
                    "day_id": [10, 20, 30, 40],
                    "person_id": [1, 1, 2, 2],
                    "hh_id": [1, 1, 1, 1],
                    "complete": [True, True, False, False],
                }
            ),
            "unlinked_trips": None,
            "linked_trips": None,
            "joint_trips": None,
            "tours": None,
        }
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight, usability_flag_col="complete")

        # Both persons keep their weight -- person 2 is still a real person.
        assert tables["persons"]["person_weight"].to_list() == [10.0, 10.0]

        days = tables["days"].sort("day_id")
        # Person 1's 2 usable days carry their own 2 days (10 each) plus person
        # 2's stranded 2 days (20 split over 2 usable days = 10 each).
        assert days["day_weight"].to_list() == pytest.approx([20.0, 20.0, 0.0, 0.0])
        # sum(day_weight) == sum over persons of person_weight * n_days
        assert days["day_weight"].sum() == pytest.approx(10.0 * 2 + 10.0 * 2)

    def test_parent_with_no_usable_child_leaves_a_shortfall(self):
        """A person keeping no day has nowhere to spread, so their days stay 0."""
        tables = _make_tables_partial_usability()
        tables["days"] = tables["days"].with_columns(pl.lit(value=False).alias("complete"))
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight, usability_flag_col="complete")

        # The shortfall is real and deliberate: it is not silently rescaled away.
        assert tables["days"]["day_weight"].to_list() == [0.0, 0.0, 0.0, 0.0]
