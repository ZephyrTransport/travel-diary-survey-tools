"""``keep_usable``: keep what a usability profile admits, then check what survived agrees.

It filters each table on its own verdict column and this profile's copy of the
joint ids, then raises, never repairs, if a reference dangles, a joint id is
carried alone, or weight went missing: each is a guarantee of the cascade or the
weighting, so a failure is a bug upstream.
"""

import polars as pl
import pytest

from data_canon.codebook.tours import TourCategory, TourDataQuality
from processing.formatting.usable_records import keep_usable

PROFILE = "ctramp"
FLAG = "usable_ctramp"  # the column that profile stamps


def _tours(rows: list[tuple[int, int | None, bool]]) -> pl.DataFrame:
    """Rows of (tour_id, joint_tour_id, usable)."""
    return pl.DataFrame(
        {
            "tour_id": [r[0] for r in rows],
            "joint_tour_id": [r[1] for r in rows],
            FLAG: [r[2] for r in rows],
        },
        schema_overrides={"joint_tour_id": pl.Int64},
    )


def _linked_trips(rows: list[tuple[int, int, int | None, bool]]) -> pl.DataFrame:
    """Rows of (linked_trip_id, tour_id, joint_trip_id, usable)."""
    return pl.DataFrame(
        {
            "linked_trip_id": [r[0] for r in rows],
            "tour_id": [r[1] for r in rows],
            "joint_trip_id": [r[2] for r in rows],
            FLAG: [r[3] for r in rows],
        },
        schema_overrides={"joint_trip_id": pl.Int64},
    )


def _groups(name: str, rows: list[tuple[int, bool]]) -> pl.DataFrame:
    return pl.DataFrame(
        {name: [r[0] for r in rows], FLAG: [r[1] for r in rows]},
        schema_overrides={name: pl.Int64},
    )


class TestTheProfilesJointIdsAreRead:
    """``joint_tour_id_<profile>`` becomes ``joint_tour_id``, on every table carrying it."""

    def test_the_bare_id_is_replaced_by_this_profiles_copy(self):
        """The survey grouped three tours; this profile keeps two on the joint tour."""
        kept = keep_usable(
            {
                "tours": pl.DataFrame(
                    {
                        "tour_id": [1, 2, 3],
                        "joint_tour_id": [500, 500, 500],
                        "joint_tour_id_ctramp": [500, 500, None],
                        "joint_tour_id_analysis": [500, 500, 500],
                        FLAG: [True, True, True],
                    }
                ),
                "linked_trips": pl.DataFrame(
                    {
                        "linked_trip_id": [10, 20, 30],
                        "tour_id": [1, 2, 3],
                        "joint_tour_id": [500, 500, 500],
                        "joint_tour_id_ctramp": [500, 500, None],
                        "joint_trip_id": [900, 900, 900],
                        "joint_trip_id_ctramp": [900, 900, None],
                        FLAG: [True, True, True],
                    }
                ),
                "unlinked_trips": pl.DataFrame(
                    {
                        "unlinked_trip_id": [100, 200, 300],
                        "tour_id": [1, 2, 3],
                        "joint_tour_id": [500, 500, 500],
                        "joint_tour_id_ctramp": [500, 500, None],
                        FLAG: [True, True, True],
                    }
                ),
                "joint_tours": _groups("joint_tour_id", [(500, True)]),
                "joint_trips": _groups("joint_trip_id", [(900, True)]),
            },
            PROFILE,
        )

        for name in ("tours", "linked_trips", "unlinked_trips"):
            assert kept[name]["joint_tour_id"].to_list() == [500, 500, None], name
            assert "joint_tour_id_ctramp" not in kept[name].columns
            assert "joint_tour_id_analysis" not in kept[name].columns
        assert kept["linked_trips"]["joint_trip_id"].to_list() == [900, 900, None]

    def test_a_table_without_a_copy_is_read_as_is(self):
        """The single-profile case: no suffixed column, the bare id stands."""
        kept = keep_usable(
            {
                "tours": _tours([(1, 500, True), (2, 500, True)]),
                "joint_tours": _groups("joint_tour_id", [(500, True)]),
            },
            PROFILE,
        )
        assert kept["tours"]["joint_tour_id"].to_list() == [500, 500]


class TestTheKeptRecordsMustAgree:
    """What the cascade guarantees, the gate verifies and refuses to patch."""

    def test_a_person_in_a_household_the_profile_rejects_raises(self):
        """A required reference to a record that is gone: the cascade did not carry down."""
        with pytest.raises(ValueError, match=r"1 persons record\(s\) .* reference a households"):
            keep_usable(
                {
                    "households": pl.DataFrame({"hh_id": [1, 2], FLAG: [True, False]}),
                    "persons": pl.DataFrame(
                        {"person_id": [10, 20], "hh_id": [1, 2], FLAG: [True, True]}
                    ),
                },
                PROFILE,
            )

    def test_a_trip_still_naming_a_rejected_joint_trip_raises(self):
        """An optional reference to a grouping that is gone: the cascade did not void it."""
        with pytest.raises(ValueError, match=r"linked_trips\.joint_trip_id -> joint_trips"):
            keep_usable(
                {
                    "tours": _tours([(1, None, True), (2, None, True)]),
                    "linked_trips": _linked_trips([(1, 1, 900, True), (2, 2, 900, True)]),
                    "joint_trips": _groups("joint_trip_id", [(900, False)]),
                },
                PROFILE,
            )

    def test_a_tour_still_naming_a_rejected_joint_tour_raises(self):
        """The same, one level up: a tour whose joint tour the profile rejected."""
        with pytest.raises(ValueError, match=r"tours\.joint_tour_id -> joint_tours"):
            keep_usable(
                {
                    "tours": _tours([(1, 500, True), (2, 500, True)]),
                    "joint_tours": _groups("joint_tour_id", [(500, False)]),
                },
                PROFILE,
            )

    def test_a_joint_id_carried_by_one_record_raises(self):
        """A group of one: the cascade did not dissolve it."""
        with pytest.raises(ValueError, match="carried by a single tours record"):
            keep_usable(
                {
                    "tours": _tours([(1, 500, True), (2, 500, False)]),
                    "joint_tours": _groups("joint_tour_id", [(500, True)]),
                },
                PROFILE,
            )

    def test_a_partly_joint_tour_passes_the_gate(self):
        """A joint tour with an unshared leg is consistent; refusing it is a formatter's rule."""
        kept = keep_usable(
            {
                "tours": _tours([(1, 500, True), (2, 500, True)]).rename({FLAG: "usable_analysis"}),
                "linked_trips": pl.DataFrame(
                    {
                        "linked_trip_id": [10, 11, 20],
                        "tour_id": [1, 1, 2],
                        "joint_tour_id": [500, 500, 500],
                        "joint_trip_id": [900, None, 900],
                        "usable_analysis": [True, True, True],
                    },
                    schema_overrides={"joint_trip_id": pl.Int64},
                ),
                "joint_tours": _groups("joint_tour_id", [(500, True)]).rename(
                    {FLAG: "usable_analysis"}
                ),
                "joint_trips": _groups("joint_trip_id", [(900, True)]).rename(
                    {FLAG: "usable_analysis"}
                ),
            },
            "analysis",
        )

        assert kept["linked_trips"]["joint_trip_id"].to_list() == [900, None, 900]

    def test_weight_on_an_excluded_record_raises(self):
        """The total in must be the total out; the weighting put 4 on a rejected tour."""
        tours = _tours([(1, None, True), (2, None, False)]).with_columns(
            tour_weight=pl.Series([10.0, 4.0])
        )
        with pytest.raises(ValueError, match=r"tours: tour_weight sums to 14\.0000 .* 10\.0000"):
            keep_usable({"tours": tours}, PROFILE)

    def test_a_dropped_record_with_weight_is_named(self):
        """Each dropped record carrying weight is named; tour 3 carries nothing and is not."""
        tours = _tours([(1, None, True), (2, None, False), (3, None, False)]).with_columns(
            tour_weight=pl.Series([10.0, 4.0, 0.0])
        )
        with pytest.raises(ValueError, match=r"1 tours .* tour_weight .* tour_id: \[2\]"):
            keep_usable({"tours": tours}, PROFILE)


class TestAnAdmittedGroupIsLeftAlone:
    """A consistent set goes through untouched."""

    def test_members_of_a_surviving_group_keep_their_id(self):
        """The cascade admitted it, so two members are usable and it stays joint."""
        kept = keep_usable(
            {
                "tours": _tours([(1, None, True), (2, None, True)]),
                "linked_trips": _linked_trips([(1, 1, 900, True), (2, 2, 900, True)]),
                "joint_trips": _groups("joint_trip_id", [(900, True)]),
            },
            PROFILE,
        )

        assert kept["joint_trips"].height == 1
        assert kept["linked_trips"]["joint_trip_id"].to_list() == [900, 900]

    def test_an_individual_trip_is_untouched(self):
        """A null group id was never a reference to check."""
        kept = keep_usable(
            {
                "tours": _tours([(1, None, True)]),
                "linked_trips": _linked_trips([(1, 1, None, True)]),
                "joint_trips": _groups("joint_trip_id", []),
            },
            PROFILE,
        )

        assert kept["linked_trips"]["joint_trip_id"].to_list() == [None]

    def test_excluded_records_carrying_nothing_pass_the_total_check(self):
        """Zero on a rejected record is the weighting doing its job."""
        tours = _tours([(1, None, True), (2, None, False)]).with_columns(
            tour_weight=pl.Series([10.0, 0.0])
        )
        kept = keep_usable({"tours": tours}, PROFILE)
        assert kept["tours"]["tour_weight"].to_list() == [10.0]


class TestTheGate:
    """The gate reads the named profile's verdict; it neither re-derives nor cascades it.

    The tour descriptors are on these fixtures because the summary log labels
    exclusions with them.
    """

    def _tours(self, rows: list[tuple[int, bool]]) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "tour_id": [r[0] for r in rows],
                "tour_data_quality": [TourDataQuality.VALID.value] * len(rows),
                "tour_category": [TourCategory.COMPLETE.value] * len(rows),
                "usable_test": [r[1] for r in rows],
            }
        )

    def _linked_trips(self, rows: list[tuple[int, bool]]) -> pl.DataFrame:
        """One non-joint linked trip per (tour id, verdict)."""
        return pl.DataFrame(
            {
                "linked_trip_id": [r[0] * 10 for r in rows],
                "tour_id": [r[0] for r in rows],
                "joint_trip_id": [None] * len(rows),
                "usable_test": [r[1] for r in rows],
            },
            schema_overrides={"joint_trip_id": pl.Int64},
        )

    def test_it_keeps_what_the_profile_admits(self):
        """Each table is filtered on its own verdict, not cascaded from tours."""
        kept = keep_usable(
            {
                "tours": self._tours([(1, True), (2, False), (3, True), (4, False)]),
                "linked_trips": self._linked_trips([(1, True), (2, False), (3, True), (4, False)]),
            },
            "test",
        )

        assert sorted(kept["tours"]["tour_id"].to_list()) == [1, 3]
        assert sorted(kept["linked_trips"]["tour_id"].to_list()) == [1, 3]

    def test_descriptors_alone_do_not_gate(self):
        """Tour descriptors without the profile's verdict column raise rather than gate."""
        tours = pl.DataFrame(
            {
                "tour_id": [1, 2],
                "tour_category": [TourCategory.COMPLETE.value, TourCategory.PARTIAL_END.value],
            }
        )

        with pytest.raises(ValueError, match="no 'usable_test' column"):
            keep_usable({"tours": tours}, "test")

    def test_a_missing_gate_names_what_is_present(self):
        """The error names the verdict columns that are present."""
        tours = pl.DataFrame({"tour_id": [1], "usable_ctramp": [True], "survey_complete": [True]})

        with pytest.raises(ValueError, match="ctramp"):
            keep_usable({"tours": tours}, "daysim")

    def test_a_null_verdict_excludes_rather_than_guesses(self):
        """A null means the cascade never reached the row -- not a licence to keep it."""
        tours = pl.DataFrame(
            {"tour_id": [1, 2], "usable_test": [True, None]},
            schema_overrides={"usable_test": pl.Boolean},
        )

        kept = keep_usable({"tours": tours}, "test")

        assert kept["tours"]["tour_id"].to_list() == [1]


class TestZeroWeightIsReportedWithItsCauses:
    """A kept record with zero weight is reported, not dropped; the weighting did not make it."""

    def test_the_message_names_the_supplied_zero_and_the_weighting(self, caplog):
        """Tour 2 is kept with weight 0: it stays, and the warning says where it came from."""
        tours = _tours([(1, None, True), (2, None, True)]).with_columns(
            tour_weight=pl.Series([10.0, 0.0])
        )
        with caplog.at_level("WARNING"):
            kept = keep_usable({"tours": tours}, PROFILE)
        assert kept["tours"]["tour_id"].to_list() == [1, 2]
        assert "1 tours admitted by usable_ctramp carry a zero tour_weight" in caplog.text
        assert "these weights came from neither" in caplog.text
        assert "parent" not in caplog.text
