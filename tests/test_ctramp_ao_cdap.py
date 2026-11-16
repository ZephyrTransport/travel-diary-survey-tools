"""aoResults and cdapResults: columns selected and renamed from formatted CT-RAMP tables."""

import polars as pl

from data_canon.codebook.ctramp import CTRAMPActivityPattern
from processing.formatting.ctramp.format_ao import format_ao_results
from processing.formatting.ctramp.format_cdap import format_cdap_results


def test_format_ao_results_maps_autos_to_ao():
    """AoResults is HHID + AO, taken from hh_id + autos."""
    households_ctramp = pl.DataFrame(
        {"hh_id": [101, 102], "autos": [0, 3], "extra_col": ["x", "y"]}
    )

    result = format_ao_results(households_ctramp)

    assert result.columns == ["HHID", "AO"]
    assert result.sort("HHID").rows() == [(101, 0), (102, 3)]


def test_format_cdap_results_schema_and_person_type_is_int():
    """CdapResults is exactly HHID, PersonID, PersonNum, PersonType (int), ActivityString."""
    persons_ctramp = pl.DataFrame(
        {
            "hh_id": [101, 101],
            "person_id": [10101, 10102],
            "person_num": [1, 2],
            "person_type": [1, 6],
            "activity_pattern": [
                CTRAMPActivityPattern.MANDATORY.value,
                CTRAMPActivityPattern.HOME.value,
            ],
            "extra_col": ["x", "y"],
        }
    )

    result = format_cdap_results(persons_ctramp)

    assert result.columns == ["HHID", "PersonID", "PersonNum", "PersonType", "ActivityString"]
    assert result.schema["PersonType"] == pl.Int32
    row = result.filter(pl.col("PersonID") == 10101).row(0, named=True)
    assert row["HHID"] == 101
    assert row["PersonNum"] == 1
    assert row["PersonType"] == 1
    assert row["ActivityString"] == CTRAMPActivityPattern.MANDATORY.value


def test_format_cdap_results_includes_person_weight_when_present():
    """The optional survey person_weight passes through when the column exists."""
    persons_ctramp = pl.DataFrame(
        {
            "hh_id": [101],
            "person_id": [10101],
            "person_num": [1],
            "person_type": [1],
            "activity_pattern": [CTRAMPActivityPattern.NON_MANDATORY.value],
            "person_weight": [2.5],
        }
    )

    result = format_cdap_results(persons_ctramp)

    assert "person_weight" in result.columns
    assert result["person_weight"][0] == 2.5
