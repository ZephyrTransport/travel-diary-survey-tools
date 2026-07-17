"""Tests for industry_empsix derivation in the CT-RAMP formatter.

industry_empsix (the six-category empsix employment sector) is derived in the
CT-RAMP formatter from the canonical `industry` code, with a keyword fallback on
the free-text `industry_other`. It is not a canonical field and is not derived
in project cleaning steps.
"""

import polars as pl

from data_canon.codebook.ctramp import CTRAMPIndustry
from data_canon.codebook.persons import Industry
from processing.formatting.ctramp.mappings import add_industry_empsix


def test_structured_industry_maps_to_empsix():
    """Each structured NAICS industry code maps to its empsix sector."""
    persons = pl.DataFrame(
        {
            "person_id": [1, 2, 3],
            "industry": [
                Industry.RETAIL_TRADE.value,
                Industry.HEALTH_AND_SOCIAL.value,
                Industry.FINANCE_AND_INSURANCE.value,
            ],
        }
    )

    out = add_industry_empsix(persons).sort("person_id")

    assert out["industry_empsix"].to_list() == [
        CTRAMPIndustry.RETEMPN.value,
        CTRAMPIndustry.HEREMPN.value,
        CTRAMPIndustry.FPSEMPN.value,
    ]


def test_industry_other_fills_only_unresolved_rows():
    """The free-text fallback fills empsix only where the structured code did not."""
    persons = pl.DataFrame(
        {
            "person_id": [1, 2],
            # 995 (Missing) has no structured mapping -> falls back to free-text
            "industry": [Industry.RETAIL_TRADE.value, 995],
            "industry_other": pl.Series([None, "Software startup"], dtype=pl.String),
        }
    )

    out = add_industry_empsix(persons).sort("person_id")

    assert out["industry_empsix"].to_list() == [
        CTRAMPIndustry.RETEMPN.value,  # structured mapping wins
        CTRAMPIndustry.FPSEMPN.value,  # "software" keyword fallback
    ]


def test_unresolved_industry_is_null_without_free_text():
    """With no industry_other column, unmapped codes stay null (no fallback)."""
    persons = pl.DataFrame({"person_id": [1], "industry": [995]})

    out = add_industry_empsix(persons)

    assert out["industry_empsix"].to_list() == [None]


def test_missing_industry_column_yields_null_column():
    """When industry is absent entirely, a null industry_empsix column is added."""
    persons = pl.DataFrame({"person_id": [1, 2]})

    out = add_industry_empsix(persons)

    assert "industry_empsix" in out.columns
    assert out["industry_empsix"].null_count() == 2
