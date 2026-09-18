"""Tests for assembling the run's diagnostics document.

One report covers the whole run, so the cases that matter are the ones about
*how many fits* it describes: a single un-profiled fit must still render, and
several fits must each get their own pane without the run-level sections being
repeated.
"""

import json
import re

import numpy as np
import polars as pl
import pytest

from processing.weighting.core.specs import (
    ControlTotals,
    GeographyCoverage,
    ImputationSummary,
    ProfileFit,
    ZoneStatus,
)
from processing.weighting.diagnostics.comparison import Comparison, WeightSet, all_pairs
from processing.weighting.diagnostics.report import generate_report

TARGETS = ["h_total", "p_total"]
ZONE = "01"


def _control_totals() -> ControlTotals:
    """Targets for one zone at both structural controls."""
    return ControlTotals(
        totals=pl.DataFrame(
            {
                "geo_id": [ZONE, ZONE],
                "control_name": ["h_total", "p_total"],
                "category": ["total", "total"],
                "target_total": [1000.0, 2500.0],
            }
        ),
        pums_hh_count=500,
        pums_person_count=1200,
        geo_ids=[ZONE],
    )


def _fit(profile: str | None, *, n_hh: int = 4) -> ProfileFit:
    """A completed fit carrying the minimum every report section reads."""
    hh_ids = list(range(1, n_hh + 1))
    seed = pl.DataFrame(
        {
            "hh_id": hh_ids,
            "ctrl_geoid": [ZONE] * n_hh,
            "study_geoid": [ZONE] * n_hh,
            "h_total": [1.0] * n_hh,
            "p_total": [2.0] * n_hh,
            "base_weight": [200.0] * n_hh,
        }
    )
    return ProfileFit(
        profile=profile,
        usability_flag_col=f"usable_{profile}" if profile else "survey_complete",
        seed_incidence=seed,
        pre_imputation_incidence=seed,
        imputation_summary=[ImputationSummary("h_size", "household", n_hh, 1, 0.4, 0.8)],
        coverage=GeographyCoverage(profile=profile, n_universe=n_hh, n_placed=n_hh),
        weights=pl.DataFrame({"hh_id": hh_ids, "hh_weight": [250.0] * n_hh}),
        statuses=[
            ZoneStatus(geo_id=ZONE, converged=True, iterations=7, delta=1e-9, max_gamma_diff=1e-9)
        ],
    )


def _tables(profiles: list[str | None]) -> dict[str, pl.DataFrame]:
    """Propagated canonical tables carrying one weight column per profile."""
    households = pl.DataFrame({"hh_id": [1, 2, 3, 4]})
    persons = pl.DataFrame({"person_id": [1, 2, 3, 4], "hh_id": [1, 1, 2, 3]})
    for profile in profiles:
        suffix = f"_{profile}" if profile else ""
        flag = f"usable_{profile}" if profile else "survey_complete"
        households = households.with_columns(
            pl.Series(flag, [True, True, True, False]),
            pl.Series(f"hh_weight{suffix}", [250.0, 250.0, 250.0, 0.0]),
        )
        persons = persons.with_columns(
            pl.Series(flag, [True, True, True, True]),
            pl.Series(f"person_weight{suffix}", [250.0, 250.0, 250.0, 250.0]),
        )
    return {"households": households, "persons": persons}


def _headings(html: str) -> list[tuple[int, str]]:
    """Every numbered section heading in document order, as (number, title)."""
    return [
        (int(number), title.strip())
        for number, title in re.findall(r"<h2>(\d+) &mdash; ([^<]*)", html)
    ]


def _numbered_headings(html: str) -> list[int]:
    """Distinct section numbers, in the order they first appear.

    The per-profile sections repeat once per pane, so a run's numbering is the
    de-duplicated sequence rather than every heading in the file.
    """
    seen: list[int] = []
    for number, _ in _headings(html):
        if number not in seen:
            seen.append(number)
    return seen


def _section(html: str, section_id: str) -> str:
    """One section's markup, from its opening tag to its close."""
    start = html.index(f'<section id="{section_id}"')
    return html[start : html.index("</section>", start)]


def _panes(html: str) -> list[str]:
    """The per-profile panes, as raw HTML fragments."""
    return html.split('class="profile-pane"')[1:]


class TestGenerateReport:
    """The document a run writes, however many profiles it fitted."""

    def test_single_unprofiled_fit_renders(self, tmp_path):
        """A run weighting the whole survey collapses the toggle to one pane."""
        out = generate_report(
            fits={None: _fit(None)},
            control_totals=_control_totals(),
            target_names=TARGETS,
            tables=_tables([None]),
            output_path=tmp_path / "diagnostics.html",
        )
        html = out.read_text(encoding="utf-8")
        assert html.count('class="profile-pane"') == 1
        assert 'data-profile="survey"' in html

    def test_one_pane_per_fit(self, tmp_path):
        """Each profile gets a pane and a button; run-level sections appear once."""
        fits = {p: _fit(p) for p in ("ctramp", "daysim", "analysis")}
        out = generate_report(
            fits=fits,
            control_totals=_control_totals(),
            target_names=TARGETS,
            tables=_tables(list(fits)),
            output_path=tmp_path / "diagnostics.html",
        )
        html = out.read_text(encoding="utf-8")

        assert html.count('class="profile-pane"') == 3
        for profile in fits:
            assert f'data-profile="{profile}"' in html

        # The run-level sections are written once, not once per fit.
        titles = [title for _, title in _headings(html)]
        assert titles.count("Profile Comparison") == 1
        assert titles.count("Weight Cascade") == 1
        # No geometry was passed, so the crosswalk is left out entirely rather
        # than rendered as a heading, a sidebar entry and an empty map.
        assert "Crosswalk Map" not in html

    def test_each_pane_names_its_profile(self, tmp_path):
        """A cropped screenshot of any per-profile section still says what it shows."""
        out = generate_report(
            fits={"ctramp": _fit("ctramp")},
            control_totals=_control_totals(),
            target_names=TARGETS,
            tables=_tables(["ctramp"]),
            output_path=tmp_path / "diagnostics.html",
        )
        html = out.read_text(encoding="utf-8")
        assert html.count('<span class="profile-name">ctramp</span>') >= 3

    def test_cascade_reports_every_profile(self, tmp_path):
        """The cascade is run-level: all profiles in one table, not behind the toggle."""
        fits = {p: _fit(p) for p in ("ctramp", "analysis")}
        out = generate_report(
            fits=fits,
            control_totals=_control_totals(),
            target_names=TARGETS,
            tables=_tables(list(fits)),
            output_path=tmp_path / "diagnostics.html",
        )
        html = out.read_text(encoding="utf-8")
        cascade = _section(html, "sec-cascade")
        assert 'colspan="3">ctramp' in cascade
        assert 'colspan="3">analysis' in cascade

    def test_run_meta_is_rendered(self, tmp_path):
        """A report shared detached from its run still identifies the run."""
        out = generate_report(
            fits={"ctramp": _fit("ctramp")},
            control_totals=_control_totals(),
            target_names=TARGETS,
            tables=_tables(["ctramp"]),
            output_path=tmp_path / "diagnostics.html",
            run_meta={"PUMS": "2023 &middot; FIPS 06"},
        )
        html = out.read_text(encoding="utf-8")
        assert "2023 &middot; FIPS 06" in html

    def test_omitted_imputation_leaves_no_gap(self, tmp_path):
        """A run that imputed nothing skips the section without skipping a number.

        Regression: the heading numbers were literals, so a run with nothing to
        impute rendered 1, 2, 3, 5 -- the section vanished and took its number
        with it.
        """
        fit = _fit("ctramp")
        fit.imputation_summary = []
        out = generate_report(
            fits={"ctramp": fit},
            control_totals=_control_totals(),
            target_names=TARGETS,
            tables=_tables(["ctramp"]),
            output_path=tmp_path / "diagnostics.html",
        )
        html = out.read_text(encoding="utf-8")
        assert "Fractional Seed Imputation" not in html
        numbers = _numbered_headings(html)
        assert numbers == list(range(1, len(numbers) + 1))

    def test_no_fits_raises(self, tmp_path):
        """An empty run has nothing to describe; say so rather than write a shell."""
        with pytest.raises(ValueError, match="at least one completed fit"):
            generate_report(
                fits={},
                control_totals=_control_totals(),
                target_names=TARGETS,
                tables={},
                output_path=tmp_path / "diagnostics.html",
            )


class TestSectionNumbering:
    """Heading numbers as addresses: consecutive, and fixed under the toggle."""

    def test_numbers_are_consecutive_from_one(self, tmp_path):
        """No gaps and no repeats, whatever the run happened to produce."""
        out = generate_report(
            fits={"ctramp": _fit("ctramp")},
            control_totals=_control_totals(),
            target_names=TARGETS,
            tables=_tables(["ctramp"]),
            output_path=tmp_path / "diagnostics.html",
        )
        numbers = _numbered_headings(out.read_text(encoding="utf-8"))
        assert numbers == list(range(1, len(numbers) + 1))

    def test_panes_share_one_numbering(self, tmp_path):
        """A section number is an address, so it cannot move under the toggle."""
        fits = {p: _fit(p) for p in ("ctramp", "daysim", "analysis")}
        out = generate_report(
            fits=fits,
            control_totals=_control_totals(),
            target_names=TARGETS,
            tables=_tables(list(fits)),
            output_path=tmp_path / "diagnostics.html",
        )
        panes = _panes(out.read_text(encoding="utf-8"))
        assert len(panes) == 3
        assert len({tuple(_headings(pane)) for pane in panes}) == 1

    def test_pane_missing_a_section_keeps_the_others_in_place(self, tmp_path):
        """One profile with nothing to impute must not renumber the other's pane.

        The number is assigned once for the document, so the profile that skips
        the section leaves a gap in its own pane rather than shifting every
        heading below it.
        """
        fits = {p: _fit(p) for p in ("ctramp", "analysis")}
        fits["analysis"].imputation_summary = []
        out = generate_report(
            fits=fits,
            control_totals=_control_totals(),
            target_names=TARGETS,
            tables=_tables(list(fits)),
            output_path=tmp_path / "diagnostics.html",
        )
        ctramp, analysis = (
            {title: number for number, title in _headings(pane)}
            for pane in _panes(out.read_text(encoding="utf-8"))
        )

        # Only ctramp renders the imputation section, and it keeps its number.
        imputed = next(t for t in ctramp if t.startswith("Fractional"))
        assert imputed not in analysis
        assert ctramp[imputed] not in analysis.values()

        # Every section both panes do render carries the same number in each.
        for title in ctramp.keys() & analysis.keys():
            assert ctramp[title] == analysis[title]

    def test_absent_crosswalk_is_not_numbered(self, tmp_path):
        """A section with nothing to show gets no heading and no number."""
        out = generate_report(
            fits={"ctramp": _fit("ctramp")},
            control_totals=_control_totals(),
            target_names=TARGETS,
            tables=_tables(["ctramp"]),
            output_path=tmp_path / "diagnostics.html",
        )
        html = out.read_text(encoding="utf-8")
        titles = [title for _, title in _headings(html)]
        assert not any(t.startswith("Crosswalk") for t in titles)
        assert titles[:2] == ["Profile Comparison", "Weight Cascade"]


class TestComparerSection:
    """The pairwise weight-set comparer, which is run-level or absent."""

    def _comparison(self, names):
        """A comparison over the household weights the fixture tables carry."""
        sets = [
            WeightSet(name=n, label=n, columns={"households": f"hh_weight_{n}"}, fitted=True)
            for n in names
        ]
        weights = np.exp(np.linspace(np.log(10), np.log(1000), 200))
        table = pl.DataFrame({f"hh_weight_{n}": weights * (i + 1) for i, n in enumerate(names)})
        return Comparison(sets=sets, pairs=all_pairs({"households": table}, sets))

    def _render(self, tmp_path, comparison):
        """Render a one-profile report carrying *comparison*."""
        out = generate_report(
            fits={"ctramp": _fit("ctramp")},
            control_totals=_control_totals(),
            target_names=TARGETS,
            tables=_tables(["ctramp"]),
            output_path=tmp_path / "diagnostics.html",
            comparison=comparison,
        )
        return out.read_text(encoding="utf-8")

    def test_absent_when_nothing_to_compare(self, tmp_path):
        """One weight set has no pair, so the section is left out entirely."""
        html = self._render(tmp_path, None)

        assert "Weight Set Comparer" not in html
        titles = [title for _, title in _headings(html)]
        assert not any(t.startswith("Weight Set Comparer") for t in titles)

    def test_sits_outside_the_profile_panes(self, tmp_path):
        """A comparison names two profiles, so it cannot live in a pane showing one."""
        html = self._render(tmp_path, self._comparison(["ctramp", "vendor"]))

        assert html.index('id="sec-comparer"') < html.index('class="profile-pane"')
        assert [t for _, t in _headings(html)].count("Weight Set Comparer") == 1

    def test_numbering_stays_consecutive_with_the_section(self, tmp_path):
        """Inserting section 4 must not leave a gap anywhere below it."""
        html = self._render(tmp_path, self._comparison(["ctramp", "vendor"]))
        numbers = _numbered_headings(html)

        assert numbers == list(range(1, len(numbers) + 1))

        # Run-level, so it precedes every per-profile section.
        comparer = next(n for n, t in _headings(html) if t.startswith("Weight Set Comparer"))
        balancer = next(n for n, t in _headings(html) if t.startswith("Balancer Performance"))
        assert comparer < balancer

    def test_every_pair_reaches_the_picker(self, tmp_path):
        """Three sets make three pairs; the picker is the only index of them."""
        html = self._render(tmp_path, self._comparison(["ctramp", "daysim", "vendor"]))
        data = json.loads(html.split("var DATA = ")[1].split(";\n")[0])

        assert len(data["pairs"]) == 3
        assert 'class="pair-row' not in html

    def test_tiles_are_the_five_agreed_summaries(self, tmp_path):
        """Median, p90 and max difference, rank correlation and R-squared."""
        html = self._render(tmp_path, self._comparison(["ctramp", "vendor"]))
        comparer = _section(html, "sec-comparer")

        for tile in ("t-med", "t-p90", "t-max", "t-rho", "t-r2"):
            assert f'id="{tile}"' in comparer
        assert comparer.count('class="tile"') == 5

    def test_inheritance_is_reported_with_the_cascade(self, tmp_path):
        """How a set's weight descends describes the set, not a pair of them."""
        html = self._render(tmp_path, self._comparison(["ctramp", "vendor"]))

        assert "How each level" not in _section(html, "sec-comparer")

    def test_the_caveat_is_above_the_plot(self, tmp_path):
        """A scatter of two weight sets invites the wrong reading; say so first."""
        html = self._render(tmp_path, self._comparison(["ctramp", "vendor"]))

        assert html.index("not a validation") < html.index('id="cmp-scatter"')

    def test_pairs_are_embedded_as_data(self, tmp_path):
        """One payload drawn in the browser, not one baked figure per pair."""
        html = self._render(tmp_path, self._comparison(["ctramp", "vendor"]))
        blob = html.split("var DATA = ")[1].split(";\n")[0]
        data = json.loads(blob)

        assert [s["name"] for s in data["sets"]] == ["ctramp", "vendor"]
        assert data["levels"] == [{"name": "households", "label": "Households"}]
        assert len(data["pairs"]) == 1
        pair = data["pairs"][0]
        assert pair["n"] == 200
        assert {"med", "p90", "max", "rho", "r2", "slope", "icept", "decAB", "decBA"} <= set(pair)


class TestNavigationAndDefinitions:
    """The sidebar outline and the Definitions section every header links into."""

    def _render(self, tmp_path, profiles=("ctramp", "analysis")):
        out = generate_report(
            fits={p: _fit(p) for p in profiles},
            control_totals=_control_totals(),
            target_names=TARGETS,
            tables=_tables(list(profiles)),
            output_path=tmp_path / "diagnostics.html",
        )
        return out.read_text(encoding="utf-8")

    def test_every_definition_link_has_a_target(self, tmp_path):
        """A header that links to a definition the page does not carry is a dead end."""
        html = self._render(tmp_path)
        links = set(re.findall(r'href="#(g-[\w]+)"', html))
        targets = set(re.findall(r'id="(g-[\w]+)"', html))

        assert links
        assert links <= targets

    def test_statistics_are_defined_before_they_are_used(self, tmp_path):
        """ESS is defined above the first table that reports it."""
        html = self._render(tmp_path)
        main = html.index("<main>")

        assert html.index('id="g-ess"') < html.index('href="#g-ess"', main)

    def test_definitions_list_only_what_the_page_uses(self, tmp_path):
        """With nothing to compare, the comparer's statistics are not listed."""
        html = self._render(tmp_path)

        assert 'id="g-ess"' in html
        assert 'id="g-rho"' not in html

    def test_sidebar_lists_every_numbered_section(self, tmp_path):
        """One entry per rendered section, carrying the same number as the heading."""
        html = self._render(tmp_path)
        nav = html[html.index('<nav class="side"') : html.index("</nav>")]
        entries = re.findall(r'data-sec="(\w+)"><span class="n">(\d+)</span>', nav)

        assert [int(n) for _, n in entries] == _numbered_headings(html)

    def test_section_ids_are_unique(self, tmp_path):
        """Each pane repeats its sections, so their ids carry the profile."""
        html = self._render(tmp_path)
        ids = re.findall(r'<section id="([^"]+)"', html)

        assert len(ids) == len(set(ids))
        assert "sec-balancer-ctramp" in ids
        assert "sec-balancer-analysis" in ids

    def test_one_profile_needs_no_toggle(self, tmp_path):
        """A single pane has nothing to switch between."""
        html = self._render(tmp_path, profiles=("ctramp",))

        assert 'class="toggle"' not in html

    def test_several_profiles_share_one_toggle(self, tmp_path):
        """The toggle sits in the sidebar, once, with a button per profile."""
        html = self._render(tmp_path)
        nav = html[html.index('<nav class="side"') : html.index("</nav>")]

        assert html.count('class="toggle"') == 1
        assert 'data-profile="ctramp"' in nav
        assert 'data-profile="analysis"' in nav

    def test_explanations_are_collapsed_not_removed(self, tmp_path):
        """The longer guidance is still on the page, behind a summary line."""
        html = self._render(tmp_path)

        assert html.count('<details class="more">') >= 5
        assert '<details class="more" open' not in html

    def test_definition_groups_start_collapsed(self, tmp_path):
        """Each group of definitions is folded, so the section is a short list at first."""
        html = self._render(tmp_path)
        definitions = _section(html, "definitions")

        assert definitions.count('<details class="defs">') >= 2
        assert '<details class="defs" open' not in definitions
        assert 'id="g-ess"' in definitions
