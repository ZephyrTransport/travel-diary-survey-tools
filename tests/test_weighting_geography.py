"""Putting the survey and the PUMS on the same map, on synthetic geometries.

The crosswalk, households the control geography cannot place, and the Census
API failing mid-run.
"""

from typing import ClassVar

import geopandas as gpd
import numpy as np
import polars as pl
import pytest
import requests
from rasterio.transform import from_bounds
from shapely.geometry import box

from processing.weighting.data_prep import census_geo
from processing.weighting.data_prep.census_geo import puma_vintage_for_pums_year
from processing.weighting.data_prep.control_data import (
    recode_pums_households,
    recode_pums_persons,
)
from processing.weighting.data_prep.crosswalk import (
    GeographyConfig,
    PumaCrosswalk,
    TargetZoneConfig,
    _load_target_zones,
)
from processing.weighting.data_prep.incidence import aggregate_control_totals, build_incidence_table
from processing.weighting.diagnostics.crosswalk_map import crosswalk_figure
from processing.weighting.validation.coverage import check_control_geography_coverage
from utils.crosswalk import (
    _cross_tabulate,
    _rasterize_categorical,
    _rasterize_weights,
    build_crosswalk,
)


@pytest.fixture
def two_pumas() -> gpd.GeoDataFrame:
    """Two adjacent rectangular PUMAs covering a 2000m x 1000m region.

    PUMA "A" covers x=[0, 1000], y=[0, 1000]
    PUMA "B" covers x=[1000, 2000], y=[0, 1000]
    """
    return gpd.GeoDataFrame(
        {"puma_id": ["A", "B"]},
        geometry=[box(0, 0, 1000, 1000), box(1000, 0, 2000, 1000)],
        crs="EPSG:5070",
    )


@pytest.fixture
def three_target_zones() -> gpd.GeoDataFrame:
    """Three target zones that cross PUMA boundaries.

    Zone "1": x=[0, 700], y=[0, 1000]     — fully inside PUMA A
    Zone "2": x=[700, 1300], y=[0, 1000]   — straddles PUMAs A and B
    Zone "3": x=[1300, 2000], y=[0, 1000]  — fully inside PUMA B
    """
    return gpd.GeoDataFrame(
        {"target_id": ["1", "2", "3"]},
        geometry=[box(0, 0, 700, 1000), box(700, 0, 1300, 1000), box(1300, 0, 2000, 1000)],
        crs="EPSG:5070",
    )


@pytest.fixture
def uniform_blocks() -> gpd.GeoDataFrame:
    """10 uniform blocks tiling the 2000x1000 region, each 200x1000m with pop=100.

    Total population: 1000.
    """
    blocks = []
    for i in range(10):
        x0 = i * 200
        blocks.append(
            {
                "block_id": f"B{i:02d}",
                "pop20": 100,
                "geometry": box(x0, 0, x0 + 200, 1000),
            }
        )
    return gpd.GeoDataFrame(blocks, crs="EPSG:5070")


@pytest.fixture
def target_zone_file(three_target_zones, tmp_path) -> str:
    """Write target zones to a temporary shapefile."""
    path = tmp_path / "targets.shp"
    three_target_zones.to_file(path)
    return str(path)


def _serve_census(monkeypatch, blocks: dict[str, int], counties=("001",)) -> list[dict]:
    """Answer Census API requests from *blocks*, recording each request's params."""
    asked: list[dict] = []

    def fake_get(_url, params, timeout):  # noqa: ARG001
        asked.append(params)
        if params["get"] == "NAME":
            return [["NAME", "state", "county"]] + [
                [f"County {c}, California", "06", c] for c in counties
            ]
        header = [params["get"], "state", "county", "tract", "block"]
        return [header] + [[str(n), g[:2], g[2:5], g[5:11], g[11:]] for g, n in blocks.items()]

    monkeypatch.setattr(census_geo, "_census_get", fake_get)
    return asked


class TestBlockHouseholds:
    """Sample-plan base weights count households, block by block."""

    BLOCKS: ClassVar[dict[str, int]] = {
        "060010001001001": 7,
        "060010001001000": 3,
        "060010001002000": 5,
    }

    def test_one_row_per_block(self, monkeypatch, tmp_path):
        """Each block keeps its own count, sorted by GEOID."""
        _serve_census(monkeypatch, self.BLOCKS)

        result = census_geo.get_block_households("06", 2023, tmp_path)

        assert result.rows() == [
            ("060010001001000", 3),
            ("060010001001001", 7),
            ("060010001002000", 5),
        ]

    @pytest.mark.parametrize(
        ("pums_year", "variable", "file"),
        [
            (2023, "H1_002N", "census_2020/block_households_06.parquet"),
            (2019, "H003002", "census_2010/block_households_06.parquet"),
        ],
    )
    def test_asks_for_occupied_units_and_saves_them_by_vintage(
        self, monkeypatch, tmp_path, pums_year, variable, file
    ):
        """Each vintage names households in its own Census variable and folder."""
        asked = _serve_census(monkeypatch, self.BLOCKS)

        census_geo.get_block_households("06", pums_year, tmp_path)

        assert [p["get"] for p in asked if p["get"] != "NAME"] == [variable]
        assert (tmp_path / file).exists()

    def test_2010_is_fetched_one_county_at_a_time(self, monkeypatch, tmp_path):
        """The 2010 endpoint has no statewide block query."""
        asked = _serve_census(monkeypatch, self.BLOCKS, counties=("001", "075"))

        census_geo.get_block_households("06", 2019, tmp_path)

        assert sorted(p["in"] for p in asked if p["get"] != "NAME") == [
            "state:06 county:001",
            "state:06 county:075",
        ]

    def test_a_saved_file_is_read_not_fetched(self, monkeypatch, tmp_path):
        """Once downloaded, later runs read the file and ask the API nothing."""
        asked = _serve_census(monkeypatch, self.BLOCKS)
        first = census_geo.get_block_households("06", 2023, tmp_path)
        n_asked = len(asked)

        second = census_geo.get_block_households("06", 2023, tmp_path)

        assert first.equals(second)
        assert len(asked) == n_asked

    def test_an_empty_response_stops(self, monkeypatch, tmp_path):
        """No counts means no base weights, so the run should not go on."""
        _serve_census(monkeypatch, {})

        with pytest.raises(ValueError, match="returned no H1_002N blocks"):
            census_geo.get_block_households("06", 2023, tmp_path)
        assert not list(tmp_path.rglob("*.parquet*"))


class TestBlockPopulation:
    """Every block gets its own population, or the files do not belong together."""

    def _write(self, folder, blocks, population):
        folder.mkdir(parents=True)
        gpd.GeoDataFrame(
            {"GEOID20": blocks}, geometry=[box(0, 0, 1, 1)] * len(blocks), crs="EPSG:4326"
        ).to_parquet(folder / "tl_2020_06_tabblock20.parquet")
        pl.DataFrame(
            {"block_id": list(population), "block_pop": list(population.values())}
        ).write_parquet(folder / "block_population_06.parquet")

    def test_population_is_joined_by_block(self, tmp_path):
        """Each block carries its own count."""
        self._write(
            tmp_path / "census_2020",
            ["060010001001000", "060010001001001"],
            {"060010001001001": 4, "060010001001000": 9},
        )

        gdf = census_geo.get_block_gdf("06", 2023, tmp_path)

        assert dict(zip(gdf["block_id"], gdf["block_pop"], strict=True)) == {
            "060010001001000": 9,
            "060010001001001": 4,
        }

    def test_a_block_without_population_stops(self, tmp_path):
        """A missing count is not a zero: the files are mismatched or damaged."""
        self._write(
            tmp_path / "census_2020", ["060010001001000", "060010001001001"], {"060010001001000": 9}
        )

        with pytest.raises(ValueError, match="1 of 2 blocks"):
            census_geo.get_block_gdf("06", 2023, tmp_path)


class TestCountyNames:
    """County names come from the Census, without the state the API appends."""

    def test_names_drop_the_state(self, monkeypatch, tmp_path):
        """'Alameda County, California' is shown as 'Alameda County'."""
        payload = [
            ["NAME", "state", "county"],
            ["Alameda County, California", "06", "001"],
            ["San Francisco County, California", "06", "075"],
        ]
        monkeypatch.setattr(census_geo, "_census_get", lambda *_a, **_k: payload)

        result = census_geo.get_county_names("06", 2023, tmp_path)

        assert result.rows() == [("06001", "Alameda County"), ("06075", "San Francisco County")]
        assert (tmp_path / "census_2020" / "county_names_06.parquet").exists()


class TestPumaVintage:
    """Verify correct PUMA vintage is returned for a given PUMS year."""

    @pytest.mark.parametrize(
        ("pums_year", "vintage"),
        [(2023, 2020), (2022, 2020), (2021, 2010), (2012, 2010)],
    )
    def test_vintage_for_year(self, pums_year, vintage):
        """2022 is the first PUMS year on the 2020 PUMA boundaries."""
        assert puma_vintage_for_pums_year(pums_year) == vintage

    def test_a_year_before_the_2010_pumas_raises(self):
        """There is no boundary set to place a 2011 extract on."""
        with pytest.raises(ValueError, match="before 2012"):
            puma_vintage_for_pums_year(2011)


class TestLoadTargetZones:
    """Tests for _load_target_zones helper function."""

    def test_with_id_field(self, three_target_zones, target_zone_file):
        """Zones load the same from a GeoDataFrame and from a file path."""
        for source in (three_target_zones, target_zone_file):
            gdf = _load_target_zones(source, "target_id")
            assert "target_zone_id" in gdf.columns
            assert len(gdf) == 3

    def test_single_boundary_mode(self, three_target_zones):
        """When id_field is None, should dissolve to a single geometry with target_zone_id=1."""
        gdf = _load_target_zones(three_target_zones, None)
        assert len(gdf) == 1
        assert gdf["target_zone_id"].iloc[0] == "1"

    def test_missing_id_field_raises(self, three_target_zones):
        """Test that a missing ID field raises a ValueError."""
        with pytest.raises(ValueError, match="not found"):
            _load_target_zones(three_target_zones, "nonexistent_col")


def _grid(bounds, resolution):
    """The transform and shape of a grid over *bounds* at *resolution*."""
    minx, miny, maxx, maxy = bounds
    w = int(np.ceil((maxx - minx) / resolution))
    h = int(np.ceil((maxy - miny) / resolution))
    transform = from_bounds(minx, miny, minx + w * resolution, miny + h * resolution, w, h)
    return transform, (h, w)


def test_resolution_must_be_positive_and_is_kept_as_given():
    """A negative resolution is refused; a positive one is not rounded or clamped."""

    def config(resolution: int) -> GeographyConfig:
        return GeographyConfig(
            target_zones=TargetZoneConfig(file="zones.shp", id_field=None),
            resolution=resolution,
            min_allocation=0.0,
            zone_groups=None,
        )

    with pytest.raises(ValueError, match="positive"):
        config(-100)
    assert config(250).resolution == 250


class TestAssignHouseholds:
    """Test PumaCrosswalk.assign_households via a minimal instance."""

    @staticmethod
    def _make_xw(target_gdf: gpd.GeoDataFrame, puma_gdf: gpd.GeoDataFrame) -> PumaCrosswalk:
        """Build a bare PumaCrosswalk with only the zone and PUMA polygons set."""
        obj = object.__new__(PumaCrosswalk)
        obj.target_gdf = _load_target_zones(target_gdf, "target_id")
        obj.puma_gdf = puma_gdf
        obj.zone_groups = {}
        obj._zone_remap = {}
        return obj

    def test_assigns_home_puma(self, three_target_zones, two_pumas):
        """Zone 2 straddles PUMAs A and B; each of its homes gets the one PUMA it is in."""
        hh = pl.DataFrame(
            {
                "hh_id": [1, 2, 3, 4, 5],
                "home_lon": [350.0, 900.0, 1100.0, 1650.0, -999.0],
                "home_lat": [500.0] * 5,
                "extra_col": ["keep_me"] * 5,
            }
        )
        target = three_target_zones.copy().set_crs("EPSG:4326", allow_override=True)
        pumas = two_pumas.copy().set_crs("EPSG:4326", allow_override=True)

        result = self._make_xw(target, pumas).assign_households(hh).sort("hh_id")
        assert result["zone_group_id"].to_list() == ["1", "2", "2", "3", None]
        assert result["home_puma"].to_list() == ["A", "A", "B", "B", None]
        assert result["extra_col"].to_list() == ["keep_me"] * 5


class TestAssignBlocks:
    """PumaCrosswalk.assign_blocks gives each Census block one PUMA and at most one zone."""

    @staticmethod
    def _make_xw(zone_groups: dict[str, list[str]] | None = None) -> PumaCrosswalk:
        """PUMAs A (x 0-1000) and B (1000-2000); zones 1 (0-600) and 2 (600-1800)."""
        zones = gpd.GeoDataFrame(
            {"target_id": ["1", "2"]},
            geometry=[box(0, 0, 600, 1000), box(600, 0, 1800, 1000)],
            crs="EPSG:5070",
        )
        obj = object.__new__(PumaCrosswalk)
        obj.target_gdf = _load_target_zones(zones, "target_id")
        obj.puma_gdf = gpd.GeoDataFrame(
            {"puma_id": ["A", "B"]},
            geometry=[box(0, 0, 1000, 1000), box(1000, 0, 2000, 1000)],
            crs="EPSG:5070",
        )
        obj.zone_groups = zone_groups or {}
        obj._zone_remap = {z: g for g, zs in obj.zone_groups.items() for z in zs}
        return obj

    @staticmethod
    def _blocks() -> gpd.GeoDataFrame:
        """Ten 200 m blocks from x = 0 to 2000, after one far outside both PUMAs.

        The outside block comes first, so dropping it shifts every row after it:
        a block's ID must stay with its own geometry.
        """
        return gpd.GeoDataFrame(
            {"block_id": ["OUT", *(f"B{i:02d}" for i in range(10))]},
            geometry=[
                box(5000, 0, 5200, 1000),
                *(box(i * 200, 0, i * 200 + 200, 1000) for i in range(10)),
            ],
            crs="EPSG:5070",
        )

    def test_each_block_gets_its_puma_and_zone(self):
        """A block outside every zone keeps its PUMA, so the PUMA's total counts it."""
        result = self._make_xw().assign_blocks(self._blocks()).sort("block_id")
        assert result["block_id"].to_list() == [f"B{i:02d}" for i in range(10)]
        assert result["puma_id"].to_list() == ["A"] * 5 + ["B"] * 5
        assert result["zone_group_id"].to_list() == ["1"] * 3 + ["2"] * 6 + [None]

    def test_zone_groups_name_the_balancing_zone(self):
        """Blocks take the group their zone balances in."""
        xw = self._make_xw({"G": ["1", "2"]})
        result = xw.assign_blocks(self._blocks()).sort("block_id")
        assert result["zone_group_id"].to_list() == ["G"] * 9 + [None]

    def test_zones_are_named_by_their_census_counties(self):
        """A zone in one county takes its name; one across counties lists each share."""
        xw = self._make_xw()
        # blocks 0-4 sit in county 001, blocks 5-9 in county 013, 100 people each
        county = ["06001"] * 5 + ["06013"] * 5
        xw.block_gdf = gpd.GeoDataFrame(
            {
                "block_id": [f"{c}{i:010d}" for i, c in enumerate(county)],
                "block_pop": [100] * 10,
            },
            geometry=[box(i * 200, 0, i * 200 + 200, 1000) for i in range(10)],
            crs="EPSG:5070",
        )
        names = pl.DataFrame(
            {
                "county_fips": ["06001", "06013"],
                "county_name": ["Alameda County", "Contra Costa County"],
            }
        )
        assert xw.zone_counties(names) == {
            "1": "Alameda County",
            "2": "Contra Costa County 67%, Alameda County 33%",
        }

    def test_a_county_without_a_census_name_stops(self):
        """A county code the Census does not name is not guessed at."""
        xw = self._make_xw()
        xw.block_gdf = gpd.GeoDataFrame(
            {"block_id": ["060010000000001"], "block_pop": [100]},
            geometry=[box(0, 0, 200, 1000)],
            crs="EPSG:5070",
        )
        empty = pl.DataFrame(schema={"county_fips": pl.Utf8, "county_name": pl.Utf8})
        with pytest.raises(ValueError, match="no name for counties"):
            xw.zone_counties(empty)


class TestAssignBlockGroups:
    """PumaCrosswalk.assign_block_groups matches homes to blocks' block groups."""

    @staticmethod
    def _make_xw() -> PumaCrosswalk:
        """Three unit blocks in a row: two in block group ...0001, one in ...0002."""
        obj = object.__new__(PumaCrosswalk)
        obj.block_gdf = gpd.GeoDataFrame(
            {"block_id": ["060010001001000", "060010001001001", "060010001002000"]},
            geometry=[box(0, 0, 1, 1), box(1, 0, 2, 1), box(2, 0, 3, 1)],
            crs="EPSG:4326",
        )
        return obj

    def test_block_groups(self):
        """Inside a block, on an edge within one block group, on an edge between two, outside."""
        hh = pl.DataFrame(
            {
                "hh_id": [1, 2, 3, 4, 5],
                "home_lon": [0.5, 1.0, 2.0, 2.5, 9.0],
                "home_lat": [0.5, 0.5, 0.5, 0.5, 0.5],
            }
        )
        result = self._make_xw().assign_block_groups(hh).sort("hh_id")
        assert result["bg_geo_id"].to_list() == [
            "060010001001",
            "060010001001",  # between blocks of the same block group: lies within it
            None,  # between block groups: within neither
            "060010001002",
            None,
        ]


class TestCrossTabulation:
    """Integration tests: exactextract cross-tabulation with coverage fractions."""

    def test_cross_tabulate_runs_on_real_geometry(
        self, two_pumas, three_target_zones, uniform_blocks
    ):
        """Rasterising and cross-tabulating runs end to end and conserves the population.

        The exact arithmetic is asserted in ``TestCrossTabMath`` on a grid built for it.
        """
        transform, shape = _grid((0, 0, 2000, 1000), resolution=50)
        pop_arr = _rasterize_weights(uniform_blocks, "pop20", transform, shape)
        puma_arr, int_to_puma = _rasterize_categorical(two_pumas, "puma_id", transform, shape)
        assert len(set(puma_arr[puma_arr > 0].ravel())) == 2
        assert set(int_to_puma.values()) == {"A", "B"}

        result = _cross_tabulate(pop_arr, puma_arr, int_to_puma, transform, three_target_zones)

        assert set(result.columns) == {"source_id", "target_id", "population"}
        assert result["population"].sum() == pytest.approx(1000.0, abs=50)


class TestCrossTabMath:
    """Exact PUMA x target-zone populations with sub-pixel coverage.

    A 4x4 grid of 100 m cells; PUMA A is x < 200 and B the rest. Zones L, M
    and R split at x = 150 and 250, so the boundary cells are half covered::

        ┌───────┬───────┬───────┬───────┐
     y3 │ 10/A  │ 10/A  │ 20/B  │ 20/B  │
        ├───────┼───────┼───────┼───────┤
     y2 │ 10/A  │ 10/A  │ 20/B  │ 20/B  │
        ├───────┼───────┼───────┼───────┤
     y1 │ 30/A  │ 30/A  │ 40/B  │ 40/B  │
        ├───────┼───────┼───────┼───────┤
     y0 │ 30/A  │ 30/A  │ 40/B  │ 40/B  │
        └───────┴───────┴───────┴───────┘
         x0      x1      x2      x3

    Each A column holds 80 and each B column 120. L gets 80 + 40 from A; M
    gets 40 from A and 60 from B; R gets 60 + 120 from B.
    """

    def test_population_by_puma_and_zone(self):
        """A to L 120, A to M 40, B to M 60, B to R 180."""
        pop = np.array(
            [[10, 10, 20, 20], [10, 10, 20, 20], [30, 30, 40, 40], [30, 30, 40, 40]],
            dtype=np.float32,
        )
        puma = np.array([[1, 1, 2, 2]] * 4, dtype=np.int32)
        zones = gpd.GeoDataFrame(
            {"target_id": ["L", "M", "R"]},
            geometry=[box(0, 0, 150, 400), box(150, 0, 250, 400), box(250, 0, 400, 400)],
            crs="EPSG:5070",
        )
        result = _cross_tabulate(
            pop, puma, {1: "A", 2: "B"}, from_bounds(0, 0, 400, 400, 4, 4), zones
        )

        def _pop(source_id: str, target_id: str) -> float:
            return result.filter(
                (pl.col("source_id") == source_id) & (pl.col("target_id") == target_id)
            )["population"].sum()

        assert _pop("A", "L") == pytest.approx(120.0, abs=1)
        assert _pop("A", "M") == pytest.approx(40.0, abs=1)
        assert _pop("B", "M") == pytest.approx(60.0, abs=1)
        assert _pop("B", "R") == pytest.approx(180.0, abs=1)


class TestControlDataCrosswalk:
    """Verify PumaCrosswalk.allocate_pums_incidence + aggregate_control_totals."""

    def test_crosswalk_redistributes_controls(self):
        """PUMA A splits 70/30 between zones T1 and T2, households and persons alike."""
        hh = pl.DataFrame(
            {
                "SERIALNO": ["H1", "H2", "H3"],
                "PUMA": ["A", "A", "B"],
                "WGTP": [100.0, 200.0, 150.0],
                "NP": [1, 3, 2],
            }
        )
        per = pl.DataFrame(
            {
                "SERIALNO": ["H1", "H2", "H2", "H2", "H3", "H3"],
                "SPORDER": [1, 1, 2, 3, 1, 2],
                "PUMA": ["A", "A", "A", "A", "B", "B"],
                "PWGTP": [110.0, 190.0, 210.0, 200.0, 150.0, 150.0],
            }
        )
        targets = ["h_total", "p_total", "h_size"]
        bundle = build_incidence_table(
            recode_pums_households(hh, per, targets),
            recode_pums_persons(per, targets),
            targets,
            hh_id_col="SERIALNO",
            extra_cols=["WGTP", "PUMA"],
        )
        persons = bundle.person_pivot.join(
            per.select("SERIALNO", "SPORDER", "PUMA", "PWGTP"), on=["SERIALNO", "SPORDER"]
        )

        xw = object.__new__(PumaCrosswalk)
        xw.crosswalk_df = pl.DataFrame(
            {
                "puma_id": ["A", "A", "B"],
                "target_zone_id": ["T1", "T2", "T2"],
                "zone_group_id": ["T1", "T2", "T2"],
                "allocation_weight": [0.7, 0.3, 1.0],
            }
        )
        result = aggregate_control_totals(
            xw.allocate_pums_incidence(bundle.incidence),
            xw.allocate_pums_incidence(persons, weight_col="PWGTP"),
            targets,
        )

        assert set(result.zone_group_ids) == {"T1", "T2"}
        by_zone = {
            (ctrl, geo): total
            for geo, ctrl, _, total in result.totals.filter(
                pl.col("control_name").is_in(["h_total", "p_total"])
            ).iter_rows()
        }
        # PUMA A: WGTP 100 + 200 = 300; PWGTP 110 + 190 + 210 + 200 = 710
        assert by_zone["h_total", "T1"] == pytest.approx(300 * 0.7)
        assert by_zone["p_total", "T1"] == pytest.approx(710 * 0.7)
        assert by_zone["p_total", "T2"] == pytest.approx(710 * 0.3 + 300)


class TestPlotCrosswalk:
    """Test crosswalk_figure produces valid Plotly HTML."""

    @pytest.fixture
    def crosswalk_data(self, two_pumas, three_target_zones, uniform_blocks):
        """Build crosswalk data without hitting Census APIs."""
        target_gdf = _load_target_zones(three_target_zones, "target_id")
        xw_df = build_crosswalk(
            source_gdf=two_pumas,
            target_gdf=target_gdf,
            weight_gdf=uniform_blocks,
            source_id_col="puma_id",
            target_id_col="target_zone_id",
            weight_col="pop20",
            resolution=50,
        ).rename({"source_id": "puma_id", "target_id": "target_zone_id"})
        return two_pumas, target_gdf, xw_df

    def test_produces_html(self, crosswalk_data):
        """The figure renders, with per-profile seeds and named zone groups on it."""
        puma_gdf, target_gdf, xw_df = crosswalk_data
        hh = pl.DataFrame({"hh_id": [1, 2, 3], "zone_group_id": ["1", "2", "3"]})
        fig = crosswalk_figure(
            puma_gdf=puma_gdf,
            target_gdf=target_gdf,
            crosswalk_df=xw_df,
            seeds={"ctramp": hh},
            zone_groups={"north": ["1", "2"]},
        )
        html = fig.to_html()
        assert "plotly" in html.lower()
        assert "north" in html.lower()

    def test_produces_html_without_zone_groups(self, crosswalk_data):
        """Grouping is optional: ungrouped zones each get their own colour."""
        puma_gdf, target_gdf, xw_df = crosswalk_data
        fig = crosswalk_figure(
            puma_gdf=puma_gdf,
            target_gdf=target_gdf,
            crosswalk_df=xw_df,
        )

        # on the traces, not the HTML: plotly's bundled script contains "north"
        assert len(fig.data) > 0
        assert "north" not in {trace.name for trace in fig.data if trace.name}


# The model zones a profile reads are assigned with a snap tolerance and the
# target zones by strict point-in-polygon, so a usable household can be in no
# zone group, and no fit can weight it.
def _seed(placed: int, unplaceable: int) -> pl.DataFrame:
    """A seed with the given split of placed and unplaceable households."""
    n = placed + unplaceable
    return pl.DataFrame(
        {
            "hh_id": list(range(1, n + 1)),
            "zone_group_id": ["06001"] * placed + [None] * unplaceable,
        }
    )


class TestCounting:
    """The counts are what turn a column of nulls into a statement."""

    @pytest.mark.parametrize(
        ("placed", "unplaceable", "share"),
        [
            pytest.param(10, 0, 0.0, id="none_unplaceable"),
            pytest.param(9, 1, 0.1, id="one_in_ten"),
            pytest.param(1, 3, 0.75, id="most_of_them"),
        ],
    )
    def test_unplaceable_households_are_counted_not_dropped_silently(
        self, placed, unplaceable, share
    ):
        """The count is the report; the caller is what removes them from the seed."""
        coverage = check_control_geography_coverage(
            _seed(placed, unplaceable), profile="analysis", max_unplaceable_share=1.0
        )
        assert coverage.n_universe == placed + unplaceable
        assert coverage.n_placed == placed
        assert coverage.n_unplaceable == unplaceable
        assert coverage.unplaceable_share == pytest.approx(share)
        assert coverage.profile == "analysis"

    def test_an_empty_seed_has_no_share_rather_than_dividing_by_zero(self):
        """A profile that admits nothing is a different complaint, made elsewhere."""
        coverage = check_control_geography_coverage(
            _seed(0, 0), profile="p", max_unplaceable_share=0.0
        )
        assert coverage.unplaceable_share == 0.0

    def test_a_seed_with_no_geography_column_places_nothing(self):
        """Rather than reading as fully placed, which would hide the whole problem."""
        seed = pl.DataFrame({"hh_id": [1, 2]})
        coverage = check_control_geography_coverage(seed, profile="p", max_unplaceable_share=1.0)
        assert coverage.n_placed == 0
        assert coverage.n_unplaceable == 2


class TestTheTolerance:
    """A boundary effect is expected; a geography that does not cover the survey is not."""

    def test_a_share_above_the_tolerance_raises(self):
        """Half the survey outside the region; the message names both layers and the setting."""
        with pytest.raises(ValueError, match="are in no target zone") as excinfo:
            check_control_geography_coverage(_seed(5, 5), profile="p", max_unplaceable_share=0.01)
        message = str(excinfo.value)
        assert "point-in-polygon" in message
        assert "snap tolerance" in message
        assert "max_unplaceable_share" in message

    def test_a_share_at_the_tolerance_is_allowed(self):
        """The tolerance is what is tolerated, not the first value rejected."""
        coverage = check_control_geography_coverage(
            _seed(99, 1), profile="p", max_unplaceable_share=0.01
        )
        assert coverage.n_unplaceable == 1


def _http_error(status: int) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status
    return requests.HTTPError(f"{status} simulated", response=response)


class TestOnlyServerFaultsAreRetried:
    """A failure without a verdict from the server is retried; our own errors are not."""

    def test_connection_and_timeout_failures_are_transient(self):
        """The request never got a verdict, so it is worth asking again."""
        assert census_geo._is_transient(requests.ConnectionError("dropped"))
        assert census_geo._is_transient(requests.Timeout("slow"))

    def test_an_unrelated_error_is_not_retried(self):
        """A bug in our own parsing must surface, not be retried four times."""
        assert not census_geo._is_transient(ValueError("bad payload"))


class TestACensusRequestRetries:
    """A server error is retried, up to a limit; a client error is not."""

    def _serve(self, monkeypatch, outcomes):
        """Each call raises or returns the next outcome; sleeping is skipped."""
        served = iter(outcomes)
        calls = {"n": 0}

        def fake_get(*_args, **_kwargs):
            calls["n"] += 1
            outcome = next(served)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        monkeypatch.setenv("CENSUS_KEY", "k")
        monkeypatch.setattr(census_geo.requests, "get", fake_get)
        monkeypatch.setattr(census_geo, "_census_json", lambda resp: resp)
        monkeypatch.setattr(census_geo.time, "sleep", lambda _s: None)
        return calls

    PARAMS: ClassVar[dict[str, str]] = {"get": "P1_001N", "in": "state:06 county:001"}

    def test_it_recovers_after_a_server_error(self, monkeypatch):
        """A 500, then the answer."""
        calls = self._serve(monkeypatch, [_http_error(500), [["P1_001N"], ["42"]]])

        result = census_geo._census_get("u", self.PARAMS, timeout=1)

        assert result == [["P1_001N"], ["42"]]
        assert calls["n"] == 2

    def test_a_sound_response_is_not_retried(self, monkeypatch):
        """Retrying success would double 58 requests for nothing."""
        calls = self._serve(monkeypatch, [[["P1_001N"], ["42"]]])

        census_geo._census_get("u", self.PARAMS, timeout=1)

        assert calls["n"] == 1

    def test_it_gives_up_rather_than_looping(self, monkeypatch):
        """Persistent failure is systematic; looping only delays reporting it."""
        attempts = census_geo._MAX_CENSUS_ATTEMPTS
        calls = self._serve(monkeypatch, [_http_error(503)] * attempts)

        with pytest.raises(requests.HTTPError):
            census_geo._census_get("u", self.PARAMS, timeout=1)

        assert calls["n"] == attempts

    def test_a_client_error_fails_immediately(self, monkeypatch):
        """No point asking three more times for something we asked for wrongly."""
        calls = self._serve(monkeypatch, [_http_error(404)])

        with pytest.raises(requests.HTTPError):
            census_geo._census_get("u", self.PARAMS, timeout=1)

        assert calls["n"] == 1
