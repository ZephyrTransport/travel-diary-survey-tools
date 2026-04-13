"""Tests for PUMA -> target-zone crosswalk (core/crosswalk.py + census_geo.py).

Uses synthetic geometries -- no Census API or TIGER downloads required.
Tests exercise rasterization, exactextract cross-tabulation with sub-pixel
coverage fractions, and allocation-weight normalisation.
"""

import geopandas as gpd
import numpy as np
import polars as pl
import pytest
from rasterio.transform import from_bounds
from shapely.geometry import box

from processing.weighting.data_prep.census_geo import puma_vintage_for_pums_year
from processing.weighting.data_prep.control_data import (
    build_control_totals,
    recode_pums_households,
    recode_pums_persons,
)
from processing.weighting.data_prep.crosswalk import (
    GeographyConfig,
    PumaCrosswalk,
    TargetZoneConfig,
    _load_target_zones,
)
from processing.weighting.diagnostics.charts import crosswalk_figure
from processing.weighting.specs import ControlSpec, ControlTotals
from utils.crosswalk import (
    _cross_tabulate,
    _rasterize_categorical,
    _rasterize_weights,
    build_crosswalk,
)


# ---------------------------------------------------------------------------
# Fixtures: synthetic geographies
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Tests: census_geo helpers
# ---------------------------------------------------------------------------
class TestPumaVintage:
    """Verify correct PUMA vintage is returned for a given PUMS year."""

    def test_2023_returns_2020(self):  # noqa: D102
        assert puma_vintage_for_pums_year(2023) == 2020

    def test_2022_returns_2020(self):  # noqa: D102
        assert puma_vintage_for_pums_year(2022) == 2020

    def test_2021_returns_2010(self):  # noqa: D102
        assert puma_vintage_for_pums_year(2021) == 2010

    def test_2012_returns_2010(self):  # noqa: D102
        assert puma_vintage_for_pums_year(2012) == 2010

    def test_2011_raises(self):  # noqa: D102
        with pytest.raises(ValueError, match="before 2012"):
            puma_vintage_for_pums_year(2011)


# ---------------------------------------------------------------------------
# Tests: target zone loading
# ---------------------------------------------------------------------------
class TestLoadTargetZones:
    """Tests for _load_target_zones helper function."""

    def test_with_id_field(self, three_target_zones):
        """Verify that target zones are loaded with the correct ID field."""
        gdf = _load_target_zones(three_target_zones, "target_id")
        assert "study_geoid" in gdf.columns
        assert len(gdf) == 3

    def test_single_boundary_mode(self, three_target_zones):
        """When id_field is None, should dissolve to a single geometry with study_geoid=1."""
        gdf = _load_target_zones(three_target_zones, None)
        assert len(gdf) == 1
        assert gdf["study_geoid"].iloc[0] == "1"

    def test_from_file(self, target_zone_file):
        """Verify that target zones can be loaded from a file path."""
        gdf = _load_target_zones(target_zone_file, "target_id")
        assert len(gdf) == 3

    def test_missing_id_field_raises(self, three_target_zones):
        """Test that a missing ID field raises a ValueError."""
        with pytest.raises(ValueError, match="not found"):
            _load_target_zones(three_target_zones, "nonexistent_col")


# ---------------------------------------------------------------------------
# Tests: rasterization
# ---------------------------------------------------------------------------
def _grid(bounds, resolution):
    """Inline helper replacing the removed _compute_grid."""
    minx, miny, maxx, maxy = bounds
    w = int(np.ceil((maxx - minx) / resolution))
    h = int(np.ceil((maxy - miny) / resolution))
    transform = from_bounds(minx, miny, minx + w * resolution, miny + h * resolution, w, h)
    return transform, (h, w)


class TestRasterization:
    """Tests for rasterization helpers: _rasterize_weights and _rasterize_categorical."""

    def test_grid_dimensions(self):
        """Verify that the grid dimensions are computed correctly."""
        _, shape = _grid((0, 0, 2000, 1000), resolution=100)
        assert shape == (10, 20)  # height=1000/100, width=2000/100

    def test_population_raster_conserves_total(self, uniform_blocks):
        """Rasterized population should approximately conserve total population."""
        transform, shape = _grid((0, 0, 2000, 1000), resolution=100)
        arr = _rasterize_weights(uniform_blocks, "pop20", transform, shape)

        total = float(arr.sum())
        expected = uniform_blocks["pop20"].sum()
        assert abs(total - expected) / expected < 0.05, f"Got {total}, expected {expected}"

    def test_categorical_raster_labels_all_zones(self, two_pumas):
        """Verify that categorical raster produces integer labels that map back to original IDs."""
        transform, shape = _grid((0, 0, 2000, 1000), resolution=100)
        arr, int_to_id = _rasterize_categorical(two_pumas, "puma_id", transform, shape)

        # Should have 2 unique non-zero values
        unique = set(arr[arr > 0].ravel())
        assert len(unique) == 2

        # Lookup should map back to original IDs
        assert set(int_to_id.values()) == {"A", "B"}


# ---------------------------------------------------------------------------
# Tests: Pydantic config models
# ---------------------------------------------------------------------------
class TestGeographyConfig:
    """Tests for GeographyConfig validation and defaults."""

    def test_valid_config(self):
        """Test that a valid GeographyConfig can be created with all fields."""
        cfg = GeographyConfig(
            target_zones=TargetZoneConfig(file="zones.shp", id_field="COUNTYFP"),
            resolution=250,
        )
        assert cfg.resolution == 250
        assert cfg.target_zones.file == "zones.shp"

    def test_default_values(self):
        """Test that default values are set correctly when optional fields are omitted."""
        cfg = GeographyConfig(
            target_zones=TargetZoneConfig(file="zones.shp"),
        )
        assert cfg.resolution == 100
        assert cfg.min_allocation == 0.0
        assert cfg.target_zones.id_field is None

    def test_negative_resolution_raises(self):
        """Resolution must be positive."""
        with pytest.raises(ValueError, match="positive"):
            GeographyConfig(
                target_zones=TargetZoneConfig(file="zones.shp"),
                resolution=-100,
            )

    def test_from_dict(self):
        """Test that a GeographyConfig can be constructed from a dict (e.g. from YAML)."""
        d = {
            "target_zones": {"file": "zones.shp", "id_field": "TAZ"},
            "resolution": 100,
        }
        cfg = GeographyConfig(**d)
        assert cfg.target_zones.id_field == "TAZ"
        assert cfg.resolution == 100


# ---------------------------------------------------------------------------
# Tests: household target zone assignment
# ---------------------------------------------------------------------------
class TestAssignHouseholds:
    """Test PumaCrosswalk.assign_households via a minimal instance."""

    @staticmethod
    def _make_xw(target_gdf: gpd.GeoDataFrame) -> PumaCrosswalk:
        """Build a bare PumaCrosswalk with only target_gdf set."""
        obj = object.__new__(PumaCrosswalk)
        obj.target_gdf = _load_target_zones(target_gdf, "target_id")
        obj.zone_groups = {}
        obj._zone_remap = {}
        return obj

    def test_assigns_correct_zones(self, three_target_zones):
        """Households should be assigned to the correct target zones based on their home coord."""
        hh = pl.DataFrame(
            {
                "hh_id": [1, 2, 3, 4],
                "home_lon": [350.0, 1000.0, 1650.0, -999.0],
                "home_lat": [500.0, 500.0, 500.0, 500.0],
            }
        )
        target = three_target_zones.copy().set_crs("EPSG:4326", allow_override=True)

        xw = self._make_xw(target)
        result = xw.assign_households(hh)
        assert "ctrl_geoid" in result.columns
        assert result.height == 4

        assigned = result.select("hh_id", "ctrl_geoid").sort("hh_id")
        assert assigned[0, "ctrl_geoid"] == "1"
        assert assigned[1, "ctrl_geoid"] == "2"
        assert assigned[2, "ctrl_geoid"] == "3"
        assert assigned[3, "ctrl_geoid"] is None

    def test_preserves_columns(self, three_target_zones):
        """All original columns in the households DataFrame should be preserved."""
        hh = pl.DataFrame(
            {
                "hh_id": [1],
                "home_lon": [350.0],
                "home_lat": [500.0],
                "extra_col": ["keep_me"],
            }
        )
        target = three_target_zones.copy().set_crs("EPSG:4326", allow_override=True)
        xw = self._make_xw(target)
        result = xw.assign_households(hh)
        assert "extra_col" in result.columns
        assert result[0, "extra_col"] == "keep_me"


# ---------------------------------------------------------------------------
# Tests: full crosswalk (integration with exactextract)
# ---------------------------------------------------------------------------
class TestCrossTabulation:
    """Integration tests: exactextract cross-tabulation with coverage fractions."""

    def test_cross_tabulate_synthetic(self, two_pumas, three_target_zones, uniform_blocks):
        """Verify allocation weights for a known synthetic geometry.

        With uniform blocks (100 pop each, 200m wide) across a 2000m region:
        - PUMA A (0-1000): 5 blocks = 500 pop
        - PUMA B (1000-2000): 5 blocks = 500 pop
        - Zone 1 (0-700): ~300-350 pop from PUMA A
        - Zone 2 (700-1300): ~300 pop mixed
        - Zone 3 (1300-2000): ~300-350 pop from PUMA B

        Boundaries that bisect a block cause the block's population to
        land in whichever zone contains the block centroid, so exact
        sub-block splits are not expected.
        """
        transform, shape = _grid((0, 0, 2000, 1000), resolution=50)
        pop_arr = _rasterize_weights(uniform_blocks, "pop20", transform, shape)
        puma_arr, int_to_puma = _rasterize_categorical(
            two_pumas,
            "puma_id",
            transform,
            shape,
        )

        result = _cross_tabulate(
            pop_arr,
            puma_arr,
            int_to_puma,
            transform,
            three_target_zones,
        )

        assert isinstance(result, pl.DataFrame)
        assert set(result.columns) == {"source_id", "target_id", "population"}

        # Check total population is approximately conserved
        total_pop = result["population"].sum()
        assert abs(total_pop - 1000) < 50, f"Total pop {total_pop}, expected ~1000"

        # PUMA A -> Zone 1 should be ~300-350 (boundary block may go either way)
        a_z1 = result.filter((pl.col("source_id") == "A") & (pl.col("target_id") == "1"))[
            "population"
        ].sum()
        assert abs(a_z1 - 300) < 100, f"PUMA A, Zone 1: {a_z1}, expected ~300-350"

    def test_allocation_weights_sum_to_one(self, two_pumas, three_target_zones, uniform_blocks):
        """Allocation weights per PUMA should approximately sum to 1.0."""
        transform, shape = _grid((0, 0, 2000, 1000), resolution=50)
        pop_arr = _rasterize_weights(uniform_blocks, "pop20", transform, shape)
        puma_arr, int_to_puma = _rasterize_categorical(
            two_pumas,
            "puma_id",
            transform,
            shape,
        )

        result = _cross_tabulate(
            pop_arr,
            puma_arr,
            int_to_puma,
            transform,
            three_target_zones,
        )

        result = result.with_columns(
            (pl.col("population") / pl.col("population").sum().over("source_id")).alias(
                "allocation_weight"
            )
        )
        weight_sums = result.group_by("source_id").agg(
            pl.col("allocation_weight").sum().alias("total")
        )
        for row in weight_sums.iter_rows(named=True):
            assert abs(row["total"] - 1.0) < 0.02, (
                f"Source {row['source_id']}: weights sum to {row['total']}"
            )


# ---------------------------------------------------------------------------
# Tests: strict cross-tabulation math (sub-pixel coverage)
# ---------------------------------------------------------------------------
class TestCrossTabMath:
    """Exact arithmetic verification of the PUMA x target-zone cross-tab.

    Uses a 4x4 grid (100m cells, 400x400m region) with two PUMAs split
    at x=200 and three target zones with a *middle zone that straddles
    the PUMA boundary at non-cell-aligned x=150 and x=250*.

    This forces exactextract to produce fractional coverage (0.5) on the
    boundary cells, so we can verify the ``pop * coverage`` math exactly.

    Grid layout (100m cells, each cell labelled ``pop / puma_int``)::

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
              |   |=========|   |
             150  zone M   250

    Zone L:  x=[0, 150]   -> full col0 + half col1 (coverage=0.5)
    Zone M:  x=[150, 250] -> half col1 + half col2  (coverage=0.5 each)
    Zone R:  x=[250, 400] -> half col2 + full col3

    Expected cross-tab (sum of pop * coverage by PUMA, target):

    Zone L, PUMA A:  col0(10*4=40)*1.0  + col1(10*2=20+30*2=60)*0.5 = 40 + 40 = 80
                     (rows y2-y3: 10*1.0=10 twice, 10*0.5=5 twice = 30)
                     (rows y0-y1: 30*1.0=30 twice, 30*0.5=15 twice = 90)
                     total = 30 + 90 = 120   ... let me recalculate properly.

    Actually let me lay out more carefully.
    col0: x=[0,100],  cells: (y0,x0)=30, (y1,x0)=30, (y2,x0)=10, (y3,x0)=10.  PUMA=A
    col1: x=[100,200], cells: 30, 30, 10, 10.  PUMA=A
    col2: x=[200,300], cells: 40, 40, 20, 20.  PUMA=B
    col3: x=[300,400], cells: 40, 40, 20, 20.  PUMA=B

    Zone L (x=0..150): full coverage on col0, 50% coverage on col1
      PUMA A only: (30+30+10+10)*1.0 + (30+30+10+10)*0.5 = 80 + 40 = 120

    Zone M (x=150..250): 50% coverage on col1 (PUMA A), 50% on col2 (PUMA B)
      PUMA A: (30+30+10+10)*0.5 = 40
      PUMA B: (40+40+20+20)*0.5 = 60

    Zone R (x=250..400): 50% on col2, full on col3
      PUMA B: (40+40+20+20)*0.5 + (40+40+20+20)*1.0 = 60 + 120 = 180

    Total: 120 + 40 + 60 + 180 = 400  ✓
    (sum of all cell values = 4*(30+30+10+10) + ... = 80+80+120+120=400 ✓)

    Allocation weights:
      PUMA A total = 120 + 40 = 160
        w(A->L) = 120/160 = 0.75,  w(A->M) = 40/160 = 0.25
      PUMA B total = 60 + 180 = 240
        w(B->M) = 60/240 = 0.25,   w(B->R) = 180/240 = 0.75
    """

    @pytest.fixture
    def grid_4x4(self):
        """Build the 4x4 test grid and run _cross_tabulate once.

        Returns the raw inputs *and* the cross-tab result so every test
        in this class can validate different properties without re-running
        the expensive exactextract call.
        """
        transform = from_bounds(0, 0, 400, 400, 4, 4)
        shape = (4, 4)

        # Population: top half 10/20, bottom half 30/40
        pop = np.array(
            [[10, 10, 20, 20], [10, 10, 20, 20], [30, 30, 40, 40], [30, 30, 40, 40]],
            dtype=np.float32,
        )
        # PUMA labels: left=1(A), right=2(B) at x=200
        puma = np.array(
            [[1, 1, 2, 2], [1, 1, 2, 2], [1, 1, 2, 2], [1, 1, 2, 2]],
            dtype=np.int32,
        )
        int_to_puma = {1: "A", 2: "B"}

        # Target zones straddle PUMA boundary at non-cell-aligned positions
        zones = gpd.GeoDataFrame(
            {"target_id": ["L", "M", "R"]},
            geometry=[
                box(0, 0, 150, 400),  # half-cell overlap with col 1
                box(150, 0, 250, 400),  # half-cell from PUMA A + half from B
                box(250, 0, 400, 400),  # half-cell overlap with col 2
            ],
            crs="EPSG:5070",
        )
        result = _cross_tabulate(pop, puma, int_to_puma, transform, zones)
        return {
            "pop": pop,
            "puma": puma,
            "int_to_puma": int_to_puma,
            "transform": transform,
            "shape": shape,
            "zones": zones,
            "result": result,
        }

    def test_population_by_puma_and_zone(self, grid_4x4):
        """Verify exact population totals with sub-pixel coverage."""
        result = grid_4x4["result"]

        def _pop(source_id: str, target_id: str) -> float:
            return result.filter(
                (pl.col("source_id") == source_id) & (pl.col("target_id") == target_id)
            )["population"].sum()

        # Zone L (x=0..150): PUMA A only
        assert _pop("A", "L") == pytest.approx(120.0, abs=1)
        # Zone M (x=150..250): straddles boundary
        assert _pop("A", "M") == pytest.approx(40.0, abs=1)
        assert _pop("B", "M") == pytest.approx(60.0, abs=1)
        # Zone R (x=250..400): PUMA B only
        assert _pop("B", "R") == pytest.approx(180.0, abs=1)

    def test_total_population_conserved(self, grid_4x4):
        """Cross-tab total must equal sum of all cell values."""
        result = grid_4x4["result"]
        pop = grid_4x4["pop"]
        assert result["population"].sum() == pytest.approx(float(pop.sum()), abs=1)

    def test_allocation_weights(self, grid_4x4):
        """Normalised weights per PUMA must match analytic values."""
        result = grid_4x4["result"]

        result = result.with_columns(
            (pl.col("population") / pl.col("population").sum().over("source_id")).alias("w"),
        )

        def _w(source_id: str, target_id: str) -> float:
            return result.filter(
                (pl.col("source_id") == source_id) & (pl.col("target_id") == target_id)
            )["w"].sum()

        # PUMA A: 120/(120+40) = 0.75 to L, 0.25 to M
        assert _w("A", "L") == pytest.approx(0.75, abs=0.02)
        assert _w("A", "M") == pytest.approx(0.25, abs=0.02)
        # PUMA B: 60/(60+180) = 0.25 to M, 0.75 to R
        assert _w("B", "M") == pytest.approx(0.25, abs=0.02)
        assert _w("B", "R") == pytest.approx(0.75, abs=0.02)

    def test_weights_sum_to_one_per_puma(self, grid_4x4):
        """Each PUMA's allocation weights must sum to exactly 1.0."""
        result = grid_4x4["result"]

        result = result.with_columns(
            (pl.col("population") / pl.col("population").sum().over("source_id")).alias("w"),
        )
        for row in result.group_by("source_id").agg(pl.col("w").sum()).iter_rows(named=True):
            assert row["w"] == pytest.approx(1.0, abs=1e-9)


# ---------------------------------------------------------------------------
# Tests: control_data crosswalk integration
# ---------------------------------------------------------------------------
class TestControlDataCrosswalk:
    """Verify PumaCrosswalk.allocate_pums_weights + build_control_totals."""

    def test_crosswalk_redistributes_controls(self):
        """Expanded PUMS totals should be split across target zones."""
        # Synthetic PUMS data — 2 PUMAs, 3 households
        hh = pl.DataFrame(
            {
                "SERIALNO": ["H1", "H2", "H3"],
                "PUMA": ["A", "A", "B"],
                "ST": ["06", "06", "06"],
                "WGTP": [100, 200, 150],
                "NP": [1, 3, 2],
                "HINCP": [50000, 100000, 75000],
                "VEH": [1, 2, 0],
                "NOC": [0, 1, 0],
                "TYPEHUGQ": [1, 1, 1],
            }
        )
        per = pl.DataFrame(
            {
                "SERIALNO": ["H1", "H2", "H2", "H2", "H3", "H3"],
                "SPORDER": [1, 1, 2, 3, 1, 2],
                "PUMA": ["A", "A", "A", "A", "B", "B"],
                "ST": ["06", "06", "06", "06", "06", "06"],
                "PWGTP": [100, 200, 200, 200, 150, 150],
                "AGEP": [35, 40, 10, 5, 55, 25],
                "SEX": [1, 2, 1, 2, 1, 2],
                "ESR": [1, 1, 0, 0, 1, 3],
                "JWTRNS": [1, 1, None, None, 6, None],
                "SCHG": [None, None, 2, 1, None, None],
                "SCHL": [21, 22, None, None, 20, 19],
                "RAC1P": [1, 1, 1, 1, 2, 2],
                "HISP": [1, 1, 1, 1, 1, 1],
            }
        )

        hh_recoded = recode_pums_households(hh, per, ["h_size"])
        per_recoded = recode_pums_persons(per, ["h_size"])

        # Crosswalk: PUMA A splits 70/30 between zones T1/T2; PUMA B is 100% T2
        crosswalk_df = pl.DataFrame(
            {
                "puma_id": ["A", "A", "B"],
                "study_geoid": ["T1", "T2", "T2"],
                "ctrl_geoid": ["T1", "T2", "T2"],
                "allocation_weight": [0.7, 0.3, 1.0],
            }
        )

        # Use PumaCrosswalk.allocate_pums_weights via a minimal instance
        xw = object.__new__(PumaCrosswalk)
        xw.crosswalk_df = crosswalk_df
        hh_xw, per_xw = xw.allocate_pums_weights(hh_recoded, per_recoded, geo_col="PUMA")

        result = build_control_totals(
            hh_xw,
            per_xw,
            [ControlSpec(name="h_size")],
            geo_col="ctrl_geoid",
        )

        assert isinstance(result, ControlTotals)
        assert set(result.geo_ids) == {"T1", "T2"}

        # T1 should have 70% of PUMA A's totals
        t1_total = result.totals.filter(pl.col("geo_id") == "T1")["target_total"].sum()
        a_total = 100 + 200  # WGTP for HH1 + HH2 in PUMA A
        expected_t1 = a_total * 0.7
        assert abs(t1_total - expected_t1) < 1, f"T1 total {t1_total}, expected {expected_t1}"

    def test_no_crosswalk_uses_puma(self):
        """Without crosswalk expansion, aggregation is by PUMA directly."""
        hh = pl.DataFrame(
            {
                "SERIALNO": ["H1", "H2"],
                "PUMA": ["A", "B"],
                "ST": ["06", "06"],
                "WGTP": [100, 150],
                "NP": [2, 1],
                "HINCP": [60000, 40000],
                "VEH": [1, 0],
                "NOC": [0, 0],
                "TYPEHUGQ": [1, 1],
            }
        )
        per = pl.DataFrame(
            {
                "SERIALNO": ["H1", "H1", "H2"],
                "SPORDER": [1, 2, 1],
                "PUMA": ["A", "A", "B"],
                "ST": ["06", "06", "06"],
                "PWGTP": [100, 100, 150],
                "AGEP": [30, 28, 45],
                "SEX": [1, 2, 1],
                "ESR": [1, 1, 1],
                "JWTRNS": [1, 1, 6],
                "SCHG": [None, None, None],
                "SCHL": [21, 21, 20],
                "RAC1P": [1, 1, 2],
                "HISP": [1, 1, 1],
            }
        )

        hh_recoded = recode_pums_households(hh, per, ["h_size"])
        per_recoded = recode_pums_persons(per, ["h_size"])

        result = build_control_totals(
            hh_recoded,
            per_recoded,
            [ControlSpec(name="h_size")],
            geo_col="PUMA",
        )
        # Should aggregate by PUMA directly
        assert set(result.geo_ids) == {"A", "B"}


# ---------------------------------------------------------------------------
# Tests: plot_crosswalk
# ---------------------------------------------------------------------------
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
            target_id_col="study_geoid",
            weight_col="pop20",
            resolution=50,
        ).rename({"source_id": "puma_id", "target_id": "study_geoid"})
        return two_pumas, target_gdf, xw_df

    def test_produces_html(self, crosswalk_data):
        """crosswalk_figure should produce valid Plotly HTML."""
        puma_gdf, target_gdf, xw_df = crosswalk_data
        fig = crosswalk_figure(puma_gdf=puma_gdf, target_gdf=target_gdf, crosswalk_df=xw_df)
        assert fig is not None
        html = fig.to_html()
        assert "plotly" in html.lower()

    def test_with_households_and_zone_groups(self, crosswalk_data):
        """crosswalk_figure should accept households and zone_groups."""
        puma_gdf, target_gdf, xw_df = crosswalk_data
        hh = pl.DataFrame({"hh_id": [1, 2, 3], "ctrl_geoid": ["1", "2", "3"]})
        groups = {"north": ["1", "2"]}
        fig = crosswalk_figure(
            puma_gdf=puma_gdf,
            target_gdf=target_gdf,
            crosswalk_df=xw_df,
            households=hh,
            zone_groups=groups,
        )
        html = fig.to_html()
        assert "plotly" in html.lower()
        assert "north" in html.lower()
