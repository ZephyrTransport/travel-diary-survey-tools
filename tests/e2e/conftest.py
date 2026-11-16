"""Fixtures for the e2e tests: the toy survey run through the pipeline once per profile.

A profile is the full step list less the steps it drops, so each one shows that turning
those steps off does not break the steps after them. Each profile runs at most once per
session, in a temp directory removed at the end.
"""

import json
import shutil
import tempfile
from pathlib import Path

import polars as pl
import pytest
import yaml

from data_canon.models import ctramp, daysim

# The value both detect_habitual_locations and extract_tours use here; each is its own setting.
HABITUAL_BUFFER_METERS = 300.0

# Zones precede the cascade: a profile can gate on whether a record has a zone.
STEP_ORDER = (
    "load_data",
    "imputation",
    "link_trips",
    "detect_habitual_locations",
    "detect_joint_trips",
    "extract_tours",
    "add_zone_ids",
    "cascade_usability",
    "add_existing_weights",
    "format_ctramp",
    "format_daysim",
    "write_data",
)

# Each profile names the steps it drops.
PROFILES = {
    "full": frozenset(),
    "no_imputation": frozenset({"imputation"}),
    "no_ctramp": frozenset({"format_ctramp"}),
    "no_daysim": frozenset({"format_daysim"}),
}

# Each formatter's output tables and the models write_data validates them against.
FORMATTED_TABLES = {
    "format_ctramp": {
        "households_ctramp": ctramp.HouseholdCTRAMPModel,
        "persons_ctramp": ctramp.PersonCTRAMPModel,
        "mandatory_locations_ctramp": ctramp.MandatoryLocationCTRAMPModel,
        "individual_tours_ctramp": ctramp.IndividualTourCTRAMPModel,
        "individual_trips_ctramp": ctramp.IndividualTripCTRAMPModel,
        "joint_tours_ctramp": ctramp.JointTourCTRAMPModel,
        "joint_trips_ctramp": ctramp.JointTripCTRAMPModel,
    },
    "format_daysim": {
        "households_daysim": daysim.HouseholdDaysimModel,
        "persons_daysim": daysim.PersonDaysimModel,
        "days_daysim": daysim.PersonDayDaysimModel,
        "linked_trips_daysim": daysim.LinkedTripDaysimModel,
        "tours_daysim": daysim.TourDaysimModel,
    },
}


def _materialize_data(tmp: Path) -> Path:
    """Write generated synthetic survey data + zones to *tmp*; return the data root."""
    from tests.e2e.generate_toy_data import ZONE_GEOJSON, build_survey_dataframes

    tables = build_survey_dataframes()
    survey_dir = tmp / "survey"
    survey_dir.mkdir(parents=True)
    for name, df in tables.items():
        df.write_parquet(survey_dir / f"{name}.parquet")

    zones_dir = tmp / "zones"
    zones_dir.mkdir()
    (zones_dir / "test_zones.geojson").write_text(json.dumps(ZONE_GEOJSON))

    _write_household_weights(tables["households"], tmp / "weights")
    return tmp


def _write_household_weights(households: pl.DataFrame, weights_dir: Path) -> None:
    """Write household weights only, so the run derives every level below them.

    Each household gets a different weight: equal weights would hide an arithmetic
    error that halves one and doubles another.
    """
    weights_dir.mkdir(parents=True, exist_ok=True)
    households.select(
        "hh_id",
        # Distinct, non-round, and none a multiple of another.
        (pl.lit(100.0) + pl.col("hh_id").cast(pl.Float64) * 7.0).alias("hh_weight"),
    ).write_csv(weights_dir / "hh_weights.csv")


def _p(path: Path) -> str:
    """Forward-slash string path (portable inside the YAML config)."""
    return str(path).replace("\\", "/")


def _step_blocks(data_dir: Path, output_dir: Path, steps: tuple[str, ...]) -> dict:
    """Return the config block for every step keyed by name."""
    survey = _p(data_dir / "survey")
    zones = _p(data_dir / "zones")
    out = _p(output_dir)

    output_paths = {
        table: f"{out}/survey/{table}.csv"
        for table in (
            "households",
            "persons",
            "household_days",
            "person_days",
            "unlinked_trips",
            "linked_trips",
            "tours",
            "joint_trips",
        )
    }
    # write_data validates only what it writes, so the formatted tables are written too.
    for step, tables in FORMATTED_TABLES.items():
        if step in steps:
            subdir = step.removeprefix("format_")
            output_paths.update({table: f"{out}/{subdir}/{table}.csv" for table in tables})

    return {
        "load_data": {
            "params": {
                "input_paths": {
                    table: f"{survey}/{table}.parquet"
                    for table in (
                        "households",
                        "persons",
                        "household_days",
                        "person_days",
                        "unlinked_trips",
                        "habitual_locations",
                    )
                }
            },
        },
        "link_trips": {
            "params": {
                "change_mode_enum": "CHANGE_MODE",
                "transit_mode_enums": ["FERRY", "TRANSIT", "LONG_DISTANCE"],
                "max_dwell_time": 180,
                "dwell_buffer_distance": 100,
                "split_on_occupancy": False,
            },
        },
        "detect_joint_trips": {
            "params": {
                "method": "mahalanobis",
                "covariance": [10000, 10000, 100, 100],
                "confidence_level": 0.75,
            },
        },
        "imputation": {
            "params": {
                "impute_columns": {
                    "households": [
                        {
                            "column": "income_bin",
                            "method": "rf",
                            "missing_values": ["MISSING"],
                            "numeric_features": ["num_vehicles", "num_people", "num_workers"],
                            "categorical_features": ["residence_type", "residence_rent_own"],
                        }
                    ]
                },
                "stash_preimputed": True,
                "random_state": 42,
                "validate_imputation": None,
            },
        },
        "detect_habitual_locations": {
            "params": {
                "include_observed_homes": True,
                "buffer_meters": HABITUAL_BUFFER_METERS,
            },
        },
        "extract_tours": {
            "params": {
                "spatial_gap_threshold_meters": 1000.0,
                "buffer_meters": HABITUAL_BUFFER_METERS,
            },
        },
        "cascade_usability": {
            # Two strict profiles the formatters name separately, plus a relaxed one
            # that must reach the output as a registered generated column. The strict
            # two take Tuesday to Thursday, so the week diaries lose their other days.
            "params": {
                "usability_profiles": {
                    "ctramp": {
                        "tour_closes_at": "primary_home",
                        "household_day_needs": "all_members",
                        "zone_coverage": "taz",
                        "joint_tours": "fully_joint",
                        "travel_days": ["tuesday", "wednesday", "thursday"],
                    },
                    "daysim": {
                        "tour_closes_at": "primary_home",
                        "household_day_needs": "all_members",
                        "zone_coverage": "taz",
                        "joint_tours": "fully_joint",
                        "travel_days": ["tuesday", "wednesday", "thursday"],
                    },
                    "analysis": {
                        "tour_closes_at": "anywhere",
                        "household_day_needs": "nothing",
                        "zone_coverage": "none",
                        "joint_tours": "partial",
                        "travel_days": [
                            "monday",
                            "tuesday",
                            "wednesday",
                            "thursday",
                            "friday",
                            "saturday",
                            "sunday",
                        ],
                    },
                }
            },
        },
        "add_existing_weights": {
            # Every profile a formatter names is weighted, and analysis too: ctramp
            # and daysim are declared identically, so only analysis differs.
            "params": {
                # A kept day's dropped trips hand their weight to its kept trips.
                "preserve_travel_totals": True,
                "household_size_top_group": 6,
                "derive_missing_weights": True,
                "weight_profiles": ["ctramp", "daysim", "analysis"],
                "weights": {
                    "hh_weight": {"weight_path": _p(data_dir / "weights" / "hh_weights.csv")},
                },
            },
        },
        "add_zone_ids": {
            "params": {
                "zone_geographies": [
                    {
                        "zone_name": "taz",
                        "shapefile": f"{zones}/test_zones.geojson",
                        "zone_id_field": "TAZ_NODE",
                    },
                    {
                        "zone_name": "maz",
                        "shapefile": f"{zones}/test_zones.geojson",
                        "zone_id_field": "MAZ_NODE",
                    },
                ],
                # No snapping: an unmatched point stays without a zone.
                "projected_crs": None,
                "max_snap_distance": None,
            },
        },
        "format_ctramp": {
            "params": {
                "usability_profile": "ctramp",
                "income_low_threshold": 60000,
                "income_med_threshold": 150000,
                "income_high_threshold": 240000,
                "income_survey_year_to_ctramp_year": 0.5319148936,  # 1/1.88: $2023 -> $2000
                "age_adult": 4,
                "gender_default_for_missing": "f",
                "taz_field": "taz",
            },
        },
        # The DaySim row models pin the output schema (e.g. half must be 1 or 2).
        "format_daysim": {
            "validate_output": True,
            "params": {"usability_profile": "daysim"},
        },
        "write_data": {
            "params": {
                "output_paths": output_paths,
                "write_only_canonical": True,
                "enum_codebook_path": None,
            },
        },
    }


def _build_config(steps: tuple[str, ...], data_dir: Path, output_dir: Path) -> dict:
    """Assemble a pipeline config dict for the given steps, without row validation."""
    blocks = _step_blocks(data_dir, output_dir, steps)
    return {"steps": [{"name": n, "validate_input": False, **blocks[n]} for n in steps]}


def _run_pipeline(steps: tuple[str, ...]):
    """Run the pipeline over the given steps. Returns (result, output_dir, tmp)."""
    import processing
    from pipeline.pipeline import Pipeline

    tmp = Path(tempfile.mkdtemp(prefix="e2e_"))
    data_dir = _materialize_data(tmp / "data")
    output_dir = tmp / "output"
    output_dir.mkdir()

    config_path = output_dir / "config.yaml"
    config_path.write_text(yaml.safe_dump(_build_config(steps, data_dir, output_dir)))

    data_models = {}
    for step, tables in FORMATTED_TABLES.items():
        if step in steps:
            data_models.update(tables)

    # Every step function is passed; the config decides which run.
    pipeline = Pipeline(
        config_path=config_path,
        steps=[getattr(processing, n) for n in STEP_ORDER],
        data_models=data_models,
    )
    return pipeline.run(), output_dir, tmp


# Memoized by profile name so each profile runs at most once.
_RUNS: dict[str, tuple] = {}


def _get_run(profile: str):
    if profile not in _RUNS:
        steps = tuple(s for s in STEP_ORDER if s not in PROFILES[profile])
        _RUNS[profile] = (steps, *_run_pipeline(steps))
    return _RUNS[profile]


@pytest.fixture(scope="session", autouse=True)
def _cleanup_runs():
    """Remove all memoized run temp dirs at session end."""
    yield
    for *_run, tmp in _RUNS.values():
        shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture(scope="session", params=list(PROFILES))
def profile_run(request):
    """Parametrized over every profile. Yields (steps, result, output_dir)."""
    steps, result, output_dir, _tmp = _get_run(request.param)
    return steps, result, output_dir


@pytest.fixture(scope="session")
def full_result():
    """Result of the 'full' profile (every step on)."""
    return _get_run("full")[1]


@pytest.fixture(scope="session")
def full_tables(full_result) -> dict[str, pl.DataFrame]:
    """The canonical tables of the 'full' profile."""
    return full_result.as_dict_non_null()


@pytest.fixture(scope="session")
def full_output_dir():
    """Output dir of the 'full' profile."""
    return _get_run("full")[2]


@pytest.fixture(scope="session")
def full_input_dir():
    """Raw input dir of the 'full' profile."""
    return _get_run("full")[3] / "data"


def pytest_collection_modifyitems(items):
    """Mark every test in this package e2e and slow, so ``-m "not e2e"`` skips the runs."""
    package_root = Path(__file__).parent
    for item in items:
        if package_root in item.path.parents:
            item.add_marker(pytest.mark.e2e)
            item.add_marker(pytest.mark.slow)
