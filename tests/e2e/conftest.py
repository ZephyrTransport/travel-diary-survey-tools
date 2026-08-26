"""Fixtures for E2E integration tests.

The pipeline config is built programmatically from a set of enabled *optional*
steps, rather than from per-"year" YAML files. This lets the suite parametrize
the step toggling and verify that turning a step on/off does not break the
downstream steps.

- Mandatory steps always run: load_data, link_trips, extract_tours, add_zone_ids,
  write_data.
- Optional steps toggle: detect_joint_trips, imputation, format_ctramp,
  format_daysim. (The format_* steps are terminal/parallel — nothing depends on
  their output — so by default they are on; they are toggled only to prove
  independence.)

Profiles are a leave-one-out matrix ("full", and full minus each optional step)
so each profile isolates the effect of removing one step. All data is generated
on the fly into a temp directory; each unique profile runs once (memoized) and
temp dirs are cleaned up at session end.
"""

import shutil
import tempfile
from pathlib import Path

import pytest
import yaml

E2E_DIR = Path(__file__).parent

# Optional steps that can be toggled (canonical execution order preserved below).
OPTIONAL_STEPS = ("detect_joint_trips", "imputation", "format_ctramp", "format_daysim")

# Inter-step dependencies: a step may only run if its requirements also run.
# format_ctramp consumes the joint_trips table (and emits joint-tour/joint-trip
# CT-RAMP outputs), so it requires detect_joint_trips.
_REQUIRES = {"format_ctramp": frozenset({"detect_joint_trips"})}


def _resolve(enabled: frozenset) -> frozenset:
    """Drop any enabled step whose required upstream steps are not enabled."""
    current = set(enabled)
    changed = True
    while changed:
        changed = False
        for step, reqs in _REQUIRES.items():
            if step in current and not reqs <= current:
                current.discard(step)
                changed = True
    return frozenset(current)


# Leave-one-out profiles: "full" plus one profile per optional step removed.
# Each is dependency-resolved (e.g. removing detect_joint_trips also removes
# format_ctramp, which depends on it).
_ALL = frozenset(OPTIONAL_STEPS)
PROFILES = {
    "full": _resolve(_ALL),
    "no_joint": _resolve(_ALL - {"detect_joint_trips"}),
    "no_imputation": _resolve(_ALL - {"imputation"}),
    "no_ctramp": _resolve(_ALL - {"format_ctramp"}),
    "no_daysim": _resolve(_ALL - {"format_daysim"}),
}

# Canonical step order (mandatory + optional interleaved correctly).
# cascade_completeness is mandatory: it mirrors the shipping bats_2023 pipeline,
# where the formatters and weighting read the flags it stamps rather than
# re-deriving them per row.
_MANDATORY = {
    "load_data",
    "link_trips",
    "extract_tours",
    "cascade_completeness",
    "add_zone_ids",
    "write_data",
}
_STEP_ORDER = (
    "load_data",
    "link_trips",
    "detect_joint_trips",
    "imputation",
    "extract_tours",
    "cascade_completeness",
    "add_zone_ids",
    "format_ctramp",
    "format_daysim",
    "write_data",
)


def _materialize_data(tmp: Path) -> Path:
    """Write generated synthetic survey data + zones to *tmp*; return the data root."""
    import json

    from tests.e2e.generate_toy_data import ZONE_GEOJSON, build_survey_dataframes

    survey_dir = tmp / "survey"
    survey_dir.mkdir(parents=True)
    for name, df in build_survey_dataframes().items():
        df.write_parquet(survey_dir / f"{name}.parquet")

    zones_dir = tmp / "zones"
    zones_dir.mkdir()
    (zones_dir / "test_zones.geojson").write_text(json.dumps(ZONE_GEOJSON))
    return tmp


def _p(path: Path) -> str:
    """Forward-slash string path (portable inside the YAML config)."""
    return str(path).replace("\\", "/")


def _step_blocks(data_dir: Path, output_dir: Path, enabled: frozenset) -> dict:
    """Return the config block for every step keyed by name."""
    survey = _p(data_dir / "survey")
    zones = _p(data_dir / "zones")
    out = _p(output_dir)

    output_paths = {
        "households": f"{out}/survey/households.csv",
        "persons": f"{out}/survey/persons.csv",
        "days": f"{out}/survey/days.csv",
        "unlinked_trips": f"{out}/survey/unlinked_trips.csv",
        "linked_trips": f"{out}/survey/linked_trips.csv",
        "tours": f"{out}/survey/tours.csv",
    }
    if "detect_joint_trips" in enabled:
        output_paths["joint_trips"] = f"{out}/survey/joint_trips.csv"

    return {
        "load_data": {
            "name": "load_data",
            "validate_input": False,
            "cache": False,
            "params": {
                "input_paths": {
                    "households": f"{survey}/households.parquet",
                    "persons": f"{survey}/persons.parquet",
                    "days": f"{survey}/days.parquet",
                    "unlinked_trips": f"{survey}/unlinked_trips.parquet",
                }
            },
        },
        "link_trips": {
            "name": "link_trips",
            "validate_input": False,
            "cache": False,
            "params": {
                "change_mode_enum": "CHANGE_MODE",
                "transit_mode_enums": ["FERRY", "TRANSIT", "LONG_DISTANCE"],
                "max_dwell_time": 180,
                "dwell_buffer_distance": 100,
                "split_on_occupancy": False,
            },
        },
        "detect_joint_trips": {
            "name": "detect_joint_trips",
            "validate_input": False,
            "cache": False,
            "params": {
                "config": {
                    "method": "mahalanobis",
                    "covariance": [10000, 10000, 100, 100],
                    "confidence_level": 0.75,
                }
            },
        },
        "imputation": {
            "name": "imputation",
            "validate_input": False,
            "cache": False,
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
            },
        },
        "extract_tours": {"name": "extract_tours", "validate_input": False, "cache": False},
        "cascade_completeness": {
            "name": "cascade_completeness",
            "validate_input": False,
            "cache": False,
        },
        "add_zone_ids": {
            "name": "add_zone_ids",
            "validate_input": False,
            "cache": False,
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
                ]
            },
        },
        "format_ctramp": {
            "name": "format_ctramp",
            "validate_input": False,
            "cache": False,
            "params": {
                "income_low_threshold": 60000,
                "income_med_threshold": 150000,
                "income_high_threshold": 240000,
                "income_survey_year_to_ctramp_year": 0.5319148936,  # 1/1.88: $2023 -> $2000
                "age_adult": 4,
                "gender_default_for_missing": "f",
                "taz_field": "taz",
                "drop_missing_taz": True,
            },
        },
        # validate_output mirrors the shipping bats_2023 config. The DaySim row
        # models pin the output schema (e.g. half must be 1 or 2), so a trip
        # carrying an internal sentinel instead of a real value fails here
        # rather than in a production run.
        "format_daysim": {
            "name": "format_daysim",
            "validate_input": False,
            "validate_output": True,
            "cache": False,
        },
        "write_data": {
            "name": "write_data",
            "validate_input": False,
            "params": {"output_paths": output_paths},
        },
    }


def _build_config(enabled: frozenset, data_dir: Path, output_dir: Path) -> dict:
    """Assemble a pipeline config dict from the enabled optional steps."""
    blocks = _step_blocks(data_dir, output_dir, enabled)
    steps = [blocks[n] for n in _STEP_ORDER if n in _MANDATORY or n in enabled]
    return {"steps": steps}


def _run_pipeline(enabled: frozenset):
    """Run the pipeline for a set of enabled optional steps. Returns (result, output_dir, tmp)."""
    from pipeline.pipeline import Pipeline
    from processing import (
        add_zone_ids,
        cascade_completeness,
        detect_joint_trips,
        extract_tours,
        format_ctramp,
        format_daysim,
        imputation,
        link_trips,
        load_data,
        write_data,
    )

    tmp = Path(tempfile.mkdtemp(prefix="e2e_"))
    data_dir = _materialize_data(tmp / "data")
    output_dir = tmp / "output"
    output_dir.mkdir()

    config_path = output_dir / "config.yaml"
    config_path.write_text(yaml.safe_dump(_build_config(enabled, data_dir, output_dir)))

    # All step functions are passed; the config decides which actually run.
    steps = [
        load_data,
        link_trips,
        detect_joint_trips,
        imputation,
        extract_tours,
        cascade_completeness,
        add_zone_ids,
        format_ctramp,
        format_daysim,
        write_data,
    ]

    data_models = {}
    if "format_daysim" in enabled:
        from data_canon.models import daysim as daysim_models

        data_models.update(
            {
                "households_daysim": daysim_models.HouseholdDaysimModel,
                "persons_daysim": daysim_models.PersonDaysimModel,
                "days_daysim": daysim_models.PersonDayDaysimModel,
                "linked_trips_daysim": daysim_models.LinkedTripDaysimModel,
                "tours_daysim": daysim_models.TourDaysimModel,
            }
        )
    if "format_ctramp" in enabled:
        from data_canon.models import ctramp as ctramp_models

        data_models.update(
            {
                "households_ctramp": ctramp_models.HouseholdCTRAMPModel,
                "persons_ctramp": ctramp_models.PersonCTRAMPModel,
                "mandatory_locations_ctramp": ctramp_models.MandatoryLocationCTRAMPModel,
                "individual_tours_ctramp": ctramp_models.IndividualTourCTRAMPModel,
                "individual_trips_ctramp": ctramp_models.IndividualTripCTRAMPModel,
                "joint_tours_ctramp": ctramp_models.JointTourCTRAMPModel,
                "joint_trips_ctramp": ctramp_models.JointTripCTRAMPModel,
            }
        )

    pipeline = Pipeline(
        config_path=config_path, steps=steps, caching=False, data_models=data_models
    )
    result = pipeline.run()
    return result, output_dir, tmp


# Memoize by enabled-set so each unique profile runs at most once.
_RUNS: dict[frozenset, tuple] = {}


def _get_run(enabled: frozenset):
    if enabled not in _RUNS:
        _RUNS[enabled] = _run_pipeline(enabled)
    return _RUNS[enabled]


@pytest.fixture(scope="session", autouse=True)
def _cleanup_runs():
    """Remove all memoized run temp dirs at session end."""
    yield
    for _result, _out, tmp in _RUNS.values():
        shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture(scope="session", params=list(PROFILES), ids=list(PROFILES))
def profile_run(request):
    """Parametrized over every toggle profile. Yields (name, enabled, result, output_dir)."""
    enabled = PROFILES[request.param]
    result, output_dir, _tmp = _get_run(enabled)
    return request.param, enabled, result, output_dir


@pytest.fixture(scope="session")
def full_result():
    """Result of the 'full' profile (all optional steps on) for feature-specific tests."""
    result, _output_dir, _tmp = _get_run(PROFILES["full"])
    return result


@pytest.fixture(scope="session")
def full_output_dir():
    """Output dir of the 'full' profile."""
    _result, output_dir, _tmp = _get_run(PROFILES["full"])
    return output_dir


@pytest.fixture(scope="session")
def full_input_dir():
    """Raw input dir of the 'full' profile.

    The vendor surface: columns present here are carried through rather than
    computed, which is what lets a test tell the two apart.
    """
    _result, _output_dir, tmp = _get_run(PROFILES["full"])
    return Path(tmp) / "data"
