"""Paths are built from roots, and a root says where a share is on this machine."""

from pathlib import Path

import pytest
import yaml

from pipeline.config import (
    _referenced_names,
    _resolve_variables,
    check_input_paths,
    check_roots_resolved,
    shorthands_used_by_steps,
    template_namespace,
)
from pipeline.pipeline import Pipeline

ROOTED = """
survey_dir: "{{ MTC_DATA }}/Data/HomeInterview/x"
output_dir: "{{ BOX_DIR }}/out"
log_file: "{{ output_dir }}/run.log"
steps:
  - name: load
    params:
      input: "{{ survey_dir }}/hh.csv"
"""

ENVS_DEFAULT = 'ENVS:\n  SHARED: "/opt/shared"\nd: "{{ SHARED }}/x"'


def _config(text: str = ROOTED) -> dict:
    return yaml.safe_load(text)


def _project(name: str) -> dict:
    return yaml.safe_load((Path("projects") / name / "config.yaml").read_text(encoding="utf-8"))


class TestOneNamespace:
    """Roots, ``ENVS`` defaults and the config's own keys all feed the templating."""

    def test_a_root_comes_from_the_environment(self, monkeypatch):
        """A root is read from the environment."""
        monkeypatch.setenv("MTC_DATA", "//share/models")

        assert template_namespace(_config())["MTC_DATA"] == "//share/models"

    def test_the_configs_own_keys_are_still_available(self):
        """The config's own keys are in the namespace too."""
        assert "output_dir" in template_namespace(_config())

    @pytest.mark.parametrize(
        ("env", "expected"),
        [
            pytest.param(None, "/opt/shared", id="unset-uses-envs-default"),
            pytest.param("/mnt/elsewhere", "/mnt/elsewhere", id="environment-wins"),
            pytest.param("", "/opt/shared", id="empty-is-unset"),
        ],
    )
    def test_envs_block_is_a_default_the_environment_overrides(
        self, monkeypatch, env: str | None, expected: str
    ):
        """``ENVS`` supplies a default; a non-empty environment variable replaces it."""
        if env is None:
            monkeypatch.delenv("SHARED", raising=False)
        else:
            monkeypatch.setenv("SHARED", env)

        assert template_namespace(_config(ENVS_DEFAULT))["SHARED"] == expected

    def test_roots_resolve_through_to_a_full_path(self, monkeypatch):
        """Root and configured remainder resolve to a full path."""
        monkeypatch.setenv("MTC_DATA", "//share/models")
        monkeypatch.setenv("BOX_DIR", "/box")

        resolved = _resolve_variables(template_namespace(_config()))

        assert resolved["survey_dir"] == "//share/models/Data/HomeInterview/x"
        assert resolved["log_file"] == "/box/out/run.log"

    def test_a_malformed_envs_block_is_refused(self):
        """A scalar ``ENVS`` block raises."""
        with pytest.raises(TypeError, match="must be a mapping"):
            template_namespace(_config("ENVS: not-a-mapping\nd: x"))


class TestAnUnsetRootIsNamed:
    """An unset root is reported before the run, not rendered literally."""

    def test_every_missing_root_is_named_at_once(self, monkeypatch):
        """All unset roots are named in one error that says where to set them."""
        for name in ("MTC_DATA", "BOX_DIR"):
            monkeypatch.delenv(name, raising=False)
        config = _config()

        with pytest.raises(ValueError, match="not set anywhere") as excinfo:
            check_roots_resolved(config, template_namespace(config))

        message = str(excinfo.value)
        assert "MTC_DATA" in message
        assert "BOX_DIR" in message
        assert ".env file beside the project" in message

    def test_nothing_is_raised_once_they_are_set(self, monkeypatch):
        """With every root set, nothing is raised."""
        monkeypatch.setenv("MTC_DATA", "//share/models")
        monkeypatch.setenv("BOX_DIR", "/box")
        config = _config()

        check_roots_resolved(config, template_namespace(config))

    def test_references_are_found_wherever_they_appear(self):
        """References are found at the top level and inside step params."""
        assert _referenced_names(_config()) >= {"MTC_DATA", "BOX_DIR", "survey_dir"}


class TestOnlyReferencedPathsAreChecked:
    """A path no step refers to cannot refuse a run."""

    def test_a_shorthand_a_step_names_is_in_scope(self):
        """A shorthand a step refers to is in scope."""
        assert shorthands_used_by_steps(_config()) == {"survey_dir"}

    def test_a_shorthand_only_a_comment_names_is_not(self):
        """A shorthand named only in a YAML comment is not in scope."""
        config = _config(
            "pums_dir: /nope\nsteps:\n  - name: load\n    params:\n"
            '      # p: "{{ pums_dir }}/x"\n      q: 1'
        )

        assert shorthands_used_by_steps(config) == set()

    def test_a_declared_but_unused_path_is_not_required_to_exist(self):
        """A declared path no step uses need not exist."""
        check_input_paths({"pums_dir": "Z:/nowhere"}, used=set())


class TestMissingInputsFailBeforeTheRun:
    """Missing input directories are reported together, before the run."""

    def test_an_existing_directory_passes(self, tmp_path):
        """An existing directory raises nothing."""
        check_input_paths({"survey_dir": str(tmp_path)}, used={"survey_dir"})

    def test_every_missing_path_is_named_at_once(self):
        """Every missing directory is named in one error."""
        config = {"survey_dir": "Z:/nowhere", "TM2_shapefile_dir": "Z:/still/nowhere"}

        with pytest.raises(ValueError, match=r"director(y|ies) do(es)? not exist") as excinfo:
            check_input_paths(config, used=set(config))

        for key in config:
            assert key in str(excinfo.value)

    def test_output_directories_are_not_required_to_exist(self, tmp_path):
        """``output_dir`` need not exist; it is created on demand."""
        check_input_paths({"output_dir": str(tmp_path / "unmade")}, used={"output_dir"})

    def test_settings_that_are_not_directories_are_ignored(self):
        """Only ``*_dir`` names a directory; the rest are values, not paths."""
        check_input_paths(
            {"state_fips": "06", "log_file": "Z:/nowhere/run.log"},
            used={"state_fips", "log_file"},
        )


class TestAMissingInputDirectoryCanBeCreated:
    """A missing input directory whose parent exists can be created on confirmation."""

    def _answer(self, monkeypatch, reply: str | None) -> list[str]:
        asked: list[str] = []

        def fake_input(prompt):
            asked.append(prompt)
            if reply is None:
                raise EOFError
            return reply

        monkeypatch.setattr("builtins.input", fake_input)
        return asked

    def test_yes_creates_it_and_the_run_goes_on(self, monkeypatch, tmp_path):
        """The user confirms, so the folder is made and nothing is raised."""
        asked = self._answer(monkeypatch, "y")
        path = tmp_path / "census"

        check_input_paths({"census_dir": str(path)}, used={"census_dir"})

        assert path.is_dir()
        assert len(asked) == 1

    @pytest.mark.parametrize("reply", ["n", "", None])
    def test_anything_else_leaves_it_missing(self, monkeypatch, tmp_path, reply):
        """No, a bare Enter, or no one to ask: the directory is not made and the check raises."""
        self._answer(monkeypatch, reply)
        path = tmp_path / "census"

        with pytest.raises(ValueError, match="census_dir"):
            check_input_paths({"census_dir": str(path)}, used={"census_dir"})
        assert not path.exists()

    def test_a_missing_parent_is_not_offered(self, monkeypatch, tmp_path):
        """A wrong root or drive is a mistake to report, not a folder to make."""
        asked = self._answer(monkeypatch, "y")

        with pytest.raises(ValueError, match="census_dir"):
            check_input_paths({"census_dir": str(tmp_path / "no" / "census")}, used={"census_dir"})
        assert asked == []


class TestTheShippedConfigs:
    """The project configs in the repository follow these rules."""

    @pytest.mark.parametrize("project", ["bats_2019", "bats_2023"])
    def test_every_path_is_built_from_a_root(self, project):
        """No ``*_dir`` path is a bare path without a root."""
        raw = _project(project)
        bare = [
            f"{key}: {value}"
            for key, value in raw.items()
            if isinstance(value, str) and key.endswith("_dir") and "{{" not in value
        ]

        assert bare == [], f"paths not built from a root: {bare}"

    @pytest.mark.parametrize("project", ["bats_2019", "bats_2023"])
    def test_example_env_declares_every_root_the_config_needs(self, project):
        """``example.env`` lists every root a config needs that ``ENVS`` does not supply."""
        raw = _project(project)
        roots = {
            name
            for name in _referenced_names(raw)
            if name.isupper() and name not in (raw.get("ENVS") or {})
        }
        example = Path("example.env").read_text(encoding="utf-8")

        undocumented = sorted(root for root in roots if f"{root}=" not in example)
        assert undocumented == [], f"roots missing from example.env: {undocumented}"


class TestShorthandsReferringToShorthands:
    """A shorthand may refer to another shorthand, declared in either order."""

    @staticmethod
    def _load(tmp_path, config: dict) -> dict:
        path = tmp_path / "config.yaml"
        path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
        return Pipeline(config_path=str(path), steps=[]).config

    @pytest.mark.parametrize("target_first", [True, False], ids=["target-first", "reference-first"])
    def test_resolution_does_not_depend_on_declaration_order(self, tmp_path, target_first: bool):
        """Either declaration order gives the same fully expanded result."""
        # Under tmp_path: Pipeline creates the log file's directory on construction.
        survey_dir = str(tmp_path / "survey")
        declarations = [("survey_dir", survey_dir), ("output_dir", "{{ survey_dir }}/out")]
        if not target_first:
            declarations.reverse()

        config = self._load(
            tmp_path,
            dict(
                declarations,
                log_file="{{ output_dir }}/run.log",
                steps=[{"name": "load_data", "params": {"out": "{{ output_dir }}/x.csv"}}],
            ),
        )

        assert config["output_dir"] == f"{survey_dir}/out"
        assert config["log_file"] == f"{survey_dir}/out/run.log"
        assert config["steps"][0]["params"]["out"] == f"{survey_dir}/out/x.csv"

    def test_chained_shorthands_resolve(self):
        """A reference several links deep still bottoms out."""
        resolved = _resolve_variables(
            {
                "root": "M:/data",
                "survey_dir": "{{ root }}/survey",
                "output_dir": "{{ survey_dir }}/analysis",
                "log_file": "{{ output_dir }}/run.log",
            }
        )

        assert resolved["log_file"] == "M:/data/survey/analysis/run.log"

    def test_a_cycle_is_reported_rather_than_spun_on(self):
        """Shorthands referring to each other raise instead of looping forever."""
        with pytest.raises(ValueError, match="cycle"):
            _resolve_variables({"a": "{{ b }}/x", "b": "{{ a }}/y"})

    def test_unknown_reference_is_left_alone(self):
        """A reference to something undeclared is passed through untouched."""
        assert _resolve_variables({"a": "{{ nowhere }}/x"})["a"] == "{{ nowhere }}/x"
