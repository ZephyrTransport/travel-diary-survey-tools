"""Paths are built from roots, and a root says where a share is on this machine.

Issue #100. A config that names ``M:/Data/...`` only runs where someone has
mapped ``M:``. Drive letters are a convention rather than a fact, and Linux has
none, so a shared config cannot hold every machine's answer.

The split is between the root and everything under it. ``/Data/HomeInterview/Bay
Area Travel Study 2023/...`` is shared knowledge and belongs in the config where
it can be reviewed; where that share is mounted is local and belongs in a
gitignored ``.env``. So a config says ``{{ MTC_DATA }}/Data/...``, which is true
on every machine, and nothing is overridden -- the config never claims to know
where the share is.

The alternative, letting the environment silently replace a whole configured
path, was rejected: the config would state one path while the run used another,
and the run's own saved copy of "the config used" would record the wrong one.
"""

from pathlib import Path

import pytest
import yaml

from pipeline.pipeline import (
    _check_input_paths,
    _check_roots_resolved,
    _referenced_names,
    _resolve_variables,
    _shorthands_used_by_steps,
    _template_namespace,
)

ROOTED = """
survey_dir: "{{ MTC_DATA }}/Data/HomeInterview/x"
output_dir: "{{ BOX_DIR }}/out"
log_file: "{{ output_dir }}/run.log"
steps:
  - name: load
    params:
      input: "{{ survey_dir }}/hh.csv"
"""


def _config(text: str = ROOTED) -> dict:
    return yaml.safe_load(text)


def _project(name: str) -> dict:
    return yaml.safe_load((Path("projects") / name / "config.yaml").read_text(encoding="utf-8"))


class TestOneNamespace:
    """Roots, ``ENVS`` defaults and the config's own keys all feed the templating.

    Not a second mechanism beside the substitution that already exists -- the
    same one, with more sources -- which is why ``{{ output_dir }}`` keeps
    working unchanged.
    """

    def test_a_root_comes_from_the_environment(self, monkeypatch):
        """The machine-specific half, and the only half that comes from outside."""
        monkeypatch.setenv("MTC_DATA", "//share/models")

        assert _template_namespace(_config())["MTC_DATA"] == "//share/models"

    def test_the_configs_own_keys_are_still_available(self):
        """``log_file: "{{ output_dir }}/run.log"`` predates roots and must survive."""
        assert "output_dir" in _template_namespace(_config())

    def test_an_envs_block_supplies_a_committed_default(self):
        """For a root that genuinely is the same everywhere."""
        config = _config('ENVS:\n  SHARED: "/opt/shared"\nd: "{{ SHARED }}/x"')

        assert _template_namespace(config)["SHARED"] == "/opt/shared"

    def test_the_environment_beats_a_committed_default(self, monkeypatch):
        """The block is a default; a machine is the authority on its own mounts."""
        monkeypatch.setenv("SHARED", "/mnt/elsewhere")
        config = _config('ENVS:\n  SHARED: "/opt/shared"\nd: "{{ SHARED }}/x"')

        assert _template_namespace(config)["SHARED"] == "/mnt/elsewhere"

    def test_an_empty_variable_is_not_an_instruction(self, monkeypatch):
        """An exported-but-empty variable is a shell accident, not a value."""
        monkeypatch.setenv("SHARED", "")
        config = _config('ENVS:\n  SHARED: "/opt/shared"\nd: "{{ SHARED }}/x"')

        assert _template_namespace(config)["SHARED"] == "/opt/shared"

    def test_roots_resolve_through_to_a_full_path(self, monkeypatch):
        """The whole point: a real path, assembled from both halves."""
        monkeypatch.setenv("MTC_DATA", "//share/models")
        monkeypatch.setenv("BOX_DIR", "/box")

        resolved = _resolve_variables(_template_namespace(_config()))

        assert resolved["survey_dir"] == "//share/models/Data/HomeInterview/x"
        assert resolved["log_file"] == "/box/out/run.log"

    def test_a_malformed_envs_block_is_refused(self):
        """A scalar where a mapping belongs would silently contribute nothing."""
        with pytest.raises(TypeError, match="must be a mapping"):
            _template_namespace(_config("ENVS: not-a-mapping\nd: x"))


class TestAnUnsetRootIsNamed:
    """Left unresolved it renders literally and fails later as a baffling path."""

    def test_a_missing_root_raises(self, monkeypatch):
        """Rather than rendering `{{ MTC_DATA }}/Data/...` and failing on open()."""
        for name in ("MTC_DATA", "BOX_DIR"):
            monkeypatch.delenv(name, raising=False)
        config = _config()

        with pytest.raises(ValueError, match="not set anywhere"):
            _check_roots_resolved(config, _template_namespace(config))

    def test_every_missing_root_is_named_at_once(self, monkeypatch):
        """Otherwise it is one root per attempt: set, re-run, discover the next."""
        for name in ("MTC_DATA", "BOX_DIR"):
            monkeypatch.delenv(name, raising=False)
        config = _config()

        with pytest.raises(ValueError, match="not set anywhere") as excinfo:
            _check_roots_resolved(config, _template_namespace(config))

        assert "MTC_DATA" in str(excinfo.value)
        assert "BOX_DIR" in str(excinfo.value)

    def test_the_error_says_where_to_set_them(self):
        """Naming the problem without the remedy just relocates the guessing."""
        config = _config('d: "{{ NOWHERE }}/x"')

        with pytest.raises(ValueError, match=r"\.env file beside the project"):
            _check_roots_resolved(config, _template_namespace(config))

    def test_nothing_is_raised_once_they_are_set(self, monkeypatch):
        """A correct setup must be silent, or the check gets worked around."""
        monkeypatch.setenv("MTC_DATA", "//share/models")
        monkeypatch.setenv("BOX_DIR", "/box")
        config = _config()

        _check_roots_resolved(config, _template_namespace(config))

    def test_references_are_found_wherever_they_appear(self):
        """Including inside step params, which is where most of them are."""
        assert _referenced_names(_config()) >= {"MTC_DATA", "BOX_DIR", "survey_dir"}


class TestOnlyReferencedPathsAreChecked:
    """A path nothing reads must not be able to refuse a run.

    bats_2023 declares ``pums_dir`` and names it from commented-out lines only.
    YAML drops comments before parsing, so a shorthand is in scope exactly when
    a step still refers to it.
    """

    def test_a_shorthand_a_step_names_is_in_scope(self):
        """A live reference is what puts a path in scope."""
        assert _shorthands_used_by_steps(_config()) == {"survey_dir"}

    def test_a_shorthand_only_a_comment_names_is_not(self):
        """This is what stops `pums_dir` refusing a bats_2023 run."""
        config = _config(
            "pums_dir: /nope\nsteps:\n  - name: load\n    params:\n"
            '      # p: "{{ pums_dir }}/x"\n      q: 1'
        )

        assert _shorthands_used_by_steps(config) == set()

    def test_a_declared_but_unused_path_is_not_required_to_exist(self):
        """Declared, referenced by nothing, missing on disk -- and still fine."""
        _check_input_paths({"pums_dir": "Z:/nowhere"}, used=set())


class TestMissingInputsFailBeforeTheRun:
    """Reported at the start, together, rather than one per wasted run."""

    def test_an_existing_directory_passes(self, tmp_path):
        """A correct config must be silent, or the check gets bypassed."""
        _check_input_paths({"survey_dir": str(tmp_path)}, used={"survey_dir"})

    def test_a_missing_directory_is_reported(self):
        """A wrong path used to surface eight steps into a run."""
        with pytest.raises(ValueError, match=r"director(y|ies) do(es)? not exist"):
            _check_input_paths({"survey_dir": "Z:/nowhere/at/all"}, used={"survey_dir"})

    def test_every_missing_path_is_named_at_once(self):
        """Otherwise the fix-run-wait loop repeats once per wrong path."""
        config = {"survey_dir": "Z:/nowhere", "TM2_shapefile_dir": "Z:/still/nowhere"}

        with pytest.raises(ValueError, match="not exist") as excinfo:
            _check_input_paths(config, used=set(config))

        for key in config:
            assert key in str(excinfo.value)

    def test_output_directories_are_not_required_to_exist(self, tmp_path):
        """They are created on demand; demanding them up front would be wrong."""
        _check_input_paths({"output_dir": str(tmp_path / "unmade")}, used={"output_dir"})

    def test_settings_that_are_not_directories_are_ignored(self):
        """Only ``*_dir`` names a directory; the rest are values, not paths."""
        _check_input_paths(
            {"state_fips": "06", "log_file": "Z:/nowhere/run.log"},
            used={"state_fips", "log_file"},
        )


class TestTheShippedConfigs:
    """What the repository actually contains has to satisfy all of this."""

    @pytest.mark.parametrize("project", ["bats_2019", "bats_2023"])
    def test_every_path_is_built_from_a_root(self, project):
        """A bare drive letter is the thing this change exists to remove."""
        raw = _project(project)
        bare = [
            f"{key}: {value}"
            for key, value in raw.items()
            if isinstance(value, str) and key.endswith("_dir") and "{{" not in value
        ]

        assert bare == [], f"paths not built from a root: {bare}"

    @pytest.mark.parametrize("project", ["bats_2019", "bats_2023"])
    def test_example_env_declares_every_root_the_config_needs(self, project):
        """Otherwise the failure names a root nobody has been told how to set."""
        raw = _project(project)
        roots = {
            name
            for name in _referenced_names(raw)
            if name.isupper() and name not in (raw.get("ENVS") or {})
        }
        example = Path("example.env").read_text(encoding="utf-8")

        undocumented = sorted(root for root in roots if f"{root}=" not in example)
        assert undocumented == [], f"roots missing from example.env: {undocumented}"
