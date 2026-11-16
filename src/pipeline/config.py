"""Reading a pipeline config: ``{{ name }}`` templates, and what they resolve to.

A config names its data locations through templates over top-level variables,
the committed ``ENVS`` defaults and the environment. These helpers resolve them
and check, before any step runs, that every root a step needs is set and every
input path it names exists.
"""

import logging
import os
import re
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


# Enough passes to resolve any sane chain of shorthands referring to shorthands,
# and few enough that a cycle is reported rather than spun on.
MAX_TEMPLATE_PASSES = 10

#: Config key holding committed root defaults. Anything declared here is a
#: default the environment may replace, not a claim about this machine.
ENVS_KEY = "ENVS"


def _substitute(text: str, variables: dict[str, str]) -> str:
    """Replace every ``{{ name }}`` in one string with its value."""
    for name, value in variables.items():
        text = text.replace(f"{{{{ {name} }}}}", str(value))
    return text


def resolve_templates(config: dict[str, Any]) -> dict[str, Any]:
    """Replace every ``{{ name }}`` in a loaded config with its value.

    The values come from the config's own variables and the environment, as
    ``template_namespace`` gathers them. A root no one set stays a template.
    """
    variables = _resolve_variables(template_namespace(config))

    def replace(obj: Any) -> Any:  # noqa: ANN401
        if isinstance(obj, str):
            return _substitute(obj, variables)
        if isinstance(obj, dict):
            return {k: replace(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [replace(item) for item in obj]
        return obj

    return replace(config)


def template_namespace(config: dict[str, Any]) -> dict[str, str]:
    """Everything a ``{{ name }}`` may refer to, lowest precedence first.

    Three sources feed one namespace, which is what keeps this from being a
    second mechanism bolted beside the templating that already exists:

    1. the config's own top-level strings -- ``output_dir`` and friends, so
       ``log_file: "{{ output_dir }}/run.log"`` keeps working;
    2. committed defaults under ``ENVS``, for a root that genuinely is the same
       everywhere;
    3. the environment, including a ``.env`` the runners load, which is where a
       machine says where it mounts a shared location.

    The environment wins, and that is not an override of anything stated as
    fact: a config says ``{{ MTC_DATA }}/Data/...``, which is true wherever
    MTC_DATA points. Only the root is machine-specific, and only the root comes
    from outside. The structure beneath it is shared knowledge and stays in the
    config where it can be reviewed.

    Args:
        config: The raw configuration, before substitution.

    Returns:
        Mapping of template name to value.
    """
    namespace = {key: value for key, value in config.items() if isinstance(value, str)}

    declared = config.get(ENVS_KEY) or {}
    if not isinstance(declared, dict):
        msg = (
            f"'{ENVS_KEY}' must be a mapping of root name to value, got {type(declared).__name__}."
        )
        raise TypeError(msg)
    namespace.update({str(k): str(v) for k, v in declared.items()})

    # The environment wins, and also supplies a root referenced but declared nowhere
    for name in [*namespace, *(_referenced_names(config) - set(namespace))]:
        if value := os.environ.get(name):
            namespace[name] = value

    return namespace


def _referenced_names(config: dict[str, Any]) -> set[str]:
    """Every ``{{ name }}`` appearing anywhere in the config."""
    return set(re.findall(r"\{\{\s*(\w+)\s*\}\}", yaml.safe_dump(config)))


def check_roots_resolved(config: dict[str, Any], namespace: dict[str, str]) -> None:
    """Fail if a referenced root is declared nowhere, naming it.

    Left unresolved, ``{{ MTC_DATA }}`` renders literally and surfaces later as
    a baffling "no such file or directory" containing brace characters. The
    root is missing, and saying so is more useful than anything downstream can
    work out.

    Raises:
        ValueError: Naming every unresolved root and where to set it.
    """
    missing = sorted(name for name in _referenced_names(config) if name not in namespace)
    if not missing:
        return
    listed = "\n  - ".join(missing)
    msg = (
        f"The config refers to root(s) that are not set anywhere:\n  - {listed}"
        f"\n\nA root says where a shared location is mounted on this machine, so it "
        f"cannot be committed. Set each one in a .env file beside the project (see "
        f"example.env), or in your environment. Everything below the root stays in the "
        f"config and is the same for everyone."
    )
    raise ValueError(msg)


def shorthands_used_by_steps(raw_config: dict[str, Any]) -> set[str]:
    """Which shorthands the configured steps actually refer to.

    Read off the *unsubstituted* steps, so a shorthand is only in scope when a
    step names it as ``{{ key }}``. A commented-out parameter is not a
    reference: YAML drops comments before this sees them, which is what makes
    the distinction free. A shorthand declared but used only by commented lines
    is not required to exist, so a run that never reads it still starts.
    """
    rendered = yaml.safe_dump(raw_config.get("steps", []))
    return {key for key in raw_config if isinstance(key, str) and f"{{{{ {key} }}}}" in rendered}


def check_input_paths(config: dict[str, Any], used: set[str]) -> None:
    """Fail before the first step if an input directory a step needs is missing.

    Every one of these is read by some step, but never the first one, so a wrong
    drive letter surfaces minutes into a run rather than at the start -- and one
    path at a time: fix, re-run, wait, discover the next. A wrong
    ``TM2_shapefile_dir`` was found this way, eight steps in, after the survey
    had been read, trips linked, tours built and weights computed.

    Only inputs a configured step refers to are checked. Output directories are
    created on demand, and a path nothing reads is not this function's business.
    A missing input directory whose parent exists is offered for creation, since
    some steps download their inputs into it; with no one to answer, it is missing.

    Args:
        config: The fully substituted configuration.
        used: Shorthands the steps refer to, from
            :func:`shorthands_used_by_steps`.

    Raises:
        ValueError: Naming every missing path at once, and how to redirect them.
    """
    missing = [
        f"{key}: {value}"
        for key, value in sorted(config.items())
        if key in used
        and isinstance(value, str)
        and key.endswith("_dir")
        and not key.startswith("output")
        and "{{" not in value  # unresolved: the template error covers it
        and not Path(value).exists()
        and not _created_on_confirmation(key, Path(value))
    ]
    if not missing:
        return

    plural = "ies do" if len(missing) > 1 else "y does"
    listed = "\n  - ".join(missing)
    msg = (
        f"Configured input director{plural} not exist on this machine:\n  - {listed}"
        f"\n\nChecked before the pipeline starts, so a wrong path costs a second "
        "rather than a run. Point each one somewhere real by setting an environment "
        "variable of the same name -- a .env file beside the project is read "
        "automatically -- which keeps machine-specific paths out of the shared config."
    )
    raise ValueError(msg)


def _created_on_confirmation(key: str, path: Path) -> bool:
    """Ask whether to create a missing input directory, and create it if told to."""
    if not path.parent.is_dir():
        return False
    try:
        answer = input(f"{key} does not exist: {path}\nCreate it? [y/N] ")
    except (EOFError, OSError):  # no one to answer: not interactive, or under pytest
        return False
    if answer.strip().lower() not in {"y", "yes"}:
        return False
    path.mkdir()
    logger.info("Created %s", path)
    return True


def _resolve_variables(variables: dict[str, str]) -> dict[str, str]:
    """Expand the shorthands against each other before anything else uses them.

    Config shorthands routinely refer to other shorthands
    (``output_dir: "{{ survey_dir }}/analysis"``). Substituting in a single pass
    resolves such a reference only when its target happens to be declared later
    in the file, because the pass has already moved past an earlier one -- so
    the same config works or silently emits a literal ``{{ survey_dir }}`` path
    depending on the order its keys were written in. Resolving the shorthands
    to a fixed point first makes declaration order irrelevant.

    Args:
        variables: Top-level string values from the config, unexpanded.

    Returns:
        The same mapping with every nested reference expanded.

    Raises:
        ValueError: A reference could not be resolved within
            ``MAX_TEMPLATE_PASSES``, which means the shorthands refer to each
            other in a cycle.
    """
    resolved = dict(variables)
    for _ in range(MAX_TEMPLATE_PASSES):
        expanded = {key: _substitute(value, resolved) for key, value in resolved.items()}
        if expanded == resolved:
            return resolved
        resolved = expanded

    unresolved = sorted(key for key, value in resolved.items() if "{{" in value)
    msg = (
        f"Config variables still reference each other after {MAX_TEMPLATE_PASSES} "
        f"passes: {unresolved}. They likely refer to each other in a cycle."
    )
    raise ValueError(msg)
