"""MkDocs hook for step pages: generated settings tables, and checked YAML examples.

A ``<!-- step-settings: <step> -->`` line becomes a table of the keys the step's
``params:`` accepts: its plain arguments, then one table per settings model (and
per model nested in one). A fenced ``yaml`` block titled ``Example`` is run through
``Pipeline.parse_step_args``, the check a real config gets, so an example with a
misspelt, unknown or missing key fails the build.

A ``<!-- weighting-controls -->`` line becomes the table of control names that
``compute_weights``' ``controls:`` takes, read from the control registry.
"""

import importlib
import inspect
import pkgutil
import re
import types
import typing
from collections.abc import Callable
from enum import Enum
from functools import cache

import yaml
from griffe import Docstring
from mkdocs.exceptions import PluginError
from pydantic import BaseModel
from pydantic_core import PydanticUndefined

import processing
from data_canon.core.dataclass import CanonicalData
from pipeline.decoration import PIPELINE_ARGS, settings_models
from pipeline.pipeline import Pipeline
from pipeline.step_registry import STEP_REGISTRY

SETTINGS_MARKER = re.compile(r"^<!-- step-settings: (\w+) -->$", re.MULTILINE)
CONTROLS_MARKER = "<!-- weighting-controls -->"
EXAMPLE_BLOCK = re.compile(
    r'^```yaml[^\n]*title="Example[^"]*"[^\n]*\n(.*?)^```', re.DOTALL | re.MULTILINE
)

NAMES = {float: "number", int: "whole number", bool: "true / false", str: "text", dict: "mapping"}

# A default longer than this shows as "built in" rather than stretching its table cell.
MAX_DEFAULT = 40


@cache
def step_functions() -> dict[str, Callable]:
    """Each registered step's function, found by importing every processing module."""
    found: dict[str, Callable] = {}
    for module in pkgutil.walk_packages(processing.__path__, "processing."):
        for name, obj in vars(importlib.import_module(module.name)).items():
            if name in STEP_REGISTRY and callable(obj):
                found.setdefault(name, obj)
    return found


def step_function(name: str, page: str) -> Callable:
    """The step called *name*, or a build error naming the page."""
    if name not in step_functions():
        msg = f"{page}: no step named '{name}'"
        raise PluginError(msg)
    return step_functions()[name]


def type_text(t: object) -> str:
    """A type as a YAML author reads it."""
    origin, args = typing.get_origin(t), typing.get_args(t)
    if origin is typing.Literal:
        return " or ".join(f"`{a}`" for a in args)
    if origin in (typing.Union, types.UnionType):
        parts = dict.fromkeys(type_text(a) for a in args if a is not type(None))
        return " or ".join(parts) + (" or null" if type(None) in args else "")
    if origin is dict:
        return "mapping"
    if (
        origin is not None
        and origin is not type
        and issubclass(origin, (list, tuple, set, typing.Sequence))
    ):
        return "list"
    if isinstance(t, type) and t.__name__ == "Path":
        return "path"
    return NAMES.get(t, getattr(t, "__name__", str(t)))


def bounds_text(metadata: list) -> str:
    """A field's numeric bounds, such as ``≥ 0, < 1``."""
    signs = (("ge", "≥"), ("gt", ">"), ("le", "≤"), ("lt", "<"))
    return ", ".join(
        f"{sign}&nbsp;{getattr(m, attr)}"
        for m in metadata
        for attr, sign in signs
        if getattr(m, attr, None) is not None
    )


def yaml_value(value: object) -> object:
    """*value* with enum members as their names, ready for yaml.safe_dump."""
    if isinstance(value, Enum):
        return value.name
    if isinstance(value, dict):
        return {yaml_value(k): yaml_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [yaml_value(v) for v in value]
    return value


def default_text(value: object) -> str:
    """A default as YAML would write it; **required** when there is none, *built in* when long."""
    if value is PydanticUndefined or value is inspect.Parameter.empty:
        return "**required**"
    text = yaml.safe_dump(yaml_value(value), default_flow_style=True, width=1_000).strip()
    text = text.removesuffix("\n...").removesuffix("...").strip()
    return f"`{text}`" if len(text) <= MAX_DEFAULT else "*built in*"


def cell(text: str | None) -> str:
    """Text on one table line."""
    return " ".join((text or "").split()).replace("|", "\\|")


def table(rows: list[tuple[str, str, str, str]]) -> str:
    """A settings table, styled by step_pages.css."""
    lines = ["| Key | Type | Default | Meaning |", "|---|---|---|---|"]
    lines += [
        f"| `{key}` | {kind} | {default} | {cell(meaning)} |"
        for key, kind, default, meaning in rows
    ]
    return '<div class="settings-table" markdown>\n\n' + "\n".join(lines) + "\n\n</div>"


def model_tables(model: type[BaseModel], path: str) -> list[str]:
    """One table for *model*, then one for each model nested in it, headed by its YAML path."""
    rows, nested = [], []
    for key, field in model.model_fields.items():
        kind = type_text(field.annotation)
        if isinstance(field.annotation, type) and issubclass(field.annotation, BaseModel):
            nested.append((key, field.annotation))
            kind = f"block (see `{f'{path}.' if path else ''}{key}`)"
        bounds = bounds_text(field.metadata)
        rows.append(
            (
                key,
                kind + (f", {bounds}" if bounds else ""),
                default_text(field.get_default(call_default_factory=True)),
                field.description,
            )
        )
    out = ([f"### `{path}`", ""] if path else []) + [table(rows), ""]
    for key, sub in nested:
        out += model_tables(sub, f"{path}.{key}" if path else key)
    return out


def settings_section(name: str, func: Callable) -> str:
    """The settings tables for one step, in the order ``parse_step_args`` reads them."""
    func = inspect.unwrap(func)
    models = settings_models(func)
    hints = typing.get_type_hints(func)
    tables = {"canonical_data", *CanonicalData().TABLE_NAMES}
    parsed = Docstring(func.__doc__ or "").parse("google", warnings=False)
    described = {
        a.name: a.description for s in parsed if s.kind.value == "parameters" for a in s.value
    }

    plain = [
        (
            arg,
            type_text(hints.get(arg, param.annotation)),
            default_text(param.default),
            described.get(arg, "").split("\n\n")[0],
        )
        for arg, param in inspect.signature(func).parameters.items()
        if arg not in PIPELINE_ARGS | tables | set(models)
        and param.kind not in (param.VAR_KEYWORD, param.VAR_POSITIONAL)
    ]
    if not plain and not models:
        return f"`{name}` takes no settings."
    out = [table(plain), ""] if plain else []
    for arg, model in models.items():
        out += model_tables(model, arg if len(models) > 1 else "")
    if len(models) > 1:
        out.append(
            "A block's keys may also sit flat under `params:`; see "
            "[Configuring a step][step-config-rules]."
        )
    return "\n".join(out)


def check_example(text: str, page: str) -> None:
    """Run each step in an example through the pipeline's own parameter check."""
    steps = yaml.safe_load(text)
    for step in steps if isinstance(steps, list) else [steps]:
        func = step_function(step["name"], page)
        stand_in = types.SimpleNamespace(config={"steps": [step]}, data=CanonicalData())
        try:
            Pipeline.parse_step_args(stand_in, step["name"], func)
        except Exception as e:
            msg = f"{page}: the '{step['name']}' example does not pass the pipeline's check: {e}"
            raise PluginError(msg) from e


def controls_table() -> str:
    """The registered weighting controls, with category names as a ``merge:`` writes them."""
    from processing.weighting.controls.registry import CONTROLS  # noqa: PLC0415

    lines = ["| Name | Level | Measures | Categories |", "|---|---|---|---|"]
    lines += [
        f"| `{name}` | {control.level} | {control.description} | "
        + ", ".join(f"`{member.lower()}`" for _, member in control.valid_members)
        + " |"
        for name, control in CONTROLS.items()
    ]
    return '<div class="settings-table" markdown>\n\n' + "\n".join(lines) + "\n\n</div>"


def on_page_markdown(markdown: str, page, **_: object) -> str:  # noqa: ANN001
    """Check the page's examples, then fill in its settings and controls tables."""
    for block in EXAMPLE_BLOCK.finditer(markdown):
        check_example(block.group(1), page.file.src_uri)
    markdown = SETTINGS_MARKER.sub(
        lambda m: settings_section(m.group(1), step_function(m.group(1), page.file.src_uri)),
        markdown,
    )
    return markdown.replace(CONTROLS_MARKER, controls_table())
