"""Decorators for pipeline steps with automatic validation."""

import functools
import inspect
import logging
import typing
from collections.abc import Callable, Iterator, Mapping
from typing import Any

import polars as pl
from pydantic import BaseModel

from data_canon.core.dataclass import CanonicalData
from pipeline.step_registry import STEP_REGISTRY, StepContract, is_config_named, register_step

logger = logging.getLogger(__name__)

# Arguments the pipeline supplies itself; never settings.
PIPELINE_ARGS = frozenset(
    {"canonical_data", "tables", "validate_input", "validate_output", "kwargs"}
)
TABLE_NAMES = frozenset(CanonicalData().TABLE_NAMES)


class Tables(Mapping[str, pl.DataFrame]):
    """The tables a step receives: those its contract declares, as passed.

    Reading a table the contract does not declare raises, so a step cannot read
    more than its contract says. An optional table that is absent is not in the
    mapping; ``tables.get(name)`` returns None for it.
    """

    def __init__(
        self, step_name: str, declared: set[str], present: dict[str, pl.DataFrame]
    ) -> None:
        """Hold *present*, the passed tables, out of the *declared* ones."""
        self._step = step_name
        self._declared = declared
        self._present = present

    def _undeclared(self, name: str) -> KeyError:
        return KeyError(f"Step '{self._step}' reads '{name}', which its contract does not declare.")

    def __getitem__(self, name: str) -> pl.DataFrame:
        """The table *name*; raises if undeclared or absent."""
        if name in self._present:
            return self._present[name]
        if name not in self._declared:
            raise self._undeclared(name)
        msg = f"Step '{self._step}': optional table '{name}' is absent; use tables.get()."
        raise KeyError(msg)

    def get(self, name: str, default: pl.DataFrame | None = None) -> pl.DataFrame | None:  # type: ignore[override]
        """The table *name*, or *default* if it is optional and absent."""
        if name not in self._declared:
            raise self._undeclared(name)
        return self._present.get(name, default)

    def __contains__(self, name: object) -> bool:
        """Whether *name* was passed; raises if undeclared."""
        if name not in self._declared:
            raise self._undeclared(str(name))
        return name in self._present

    def __iter__(self) -> Iterator[str]:
        """The present tables' names."""
        return iter(self._present)

    def __len__(self) -> int:
        """How many tables are present."""
        return len(self._present)


def settings_models(func: Callable[..., Any]) -> dict[str, type[BaseModel]]:
    """The step's arguments annotated with a pydantic model, by argument name."""
    hints = typing.get_type_hints(inspect.unwrap(func))
    return {
        name: hint
        for name, hint in hints.items()
        if isinstance(hint, type) and issubclass(hint, BaseModel)
    }


def field_owners(func: Callable[..., Any]) -> dict[str, str]:
    """Each settings-model field name, mapped to the argument whose model holds it.

    A flat config key is routed by this map, so every name must have one owner.

    Raises:
        TypeError: If a field name is also another model's field, a step
            argument, a canonical table or a pipeline argument.
    """
    params = set(inspect.signature(func).parameters)
    reserved = PIPELINE_ARGS | TABLE_NAMES
    owners: dict[str, str] = {}
    for arg, model in settings_models(func).items():
        for name in model.model_fields:
            clash = (
                f"another model's field ({owners[name]})"
                if name in owners
                else "a step argument"
                if name in params
                else "a table or pipeline argument"
                if name in reserved
                else None
            )
            if clash:
                msg = (
                    f"{func.__name__}: {model.__name__}.{name} has the same name as "
                    f"{clash}, so a config key '{name}' would have two owners."
                )
                raise TypeError(msg)
            owners[name] = arg
    return owners


def step(
    *,
    validate_input: bool = True,
    validate_output: bool = False,
    requires: dict[str, set[str]] | None = None,
    produces: dict[str, set[str]] | None = None,
    optional: dict[str, set[str]] | None = None,
    generated_columns: Callable[..., dict[str, Any]] | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator for pipeline steps with automatic validation.

    A step's contract names the tables it takes and the columns it reads:

    - ``requires``: tables the step takes, each with the columns it reads.
    - ``optional``: tables the step takes that may be absent, likewise.
    - ``produces``: mapping of table name to set of field names that the
      step is responsible for after execution (newly created or updated).

    The step receives them as one ``tables`` argument; callers pass them by
    name. Before the step runs, the decorator raises if a required table is
    missing, a listed column is absent, or a table it does not take is passed.
    This always runs; ``validate_input`` adds row validation against the models.

    A step whose output columns are named from its settings, such as one per
    configured zone or profile, says which with ``generated_columns``: a function in the
    step's own module that takes some of the step's settings, by name, and
    returns ``{table: columns}`` in any form
    [`register_generated_columns`][data_canon.core.dataclass.CanonicalData.register_generated_columns]
    accepts. The decorator registers them on every call, so ``write_data`` keeps
    them.

    The default value is to only validate inputs to avoid duplicate validation.
    Recommend putting a final_check step at the end of the pipeline
    to validate all tables after all processing is complete.

    Args:
        validate_input: Whether to validate inputs. Defaults to True.
        validate_output: Whether to validate outputs. Defaults to False.
        requires: Tables the step takes, each with the columns it reads.
        produces: Mapping of table name to set of produced output field names.
        optional: Tables the step takes that may be absent, each with the
            columns it reads when present.
        generated_columns: Function from some of the step's settings to the config-named
            columns it writes, per table.

    Example:
        >>> @step(
        ...     requires={"unlinked_trips": {"person_day_id", "depart_time"}},
        ...     produces={"linked_trips": {"linked_trip_id"}},
        ... )
        ... def link_trips(
        ...     tables: Tables,
        ...     max_dwell_time: float,  # a setting, from the step's params
        ... ) -> dict[str, pl.DataFrame]:
        ...     unlinked_trips = tables["unlinked_trips"]
        ...     linked_trips = ...
        ...     return {"linked_trips": linked_trips}

        >>> @step(validate_input=False)
        ... def load_data(input_paths: dict) -> dict[str, pl.DataFrame]:
        ...     return {
        ...         "households": households_df,
        ...         "persons": persons_df,
        ...         ...
        ...     }

    Returns:
        Decorated function with validation
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        # Register workflow contract at decoration (import) time
        register_step(func.__name__, requires=requires, produces=produces, optional=optional)
        contract = STEP_REGISTRY[func.__name__]
        _check_signature(func, contract)
        field_owners(func)  # a name clash fails here, not on a pipeline run

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
            if args:
                msg = f"Step '{func.__name__}' takes keyword arguments only."
                raise TypeError(msg)
            # Extract validation flags
            should_validate_input = kwargs.pop("validate_input", validate_input)
            should_validate_output = kwargs.pop("validate_output", validate_output)

            # Only pop canonical_data if function doesn't expect it
            sig = inspect.signature(func)
            if "canonical_data" in sig.parameters:
                canonical_data: CanonicalData = kwargs.get("canonical_data", CanonicalData())
            else:
                canonical_data: CanonicalData = kwargs.pop("canonical_data", CanonicalData())

            tables = _take_tables(func.__name__, contract, kwargs)
            if contract.taken:
                kwargs["tables"] = tables

            _register_generated(generated_columns, func, kwargs, canonical_data)

            # A step that takes a flag itself (write_data) gets the value in force,
            # whether the caller passed it or the decorator default applies.
            resolved = {
                "validate_input": should_validate_input,
                "validate_output": should_validate_output,
            }
            kwargs.update({key: value for key, value in resolved.items() if key in sig.parameters})

            if should_validate_input:
                _validates(func.__name__, tables, canonical_data)

            result = func(**kwargs)

            # Update canonical_data with results if available
            if canonical_data is not None and isinstance(result, dict):
                _update_canonical_data(canonical_data, result)

            if should_validate_output and isinstance(result, dict):
                _validate_dict_outputs(result, func.__name__, canonical_data)

            return result

        wrapper.contract = contract  # type: ignore[attr-defined]
        return wrapper  # type: ignore[return-value]

    return decorator


def _check_signature(func: Callable, contract: StepContract) -> None:
    """Raise unless *func* takes ``tables`` exactly when its contract declares any.

    Raises:
        TypeError: If an argument is named after a table, or ``tables`` and the
            contract disagree.
    """
    params = inspect.signature(func).parameters
    named = sorted(TABLE_NAMES & set(params))
    if named:
        msg = f"{func.__name__}: takes {named} as arguments; take them through `tables`."
        raise TypeError(msg)
    if bool(contract.taken) != ("tables" in params):
        msg = (
            f"{func.__name__}: a step takes `tables` exactly when its requires or "
            "optional names a table."
        )
        raise TypeError(msg)


def _take_tables(step_name: str, contract: StepContract, kwargs: dict) -> Tables:
    """Pop the passed tables from *kwargs* and check them against *contract*.

    Raises:
        TypeError: If a table the step does not take is passed.
        ValueError: Naming every required table that is missing, and every
            listed column that is absent.
    """
    taken = contract.taken
    passed = {k: kwargs.pop(k) for k in list(kwargs) if k in TABLE_NAMES or k in taken}
    undeclared = sorted(set(passed) - set(taken))
    if undeclared:
        msg = f"Step '{step_name}' was passed {undeclared}, which its contract does not declare."
        raise TypeError(msg)

    problems = []
    for name, tc in taken.items():
        df = passed.get(name)
        if df is None:
            if not tc.optional:
                problems.append(f"{name}: missing")
            continue
        absent = sorted(c for c in tc.requires if not is_config_named(c) and c not in df.columns)
        if absent:
            problems.append(f"{name}: missing columns {absent}")
    if problems:
        msg = f"Step '{step_name}' cannot run:\n" + "\n".join(f"  - {p}" for p in problems)
        raise ValueError(msg)

    present = {k: v for k, v in passed.items() if v is not None}
    return Tables(step_name, set(taken), present)


def _register_generated(
    generated_columns: Callable[..., dict[str, Any]] | None,
    func: Callable,
    kwargs: dict,
    canonical_data: CanonicalData,
) -> None:
    """Register the config-named columns *generated_columns* lists for this call's settings."""
    if generated_columns is None:
        return
    bound = inspect.signature(func).bind_partial(**kwargs)
    bound.apply_defaults()
    wanted = inspect.signature(generated_columns).parameters
    for table, cols in generated_columns(
        **{k: v for k, v in bound.arguments.items() if k in wanted}
    ).items():
        canonical_data.register_generated_columns(table, cols)


def _update_canonical_data(
    canonical_data: CanonicalData,
    result: dict[str, pl.DataFrame],
) -> None:
    """Update canonical_data instance with result DataFrames."""
    for key, value in result.items():
        if key in canonical_data.models and isinstance(value, pl.DataFrame):
            logger.info("Updating canonical_data with output '%s'", key)
        else:
            logger.warning(
                "Output '%s' is not a canonical table. This cannot be validated automatically.",
                key,
            )
        # Add to canonical_data instance either way
        setattr(canonical_data, key, value)


def _validates(step_name: str, tables: Tables, canonical_data: CanonicalData) -> None:
    """Row-validate each table the step received against its model.

    Every table is attached before any is validated, so a validator reading
    several tables sees all of them whatever the order.
    """
    for name, df in tables.items():
        setattr(canonical_data, name, df)
    for name in tables:
        canonical_data.validate(name, step=step_name)


def _validate_dict_outputs(
    result: dict,
    func_name: str,
    canonical_data: CanonicalData,
) -> None:
    """Validate outputs in dict format.

    An output that is not a canonical table was already warned about when it
    was stored on ``canonical_data``, so it is skipped here without a word.
    """
    for key, value in result.items():
        if key not in canonical_data.models and isinstance(value, pl.DataFrame):
            continue

        logger.info(
            "Validating output '%s' from step '%s'",
            key,
            func_name,
        )
        # Validate using canonical_data instance
        # Data already updated by wrapper, just validate
        canonical_data.validate(key, step=func_name)
