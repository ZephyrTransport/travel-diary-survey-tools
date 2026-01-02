"""Decorators for pipeline steps with automatic validation and caching."""

import functools
import inspect
import logging
from collections.abc import Callable
from typing import Any

import polars as pl

from data_canon.core.dataclass import CanonicalData

logger = logging.getLogger(__name__)

# Canonical table names that can be validated
CANONICAL_TABLES = set(CanonicalData.__annotations__.keys())


def step(
    *,
    validate_input: bool = False,
    validate_output: bool = False,
    cache: bool = False,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator for pipeline steps with automatic validation and caching.

    This decorator validates canonical data inputs and/or outputs using
    the Pydantic models defined in data.models. Only parameters/returns
    that match canonical table names (households, persons, days,
    unlinked_trips, linked_trips, tours) are validated.

    Validation is skipped for tables that have already been validated
    if a CanonicalData instance is passed as 'canonical_data' parameter.

    When caching is enabled, the decorator will check for cached outputs
    before executing the step function. If valid cache exists, outputs are
    loaded from parquet files. Otherwise, the step executes and results
    are cached after successful validation.

    The default value is to only validate inputs to avoid duplicate validation.
    Recommend putting a final step full_check step at the end of the pipeline
    to validate all tables after all processing is complete.

    Args:
        validate_input: Whether to validate inputs. Defaults to True.
        validate_output: Whether to validate outputs. Defaults to False.
        cache: Whether to enable caching for this step. Defaults to False.

    Example:
        >>> @step(validate_input=True)
        ... def link_trips(
        ...     unlinked_trips: pl.DataFrame,
        ...     config: dict
        ... ) -> dict[str, pl.DataFrame]:
        ...     # Process trips
        ...     linked_trips = ...
        ...     return {"linked_trips": linked_trips}

        >>> @step(validate=False)
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
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
            # Save a copy of original kwargs to restore after validation
            kwargs_copy = kwargs.copy()

            # Extract validation flags and cache configuration
            should_validate_input = kwargs.pop("validate_input", validate_input)
            should_validate_output = kwargs.pop("validate_output", validate_output)
            should_cache = kwargs.pop("cache", cache)
            pipeline_cache = kwargs.pop("pipeline_cache", None)

            # Only pop canonical_data if function doesn't expect it
            sig = inspect.signature(func)
            if "canonical_data" in sig.parameters:
                canonical_data = kwargs.get("canonical_data")
            else:
                canonical_data = kwargs.pop("canonical_data", None)

            # Check cache if enabled
            if should_cache and pipeline_cache:
                cached_result = _try_load_from_cache(
                    func, pipeline_cache, args, kwargs, canonical_data
                )
                if cached_result is not None:
                    return cached_result

            # Add back in any popped flags if requested by the function
            kwargs.update(
                {key: value for key, value in kwargs_copy.items() if key in sig.parameters}
            )

            # Cache miss or caching disabled - execute step
            if should_validate_input:
                _validates(func, args, kwargs, canonical_data)

            result = func(*args, **kwargs)

            # Update canonical_data with results if available
            if canonical_data and isinstance(result, dict):
                _update_canonical_data(canonical_data, result)

            if should_validate_output and isinstance(result, dict):
                _validate_dict_outputs(result, func.__name__, canonical_data)

            # Cache result if enabled and validation passed
            if should_cache and pipeline_cache and isinstance(result, dict):
                _save_to_cache(func, pipeline_cache, args, kwargs, result)

            return result

        return wrapper  # type: ignore[return-value]

    return decorator


def _update_canonical_data(
    canonical_data: CanonicalData,
    result: dict[str, pl.DataFrame],
) -> None:
    """Update canonical_data instance with result DataFrames."""
    for key, value in result.items():
        if _is_canonical_dataframe(key, value):
            logger.info("Updating canonical_data with output '%s'", key)
        else:
            logger.warning(
                "Output '%s' is not a canonical table. This cannot be validated automatically.",
                key,
            )
        # Add to canonical_data instance either way
        setattr(canonical_data, key, value)


def _try_load_from_cache(
    func: Callable,
    pipeline_cache: Any,  # noqa: ANN401
    args: tuple,
    kwargs: dict,
    canonical_data: CanonicalData | None,
) -> dict[str, pl.DataFrame] | None:
    """Try to load cached result for a step.

    Returns cached result dict if found, None otherwise.
    """
    sig = inspect.signature(func)
    bound = sig.bind(*args, **kwargs)
    bound.apply_defaults()

    # Get input DataFrames for cache key generation
    input_dfs = {
        name: value
        for name, value in bound.arguments.items()
        if _is_canonical_dataframe(name, value)
    }

    # Extract params (non-DataFrame arguments)
    params = {
        name: value
        for name, value in bound.arguments.items()
        if name != "canonical_data" and not _is_canonical_dataframe(name, value)
    }

    # Generate cache key
    cache_key = pipeline_cache.get_cache_key(
        func.__name__,
        input_dfs if input_dfs else None,
        params if params else None,
    )

    # Try to load from cache
    cached_result = pipeline_cache.load(func.__name__, cache_key)
    if cached_result is not None:
        # Update canonical_data with cached results
        if canonical_data:
            for key, value in cached_result.items():
                setattr(canonical_data, key, value)

        return cached_result

    return None


def _save_to_cache(
    func: Callable,
    pipeline_cache: Any,  # noqa: ANN401
    args: tuple,
    kwargs: dict,
    result: dict[str, pl.DataFrame],
) -> None:
    """Save step result to cache.

    Only caches outputs that are canonical DataFrames.
    """
    if not result.items():
        return

    sig = inspect.signature(func)
    bound = sig.bind(*args, **kwargs)
    bound.apply_defaults()

    # Get input DataFrames for cache key generation
    input_dfs = {
        name: value
        for name, value in bound.arguments.items()
        if _is_canonical_dataframe(name, value)
    }

    # Extract params (non-DataFrame arguments)
    params = {
        name: value
        for name, value in bound.arguments.items()
        if name != "canonical_data" and not _is_canonical_dataframe(name, value)
    }

    # Generate cache key
    cache_key = pipeline_cache.get_cache_key(
        func.__name__,
        input_dfs if input_dfs else None,
        params if params else None,
    )

    pipeline_cache.save(func.__name__, cache_key, result)


def _validates(
    func: Callable,
    args: tuple,
    kwargs: dict,
    canonical_data: CanonicalData | None = None,
) -> None:
    """Validate input parameters that are canonical DataFrames."""
    sig = inspect.signature(func)
    bound = sig.bind(*args, **kwargs)
    bound.apply_defaults()

    # Use provided instance or check if one exists in kwargs
    validator = canonical_data
    if validator is None and "canonical_data" in bound.arguments:
        validator = bound.arguments["canonical_data"]

    for param_name, param_value in bound.arguments.items():
        if not _is_canonical_dataframe(param_name, param_value):
            continue

        logger.info(
            "Validating input '%s' for step '%s'",
            param_name,
            func.__name__,
        )
        # Use validator instance if available, otherwise create temporary
        step_name = func.__name__
        if validator:
            setattr(validator, param_name, param_value)
            validator.validate(param_name, step=step_name)
        else:
            temp_validator = CanonicalData()
            setattr(temp_validator, param_name, param_value)
            temp_validator.validate(param_name, step=step_name)


def _validate_dict_outputs(
    result: dict,
    func_name: str,
    canonical_data: CanonicalData | None = None,
) -> None:
    """Validate outputs in dict format."""
    for key, value in result.items():
        if not _is_canonical_dataframe(key, value):
            logger.warning(
                "Output '%s' from step '%s' is not a canonical "
                "table. This cannot be validated automatically.",
                key,
                func_name,
            )
            continue

        logger.info(
            "Validating output '%s' from step '%s'",
            key,
            func_name,
        )
        # Validate using canonical_data instance or create temporary
        if canonical_data:
            # Data already updated by wrapper, just validate
            canonical_data.validate(key, step=func_name)
        else:
            # No canonical_data instance, validate with temporary
            temp_validator = CanonicalData()
            setattr(temp_validator, key, value)
            temp_validator.validate(key, step=func_name)


def _is_canonical_dataframe(name: str, value: Any) -> bool:  # noqa: ANN401
    """Check if a value is a DataFrame for a canonical table."""
    return name in CANONICAL_TABLES and isinstance(value, pl.DataFrame)
