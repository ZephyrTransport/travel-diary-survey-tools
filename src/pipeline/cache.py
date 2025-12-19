"""Pipeline caching system using Parquet for fast checkpointing.

Provides hash-based cache invalidation and parquet storage for
pipeline step outputs, enabling fast debugging and iteration.
"""

import hashlib
import json
import logging
import pickle
import shutil
from pathlib import Path
from typing import Any

import geopandas as gpd
import polars as pl

logger = logging.getLogger(__name__)


class PipelineCache:
    """Manages parquet-based caching for pipeline steps.

    Cache structure:
        .cache/
            {step_name}/
                {cache_key}/
                    metadata.json
                    {table_name}.parquet
                    ...

    The cache key is a hash of:
    - Step name
    - Input data (schema + row count + content hash of all rows)
    - Step parameters

    This ensures cache invalidation when inputs or configuration change.
    """

    def __init__(self, cache_dir: Path | str = Path(".cache")) -> None:
        """Initialize pipeline cache.

        Args:
            cache_dir: Root directory for cache storage
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._stats = {
            "loaded": 0,  # Loaded from cache
            "missing": 0,  # No cache found
            "stale": 0,  # Cache outdated/corrupted
        }

    def get_cache_key(
        self,
        step_name: str,
        inputs: dict[str, pl.DataFrame] | None,
        params: dict[str, Any] | None,
    ) -> str:
        """Generate cache key from step name, inputs, and parameters.

        The key is a hash of:
        - Step name
        - For each input DataFrame:
            - Table name
            - Schema (column names and types)
            - Row count
            - Content hash (hash of all row values)
        - Step parameters (as sorted JSON)

        This ensures the cache invalidates when any input data or
        configuration changes.

        Args:
            step_name: Name of the pipeline step
            inputs: Input DataFrames (or None for first step)
            params: Step parameters from config

        Returns:
            16-character hex hash string
        """
        # Start with step name
        hash_parts = [step_name]

        # Hash input data schemas and content
        if inputs:
            for table_name in sorted(inputs.keys()):
                df = inputs[table_name]
                if df is not None:
                    # Hash the entire DataFrame for robust cache invalidation
                    # Polars hash_rows() is efficient even for large DataFrames
                    schema_str = str(df.schema)
                    row_count = len(df)
                    data_hash = ""
                    if row_count > 0:
                        # Hash all rows - Polars does this efficiently
                        data_hash = str(df.hash_rows().sum())

                    hash_parts.append(
                        f"{table_name}:{schema_str}:{row_count}:{data_hash}"
                    )

        # Hash parameters
        if params:
            # Remove any non-serializable entries (e.g., DataFrames)
            serializable_params = {}
            for k, v in params.items():
                try:
                    json.dumps(v)
                    serializable_params[k] = v
                except (TypeError, ValueError):
                    # Skip non-serializable values
                    logger.debug("Skipping non-serializable param: %s", k)

            # Sort keys for deterministic hashing
            params_str = json.dumps(serializable_params, sort_keys=True)
            hash_parts.append(params_str)

        # Generate hash
        combined = "|".join(hash_parts)
        return hashlib.sha256(combined.encode()).hexdigest()[:16]

    def load(
        self,
        step_name: str,
        cache_key: str,
    ) -> dict[str, pl.DataFrame] | None:
        """Load cached step outputs from parquet.

        Args:
            step_name: Name of the pipeline step
            cache_key: Cache key from get_cache_key()

        Returns:
            Dictionary of table name -> DataFrame, or None if cache miss
        """
        cache_path = self.cache_dir / step_name / cache_key

        if not cache_path.exists():
            self._stats["missing"] += 1
            logger.debug(
                "Cache not found for %s (key: %s)", step_name, cache_key
            )
            return None

        metadata_path = cache_path / "metadata.json"
        if not metadata_path.exists():
            self._stats["stale"] += 1
            logger.warning(
                "Cache corrupted: missing metadata for %s", step_name
            )
            return None

        # Load metadata
        try:
            with metadata_path.open() as f:
                metadata = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            self._stats["stale"] += 1
            logger.warning("Failed to read metadata for %s: %s", step_name, e)
            return None

        # Load each table based on its type
        outputs = {}
        load_info = []
        for table_name in metadata.get("tables", []):
            table_type = metadata.get("table_types", {}).get(
                table_name, "polars"
            )
            obj = _load_data(cache_path, table_name, table_type)

            if obj is None:
                self._stats["stale"] += 1
                return None

            outputs[table_name] = obj

            # Build info string
            obj_type = type(obj).__name__
            shape = ""
            if isinstance(obj, (pl.DataFrame, gpd.GeoDataFrame)):
                shape = f" ({len(obj)}x{len(obj.columns)})"
            elif hasattr(obj, "shape"):
                shape = f" {obj.shape}"
            elif hasattr(obj, "__len__"):
                shape = f" (len={len(obj)})"

            # Get file size and format
            file_ext = ".pkl" if table_type == "pickle" else ".parquet"
            file_path = cache_path / f"{table_name}{file_ext}"
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            size_str = f" [{file_size_mb:.2f} MB]"
            format_name = "pickle" if table_type == "pickle" else "parquet"

            load_info.append(
                f"  ← {table_name} {obj_type}: {format_name}{shape}{size_str}"
            )

        self._stats["loaded"] += 1
        logger.info(
            "Loaded from cache: %s (key: %s)\n%s",
            step_name,
            cache_key,
            "\n".join(load_info),
        )
        return outputs

    def save(
        self,
        step_name: str,
        cache_key: str,
        outputs: dict[str, pl.DataFrame],
    ) -> None:
        """Save step outputs to parquet cache.

        Args:
            step_name: Name of the pipeline step
            cache_key: Cache key from get_cache_key()
            outputs: Dictionary of table name -> DataFrame
        """
        cache_path = self.cache_dir / step_name / cache_key
        cache_path.mkdir(parents=True, exist_ok=True)

        try:
            # Save each obj as parquet or pickle
            save_info = []
            table_types = {}  # Track type for each table

            for table_name, obj in outputs.items():
                obj_type, format_str = _save_data(cache_path, table_name, obj)
                table_types[table_name] = obj_type

                # Build info string
                save_info.append(format_str)

            # Save metadata
            metadata = {
                "step_name": step_name,
                "cache_key": cache_key,
                "tables": list(outputs.keys()),
                "table_types": table_types,  # Add type info
                "row_counts": {
                    name: len(df) if df is not None else 0
                    for name, df in outputs.items()
                },
            }
            metadata_path = cache_path / "metadata.json"
            with metadata_path.open("w") as f:
                json.dump(metadata, f, indent=2)

            logger.info(
                "Cached step: %s (key: %s)\n%s",
                step_name,
                cache_key,
                "\n".join(save_info),
            )

        except Exception:
            logger.exception("Failed to save cache for %s", step_name)
            # Clean up partial cache
            if cache_path.exists():
                shutil.rmtree(cache_path)

    def invalidate(self, step_name: str | None = None) -> None:
        """Invalidate cache for a step or all steps.

        Args:
            step_name: Name of step to invalidate, or None for all steps
        """
        if step_name:
            step_path = self.cache_dir / step_name
            if step_path.exists():
                shutil.rmtree(step_path)
                logger.info("Invalidated cache for %s", step_name)
        elif self.cache_dir.exists():
            shutil.rmtree(self.cache_dir)
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            logger.info("Invalidated all caches")

    def list_cached_steps(self) -> list[dict[str, Any]]:
        """List all cached steps with metadata.

        Returns:
            List of dicts with step info (name, cache_key, tables, sizes)
        """
        cached_steps = []

        if not self.cache_dir.exists():
            return cached_steps

        for step_dir in self.cache_dir.iterdir():
            if not step_dir.is_dir():
                continue

            step_name = step_dir.name

            for cache_dir in step_dir.iterdir():
                if not cache_dir.is_dir():
                    continue

                cache_key = cache_dir.name
                metadata_path = cache_dir / "metadata.json"

                if metadata_path.exists():
                    try:
                        with metadata_path.open() as f:
                            metadata = json.load(f)

                        # Calculate total size
                        total_size = sum(
                            p.stat().st_size
                            for p in cache_dir.glob("*.parquet")
                        )

                        cached_steps.append(
                            {
                                "step_name": step_name,
                                "cache_key": cache_key,
                                "tables": metadata.get("tables", []),
                                "row_counts": metadata.get("row_counts", {}),
                                "size_mb": total_size / (1024 * 1024),
                                "path": str(cache_dir),
                            }
                        )
                    except (OSError, json.JSONDecodeError) as e:
                        logger.warning("Failed to read cache metadata: %s", e)

        return cached_steps

    def get_stats(self) -> dict[str, int | float]:
        """Get cache statistics.

        Returns:
            Dict with 'loaded', 'missing', 'stale', 'total',
            and 'load_rate' keys
        """
        total = (
            self._stats["loaded"]
            + self._stats["missing"]
            + self._stats["stale"]
        )
        load_rate = self._stats["loaded"] / total if total > 0 else 0.0

        return {
            "loaded": self._stats["loaded"],
            "missing": self._stats["missing"],
            "stale": self._stats["stale"],
            "total": total,
            "load_rate": load_rate,
        }

    def reset_stats(self) -> None:
        """Reset cache statistics."""
        self._stats = {"loaded": 0, "missing": 0, "stale": 0}


def _load_data(cache_path: Path, name: str, data_type: str) -> Any:  # noqa: ANN401
    """Load a single table from cache based on its type.

    Args:
        cache_path: Path to the cache directory
        name: Name of the table to load
        data_type: Type of table ('polars', 'geopandas', or 'pickle')

    Returns:
        Loaded object or None if file missing
    """
    try:
        if data_type == "pickle":
            file_path = cache_path / f"{name}.pkl"
            with file_path.open("rb") as f:
                return pickle.load(f)  # noqa: S301
        elif data_type == "geopandas":
            file_path = cache_path / f"{name}.parquet"
            return gpd.read_parquet(file_path)
        else:  # polars (default)
            file_path = cache_path / f"{name}.parquet"
            return pl.read_parquet(file_path)
    except FileNotFoundError:
        logger.warning("Cache corrupted: missing %s", file_path.name)
        return None
    except (OSError, pickle.UnpicklingError, ValueError) as e:
        logger.warning("Failed to load %s: %s", name, e)
        return None


def _save_data(cache_path: Path, name: str, obj: Any) -> tuple[str, str]:  # noqa: ANN401
    """Save a single object to cache and return metadata.

    Args:
        cache_path: Path to the cache directory
        name: Name of the table to save
        obj: Object to save

    Returns:
        Tuple of (data_type, info_string) where info_string is formatted
        as "name obj_type → format (shape)"
    """
    obj_path = cache_path / name
    obj_type = type(obj).__name__

    # Determine format and save data
    if isinstance(obj, (pl.DataFrame, gpd.GeoDataFrame)):
        obj_path = obj_path.with_suffix(".parquet")
        if isinstance(obj, pl.DataFrame):
            obj.write_parquet(obj_path)
            data_type = "polars"
        else:
            obj.to_parquet(obj_path)
            data_type = "geopandas"
        format_name = "parquet"
        shape = f" ({len(obj)}x{len(obj.columns)})"
    else:
        obj_path = obj_path.with_suffix(".pkl")
        with obj_path.open("wb") as f:
            pickle.dump(obj, f)
        data_type = "pickle"
        format_name = "pickle"
        # Format shape info
        if hasattr(obj, "shape"):
            shape = f" ({obj.shape})"
        elif hasattr(obj, "__len__"):
            shape = f" (len={len(obj)})"
        else:
            shape = ""

    # Build info string (common for all types)
    file_size_mb = obj_path.stat().st_size / (1024 * 1024)
    info = (
        f"  → {name} {obj_type}: {format_name}{shape} [{file_size_mb:.2f} MB]"
    )

    return data_type, info
