"""Helper functions for comparing CTRAMP model output to survey data."""

# flake8: noqa: T201

import logging
import math
import sys
from pathlib import Path

import plotly.graph_objects as go
import polars as pl
from polars.exceptions import PolarsError

from data_canon.models import ctramp

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Model output directory
model_dir = r"\\model3-b\Model3B-Share\Projects\2023_TM161_IPA_35\main"

# Survey output directory
survey_dir = r"\\models.ad.mtc.ca.gov\data\models\Data\HomeInterview\Bay Area Travel Study 2023\Data\Processed\pipeline_20251230\ctramp"  # noqa: E501

# Check if directories exist
for path in [survey_dir, model_dir]:
    if not Path(path).exists():
        msg = f"Directory does not exist: {path}"
        logger.error(msg)
        sys.exit(1)


def _nice_step(raw_step: float) -> float:
    """Round a positive step to 1, 2, or 5 x 10^k."""
    if raw_step <= 0 or raw_step is None or math.isnan(raw_step):
        return raw_step
    exp = math.floor(math.log10(raw_step))
    base = 10**exp
    frac = raw_step / base

    if frac <= 1:
        nice_frac = 1
    elif frac <= 2:  # noqa: PLR2004
        nice_frac = 2
    elif frac <= 5:  # noqa: PLR2004
        nice_frac = 5
    else:
        nice_frac = 10

    return nice_frac * base


def bin_field(
    model_lf: pl.LazyFrame, survey_lf: pl.LazyFrame, field: str, bins: int = 20
) -> tuple[pl.LazyFrame, pl.LazyFrame, str, str | None]:
    """Bin a numeric field into specified number of bins for both model and survey data."""
    binned = f"{field}__bin"
    bin_id = f"{field}__bin_id"

    stats = (
        pl.concat([model_lf.select(field), survey_lf.select(field)])
        .select(pl.col(field).min().alias("min"), pl.col(field).max().alias("max"))
        .collect()
    )
    gmin, gmax = stats.row(0)

    if gmin is None or gmax is None or gmin == gmax:
        return model_lf, survey_lf, field, None

    step = _nice_step((gmax - gmin) / bins)
    start = math.floor(gmin / step) * step
    end = math.ceil(gmax / step) * step
    n = int((end - start) / step)

    breaks = [start + step * i for i in range(1, n)]

    # pretty labels with thousands separators
    labels = [f"{int(start + step * i):,} - {int(start + step * (i + 1)):,}" for i in range(n)]

    def with_bins(lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.with_columns(
            [
                pl.col(field).cut(breaks, labels=labels).alias(binned),
                ((pl.col(field) - start) // step).cast(pl.Int32).alias(bin_id),
            ]
        )

    return with_bins(model_lf), with_bins(survey_lf), binned, bin_id


# Basic comparison, select common fields, make bar plot of distributions for each field
# For integer coded fields, add labels from codebooks if available
def plot_distribution_comparison(
    model_lf: pl.LazyFrame,
    survey_lf: pl.LazyFrame,
    field_name: str,
    table_name: str | None = None,
    bins: int = 20,
    unique_threshold: int = 100,
) -> None:
    """Plot distribution comparison for a single field between model and survey data."""
    used_field = field_name
    order_field = None

    # Model vs Survey unique counts (one collect)
    uniq = (
        pl.concat(
            [
                model_lf.select(pl.lit("model").alias("src"), pl.col(field_name).alias("v")),
                survey_lf.select(pl.lit("survey").alias("src"), pl.col(field_name).alias("v")),
            ]
        )
        .group_by("src")
        .agg(pl.col("v").drop_nulls().n_unique().alias("n_unique"))
        .collect()
    )
    uniq_map = {r[0]: r[1] for r in uniq.rows()}
    model_unique = uniq_map.get("model", 0)
    survey_unique = uniq_map.get("survey", 0)

    # decide whether to bin (use max of the two)
    if max(model_unique, survey_unique) > unique_threshold:
        model_lf, survey_lf, used_field, order_field = bin_field(
            model_lf, survey_lf, field_name, bins=bins
        )

    group_cols = [used_field] + ([order_field] if order_field else [])

    model_counts = model_lf.group_by(group_cols).len().rename({"len": "model_count"}).collect()
    survey_counts = survey_lf.group_by(group_cols).len().rename({"len": "survey_count"}).collect()

    comparison_df = model_counts.join(
        survey_counts, on=group_cols, how="full", coalesce=True
    ).fill_null(0)

    model_total = comparison_df["model_count"].sum()
    survey_total = comparison_df["survey_count"].sum()

    comparison_df = comparison_df.with_columns(
        [
            (pl.col("model_count") / model_total * 100).alias("model_percent"),
            (pl.col("survey_count") / survey_total * 100).alias("survey_percent"),
        ]
    ).sort(order_field or used_field)

    x = comparison_df[used_field].to_list()
    fig = go.Figure(
        [
            go.Bar(name="Model", x=x, y=comparison_df["model_percent"].to_list()),
            go.Bar(name="Survey", x=x, y=comparison_df["survey_percent"].to_list()),
        ]
    )
    # Format title
    title_text = (
        f'Distribution Comparison for "{field_name}"'
        + (f' in "{table_name}"' if table_name else "")
        + " (Unweighted%)"
    )
    title_text += f" — bins ({bins})" if used_field != field_name else ""
    subtitle_text = f"Model unique: {model_unique:,} · Survey unique: {survey_unique:,}"

    fig.update_layout(
        barmode="group",
        title={
            "text": title_text,
            "subtitle": {"text": subtitle_text},
        },
        yaxis_title="Percent",
    )
    fig.show()


def compare_distributions(
    data: dict[str, pl.LazyFrame],
    fields: list[str],
    table_name: str | None = None,
    bins: int = 20,
    unique_threshold: int = 100,
) -> None:
    """Compare distributions of specified fields between model and survey data."""
    model_lf: pl.LazyFrame = data["model"]
    survey_lf: pl.LazyFrame = data["survey"]

    for field_name in fields:
        try:
            plot_distribution_comparison(
                model_lf,
                survey_lf,
                field_name,
                table_name=table_name,
                bins=bins,
                unique_threshold=unique_threshold,
            )
        except (PolarsError, ValueError, KeyError):
            logger.exception('Error comparing field "%s"', field_name)


def load_data(
    data_models: dict,
    iteration: int,
    model_dir: Path | str,
    survey_dir: Path | str,
    cache: bool | Path | str = True,
) -> dict[str, dict[str, pl.LazyFrame]]:
    """Load model and survey data for a specified table.

    If `cache` is truthy:
      - If `cache` is True, use a .cache directory alongside this file.
      - If `cache` is a Path/str, use that directory.
    Cached files are stored as parquet named: <table>_<iteration>_model.parquet
    and <table>_<iteration>_survey.parquet. When cache files exist they are
    loaded lazily via pl.scan_parquet.
    """
    # Resolve cache directory
    cache_dir: Path | None = None
    if cache is True:
        cache_dir = Path(__file__).resolve().parent / ".cache"
    elif cache:
        cache_dir = Path(cache)
    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)

    def _load_source(csv_path: Path, cache_path: Path | None, source_name: str) -> pl.LazyFrame:
        """Load data from cache (parquet) or CSV, creating cache if needed."""
        if cache_path and cache_path.exists():
            logger.info("Loading %s from cache %s", source_name, cache_path)
            try:
                return pl.scan_parquet(cache_path)
            except Exception:
                logger.exception("Failed to read cache %s, falling back to CSV", cache_path)

        logger.info("Loading %s from %s", source_name, csv_path)
        df = pl.read_csv(csv_path, infer_schema_length=10000)

        if cache_path:
            try:
                df.write_parquet(cache_path)
                logger.info("Wrote cache %s", cache_path)
            except Exception:
                logger.exception("Failed to write cache %s", cache_path)

        return df.lazy()

    data: dict[str, dict[str, pl.LazyFrame]] = {}

    for table_name in data_models:
        model_csv = Path(model_dir) / f"{table_name}_{iteration}.csv"
        survey_csv = Path(survey_dir) / f"{table_name}.csv"

        model_cache = cache_dir / f"{table_name}_{iteration}_model.parquet" if cache_dir else None
        survey_cache = cache_dir / f"{table_name}_{iteration}_survey.parquet" if cache_dir else None

        data[table_name] = {
            "model": _load_source(model_csv, model_cache, f"{table_name} model"),
            "survey": _load_source(survey_csv, survey_cache, f"{table_name} survey"),
        }

    return data


if __name__ == "__main__":
    # Tables to compare
    data_models = {
        #   "HouseholdData": ctramp.HouseholdCTRAMPModel,
        #   "PersonData": ctramp.PersonCTRAMPModel,
        # "MandatoryLocationData": ctramp.MandatoryLocationCTRAMPModel,
        #   "IndivTripData": ctramp.IndividualTripCTRAMPModel,
        "IndivTourData": ctramp.IndividualTourCTRAMPModel,
        #   "JointTripData": ctramp.JointTripCTRAMPModel,
        #   "JointTourData": ctramp.JointTourCTRAMPModel,
    }
    # Model output directory
    model_dir = r"\\model3-b\Model3B-Share\Projects\2023_TM161_IPA_35\main"

    # Survey output directory
    survey_dir = r"\\models.ad.mtc.ca.gov\data\models\Data\HomeInterview\Bay Area Travel Study 2023\Data\Processed\pipeline_20251230\ctramp"  # noqa: E501

    data = load_data(
        data_models=data_models, iteration=3, model_dir=model_dir, survey_dir=survey_dir
    )

    indiv_tour_fields = ["num_ob_stops", "num_ib_stops"]

    # Example comparison for IndivTourData table
    compare_distributions(data["IndivTourData"], indiv_tour_fields, table_name="IndivTourData")

    print("Comparison complete.")
