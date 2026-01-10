[← Back to Main README](../../../README.md)

# Read/Write Pipeline Steps

This module provides data input/output operations for the survey processing pipeline.

## Pipeline Steps

### `load_data`

Loads canonical survey tables from file paths into memory.

**Inputs:**
- `input_paths`: Dictionary mapping table names to file paths
  - Supported formats: CSV, Parquet, Shapefile, GeoJSON

**Outputs:**
- Dictionary of table names to DataFrames (`pl.DataFrame` or `gpd.GeoDataFrame`)
  - Typical tables: households, persons, days, unlinked_trips, etc.

**Core Algorithm:**
1. Iterate through each table name and file path in `input_paths`
2. Validate file path exists, providing helpful error message with broken path component
3. Load data based on file extension:
   - `.csv` → `polars.read_csv()`
   - `.parquet` → `polars.read_parquet()`
   - `.shp`, `.shp.zip`, `.geojson` → `geopandas.read_file()`
4. Return dictionary of loaded tables

**Notes:**
- All CSV/Parquet files loaded as Polars DataFrames for performance
- Geospatial files loaded as GeoPandas GeoDataFrames
- Path validation helps diagnose configuration errors

---

### `write_data`

Writes canonical survey tables to output file paths.

**Inputs:**
- `output_paths`: Dictionary mapping table names to output file paths
- `canonical_data`: Dictionary of DataFrames to write
- `validate_input`: Whether to run validation before writing
- `create_dirs`: Whether to create parent directories (default: True)

**Outputs:**
- None (writes files to disk)

**Core Algorithm:**
1. If `validate_input=True`, validate each table using canonical data models
2. For each table in `output_paths`:
   - Retrieve DataFrame from `canonical_data`
   - Create parent directories if needed
   - Write data based on file extension:
     - `.csv` → `DataFrame.write_csv()`
     - `.parquet` → `DataFrame.write_parquet()`
     - `.shp`, `.shp.zip`, `.geojson` → `GeoDataFrame.to_file()`
     - `.txt` → `Path.write_text()`
3. Log completion status

**Notes:**
- Validation ensures output conforms to canonical data schemas
- Automatic directory creation prevents path errors
- Supports multiple output formats for flexibility
