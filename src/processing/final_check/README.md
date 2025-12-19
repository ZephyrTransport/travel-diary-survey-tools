[← Back to Main README](../../../README.md)

# Final Check Pipeline Step

This module performs final validation checks on the complete processed dataset to ensure data quality and schema compliance. It is basically a dummy module to run Pydantic validation on all tables at the end of the pipeline.

## Pipeline Steps

### `final_check`

Runs comprehensive validation on all canonical survey tables at the end of the pipeline.

**Inputs:**
- `households`: Processed household table (pl.DataFrame)
- `persons`: Processed person table (pl.DataFrame)
- `days`: Processed person-day table (pl.DataFrame)
- `unlinked_trips`: Processed unlinked trip records (pl.DataFrame)
- `linked_trips`: Processed linked trip records (pl.DataFrame)
- `tours`: Processed tour records (pl.DataFrame)

**Outputs:**
- Dictionary containing the same validated tables:
  - `households`
  - `persons`
  - `days`
  - `unlinked_trips`
  - `linked_trips`
  - `tours`

**Core Algorithm:**

**Pydantic Model Validation:**
1. This step is decorated with `@step(validate_input=True, validate_output=True)`
2. The pipeline framework automatically validates all input/output against Pydantic data models
3. Validation checks:
   - **Schema Compliance:** All required columns present with correct data types
   - **Value Constraints:** Numeric ranges, categorical values, enum memberships
   - **Referential Integrity:** Foreign keys match (person_id → persons, hh_id → households, etc.)
   - **Business Rules:** Domain-specific constraints (e.g., depart_time < arrive_time)

**Custom Validation Space:**
- The function body is intentionally simple (pass-through)
- Pydantic handles validation automatically at model instantiation
- This space *could* be extended with additional custom checks not covered by models:
  - Cross-table consistency checks
  - Statistical outlier detection
  - Survey-specific business rules
  - Data quality metrics logging
- However, validation logic should ideally be implemented in Pydantic models themselves for reusability

**Validation Failure Handling:**
- If validation fails, raises `DataValidationError` with detailed error messages
- Error messages indicate:
  - Which table failed validation
  - Which rows/columns have issues
  - What constraint was violated
- Pipeline execution halts on validation failure

**Notes:**
- This is the last checkpoint before data export
- Ensures output meets canonical data specifications
- Validation errors caught here prevent invalid data from reaching models/analyses
- Pydantic models defined in `src/data_canon/models/` provide the validation rules
- Comprehensive logging helps diagnose data quality issues
- Pass-through design allows validation to occur transparently
