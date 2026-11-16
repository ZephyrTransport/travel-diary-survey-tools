# Pipeline

The pipeline module provides the execution framework for running sequential data processing steps with configuration and logging support.

<!-- include architecture and diagram from top level readme -->
{%
    include "../README.md"
    start="## Architecture"
    end="### Pipeline Steps"

%}

[](){#step-config-rules}

## Configuring a step

Each entry under `steps:` names a step and gives its settings under `params:`.
The pipeline matches them to the step function's arguments by name:

- **Canonical tables** (`households`, `persons`, ...) come from the pipeline's data,
  never from `params:`. A key naming a table is an error.
- **A plain argument** (`label: str`) takes the key of the same name.
- **A settings model** (an argument annotated with a pydantic model, such as
  `matching: JointTripConfig` on `detect_joint_trips`) is built from its fields.
  Write the fields flat under `params:`; each key goes to the model that owns that
  field. A block named after the argument works too, for grouping:

```yaml
# Flat: each key goes to whichever model owns it
- name: detect_joint_trips
  params:
    method: mahalanobis
    covariance: [10000, 10000, 100, 100]
    confidence_level: 0.90

# Nested, where a step has several models (compute_weights takes census,
# geography, balancing and diagnostics)
- name: compute_weights
  params:
    census:
      state_fips: "06"
      pums_year: 2023
      # ...the rest of the census settings
```

The rules that keep a setting from going astray:

| Situation | Result |
|---|---|
| A key the step does not take, or a misspelt one | Error, naming the step and the key |
| A setting with no default left out | Error: the config must give it |
| A block and flat keys for the same model | Error: use one or the other |
| A setting with a default left out | The default applies |

A step's settings models set `extra="forbid"`, so a misspelt key inside a block is
caught too. Two models in one step may not share a field name; that fails when the
step is defined, not when it runs.

The step-level keys beside `params:` are `validate_input` and `validate_output`.
Each overrides the step's `@step()` value only when given; any other key stops the
run. Every step runs every time: nothing is cached between runs.

The pipeline system allows you to:

- Define processing steps as Python functions
- Configure pipeline execution via YAML files
- Track and log processing progress
- Validate data between steps

The `Pipeline` class, logging and the `@step()` decorator are in the
[Developer Reference](reference/pipeline.md).
