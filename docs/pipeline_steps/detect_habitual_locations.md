# Detect Habitual Locations

::: processing.habitual_locations
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

## The step

::: processing.habitual_locations.detect_habitual_locations.detect_habitual_locations
    options:
      show_root_heading: false
      show_root_toc_entry: false
      show_signature: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_raises: false

## Example

```yaml title="Example"
- name: detect_habitual_locations
  params:
    buffer_meters: 300
    include_observed_homes: true
```

## Settings

<!-- step-settings: detect_habitual_locations -->

## Delivering the reported locations

Survey cleaning builds the delivered table with this helper.

::: processing.habitual_locations.reported
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members:
        - reported_habitual_locations
      filters:
        - "!^logger$"
        - "!^_"

Code: [processing.habitual_locations](../reference/processing/habitual_locations.md)
