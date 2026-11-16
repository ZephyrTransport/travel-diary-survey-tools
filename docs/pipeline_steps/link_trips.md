# Link Trips

::: processing.link_trips.link.link_trips
    options:
      show_root_heading: false
      show_root_toc_entry: false
      show_signature: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_raises: false

## Example

```yaml title="Example"
- name: link_trips
  params:
    change_mode_enum: CHANGE_MODE
    transit_mode_enums: [FERRY, TRANSIT, LONG_DISTANCE]
    max_dwell_time: 180         # minutes
    dwell_buffer_distance: 100  # meters
    split_on_occupancy: false
```

## Settings

<!-- step-settings: link_trips -->

Code: [processing.link_trips](../reference/processing/link_trips.md)
