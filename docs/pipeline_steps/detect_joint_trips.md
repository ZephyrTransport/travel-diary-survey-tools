# Detect Joint Trips

::: processing.joint_trips.detect_joint_trips.detect_joint_trips
    options:
      show_root_heading: false
      show_root_toc_entry: false
      show_signature: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_raises: false

## Example

```yaml title="Example"
- name: detect_joint_trips
  params:
    method: mahalanobis
    covariance: [10000, 10000, 100, 100]  # 100 m, 100 m, 10 min, 10 min std devs
    confidence_level: 0.90
```

## Settings

<!-- step-settings: detect_joint_trips -->

::: processing.joint_trips.joint_trip_configs
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members: false

Code: [processing.joint_trips](../reference/processing/joint_trips.md)
