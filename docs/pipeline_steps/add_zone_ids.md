# Add Zone IDs

Assigns each location in the survey to the zones of every configured zone
system (for example TAZ and MAZ). Each system adds one column per location,
named after the zone system: `home_taz`, `work_taz`, `o_taz` and so on. A
point outside every zone can be snapped to the nearest one within
`max_snap_distance`, or left null.

It runs after `extract_tours`, so tours get zones too, and before
`cascade_usability`, so a usability profile can require that a record has
a zone.

::: processing.add_zone_ids.add_zone_ids.add_zone_ids
    options:
      show_root_heading: false
      show_root_toc_entry: false
      show_signature: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_raises: false

## Example

```yaml title="Example"
- name: add_zone_ids
  params:
    projected_crs: "EPSG:26910"  # UTM Zone 10N, for the nearest-zone snap
    max_snap_distance: 500       # meters; points farther from every zone stay null
    zone_geographies:
      - zone_name: taz
        shapefile: zones/taz.shp
        zone_id_field: TAZ_NODE
      - zone_name: maz
        shapefile: zones/maz.shp
        zone_id_field: MAZ_NODE
```

## Settings

<!-- step-settings: add_zone_ids -->

Code: [processing.add_zone_ids](../reference/processing/add_zone_ids.md)
