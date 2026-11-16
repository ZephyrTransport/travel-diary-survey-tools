# Controls

The `controls:` list of [Compute Weights](compute_weights.md#settings) names the
targets the weights are balanced to. Each entry is a name from the table below,
with an optional `importance` and an optional `merge` that joins categories into
one, using the category names shown:

```yaml
controls:
  - name: h_total
    importance: 200
  - name: h_size
    merge:
      size_4_plus: [size_4, size_5, size_6, size_7, size_8, size_9, size_10_plus]
  - name: p_gender
```

- `h_total` is required: the balancer matches every zone's household total first.
- A control without its own `importance` gets one from its PUMS margin of error
  when `balancing.moe_based_importance` is true, and `balancing.default_importance`
  otherwise.
- Group and category names are read in lower case, so `AAPI:` names the group `aapi`.

## Merging in some zones only

`zone_merges` joins categories in the zones it names and leaves the others as they
are. Each key is a zone group: a group's name from `zone_groups`, or the ID of a
target zone in no group. A zone merge can join groups the global `merge` already made:

```yaml
controls:
  - name: h_workers
    merge:
      workers_3_plus: [workers_3, workers_4, workers_5_plus]
    zone_merges:
      "Marin/Napa/Solano/Sonoma":
        workers_2_plus: [workers_2, workers_3_plus]
```

The merged group is a target only in the zones listed. Elsewhere the separate
categories stay targets.

## Households per PUMA

`h_puma` is listed like any other control, but it is not in the table below: its
categories are the crosswalk's PUMAs, found when the run starts, named
`puma_<PUMA ID>` (such as `puma_01308`), plus `outside_region` for PUMS households
outside every zone.

- Each home's PUMA comes from the same PUMA boundaries the PUMS is allocated by, and
  is written to `household_weights` as `home_puma`.
- It is never imputed: a household with no PUMA stops the run.

A survey rarely reaches enough homes in every PUMA to target each one. Merging a
zone's PUMAs into groups, with `zone_merges`, targets the groups instead. Merging
all of a zone's PUMAs into one group turns the target off in that zone. A category
that merges replace in every zone is left out of the report's tables.

## Households per target zone

`h_target_zone` targets the households in each target zone, the polygons of
`geography.target_zones`, within each zone group. Where `zone_groups` joins
target zones to balance them together, it keeps each one's household total right
without a zone group of its own. Where target zones are counties, these are the
county totals.

- Like `h_puma`, it is not in the table below: its categories are the target zone
  IDs, found when the run starts, such as `001`.
- A survey home's zone is the one its point falls in. A PUMS record's zone comes
  from the crosswalk, which copies each record into every zone its PUMA reaches,
  so each copy counts in its own zone.
- In a zone group made of one target zone it has no target: there it would be
  the zone's household total, already `h_total`.
- It is never imputed: a household in no zone stops the run.

```yaml
controls:
  - name: h_target_zone
```

## Available controls

<!-- weighting-controls -->

Code: [processing.weighting.controls](../../reference/processing/weighting/controls.md)
