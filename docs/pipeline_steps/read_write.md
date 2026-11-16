# Load / Write Data

## Load data

::: processing.read_write.read_write.load_data
    options:
      show_root_heading: false
      show_root_toc_entry: false
      show_signature: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_raises: false

```yaml title="Example"
- name: load_data
  params:
    input_paths:
      households: data/hh.csv
      persons: data/person.csv
      person_days: data/day.csv
      unlinked_trips: data/trip.csv
```

### Settings

<!-- step-settings: load_data -->

## Write data

::: processing.read_write.read_write.write_data
    options:
      show_root_heading: false
      show_root_toc_entry: false
      show_signature: false
      show_docstring_parameters: false
      show_docstring_returns: false
      show_docstring_raises: false

```yaml title="Example"
- name: write_data
  params:
    write_only_canonical: true
    output_paths:
      households: output/households.csv
      persons: output/persons.csv
      person_days: output/person_days.csv
      linked_trips: output/linked_trips.csv
      tours: output/tours.csv
```

### Settings

<!-- step-settings: write_data -->

Code: [processing.read_write](../reference/processing/read_write.md)
