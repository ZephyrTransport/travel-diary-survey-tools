# BATS 2023 analyses

One-off analyses of the BATS 2023 data. They are not part of the pipeline: no
runner or config in this folder, and none of them is registered in
`projects/bats_2023/run.py`.

| File | What it is |
|---|---|
| `analyze_transit.py` | A `@step()`, `summarize_transit_trips`: weighted transit trips, boardings per linked trip, and a county origin-destination matrix. Not registered in any runner. |
| `purpose_by_mode.py` | A script that reads the delivered `linked_trips_2023.csv` and tabulates weighted trips by mode and destination purpose |
| `analyze_wfh.qmd` / `.html` | Quarto report on working from home among employed persons: telework and commute frequency, the telework ratio, and impossible combinations |
| `investigation.qmd` / `.html` | Quarto notebook exploring the 2023 travel diary |

The `.html` files are the rendered reports. Re-render one with
`quarto render analyze_wfh.qmd`.
