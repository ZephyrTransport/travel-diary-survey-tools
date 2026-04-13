# Compute Weights

This path computes new expansion weights from PUMS controls and survey seed data.

It is the full weighting workflow, including:

- data preparation and PUMS fetching
- control recoding and shared incidence construction
- survey null imputation
- zone assignment and control merges
- control aggregation and importance resolution
- max-entropy balancing
- weight propagation, diagnostics, and validation

Both **PUMS** (Census microdata) and **Survey** (travel-diary seed) are conformed to the same **incidence schema**: one row per household, with shared 1-D controls and **cross-tab targets** expanded into a common set of incidence columns. That shared representation is what allows the same geography and merge logic to operate on both datasets.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {
  'background': '#fcfaf6',
  'primaryTextColor': '#22313f',
  'primaryBorderColor': '#9f8b6d',
  'lineColor': '#6b7280',
  'fontFamily': 'IBM Plex Sans, Segoe UI, sans-serif'
}}}%%
flowchart TD
  A("Setup\n• Register controls\n• Resolve 1-D targets\n• Resolve cross-tab targets\n• Build crosswalk\n• Fetch PUMS")

  subgraph inputs ["**Normalize to shared incidence schema**"]
    direction LR
    P0(["PUMS HH + Person"]) --> P1("Recode + pivot") --> PI[["PUMS incidence"]]
    S0(["Survey HH + Person"]) --> S1("Recode + pivot") --> SI[["Survey incidence"]]
  end

  NI("Null imputation on survey only\nRF trained on PUMS incidence")

  subgraph shared_stage ["**Shared incidence transforms**"]
    direction TD
    G1("Zone assignment\n• assign survey HHs\n• allocate PUMAs to control zones")
    G2("Create cross-tab targets")
    G3("Apply merges\ncollapse merged categories")
    G1 --> G2
    G2 --> G3
  end

  subgraph outputs ["**Divergent downstream roles**"]
    direction LR
    PZ[["PUMS incidence\nwith geography + merges"]] --> CT[["Control totals\naggregate PUMS by zone"]]
    PZ --> IM[["Importance\nMOE from PUMS or explicit config"]]
    SZ[["Seed incidence\nwith geography + merges"]]
  end

  B("Max-entropy balancing\nseed incidence × control totals × importance")
  O(["Weight propagation\ndiagnostics\nvalidation"])

  A --> P0
  A --> S0
  PI -. "Train RF Model" .-> NI
  SI --> NI
  PI --> G1
  NI --> G1
  G3 --> PZ
  G3 --> SZ
  SZ --> B
  CT --> B
  IM --> B
  B --> O

  classDef setup fill:#dce7f5,stroke:#557aa3,color:#1d2c3c,stroke-width:2px;
  classDef source fill:#f8f1e5,stroke:#c7a977,color:#3a3126,stroke-width:1.5px;
  classDef action fill:#efe7d8,stroke:#bca07a,color:#2f2a22,stroke-width:1.5px;
  classDef pums fill:#d9ecf7,stroke:#5b8fb9,color:#17324d,stroke-width:1.5px;
  classDef survey fill:#f8dfc9,stroke:#cf8c52,color:#4a2b15,stroke-width:1.5px;
  classDef shared fill:#e3efe2,stroke:#7ca16f,color:#233821,stroke-width:1.5px;
  classDef balance fill:#dcefe6,stroke:#4f7b6c,color:#183229,stroke-width:2px;
  classDef output fill:#f3dca2,stroke:#b6903b,color:#47360f,stroke-width:1.5px;

  class A setup;
  class P0,S0 source;
  class P1,S1,NI action;
  class PI,PZ,CT,IM pums;
  class SI,SZ survey;
  class G1,G2,G3 shared;
  class B balance;
  class O output;

  style inputs fill:#f9f4ec,stroke:#ccb99d,stroke-width:2px,color:#3a3126;
  style shared_stage fill:#edf5ec,stroke:#9db798,stroke-width:2px,color:#233821;
  style outputs fill:#f9f4ec,stroke:#ccb99d,stroke-width:2px,color:#3a3126;
```

The important split is not that PUMS and survey run as fully parallel pipelines. It is that they are both transformed into the same incidence format. From there, **PUMS incidence** is used to produce **control totals** and MOE-based importance, while the **survey incidence** becomes the **seed** to be reweighted. Null imputation is applied only to the survey side; PUMS is used as the training source for that step.

::: processing.weighting.compute_weights
    options:
      show_root_heading: true
      members:
        - compute_weights

## WeightingPipeline

`WeightingPipeline` is the orchestration class that `compute_weights` constructs and drives. It holds all intermediate state (crosswalk, incidence, control totals, weights, diagnostics) and exposes each stage as an explicit method.

::: processing.weighting.weighting_pipeline
    options:
      show_root_heading: true
      members:
        - WeightingPipeline

## Related Topics

- [Data Preparation](data_preparation.md)
- [Crosswalk](crosswalk.md)
- [Balancing](balancing.md)
- [Controls](controls.md)
- [Validation](validation.md)
- [Diagnostics](diagnostics.md)
