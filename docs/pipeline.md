# Pipeline

The pipeline module provides the execution framework for running sequential data processing steps with configuration, caching, and logging support.

<!-- include architecture and diagram from top level readme -->
{%
    include "../README.md"
    start="## Architecture"
    end="# Pipeline Steps"

%}

The pipeline system allows you to:

- Define processing steps as Python functions
- Configure pipeline execution via YAML files
- Cache intermediate results for faster re-runs
- Track and log processing progress
- Validate data between steps

## Pipeline Class

::: pipeline.pipeline.Pipeline

## Caching

::: pipeline.cache

## Logging

::: pipeline.logger

## Decorators

::: pipeline.decoration
