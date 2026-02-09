# startup-analytics-ab

A zero-cost, local-first analytics and A/B experimentation platform built for startups.

## Overview

This system demonstrates a production-grade approach to:
- Event tracking and ingestion
- Data warehousing with DuckDB
- dbt-style transformations
- Deterministic A/B experiment assignment
- Funnel metrics and statistical analysis
- Data-driven ship/no-ship decisions

All components run locally with no paid services or cloud dependencies.

## Dashboard

![Analytics Dashboard](docs/dashboard-screenshot.png)

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Event      │────▶│   DuckDB     │────▶│   dbt        │
│   Collector  │     │   Warehouse  │     │   Models     │
│   (FastAPI)  │     │              │     │              │
└──────────────┘     └──────────────┘     └──────┬───────┘
                                                 │
┌──────────────┐     ┌──────────────┐            │
│   User       │     │   Dashboard  │◀───────────┘
│   Simulator  │     │   (HTML/JS)  │
└──────────────┘     └──────────────┘
                            │
                     ┌──────▼───────┐
                     │  Experiment  │
                     │  Analysis    │
                     │  (scipy)     │
                     └──────────────┘
```

## Stack

| Component       | Technology         |
|-----------------|--------------------|
| Event API       | Python / FastAPI   |
| Warehouse       | DuckDB             |
| Transformations | dbt (local)        |
| Simulator       | Python             |
| Analysis        | scipy / statsmodels|
| Dashboard       | HTML + vanilla JS  |
| CI              | GitHub Actions     |

## Project Structure

```
startup-analytics-ab/
├── src/
│   ├── collector/      # FastAPI event ingestion service
│   ├── warehouse/      # DuckDB storage layer
│   ├── simulator/      # User behavior event generator
│   ├── ab/             # A/B assignment logic
│   ├── analysis/       # Statistical experiment analysis
│   └── dashboard/      # HTML + JS visualization
├── dbt/
│   ├── models/
│   │   ├── staging/    # Raw event staging models
│   │   └── marts/      # Funnel and experiment metrics
│   └── tests/          # dbt data quality tests
├── tests/              # Unit and integration tests
├── data/               # Local DuckDB database files
├── ci/                 # CI pipeline configuration
├── Makefile            # Project commands
└── requirements.txt    # Python dependencies
```

## Quick Start

```bash
# Install dependencies
make install

# Run the event collector
make run-collector

# Generate simulated events
make simulate

# Run dbt transformations
make transform

# Run experiment analysis
make analyze

# Launch the dashboard
make dashboard
```

## Design Principles

- **Append-only events**: Raw events are never mutated
- **Reproducible data**: All transformations are deterministic
- **Deterministic experiments**: A/B assignment is hash-based, not random
- **Explainable metrics**: Every number can be traced back to raw events
- **Detectable failures**: CI validates analytics integrity end-to-end

## Status

🚧 Under active development — see commit history for progress.
