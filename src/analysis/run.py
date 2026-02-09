"""CLI entrypoint: run experiment analysis against the warehouse.

Reads experiment assignment and conversion data from DuckDB,
runs the statistical analysis, and prints a decision report.

Usage:
    python -m src.analysis.run
    python -m src.analysis.run --db data/analytics.duckdb
"""

import argparse
from pathlib import Path

from src.analysis.stats import (
    ExperimentResult,
    VariantStats,
    analyze_experiment,
    format_report,
)
from src.warehouse.db import get_connection, init_db

# SQL that mirrors the fct_experiment_results dbt model logic
_EXPERIMENT_QUERY = """
WITH assignments AS (
    SELECT
        user_id,
        json_extract_string(properties, '$.experiment_id') AS experiment_id,
        json_extract_string(properties, '$.variant') AS variant
    FROM raw_events
    WHERE event_type = 'experiment_assignment'
),
purchases AS (
    SELECT DISTINCT user_id
    FROM raw_events
    WHERE event_type = 'purchase'
),
experiment_users AS (
    SELECT
        a.experiment_id,
        a.variant,
        a.user_id,
        CASE WHEN p.user_id IS NOT NULL THEN 1 ELSE 0 END AS converted
    FROM assignments a
    LEFT JOIN purchases p ON a.user_id = p.user_id
)
SELECT
    experiment_id,
    variant,
    COUNT(*) AS users,
    SUM(converted) AS conversions
FROM experiment_users
GROUP BY experiment_id, variant
ORDER BY experiment_id, variant
"""


def run_analysis(db_path: str = "data/analytics.duckdb") -> list[ExperimentResult]:
    """Run analysis for all experiments in the warehouse."""
    conn = get_connection(db_path)
    init_db(conn)

    rows = conn.execute(_EXPERIMENT_QUERY).fetchall()
    conn.close()

    if not rows:
        print("No experiment data found in warehouse.")
        return []

    # Group rows by experiment_id
    experiments: dict[str, dict[str, VariantStats]] = {}
    for experiment_id, variant, users, conversions in rows:
        if experiment_id not in experiments:
            experiments[experiment_id] = {}
        experiments[experiment_id][variant] = VariantStats(
            name=variant, users=users, conversions=conversions,
        )

    results = []
    for exp_id, variants in experiments.items():
        control = variants.get("control")
        treatment = variants.get("treatment")

        if not control or not treatment:
            print(f"Skipping {exp_id}: missing control or treatment variant")
            continue

        result = analyze_experiment(exp_id, control, treatment)
        results.append(result)
        print(format_report(result))
        print()

    return results


def main(args: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run experiment analysis")
    parser.add_argument("--db", type=str, default="data/analytics.duckdb", help="Database path")
    opts = parser.parse_args(args)
    run_analysis(opts.db)


if __name__ == "__main__":
    main()
