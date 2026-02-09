"""Export warehouse data as JSON for the static dashboard.

Reads funnel metrics and experiment results from DuckDB and writes
a single JSON file that the HTML dashboard consumes.

Usage:
    python -m src.analysis.export
    python -m src.analysis.export --db data/analytics.duckdb --out src/dashboard/data.json
"""

import argparse
import json
from pathlib import Path

from src.analysis.stats import VariantStats, analyze_experiment
from src.warehouse.db import get_connection, init_db

_FUNNEL_QUERY = """
WITH user_journey AS (
    SELECT
        user_id,
        MAX(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS reached_page_view,
        MAX(CASE WHEN event_type = 'signup' THEN 1 ELSE 0 END) AS reached_signup,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS reached_purchase
    FROM raw_events
    GROUP BY user_id
)
SELECT
    'page_view' AS step, 1 AS step_order,
    SUM(reached_page_view) AS users
FROM user_journey
UNION ALL
SELECT
    'signup', 2,
    SUM(reached_signup)
FROM user_journey
UNION ALL
SELECT
    'purchase', 3,
    SUM(reached_purchase)
FROM user_journey
ORDER BY step_order
"""

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
)
SELECT
    a.experiment_id,
    a.variant,
    COUNT(*) AS users,
    SUM(CASE WHEN p.user_id IS NOT NULL THEN 1 ELSE 0 END) AS conversions
FROM assignments a
LEFT JOIN purchases p ON a.user_id = p.user_id
GROUP BY a.experiment_id, a.variant
ORDER BY a.experiment_id, a.variant
"""

_EVENT_SUMMARY_QUERY = """
SELECT
    event_type,
    COUNT(*) AS count,
    COUNT(DISTINCT user_id) AS unique_users
FROM raw_events
GROUP BY event_type
ORDER BY count DESC
"""


def export_dashboard_data(
    db_path: str = "data/analytics.duckdb",
    output_path: str = "src/dashboard/data.json",
) -> dict:
    """Export all dashboard data to a JSON file."""
    conn = get_connection(db_path)
    init_db(conn)

    data = {
        "funnel": _export_funnel(conn),
        "experiments": _export_experiments(conn),
        "event_summary": _export_event_summary(conn),
    }

    conn.close()

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, indent=2))
    print(f"Dashboard data exported to {output_path}")
    return data


def _export_funnel(conn) -> list[dict]:
    rows = conn.execute(_FUNNEL_QUERY).fetchall()
    if not rows:
        return []

    # SUM on empty table returns NULL — treat as 0
    first_users = rows[0][2]
    if first_users is None or first_users == 0:
        return []

    total = int(first_users)
    funnel = []
    prev_users = None
    for step, order, users in rows:
        users = int(users or 0)
        entry = {
            "step": step,
            "step_order": order,
            "users": users,
            "conversion_rate_pct": round(users * 100.0 / total, 2) if total else 0,
        }
        if prev_users is not None:
            entry["step_conversion_rate_pct"] = round(users * 100.0 / prev_users, 2) if prev_users else 0
        else:
            entry["step_conversion_rate_pct"] = 100.0
        prev_users = users
        funnel.append(entry)
    return funnel


def _export_experiments(conn) -> list[dict]:
    rows = conn.execute(_EXPERIMENT_QUERY).fetchall()
    if not rows:
        return []

    # Group by experiment
    experiments: dict[str, dict] = {}
    for exp_id, variant, users, conversions in rows:
        if exp_id not in experiments:
            experiments[exp_id] = {"experiment_id": exp_id, "variants": {}}
        experiments[exp_id]["variants"][variant] = {
            "name": variant,
            "users": int(users),
            "conversions": int(conversions),
            "conversion_rate": round(int(conversions) / int(users), 6) if users else 0,
        }

    results = []
    for exp_id, exp_data in experiments.items():
        control_data = exp_data["variants"].get("control")
        treatment_data = exp_data["variants"].get("treatment")

        entry = {
            "experiment_id": exp_id,
            "variants": list(exp_data["variants"].values()),
        }

        if control_data and treatment_data:
            control = VariantStats("control", control_data["users"], control_data["conversions"])
            treatment = VariantStats("treatment", treatment_data["users"], treatment_data["conversions"])
            result = analyze_experiment(exp_id, control, treatment)
            entry["analysis"] = {
                "absolute_uplift": float(result.absolute_uplift),
                "relative_uplift": float(result.relative_uplift),
                "p_value": float(result.p_value),
                "ci_lower": float(result.ci_lower),
                "ci_upper": float(result.ci_upper),
                "is_significant": bool(result.is_significant),
                "decision": result.decision,
                "reason": result.reason,
            }

        results.append(entry)
    return results


def _export_event_summary(conn) -> list[dict]:
    rows = conn.execute(_EVENT_SUMMARY_QUERY).fetchall()
    return [
        {"event_type": et, "count": int(c), "unique_users": int(u)}
        for et, c, u in rows
    ]


def main(args: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Export dashboard data")
    parser.add_argument("--db", default="data/analytics.duckdb", help="Database path")
    parser.add_argument("--out", default="src/dashboard/data.json", help="Output JSON path")
    opts = parser.parse_args(args)
    export_dashboard_data(opts.db, opts.out)


if __name__ == "__main__":
    main()
