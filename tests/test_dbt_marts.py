"""Tests that validate dbt mart SQL logic directly against DuckDB.

Runs the full model chain: raw_events -> stg_events -> fct_user_journey -> fct_funnel / fct_experiment_results.
"""

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from src.warehouse.db import init_db, insert_events

SQL_DIR = Path("dbt/models")


def _read_sql(path: Path) -> str:
    return path.read_text()


def _resolve_refs(sql: str, views: dict[str, str]) -> str:
    """Replace {{ ref('name') }} and {{ source(...) }} with actual table/view names."""
    sql = sql.replace("{{ source('raw', 'raw_events') }}", "raw_events")
    for name, view_name in views.items():
        sql = sql.replace(f"{{{{ ref('{name}') }}}}", view_name)
    return sql


@pytest.fixture
def db():
    """In-memory DuckDB with test data and all models materialized."""
    conn = duckdb.connect(":memory:")
    init_db(conn)

    # Insert a mix of users at different funnel stages
    events = [
        # User A: full funnel (page_view -> signup -> purchase)
        _evt("a_pv", "user_a", "page_view", h=1, props={"page": "/"}),
        _evt("a_su", "user_a", "signup", h=2, props={"source": "web"}),
        _evt("a_pu", "user_a", "purchase", h=3, props={"plan": "pro", "amount": 99.0}),
        # User B: page_view + signup only (no purchase)
        _evt("b_pv", "user_b", "page_view", h=1, props={"page": "/pricing"}),
        _evt("b_su", "user_b", "signup", h=2, props={"source": "web"}),
        # User C: page_view only
        _evt("c_pv", "user_c", "page_view", h=1, props={"page": "/features"}),
        # User D: full funnel with experiment assignment
        _evt("d_pv", "user_d", "page_view", h=1, props={"page": "/"}),
        _evt("d_ex", "user_d", "experiment_assignment", h=1.5,
             props={"experiment_id": "exp_001", "variant": "treatment"}),
        _evt("d_su", "user_d", "signup", h=2, props={"source": "web"}),
        _evt("d_pu", "user_d", "purchase", h=3, props={"plan": "starter", "amount": 29.0}),
        # User E: experiment control, no conversion
        _evt("e_pv", "user_e", "page_view", h=1, props={"page": "/"}),
        _evt("e_ex", "user_e", "experiment_assignment", h=1.5,
             props={"experiment_id": "exp_001", "variant": "control"}),
    ]
    insert_events(conn, events)

    # Materialize models in dependency order
    stg_sql = _resolve_refs(
        _read_sql(SQL_DIR / "staging" / "stg_events.sql"),
        {},
    )
    conn.execute(f"CREATE VIEW stg_events AS {stg_sql}")

    journey_sql = _resolve_refs(
        _read_sql(SQL_DIR / "marts" / "fct_user_journey.sql"),
        {"stg_events": "stg_events"},
    )
    conn.execute(f"CREATE TABLE fct_user_journey AS {journey_sql}")

    funnel_sql = _resolve_refs(
        _read_sql(SQL_DIR / "marts" / "fct_funnel.sql"),
        {"fct_user_journey": "fct_user_journey"},
    )
    conn.execute(f"CREATE TABLE fct_funnel AS {funnel_sql}")

    exp_sql = _resolve_refs(
        _read_sql(SQL_DIR / "marts" / "fct_experiment_results.sql"),
        {"stg_events": "stg_events", "fct_user_journey": "fct_user_journey"},
    )
    conn.execute(f"CREATE TABLE fct_experiment_results AS {exp_sql}")

    yield conn
    conn.close()


def _evt(eid: str, uid: str, etype: str, h: float, props: dict) -> dict:
    """Helper to create event dicts for test fixtures."""
    return {
        "event_id": eid,
        "user_id": uid,
        "event_type": etype,
        "timestamp": datetime(2025, 1, 15, int(h), int((h % 1) * 60), 0, tzinfo=timezone.utc).isoformat(),
        "properties": props,
    }


class TestUserJourney:
    def test_one_row_per_user(self, db):
        rows = db.execute("SELECT * FROM fct_user_journey").fetchdf()
        assert len(rows) == 5  # users A through E

    def test_user_ids_unique(self, db):
        rows = db.execute("SELECT user_id FROM fct_user_journey").fetchdf()
        assert rows["user_id"].nunique() == len(rows)

    def test_full_funnel_user(self, db):
        row = db.execute(
            "SELECT * FROM fct_user_journey WHERE user_id = 'user_a'"
        ).fetchdf().to_dict("records")[0]
        assert row["reached_page_view"]
        assert row["reached_signup"]
        assert row["reached_purchase"]
        assert row["is_converted"]

    def test_partial_funnel_user(self, db):
        row = db.execute(
            "SELECT * FROM fct_user_journey WHERE user_id = 'user_b'"
        ).fetchdf().to_dict("records")[0]
        assert row["reached_page_view"]
        assert row["reached_signup"]
        assert not row["reached_purchase"]
        assert not row["is_converted"]

    def test_page_view_only_user(self, db):
        row = db.execute(
            "SELECT * FROM fct_user_journey WHERE user_id = 'user_c'"
        ).fetchdf().to_dict("records")[0]
        assert row["reached_page_view"]
        assert not row["reached_signup"]
        assert not row["reached_purchase"]

    def test_event_counts(self, db):
        row = db.execute(
            "SELECT * FROM fct_user_journey WHERE user_id = 'user_a'"
        ).fetchdf().to_dict("records")[0]
        assert row["page_view_count"] == 1
        assert row["signup_count"] == 1
        assert row["purchase_count"] == 1
        assert row["total_events"] == 3


class TestFunnel:
    def test_three_funnel_steps(self, db):
        rows = db.execute("SELECT * FROM fct_funnel ORDER BY step_order").fetchdf()
        assert len(rows) == 3
        assert list(rows["step"]) == ["page_view", "signup", "purchase"]

    def test_funnel_is_monotonically_decreasing(self, db):
        rows = db.execute(
            "SELECT users_reached FROM fct_funnel ORDER BY step_order"
        ).fetchdf()
        values = list(rows["users_reached"])
        for i in range(1, len(values)):
            assert values[i] <= values[i - 1], f"Step {i} has more users than step {i-1}"

    def test_page_view_count(self, db):
        row = db.execute(
            "SELECT users_reached FROM fct_funnel WHERE step = 'page_view'"
        ).fetchone()
        assert row[0] == 5  # all 5 users had page views

    def test_signup_count(self, db):
        row = db.execute(
            "SELECT users_reached FROM fct_funnel WHERE step = 'signup'"
        ).fetchone()
        assert row[0] == 3  # users A, B, D signed up

    def test_purchase_count(self, db):
        row = db.execute(
            "SELECT users_reached FROM fct_funnel WHERE step = 'purchase'"
        ).fetchone()
        assert row[0] == 2  # users A, D purchased

    def test_conversion_rate_calculated(self, db):
        row = db.execute(
            "SELECT conversion_rate_pct FROM fct_funnel WHERE step = 'purchase'"
        ).fetchone()
        # 2 purchases / 5 page views = 40%
        assert row[0] == 40.0


class TestExperimentResults:
    def test_two_variants(self, db):
        rows = db.execute("SELECT * FROM fct_experiment_results").fetchdf()
        assert len(rows) == 2

    def test_treatment_conversion(self, db):
        row = db.execute(
            "SELECT * FROM fct_experiment_results WHERE variant = 'treatment'"
        ).fetchdf().to_dict("records")[0]
        assert row["users"] == 1
        assert row["conversions"] == 1
        assert row["conversion_rate"] == 1.0

    def test_control_no_conversion(self, db):
        row = db.execute(
            "SELECT * FROM fct_experiment_results WHERE variant = 'control'"
        ).fetchdf().to_dict("records")[0]
        assert row["users"] == 1
        assert row["conversions"] == 0
        assert row["conversion_rate"] == 0.0

    def test_experiment_id_present(self, db):
        rows = db.execute(
            "SELECT DISTINCT experiment_id FROM fct_experiment_results"
        ).fetchdf()
        assert list(rows["experiment_id"]) == ["exp_001"]
