"""Tests that validate dbt staging SQL logic directly against DuckDB.

Since dbt-core requires Python <=3.12, we validate the transformation
SQL by running it directly against DuckDB with test data. This ensures
the staging model logic is correct regardless of the dbt runtime.
"""

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from src.warehouse.db import init_db, insert_events

STAGING_SQL_PATH = Path("dbt/models/staging/stg_events.sql")


@pytest.fixture
def db():
    """In-memory DuckDB with test events loaded."""
    conn = duckdb.connect(":memory:")
    init_db(conn)

    events = [
        {
            "event_id": "evt_pv_1",
            "user_id": "user_001",
            "event_type": "page_view",
            "timestamp": datetime(2025, 1, 15, 10, 0, 0, tzinfo=timezone.utc).isoformat(),
            "properties": {"page": "/pricing"},
        },
        {
            "event_id": "evt_click_1",
            "user_id": "user_001",
            "event_type": "click",
            "timestamp": datetime(2025, 1, 15, 10, 1, 0, tzinfo=timezone.utc).isoformat(),
            "properties": {"target": "cta_hero"},
        },
        {
            "event_id": "evt_signup_1",
            "user_id": "user_001",
            "event_type": "signup",
            "timestamp": datetime(2025, 1, 15, 10, 5, 0, tzinfo=timezone.utc).isoformat(),
            "properties": {"source": "web"},
        },
        {
            "event_id": "evt_purchase_1",
            "user_id": "user_001",
            "event_type": "purchase",
            "timestamp": datetime(2025, 1, 15, 11, 0, 0, tzinfo=timezone.utc).isoformat(),
            "properties": {"plan": "pro", "amount": 99.0},
        },
    ]
    insert_events(conn, events)
    yield conn
    conn.close()


def _run_staging_query(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    """Execute the staging model SQL, replacing dbt source() with direct table ref."""
    sql = STAGING_SQL_PATH.read_text()
    # Replace dbt source() macro with direct table reference
    sql = sql.replace("{{ source('raw', 'raw_events') }}", "raw_events")
    result = conn.execute(sql).fetchdf()
    return result.to_dict(orient="records")


class TestStagingModel:
    def test_all_events_staged(self, db):
        rows = _run_staging_query(db)
        assert len(rows) == 4

    def test_event_id_preserved(self, db):
        rows = _run_staging_query(db)
        ids = {r["event_id"] for r in rows}
        assert "evt_pv_1" in ids
        assert "evt_purchase_1" in ids

    def test_timestamp_renamed(self, db):
        rows = _run_staging_query(db)
        for row in rows:
            assert "event_timestamp" in row
            assert row["event_timestamp"] is not None

    def test_page_extracted_for_page_view(self, db):
        rows = _run_staging_query(db)
        pv = next(r for r in rows if r["event_id"] == "evt_pv_1")
        assert pv["page"] == "/pricing"

    def test_click_target_extracted(self, db):
        rows = _run_staging_query(db)
        click = next(r for r in rows if r["event_id"] == "evt_click_1")
        assert click["click_target"] == "cta_hero"

    def test_signup_source_extracted(self, db):
        rows = _run_staging_query(db)
        signup = next(r for r in rows if r["event_id"] == "evt_signup_1")
        assert signup["signup_source"] == "web"

    def test_purchase_plan_extracted(self, db):
        rows = _run_staging_query(db)
        purchase = next(r for r in rows if r["event_id"] == "evt_purchase_1")
        assert purchase["purchase_plan"] == "pro"

    def test_purchase_amount_cast_to_double(self, db):
        rows = _run_staging_query(db)
        purchase = next(r for r in rows if r["event_id"] == "evt_purchase_1")
        assert purchase["purchase_amount"] == 99.0
        assert isinstance(purchase["purchase_amount"], float)

    def test_null_properties_dont_fail(self, db):
        """Non-matching property extractions should return null, not error."""
        rows = _run_staging_query(db)
        pv = next(r for r in rows if r["event_id"] == "evt_pv_1")
        # page_view shouldn't have purchase_plan (pandas converts NULL to NaN)
        import math
        assert pv["purchase_plan"] is None or (isinstance(pv["purchase_plan"], float) and math.isnan(pv["purchase_plan"]))
