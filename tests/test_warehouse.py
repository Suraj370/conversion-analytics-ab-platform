"""Tests for the DuckDB warehouse layer."""

from datetime import datetime, timezone

import pytest

from src.warehouse.db import count_events, get_connection, init_db, insert_events


@pytest.fixture
def db():
    """In-memory DuckDB connection for testing."""
    conn = get_connection(db_path=":memory:")
    init_db(conn)
    yield conn
    conn.close()


def _make_event(event_id: str = "evt_1", user_id: str = "user_1") -> dict:
    return {
        "event_id": event_id,
        "user_id": user_id,
        "event_type": "page_view",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "properties": {"page": "/home"},
    }


class TestInitDb:
    def test_creates_raw_events_table(self, db):
        tables = db.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'raw_events'"
        ).fetchall()
        assert len(tables) == 1

    def test_idempotent_init(self, db):
        # calling init_db again should not raise
        init_db(db)
        assert count_events(db) == 0


class TestInsertEvents:
    def test_insert_single_event(self, db):
        inserted, dupes = insert_events(db, [_make_event()])
        assert inserted == 1
        assert dupes == 0
        assert count_events(db) == 1

    def test_insert_multiple_events(self, db):
        events = [_make_event(f"evt_{i}") for i in range(5)]
        inserted, dupes = insert_events(db, events)
        assert inserted == 5
        assert dupes == 0
        assert count_events(db) == 5

    def test_duplicate_event_rejected(self, db):
        event = _make_event("evt_dup")
        insert_events(db, [event])
        inserted, dupes = insert_events(db, [event])
        assert inserted == 0
        assert dupes == 1
        assert count_events(db) == 1

    def test_mixed_new_and_duplicate(self, db):
        insert_events(db, [_make_event("evt_1")])
        batch = [_make_event("evt_1"), _make_event("evt_2"), _make_event("evt_3")]
        inserted, dupes = insert_events(db, batch)
        assert inserted == 2
        assert dupes == 1
        assert count_events(db) == 3

    def test_empty_batch(self, db):
        inserted, dupes = insert_events(db, [])
        assert inserted == 0
        assert dupes == 0

    def test_properties_stored_as_json(self, db):
        event = _make_event()
        event["properties"] = {"amount": 49.99, "currency": "USD"}
        insert_events(db, [event])
        row = db.execute(
            "SELECT properties FROM raw_events WHERE event_id = ?", ["evt_1"]
        ).fetchone()
        assert "49.99" in row[0]

    def test_ingested_at_populated(self, db):
        insert_events(db, [_make_event()])
        row = db.execute(
            "SELECT ingested_at FROM raw_events WHERE event_id = 'evt_1'"
        ).fetchone()
        assert row[0] is not None
