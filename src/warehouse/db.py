"""DuckDB warehouse initialization and connection management.

The warehouse stores raw analytics events in an append-only table.
DuckDB is used as a local, zero-cost analytical database.
"""

import json
from pathlib import Path

import duckdb

DEFAULT_DB_PATH = Path("data/analytics.duckdb")

# Schema for the raw events table
_CREATE_EVENTS_TABLE = """
CREATE TABLE IF NOT EXISTS raw_events (
    event_id    VARCHAR PRIMARY KEY,
    user_id     VARCHAR NOT NULL,
    event_type  VARCHAR NOT NULL,
    timestamp   TIMESTAMP WITH TIME ZONE NOT NULL,
    properties  JSON,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
"""


def get_connection(db_path: Path | str = DEFAULT_DB_PATH) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection, creating the database file if needed.

    Pass \":memory:\" for an in-memory database (useful for testing).
    """
    if str(db_path) == ":memory:":
        return duckdb.connect(":memory:")
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def init_db(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the raw_events table if it doesn't exist."""
    conn.execute(_CREATE_EVENTS_TABLE)


def insert_events(
    conn: duckdb.DuckDBPyConnection, events: list[dict]
) -> tuple[int, int]:
    """Insert events into the warehouse, skipping duplicates.

    Returns (inserted_count, duplicate_count).
    Idempotency is enforced by the PRIMARY KEY on event_id —
    duplicate event_ids are silently ignored.
    """
    if not events:
        return 0, 0

    inserted = 0
    duplicates = 0

    for event in events:
        props_json = json.dumps(event.get("properties", {}))
        try:
            conn.execute(
                """
                INSERT INTO raw_events (event_id, user_id, event_type, timestamp, properties)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    event["event_id"],
                    event["user_id"],
                    event["event_type"],
                    event["timestamp"],
                    props_json,
                ],
            )
            inserted += 1
        except duckdb.ConstraintException:
            duplicates += 1

    return inserted, duplicates


def count_events(conn: duckdb.DuckDBPyConnection) -> int:
    """Return the total number of events in the warehouse."""
    result = conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()
    return result[0]
