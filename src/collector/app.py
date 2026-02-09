"""FastAPI event collector service.

Accepts analytics events via HTTP, validates them against the schema,
persists them to the DuckDB warehouse, and returns acceptance status.
"""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from src.collector.schemas import Event, EventBatch, IngestResponse
from src.warehouse.db import get_connection, init_db, insert_events

DB_PATH = Path("data/analytics.duckdb")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize the warehouse on startup."""
    conn = get_connection(DB_PATH)
    init_db(conn)
    app.state.db = conn
    yield
    conn.close()


app = FastAPI(
    title="Analytics Event Collector",
    description="Ingests and validates analytics events for the startup-analytics-ab platform.",
    version="0.2.0",
    lifespan=lifespan,
)


def _events_to_dicts(events: list[Event]) -> list[dict]:
    return [e.model_dump(mode="json") for e in events]


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/ingest", response_model=IngestResponse)
def ingest_events(batch: EventBatch) -> IngestResponse:
    """Accept a batch of events, persist to warehouse."""
    rows = _events_to_dicts(batch.events)
    inserted, duplicates = insert_events(app.state.db, rows)
    return IngestResponse(accepted=inserted, duplicate_count=duplicates)


@app.post("/ingest/single", response_model=IngestResponse)
def ingest_single(event: Event) -> IngestResponse:
    """Convenience endpoint to ingest a single event."""
    rows = _events_to_dicts([event])
    inserted, duplicates = insert_events(app.state.db, rows)
    return IngestResponse(accepted=inserted, duplicate_count=duplicates)
