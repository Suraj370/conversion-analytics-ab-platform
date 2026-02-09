"""FastAPI event collector service.

Accepts analytics events via HTTP, validates them against the schema,
and returns acceptance status. No persistence yet — that comes in Commit 3.
"""

from fastapi import FastAPI

from src.collector.schemas import Event, EventBatch, IngestResponse

app = FastAPI(
    title="Analytics Event Collector",
    description="Ingests and validates analytics events for the startup-analytics-ab platform.",
    version="0.1.0",
)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/ingest", response_model=IngestResponse)
def ingest_events(batch: EventBatch) -> IngestResponse:
    """Accept a batch of events.

    Currently validates the event schema only.
    Persistence will be added when the warehouse layer is ready.
    """
    # For now, all valid events are "accepted" — no storage yet
    return IngestResponse(accepted=len(batch.events))


@app.post("/ingest/single", response_model=IngestResponse)
def ingest_single(event: Event) -> IngestResponse:
    """Convenience endpoint to ingest a single event."""
    return IngestResponse(accepted=1)
