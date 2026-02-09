"""Event schema definitions for the analytics collector.

All events follow a common envelope (user_id, event_type, timestamp)
with a flexible properties dict for event-specific data.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


class EventType(str, Enum):
    PAGE_VIEW = "page_view"
    CLICK = "click"
    SIGNUP = "signup"
    PURCHASE = "purchase"
    EXPERIMENT_ASSIGNMENT = "experiment_assignment"
    CUSTOM = "custom"


class Event(BaseModel):
    """Core event envelope.

    Every event has a unique ID, a user, a type, a timestamp, and
    an optional properties bag for event-specific fields.
    """

    event_id: str = Field(default_factory=lambda: uuid4().hex)
    user_id: str
    event_type: EventType
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    properties: dict[str, Any] = Field(default_factory=dict)

    @field_validator("user_id")
    @classmethod
    def user_id_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("user_id must not be empty")
        return v.strip()

    @field_validator("timestamp")
    @classmethod
    def timestamp_not_future(cls, v: datetime) -> datetime:
        now = datetime.now(timezone.utc)
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        if v > now:
            raise ValueError("timestamp must not be in the future")
        return v


class EventBatch(BaseModel):
    """A batch of events submitted in a single request."""

    events: list[Event] = Field(..., min_length=1, max_length=1000)


class IngestResponse(BaseModel):
    """Response returned after event ingestion."""

    accepted: int
    duplicate_count: int = 0
    errors: list[str] = Field(default_factory=list)
