"""Tests for event schema validation."""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from src.collector.schemas import Event, EventBatch, EventType


class TestEvent:
    def test_valid_event_minimal(self):
        event = Event(user_id="user_1", event_type=EventType.PAGE_VIEW)
        assert event.user_id == "user_1"
        assert event.event_type == EventType.PAGE_VIEW
        assert event.event_id  # auto-generated
        assert event.timestamp  # auto-generated
        assert event.properties == {}

    def test_valid_event_with_properties(self):
        event = Event(
            user_id="user_2",
            event_type=EventType.PURCHASE,
            properties={"amount": 49.99, "currency": "USD"},
        )
        assert event.properties["amount"] == 49.99

    def test_valid_event_with_explicit_id(self):
        event = Event(
            event_id="custom_id_123",
            user_id="user_3",
            event_type=EventType.CLICK,
        )
        assert event.event_id == "custom_id_123"

    def test_empty_user_id_rejected(self):
        with pytest.raises(ValidationError, match="user_id must not be empty"):
            Event(user_id="", event_type=EventType.PAGE_VIEW)

    def test_whitespace_user_id_rejected(self):
        with pytest.raises(ValidationError, match="user_id must not be empty"):
            Event(user_id="   ", event_type=EventType.PAGE_VIEW)

    def test_user_id_stripped(self):
        event = Event(user_id="  user_1  ", event_type=EventType.PAGE_VIEW)
        assert event.user_id == "user_1"

    def test_invalid_event_type_rejected(self):
        with pytest.raises(ValidationError):
            Event(user_id="user_1", event_type="not_a_real_type")

    def test_future_timestamp_rejected(self):
        future = datetime(2099, 1, 1, tzinfo=timezone.utc)
        with pytest.raises(ValidationError, match="must not be in the future"):
            Event(
                user_id="user_1",
                event_type=EventType.PAGE_VIEW,
                timestamp=future,
            )

    def test_all_event_types_valid(self):
        for event_type in EventType:
            event = Event(user_id="user_1", event_type=event_type)
            assert event.event_type == event_type


class TestEventBatch:
    def test_valid_batch(self):
        events = [
            Event(user_id="user_1", event_type=EventType.PAGE_VIEW),
            Event(user_id="user_2", event_type=EventType.CLICK),
        ]
        batch = EventBatch(events=events)
        assert len(batch.events) == 2

    def test_empty_batch_rejected(self):
        with pytest.raises(ValidationError):
            EventBatch(events=[])

    def test_single_event_batch(self):
        batch = EventBatch(
            events=[Event(user_id="user_1", event_type=EventType.SIGNUP)]
        )
        assert len(batch.events) == 1
