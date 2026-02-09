"""Tests for the event collector API endpoints."""

import importlib
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    """Test client backed by an in-memory DuckDB."""
    with patch("src.collector.app.DB_PATH", ":memory:"):
        import src.collector.app as app_module

        importlib.reload(app_module)
        with TestClient(app_module.app) as c:
            yield c


class TestHealthEndpoint:
    def test_health_returns_ok(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


class TestIngestEndpoint:
    def test_ingest_valid_batch(self, client):
        payload = {
            "events": [
                {"user_id": "user_1", "event_type": "page_view"},
                {"user_id": "user_2", "event_type": "click"},
            ]
        }
        response = client.post("/ingest", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["accepted"] == 2
        assert data["duplicate_count"] == 0
        assert data["errors"] == []

    def test_ingest_rejects_empty_batch(self, client):
        response = client.post("/ingest", json={"events": []})
        assert response.status_code == 422

    def test_ingest_rejects_invalid_event_type(self, client):
        payload = {
            "events": [{"user_id": "user_1", "event_type": "invalid_type"}]
        }
        response = client.post("/ingest", json=payload)
        assert response.status_code == 422

    def test_ingest_rejects_missing_user_id(self, client):
        payload = {"events": [{"event_type": "page_view"}]}
        response = client.post("/ingest", json=payload)
        assert response.status_code == 422

    def test_ingest_duplicate_events(self, client):
        event = {"user_id": "user_dup", "event_type": "page_view", "event_id": "dup_test_1"}
        client.post("/ingest/single", json=event)
        response = client.post("/ingest/single", json=event)
        assert response.status_code == 200
        data = response.json()
        assert data["accepted"] == 0
        assert data["duplicate_count"] == 1


class TestIngestSingleEndpoint:
    def test_ingest_single_valid_event(self, client):
        payload = {"user_id": "user_1", "event_type": "signup"}
        response = client.post("/ingest/single", json=payload)
        assert response.status_code == 200
        assert response.json()["accepted"] == 1

    def test_ingest_single_with_properties(self, client):
        payload = {
            "user_id": "user_1",
            "event_type": "purchase",
            "properties": {"amount": 29.99, "plan": "pro"},
        }
        response = client.post("/ingest/single", json=payload)
        assert response.status_code == 200
        assert response.json()["accepted"] == 1
