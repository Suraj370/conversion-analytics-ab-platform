"""Tests for the event collector API endpoints."""

from fastapi.testclient import TestClient

from src.collector.app import app

client = TestClient(app)


class TestHealthEndpoint:
    def test_health_returns_ok(self):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


class TestIngestEndpoint:
    def test_ingest_valid_batch(self):
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

    def test_ingest_rejects_empty_batch(self):
        response = client.post("/ingest", json={"events": []})
        assert response.status_code == 422

    def test_ingest_rejects_invalid_event_type(self):
        payload = {
            "events": [{"user_id": "user_1", "event_type": "invalid_type"}]
        }
        response = client.post("/ingest", json=payload)
        assert response.status_code == 422

    def test_ingest_rejects_missing_user_id(self):
        payload = {"events": [{"event_type": "page_view"}]}
        response = client.post("/ingest", json=payload)
        assert response.status_code == 422


class TestIngestSingleEndpoint:
    def test_ingest_single_valid_event(self):
        payload = {"user_id": "user_1", "event_type": "signup"}
        response = client.post("/ingest/single", json=payload)
        assert response.status_code == 200
        assert response.json()["accepted"] == 1

    def test_ingest_single_with_properties(self):
        payload = {
            "user_id": "user_1",
            "event_type": "purchase",
            "properties": {"amount": 29.99, "plan": "pro"},
        }
        response = client.post("/ingest/single", json=payload)
        assert response.status_code == 200
        assert response.json()["accepted"] == 1
