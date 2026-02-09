"""Tests for dashboard data export."""

import json
import tempfile
from pathlib import Path

import pytest

from src.ab.experiment import PRICING_PAGE_EXPERIMENT
from src.analysis.export import export_dashboard_data
from src.collector.schemas import Event, EventType
from src.simulator.config import SimulationConfig
from src.simulator.engine import generate_events
from src.warehouse.db import get_connection, init_db, insert_events


@pytest.fixture()
def populated_db(tmp_path):
    """Create an in-memory DB with simulated events including experiment."""
    db_path = str(tmp_path / "test.duckdb")
    conn = get_connection(db_path)
    init_db(conn)

    config = SimulationConfig(num_users=200, days=7, seed=42)
    events = generate_events(config, PRICING_PAGE_EXPERIMENT)
    rows = [e.model_dump(mode="json") for e in events]
    insert_events(conn, rows)
    conn.close()
    return db_path


class TestExportDashboardData:
    def test_export_creates_json_file(self, populated_db, tmp_path):
        out = str(tmp_path / "dashboard" / "data.json")
        data = export_dashboard_data(populated_db, out)

        assert Path(out).exists()
        content = json.loads(Path(out).read_text())
        assert "funnel" in content
        assert "experiments" in content
        assert "event_summary" in content

    def test_json_is_valid(self, populated_db, tmp_path):
        """Exported JSON must be parseable (no numpy types etc)."""
        out = str(tmp_path / "out.json")
        export_dashboard_data(populated_db, out)

        raw = Path(out).read_text()
        data = json.loads(raw)
        # Re-serialize to catch any non-serializable types
        json.dumps(data)

    def test_funnel_has_three_steps(self, populated_db, tmp_path):
        out = str(tmp_path / "out.json")
        data = export_dashboard_data(populated_db, out)

        assert len(data["funnel"]) == 3
        steps = [s["step"] for s in data["funnel"]]
        assert steps == ["page_view", "signup", "purchase"]

    def test_funnel_is_monotonically_decreasing(self, populated_db, tmp_path):
        out = str(tmp_path / "out.json")
        data = export_dashboard_data(populated_db, out)

        users = [s["users"] for s in data["funnel"]]
        assert users[0] >= users[1] >= users[2]

    def test_funnel_conversion_rates(self, populated_db, tmp_path):
        out = str(tmp_path / "out.json")
        data = export_dashboard_data(populated_db, out)

        for step in data["funnel"]:
            assert "conversion_rate_pct" in step
            assert 0 <= step["conversion_rate_pct"] <= 100

    def test_experiment_present(self, populated_db, tmp_path):
        out = str(tmp_path / "out.json")
        data = export_dashboard_data(populated_db, out)

        assert len(data["experiments"]) >= 1
        exp = data["experiments"][0]
        assert exp["experiment_id"] == "exp_pricing_page_v1"

    def test_experiment_has_two_variants(self, populated_db, tmp_path):
        out = str(tmp_path / "out.json")
        data = export_dashboard_data(populated_db, out)

        exp = data["experiments"][0]
        names = {v["name"] for v in exp["variants"]}
        assert names == {"control", "treatment"}

    def test_experiment_analysis_present(self, populated_db, tmp_path):
        out = str(tmp_path / "out.json")
        data = export_dashboard_data(populated_db, out)

        exp = data["experiments"][0]
        assert "analysis" in exp
        a = exp["analysis"]
        assert "p_value" in a
        assert "decision" in a
        assert "is_significant" in a
        assert "relative_uplift" in a
        assert "ci_lower" in a
        assert "ci_upper" in a

    def test_analysis_types_are_json_native(self, populated_db, tmp_path):
        """All analysis values must be native JSON types (not numpy)."""
        out = str(tmp_path / "out.json")
        data = export_dashboard_data(populated_db, out)

        a = data["experiments"][0]["analysis"]
        assert isinstance(a["p_value"], float)
        assert isinstance(a["is_significant"], bool)
        assert isinstance(a["relative_uplift"], float)
        assert isinstance(a["decision"], str)

    def test_event_summary_present(self, populated_db, tmp_path):
        out = str(tmp_path / "out.json")
        data = export_dashboard_data(populated_db, out)

        assert len(data["event_summary"]) > 0
        for item in data["event_summary"]:
            assert "event_type" in item
            assert "count" in item
            assert "unique_users" in item

    def test_empty_database(self, tmp_path):
        """Export with no data should produce empty arrays, not crash."""
        db_path = str(tmp_path / "empty.duckdb")
        conn = get_connection(db_path)
        init_db(conn)
        conn.close()

        out = str(tmp_path / "empty.json")
        data = export_dashboard_data(db_path, out)

        assert data["funnel"] == []
        assert data["experiments"] == []
        assert data["event_summary"] == []
