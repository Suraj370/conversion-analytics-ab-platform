"""Tests for CI analytics validation script."""

from ci.validate_analytics import validate


def _valid_data():
    """Return minimal valid dashboard data."""
    return {
        "event_summary": [
            {"event_type": "page_view", "count": 1000, "unique_users": 500},
            {"event_type": "signup", "count": 200, "unique_users": 200},
            {"event_type": "purchase", "count": 50, "unique_users": 50},
        ],
        "funnel": [
            {"step": "page_view", "step_order": 1, "users": 500, "conversion_rate_pct": 100.0},
            {"step": "signup", "step_order": 2, "users": 200, "conversion_rate_pct": 40.0},
            {"step": "purchase", "step_order": 3, "users": 50, "conversion_rate_pct": 10.0},
        ],
        "experiments": [
            {
                "experiment_id": "exp_1",
                "variants": [
                    {"name": "control", "users": 250, "conversions": 20, "conversion_rate": 0.08},
                    {"name": "treatment", "users": 250, "conversions": 30, "conversion_rate": 0.12},
                ],
                "analysis": {
                    "absolute_uplift": 0.04,
                    "relative_uplift": 0.5,
                    "p_value": 0.12,
                    "ci_lower": -0.01,
                    "ci_upper": 0.09,
                    "is_significant": False,
                    "decision": "DO NOT SHIP",
                    "reason": "Not significant",
                },
            }
        ],
    }


class TestValidate:
    def test_valid_data_passes(self):
        errors = validate(_valid_data())
        assert errors == []

    def test_missing_top_level_key(self):
        data = _valid_data()
        del data["funnel"]
        errors = validate(data)
        assert any("Missing top-level key: funnel" in e for e in errors)

    def test_empty_event_summary(self):
        data = _valid_data()
        data["event_summary"] = []
        errors = validate(data)
        assert any("event_summary is empty" in e for e in errors)

    def test_missing_required_event_type(self):
        data = _valid_data()
        data["event_summary"] = [
            {"event_type": "page_view", "count": 100, "unique_users": 50},
        ]
        errors = validate(data)
        assert any("missing required type: signup" in e for e in errors)

    def test_empty_funnel(self):
        data = _valid_data()
        data["funnel"] = []
        errors = validate(data)
        assert any("funnel is empty" in e for e in errors)

    def test_funnel_wrong_order(self):
        data = _valid_data()
        data["funnel"][0]["users"] = 100
        data["funnel"][1]["users"] = 200  # Increasing = bad
        errors = validate(data)
        assert any("monotonically decreasing" in e for e in errors)

    def test_empty_experiments(self):
        data = _valid_data()
        data["experiments"] = []
        errors = validate(data)
        assert any("experiments is empty" in e for e in errors)

    def test_missing_control_variant(self):
        data = _valid_data()
        data["experiments"][0]["variants"] = [
            {"name": "treatment", "users": 250, "conversions": 30, "conversion_rate": 0.12},
        ]
        errors = validate(data)
        assert any("missing 'control'" in e for e in errors)

    def test_missing_analysis(self):
        data = _valid_data()
        del data["experiments"][0]["analysis"]
        errors = validate(data)
        assert any("missing analysis results" in e for e in errors)

    def test_invalid_p_value(self):
        data = _valid_data()
        data["experiments"][0]["analysis"]["p_value"] = 1.5
        errors = validate(data)
        assert any("p-value out of range" in e for e in errors)

    def test_invalid_decision(self):
        data = _valid_data()
        data["experiments"][0]["analysis"]["decision"] = "MAYBE"
        errors = validate(data)
        assert any("invalid decision" in e for e in errors)

    def test_zero_users_in_variant(self):
        data = _valid_data()
        data["experiments"][0]["variants"][0]["users"] = 0
        errors = validate(data)
        assert any("0 users" in e for e in errors)
