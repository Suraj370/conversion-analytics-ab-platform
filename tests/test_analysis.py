"""Tests for statistical experiment analysis."""

import pytest

from src.analysis.stats import (
    ExperimentResult,
    VariantStats,
    analyze_experiment,
    format_report,
)


class TestVariantStats:
    def test_conversion_rate(self):
        v = VariantStats(name="control", users=1000, conversions=100)
        assert v.conversion_rate == 0.1

    def test_conversion_rate_zero_users(self):
        v = VariantStats(name="control", users=0, conversions=0)
        assert v.conversion_rate == 0.0


class TestAnalyzeExperiment:
    def test_significant_positive_result(self):
        """Large sample with clear uplift should produce SHIP."""
        control = VariantStats("control", users=5000, conversions=500)    # 10%
        treatment = VariantStats("treatment", users=5000, conversions=650)  # 13%
        result = analyze_experiment("exp_1", control, treatment)

        assert result.decision == "SHIP"
        assert result.is_significant
        assert result.absolute_uplift > 0
        assert result.relative_uplift > 0
        assert result.p_value < 0.05
        assert result.ci_lower > 0

    def test_no_significant_difference(self):
        """Similar rates with moderate sample should produce DO NOT SHIP."""
        control = VariantStats("control", users=500, conversions=50)     # 10%
        treatment = VariantStats("treatment", users=500, conversions=52)   # 10.4%
        result = analyze_experiment("exp_2", control, treatment)

        assert result.decision == "DO NOT SHIP"
        assert not result.is_significant
        assert result.p_value >= 0.05

    def test_significant_negative_result(self):
        """Treatment worse than control should produce DO NOT SHIP."""
        control = VariantStats("control", users=5000, conversions=500)    # 10%
        treatment = VariantStats("treatment", users=5000, conversions=350)  # 7%
        result = analyze_experiment("exp_3", control, treatment)

        assert result.decision == "DO NOT SHIP"
        assert result.is_significant
        assert result.absolute_uplift < 0
        assert "NEGATIVE" in result.reason

    def test_insufficient_sample_size(self):
        """Too few users should produce INCONCLUSIVE."""
        control = VariantStats("control", users=20, conversions=2)
        treatment = VariantStats("treatment", users=20, conversions=5)
        result = analyze_experiment("exp_4", control, treatment)

        assert result.decision == "INCONCLUSIVE"
        assert "sample size" in result.reason.lower()

    def test_zero_conversions_both(self):
        """No conversions in either variant should not crash."""
        control = VariantStats("control", users=1000, conversions=0)
        treatment = VariantStats("treatment", users=1000, conversions=0)
        result = analyze_experiment("exp_5", control, treatment)

        assert result.absolute_uplift == 0.0
        assert result.decision == "DO NOT SHIP"

    def test_perfect_conversion_both(self):
        """100% conversion in both should not crash."""
        control = VariantStats("control", users=1000, conversions=1000)
        treatment = VariantStats("treatment", users=1000, conversions=1000)
        result = analyze_experiment("exp_6", control, treatment)

        assert result.absolute_uplift == 0.0

    def test_confidence_interval_contains_uplift(self):
        """The CI should always contain the point estimate."""
        control = VariantStats("control", users=2000, conversions=200)
        treatment = VariantStats("treatment", users=2000, conversions=260)
        result = analyze_experiment("exp_7", control, treatment)

        assert result.ci_lower <= result.absolute_uplift <= result.ci_upper

    def test_custom_confidence_level(self):
        """99% confidence should be stricter than 95%."""
        control = VariantStats("control", users=1000, conversions=100)
        treatment = VariantStats("treatment", users=1000, conversions=130)

        result_95 = analyze_experiment("exp_8", control, treatment, confidence_level=0.95)
        result_99 = analyze_experiment("exp_8", control, treatment, confidence_level=0.99)

        # 99% CI should be wider
        width_95 = result_95.ci_upper - result_95.ci_lower
        width_99 = result_99.ci_upper - result_99.ci_lower
        assert width_99 > width_95

    def test_result_has_all_fields(self):
        control = VariantStats("control", users=1000, conversions=100)
        treatment = VariantStats("treatment", users=1000, conversions=130)
        result = analyze_experiment("exp_9", control, treatment)

        assert result.experiment_id == "exp_9"
        assert result.control == control
        assert result.treatment == treatment
        assert isinstance(result.p_value, float)
        assert isinstance(result.ci_lower, float)
        assert isinstance(result.ci_upper, float)
        assert result.decision in ("SHIP", "DO NOT SHIP", "INCONCLUSIVE")
        assert len(result.reason) > 0


class TestFormatReport:
    def test_report_contains_key_info(self):
        control = VariantStats("control", users=1000, conversions=100)
        treatment = VariantStats("treatment", users=1000, conversions=150)
        result = analyze_experiment("exp_report", control, treatment)
        report = format_report(result)

        assert "exp_report" in report
        assert "Control" in report
        assert "Treatment" in report
        assert "p-value" in report
        assert "DECISION" in report
        assert result.decision in report

    def test_report_is_multiline(self):
        control = VariantStats("control", users=1000, conversions=100)
        treatment = VariantStats("treatment", users=1000, conversions=150)
        result = analyze_experiment("exp_fmt", control, treatment)
        report = format_report(result)

        assert report.count("\n") > 5
