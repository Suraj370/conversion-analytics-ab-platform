"""Statistical analysis for A/B experiments.

Implements a two-proportion z-test comparing treatment vs control
conversion rates, with confidence intervals and a decision framework.
"""

import math
from dataclasses import dataclass

from scipy import stats


@dataclass(frozen=True)
class VariantStats:
    """Raw counts for one variant."""

    name: str
    users: int
    conversions: int

    @property
    def conversion_rate(self) -> float:
        if self.users == 0:
            return 0.0
        return self.conversions / self.users


@dataclass(frozen=True)
class ExperimentResult:
    """Full statistical result of an A/B experiment comparison."""

    experiment_id: str
    control: VariantStats
    treatment: VariantStats

    # Statistical outputs
    absolute_uplift: float      # treatment_rate - control_rate
    relative_uplift: float      # (treatment_rate - control_rate) / control_rate
    p_value: float
    confidence_level: float     # e.g. 0.95
    ci_lower: float             # lower bound of absolute uplift CI
    ci_upper: float             # upper bound of absolute uplift CI
    is_significant: bool
    decision: str               # "SHIP", "DO NOT SHIP", or "INCONCLUSIVE"
    reason: str                 # Human-readable explanation


def analyze_experiment(
    experiment_id: str,
    control: VariantStats,
    treatment: VariantStats,
    confidence_level: float = 0.95,
    min_sample_size: int = 100,
) -> ExperimentResult:
    """Run a two-proportion z-test and produce a ship/no-ship decision.

    Args:
        experiment_id: Identifier for the experiment.
        control: Stats for the control variant.
        treatment: Stats for the treatment variant.
        confidence_level: Desired confidence level (default 0.95 = 95%).
        min_sample_size: Minimum users per variant to consider results valid.
    """
    p_c = control.conversion_rate
    p_t = treatment.conversion_rate
    n_c = control.users
    n_t = treatment.users

    absolute_uplift = p_t - p_c
    relative_uplift = absolute_uplift / p_c if p_c > 0 else 0.0

    # Check minimum sample size
    if n_c < min_sample_size or n_t < min_sample_size:
        return ExperimentResult(
            experiment_id=experiment_id,
            control=control,
            treatment=treatment,
            absolute_uplift=absolute_uplift,
            relative_uplift=relative_uplift,
            p_value=1.0,
            confidence_level=confidence_level,
            ci_lower=0.0,
            ci_upper=0.0,
            is_significant=False,
            decision="INCONCLUSIVE",
            reason=f"Insufficient sample size (need >={min_sample_size} per variant, "
                   f"got control={n_c}, treatment={n_t})",
        )

    # Pooled proportion under the null hypothesis
    p_pool = (control.conversions + treatment.conversions) / (n_c + n_t)

    # Standard error for the z-test
    se_test = math.sqrt(p_pool * (1 - p_pool) * (1 / n_c + 1 / n_t))

    if se_test == 0:
        p_value = 1.0
        z_stat = 0.0
    else:
        z_stat = absolute_uplift / se_test
        # Two-tailed test
        p_value = 2 * (1 - stats.norm.cdf(abs(z_stat)))

    # Confidence interval on the difference (unpooled SE)
    se_ci = math.sqrt(
        (p_c * (1 - p_c) / n_c) + (p_t * (1 - p_t) / n_t)
    )
    z_crit = stats.norm.ppf(1 - (1 - confidence_level) / 2)
    ci_lower = absolute_uplift - z_crit * se_ci
    ci_upper = absolute_uplift + z_crit * se_ci

    is_significant = p_value < (1 - confidence_level)

    # Decision logic
    decision, reason = _make_decision(
        is_significant, absolute_uplift, relative_uplift,
        p_value, ci_lower, ci_upper, confidence_level,
    )

    return ExperimentResult(
        experiment_id=experiment_id,
        control=control,
        treatment=treatment,
        absolute_uplift=round(absolute_uplift, 6),
        relative_uplift=round(relative_uplift, 4),
        p_value=round(p_value, 6),
        confidence_level=confidence_level,
        ci_lower=round(ci_lower, 6),
        ci_upper=round(ci_upper, 6),
        is_significant=is_significant,
        decision=decision,
        reason=reason,
    )


def _make_decision(
    is_significant: bool,
    absolute_uplift: float,
    relative_uplift: float,
    p_value: float,
    ci_lower: float,
    ci_upper: float,
    confidence_level: float,
) -> tuple[str, str]:
    """Produce a human-readable decision from statistical results."""
    alpha = 1 - confidence_level

    if not is_significant:
        return (
            "DO NOT SHIP",
            f"Not statistically significant (p={p_value:.4f} >= {alpha}). "
            f"Cannot confidently attribute the observed "
            f"{relative_uplift:+.1%} change to the treatment.",
        )

    if absolute_uplift > 0 and ci_lower > 0:
        return (
            "SHIP",
            f"Statistically significant positive effect (p={p_value:.4f}). "
            f"Treatment increases conversion by {relative_uplift:+.1%} "
            f"(95% CI: [{ci_lower:+.4f}, {ci_upper:+.4f}]).",
        )

    if absolute_uplift < 0 and ci_upper < 0:
        return (
            "DO NOT SHIP",
            f"Statistically significant NEGATIVE effect (p={p_value:.4f}). "
            f"Treatment decreases conversion by {relative_uplift:+.1%}.",
        )

    return (
        "DO NOT SHIP",
        f"Significant result (p={p_value:.4f}) but confidence interval "
        f"crosses zero [{ci_lower:+.4f}, {ci_upper:+.4f}]. Effect direction uncertain.",
    )


def format_report(result: ExperimentResult) -> str:
    """Format an experiment result as a human-readable report."""
    lines = [
        f"{'=' * 60}",
        f"EXPERIMENT ANALYSIS: {result.experiment_id}",
        f"{'=' * 60}",
        "",
        "VARIANT SUMMARY",
        f"  Control:   {result.control.conversions}/{result.control.users} "
        f"= {result.control.conversion_rate:.2%} conversion rate",
        f"  Treatment: {result.treatment.conversions}/{result.treatment.users} "
        f"= {result.treatment.conversion_rate:.2%} conversion rate",
        "",
        "STATISTICAL RESULTS",
        f"  Absolute uplift:  {result.absolute_uplift:+.4f} "
        f"({result.relative_uplift:+.1%} relative)",
        f"  p-value:          {result.p_value:.4f}",
        f"  95% CI:           [{result.ci_lower:+.4f}, {result.ci_upper:+.4f}]",
        f"  Significant:      {'YES' if result.is_significant else 'NO'}",
        "",
        f"DECISION: {result.decision}",
        f"  {result.reason}",
        f"{'=' * 60}",
    ]
    return "\n".join(lines)
