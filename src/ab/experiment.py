"""Experiment definitions and metadata.

Each experiment has a unique ID, a list of variants with traffic weights,
and a metric it aims to influence.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Variant:
    name: str
    weight: float  # Traffic proportion (0.0 to 1.0)


@dataclass(frozen=True)
class Experiment:
    experiment_id: str
    name: str
    variants: list[Variant]
    target_metric: str = "purchase"

    def __post_init__(self):
        total = sum(v.weight for v in self.variants)
        if abs(total - 1.0) > 0.001:
            raise ValueError(f"Variant weights must sum to 1.0, got {total}")
        if len(self.variants) < 2:
            raise ValueError("Experiment must have at least 2 variants")
        names = [v.name for v in self.variants]
        if len(names) != len(set(names)):
            raise ValueError("Variant names must be unique")


# Default experiment used by the simulator
PRICING_PAGE_EXPERIMENT = Experiment(
    experiment_id="exp_pricing_page_v1",
    name="Pricing Page Redesign",
    variants=[
        Variant(name="control", weight=0.5),
        Variant(name="treatment", weight=0.5),
    ],
    target_metric="purchase",
)
