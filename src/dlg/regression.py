"""Regression baseline: a committed snapshot of the gold metrics.

The baseline holds two kinds of assertion:

* **counts** -- mart row counts and volume invariants, which must match exactly;
* **rates** -- DAU, ARPDAU, conversion, retention, churn, which are compared with
  a documented relative tolerance because they are statistical estimates over a
  sampled dataset.

Baselines are stored per scale (``full`` and ``small``).  CI runs the pipeline at
``DLG_SCALE=small`` because a full run is too slow for a pull request, and the
test picks the entry matching the scale it is running at.  Regenerate with
``make baseline-update`` (and once with ``DLG_SCALE=small`` for the CI entry).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession

from dlg.config import Settings, get_settings
from dlg.pipelines.gold import headline_metrics

__all__ = [
    "DEFAULT_BASELINE_PATH",
    "RELATIVE_TOLERANCE",
    "BaselineMismatch",
    "compute_baseline",
    "load_baseline",
    "save_baseline",
    "compare",
    "baseline_path",
]

DEFAULT_BASELINE_PATH = Path("tests/baseline/metrics_baseline.json")

#: relative tolerance for rate metrics (2%)
RELATIVE_TOLERANCE = 0.02

#: exact-match metrics
EXACT_KEYS = (
    "days_observed",
    "purchases_total",
    "cohorts_d1",
    "cohorts_d7",
)


@dataclass
class BaselineMismatch:
    key: str
    expected: float
    actual: float
    relative_delta: float
    tolerance: float

    def __str__(self) -> str:  # pragma: no cover - formatting
        return (
            f"{self.key}: expected {self.expected}, got {self.actual} "
            f"(delta {self.relative_delta:.4%}, tolerance {self.tolerance:.2%})"
        )


@dataclass
class BaselineEntry:
    scale: str
    generated_at: str
    headline_metrics: dict = field(default_factory=dict)
    mart_rows: dict = field(default_factory=dict)
    invariants: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return asdict(self)


def baseline_path(settings: Settings | None = None, path: str | Path | None = None) -> Path:
    if path:
        return Path(path)
    return Path(DEFAULT_BASELINE_PATH)


def compute_invariants(spark: SparkSession) -> dict[str, float]:
    """Scale-free relationships that must hold whatever the row count."""
    player_days = spark.sql(
        """
        SELECT count(*) AS rows,
               sum(CASE WHEN spend_usd < 0 THEN 1 ELSE 0 END) AS negative_spend,
               cast(sum(spend_usd) AS DOUBLE) AS spend_total
        FROM gold.fct_player_daily
        """
    ).collect()[0]
    dim = spark.sql(
        "SELECT count(*) AS players, max(lifetime_spend) AS max_spend FROM gold.dim_player"
    ).collect()[0]
    daily = spark.sql(
        """
        SELECT count(*) AS days,
               cast(sum(revenue_usd) AS DOUBLE) AS revenue,
               sum(purchases) AS purchases,
               max(conversion_rate) AS max_conversion,
               min(arpdau) AS min_arpdau
        FROM gold.fct_monetization_daily
        """
    ).collect()[0]
    silver_sessions = spark.table("silver.sessions").count()
    silver_purchases = spark.table("silver.purchases").count()
    distinct_players = spark.table("silver.sessions").select("player_id").distinct().count()

    return {
        "fct_player_daily_rows": int(player_days["rows"]),
        "fct_player_daily_negative_spend": int(player_days["negative_spend"]),
        "spend_total_usd": round(float(player_days["spend_total"]), 2),
        "monetization_revenue_usd": round(float(daily["revenue"]), 2),
        "monetization_purchases": int(daily["purchases"]),
        "monetization_days": int(daily["days"]),
        "max_conversion_rate": round(float(daily["max_conversion"]), 6),
        "min_arpdau": round(float(daily["min_arpdau"]), 6),
        "dim_player_rows": int(dim["players"]),
        "silver_sessions_rows": int(silver_sessions),
        "silver_purchases_rows": int(silver_purchases),
        "silver_distinct_players": int(distinct_players),
    }


def compute_baseline(spark: SparkSession, settings: Settings | None = None) -> BaselineEntry:
    settings = settings or get_settings()
    marts = {}
    for mart in (
        "fct_player_daily",
        "dim_player",
        "fct_monetization_daily",
        "fct_retention_cohort",
    ):
        marts[f"gold.{mart}"] = spark.table(f"gold.{mart}").count()
    return BaselineEntry(
        scale=settings.scale,
        generated_at=datetime.now(UTC).isoformat(),
        headline_metrics=headline_metrics(spark),
        mart_rows=marts,
        invariants=compute_invariants(spark),
    )


def load_baseline(path: str | Path | None = None) -> dict:
    target = Path(path or DEFAULT_BASELINE_PATH)
    if not target.exists():
        return {"entries": {}}
    return json.loads(target.read_text())


def save_baseline(entry: BaselineEntry, path: str | Path | None = None) -> Path:
    target = Path(path or DEFAULT_BASELINE_PATH)
    payload = load_baseline(target)
    payload.setdefault("entries", {})
    payload["entries"][entry.scale] = entry.as_dict()
    payload["relative_tolerance"] = RELATIVE_TOLERANCE
    payload["exact_keys"] = list(EXACT_KEYS)
    payload["description"] = (
        "Committed gold-metric regression baseline. Counts and invariants must match "
        "exactly; rate metrics are compared with a relative tolerance. Regenerate with "
        "`make baseline-update` (and with DLG_SCALE=small for the CI entry)."
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, sort_keys=True))
    return target


def compare(
    actual: BaselineEntry,
    expected: BaselineEntry,
    *,
    tolerance: float = RELATIVE_TOLERANCE,
) -> list[BaselineMismatch]:
    """Return the mismatches between an actual and an expected baseline."""
    mismatches: list[BaselineMismatch] = []

    for key, expected_value in expected.mart_rows.items():
        actual_value = actual.mart_rows.get(key)
        if actual_value != expected_value:
            mismatches.append(
                BaselineMismatch(
                    str(key), float(expected_value), float(actual_value or 0), 1.0, 0.0
                )
            )

    for key, expected_value in expected.invariants.items():
        actual_value = actual.invariants.get(key)
        if actual_value is None:
            mismatches.append(BaselineMismatch(str(key), float(expected_value), 0.0, 1.0, 0.0))
            continue
        if (
            key in EXACT_KEYS
            or isinstance(expected_value, int)
            and not isinstance(expected_value, bool)
        ):
            if actual_value != expected_value:
                mismatches.append(
                    BaselineMismatch(str(key), float(expected_value), float(actual_value), 1.0, 0.0)
                )
            continue
        delta = abs(float(actual_value) - float(expected_value)) / max(
            abs(float(expected_value)), 1e-9
        )
        if delta > tolerance:
            mismatches.append(
                BaselineMismatch(
                    str(key), float(expected_value), float(actual_value), delta, tolerance
                )
            )

    for key, expected_value in expected.headline_metrics.items():
        actual_value = actual.headline_metrics.get(key)
        if actual_value is None:
            mismatches.append(BaselineMismatch(str(key), float(expected_value), 0.0, 1.0, 0.0))
            continue
        if (
            key in EXACT_KEYS
            or isinstance(expected_value, int)
            and not isinstance(expected_value, bool)
        ):
            if actual_value != expected_value:
                mismatches.append(
                    BaselineMismatch(str(key), float(expected_value), float(actual_value), 1.0, 0.0)
                )
            continue
        delta = abs(float(actual_value) - float(expected_value)) / max(
            abs(float(expected_value)), 1e-9
        )
        if delta > tolerance:
            mismatches.append(
                BaselineMismatch(
                    str(key), float(expected_value), float(actual_value), delta, tolerance
                )
            )

    return mismatches
