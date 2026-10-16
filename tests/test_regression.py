"""Regression gate: the marts must still reproduce the committed baseline.

Counts and volume invariants are exact; rate metrics carry a documented relative
tolerance because they are estimates over a sampled dataset.
"""

from __future__ import annotations

import pytest

from dlg.regression import (
    DEFAULT_BASELINE_PATH,
    RELATIVE_TOLERANCE,
    compare,
    compute_baseline,
    load_baseline,
)


@pytest.fixture(scope="module")
def baseline_document() -> dict:
    return load_baseline(DEFAULT_BASELINE_PATH)


@pytest.fixture(scope="module")
def entry_for_scale(spark, lakehouse, settings, baseline_document):
    entries = baseline_document.get("entries") or {}
    entry = entries.get(settings.scale)
    if entry is None:
        pytest.skip(
            f"no committed baseline for scale {settings.scale!r}; run `make baseline-update`"
        )
    from dlg.regression import BaselineEntry

    return BaselineEntry(**entry)


def test_baseline_file_exists_and_is_well_formed(baseline_document):
    assert baseline_document, "tests/baseline/metrics_baseline.json is missing"
    assert "entries" in baseline_document
    assert baseline_document["relative_tolerance"] == RELATIVE_TOLERANCE


def test_recomputed_metrics_match_the_baseline(spark, entry_for_scale, settings):
    actual = compute_baseline(spark, settings)
    mismatches = compare(actual, entry_for_scale)
    assert mismatches == [], "\n".join(str(m) for m in mismatches)


def test_mart_row_counts_are_non_trivial(spark, lakehouse):
    rows = {
        mart: spark.table(f"gold.{mart}").count()
        for mart in (
            "fct_player_daily",
            "dim_player",
            "fct_monetization_daily",
            "fct_retention_cohort",
        )
    }
    assert all(count > 0 for count in rows.values()), rows
    assert rows["dim_player"] >= 1
    assert rows["fct_monetization_daily"] >= 1


# ---------------------------------------------------------------------------
# scale-free invariants
# ---------------------------------------------------------------------------


def test_spend_totals_agree_across_marts(spark, lakehouse):
    """fct_player_daily and fct_monetization_daily must reconcile on revenue."""
    player_spend = spark.sql(
        "SELECT cast(sum(spend_usd) AS DOUBLE) AS s FROM gold.fct_player_daily"
    ).collect()[0]["s"]
    daily_revenue = spark.sql(
        "SELECT cast(sum(revenue_usd) AS DOUBLE) AS s FROM gold.fct_monetization_daily"
    ).collect()[0]["s"]
    assert player_spend == pytest.approx(daily_revenue, rel=1e-9)


def test_purchase_count_reconciles_with_silver(spark, lakehouse):
    mart_purchases = spark.sql(
        "SELECT sum(purchases) AS n FROM gold.fct_monetization_daily"
    ).collect()[0]["n"]
    silver_purchases = spark.table("silver.purchases").count()
    assert mart_purchases == silver_purchases


def test_dim_player_matches_distinct_players_in_silver(spark, lakehouse):
    dim = spark.table("gold.dim_player").count()
    distinct = spark.table("silver.sessions").select("player_id").distinct().count()
    assert dim == distinct


def test_no_negative_or_null_money(spark, lakehouse):
    row = spark.sql(
        """
        SELECT
          sum(CASE WHEN spend_usd < 0 THEN 1 ELSE 0 END) AS neg_spend,
          sum(CASE WHEN revenue_usd < 0 THEN 1 ELSE 0 END) AS neg_revenue,
          sum(CASE WHEN lifetime_spend < 0 THEN 1 ELSE 0 END) AS neg_lifetime
        FROM gold.fct_player_daily f
        CROSS JOIN gold.fct_monetization_daily m
        CROSS JOIN gold.dim_player d
        """
    ).collect()[0]
    assert row["neg_spend"] == 0
    assert row["neg_revenue"] == 0
    assert row["neg_lifetime"] == 0


def test_retention_is_monotonic_across_horizons(spark, lakehouse):
    row = spark.sql(
        """
        SELECT
          sum(d1_retained) AS d1, sum(d7_retained) AS d7, sum(d30_retained) AS d30,
          sum(CASE WHEN d1_retained IS NOT NULL THEN cohort_size END) AS n1,
          sum(CASE WHEN d7_retained IS NOT NULL THEN cohort_size END) AS n7,
          sum(CASE WHEN d30_retained IS NOT NULL THEN cohort_size END) AS n30
        FROM gold.fct_retention_cohort
        """
    ).collect()[0]
    assert row["d1"] / row["n1"] >= row["d7"] / row["n7"]
    assert row["d7"] / row["n7"] >= row["d30"] / row["n30"]


def test_cohort_sizes_sum_to_the_player_count(spark, lakehouse):
    total_cohort = spark.sql(
        "SELECT sum(cohort_size) AS n FROM gold.fct_retention_cohort"
    ).collect()[0]["n"]
    players = spark.table("gold.dim_player").count()
    assert total_cohort == players
