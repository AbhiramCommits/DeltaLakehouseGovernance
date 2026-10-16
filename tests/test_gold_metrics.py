"""Gold mart maths, checked against hand-computed fixtures.

The marts are SQL over ``silver.*``.  Rather than mocking, these tests create
throwaway tables holding hand-built rows and run the *real* mart SQL against
them by rewriting the source table names, so the assertions are about the
formulas the pipeline actually ships.
"""

from __future__ import annotations

import pytest

from dlg.pipelines.gold import MART_SQL
from dlg.schemas import SILVER_SCHEMAS

SESSIONS = SILVER_SCHEMAS["sessions"]
PURCHASES = SILVER_SCHEMAS["purchases"]


def _session(player_id, session_id, day, duration=30, matches=1, kills=2, deaths=1, store=0):
    """``day`` is an offset from 2025-09-01, so day 31 spills into October."""
    from datetime import datetime, timedelta

    start = datetime(2025, 9, 1, 10, 0, 0) + timedelta(days=day - 1)
    return {
        "session_id": session_id,
        "player_id": player_id,
        "player_email": f"{player_id.lower()}@example.com",
        "player_ip": "10.0.0.1",
        "country": "US",
        "region": "NA",
        "birth_year": 1990,
        "device": "pc",
        "platform": "pc",
        "app_version": "1.4.0",
        "session_start_ts": start,
        "session_end_ts": start,
        "event_ts": start,
        "event_date": start.date(),
        "duration_minutes": duration,
        "matches_played": matches,
        "kills": kills,
        "deaths": deaths,
        "store_views": store,
        "_record_hash": f"h_{session_id}",
        "_ingest_ts": start,
        "_batch_id": "b1",
    }


def _purchase(purchase_id, player_id, day, price, quantity=1):
    from datetime import datetime, timedelta

    ts = datetime(2025, 9, 1, 12, 0, 0) + timedelta(days=day - 1)
    return {
        "purchase_id": purchase_id,
        "session_id": f"S_{player_id}",
        "player_id": player_id,
        "sku": "SKU1",
        "item_name": "Item",
        "category": "skin",
        "price_usd": price,
        "currency": "USD",
        "quantity": quantity,
        "purchase_ts": ts,
        "event_ts": ts,
        "event_date": ts.date(),
        "payment_method": "card",
        "_record_hash": f"hp_{purchase_id}",
        "_ingest_ts": ts,
        "_batch_id": "b1",
    }


@pytest.fixture
def silver_sources(spark, tmp_table):
    """Two throwaway silver tables + a function to run a real mart against them."""
    sessions_id, sessions_path = tmp_table("src_sessions", SESSIONS, partition_by=["event_date"])
    purchases_id, purchases_path = tmp_table(
        "src_purchases", PURCHASES, partition_by=["event_date"]
    )

    def load(session_rows, purchase_rows):
        spark.createDataFrame(session_rows, SESSIONS).write.format("delta").mode("overwrite").save(
            sessions_path
        )
        spark.createDataFrame(purchase_rows, PURCHASES).write.format("delta").mode(
            "overwrite"
        ).save(purchases_path)

    def run(mart: str):
        sql = MART_SQL[mart]
        sql = sql.replace("silver.sessions", sessions_id).replace("silver.purchases", purchases_id)
        sql = sql.replace(
            "gold.dim_player",
            f"(SELECT player_id, country, region FROM {sessions_id} GROUP BY player_id, country, region)",
        )
        return [r.asDict() for r in spark.sql(sql).collect()]

    return {"load": load, "run": run}


def test_arpdau_and_conversion_are_hand_computable(silver_sources):
    """Day 1: 3 players active, 1 of them pays $20 -> ARPDAU 6.667, conversion 33.3%."""
    silver_sources["load"](
        [
            _session("P1", "S1", 1),
            _session("P1", "S2", 1),
            _session("P2", "S3", 1),
            _session("P3", "S4", 1),
        ],
        [_purchase("U1", "P1", 1, 20.0)],
    )
    rows = {r["event_date"]: r for r in silver_sources["run"]("fct_monetization_daily")}
    day1 = rows[next(iter(rows))]
    assert day1["dau"] == 3
    assert day1["paying_users"] == 1
    assert day1["revenue_usd"] == pytest.approx(20.0)
    assert day1["arpdau"] == pytest.approx(20.0 / 3.0, rel=1e-9)
    assert day1["conversion_rate"] == pytest.approx(1 / 3, rel=1e-9)
    assert day1["sessions"] == 4


def test_spend_sums_price_times_quantity(silver_sources):
    silver_sources["load"](
        [_session("P1", "S1", 1), _session("P2", "S2", 1)],
        [
            _purchase("U1", "P1", 1, 10.0, quantity=2),
            _purchase("U2", "P1", 1, 5.0, quantity=1),
        ],
    )
    rows = {r["player_id"]: r for r in silver_sources["run"]("fct_player_daily")}
    assert rows["P1"]["spend_usd"] == pytest.approx(25.0)
    assert rows["P2"]["spend_usd"] == pytest.approx(0.0)
    assert rows["P1"]["sessions"] == 1


def test_d1_retention_is_exact_for_a_hand_built_cohort(silver_sources):
    """Cohort of 4 on day 1; 2 return on day 2 -> D1 = 50%."""
    silver_sources["load"](
        [
            _session("P1", "S1", 1),
            _session("P2", "S2", 1),
            _session("P3", "S3", 1),
            _session("P4", "S4", 1),
            _session("P1", "S5", 2),
            _session("P2", "S6", 2),
            # enough days of tail data that the cohort matures
            _session("P9", "S9", 31),
        ],
        [],
    )
    rows = {r["cohort_date"]: r for r in silver_sources["run"]("fct_retention_cohort")}
    cohort = next(r for d, r in rows.items() if d.day == 1)
    assert cohort["cohort_size"] == 4
    assert cohort["d1_retained"] == 2
    assert cohort["d1_retention"] == pytest.approx(0.5)
    assert cohort["is_matured_d7"] is True


def test_d7_is_null_until_the_cohort_matures(silver_sources):
    """A cohort whose D7 has not happened yet must report NULL, not zero."""
    silver_sources["load"](
        [_session("P1", "S1", 1), _session("P2", "S2", 1), _session("P1", "S3", 2)],
        [],
    )
    rows = list(silver_sources["run"]("fct_retention_cohort"))
    cohort = next(r for r in rows if r["cohort_date"].day == 1)
    assert cohort["d1_retention"] == pytest.approx(0.5)
    assert cohort["d7_retained"] is None
    assert cohort["d7_retention"] is None
    assert cohort["is_matured_d7"] is False


def test_churn_flag_is_days_since_last_seen_over_seven(silver_sources):
    """Day 10 is the last observed day; P1 active day 10 -> not churned, P2 day 1 -> churned."""
    silver_sources["load"](
        [
            _session("P1", "S1", 1),
            _session("P2", "S2", 1),
            _session("P1", "S3", 10),
        ],
        [],
    )
    rows = {r["player_id"]: r for r in silver_sources["run"]("dim_player")}
    assert rows["P1"]["days_since_last_seen"] == 0
    assert rows["P1"]["churn_flag"] is False
    assert rows["P2"]["days_since_last_seen"] == 9
    assert rows["P2"]["churn_flag"] is True


def test_player_daily_volume_matches_the_engine_of_the_real_lake(spark, lakehouse):
    """Sanity-check the shipped marts: no negative spend, sessions bounded."""
    row = spark.sql(
        """
        SELECT count(*) AS rows,
               sum(CASE WHEN spend_usd < 0 THEN 1 ELSE 0 END) AS negative_spend,
               max(sessions) AS max_sessions
        FROM gold.fct_player_daily
        """
    ).collect()[0]
    assert row["rows"] > 0
    assert row["negative_spend"] == 0
    assert row["max_sessions"] <= 100


def test_headline_metrics_are_internally_consistent(spark, lakehouse):
    from dlg.pipelines.gold import headline_metrics

    metrics = headline_metrics(spark)
    assert metrics["players"] == 25_000 or metrics["players"] > 0
    assert 0 <= metrics["churn_rate_pct"] <= 100
    assert 0 <= metrics["conversion_rate_avg"] <= 1
    for key in ("d1_retention_pct", "d7_retention_pct"):
        assert 0 <= metrics[key] <= 100
    # retention is monotonically decreasing across horizons
    assert metrics["d1_retention_pct"] >= metrics["d7_retention_pct"]


def test_mart_sql_only_reads_silver_and_gold():
    for mart, sql in MART_SQL.items():
        lowered = sql.lower()
        assert "bronze." not in lowered, mart
        assert " from silver." in lowered or "join silver." in lowered, mart
