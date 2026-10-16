"""Gold marts: the analytics-ready, business-facing layer.

Four marts are built with Spark SQL from the silver layer.  Each one is written
against an explicit ``StructType`` with ``overwriteSchema=false``, so the gold
layer is protected by the same schema enforcement as bronze and silver.

Retention cohorts are **maturity-gated**: a cohort only reports D7/D30 once the
target day actually falls inside the observed window.  With a 30-day dataset that
means D7 matures for most cohorts while D30 matures only for the day-0 cohort --
reported as ``NULL`` rather than a misleading zero.
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field

from pyspark.sql import SparkSession

from dlg.config import Settings, get_settings
from dlg.lakehouse import ensure_table, overwrite_delta
from dlg.schemas import GOLD_SCHEMAS

__all__ = ["GoldResult", "build_all", "headline_metrics", "GOLD_TABLES", "MART_SQL"]

GOLD_TABLES = (
    "fct_player_daily",
    "dim_player",
    "fct_monetization_daily",
    "fct_retention_cohort",
)

#: Player-day fact: engagement + spend per player per day.
FCT_PLAYER_DAILY = """
WITH sess AS (
  SELECT
    event_date,
    player_id,
    max(country)  AS country,
    max(region)   AS region,
    count(*)      AS sessions,
    sum(duration_minutes) AS playtime_minutes,
    sum(matches_played)   AS matches,
    sum(kills)            AS kills,
    sum(deaths)           AS deaths
  FROM silver.sessions
  GROUP BY event_date, player_id
),
spend AS (
  SELECT event_date, player_id, sum(price_usd * quantity) AS spend_usd
  FROM silver.purchases
  GROUP BY event_date, player_id
)
SELECT
  coalesce(sess.event_date, spend.event_date) AS event_date,
  coalesce(sess.player_id, spend.player_id)   AS player_id,
  sess.country,
  sess.region,
  coalesce(sess.sessions, 0)          AS sessions,
  coalesce(sess.playtime_minutes, 0)  AS playtime_minutes,
  coalesce(sess.matches, 0)           AS matches,
  coalesce(sess.kills, 0)             AS kills,
  coalesce(sess.deaths, 0)            AS deaths,
  cast(coalesce(spend.spend_usd, 0.0) AS DOUBLE) AS spend_usd
FROM sess
FULL OUTER JOIN spend
  ON sess.event_date = spend.event_date AND sess.player_id = spend.player_id
"""

#: Conformed player dimension with a churn flag (no session in the last 7 days).
DIM_PLAYER = """
WITH max_date AS (SELECT max(event_date) AS d FROM silver.sessions),
agg AS (
  SELECT
    player_id,
    min(session_start_ts) AS first_seen,
    max(session_start_ts) AS last_seen,
    max(country) AS country,
    max(region)  AS region,
    count(DISTINCT session_id) AS lifetime_sessions,
    sum(duration_minutes)      AS lifetime_playtime_minutes,
    max(event_date)            AS last_event_date
  FROM silver.sessions
  GROUP BY player_id
),
spend AS (
  SELECT player_id, sum(price_usd * quantity) AS lifetime_spend
  FROM silver.purchases
  GROUP BY player_id
)
SELECT
  agg.player_id,
  agg.first_seen,
  agg.last_seen,
  agg.country,
  agg.region,
  cast(coalesce(spend.lifetime_spend, 0.0) AS DOUBLE) AS lifetime_spend,
  agg.lifetime_sessions,
  agg.lifetime_playtime_minutes,
  datediff(max_date.d, agg.last_event_date) AS days_since_last_seen,
  (datediff(max_date.d, agg.last_event_date) > 7) AS churn_flag
FROM agg
CROSS JOIN max_date
LEFT JOIN spend ON spend.player_id = agg.player_id
"""

#: Daily monetization funnel: DAU, payers, revenue, ARPDAU, conversion.
FCT_MONETIZATION_DAILY = """
WITH act AS (
  SELECT event_date, count(DISTINCT player_id) AS dau, count(*) AS sessions
  FROM silver.sessions
  GROUP BY event_date
),
pay AS (
  SELECT
    event_date,
    count(DISTINCT player_id) AS paying_users,
    sum(price_usd * quantity) AS revenue_usd,
    count(*) AS purchases
  FROM silver.purchases
  GROUP BY event_date
)
SELECT
  coalesce(act.event_date, pay.event_date) AS event_date,
  coalesce(act.dau, 0)            AS dau,
  coalesce(pay.paying_users, 0)   AS paying_users,
  cast(coalesce(pay.revenue_usd, 0.0) AS DOUBLE) AS revenue_usd,
  cast(
    CASE WHEN coalesce(act.dau, 0) = 0 THEN 0.0
         ELSE coalesce(pay.revenue_usd, 0.0) / act.dau END AS DOUBLE) AS arpdau,
  cast(
    CASE WHEN coalesce(act.dau, 0) = 0 THEN 0.0
         ELSE coalesce(pay.paying_users, 0) / act.dau END AS DOUBLE) AS conversion_rate,
  coalesce(act.sessions, 0) AS sessions,
  coalesce(pay.purchases, 0) AS purchases
FROM act
FULL OUTER JOIN pay ON act.event_date = pay.event_date
"""

#: Install-cohort retention, maturity-gated against the observed window.
FCT_RETENTION_COHORT = """
WITH cohort AS (
  SELECT player_id, min(event_date) AS cohort_date
  FROM silver.sessions
  GROUP BY player_id
),
sizes AS (
  SELECT cohort_date, count(*) AS cohort_size FROM cohort GROUP BY cohort_date
),
activity AS (
  SELECT DISTINCT
    c.cohort_date,
    s.player_id,
    datediff(s.event_date, c.cohort_date) AS day_n
  FROM silver.sessions s
  JOIN cohort c ON c.player_id = s.player_id
),
ret AS (
  SELECT
    cohort_date,
    count(DISTINCT CASE WHEN day_n = 1  THEN player_id END) AS d1_retained,
    count(DISTINCT CASE WHEN day_n = 7  THEN player_id END) AS d7_retained,
    count(DISTINCT CASE WHEN day_n = 30 THEN player_id END) AS d30_retained
  FROM activity
  GROUP BY cohort_date
),
bounds AS (SELECT max(event_date) AS max_date FROM silver.sessions)
SELECT
  s.cohort_date,
  s.cohort_size,
  CASE WHEN date_add(s.cohort_date, 1)  <= b.max_date THEN r.d1_retained  END AS d1_retained,
  CASE WHEN date_add(s.cohort_date, 7)  <= b.max_date THEN r.d7_retained  END AS d7_retained,
  CASE WHEN date_add(s.cohort_date, 30) <= b.max_date THEN r.d30_retained END AS d30_retained,
  cast(CASE WHEN date_add(s.cohort_date, 1)  <= b.max_date
            THEN r.d1_retained / s.cohort_size END AS DOUBLE) AS d1_retention,
  cast(CASE WHEN date_add(s.cohort_date, 7)  <= b.max_date
            THEN r.d7_retained / s.cohort_size END AS DOUBLE) AS d7_retention,
  cast(CASE WHEN date_add(s.cohort_date, 30) <= b.max_date
            THEN r.d30_retained / s.cohort_size END AS DOUBLE) AS d30_retention,
  (date_add(s.cohort_date, 7)  <= b.max_date) AS is_matured_d7,
  (date_add(s.cohort_date, 30) <= b.max_date) AS is_matured_d30
FROM sizes s
JOIN ret r ON r.cohort_date = s.cohort_date
CROSS JOIN bounds b
"""

MART_SQL: dict[str, str] = {
    "fct_player_daily": FCT_PLAYER_DAILY,
    "dim_player": DIM_PLAYER,
    "fct_monetization_daily": FCT_MONETIZATION_DAILY,
    "fct_retention_cohort": FCT_RETENTION_COHORT,
}

#: Table-level transform metadata, consumed by the lineage builder.
MART_INPUTS: dict[str, tuple[str, ...]] = {
    "fct_player_daily": ("silver.sessions", "silver.purchases"),
    "dim_player": ("silver.sessions", "silver.purchases"),
    "fct_monetization_daily": ("silver.sessions", "silver.purchases"),
    "fct_retention_cohort": ("silver.sessions",),
}


@dataclass
class MartResult:
    table: str
    rows: int
    columns: int
    seconds: float
    inputs: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass
class GoldResult:
    marts: dict[str, MartResult] = field(default_factory=dict)
    seconds: float = 0.0

    def as_dict(self) -> dict:
        return {
            "marts": {k: v.as_dict() for k, v in self.marts.items()},
            "seconds": self.seconds,
        }


def build_mart(spark: SparkSession, mart: str, settings: Settings | None = None) -> MartResult:
    settings = settings or get_settings()
    started = time.perf_counter()
    target = f"gold.{mart}"
    schema = GOLD_SCHEMAS[mart]
    ensure_table(spark, target, schema, cdf=True, settings=settings)
    df = spark.sql(MART_SQL[mart])
    overwrite_delta(spark, target, df)
    return MartResult(
        table=target,
        rows=spark.table(target).count(),
        columns=len(schema.fields),
        seconds=round(time.perf_counter() - started, 2),
        inputs=list(MART_INPUTS[mart]),
    )


def build_all(spark: SparkSession, settings: Settings | None = None) -> GoldResult:
    """Build every gold mart. ``dim_player`` is first: the rest do not need it,
    but the catalog declares it as a hub so lineage resolution stays stable."""
    started = time.perf_counter()
    settings = settings or get_settings()
    marts = {
        name: build_mart(spark, name, settings=settings)
        for name in (
            "dim_player",
            "fct_player_daily",
            "fct_monetization_daily",
            "fct_retention_cohort",
        )
    }
    return GoldResult(marts=marts, seconds=round(time.perf_counter() - started, 2))


def headline_metrics(spark: SparkSession) -> dict[str, float | int]:
    """The business headline numbers, computed from the live gold tables."""
    row = spark.sql(
        """
        SELECT
          count(*)                                            AS days_observed,
          avg(dau)                                            AS dau_avg,
          max(dau)                                            AS dau_max,
          sum(revenue_usd)                                    AS revenue_total,
          avg(arpdau)                                         AS arpdau_avg,
          avg(conversion_rate)                                AS conversion_rate_avg,
          sum(purchases)                                      AS purchases_total
        FROM gold.fct_monetization_daily
        """
    ).collect()[0]
    ret = spark.sql(
        """
        SELECT
          sum(d1_retained)  / sum(CASE WHEN d1_retained  IS NOT NULL THEN cohort_size END) AS d1,
          sum(d7_retained)  / sum(CASE WHEN d7_retained  IS NOT NULL THEN cohort_size END) AS d7,
          sum(d30_retained) / sum(CASE WHEN d30_retained IS NOT NULL THEN cohort_size END) AS d30,
          sum(CASE WHEN d1_retained  IS NOT NULL THEN 1 ELSE 0 END)  AS n_d1,
          sum(CASE WHEN d7_retained  IS NOT NULL THEN 1 ELSE 0 END)  AS n_d7,
          sum(CASE WHEN d30_retained IS NOT NULL THEN 1 ELSE 0 END)  AS n_d30
        FROM gold.fct_retention_cohort
        """
    ).collect()[0]
    churn = spark.sql(
        """
        SELECT
          count(*) AS players,
          sum(CASE WHEN churn_flag THEN 1 ELSE 0 END) AS churned,
          avg(lifetime_spend) AS avg_lifetime_spend
        FROM gold.dim_player
        """
    ).collect()[0]
    out: dict[str, float | int] = {
        "days_observed": int(row["days_observed"]),
        "dau_avg": round(float(row["dau_avg"]), 2),
        "dau_max": int(row["dau_max"]),
        "revenue_total_usd": round(float(row["revenue_total"]), 2),
        "arpdau_avg": round(float(row["arpdau_avg"]), 4),
        "conversion_rate_avg": round(float(row["conversion_rate_avg"]), 4),
        "purchases_total": int(row["purchases_total"]),
        "d1_retention_pct": round(100.0 * float(ret["d1"]), 2),
        "d7_retention_pct": round(100.0 * float(ret["d7"]), 2),
        "d30_retention_pct": round(100.0 * float(ret["d30"]), 2),
        "cohorts_d1": int(ret["n_d1"]),
        "cohorts_d7": int(ret["n_d7"]),
        "cohorts_d30": int(ret["n_d30"]),
        "players": int(churn["players"]),
        "churned_players": int(churn["churned"]),
        "churn_rate_pct": round(100.0 * float(churn["churned"]) / float(churn["players"]), 2),
        "avg_lifetime_spend_usd": round(float(churn["avg_lifetime_spend"]), 2),
    }
    return out


def print_report(result: GoldResult) -> None:
    from tabulate import tabulate

    rows = [
        [m.table, f"{m.rows:,}", m.columns, ", ".join(m.inputs), f"{m.seconds:.2f}"]
        for m in result.marts.values()
    ]
    print(tabulate(rows, headers=["mart", "rows", "cols", "inputs", "secs"], tablefmt="github"))
