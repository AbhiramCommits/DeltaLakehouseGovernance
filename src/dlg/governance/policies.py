"""Row-level security and column masking, implemented as governed Delta views.

Unity Catalog enforces row filters and column masks inside the engine.  The OSS
equivalent is to publish a *governed view* per role and grant access to the view
instead of the base table.  This module builds those views in the ``secure``
schema:

======================  ==========================================================
governed view           governance applied
======================  ==========================================================
``v_dim_player_analyst``    ``player_email`` -> SHA-256 pseudonym, ``player_ip`` -> NULL,
                            ``birth_year`` -> ``age_bucket``, ``country`` -> NULL
``v_dim_player_emea``       as above plus a row filter ``region = 'EMEA'``
``v_fct_player_daily``      ``country`` -> NULL (region retained)
``v_fct_player_daily_emea`` row filter ``region = 'EMEA'``
``v_monetization_daily``    aggregate only, no PII columns
``v_retention_cohort``      aggregate only, no PII columns
``v_purchases_analyst``     ``payment_method`` -> NULL
======================  ==========================================================

Age buckets are anchored on the dashboard's reference year (derived from the
data, not from wall-clock time) so the masked output is reproducible.
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field

from pyspark.sql import SparkSession

from dlg.config import Settings, get_settings

__all__ = [
    "PolicyReport",
    "apply_policies",
    "GOVERNED_VIEWS",
    "MASKED_PII_COLUMNS",
    "SECURE_SCHEMA",
    "reference_year",
]

SECURE_SCHEMA = "secure"

#: PII columns that a governed view rewrites or removes.
MASKED_PII_COLUMNS = (
    "player_email",
    "player_ip",
    "country",
    "birth_year",
    "payment_method",
)

PSEUDONYM_PREFIX = "pii_"
#: Bucket boundaries in years of age, evaluated against the reference year.
AGE_BUCKETS = ((18, "<18"), (25, "18-24"), (35, "25-34"), (45, "35-44"), (55, "45-54"))


def reference_year(spark: SparkSession) -> int:
    """Dashboard reference year: the latest year present in the gold marts."""
    row = spark.sql("SELECT year(max(event_date)) AS y FROM gold.fct_monetization_daily").collect()[
        0
    ]
    return int(row["y"])


def _age_bucket_expr(birth_year_col: str, ref_year: int = 2025) -> str:
    cases = "\n        ".join(
        f"WHEN {ref_year} - {birth_year_col} < {limit} THEN '{label}'"
        for limit, label in AGE_BUCKETS
    )
    return (
        f"CASE\n        {cases}\n"
        f"        WHEN {birth_year_col} IS NULL THEN 'unknown'\n"
        f"        ELSE '55+'\n      END"
    )


def _masked_player_cte(ref_year: int) -> str:
    """A CTE that pseudonymises the PII carried on ``silver.sessions``."""
    return f"""
    pii AS (
      SELECT
        player_id,
        max(player_email) AS player_email,
        max(player_ip)    AS player_ip,
        max(country)      AS country,
        max(birth_year)   AS birth_year
      FROM silver.sessions
      GROUP BY player_id
    ),
    masked AS (
      SELECT
        player_id,
        concat('{PSEUDONYM_PREFIX}', substr(sha2(lower(trim(player_email)), 256), 1, 32))
            AS player_email,
        cast(NULL AS string) AS player_ip,
        cast(NULL AS string) AS country,
        {_age_bucket_expr("birth_year", ref_year)} AS age_bucket
      FROM pii
    )"""


def _view_sql(ref_year: int) -> dict[str, str]:
    """The full DDL for every governed view."""
    masked = _masked_player_cte(ref_year)
    return {
        # ---- player dimension, masked PII (analyst / marketing) ----------
        "v_dim_player_analyst": f"""
CREATE OR REPLACE VIEW secure.v_dim_player_analyst AS
WITH {masked}
SELECT
  d.player_id,
  d.first_seen,
  d.last_seen,
  d.region,
  m.country                 AS country,
  m.player_email            AS player_email,
  m.player_ip               AS player_ip,
  m.age_bucket              AS age_bucket,
  d.lifetime_spend,
  d.lifetime_sessions,
  d.lifetime_playtime_minutes,
  d.days_since_last_seen,
  d.churn_flag
FROM gold.dim_player d
LEFT JOIN masked m ON m.player_id = d.player_id
""",
        # ---- player dimension, masked PII + EMEA row filter ---------------
        "v_dim_player_emea": f"""
CREATE OR REPLACE VIEW secure.v_dim_player_emea AS
WITH {masked}
SELECT
  d.player_id,
  d.first_seen,
  d.last_seen,
  d.region,
  m.country                 AS country,
  m.player_email            AS player_email,
  m.player_ip               AS player_ip,
  m.age_bucket              AS age_bucket,
  d.lifetime_spend,
  d.lifetime_sessions,
  d.lifetime_playtime_minutes,
  d.days_since_last_seen,
  d.churn_flag
FROM gold.dim_player d
LEFT JOIN masked m ON m.player_id = d.player_id
WHERE d.region = 'EMEA'
""",
        # ---- player-day fact, geography masked ---------------------------
        "v_fct_player_daily": """
CREATE OR REPLACE VIEW secure.v_fct_player_daily AS
SELECT
  event_date,
  player_id,
  cast(NULL AS string) AS country,
  region,
  sessions,
  playtime_minutes,
  matches,
  kills,
  deaths,
  spend_usd
FROM gold.fct_player_daily
""",
        # ---- player-day fact, EMEA row filter ----------------------------
        "v_fct_player_daily_emea": """
CREATE OR REPLACE VIEW secure.v_fct_player_daily_emea AS
SELECT
  event_date,
  player_id,
  cast(NULL AS string) AS country,
  region,
  sessions,
  playtime_minutes,
  matches,
  kills,
  deaths,
  spend_usd
FROM gold.fct_player_daily
WHERE region = 'EMEA'
""",
        # ---- monetization: aggregates only -------------------------------
        "v_monetization_daily": """
CREATE OR REPLACE VIEW secure.v_monetization_daily AS
SELECT
  event_date, dau, paying_users, revenue_usd, arpdau, conversion_rate, sessions, purchases
FROM gold.fct_monetization_daily
""",
        # ---- retention cohorts: aggregates only --------------------------
        "v_retention_cohort": """
CREATE OR REPLACE VIEW secure.v_retention_cohort AS
SELECT
  cohort_date, cohort_size, d1_retained, d7_retained, d30_retained,
  d1_retention, d7_retention, d30_retention, is_matured_d7, is_matured_d30
FROM gold.fct_retention_cohort
""",
        # ---- purchases: payment instrument masked -------------------------
        "v_purchases_analyst": """
CREATE OR REPLACE VIEW secure.v_purchases_analyst AS
SELECT
  purchase_id,
  session_id,
  player_id,
  sku,
  item_name,
  category,
  price_usd,
  currency,
  quantity,
  purchase_ts,
  event_date,
  cast(NULL AS string) AS payment_method
FROM silver.purchases
""",
    }


GOVERNED_VIEWS = tuple(_view_sql(2025))

#: Registered transform metadata for the governed views: which base tables they
#: read and where each view column comes from.  Consumed by the lineage builder
#: so masked PII is visible as a first-class edge in the graph.
GOVERNED_VIEW_LINEAGE: dict[str, dict] = {
    "secure.v_dim_player_analyst": {
        "tables": ["gold.dim_player", "silver.sessions"],
        "columns": {
            "player_id": ["gold.dim_player.player_id"],
            "first_seen": ["gold.dim_player.first_seen"],
            "last_seen": ["gold.dim_player.last_seen"],
            "region": ["gold.dim_player.region"],
            "country": ["silver.sessions.country"],
            "player_email": ["silver.sessions.player_email"],
            "player_ip": ["silver.sessions.player_ip"],
            "age_bucket": ["silver.sessions.birth_year"],
            "lifetime_spend": ["gold.dim_player.lifetime_spend"],
            "lifetime_sessions": ["gold.dim_player.lifetime_sessions"],
            "lifetime_playtime_minutes": ["gold.dim_player.lifetime_playtime_minutes"],
            "days_since_last_seen": ["gold.dim_player.days_since_last_seen"],
            "churn_flag": ["gold.dim_player.churn_flag"],
        },
    },
    "secure.v_dim_player_emea": {
        "tables": ["gold.dim_player", "silver.sessions"],
        "columns": {
            "player_id": ["gold.dim_player.player_id"],
            "region": ["gold.dim_player.region"],
            "country": ["silver.sessions.country"],
            "player_email": ["silver.sessions.player_email"],
            "player_ip": ["silver.sessions.player_ip"],
            "age_bucket": ["silver.sessions.birth_year"],
            "lifetime_spend": ["gold.dim_player.lifetime_spend"],
            "churn_flag": ["gold.dim_player.churn_flag"],
        },
    },
    "secure.v_fct_player_daily": {
        "tables": ["gold.fct_player_daily"],
        "columns": {
            "event_date": ["gold.fct_player_daily.event_date"],
            "player_id": ["gold.fct_player_daily.player_id"],
            "country": ["gold.fct_player_daily.country"],
            "region": ["gold.fct_player_daily.region"],
            "sessions": ["gold.fct_player_daily.sessions"],
            "playtime_minutes": ["gold.fct_player_daily.playtime_minutes"],
            "spend_usd": ["gold.fct_player_daily.spend_usd"],
        },
    },
    "secure.v_fct_player_daily_emea": {
        "tables": ["gold.fct_player_daily"],
        "columns": {
            "event_date": ["gold.fct_player_daily.event_date"],
            "player_id": ["gold.fct_player_daily.player_id"],
            "region": ["gold.fct_player_daily.region"],
            "spend_usd": ["gold.fct_player_daily.spend_usd"],
        },
    },
    "secure.v_monetization_daily": {
        "tables": ["gold.fct_monetization_daily"],
        "columns": {
            "event_date": ["gold.fct_monetization_daily.event_date"],
            "dau": ["gold.fct_monetization_daily.dau"],
            "paying_users": ["gold.fct_monetization_daily.paying_users"],
            "revenue_usd": ["gold.fct_monetization_daily.revenue_usd"],
            "arpdau": ["gold.fct_monetization_daily.arpdau"],
            "conversion_rate": ["gold.fct_monetization_daily.conversion_rate"],
        },
    },
    "secure.v_retention_cohort": {
        "tables": ["gold.fct_retention_cohort"],
        "columns": {
            "cohort_date": ["gold.fct_retention_cohort.cohort_date"],
            "cohort_size": ["gold.fct_retention_cohort.cohort_size"],
            "d1_retention": ["gold.fct_retention_cohort.d1_retention"],
            "d7_retention": ["gold.fct_retention_cohort.d7_retention"],
            "d30_retention": ["gold.fct_retention_cohort.d30_retention"],
        },
    },
    "secure.v_purchases_analyst": {
        "tables": ["silver.purchases"],
        "columns": {
            "purchase_id": ["silver.purchases.purchase_id"],
            "player_id": ["silver.purchases.player_id"],
            "price_usd": ["silver.purchases.price_usd"],
            "quantity": ["silver.purchases.quantity"],
            "payment_method": ["silver.purchases.payment_method"],
        },
    },
}


@dataclass
class PolicyReport:
    views_created: list[str] = field(default_factory=list)
    masked_pii_columns: list[str] = field(default_factory=list)
    row_filtered_views: list[str] = field(default_factory=list)
    reference_year: int = 0
    seconds: float = 0.0

    def as_dict(self) -> dict:
        return asdict(self)


def apply_policies(spark: SparkSession, settings: Settings | None = None) -> PolicyReport:
    """(Re)create every governed view and report what was masked."""
    settings = settings or get_settings()
    started = time.perf_counter()
    ref_year = reference_year(spark)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SECURE_SCHEMA}")

    sql_map = _view_sql(ref_year)
    created: list[str] = []
    row_filtered: list[str] = []
    for name, sql in sql_map.items():
        spark.sql(sql)
        created.append(f"{SECURE_SCHEMA}.{name}")
        if "region = 'EMEA'" in sql:
            row_filtered.append(f"{SECURE_SCHEMA}.{name}")

    return PolicyReport(
        views_created=created,
        masked_pii_columns=list(MASKED_PII_COLUMNS),
        row_filtered_views=row_filtered,
        reference_year=ref_year,
        seconds=round(time.perf_counter() - started, 2),
    )


def masking_proof(spark: SparkSession, player_id: str | None = None) -> dict[str, dict]:
    """Raw vs governed values for one player, used as evidence in the results."""
    if player_id is None:
        row = spark.sql(
            "SELECT player_id FROM gold.dim_player ORDER BY player_id LIMIT 1"
        ).collect()
        player_id = row[0]["player_id"]

    raw = spark.sql(
        f"""
        SELECT player_email, player_ip, country, birth_year
        FROM silver.sessions WHERE player_id = '{player_id}' LIMIT 1
        """
    ).collect()[0]
    masked = spark.sql(
        f"""
        SELECT player_email, player_ip, country, age_bucket
        FROM secure.v_dim_player_analyst WHERE player_id = '{player_id}' LIMIT 1
        """
    ).collect()[0]
    return {
        "player_id": player_id,
        "raw": {
            "player_email": raw["player_email"],
            "player_ip": raw["player_ip"],
            "country": raw["country"],
            "birth_year": raw["birth_year"],
        },
        "governed": {
            "player_email": masked["player_email"],
            "player_ip": masked["player_ip"],
            "country": masked["country"],
            "age_bucket": masked["age_bucket"],
        },
    }


def region_filter_proof(spark: SparkSession) -> dict[str, int]:
    """Row counts proving the EMEA row filter actually restricts rows."""
    base = spark.sql("SELECT count(*) AS c FROM gold.dim_player").collect()[0]["c"]
    emea = spark.sql("SELECT count(*) AS c FROM gold.dim_player WHERE region = 'EMEA'").collect()[
        0
    ]["c"]
    view = spark.sql("SELECT count(*) AS c FROM secure.v_dim_player_emea").collect()[0]["c"]
    distinct_regions = [
        r["region"]
        for r in spark.sql("SELECT DISTINCT region FROM secure.v_dim_player_emea").collect()
    ]
    return {
        "dim_player_rows": int(base),
        "emea_rows_in_base": int(emea),
        "view_rows": int(view),
        "distinct_regions_in_view": len(distinct_regions),
    }
