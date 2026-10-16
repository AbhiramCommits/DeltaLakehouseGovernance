"""PII masking and row-level security in the governed views."""

from __future__ import annotations

import pytest

from dlg.governance.policies import (
    AGE_BUCKETS,
    GOVERNED_VIEWS,
    PSEUDONYM_PREFIX,
    apply_policies,
    masking_proof,
    reference_year,
    region_filter_proof,
)


@pytest.fixture(scope="module")
def player(spark, lakehouse) -> str:
    row = spark.sql("SELECT player_id FROM silver.sessions ORDER BY player_id LIMIT 1").collect()
    return row[0]["player_id"]


def test_all_governed_views_are_created(spark, lakehouse):
    views = {t.name for t in spark.catalog.listTables("secure")}
    for view in GOVERNED_VIEWS:
        assert view in views, view


def test_email_is_pseudonymised_not_exposed(spark, lakehouse, player):
    raw = spark.sql(
        f"SELECT player_email FROM silver.sessions WHERE player_id = '{player}' LIMIT 1"
    ).collect()[0]["player_email"]
    masked = spark.sql(
        f"SELECT player_email FROM secure.v_dim_player_analyst WHERE player_id = '{player}'"
    ).collect()[0]["player_email"]
    assert masked != raw
    assert masked.startswith(PSEUDONYM_PREFIX)
    # sha2-256 truncated to 32 hex characters
    assert len(masked) == len(PSEUDONYM_PREFIX) + 32
    assert all(c in "0123456789abcdef" for c in masked[len(PSEUDONYM_PREFIX) :])


def test_pseudonym_is_deterministic(spark, lakehouse, player):
    query = f"SELECT player_email FROM secure.v_dim_player_analyst WHERE player_id = '{player}'"
    first = spark.sql(query).collect()[0]["player_email"]
    second = spark.sql(query).collect()[0]["player_email"]
    assert first == second


def test_ip_is_nulled(spark, lakehouse, player):
    row = spark.sql(
        f"SELECT player_ip FROM secure.v_dim_player_analyst WHERE player_id = '{player}'"
    ).collect()[0]
    assert row["player_ip"] is None


def test_country_is_removed_from_the_analyst_view(spark, lakehouse, player):
    row = spark.sql(
        f"SELECT country FROM secure.v_dim_player_analyst WHERE player_id = '{player}'"
    ).collect()[0]
    assert row["country"] is None


def test_birth_year_becomes_an_age_bucket(spark, lakehouse, player):
    proof = masking_proof(spark, player)
    assert "birth_year" not in proof["governed"]
    bucket = proof["governed"]["age_bucket"]
    labels = {label for _, label in AGE_BUCKETS} | {"55+", "unknown"}
    assert bucket in labels, bucket
    # the bucket must be derivable from the raw year
    year = reference_year(spark)
    raw_year = proof["raw"]["birth_year"]
    if raw_year is not None:
        age = year - raw_year
        expected = next((label for limit, label in AGE_BUCKETS if age < limit), "55+")
        assert bucket == expected


def test_region_row_filter_restricts_the_emea_view(spark, lakehouse):
    proof = region_filter_proof(spark)
    assert proof["view_rows"] == proof["emea_rows_in_base"]
    assert proof["view_rows"] < proof["dim_player_rows"]
    assert proof["distinct_regions_in_view"] == 1


def test_row_filter_view_contains_only_emea(spark, lakehouse):
    regions = {
        r["region"]
        for r in spark.sql("SELECT DISTINCT region FROM secure.v_dim_player_emea").collect()
    }
    assert regions == {"EMEA"}


def test_policies_are_idempotent(spark, settings, lakehouse):
    first = apply_policies(spark, settings)
    second = apply_policies(spark, settings)
    assert first.views_created == second.views_created
    assert len(second.masked_pii_columns) == 5


def test_analyst_view_row_count_matches_the_dimension(spark, lakehouse):
    dim = spark.table("gold.dim_player").count()
    view = spark.table("secure.v_dim_player_analyst").count()
    assert view == dim


def test_purchases_view_nulls_the_payment_method(spark, lakehouse):
    row = spark.sql("SELECT payment_method FROM secure.v_purchases_analyst LIMIT 1").collect()[0]
    assert row["payment_method"] is None
