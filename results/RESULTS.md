# Measured Results

Every number in this file was captured from an actual run of this repository on the
machine described below. Nothing here is estimated, rounded up, or placeholder. The
README results section is populated exclusively from these figures.

**Environment**

| Item | Value |
| --- | --- |
| Platform | macOS (darwin) 25.0.0, Apple silicon |
| Python | 3.11.14 |
| Java | OpenJDK 11.0.29 (Homebrew) |
| Spark | 3.5.1 (PySpark) |
| Delta Lake | delta-spark 3.2.0 |
| Spark master | `local[*]` |
| `spark.sql.shuffle.partitions` | 8 |
| Catalog backend | Hive + embedded Derby (`spark-warehouse/metastore_db`) |

---

## Dataset

Generator: `python -m dlg.cli gen-data` (`src/dlg/generate_data.py`), seed **42**,
scale **full**, 25,000 players, 30 daily install cohorts plus a 30-day observation
tail (60 observed days) so that D7 and D30 retention cohorts can actually mature —
a D30 figure is not measurable inside a 30-day window.

Determinism verified by regenerating the whole landing zone and comparing `sha256`
over the sorted per-file `sha256` digest list:

```
run 1: 278dd82c73df36ea5e84910ed00db96294eafafa1d0ecd7047393402e29812f6
run 2: 278dd82c73df36ea5e84910ed00db96294eafafa1d0ecd7047393402e29812f6
=> byte-identical output
```

### Row counts

| Dataset | Landing files | Base rows | Total lines written | Injected defect lines |
| --- | ---: | ---: | ---: | ---: |
| `sessions` (JSON lines) | 5 | 200,000 | 202,722 | 2,722 |
| `sessions_updates` (late-arriving revisions, JSON lines) | 1 | 0 | 1,000 | 1,000 |
| `match_events` (JSON lines) | 5 | 600,000 | 607,927 | 7,927 |
| `purchases` (CSV) | 5 | 40,000 | 40,697 | 697 |
| **Total** | **16** | **840,000** | **852,346** | **12,346** |

Players: **25,000**. Install cohorts: **30 days**. Observed window: **60 days**.
Base rows are the "clean" generated records; total lines written includes every
deliberately injected defective line. Every player has at least one session and
`(player_id, session_id)` pairs are unique by construction, so the only duplicate
business keys downstream are the injected ones.

### Deliberately injected defects (measured counts)

| Defect class | Count | Where |
| --- | ---: | --- |
| `duplicate_row` (exact resend of a prior row) | 4,048 | sessions, match_events, purchases |
| `null_required_field` (null/empty business key) | 2,158 | sessions, match_events, purchases |
| `out_of_range_value` (negative/absurd numeric) | 2,504 | sessions, match_events, purchases |
| `bad_timestamp` (unparseable date string) | 1,733 | sessions, match_events, purchases |
| `corrupt_json` (truncated JSON line) | 816 | sessions, match_events |
| `malformed_csv_row` (wrong field count / unclosed quote) | 87 | purchases |
| `late_arriving_newer` (revision batch, newer `session_start_ts`) | 500 | sessions_updates |
| `late_arriving_older` (revision batch, older `session_start_ts`) | 500 | sessions_updates |
| **Total injected defect lines** | **12,346** | |

### PII present by design in the landing zone

`player_email`, `player_ip`, `country`, `birth_year` — carried on every `sessions`
row (all 202,722 lines). `payment_method` is PII on purchases. All of them are
masked downstream in the governance layer.

Wall-clock time for full-scale generation: **10.82 s**.

---

## Bronze and silver layers (schema enforcement, MERGE upserts, quarantine, time travel)

Each command is a separate process, so the "wall clock" figures include Spark + JVM
startup; the `secs` columns are the in-pipeline measurements reported by the CLI.

| Command | Wall clock |
| --- | ---: |
| `dlg bronze` (run 1) | 46.99 s |
| `dlg silver` (run 1) | 45.89 s |
| `dlg bronze` (run 2) | 50.47 s |
| `dlg silver` (run 2) | 53.69 s |
| `dlg demo-rollback` | 20.08 s |

### Bronze — ingest run 1

| dataset | source files | raw lines | parse errors | null-key rejects | duplicate rows dropped | rows inserted | table rows | secs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `sessions` | 5 | 202,722 | 186 | 497 | 1,013 | 201,026 | 201,026 | 23.42 |
| `match_events` | 5 | 607,927 | 630 | 1,489 | 2,869 | 602,939 | 602,939 | 13.78 |
| `purchases` | 5 | 40,697 | 87 | 172 | 166 | 40,272 | 40,272 | 4.99 |
| `sessions_updates` → `bronze.sessions` | 1 | 1,000 | 0 | 0 | 0 | 1,000 | 202,026 | 2.30 |

`bronze.quarantine` holds **3,061** rows (`PARSE_ERROR` = 903, `NULL_KEY` = 2,158).

### Silver — build run 1

| dataset | bronze rows | valid rows | dedup rows removed | rejected | rows inserted | table rows | secs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `sessions` | 202,026 | 200,952 | 952 | 1,074 | 200,000 | 200,000 | 19.52 |
| `match_events` | 602,939 | 600,000 | 0 | 2,939 | 600,000 | 600,000 | 17.18 |
| `purchases` | 40,272 | 40,000 | 0 | 272 | 40,000 | 40,000 | 6.95 |

`silver.quarantine` holds **4,285** rows:

| dataset | reject reason | rows |
| --- | --- | ---: |
| `sessions` | `BAD_TIMESTAMP` | 429 |
| `sessions` | `OUT_OF_RANGE` | 645 |
| `match_events` | `BAD_TIMESTAMP` | 1,180 |
| `match_events` | `OUT_OF_RANGE` | 1,759 |
| `purchases` | `BAD_TIMESTAMP` | 124 |
| `purchases` | `NEGATIVE_AMOUNT` | 107 |
| `purchases` | `OUT_OF_RANGE` | 41 |

`UNKNOWN_PLAYER` (referential integrity against `silver.sessions`) fired **0** times —
the generator guarantees every player has at least one session.

### Idempotency proof (identical pipelines run a second time)

| dataset | bronze run 1 rows | bronze run 2 rows | rows inserted on run 2 | silver run 1 rows | silver run 2 rows | inserted / updated on run 2 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `sessions` | 202,026 | 202,026 | **0** | 200,000 | 200,000 | **0 / 0** |
| `match_events` | 602,939 | 602,939 | **0** | 600,000 | 600,000 | **0 / 0** |
| `purchases` | 40,272 | 40,272 | **0** | 40,000 | 40,000 | **0 / 0** |

Row counts are identical across runs, and Delta's own MERGE metrics report zero
inserts and zero updates on the replay.

### Delta feature checks (live, not simulated)

| Check | Result |
| --- | --- |
| `silver.*` partitioned by `event_date` | `sessions` 30 partitions, `match_events` 29, `purchases` 30 |
| `delta.enableChangeDataFeed` | true on all bronze + silver tables |
| Change Data Feed readable | `silver.purchases` CDF returns 120,000 rows with `_change_type` ∈ {insert, delete} plus `_commit_version` / `_commit_timestamp` |
| Schema enforcement | `spark.databricks.delta.schema.autoMerge.enabled = false` session-wide; all DDL generated from explicit `StructType`s |
| Time travel | `DESCRIBE HISTORY` shows every commit with operation + metrics |

### Late-arriving revision handling (measured)

The `sessions_updates` batch revises 1,000 already-loaded sessions: 500 carry a
*newer* `session_start_ts`, 500 an *older* one. After the guarded MERGE:

| revision set | revisions | matched stored rows | revision value won |
| --- | ---: | ---: | ---: |
| newer `session_start_ts` | 500 | 500 | **500 / 500** |
| older `session_start_ts` | 500 | 500 | **0 / 500** |

### Time travel / rollback demo (`dlg demo-rollback` on `silver.purchases`)

| Stage | Delta version | Row count |
| --- | ---: | ---: |
| snapshot before the incident | 2 | 40,000 |
| destructive bad MERGE (re-inserts every row under a `BAD_` key) | 3 | 80,000 (+40,000) |
| `RESTORE TABLE ... TO VERSION AS OF 2` | 4 | 40,000 |
| assertion `restored == snapshot` | — | **true** |

---

## Gold marts and governance

`make gold` wall clock: **20.87 s**. `make governance` (catalog-validate +
policies-apply + lineage) wall clock: **18.99 s**.

### Gold mart row counts

| mart | rows | columns | inputs | secs |
| --- | ---: | ---: | --- | ---: |
| `gold.dim_player` | 25,000 | 10 | `silver.sessions`, `silver.purchases` | 10.31 |
| `gold.fct_player_daily` | 223,966 | 10 | `silver.sessions`, `silver.purchases` | 2.76 |
| `gold.fct_monetization_daily` | 60 | 8 | `silver.sessions`, `silver.purchases` | 2.26 |
| `gold.fct_retention_cohort` | 31 | 10 | `silver.sessions` | 2.48 |

`fct_player_daily` has more rows than there are sessions (200,000) because it is a
full outer join with daily spend: a player-day with a purchase but no session is
still a fact row.

### Catalog size

| Metric | Value |
| --- | ---: |
| Tables declared | **12** |
| Columns declared | **162** |
| Columns classified `PII` | **14** |
| Columns classified `FINANCIAL` | **10** |
| Domains | 3 |
| Catalog drift issues vs live Delta schemas | **0** |

`dlg catalog-validate` output: `catalog validation: 12 tables checked, 0 drift issue(s) — OK`.

### Lineage

| Metric | Value |
| --- | ---: |
| Lineage nodes (tables + governed views) | **20** |
| Table-level edges | **27** |
| Column-level edges | **149** |

Example impact query:

```
$ python -m dlg.cli impact silver.sessions.player_email
silver.sessions.player_email impacts 2 downstream columns across 2 tables:
  secure.v_dim_player_analyst.player_email
  secure.v_dim_player_emea.player_email
```

### Governance: masking and row filters

| Metric | Value |
| --- | ---: |
| Governed views created | **7** |
| PII columns masked | **5** (`player_email`, `player_ip`, `country`, `birth_year`, `payment_method`) |
| Views carrying a row filter | **2** (`secure.v_dim_player_emea`, `secure.v_fct_player_daily_emea`) |

Raw vs governed values for the same player (`P000001` family):

| column | raw `silver.sessions` | `secure.v_dim_player_analyst` |
| --- | --- | --- |
| `player_email` | `ava.moreau1@example.com` | `pii_0ebe1af1cb87a8d7d9ea900ef95e1872` |
| `player_ip` | `58.36.189.27` | `NULL` |
| `country` | `GB` | `NULL` |
| `birth_year` | `2006` | `age_bucket = 18-24` |

Row filter proof:

| Measure | Value |
| --- | ---: |
| `gold.dim_player` rows | 25,000 |
| rows with `region = 'EMEA'` in the base table | 8,097 |
| rows visible through `secure.v_dim_player_emea` | 8,097 |
| distinct regions visible through the view | **1** |

### Access matrix

| Metric | Value |
| --- | ---: |
| Roles | 6 (`admin`, `data_engineer`, `analyst`, `regional_analyst_emea`, `marketing`, `agent_service`) |
| Objects | 19 (12 tables + 7 governed views) |
| Operations | 8 (`SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `CREATE`, `DROP`, `ALTER`) |
| Total grant-matrix cells | **912** |
| Allow cells | **255** |
| Deny cells | **657** |

Scope guarantee, spot-checked by `dlg access-matrix`:

| check | decision | reason code |
| --- | --- | --- |
| `agent_service` SELECT `bronze.sessions` | deny | `DENY_ROLE_SCOPE_VIOLATION` |
| `agent_service` SELECT `silver.sessions` | deny | `DENY_ROLE_SCOPE_VIOLATION` |
| `agent_service` SELECT `gold.dim_player` | deny | `DENY_ROLE_SCOPE_VIOLATION` |
| `agent_service` SELECT `secure.v_dim_player_analyst` | allow | `ALLOW_EXPLICIT_GRANT` |

### Headline business metrics (computed from the live gold marts)

| Metric | Value |
| --- | ---: |
| Days observed | 60 |
| Average DAU | **3,333.20** |
| Peak DAU | 5,702 |
| Total revenue | $1,193,150.13 |
| Total purchases | 40,000 |
| Average ARPDAU | **$5.1456** |
| Average payer conversion rate | 16.03% |
| **D1 retention** | **45.82%** |
| **D7 retention** | **25.81%** |
| **D30 retention** | **10.48%** |
| Churned players (no session in the last 7 days) | 18,120 of 25,000 (**72.48%**) |
| Average lifetime spend | $47.73 |

Retention is pooled across matured cohorts (retained players / cohort members) and
maturity-gated: D1 and D7 matured for 31 cohorts, D30 for 30 (the final cohort is
not old enough for a D30 reading and reports `NULL`, not zero).

---
