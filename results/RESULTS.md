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
| Table-level edges | **28** |
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

## Data quality, SLAs and observability

`make quality` = `quality-run` + `sla-check` + `observe`. Wall clock for the whole
target: **96.58 s** (quality 42.77 s, SLA 27.47 s, observe 28.14 s, each a separate
JVM).

### Expectation run

| Metric | Value |
| --- | ---: |
| Expectation suites | 12 (one per silver/gold table plus two bronze contracts) |
| Expectations evaluated | **57** |
| Passed | **56** |
| Failed | **1** |
| BLOCKING failures | **0** (the build gates on this and passes) |
| WARN failures | **1** |
| Overall pass rate | **98.25%** |
| Quality run wall clock | **39.02 s** (in-pipeline) |

Results are persisted to `ops.dq_results` — **57 rows**, 14 columns:

| status | rows |
| --- | ---: |
| `PASS` | 56 |
| `FAIL` | 1 |

The single failing expectation is the deliberate one:

| | |
| --- | --- |
| expectation | `bronze.sessions.raw_session_start_ts_is_iso8601` (WARN) |
| kind | `regex` on `bronze.sessions.session_start_ts` |
| rows checked | 202,026 |
| rows failed | **429** |
| fail rate | **0.002123** (0.2123%) |
| threshold | 0.0005 (a 99.95% ISO-8601 conformance SLO) |
| why it fails | the generator injects unparseable client timestamps into the landing files. `silver` quarantines those rows as `BAD_TIMESTAMP`, but the **raw** contract is still violated — which is exactly the signal the alerting path exists to surface. |

### SLA checks

| Metric | Value |
| --- | ---: |
| SLA checks | **36** (3 per table: staleness, volume, null rate) |
| OK | 33 |
| Breached | **1** |
| Not applicable | 2 (quarantine tables have only audit columns) |
| SLA run wall clock | 16.14 s |

`ops.sla_status` — **36 rows**, 9 columns.

The real breach:

| | |
| --- | --- |
| table | `gold.fct_retention_cohort` |
| SLA | `max_null_rate` |
| measured | **0.032258** (3.23%) |
| threshold | 0.02 (2%) |
| worst column | `d30_retained` |
| why it breaches | the newest install cohort has not matured for D30, so its D30 aggregates are `NULL` rather than zero. The maturity gate is doing its job; the alert correctly flags that the aggregate is not fully populated. |

Staleness is measured against the lake's own clock (ingest timestamp for
bronze/silver, business clock for marts). All 12 tables pass staleness; the
largest measured staleness is 434.11 minutes against a 43,200-minute limit.

### Alerts

`results/alerts.json` — **2 alerts**:

| severity | source | id | metric vs threshold |
| --- | --- | --- | --- |
| `WARNING` | `dq_expectation` | `dq::bronze.sessions::raw_session_start_ts_is_iso8601` | fail_rate 0.002123 vs 0.0005 |
| `CRITICAL` | `sla` | `sla::gold.fct_retention_cohort::max_null_rate` | 0.032258 vs 0.02 |

### Observability snapshot

`results/observability_report.md` + `ops.table_metrics` (**12 rows**, 15 columns):

| Metric | Value |
| --- | ---: |
| Tables instrumented | **12** |
| Total rows | **1,941,640** |
| Total size | **175,709,453 bytes** (175.7 MB) |
| Total active files | **150** |
| Snapshot wall clock | 23.86 s |

Per-table highlights (rows / size / files / delta version):

| table | rows | size (MB) | files | delta v |
| --- | ---: | ---: | ---: | ---: |
| `bronze.sessions` | 202,026 | 22.16 | 2 | 4 |
| `bronze.match_events` | 602,939 | 55.89 | 1 | 2 |
| `bronze.purchases` | 40,272 | 3.71 | 1 | 2 |
| `bronze.quarantine` | 3,061 | 0.40 | 3 | 6 |
| `silver.sessions` | 200,000 | 27.10 | 60 | 2 |
| `silver.match_events` | 600,000 | 59.19 | 36 | 2 |
| `silver.purchases` | 40,000 | 4.34 | 40 | 4 |
| `silver.quarantine` | 4,285 | 0.60 | 3 | 6 |
| `gold.fct_player_daily` | 223,966 | 1.53 | 1 | 4 |
| `gold.dim_player` | 25,000 | 0.79 | 1 | 4 |
| `gold.fct_monetization_daily` | 60 | 0.00 | 1 | 4 |
| `gold.fct_retention_cohort` | 31 | 0.00 | 1 | 4 |

---

## Incremental Structured Streaming ingest

Script: `scripts/stream_demo.sh` (exit code **0**, all four assertions passed).
Source: `landing/sessions_stream`, an Auto-Loader-style file source with an explicit
schema, `maxFilesPerTrigger=2` and a checkpoint under `_checkpoints/sessions_stream`.
Every number below is read back from `ops.stream_batches` and the live Delta tables.

### Micro-batches processed

| batch_id | files | rows read | bronze + | silver + | silver ~ | duplicates suppressed | quarantined | end-to-end latency (ms) | handler (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `stream_f78924bf520328d6` | 2 | 100,000 | 99,189 | 98,676 | 0 | 77 | 734 | 22,385 | 19.07 |
| `stream_e99e457be720c178` | 1 | 50,000 | 49,581 | 49,319 | 0 | 42 | 377 | 33,093 | 9.00 |
| `stream_4a4f1c3b0990a7a5` | 2 | 52,722 | 52,256 | 52,005 | 0 | 61 | 405 | 22,956 | 19.76 |
| `stream_cae9305cb155033c` (duplicate file) | 1 | 50,000 | **0** | **0** | 0 | **50,000** | **0** | 37,721 | 13.50 |
| `stream_befd167bb5444369` (duplicate-only batch) | 1 | 50,000 | **0** | **0** | 0 | **50,000** | **0** | 25,957 | 22.67 |
| **total** | **7** | **302,722** | **201,026** | **200,000** | **0** | **100,180** | **1,516** | — | — |

* micro-batches processed: **5**
* rows ingested: **200,000** into `silver.sessions_stream`, **201,026** into
  `bronze.sessions_stream`
* duplicate rows suppressed: **100,180** (of which 100,000 came from the two
  byte-identical file copies)
* streaming rows quarantined: **1,516** (`PARSE_ERROR`, `NULL_KEY`, `BAD_TIMESTAMP`,
  `OUT_OF_RANGE` — all written to `bronze.quarantine` with `_dataset = sessions_stream`)
* end-to-end streaming latency per batch: **22,385 / 33,093 / 22,956 / 37,721 /
  25,957 ms**; mean **28,422 ms**, p95 **37,721 ms**
  *(measured from the newest source file's mtime to the micro-batch merge completing;
  the first batch includes the query + JVM warm-up)*
* within-batch handler time: 19.07 / 9.00 / 19.76 / 13.50 / 22.67 s; mean **16.80 s**

The streaming path reproduces the batch layer exactly: `bronze.sessions_stream` =
**201,026** rows and `silver.sessions_stream` = **200,000** rows, identical to
`bronze.sessions` / `silver.sessions`.

### Idempotency assertions

| # | assertion | evidence | result |
| --- | --- | --- | --- |
| (a) | new rows appear after two new micro-batch files are dropped | 147,995 → 200,000 (+52,005) | **PASS** |
| (b) | a byte-identical copy of an already-processed file adds zero rows | silver +0, bronze +0, quarantine +0 | **PASS** |
| (c) | re-running with no new files adds zero rows (file-source checkpoint) | 200,000 → 200,000 | **PASS** |
| (d) | replaying the last micro-batch through the same handler adds zero rows | `skipped_replay=true`, bronze +0, silver +0, quarantine +0 | **PASS** |

Assertion (d) is the content-addressed `_batch_id` guard: the batch id is a hash of
the batch's source files, so re-delivering the same batch is recognised in
`ops.stream_batches` and short-circuits before any MERGE runs. This is what makes
the pipeline exactly-once-style even if the checkpoint is lost, because the
dedup key is derived from the data rather than from the query's state.

---

## Governed agentic analytics service

Provider: `stub-rule-based` (deterministic, offline — no network, no API key).
`make agent-eval` wall clock: **100.49 s** (includes Spark startup).

### Eval suite v1

| Metric | Value |
| --- | ---: |
| Eval cases | **53** |
| Answer accuracy (exact expected tool or refusal code) | **100.00%** (53/53) |
| Tool-selection accuracy (over the 24 cases that expect a tool) | **100.00%** (24/24) |
| Refusal precision | **100.00%** |
| Refusal recall | **100.00%** |
| Cases that expect a refusal | 29 |
| **Safety-refusal rate** (19 `safety`-tagged cases) | **100.00%** (19/19) |
| False-refusal rate on benign cases | **0.00%** (0/24) |
| Tool errors | **0** |
| Mean latency | **397 ms** |
| p50 latency | **49 ms** |
| p95 latency | **615 ms** |
| Max latency | 10,945 ms (first query of the run — Spark warm-up) |

Coverage by tag:

| tag | cases | matched | accuracy |
| --- | ---: | ---: | ---: |
| `happy` (metric / metadata / freshness / SQL lookups) | 24 | 24 | 100% |
| `pii` (direct PII requests) | 6 | 6 | 100% |
| `sql` (DDL / DML / injection attempts) | 7 | 7 | 100% |
| `ungranted` (bronze / silver / gold / ops access) | 6 | 6 | 100% |
| `ambiguous` (under-specified questions) | 5 | 5 | 100% |
| `scope` (out-of-scope topics) | 5 | 5 | 100% |

The gate is real: an earlier revision of the planner scored 92.45% with a
89.47% safety-refusal rate (3 of 19 safety cases were answered instead of
refused), and `run_evals.py` **exited non-zero**. The planner's intent
precedence was fixed and the suite was re-run to green.

### Reason codes exercised

| reason code | meaning | caught by |
| --- | --- | --- |
| `PII_BLOCKED` | the question or SQL asks for a classified PII attribute | question policy + SQL guard |
| `UNGRANTED_OBJECT` | the role cannot `SELECT` the referenced object | access matrix |
| `UNSAFE_SQL` | not a single read-only SELECT (multi-statement, DDL, DML) | SQL guard |
| `OUT_OF_SCOPE` | the question is not about this lakehouse | question policy |
| `AMBIGUOUS` | the question or arguments cannot be resolved | question policy + tool validation |

### Audit trail

`ops.agent_audit` after one eval run over a clean lake: **77 rows** — one row per
agent step (24 tool cases × 2 steps + 29 refusal cases × 1 step). The table is
append-only, so it accumulates across runs; this is the count for a single suite.

| status | rows |
| --- | ---: |
| `ok` | 48 |
| `refused` | 29 |

### Service smoke test

`make serve` (uvicorn, `127.0.0.1`) then:

| request | result |
| --- | --- |
| `GET /health` | `status=ok`, 6 roles, 6 tools, provider `stub-rule-based` |
| `POST /ask` `X-DLG-Role: analyst` — "What was the average DAU last week?" | `status=ok`, `tool=get_metric`, `sql=SELECT event_date, dau FROM secure.v_monetization_daily LIMIT 100`, 60 rows, 6,043 ms |
| `POST /ask` `X-DLG-Role: analyst` — "List the player emails for our top spenders" | **`status=refused`, `reason_code=PII_BLOCKED`** |
| `POST /ask` `X-DLG-Role: agent_service` — "SELECT * FROM bronze.sessions" | **`status=refused`, `reason_code=UNGRANTED_OBJECT`** |
| `GET /tools` | 6 typed tools with JSON schemas |
| `GET /audit?limit=6` | latest audit rows, newest first |

---

## Tests, CI and end-to-end verification

### Test suite

`make test` = `pytest --cov=dlg --cov-report=term-missing`.

| Metric | Value |
| --- | ---: |
| Tests | **211 passing** |
| Coverage (`dlg` package) | **80%** |
| `make test` wall clock (with coverage) | **448.67 s** |
| Failing / skipped | 0 / 0 |

Coverage by area (selected):

| module | coverage |
| --- | ---: |
| `dlg/schemas.py` | 100% |
| `dlg/quality/expectations.py` | 98% |
| `dlg/timetravel.py` | 98% |
| `dlg/agent/policy.py` | 96% |
| `dlg/quality/alerts.py` | 94% |
| `dlg/quality/observability.py` | 94% |
| `dlg/pipelines/bronze.py` | 95% |
| `dlg/governance/policies.py` | 95% |
| `dlg/pipelines/gold.py` | 94% |
| `dlg/agent/tools.py` | 91% |
| `dlg/pipelines/stream.py` | 91% |
| `dlg/governance/catalog.py` | 92% |
| `dlg/governance/access.py` | 88% |
| `dlg/pipelines/silver.py` | 88% |

`dlg/cli.py` and `dlg/agent/service.py` show as 0% because they are exercised by the
`make` targets and the HTTP smoke test rather than by pytest — `cli.py` is process
entry-point wiring and `service.py` is FastAPI route wiring.

The suite shares one session-scoped SparkSession pointed at a temporary lake root
(`tests/conftest.py`), generates a 2% dataset with the real generator, and builds the
whole medallion once. Tests then assert against real Delta tables rather than mocks.

### Regression baseline

`tests/baseline/metrics_baseline.json` holds the real gold metrics for both scales
(`full`, used by `make baseline-update` on a developer machine; `small`, used by CI).
Counts and volume invariants are compared exactly, rate metrics with a **2%**
relative tolerance. `python -m dlg.cli baseline-check` exits non-zero on drift.

### End-to-end run

A full teardown (`rm -rf lakehouse spark-warehouse landing _checkpoints`) followed by
`make all` completed every stage: gen-data → bronze → silver → gold → governance →
quality → stream → agent-eval → test. The numbers in this file were re-verified
against that run:

| check | result |
| --- | --- |
| bronze row counts | 202,026 / 602,939 / 40,272 — identical |
| silver row counts | 200,000 / 600,000 / 40,000 — identical |
| silver quarantine by reason | 429 / 645 / 1,180 / 1,759 / 124 / 107 / 41 — identical |
| gold mart row counts | 223,966 / 25,000 / 60 / 31 — identical |
| headline metrics | DAU 3,333.20, ARPDAU $5.1456, D1 45.82%, D7 25.81%, D30 10.48% — identical |
| catalog drift | 0 issues over 162 columns |
| lineage | 20 nodes / 28 table edges / 149 column edges |
| grant matrix | 912 cells, 255 allow / 657 deny |
| DQ | 57 expectations, 56 pass, 1 WARN fail, 98.25% |
| SLA | 36 checks, 33 OK, 1 breach, 2 N/A |
| streaming (`make stream`, main landing zone) | 3 micro-batches, 200,000 rows into `silver.sessions_stream`, 1,516 quarantined |
| eval | 53 cases, 100% answer accuracy, 100% safety-refusal rate |
| baseline check | OK for scale `full` |

`make stream` over the main landing zone produced 3 micro-batches (2 + 2 + 1 files at
`maxFilesPerTrigger=2`) and reproduced the batch layer exactly:
`bronze.sessions_stream` = 201,026 and `silver.sessions_stream` = 200,000, the same
row counts as the batch tables.

### CI

`.github/workflows/ci.yml` has two jobs:

1. **`test`** — ubuntu-latest, Java 17, Python 3.11, pip + ivy caching; runs
   `ruff check`, `ruff format --check` and `pytest --cov`; uploads `coverage.xml`.
2. **`data-tests`** — `DLG_SCALE=small`; runs gen-data → bronze → silver → gold →
   governance → `quality-run` → `sla-check` → `observe` → `stream --once` →
   `agent-eval` → `baseline-check`, and uploads `results/` as an artifact.

Both hard gates are real: `quality-run` exits non-zero on any BLOCKING expectation
failure, and `agent-eval` exits non-zero unless **every** safety-tagged case is
refused. Because the volume guards are ratio-based rather than absolute, the same
`suites.yml` gates both the full run and CI's reduced run.

---
