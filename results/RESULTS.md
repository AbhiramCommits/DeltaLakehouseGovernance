# Measured Results

Every number in this file was captured from an actual run of this repository on the
machine described below. Nothing here is estimated, rounded up, or placeholder. The
README results section is populated exclusively from these figures.

**Environment**

| Item | Value |
| --- | --- |
| Platform | macOS (darwin), Apple silicon |
| Python | 3.11.14 |
| Java | OpenJDK 11.0.29 (Homebrew) |
| Spark | 3.5.1 (PySpark) |
| Delta Lake | delta-spark 3.2.0 |
| Spark master | `local[*]` |
| `spark.sql.shuffle.partitions` | 8 |

---

## Dataset

Generator: `python -m dlg.cli gen-data` (`src/dlg/generate_data.py`), seed **42**,
scale **full**, 30 days starting 2025-09-01, 25,000 players.

Determinism was verified by regenerating the whole landing zone and comparing
`sha256` over the sorted per-file `sha256` digest list:

```
run 1: 4222894becf44b2cb128a3563666e3a1e5249b6a9efae97abc2b2020e3eaa576
run 2: 4222894becf44b2cb128a3563666e3a1e5249b6a9efae97abc2b2020e3eaa576
=> byte-identical output
```

### Row counts

| Dataset | Landing files | Base rows | Total lines written | Injected defect lines |
| --- | ---: | ---: | ---: | ---: |
| `sessions` (JSON lines) | 5 | 200,000 | 202,715 | 2,715 |
| `sessions_updates` (late-arriving revisions, JSON lines) | 1 | 0 | 1,000 | 1,000 |
| `match_events` (JSON lines) | 5 | 600,000 | 607,930 | 7,930 |
| `purchases` (CSV) | 5 | 40,000 | 40,784 | 784 |
| **Total** | **16** | **840,000** | **852,429** | **12,429** |

Players: **25,000**. Days: **30**. Base rows are the "clean" generated records; total
lines written includes every deliberately injected defective line. Every generated
player has at least one session, and `(player_id, session_id)` pairs are unique by
construction, so the only duplicate business keys downstream are the injected ones.

### Deliberately injected defects (measured counts)

| Defect class | Count | Where |
| --- | ---: | --- |
| `duplicate_row` (exact resend of a prior row) | 4,104 | sessions, match_events, purchases |
| `null_required_field` (null/empty business key) | 2,182 | sessions, match_events, purchases |
| `out_of_range_value` (negative/absurd numeric) | 2,495 | sessions, match_events, purchases |
| `bad_timestamp` (unparseable date string) | 1,735 | sessions, match_events, purchases |
| `corrupt_json` (truncated JSON line) | 823 | sessions, match_events |
| `malformed_csv_row` (wrong field count / unclosed quote) | 90 | purchases |
| `late_arriving_newer` (revision batch, newer `session_start_ts`) | 500 | sessions_updates |
| `late_arriving_older` (revision batch, older `session_start_ts`) | 500 | sessions_updates |
| **Total injected defect lines** | **12,429** | |

### PII present by design in the landing zone

`player_email`, `player_ip`, `country`, `birth_year` — carried on every `sessions`
row (all 202,715 lines), and masked downstream in the governance layer.

Wall-clock time for full-scale generation: **9.46 s** (generator-internal
`wall_clock_seconds`).

---

## Bronze and silver layers (schema enforcement, MERGE upserts, quarantine, time travel)

Commands (each is a separate process, so the timings include Spark + JVM startup):

```
python -m dlg.cli bronze   # run 1 = 52.83 s wall clock
python -m dlg.cli silver   # run 1 = 75.22 s wall clock
python -m dlg.cli bronze   # run 2 = 115.67 s wall clock
python -m dlg.cli silver   # run 2 = 82.04 s wall clock
python -m dlg.cli demo-rollback   # 22.12 s wall clock
```

The `secs` columns below are the in-pipeline `wall_clock` measurements reported by
the CLI itself, excluding JVM startup.

### Bronze — ingest run 1

| dataset | source files | raw lines | parse errors | null-key rejects | duplicate rows dropped | rows inserted | table rows | secs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `sessions` | 5 | 202,715 | 190 | 522 | 1,007 | 200,996 | 200,996 | 19.47 |
| `match_events` | 5 | 607,930 | 633 | 1,472 | 2,868 | 602,957 | 602,957 | 18.27 |
| `purchases` | 5 | 40,784 | 90 | 188 | 229 | 40,277 | 40,277 | 9.19 |
| `sessions_updates` → `bronze.sessions` | 1 | 1,000 | 0 | 0 | 0 | 1,000 | 201,996 | 3.19 |

Parse errors come from truncated JSON lines and malformed CSV rows; null-key rejects
are rows whose business key is null or blank. `bronze.quarantine` holds **3,095** rows
in total (`PARSE_ERROR` = 913, `NULL_KEY` = 2,182).

### Silver — build run 1

| dataset | bronze rows | valid rows | dedup rows removed | rejected | rows inserted | table rows | secs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `sessions` | 201,996 | 200,951 | 951 | 1,045 | 200,000 | 200,000 | 31.06 |
| `match_events` | 602,957 | 600,000 | 0 | 2,957 | 600,000 | 600,000 | 29.73 |
| `purchases` | 40,277 | 40,000 | 0 | 277 | 40,000 | 40,000 | 12.13 |

`silver.quarantine` holds **4,279** rows:

| dataset | reject reason | rows |
| --- | --- | ---: |
| `sessions` | `BAD_TIMESTAMP` | 431 |
| `sessions` | `OUT_OF_RANGE` | 614 |
| `match_events` | `BAD_TIMESTAMP` | 1,190 |
| `match_events` | `OUT_OF_RANGE` | 1,767 |
| `purchases` | `BAD_TIMESTAMP` | 114 |
| `purchases` | `NEGATIVE_AMOUNT` | 115 |
| `purchases` | `OUT_OF_RANGE` | 48 |

`UNKNOWN_PLAYER` (referential integrity against `silver.sessions`) triggered **0**
times — the generator guarantees every player has at least one session.

### Idempotency proof (identical pipelines run a second time)

| dataset | bronze run 1 rows | bronze run 2 rows | rows inserted on run 2 | silver run 1 rows | silver run 2 rows | rows inserted/updated on run 2 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `sessions` | 201,996 | 201,996 | **0** | 200,000 | 200,000 | **0 / 0** |
| `match_events` | 602,957 | 602,957 | **0** | 600,000 | 600,000 | **0 / 0** |
| `purchases` | 40,277 | 40,277 | **0** | 40,000 | 40,000 | **0 / 0** |

Row counts are byte-for-byte identical across runs and Delta's own MERGE metrics
report zero inserts and zero updates on the replay.

### Delta feature checks (live, not simulated)

| Check | Result |
| --- | --- |
| `silver.*` partitioned by `event_date` | `sessions` 30 partitions, `match_events` 29, `purchases` 30 |
| `delta.enableChangeDataFeed` on bronze + silver | true on all 6 tables |
| Change Data Feed readable | `silver.purchases` CDF returns 120,000 rows with `_change_type` ∈ {insert, delete}, `_commit_version`, `_commit_timestamp` |
| Schema enforcement | session-wide `schema.autoMerge = false`; DDL is generated from explicit `StructType`s |
| `schema.autoMerge.enabled` | false |

### Late-arriving revision handling (measured)

The `sessions_updates` batch revises 1,000 already-loaded sessions: 500 carry a
*newer* `session_start_ts`, 500 an *older* one. After MERGE:

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

Delta history for `silver.purchases` after the demo:
`CREATE TABLE(0) → MERGE(1) → MERGE(2) → MERGE(3, the bad one) → RESTORE(4)`.

---

