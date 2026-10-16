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
run 1: f3088c8882deea9ac199a10c6e1b119ec4c4fc6d3d8ab38d3f128da7f775642d
run 2: f3088c8882deea9ac199a10c6e1b119ec4c4fc6d3d8ab38d3f128da7f775642d
=> byte-identical output
```

### Row counts

| Dataset | Landing files | Base rows | Total lines written | Injected defect lines |
| --- | ---: | ---: | ---: | ---: |
| `sessions` (JSON lines) | 5 | 200,000 | 202,732 | 2,732 |
| `sessions_updates` (late-arriving revisions, JSON lines) | 1 | 0 | 1,000 | 1,000 |
| `match_events` (JSON lines) | 5 | 600,000 | 607,944 | 7,944 |
| `purchases` (CSV) | 5 | 40,000 | 40,755 | 755 |
| **Total** | **16** | **840,000** | **852,431** | **12,431** |

Players: **25,000**. Days: **30**. Base rows are the "clean" generated records; total
lines written includes every deliberately injected defective line.

### Deliberately injected defects (measured counts)

| Defect class | Count | Where |
| --- | ---: | --- |
| `duplicate_row` (exact resend of a prior row) | 4,102 | sessions, match_events, purchases |
| `null_required_field` (null/empty business key) | 2,166 | sessions, match_events, purchases |
| `out_of_range_value` (negative/absurd numeric) | 2,524 | sessions, match_events, purchases |
| `bad_timestamp` (unparseable date string) | 1,720 | sessions, match_events, purchases |
| `corrupt_json` (truncated JSON line) | 822 | sessions, match_events |
| `malformed_csv_row` (wrong field count / unclosed quote) | 97 | purchases |
| `late_arriving_newer` (revision batch, newer `session_start_ts`) | 500 | sessions_updates |
| `late_arriving_older` (revision batch, older `session_start_ts`) | 500 | sessions_updates |
| **Total injected defect lines** | **12,431** | |

### PII present by design in the landing zone

`player_email`, `player_ip`, `country`, `birth_year` — carried on every `sessions`
row (all 202,732 lines), and masked downstream in the governance layer.

Wall-clock time for full-scale generation: **9.54 s** (generator-internal
`wall_clock_seconds`; end-to-end `make gen-data` = 9.8 s).

---
