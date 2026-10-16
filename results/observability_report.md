# Observability report

Generated 2026-10-16T07:47:36.617061+00:00 by `dlg observe` (run `observe`).

## Lake summary

| metric | value |
| --- | ---: |
| tables | 12 |
| total rows | 1,941,640 |
| total size (MB) | 175.7 |
| total files | 150 |
| snapshot seconds | 23.86 |

## Per-table metrics

| table | layer | domain | class | rows | size (MB) | files | delta v | last commit | key | key null rate | PII cols |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: |
| `bronze.match_events` | bronze | gameplay | INTERNAL | 602,939 | 55.89 | 1 | 2 | 2026-10-16 00:35:43.712000 | event_id | 0.0000 | 0 |
| `bronze.purchases` | bronze | monetization | FINANCIAL | 40,272 | 3.71 | 1 | 2 | 2026-10-16 00:35:48.883000 | purchase_id | 0.0000 | 1 |
| `bronze.quarantine` | bronze | platform | INTERNAL | 3,061 | 0.40 | 3 | 6 | 2026-10-16 00:35:47.091000 | _record_hash | 0.0000 | 1 |
| `bronze.sessions` | bronze | gameplay | INTERNAL | 202,026 | 22.16 | 2 | 4 | 2026-10-16 00:35:51.588000 | player_id | 0.0000 | 4 |
| `gold.dim_player` | gold | gameplay | PII | 25,000 | 0.79 | 1 | 4 | 2026-10-16 00:43:37.302000 | player_id | 0.0000 | 1 |
| `gold.fct_monetization_daily` | gold | monetization | FINANCIAL | 60 | 0.00 | 1 | 4 | 2026-10-16 00:43:43.477000 | event_date | 0.0000 | 0 |
| `gold.fct_player_daily` | gold | gameplay | INTERNAL | 223,966 | 1.53 | 1 | 4 | 2026-10-16 00:43:41.214000 | event_date | 0.0000 | 1 |
| `gold.fct_retention_cohort` | gold | gameplay | INTERNAL | 31 | 0.00 | 1 | 4 | 2026-10-16 00:43:46.011000 | cohort_date | 0.0000 | 0 |
| `silver.match_events` | silver | gameplay | INTERNAL | 600,000 | 59.19 | 36 | 2 | 2026-10-16 00:36:37.550000 | event_id | 0.0000 | 0 |
| `silver.purchases` | silver | monetization | FINANCIAL | 40,000 | 4.34 | 40 | 4 | 2026-10-16 00:37:05.340000 | purchase_id | 0.0000 | 1 |
| `silver.quarantine` | silver | platform | INTERNAL | 4,285 | 0.60 | 3 | 6 | 2026-10-16 00:36:43.605000 | _record_hash | 0.0000 | 1 |
| `silver.sessions` | silver | gameplay | PII | 200,000 | 27.10 | 60 | 2 | 2026-10-16 00:36:17.626000 | session_id | 0.0000 | 4 |

## Notes

* `key null rate` is the NULL rate on the first primary-key column found in the
  catalog for each table (or the partition column when there is no primary key).
* `delta v` is the latest Delta commit version, i.e. how many times the table has
  been written; `last commit` is that commit's timestamp.
* Tables whose catalog classification is `PII` still list PII columns: the masking
  happens in the `secure.*` views, not by deleting data from the lake.
