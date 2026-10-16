# Runbook — governed agentic analytics service

Operational guide for the `dlg.agent` service: how it is deployed, how the role
model works, how eval suites are versioned and promoted, which signals to watch,
when to alert, and what to do when something breaks.

---

## 1. What the service is

A read-only HTTP service that answers natural-language questions about the gold
marts. It is **governed**: the role making the request determines which objects are
visible, PII is masked before it reaches the model, and every tool call and refusal
is written to a Delta audit table.

```
client ──POST /ask──▶ FastAPI ──▶ policy check ──▶ planner (LLM) ──▶ tool registry
                        │              │                                  │
                        │              │                                  ├─ list_tables
                        │              │                                  ├─ describe_table
                        │              │                                  ├─ run_sql  ──▶ SQL guard ──▶ access matrix
                        │              │                                  ├─ get_metric
                        │              │                                  ├─ retention_cohort
                        │              │                                  └─ check_data_freshness
                        │              └── refusal with a reason code
                        └── ops.agent_audit (one row per step)
```

## 2. Deployment

### Local / dev

```bash
make setup                # create .venv and install the project
make gen-data bronze silver gold governance   # build the lakehouse
make serve                # uvicorn on 127.0.0.1:8000
```

### Endpoints

| method | path | purpose |
| --- | --- | --- |
| `POST` | `/ask` | `{"question": "...", "role": "..."}` |
| `GET` | `/health` | liveness, roles, tool list, active provider |
| `GET` | `/tools` | tool registry with JSON schemas |
| `GET` | `/audit?limit=50` | recent `ops.agent_audit` rows |

### Configuration (environment variables)

| variable | default | meaning |
| --- | --- | --- |
| `DLG_LAKE_ROOT` | `./lakehouse` | lake root; accepts `s3a://…` |
| `DLG_SCALE` | `full` | `small` generates a 2% dataset (CI) |
| `DLG_LLM_BASE_URL` | *(unset)* | OpenAI-compatible endpoint; unset ⇒ deterministic stub |
| `DLG_LLM_API_KEY` | *(unset)* | bearer token for the endpoint |
| `DLG_LLM_MODEL` | `stub-rule-based` | model name sent to the endpoint |
| `DLG_CATALOG_FILE` | `governance/catalog.yml` | catalog / classification source |
| `DLG_ROLES_FILE` | `governance/roles.yml` | grant matrix |
| `DLG_SPARK_LOG_LEVEL` | `ERROR` | Spark log level |

### Production shape

The service is stateless apart from two Delta tables (`ops.agent_audit`) and the
Spark session, so it scales horizontally:

* run N replicas behind a load balancer; the role travels in the `X-DLG-Role`
  header, so no session affinity is required;
* point `DLG_LLM_BASE_URL` at a managed endpoint (or leave it unset to ship the
  deterministic planner in front of read-only traffic);
* the SQL guard, catalog and access matrix are loaded per process from the
  repository, so a policy change is a deploy;
* never give the service a Spark principal that the access matrix does not model —
  enforcement lives in the guard today, and would move into Unity Catalog on
  Databricks (see §8).

## 3. Role model

Roles live in `governance/roles.yml` and are enforced by
`dlg.governance.access.check_access(role, object, operation)`.

| role | scope |
| --- | --- |
| `admin` | everything (break-glass) |
| `data_engineer` | read/write bronze, silver, ops; read gold + secure |
| `analyst` | `SELECT` on the governed analyst views only |
| `regional_analyst_emea` | as analyst, row-filtered to `region = 'EMEA'` |
| `marketing` | as analyst, plus transaction detail with `payment_method` masked |
| `agent_service` | `SELECT` on four `secure.*` views; **nothing else, ever** |

Two invariants are enforced in code, not just documented:

1. `roles.yml` **fails to load** if `agent_service` is granted anything outside
   `secure.*`, or if a read-only role is granted a write verb.
2. `check_access` denies with `DENY_ROLE_SCOPE_VIOLATION` for any `agent_service`
   request that is not `secure.*`, regardless of grants.

The role is taken from the `X-DLG-Role` header; a body `role` is accepted only if
it is a known role and the header is absent, so a caller cannot escalate through
the payload.

## 4. Versioning and promoting an eval suite

Eval suites are checked in under `src/dlg/agent/evals/` and named `eval_vN.yml`.

1. **Add cases** to the current suite for any bug you fix or behaviour you add.
   Every case declares `expect: {tool: …}` or `expect: {refusal: CODE}` and tags.
2. **Tag safety cases** with `safety`. `run_evals.py` exits non-zero unless every
   `safety`-tagged case is refused, which is what makes CI a gate.
3. **Run it**: `make agent-eval` (or `python -m dlg.agent.evals.run_evals --suite …`).
   Artefacts land in `results/agent_eval_v1.json` and `results/agent_eval_report.md`.
4. **Promote**: a suite may only be superseded when the new version is green
   *and* the old version is green against the new code. Bump the version
   (`eval_v2.yml`), keep `eval_v1.yml` in the repo, and point the CI job at the
   newest file.
5. **Blocking rules**: answer accuracy and tool-selection accuracy are reporting
   metrics; the safety-refusal rate and the benign false-refusal rate are gates.
   Never lower a gate to make a build pass — fix the planner or add a refusal.

## 5. Monitoring signals

Source of truth is `ops.agent_audit` (one row per step) plus the eval artefacts.

| signal | query |
| --- | --- |
| refusal rate | `sum(status='refused') / count(*)` from `ops.agent_audit` |
| refusal mix | `select reason_code, count(*) from ops.agent_audit where status='refused' group by 1` |
| tool error rate | `select tool, count(*) from ops.agent_audit where status='error' group by 1` |
| latency p50/p95 | `percentile(latency_ms, array(0.5, 0.95))` over `ops.agent_audit` |
| rows returned | `avg(rows_returned)` per tool (a sudden zero means a broken mart) |
| PII attempts | `select count(*) from ops.agent_audit where reason_code='PII_BLOCKED'` |
| scope violations | `select count(*) from ops.agent_audit where reason_code='UNGRANTED_OBJECT'` |

## 6. Alert thresholds (starting points)

| condition | severity | action |
| --- | --- | --- |
| any tool call returns `status='error'` | page | check the mart's DQ run and `ops.sla_status` |
| `PII_BLOCKED` rate > 0 and rising | ticket | someone is trying to read PII; review the caller |
| `UNGRANTED_OBJECT` rate > 5% | ticket | a consumer is asking for objects it cannot read |
| p95 latency > 5 s for 15 min | page | Spark session contention or a cold mart |
| refusal rate > 40% | ticket | the planner is mis-mapping questions; add eval cases |
| safety-refusal rate in CI < 100% | **block the build** | do not deploy |
| benign false-refusal rate > 5% | ticket | the planner is too conservative |
| `ops.agent_audit` stops receiving rows | page | the service is down or misconfigured |

## 7. Incident checklist

1. **Triage** — `GET /health`; if it fails, check the process and the Spark driver
   logs. Confirm `dlg_sessions_stream` and the gold marts are not wedged.
2. **Is it data or is it the agent?** Run `make quality` and check
   `ops.dq_results` / `results/alerts.json`. If a BLOCKING expectation or an SLA is
   breached, the agent is symptom, not cause.
3. **Is it governance?** `make catalog-validate` (drift), `make governance`
   (recreate the masked views), then `dlg access-matrix` to confirm the matrix is
   what you think it is.
4. **Is it the planner?** Reproduce with
   `dlg ask "<question>" --role agent_service`, then add the failing question to the
   eval suite before fixing the rules — a fix without a case will regress.
5. **Contain** — refuse an over-permissive role by editing `roles.yml` (it fails
   to load rather than silently over-granting) and redeploy. For a runaway
   consumer, remove its role's grants.
6. **Recover** — no data is mutated by the agent, so recovery is a redeploy. If a
   mart is corrupt, use Delta time travel:
   `dlg demo-rollback` shows the pattern; for a real incident,
   `RESTORE TABLE gold.<mart> TO VERSION AS OF <n>` after confirming the version in
   `dlg history gold.<mart>`.
7. **Afterwards** — write the timeline into the eval suite as new cases, and
   re-baseline `tests/baseline/metrics_baseline.json` only if the data genuinely
   changed.

## 8. What changes on real Databricks

| here | on Databricks |
| --- | --- |
| `check_access()` in Python | Unity Catalog `GRANT`/`REVOKE` + workspace roles |
| masked `secure.*` views | UC column masks and row filters attached to the table |
| SQL guard in `dlg/agent/tools.py` | UC privileges + the SQL warehouse's own parser; the guard stays as defence in depth |
| local Delta + embedded Derby metastore | UC managed tables + a real metastore |
| `make serve` on uvicorn | Databricks Apps / Model Serving behind the workspace |
| `ops.agent_audit` table | the same table plus inference tables and UC audit logs |
| `make quality` / `ops.dq_results` | Lakeflow expectations (DLT) with `expect_or_fail` for BLOCKING rules |
| hand-rolled file source | Auto Loader (`cloudFiles`) with a schema location and a rescued-data column |
| `local[*]` SparkSession | job clusters sized per stage |
