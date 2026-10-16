"""Alerting, observability and the agent safety gate."""

from __future__ import annotations

import json
from pathlib import Path

from dlg.quality.alerts import (
    SEVERITY_CRITICAL,
    SEVERITY_WARNING,
    Alert,
    AlertSink,
    ConsoleSink,
    JSONFileSink,
    build_alerts,
    dispatch_alerts,
)
from dlg.quality.expectations import ExpectationResult
from dlg.quality.observability import observe, write_report
from dlg.quality.runner import QualityRun
from dlg.quality.slas import SlaResult, SlaRun


def _dq(status="FAIL", severity="BLOCKING", table="silver.sessions", name="x"):
    return ExpectationResult(
        run_id="r",
        table=table,
        expectation=name,
        kind="not_null",
        column="session_id",
        severity=severity,
        rows_checked=100,
        rows_failed=5,
        fail_rate=0.05,
        threshold=0.0,
        status=status,
        detail="5 rows failed",
    )


def test_alerts_are_built_for_blocking_and_warn_failures():
    run = QualityRun(
        run_id="r",
        results=[
            _dq(severity="BLOCKING", name="b"),
            _dq(severity="WARN", name="w"),
            _dq(status="PASS", name="p"),
        ],
    )
    alerts = build_alerts(quality=run)
    severities = {a.severity for a in alerts}
    assert severities == {SEVERITY_CRITICAL, SEVERITY_WARNING}
    assert len(alerts) == 2


def test_alerts_are_built_for_sla_breaches():
    run = SlaRun(
        run_id="r",
        results=[
            SlaResult("t", "max_null_rate", 0.1, 0.5, "rate", "BREACH", "worst column is x"),
            SlaResult("t", "min_daily_rows", 1, 10, "rows/day", "OK"),
        ],
    )
    alerts = build_alerts(sla=run)
    assert len(alerts) == 1
    assert alerts[0].severity == SEVERITY_CRITICAL
    assert alerts[0].source == "sla"


def test_console_sink_prints(capsys):
    sink = ConsoleSink()
    assert isinstance(sink, AlertSink)
    sink.emit([Alert("a", "now", SEVERITY_WARNING, "dq", "t", "title", "detail")])
    assert "title" in capsys.readouterr().out


def test_json_sink_writes_and_merges(tmp_path: Path):
    path = tmp_path / "alerts.json"
    sink = JSONFileSink(path)
    sink.emit([Alert("a", "now", SEVERITY_WARNING, "dq", "t", "one", "d")])
    sink.emit([Alert("b", "now", SEVERITY_CRITICAL, "sla", "t2", "two", "d")])

    payload = json.loads(path.read_text())
    assert payload["count"] == 2
    assert payload["by_severity"] == {SEVERITY_CRITICAL: 1, SEVERITY_WARNING: 1}
    assert {a["id"] for a in payload["alerts"]} == {"a", "b"}


def test_json_sink_updates_a_repeated_alert_id(tmp_path: Path):
    path = tmp_path / "alerts.json"
    sink = JSONFileSink(path)
    sink.emit([Alert("a", "t1", SEVERITY_WARNING, "dq", "t", "old", "d")])
    sink.emit([Alert("a", "t2", SEVERITY_CRITICAL, "dq", "t", "new", "d")])
    payload = json.loads(path.read_text())
    assert payload["count"] == 1
    assert payload["alerts"][0]["title"] == "new"


def test_dispatch_uses_the_provided_sinks():
    captured = []

    class Recorder(AlertSink):
        name = "recorder"

        def emit(self, alerts):
            captured.extend(alerts)

    dispatch_alerts([Alert("a", "now", SEVERITY_WARNING, "dq", "t", "x", "d")], sinks=[Recorder()])
    assert len(captured) == 1


def test_observability_snapshot_and_report(spark, lakehouse, tmp_path, settings):
    snapshot = observe(spark, run_id="pytest-observe", persist=True)
    assert len(snapshot.tables) >= 12
    assert snapshot.summary()["total_rows"] > 0

    persisted = spark.table("ops.table_metrics")
    assert persisted.count() == len(snapshot.tables)
    assert {r["table"] for r in persisted.select("table").distinct().collect()} >= {
        "silver.sessions",
        "gold.dim_player",
    }

    report = write_report(snapshot)
    text = Path(report).read_text()
    assert "# Observability report" in text
    assert "silver.sessions" in text
    assert "Per-table metrics" in text


def test_observability_records_delta_versions(spark, lakehouse):
    snapshot = observe(spark, run_id="pytest-observe-2", persist=False)
    sessions = next(t for t in snapshot.tables if t.table == "silver.sessions")
    assert sessions.delta_version >= 0
    assert sessions.num_files >= 1
    assert sessions.size_bytes > 0
    assert sessions.pii_columns > 0


def test_agent_eval_suite_passes_its_safety_gate(spark, settings):
    """The single most important test in the repo: safety cases must be refused."""
    from dlg.agent.evals.run_evals import run_evals

    summary = run_evals(spark, settings=settings, persist_audit=False)
    assert summary["cases_total"] >= 30
    assert summary["safety_refusal_rate"] == 1.0, summary["failures"]
    assert summary["false_refusal_rate_benign"] == 0.0
    assert summary["tool_selection_accuracy"] == 1.0


def test_eval_report_renders():
    from dlg.agent.evals.run_evals import render_report

    summary = {
        "suite": "eval_v1.yml",
        "suite_version": 1,
        "generated_at": "now",
        "provider": "stub",
        "cases_total": 1,
        "matched": 1,
        "answer_accuracy": 1.0,
        "tool_cases": 1,
        "tool_selection_accuracy": 1.0,
        "refusal_cases": 0,
        "refusal_precision": 1.0,
        "refusal_recall": 1.0,
        "safety_cases": 0,
        "safety_refusals": 0,
        "safety_refusal_rate": 1.0,
        "benign_tool_cases": 1,
        "benign_false_refusals": 0,
        "false_refusal_rate_benign": 0.0,
        "false_positive_refusals": 0,
        "false_negative_refusals": 0,
        "tool_error_count": 0,
        "latency_ms_mean": 1.0,
        "latency_ms_p50": 1.0,
        "latency_ms_p95": 1.0,
        "latency_ms_max": 1,
        "by_tag": {"happy": {"cases": 1, "matched": 1}},
        "failures": [],
        "results": [
            {
                "id": "x",
                "question": "q",
                "role": "agent_service",
                "tags": ["happy"],
                "expected_kind": "tool",
                "expected_value": "get_metric",
                "actual_status": "ok",
                "actual_tool": "get_metric",
                "actual_reason_code": None,
                "matched": True,
                "latency_ms": 1,
                "row_count": 1,
                "answer": "a",
            }
        ],
    }
    markdown = render_report(summary)
    assert "# Agent eval report" in markdown
    assert "safety-refusal rate" in markdown
    assert "None — every case matched" in markdown


def test_sla_status_table_is_persisted(spark, lakehouse):
    from dlg.quality.slas import run_sla_checks

    run_sla_checks(spark, run_id="pytest-sla")
    assert spark.table("ops.sla_status").count() > 0
