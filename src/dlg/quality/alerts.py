"""Alerting for data-quality failures and SLA breaches.

:class:`AlertSink` is the extension point -- anything that can ``emit`` an alert.
Two sinks ship in the box, and they are composed:

* :class:`ConsoleSink` -- writes a human-readable line (shows up in CI logs);
* :class:`JSONFileSink` -- appends to ``results/alerts.json`` so the alert stream
  is an artifact that can be diffed or shipped.

Blocking DQ failures and breached SLAs both raise alerts. A WARN-severity failure
raises an alert without gating the build.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from dlg.config import Settings, get_settings
from dlg.quality.runner import QualityRun
from dlg.quality.slas import SlaRun

__all__ = [
    "Alert",
    "AlertSink",
    "ConsoleSink",
    "JSONFileSink",
    "build_alerts",
    "dispatch_alerts",
    "alerts_path",
]

SEVERITY_CRITICAL = "CRITICAL"
SEVERITY_WARNING = "WARNING"


@dataclass
class Alert:
    id: str
    ts: str
    severity: str
    source: str
    table: str
    title: str
    detail: str
    metric: float | None = None
    threshold: float | None = None

    def as_dict(self) -> dict:
        return asdict(self)


class AlertSink(ABC):
    """Anything that can receive alerts."""

    name = "sink"

    @abstractmethod
    def emit(self, alerts: list[Alert]) -> None:  # pragma: no cover - interface
        raise NotImplementedError


class ConsoleSink(AlertSink):
    name = "console"

    def emit(self, alerts: list[Alert]) -> None:
        if not alerts:
            print("[alerts] no alerts")
            return
        for alert in alerts:
            print(
                f"[alert:{alert.severity}] {alert.source} {alert.table}: "
                f"{alert.title} — {alert.detail}"
            )


class JSONFileSink(AlertSink):
    """Writes the alert stream to ``results/alerts.json``.

    Alerts are merged by id rather than overwritten, so a subsequent
    ``sla-check`` run adds to the data-quality alerts already recorded from
    ``quality-run`` instead of erasing them.
    """

    name = "json-file"

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def _load_existing(self) -> dict[str, Alert]:
        if not self.path.exists():
            return {}
        try:
            payload = json.loads(self.path.read_text())
        except (OSError, json.JSONDecodeError):  # pragma: no cover - defensive
            return {}
        out: dict[str, Alert] = {}
        for item in payload.get("alerts", []):
            try:
                out[item["id"]] = Alert(**item)
            except TypeError:  # pragma: no cover - forward compatibility
                continue
        return out

    def emit(self, alerts: list[Alert]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        merged = self._load_existing()
        for alert in alerts:
            merged[alert.id] = alert
        ordered = sorted(merged.values(), key=lambda a: (a.severity, a.table, a.title))
        payload = {
            "generated_at": datetime.now(UTC).isoformat(),
            "count": len(ordered),
            "by_severity": {
                severity: sum(1 for a in ordered if a.severity == severity)
                for severity in sorted({a.severity for a in ordered})
            },
            "alerts": [a.as_dict() for a in ordered],
        }
        self.path.write_text(json.dumps(payload, indent=2))


def alerts_path(settings: Settings | None = None) -> Path:
    settings = settings or get_settings()
    return Path(settings.results_dir) / "alerts.json"


def build_alerts(quality: QualityRun | None = None, sla: SlaRun | None = None) -> list[Alert]:
    """Turn DQ results and SLA results into alerts."""
    now = datetime.now(UTC).isoformat()
    alerts: list[Alert] = []

    if quality is not None:
        for result in quality.failed:
            critical = result.severity == "BLOCKING"
            alerts.append(
                Alert(
                    id=f"dq::{result.table}::{result.expectation}",
                    ts=now,
                    severity=SEVERITY_CRITICAL if critical else SEVERITY_WARNING,
                    source="dq_expectation",
                    table=result.table,
                    title=f"{result.expectation} failed",
                    detail=result.detail,
                    metric=result.fail_rate,
                    threshold=result.threshold,
                )
            )

    if sla is not None:
        for result in sla.breaches:
            alerts.append(
                Alert(
                    id=f"sla::{result.table}::{result.sla_name}",
                    ts=now,
                    severity=SEVERITY_CRITICAL,
                    source="sla",
                    table=result.table,
                    title=f"SLA {result.sla_name} breached",
                    detail=(
                        f"measured {result.measured:g} {result.unit} vs threshold "
                        f"{result.threshold:g} ({result.detail})"
                    ),
                    metric=result.measured,
                    threshold=result.threshold,
                )
            )

    alerts.sort(key=lambda a: (a.severity, a.table, a.title))
    return alerts


def dispatch_alerts(
    alerts: list[Alert],
    sinks: list[AlertSink] | None = None,
    settings: Settings | None = None,
) -> list[Alert]:
    """Emit alerts to every sink (defaults to console + the JSON file)."""
    if sinks is None:
        sinks = [ConsoleSink(), JSONFileSink(alerts_path(settings))]
    for sink in sinks:
        sink.emit(alerts)
    return alerts


def _unused() -> None:  # pragma: no cover
    _ = field
