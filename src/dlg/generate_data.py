"""Deterministic synthetic game-telemetry generator.

Writes landing files for three datasets (sessions, match events, store
purchases) plus a separate late-arriving revision batch, and deliberately
injects a catalogue of realistic data defects.  Every injected defect is
counted and printed so the numbers in ``results/RESULTS.md`` are measured, not
guessed.

Determinism: a single ``random.Random(seed)`` instance drives the whole run, so
``seed=42`` always yields byte-identical output.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import shutil
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

from dlg.config import Settings, get_settings

# --------------------------------------------------------------------------
# Domain constants
# --------------------------------------------------------------------------

DATA_START = datetime(2025, 9, 1, 0, 0, 0, tzinfo=UTC)

COUNTRIES: dict[str, tuple[str, int]] = {
    # country -> (region, weight)
    "US": ("NA", 30),
    "CA": ("NA", 5),
    "MX": ("NA", 4),
    "BR": ("LATAM", 8),
    "AR": ("LATAM", 3),
    "CL": ("LATAM", 2),
    "DE": ("EMEA", 9),
    "FR": ("EMEA", 7),
    "GB": ("EMEA", 8),
    "ES": ("EMEA", 4),
    "IT": ("EMEA", 4),
    "PL": ("EMEA", 3),
    "SE": ("EMEA", 2),
    "JP": ("APAC", 6),
    "KR": ("APAC", 5),
    "SG": ("APAC", 2),
    "AU": ("APAC", 4),
    "IN": ("APAC", 7),
    "PH": ("APAC", 2),
}

DEVICES = ["ios", "android", "pc", "console", "mac"]
PLATFORMS = ["mobile", "pc", "console"]
APP_VERSIONS = ["1.4.0", "1.4.1", "1.4.2", "1.5.0-beta"]
MAPS = ["nimbus", "scrapyard", "orbital", "sandstorm", "frostbite", "metro"]
MODES = ["ranked", "casual", "duel", "brawl"]
EVENT_TYPES = ["kill", "death", "objective", "match_end"]
PAYMENT_METHODS = ["card", "paypal", "store_credit", "gift_card"]
CATALOG = [
    ("BATTLEPASS_S15", "Season 15 Battle Pass", "battle_pass", 9.99),
    ("SKIN_EMBER", "Ember Warden Skin", "skin", 19.99),
    ("SKIN_FROST", "Frost Sentinel Skin", "skin", 14.99),
    ("CURRENCY_500", "500 Shards", "currency", 4.99),
    ("CURRENCY_2000", "2000 Shards", "currency", 19.99),
    ("CURRENCY_10000", "10000 Shards", "currency", 99.99),
    ("BUNDLE_STARTER", "Starter Bundle", "bundle", 4.99),
    ("BUNDLE_MEGA", "Mega Vault Bundle", "bundle", 49.99),
    ("EMOTE_DANCE", "Victory Dance Emote", "emote", 0.99),
]

FIRST_NAMES = [
    "ava",
    "liam",
    "noah",
    "mia",
    "zoe",
    "kai",
    "luca",
    "ines",
    "omar",
    "yuki",
    "sana",
    "diego",
    "elena",
    "tariq",
    "mei",
    "jonas",
    "priya",
    "ivan",
    "nora",
    "sven",
]
LAST_NAMES = [
    "okafor",
    "nguyen",
    "silva",
    "kim",
    "rossi",
    "haddad",
    "novak",
    "tanaka",
    "moreau",
    "kumar",
    "ferreira",
    "kowalski",
    "lindqvist",
    "abadi",
    "costa",
    "petrov",
    "diaz",
    "schmidt",
    "ahmed",
    "yamamoto",
]
EMAIL_DOMAINS = ["example.com", "mail.example.net", "players.example.org"]

# --------------------------------------------------------------------------
# Engagement model (drives the retention curve)
# --------------------------------------------------------------------------

#: Share of players that stick around for weeks rather than days.
CORE_FRACTION = 0.35
#: Daily survival probability for a core player (~29% still active at D30).
CORE_SURVIVAL = 0.96
#: Daily survival probability for a casual player (churns within a day or two).
CASUAL_SURVIVAL = 0.20


# --------------------------------------------------------------------------
# Defect accounting
# --------------------------------------------------------------------------


@dataclass
class DefectLog:
    """Counts of every defect class injected into the landing zone."""

    invalid_rows: Counter = field(default_factory=Counter)
    total_lines: Counter = field(default_factory=Counter)
    base_rows: Counter = field(default_factory=Counter)

    def defect(self, dataset: str, kind: str, n: int = 1) -> None:
        self.invalid_rows[kind] += n
        # keep the dataset association for the report
        self.invalid_rows[f"{dataset}::{kind}"] += n

    def emit(self, dataset: str, n: int = 1) -> None:
        self.total_lines[dataset] += n

    def as_dict(self) -> dict[str, object]:
        return {
            "base_rows": dict(self.base_rows),
            "total_lines": dict(self.total_lines),
            "injected_defects": {
                k: v for k, v in sorted(self.invalid_rows.items()) if "::" not in k
            },
            "injected_defects_by_dataset": {
                k: v for k, v in sorted(self.invalid_rows.items()) if "::" in k
            },
        }


# --------------------------------------------------------------------------
# Shard writer
# --------------------------------------------------------------------------


class ShardWriter:
    """Writes JSON-lines or CSV shards of roughly equal size."""

    def __init__(
        self,
        directory: Path,
        prefix: str,
        files: int,
        rows: int,
        fmt: str,
        csv_header: list[str] | None = None,
    ) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        self.directory = directory
        self.prefix = prefix
        self.files = files
        self.rows_per_file = max(1, math.ceil(rows / files))
        self.fmt = fmt
        self.index = 0
        self.count_in_file = 0
        self._fh = None
        self._csv = None
        self.csv_header = csv_header
        self._open_next()

    def _open_next(self) -> None:
        if self._fh is not None:
            self._fh.close()
        self.index += 1
        ext = "json" if self.fmt == "json" else "csv"
        path = self.directory / f"{self.prefix}-part-{self.index - 1:04d}.{ext}"
        self._fh = path.open("w", encoding="utf-8")
        self._csv = None
        if self.fmt == "csv" and self.csv_header:
            self._csv = csv.writer(self._fh)
            self._csv.writerow(self.csv_header)
        self.count_in_file = 0

    def write(self, record: dict) -> None:
        if self.count_in_file >= self.rows_per_file:
            self._open_next()
        if self.fmt == "json":
            self._fh.write(json.dumps(record, separators=(",", ":"), default=str) + "\n")
        else:
            self._csv.writerow([_csv_cell(record.get(col)) for col in self.csv_header])
        self.count_in_file += 1

    def write_raw(self, line: str) -> None:
        """Write a literal line (used for corrupt/truncated records)."""
        self._fh.write(line + "\n")
        self.count_in_file += 1

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    @property
    def files_written(self) -> int:
        return self.index


def _csv_cell(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


# --------------------------------------------------------------------------
# Generator
# --------------------------------------------------------------------------


def _iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _weighted_choice(rng: random.Random, mapping: dict[str, tuple[str, int]]) -> str:
    keys = list(mapping)
    weights = [mapping[k][1] for k in keys]
    return rng.choices(keys, weights=weights, k=1)[0]


class Generator:
    """Builds the whole landing zone from a single deterministic RNG."""

    def __init__(self, settings: Settings, rng: random.Random | None = None) -> None:
        self.settings = settings
        self.rng = rng or random.Random(settings.seed)
        self.log = DefectLog()
        self.landing = Path(settings.landing)
        self.players: list[dict] = []
        #: A sample of rows actually written to landing/sessions, used so that the
        #: late-arriving revision batch genuinely targets existing session_ids.
        self.session_pool: list[dict] = []

    # ---- players -------------------------------------------------------
    def build_players(self) -> None:
        n = self.settings.scaled(self.settings.n_players)
        for i in range(n):
            pid = f"P{i + 1:06d}"
            country = _weighted_choice(self.rng, COUNTRIES)
            region = COUNTRIES[country][0]
            first = self.rng.choice(FIRST_NAMES)
            last = self.rng.choice(LAST_NAMES)
            email = f"{first}.{last}{i + 1}@{self.rng.choice(EMAIL_DOMAINS)}"
            ip = ".".join(str(self.rng.randint(1, 254)) for _ in range(4))
            birth_year = self.rng.randint(1985, 2012)
            # install cohorts span days 0..n_days-1; activity is generated over the
            # full observation window so retention cohorts actually mature
            install_offset = self.rng.randint(0, max(0, self.settings.n_days - 1))
            self.players.append(
                {
                    "player_id": pid,
                    "player_email": email,
                    "player_ip": ip,
                    "country": country,
                    "region": region,
                    "birth_year": birth_year,
                    "install_day": install_offset,
                }
            )

    # ---- sessions ------------------------------------------------------
    def _session_row(self, player: dict, day_offset: int, seq: int) -> dict:
        rng = self.rng
        start = DATA_START + timedelta(
            days=day_offset, seconds=rng.randint(0, 86_399), milliseconds=rng.randint(0, 999)
        )
        duration = max(1, int(rng.gauss(38, 24)))
        end = start + timedelta(minutes=duration)
        device = rng.choice(DEVICES)
        platform = {
            "ios": "mobile",
            "android": "mobile",
            "pc": "pc",
            "mac": "pc",
            "console": "console",
        }[device]
        return {
            "player_id": player["player_id"],
            "session_id": f"S{player['player_id'][1:]}{day_offset:02d}{seq:03d}",
            "player_email": player["player_email"],
            "player_ip": player["player_ip"],
            "country": player["country"],
            "region": player["region"],
            "birth_year": player["birth_year"],
            "device": device,
            "platform": platform,
            "app_version": rng.choice(APP_VERSIONS),
            "session_start_ts": _iso(start),
            "session_end_ts": _iso(end),
            "duration_minutes": duration,
            "matches_played": rng.randint(0, 9),
            "kills": max(0, int(rng.gauss(14, 9))),
            "deaths": max(0, int(rng.gauss(12, 7))),
            "store_views": rng.randint(0, 6),
        }

    def _player_day_plan(self, n_sessions: int) -> list[tuple[dict, int]]:
        """Deterministic list of ``(player, day_offset)`` pairs, one per session.

        Engagement is modelled with a two-tier survival curve, which is what makes
        the retention numbers behave like a real live-service game instead of a
        flat geometric decay:

        * a **core** share of players stick around (``P(active on day n)`` ≈
          ``CORE_SURVIVAL ** n``), producing a long tail that is still active at
          D30;
        * the rest are **casual** and churn within a day or two.

        Two invariants matter downstream:

        * **full player coverage** -- every generated player keeps a session on
          their install day, so ``cohort_size`` equals the player count and
          ``silver.purchases`` -> ``silver.sessions`` referential integrity is a
          meaningful check;
        * **unique ``(player_id, session_id)`` pairs** -- so the only duplicate
          business keys downstream are the injected ones.

        Candidate days are generated first and then deterministically trimmed to
        exactly ``n_sessions``, keeping every install-day anchor.
        """
        rng = self.rng
        max_day = self.settings.total_days - 1
        anchors: list[tuple[dict, int]] = []
        extras: list[tuple[dict, int]] = []

        for player in self.players:
            install = min(player["install_day"], max_day)
            anchors.append((player, install))
            survival = CORE_SURVIVAL if rng.random() < CORE_FRACTION else CASUAL_SURVIVAL
            p = survival
            day = install + 1
            while day <= max_day:
                if rng.random() < p:
                    extras.append((player, day))
                p *= survival
                day += 1

        rng.shuffle(extras)
        n_extra = max(0, n_sessions - len(anchors))
        if len(extras) >= n_extra:
            plan = anchors + extras[:n_extra]
        else:
            # Small-scale runs can generate fewer candidate days than requested;
            # top up deterministically so the requested row count is still met.
            plan = anchors + extras
            seen = {(p["player_id"], d) for p, d in plan}
            while len(plan) < n_sessions:
                player = rng.choice(self.players)
                install = min(player["install_day"], max_day)
                day = rng.randint(install, max_day)
                if (player["player_id"], day) in seen:
                    continue
                seen.add((player["player_id"], day))
                plan.append((player, day))
        rng.shuffle(plan)
        return plan[:n_sessions]

    def generate_sessions(self) -> None:
        n = self.settings.scaled(self.settings.n_sessions)
        files = self.settings.files_per_dataset
        writer = ShardWriter(self.landing / "sessions", "sessions", files, n, "json")

        plan = self._player_day_plan(n)
        per_player_day: Counter = Counter()
        base_rows = 0
        for player, day in plan:
            key = (player["player_id"], day)
            per_player_day[key] += 1
            row = self._session_row(player, day, 1)
            writer.write(row)
            base_rows += 1
            self.log.emit("sessions")
            if len(self.session_pool) < 5000:
                self.session_pool.append(dict(row))

            # ---- injections -------------------------------------------
            r = self.rng.random()
            if r < 0.005:  # duplicate row (exact resend)
                writer.write(dict(row))
                self.log.emit("sessions")
                self.log.defect("sessions", "duplicate_row")
            elif r < 0.0075:  # null required key
                bad = dict(row)
                bad["player_id"] = None if self.rng.random() < 0.5 else ""
                writer.write(bad)
                self.log.emit("sessions")
                self.log.defect("sessions", "null_required_field")
            elif r < 0.0095:  # unparseable timestamp
                bad = dict(row)
                bad["session_start_ts"] = self.rng.choice(
                    ["not-a-timestamp", "2025-13-45T99:99:99Z", "", "0000-00-00T00:00:00Z"]
                )
                writer.write(bad)
                self.log.emit("sessions")
                self.log.defect("sessions", "bad_timestamp")
            elif r < 0.0125:  # out-of-range value
                bad = dict(row)
                field_name = self.rng.choice(["duration_minutes", "kills", "birth_year"])
                bad[field_name] = {
                    "duration_minutes": self.rng.choice([-5, 1441, 99999]),
                    "kills": self.rng.choice([800, 5000]),
                    "birth_year": self.rng.choice([1810, 2029]),
                }[field_name]
                writer.write(bad)
                self.log.emit("sessions")
                self.log.defect("sessions", "out_of_range_value")
            elif r < 0.0135:  # truncated JSON line (unparseable)
                line = json.dumps(row, separators=(",", ":"))
                writer.write_raw(line[: self.rng.randint(20, max(21, len(line) - 5))])
                self.log.emit("sessions")
                self.log.defect("sessions", "corrupt_json")

        writer.close()
        self.log.base_rows["sessions"] = base_rows
        print(
            f"[sessions] base_rows={base_rows} total_lines={self.log.total_lines['sessions']} "
            f"files={writer.files_written} -> {writer.directory}"
        )

    # ---- late-arriving revisions ---------------------------------------
    def generate_session_revisions(self) -> None:
        """A second batch that revises already-loaded sessions.

        Half the revisions carry a *newer* ``session_start_ts`` and must win on
        MERGE; half carry an *older* timestamp and must be ignored.  Targets are
        drawn from :attr:`session_pool` so every revision references an
        ``session_id`` that really exists in ``landing/sessions``.
        """
        n_newer = self.settings.scaled(500)
        n_older = self.settings.scaled(500)
        rng = self.rng
        out = self.landing / "sessions_updates"
        writer = ShardWriter(out, "sessions_updates", 1, n_newer + n_older, "json")

        pool = self.session_pool or []
        if not pool:  # pragma: no cover - defensive
            raise RuntimeError("session_pool is empty; generate_sessions must run first")
        rng.shuffle(pool)

        for idx in range(n_newer + n_older):
            base = dict(pool[idx % len(pool)])
            shift = timedelta(hours=1) if idx < n_newer else timedelta(hours=-1)
            start = datetime.strptime(base["session_start_ts"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(
                tzinfo=UTC
            )
            end = datetime.strptime(base["session_end_ts"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(
                tzinfo=UTC
            )
            base["session_start_ts"] = _iso(start + shift)
            base["session_end_ts"] = _iso(end + shift)
            base["duration_minutes"] = base["duration_minutes"] + (5 if idx < n_newer else -5)
            writer.write(base)
            self.log.emit("sessions_updates")
            self.log.defect(
                "sessions_updates",
                "late_arriving_newer" if idx < n_newer else "late_arriving_older",
            )
        writer.close()
        print(
            f"[sessions_updates] newer={n_newer} older={n_older} "
            f"total_lines={self.log.total_lines['sessions_updates']} -> {out}"
        )

    # ---- match events --------------------------------------------------
    def generate_match_events(self) -> None:
        n = self.settings.scaled(self.settings.n_match_events)
        files = self.settings.files_per_dataset
        writer = ShardWriter(self.landing / "match_events", "match_events", files, n, "json")
        rng = self.rng
        base_rows = 0
        for i in range(n):
            player = rng.choice(self.players)
            day = min(
                self.settings.total_days - 1,
                max(0, player["install_day"] + rng.randint(0, 6)),
            )
            ts = DATA_START + timedelta(days=day, seconds=rng.randint(0, 86_399))
            etype = rng.choices(EVENT_TYPES, weights=[45, 45, 5, 5], k=1)[0]
            row = {
                "event_id": f"E{i + 1:08d}",
                "session_id": f"S{player['player_id'][1:]}{day:02d}{rng.randint(1, 3):03d}",
                "player_id": player["player_id"],
                "match_id": f"M{rng.randint(1, 40000):07d}",
                "event_type": etype,
                "event_ts": _iso(ts),
                "map_id": rng.choice(MAPS),
                "mode": rng.choice(MODES),
                "kills": 1 if etype == "kill" else 0,
                "deaths": 1 if etype == "death" else 0,
                "damage_dealt": rng.randint(0, 450),
                "ping_ms": max(5, int(rng.gauss(46, 28))),
                "is_bot": rng.random() < 0.06,
            }
            writer.write(row)
            base_rows += 1
            self.log.emit("match_events")

            r = rng.random()
            if r < 0.005:
                writer.write(dict(row))
                self.log.emit("match_events")
                self.log.defect("match_events", "duplicate_row")
            elif r < 0.0075:
                bad = dict(row)
                bad["event_id"] = None if rng.random() < 0.5 else ""
                writer.write(bad)
                self.log.emit("match_events")
                self.log.defect("match_events", "null_required_field")
            elif r < 0.0095:
                bad = dict(row)
                bad["event_ts"] = rng.choice(["not-a-timestamp", "2025-02-30T25:00:00Z", ""])
                writer.write(bad)
                self.log.emit("match_events")
                self.log.defect("match_events", "bad_timestamp")
            elif r < 0.0125:
                bad = dict(row)
                bad["ping_ms"] = rng.choice([-1, 9000])
                writer.write(bad)
                self.log.emit("match_events")
                self.log.defect("match_events", "out_of_range_value")
            elif r < 0.0135:
                line = json.dumps(row, separators=(",", ":"))
                writer.write_raw(line[: rng.randint(20, max(21, len(line) - 5))])
                self.log.emit("match_events")
                self.log.defect("match_events", "corrupt_json")

        writer.close()
        self.log.base_rows["match_events"] = base_rows
        print(
            f"[match_events] base_rows={base_rows} total_lines={self.log.total_lines['match_events']} "
            f"files={writer.files_written} -> {writer.directory}"
        )

    # ---- purchases -----------------------------------------------------
    def generate_purchases(self) -> None:
        n = self.settings.scaled(self.settings.n_purchases)
        files = self.settings.files_per_dataset
        header = [
            "purchase_id",
            "session_id",
            "player_id",
            "sku",
            "item_name",
            "category",
            "price_usd",
            "currency",
            "quantity",
            "purchase_ts",
            "payment_method",
        ]
        writer = ShardWriter(self.landing / "purchases", "purchases", files, n, "csv", header)
        rng = self.rng
        base_rows = 0
        for i in range(n):
            player = rng.choice(self.players)
            day = min(
                self.settings.total_days - 1,
                max(0, player["install_day"] + rng.randint(0, 10)),
            )
            ts = DATA_START + timedelta(days=day, seconds=rng.randint(0, 86_399))
            sku, item, category, price = rng.choice(CATALOG)
            row = {
                "purchase_id": f"U{i + 1:08d}",
                "session_id": f"S{player['player_id'][1:]}{day:02d}{rng.randint(1, 3):03d}",
                "player_id": player["player_id"],
                "sku": sku,
                "item_name": item,
                "category": category,
                "price_usd": price,
                "currency": "USD",
                "quantity": rng.choices([1, 2, 3], weights=[85, 12, 3], k=1)[0],
                "purchase_ts": _iso(ts),
                "payment_method": rng.choice(PAYMENT_METHODS),
            }
            writer.write(row)
            base_rows += 1
            self.log.emit("purchases")

            r = rng.random()
            if r < 0.005:
                writer.write(dict(row))
                self.log.emit("purchases")
                self.log.defect("purchases", "duplicate_row")
            elif r < 0.009:
                bad = dict(row)
                bad["player_id"] = None if rng.random() < 0.5 else ""
                writer.write(bad)
                self.log.emit("purchases")
                self.log.defect("purchases", "null_required_field")
            elif r < 0.012:
                bad = dict(row)
                bad["purchase_ts"] = rng.choice(["not-a-timestamp", "31/02/2025 25:61", ""])
                writer.write(bad)
                self.log.emit("purchases")
                self.log.defect("purchases", "bad_timestamp")
            elif r < 0.016:
                bad = dict(row)
                bad["price_usd"] = rng.choice([-9.99, 0.0, 2500.0])
                writer.write(bad)
                self.log.emit("purchases")
                self.log.defect("purchases", "out_of_range_value")
            elif r < 0.0185:
                # malformed CSV row: wrong field count -> Spark _corrupt_record
                raw = ",".join(_csv_cell(row.get(c)) for c in header[: rng.randint(3, 7)])
                writer.write_raw(raw)
                self.log.emit("purchases")
                self.log.defect("purchases", "malformed_csv_row")

        writer.close()
        self.log.base_rows["purchases"] = base_rows
        print(
            f"[purchases] base_rows={base_rows} total_lines={self.log.total_lines['purchases']} "
            f"files={writer.files_written} -> {writer.directory}"
        )

    # ---- orchestration -------------------------------------------------
    def run(self, clean: bool = True) -> dict[str, object]:
        start = time.perf_counter()
        if clean:
            for name in ("sessions", "sessions_updates", "match_events", "purchases"):
                target = self.landing / name
                if target.exists():
                    shutil.rmtree(target)
        self.build_players()
        self.generate_sessions()
        self.generate_session_revisions()
        self.generate_match_events()
        self.generate_purchases()
        elapsed = time.perf_counter() - start

        summary = self.log.as_dict()
        summary["players"] = len(self.players)
        summary["install_cohort_days"] = self.settings.n_days
        summary["observation_days"] = self.settings.total_days
        summary["seed"] = self.settings.seed
        summary["scale"] = self.settings.scale
        summary["wall_clock_seconds"] = round(elapsed, 2)
        summary["landing_root"] = str(self.landing)
        summary["pii_columns"] = ["player_email", "player_ip", "country", "birth_year"]
        return summary


def format_summary(summary: dict) -> str:
    """Human-readable defect/row-count report printed to stdout."""
    lines = []
    lines.append("=" * 78)
    lines.append("LANDING ZONE — GENERATED ROW COUNTS AND INJECTED DEFECTS")
    lines.append("=" * 78)
    lines.append(
        f"seed={summary['seed']} scale={summary['scale']} "
        f"install_cohort_days={summary['install_cohort_days']} "
        f"observation_days={summary['observation_days']} players={summary['players']:,}"
    )
    lines.append(f"landing_root={summary['landing_root']}")
    lines.append(f"wall_clock_seconds={summary['wall_clock_seconds']}")
    lines.append("")
    lines.append(f"{'dataset':<20}{'base_rows':>14}{'total_lines':>14}{'injected':>12}")
    lines.append("-" * 78)
    defects_by_ds = summary["injected_defects_by_dataset"]
    total_base = total_lines = total_defects = 0
    for dataset in ("sessions", "sessions_updates", "match_events", "purchases"):
        base = summary["base_rows"].get(dataset, 0)
        lines_written = summary["total_lines"].get(dataset, 0)
        injected = sum(v for k, v in defects_by_ds.items() if k.startswith(f"{dataset}::"))
        total_base += base
        total_lines += lines_written
        total_defects += injected
        lines.append(f"{dataset:<20}{base:>14,}{lines_written:>14,}{injected:>12,}")
    lines.append("-" * 78)
    lines.append(f"{'TOTAL':<20}{total_base:>14,}{total_lines:>14,}{total_defects:>12,}")
    lines.append("")
    lines.append("Injected defects by class:")
    for kind, count in summary["injected_defects"].items():
        lines.append(f"  {kind:<26}{count:>10,}")
    lines.append("")
    lines.append("PII columns present in landing data: " + ", ".join(summary["pii_columns"]))
    lines.append("=" * 78)
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> dict:
    parser = argparse.ArgumentParser(description="Generate synthetic game telemetry landing files")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--scale", default=None, choices=["full", "small"])
    parser.add_argument("--no-clean", action="store_true", help="keep existing files")
    parser.add_argument("--json-out", default=None, help="also write the summary as JSON")
    args = parser.parse_args(argv)

    overrides: dict[str, object] = {}
    if args.seed is not None:
        overrides["seed"] = args.seed
    if args.scale is not None:
        overrides["scale"] = args.scale
    settings = Settings(**overrides) if overrides else get_settings()
    settings.ensure_dirs()

    generator = Generator(settings)
    summary = generator.run(clean=not args.no_clean)
    print(format_summary(summary))

    if args.json_out:
        Path(args.json_out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.json_out).write_text(json.dumps(summary, indent=2, default=str))
    return summary


if __name__ == "__main__":  # pragma: no cover
    main()
