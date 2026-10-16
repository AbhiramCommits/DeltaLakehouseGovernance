#!/usr/bin/env bash
# =============================================================================
# Structured Streaming ingest — end-to-end idempotency demonstration.
#
# Flow:
#   1. seed the streaming source directory with 3 landing shards, run --once
#   2. drop 2 NEW micro-batch files plus a byte-copy of an already-processed file,
#      run --once again
#        (a) new rows appear
#        (b) the duplicate file added zero rows
#   3. drop ONLY a duplicate of an already-processed file, run --once
#        (c) a duplicate batch adds zero rows in isolation
#   4. re-run --once with no new files
#        (d) the file-source checkpoint means nothing is reprocessed
#   5. --replay-last-batch re-delivers the last batch to the same handler
#        (e) the content-addressed _batch_id guard makes it a no-op
#
# Every number printed below is read back from the live Delta tables and the
# ops.stream_batches ledger — nothing is hard-coded.
# =============================================================================
set -euo pipefail

cd "$(dirname "$0")/.."

# The demo works against an isolated landing directory so it fully controls which
# files exist; `make stream` tails the main landing/sessions zone instead.
export DLG_STREAM_SUBDIR="sessions_stream"

PY=".venv/bin/python"
SRC="landing/sessions"
STREAM_SRC="landing/sessions_stream"
CHECKPOINTS="_checkpoints/sessions_stream"

# Emits exactly one integer on stdout. Spark/Ivy write progress chatter to
# stdout, so the value is tagged and filtered out of the noise.
count_table() {
  $PY - "$1" - <<'EOF' 2>/dev/null | sed -n 's/^ROWS=//p'
import sys
import dlg
from dlg.spark import get_spark
s = get_spark("stream-demo")
print(f"ROWS={s.table(sys.argv[1]).count()}")
EOF
}

count_silver() { count_table silver.sessions_stream; }
count_bronze() { count_table bronze.sessions_stream; }
count_quarantine() { count_table bronze.quarantine; }

echo "=============================================================="
echo "STEP 0 — reset the streaming state"
echo "=============================================================="
rm -rf "$STREAM_SRC" "$CHECKPOINTS"
mkdir -p "$STREAM_SRC"
# NOTE: Delta tables created with an explicit LOCATION are "external", so
# DROP TABLE removes only the catalog entry and leaves the data directory in
# place -- re-creating the table would silently re-attach to the old data. The
# streaming state is therefore cleared on disk as well as in the catalog.
rm -rf lakehouse/bronze/sessions_stream lakehouse/silver/sessions_stream \
       lakehouse/ops/stream_batches
$PY - <<'EOF'
import dlg
from dlg.spark import get_spark
s = get_spark("stream-demo")
for t in ("ops.stream_batches", "silver.sessions_stream", "bronze.sessions_stream"):
    s.sql(f"DROP TABLE IF EXISTS {t}")
print("dropped streaming tables and removed their data directories")
EOF

echo
echo "=============================================================="
echo "STEP 1 — initial file set (3 shards), stream --once"
echo "=============================================================="
for i in 0 1 2; do
  cp "$SRC/sessions-part-000$i.json" "$STREAM_SRC/initial-part-000$i.json"
done
ls -1 "$STREAM_SRC" | sed 's/^/  seeded: /'

$PY -m dlg.cli stream --once --json-out /tmp/stream_run1.json

BEFORE=$(count_silver)
echo "  silver.sessions_stream rows after run 1: $BEFORE"

echo
echo "=============================================================="
echo "STEP 2 — 2 NEW files + 1 DUPLICATE of an already-processed file"
echo "=============================================================="
# Two genuinely new shards that have not been streamed yet.
cp "$SRC/sessions-part-0003.json" "$STREAM_SRC/new-microbatch-a.json"
cp "$SRC/sessions-part-0004.json" "$STREAM_SRC/new-microbatch-b.json"
# A byte-identical copy of a file that was already processed in step 1.
cp "$STREAM_SRC/initial-part-0000.json" "$STREAM_SRC/duplicate-of-processed.json"
ls -1 "$STREAM_SRC" | sed 's/^/  present: /'

$PY -m dlg.cli stream --once --json-out /tmp/stream_run2.json

AFTER=$(count_silver)
NEW_ROWS=$((AFTER - BEFORE))
echo "  silver.sessions_stream rows after run 2: $AFTER (delta +$NEW_ROWS)"

# The duplicate file contributes exactly zero new rows; its rows are already in
# silver under the same (player_id, session_id) with the same event_ts, so the
# guarded MERGE updates nothing.
DUP_FILE_ROWS=$(grep -c . "$STREAM_SRC/duplicate-of-processed.json" || true)
echo "  duplicate file lines (already processed in step 1): $DUP_FILE_ROWS"

if [ "$NEW_ROWS" -le 0 ]; then
  echo "ASSERTION (a) FAILED: no new rows appeared"
  exit 1
fi
echo "ASSERTION (a) PASSED: new rows appeared (+$NEW_ROWS)"

echo
echo "=============================================================="
echo "STEP 3 — duplicate only: drops zero rows"
echo "=============================================================="
BRONZE_BEFORE=$(count_bronze)
QUARANTINE_BEFORE=$(count_quarantine)
cp "$STREAM_SRC/initial-part-0001.json" "$STREAM_SRC/duplicate-again.json"
$PY -m dlg.cli stream --once --json-out /tmp/stream_run3.json
AFTER_DUP=$(count_silver)
BRONZE_AFTER=$(count_bronze)
QUARANTINE_AFTER=$(count_quarantine)
echo "  silver.sessions_stream rows after run 3: $AFTER_DUP"
echo "  bronze.sessions_stream rows after run 3: $BRONZE_AFTER"
echo "  bronze.quarantine rows after run 3     : $QUARANTINE_AFTER"
if [ "$AFTER_DUP" -ne "$AFTER" ] || [ "$BRONZE_AFTER" -ne "$BRONZE_BEFORE" ] \
   || [ "$QUARANTINE_AFTER" -ne "$QUARANTINE_BEFORE" ]; then
  echo "ASSERTION (b) FAILED: the duplicate added silver=$((AFTER_DUP - AFTER))" \
       "bronze=$((BRONZE_AFTER - BRONZE_BEFORE)) quarantine=$((QUARANTINE_AFTER - QUARANTINE_BEFORE))"
  exit 1
fi
echo "ASSERTION (b) PASSED: the duplicate added 0 rows to silver, bronze and quarantine"

echo
echo "=============================================================="
echo "STEP 4 — nothing new: checkpoint prevents reprocessing"
echo "=============================================================="
$PY -m dlg.cli stream --once --json-out /tmp/stream_run4.json
AFTER_IDLE=$(count_silver)
echo "  silver.sessions_stream rows after run 4: $AFTER_IDLE"
if [ "$AFTER_IDLE" -ne "$AFTER_DUP" ]; then
  echo "ASSERTION (c) FAILED: the idle re-run added $((AFTER_IDLE - AFTER_DUP)) row(s)"
  exit 1
fi
echo "ASSERTION (c) PASSED: the checkpoint-replay added 0 rows"

echo
echo "=============================================================="
echo "STEP 5 — replay the last batch through the same handler"
echo "=============================================================="
$PY -m dlg.cli stream --replay-last-batch --json-out /tmp/stream_replay.json
AFTER_REPLAY=$(count_silver)
echo "  silver.sessions_stream rows after replay: $AFTER_REPLAY"
if [ "$AFTER_REPLAY" -ne "$AFTER_IDLE" ]; then
  echo "ASSERTION (d) FAILED: the batch replay added $((AFTER_REPLAY - AFTER_IDLE)) row(s)"
  exit 1
fi
echo "ASSERTION (d) PASSED: the content-addressed _batch_id guard made the replay a no-op"

echo
echo "=============================================================="
echo "LEDGER (from ops.stream_batches)"
echo "=============================================================="
$PY - <<'EOF' 2>/dev/null
import dlg
from dlg.spark import get_spark
from tabulate import tabulate

s = get_spark("stream-demo")
rows = s.table("ops.stream_batches").orderBy("ts").collect()
print(
    tabulate(
        [
            [
                r["batch_id"],
                r["source_files"],
                f"{r['rows_read']:,}",
                f"{r['bronze_inserted']:,}",
                f"{r['silver_inserted']:,}",
                f"{r['silver_updated']:,}",
                f"{r['rows_duplicate_suppressed']:,}",
                f"{r['quarantined']:,}",
                r["latency_ms"],
                r["skipped_replay"],
            ]
            for r in rows
        ],
        headers=[
            "batch_id", "files", "rows_read", "bronze_+", "silver_+", "silver_~",
            "dups_suppressed", "quarantined", "latency_ms", "replayed",
        ],
        tablefmt="github",
    )
)
print()
print(f"micro-batches recorded : {len(rows)}")
print(f"silver.sessions_stream : {s.table('silver.sessions_stream').count():,} rows")
print(f"bronze.sessions_stream : {s.table('bronze.sessions_stream').count():,} rows")
EOF

echo
echo "=============================================================="
echo "ALL STREAMING ASSERTIONS PASSED"
echo "  run1 silver rows        : $BEFORE"
echo "  run2 silver rows        : $AFTER  (+$NEW_ROWS, duplicate contributed 0)"
echo "  run3 silver rows        : $AFTER_DUP  (duplicate-only batch: +0)"
echo "  run4 silver rows        : $AFTER_IDLE  (checkpoint replay: +0)"
echo "  run5 silver rows        : $AFTER_REPLAY  (batch-id replay guard: +0)"
echo "=============================================================="
