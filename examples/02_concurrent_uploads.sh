#!/usr/bin/env bash
# Example 2 — concurrent uploads while a run is in progress, idempotent
# re-upload, and all-or-nothing conflict rejection.
#
# A run freezes its acceptance watermark at creation: batches uploaded
# while it runs are accepted (201) but belong to the NEXT run.
#
# Tip: to widen the observable window, start the stack with a per-chunk
# delay, e.g.  RUN_CHUNK_DELAY_MS=50 docker compose up -d --build
set -euo pipefail
cd "$(dirname "$0")"
source ./_common.sh
require_app

DATA=/tmp/example2_samples.csv
python3 - <<'EOF'
import csv
with open('/tmp/example2_samples.csv', 'w', newline='') as f:
    w = csv.writer(f)
    w.writerow(['sample_id', 'account_id', 'prediction_at_utc'])
    for i in range(5000):
        w.writerow([f'ex2-{i:05d}', 'acct-A', '2026-11-15T12:00:00Z'])
print('generated 5000 samples -> /tmp/example2_samples.csv')
EOF

echo "== 1. upload 5000 samples and create a run =="
post_csv /sample-batches "$DATA"; echo
RUN1_JSON=$(curl -sf -X POST "$BASE/training-runs")
RUN1=$(json_field "$RUN1_JSON" "d['run_id']")
TOTAL1=$(json_field "$RUN1_JSON" "d['total_samples']")
echo "run $RUN1 created with total_samples=$TOTAL1"

echo "== 2. while it runs: upload 50 more samples (accepted, but excluded from run $RUN1) =="
python3 - <<'EOF'
import csv
with open('/tmp/example2_extra.csv', 'w', newline='') as f:
    w = csv.writer(f)
    w.writerow(['sample_id', 'account_id', 'prediction_at_utc'])
    for i in range(50):
        w.writerow([f'ex2-extra-{i:03d}', 'acct-A', '2026-11-15T12:00:00Z'])
EOF
post_csv /sample-batches /tmp/example2_extra.csv; echo

echo "== 3. idempotent re-upload of identical revisions (all reused) =="
post_csv /payment-revision-batches data/revisions_base.csv; echo

echo "== 4. conflicting revision (p1#1 with different amount) -> HTTP 409, whole batch rejected =="
post_csv_expect_conflict /payment-revision-batches data/revisions_conflict.csv; echo
echo "revision rows still stored: $(curl -sf "$BASE/stats" | python3 -c "import sys,json;print(json.load(sys.stdin)['payment_revisions'])")"

echo "== 5. run $RUN1 progress (total stays $TOTAL1) =="
wait_run "$RUN1"
ROWS=$(curl -sf "$BASE/training-runs/$RUN1/dataset.csv" | tail -n +2 | wc -l)
EXTRA=$(curl -sf "$BASE/training-runs/$RUN1/dataset.csv" | grep -c '^ex2-extra-' || true)
echo "run $RUN1 dataset rows: $ROWS (total_samples=$TOTAL1), ex2-extra rows: $EXTRA (must be 0)"

echo "== 6. next run picks up the late batch =="
RUN2=$(curl -sf -X POST "$BASE/training-runs" | python3 -c "import sys,json;print(json.load(sys.stdin)['run_id'])")
wait_run "$RUN2"
EXTRA2=$(curl -sf "$BASE/training-runs/$RUN2/dataset.csv" | grep -c '^ex2-extra-' || true)
echo "run $RUN2 dataset now contains $EXTRA2 ex2-extra rows (must be 50)"
