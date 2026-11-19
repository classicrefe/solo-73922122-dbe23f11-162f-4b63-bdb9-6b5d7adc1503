#!/usr/bin/env bash
# Example 4 — channel re-upload with per-payment rejection and a
# correction CSV (the "receipt" flow).
#
# Unlike the original endpoint (all-or-nothing), the re-upload endpoint
# stores the good payments and rejects only the payments whose revisions
# conflict — then hands every source line of those payments back in a
# correction CSV so the channel can fix and re-upload them.
#
# Expected on a fresh stack (revisions_base.csv loaded in step 1):
#   * p1#1 conflicts with the stored row (7777 vs 1000) -> p1 fully rejected
#   * p8#1 appears twice with different amounts -> p8 fully rejected
#   * p7#1 is new -> inserted; p2#1 is identical -> reused
set -euo pipefail
cd "$(dirname "$0")"
source ./_common.sh
require_app

echo "== 1. ensure base revisions are present (idempotent re-use) =="
post_csv /payment-revision-batches data/revisions_base.csv; echo

echo "== 2. re-upload a batch with two problematic payments =="
RESP=$(curl -sf -X POST "$BASE/payment-revision-reuploads" \
  -H 'Content-Type: text/csv' --data-binary @data/revisions_reupload.csv)
echo "$RESP" | python3 -m json.tool
BATCH=$(json_field "$RESP" "d['batch_id']")
echo "receipt: batch $BATCH — inserted=$(json_field "$RESP" "d['rows_inserted']")" \
  "reused=$(json_field "$RESP" "d['rows_reused']") rejected=$(json_field "$RESP" "d['rows_rejected']")"

echo "== 3. fetch the receipt by batch id =="
curl -sf "$BASE/payment-revision-reuploads/$BATCH" | python3 -m json.tool

echo "== 4. download the correction CSV (all source lines of rejected payments) =="
curl -sf "$BASE/payment-revision-reuploads/$BATCH/rejected.csv" | tee /tmp/example4_rejected.csv

echo "== 5. the channel fixes the lines and re-uploads the correction CSV =="
python3 - <<'EOF'
import csv
with open('/tmp/example4_rejected.csv') as f:
    rows = list(csv.reader(f))
header, data = rows[0], rows[1:]
fixed, seen_p8 = [], False
for r in data:
    if r[0] == 'p1' and r[1] == '1':
        r[5] = '1000'            # restore the amount that matches the stored row
    if r[0] == 'p8':
        if seen_p8:
            continue             # drop the conflicting duplicate line
        seen_p8 = True
        r[5] = '250'             # settle on a single amount
    fixed.append(r)
with open('/tmp/example4_fixed.csv', 'w', newline='') as f:
    w = csv.writer(f, lineterminator='\n')
    w.writerow(header)
    w.writerows(fixed)
print('fixed CSV -> /tmp/example4_fixed.csv')
EOF
curl -sf -X POST "$BASE/payment-revision-reuploads" \
  -H 'Content-Type: text/csv' --data-binary @/tmp/example4_fixed.csv | python3 -m json.tool

echo "== 6. the original endpoint is still all-or-nothing =="
post_csv_expect_conflict /payment-revision-batches data/revisions_conflict.csv; echo
echo "p9 stored? $(curl -sf "$BASE/stats" | python3 -c "import sys,json;print(json.load(sys.stdin)['payment_revisions'])") revision rows in total (p9 must be absent)"
