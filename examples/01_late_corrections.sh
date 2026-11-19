#!/usr/bin/env bash
# Example 1 — late-arriving corrections and point-in-time reconstruction.
#
# Timeline (all UTC, T = 2026-11-15T12:00:00Z):
#   * base revisions are visible before T
#   * corrections become visible at T+1h (after the first predictions)
#   * one backdated revision (p6) was *knowable* at T but arrives late
#
# Expected results:
#   run 1: s1(A@T)=1300 (p1 1000 + p4 300; p2 outside 24h window), s2(B@T)=700
#   run 2: s3(A@T+2h)=6249 (p2 5000 moved into window + p5 250 + p6 999;
#                        p1 corrected away to acct-C, p4 revoked)
#          s4(C@T+2h)=1000 (p1 now belongs to acct-C)
#          s5(A@T)=2299   (same prediction time as s1, recomputed later:
#                        p6 backdated-visible-at-T now included = 999 more;
#                        T+1h corrections still correctly excluded)
#   run 1 output is immutable: s1 stays 1300 there forever.
set -euo pipefail
cd "$(dirname "$0")"
source ./_common.sh
require_app

echo "== 1. import base revisions (4 rows) =="
post_csv /payment-revision-batches data/revisions_base.csv; echo

echo "== 2. import first samples (s1, s2) and create run 1 =="
post_csv /sample-batches data/samples_1.csv; echo
RUN1=$(curl -sf -X POST "$BASE/training-runs" | python3 -c "import sys,json;print(json.load(sys.stdin)['run_id'])")
echo "run $RUN1: $(wait_run "$RUN1")"
echo "--- dataset run $RUN1 ---"
curl -sf "$BASE/training-runs/$RUN1/dataset.csv"

echo "== 3. late corrections arrive (visible at T+1h, plus backdated p6) =="
post_csv /payment-revision-batches data/revisions_late.csv; echo

echo "== 4. more samples (s3, s4, s5) and run 2 =="
post_csv /sample-batches data/samples_2.csv; echo
RUN2=$(curl -sf -X POST "$BASE/training-runs" | python3 -c "import sys,json;print(json.load(sys.stdin)['run_id'])")
echo "run $RUN2: $(wait_run "$RUN2")"
echo "--- dataset run $RUN2 ---"
curl -sf "$BASE/training-runs/$RUN2/dataset.csv"

echo "--- dataset run $RUN1 again (immutable, s1 must still be 1300) ---"
curl -sf "$BASE/training-runs/$RUN1/dataset.csv"

cat <<EOF

Reading the numbers (T = 2026-11-15T12:00:00Z):
  s1 acct-A@T       run1=1300  p1(1000)+p4(300); p2 occurred T-25h, outside [T-24h,T)
  s3 acct-A@T+2h    run2=6249  p2 corrected to T-5h (5000) + p5 late (250) + p6 (999);
                               p1 corrected to acct-C, p4 revoked (old revision NOT restored)
  s4 acct-C@T+2h    run2=1000  p1's correction moved it to acct-C
  s5 acct-A@T       run2=2299  same prediction instant as s1: corrections visible at
                               T+1h are still excluded; only backdated p6 (visible T-5h)
                               is added -> 1300+999
EOF
