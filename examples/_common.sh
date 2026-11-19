#!/usr/bin/env bash
# Common helpers for the example scripts.
set -euo pipefail

BASE="${BASE:-http://localhost:8000}"

post_csv() { # path file
  curl -sf -X POST "$BASE$1" -H 'Content-Type: text/csv' --data-binary @"$2"
}

post_csv_expect_conflict() { # path file -> prints body, returns 0 on HTTP 409
  curl -s -o /tmp/last_body.json -w '%{http_code}' -X POST "$BASE$1" \
    -H 'Content-Type: text/csv' --data-binary @"$2" | grep -q '^409$' \
    && cat /tmp/last_body.json
}

json_field() { # json_field <json> <python-expr-on-d>
  python3 -c "import sys,json; d=json.load(sys.stdin); print($2)" <<<"$1"
}

run_status() { # run_id -> "status done total"
  curl -sf "$BASE/training-runs/$1" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); print(d['status'], d['done_samples'], d['total_samples'])"
}

wait_run() { # run_id -> waits for completed/failed, prints final status line
  local id="$1" last=""
  for _ in $(seq 1 300); do
    last="$(run_status "$id")"
    case "${last%% *}" in
      completed|failed) echo "$last"; return 0 ;;
    esac
    sleep 1
  done
  echo "timeout waiting for run $id (last: $last)" >&2
  return 1
}

require_app() {
  curl -sf "$BASE/health" >/dev/null || {
    echo "API not reachable at $BASE — start it with: docker compose up -d --build" >&2
    exit 1
  }
}
