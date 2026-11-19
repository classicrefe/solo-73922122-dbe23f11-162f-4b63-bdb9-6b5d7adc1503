"""Point-in-time feature reconstruction.

For one sample (account_id, prediction_at) we rebuild exactly what was
knowable at prediction time:

* among all revisions of a payment with ``visible_at <= prediction_at``
  take the one with the highest revision_id (later corrections supersede
  earlier ones and may change account, occurred time and amount);
* if that top revision is revoked the payment contributes nothing —
  revocation does NOT resurrect older revisions;
* the payment counts towards the feature when its (corrected) account
  matches and its (corrected) occurred_at falls inside
  [prediction_at - 24h, prediction_at).

The query runs in a short read transaction against the live table. No
database snapshot is held across samples and no private copy of the
revision history is materialized for a run.
"""

from __future__ import annotations

from datetime import datetime

from psycopg import Connection

FEATURE_SQL = """
WITH top_rev AS (
    SELECT DISTINCT ON (payment_id)
           payment_id, account_id, occurred_at, amount_cents, revoked
    FROM payment_revisions
    WHERE visible_at <= %(pred)s
    ORDER BY payment_id, revision_id DESC
)
SELECT COALESCE(SUM(amount_cents), 0)::bigint AS amount_cents,
       COUNT(*)::int AS payment_count
FROM top_rev
WHERE NOT revoked
  AND account_id = %(acc)s
  AND occurred_at >= %(pred)s - INTERVAL '24 hours'
  AND occurred_at <  %(pred)s
"""


def compute_feature(
    conn: Connection, account_id: str, prediction_at: datetime
) -> tuple[int, int]:
    """Return (amount_last_24h_cents, payment_count) for one sample."""
    row = conn.execute(
        FEATURE_SQL, {"acc": account_id, "pred": prediction_at}
    ).fetchone()
    return int(row[0]), int(row[1])
