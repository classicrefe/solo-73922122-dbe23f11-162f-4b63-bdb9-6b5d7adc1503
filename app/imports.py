"""Transactional batch imports.

Two entry points share the same storage rules:

* ``import_revision_batch`` / ``import_sample_batch`` (original endpoints):
  all-or-nothing — same key with identical content is reused, same key with
  different content rejects the ENTIRE batch (the caller's transaction
  rolls back, nothing is stored).
* ``import_revision_reupload`` (channel re-upload endpoint): per-payment
  rejection — a payment whose revisions conflict (in-file same key with
  different content, or stored content differing for a key) loses ALL its
  rows of this batch, while every other payment is inserted/reused as
  usual. The receipt (counts, conflict keys, correction CSV) is written in
  the same transaction, so rows and receipt commit or vanish together.

Both insert revision rows in ascending (payment_id, revision_id) order so
concurrent imports through the two endpoints take row locks in the same
order and cannot deadlock each other; a key raced between them is resolved
by INSERT ... ON CONFLICT DO NOTHING + re-read, never by check-then-insert,
so a payment can never be half-stored.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from psycopg import Connection
from psycopg.types.json import Jsonb

from .csvio import REVISION_COLUMNS, RevisionRecord, RevisionRow, SampleRow

# Advisory lock key serializing "sample batch acceptance" against
# "training run creation" so a run's watermark is an exact acceptance
# instant: every batch committed before the lock is included, every batch
# after it is not.
ACCEPTANCE_LOCK_KEY = 730001

_INSERT_REVISION_SQL = """
INSERT INTO payment_revisions
    (payment_id, revision_id, account_id, occurred_at, visible_at,
     amount_cents, revoked)
VALUES (%(payment_id)s, %(revision_id)s, %(account_id)s,
        %(occurred_at)s, %(visible_at)s, %(amount_cents)s, %(revoked)s)
ON CONFLICT (payment_id, revision_id) DO NOTHING
RETURNING payment_id
"""

_SELECT_REVISION_SQL = """
SELECT account_id, occurred_at, visible_at, amount_cents, revoked
FROM payment_revisions
WHERE payment_id = %s AND revision_id = %s
"""


class ConflictError(Exception):
    """Same key already exists with different content; batch rejected."""

    def __init__(self, conflicts: list[dict[str, Any]]):
        self.conflicts = conflicts
        keys = ", ".join(c["key"] for c in conflicts[:5])
        super().__init__(
            f"{len(conflicts)} conflicting row(s) already exist with different "
            f"content; entire batch rejected. First keys: {keys}"
        )


def _ts(dt: datetime) -> str:
    return dt.isoformat()


def _revision_params(r: RevisionRow) -> dict[str, Any]:
    return {
        "payment_id": r.payment_id,
        "revision_id": r.revision_id,
        "account_id": r.account_id,
        "occurred_at": r.occurred_at,
        "visible_at": r.visible_at,
        "amount_cents": r.amount_cents,
        "revoked": r.revoked,
    }


def _revision_content_dict(
    account_id: str,
    occurred_at: datetime,
    visible_at: datetime,
    amount_cents: int,
    revoked: bool,
) -> dict[str, Any]:
    return {
        "account_id": account_id,
        "occurred_at_utc": _ts(occurred_at),
        "visible_at_utc": _ts(visible_at),
        "amount_cents": amount_cents,
        "revoked": revoked,
    }


def _existing_revision_dict(existing: tuple) -> dict[str, Any]:
    return _revision_content_dict(*existing)


def _incoming_revision_dict(r: RevisionRow) -> dict[str, Any]:
    return _revision_content_dict(
        r.account_id, r.occurred_at, r.visible_at, r.amount_cents, r.revoked
    )


def import_revision_batch(
    conn: Connection, rows: list[RevisionRow], in_file_dups: int
) -> tuple[int, int, int]:
    """Insert one revision batch. Returns (batch_id, inserted, reused).

    Must be called inside a transaction; raises ConflictError to roll the
    whole batch back.
    """
    inserted = 0
    reused = in_file_dups
    conflicts: list[dict[str, Any]] = []
    for r in sorted(rows, key=lambda row: (row.payment_id, row.revision_id)):
        got = conn.execute(_INSERT_REVISION_SQL, _revision_params(r)).fetchone()
        if got:
            inserted += 1
            continue
        existing = conn.execute(
            _SELECT_REVISION_SQL, (r.payment_id, r.revision_id)
        ).fetchone()
        if existing and tuple(existing) == r.content():
            reused += 1
        else:
            conflicts.append(
                {
                    "key": f"{r.payment_id}#{r.revision_id}",
                    "existing": _existing_revision_dict(existing) if existing else None,
                    "incoming": _incoming_revision_dict(r),
                }
            )
    if conflicts:
        raise ConflictError(conflicts)
    batch_id = conn.execute(
        """
        INSERT INTO revision_batches (rows_total, rows_inserted, rows_reused)
        VALUES (%s, %s, %s)
        RETURNING batch_id
        """,
        (len(rows) + in_file_dups, inserted, reused),
    ).fetchone()[0]
    return batch_id, inserted, reused


def _render_correction_csv(sources: list[tuple[int, str]]) -> str:
    """Header + the exact source lines of rejected payments, in file order."""
    out = [",".join(REVISION_COLUMNS), "\n"]
    for _, source in sorted(sources):
        out.append(source)
        if not source.endswith("\n"):
            out.append("\n")
    return "".join(out)


def import_revision_reupload(
    conn: Connection, records: list[RevisionRecord]
) -> dict[str, Any]:
    """Import a re-uploaded revision batch with per-payment rejection.

    A payment is rejected — ALL its lines of this batch — when the file
    carries the same (payment_id, revision_id) with different content, or
    when a key's stored content differs from the file. Rows of a rejected
    payment inserted earlier in this transaction are rolled back to a
    per-payment savepoint, so a payment is never half-stored; racing
    imports through the original endpoint are detected the same way
    (INSERT ... ON CONFLICT DO NOTHING then re-read). Every other payment
    is inserted/reused by the usual rules.

    The receipt (counts, conflict keys, correction CSV with the rejected
    payments' exact source lines) is inserted in the SAME transaction:
    rows and receipt commit together or not at all. Must be called inside
    a transaction. Returns the receipt as a dict.
    """
    by_payment: dict[str, list[RevisionRecord]] = {}
    for rec in records:
        by_payment.setdefault(rec.row.payment_id, []).append(rec)

    inserted = 0
    reused = 0
    rejected = 0
    conflicts: list[dict[str, Any]] = []
    rejected_payments: list[str] = []
    rejected_sources: list[tuple[int, str]] = []

    for payment_id in sorted(by_payment):
        recs = by_payment[payment_id]
        reference: dict[tuple[str, int], RevisionRow] = {}
        bad_keys: set[tuple[str, int]] = set()
        dup_lines = 0
        for rec in recs:
            key = (rec.row.payment_id, rec.row.revision_id)
            prev = reference.get(key)
            if prev is None:
                reference[key] = rec.row
            elif prev.content() != rec.row.content():
                bad_keys.add(key)
            else:
                dup_lines += 1
        if bad_keys:
            rejected += len(recs)
            rejected_payments.append(payment_id)
            for pid, rid in sorted(bad_keys):
                conflicts.append(
                    {
                        "key": f"{pid}#{rid}",
                        "reason": "in_file_conflict",
                        "detail": "key appears with different content within "
                        "this file; all revisions of the payment were rejected",
                    }
                )
            rejected_sources.extend((r.line_no, r.source) for r in recs)
            continue

        conn.execute("SAVEPOINT reupload_payment")
        payment_inserted = 0
        payment_reused = dup_lines
        conflict: dict[str, Any] | None = None
        for r in sorted(reference.values(), key=lambda row: row.revision_id):
            got = conn.execute(_INSERT_REVISION_SQL, _revision_params(r)).fetchone()
            if got:
                payment_inserted += 1
                continue
            existing = conn.execute(
                _SELECT_REVISION_SQL, (r.payment_id, r.revision_id)
            ).fetchone()
            if existing and tuple(existing) == r.content():
                payment_reused += 1
                continue
            conflict = {
                "key": f"{r.payment_id}#{r.revision_id}",
                "reason": "db_conflict",
                "existing": _existing_revision_dict(existing) if existing else None,
                "incoming": _incoming_revision_dict(r),
            }
            break
        if conflict is None:
            conn.execute("RELEASE SAVEPOINT reupload_payment")
            inserted += payment_inserted
            reused += payment_reused
        else:
            conn.execute("ROLLBACK TO SAVEPOINT reupload_payment")
            conn.execute("RELEASE SAVEPOINT reupload_payment")
            rejected += len(recs)
            rejected_payments.append(payment_id)
            conflicts.append(conflict)
            rejected_sources.extend((r.line_no, r.source) for r in recs)

    rejected_csv = _render_correction_csv(rejected_sources)
    batch_id, received_at = conn.execute(
        """
        INSERT INTO revision_reupload_batches
            (rows_total, rows_inserted, rows_reused, rows_rejected,
             rejected_payments, conflicts, rejected_csv)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING batch_id, received_at
        """,
        (
            len(records),
            inserted,
            reused,
            rejected,
            Jsonb(rejected_payments),
            Jsonb(conflicts),
            rejected_csv,
        ),
    ).fetchone()
    return {
        "batch_id": batch_id,
        "received_at": received_at,
        "rows_total": len(records),
        "rows_inserted": inserted,
        "rows_reused": reused,
        "rows_rejected": rejected,
        "rejected_payments": rejected_payments,
        "conflicts": conflicts,
        "rejected_csv": rejected_csv,
    }


def import_sample_batch(
    conn: Connection, rows: list[SampleRow], in_file_dups: int
) -> tuple[int, int, int]:
    """Insert one sample batch. Returns (batch_id, inserted, reused).

    Takes the acceptance advisory lock so training-run watermarks are exact.
    """
    conn.execute("SELECT pg_advisory_xact_lock(%s)", (ACCEPTANCE_LOCK_KEY,))
    batch_id = conn.execute(
        """
        INSERT INTO sample_batches (rows_total, rows_inserted, rows_reused)
        VALUES (%s, 0, 0)
        RETURNING batch_id
        """,
        (len(rows) + in_file_dups,),
    ).fetchone()[0]
    inserted = 0
    reused = in_file_dups
    conflicts: list[dict[str, Any]] = []
    for r in rows:
        got = conn.execute(
            """
            INSERT INTO samples (batch_id, sample_id, account_id, prediction_at)
            VALUES (%(batch_id)s, %(sample_id)s, %(account_id)s, %(prediction_at)s)
            ON CONFLICT (sample_id) DO NOTHING
            RETURNING seq
            """,
            {
                "batch_id": batch_id,
                "sample_id": r.sample_id,
                "account_id": r.account_id,
                "prediction_at": r.prediction_at,
            },
        ).fetchone()
        if got:
            inserted += 1
            continue
        existing = conn.execute(
            "SELECT account_id, prediction_at FROM samples WHERE sample_id = %s",
            (r.sample_id,),
        ).fetchone()
        if existing and tuple(existing) == r.content():
            reused += 1
        else:
            conflicts.append(
                {
                    "key": r.sample_id,
                    "existing": {
                        "account_id": existing[0],
                        "prediction_at_utc": _ts(existing[1]),
                    }
                    if existing
                    else None,
                    "incoming": {
                        "account_id": r.account_id,
                        "prediction_at_utc": _ts(r.prediction_at),
                    },
                }
            )
    if conflicts:
        raise ConflictError(conflicts)
    conn.execute(
        "UPDATE sample_batches SET rows_inserted = %s, rows_reused = %s WHERE batch_id = %s",
        (inserted, reused, batch_id),
    )
    return batch_id, inserted, reused
