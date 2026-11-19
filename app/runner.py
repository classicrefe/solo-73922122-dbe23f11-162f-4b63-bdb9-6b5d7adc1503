"""Background training-run worker.

Semantics:
* A run covers exactly the samples with ``seq <= watermark_seq`` frozen at
  run-creation time. Batches uploaded while the run is in progress are
  accepted but belong to the NEXT run.
* Progress is persisted per sample in ``run_results`` (INSERT ... ON
  CONFLICT DO NOTHING), so a crash/restart loses at most the in-flight
  sample. On startup a stale 'running' run is moved back to 'pending' and
  resumes from the first sample without a stored result — completed
  samples are never recomputed and later-submitted batches never leak in.
* Each sample is computed in its own short transaction; no snapshot is
  held across samples.
"""

from __future__ import annotations

import logging
import threading

from psycopg_pool import ConnectionPool

from .features import compute_feature

log = logging.getLogger("runner")

_PENDING_SAMPLES_SQL = """
SELECT s.seq, s.sample_id, s.account_id, s.prediction_at
FROM samples s
WHERE s.seq <= %(watermark)s
  AND NOT EXISTS (
      SELECT 1 FROM run_results r
      WHERE r.run_id = %(run_id)s AND r.sample_seq = s.seq
  )
ORDER BY s.seq
LIMIT %(limit)s
"""

_INSERT_RESULT_SQL = """
INSERT INTO run_results
    (run_id, sample_seq, sample_id, account_id, prediction_at,
     amount_cents, payment_count)
VALUES (%(run_id)s, %(seq)s, %(sample_id)s, %(account_id)s, %(prediction_at)s,
        %(amount)s, %(count)s)
ON CONFLICT (run_id, sample_seq) DO NOTHING
"""

_REFRESH_PROGRESS_SQL = """
UPDATE training_runs
SET done_samples = (SELECT count(*) FROM run_results WHERE run_id = %(run_id)s)
WHERE run_id = %(run_id)s
"""


class RunWorker(threading.Thread):
    def __init__(
        self,
        pool: ConnectionPool,
        stop_event: threading.Event,
        chunk_size: int = 200,
        chunk_delay_s: float = 0.0,
        idle_sleep_s: float = 0.5,
    ):
        super().__init__(name="run-worker", daemon=True)
        self.pool = pool
        self.stop_event = stop_event
        self.chunk_size = chunk_size
        self.chunk_delay_s = chunk_delay_s
        self.idle_sleep_s = idle_sleep_s

    def run(self) -> None:
        self._reset_stale_runs()
        while not self.stop_event.is_set():
            claimed = self._claim_next()
            if claimed is None:
                self.stop_event.wait(self.idle_sleep_s)
                continue
            run_id, watermark_seq = claimed
            try:
                self._process(run_id, watermark_seq)
            except Exception:
                log.exception("run %s failed", run_id)
                try:
                    with self.pool.connection() as conn:
                        conn.execute(
                            "UPDATE training_runs SET status = 'failed', "
                            "error = %s, finished_at = now() "
                            "WHERE run_id = %s AND status = 'running'",
                            ("worker error, see service logs", run_id),
                        )
                except Exception:
                    log.exception("could not mark run %s as failed", run_id)

    def _reset_stale_runs(self) -> None:
        with self.pool.connection() as conn:
            n = conn.execute(
                "UPDATE training_runs SET status = 'pending' WHERE status = 'running'"
            ).rowcount
        if n:
            log.info("re-queued %s run(s) interrupted by shutdown", n)

    def _claim_next(self) -> tuple[int, int] | None:
        with self.pool.connection() as conn:
            row = conn.execute(
                """
                UPDATE training_runs
                SET status = 'running',
                    started_at = COALESCE(started_at, now())
                WHERE run_id = (
                    SELECT run_id FROM training_runs
                    WHERE status = 'pending'
                    ORDER BY run_id
                    LIMIT 1
                )
                RETURNING run_id, watermark_seq
                """
            ).fetchone()
        return (row[0], row[1]) if row else None

    def _process(self, run_id: int, watermark_seq: int) -> None:
        log.info("run %s: processing samples up to seq %s", run_id, watermark_seq)
        while not self.stop_event.is_set():
            with self.pool.connection() as conn:
                rows = conn.execute(
                    _PENDING_SAMPLES_SQL,
                    {"run_id": run_id, "watermark": watermark_seq, "limit": self.chunk_size},
                ).fetchall()
            if not rows:
                break
            for seq, sample_id, account_id, prediction_at in rows:
                if self.stop_event.is_set():
                    break
                # One short transaction per sample: compute + persist.
                with self.pool.connection() as conn:
                    amount, count = compute_feature(conn, account_id, prediction_at)
                    conn.execute(
                        _INSERT_RESULT_SQL,
                        {
                            "run_id": run_id,
                            "seq": seq,
                            "sample_id": sample_id,
                            "account_id": account_id,
                            "prediction_at": prediction_at,
                            "amount": amount,
                            "count": count,
                        },
                    )
            with self.pool.connection() as conn:
                conn.execute(_REFRESH_PROGRESS_SQL, {"run_id": run_id})
            if self.chunk_delay_s:
                self.stop_event.wait(self.chunk_delay_s)
        if self.stop_event.is_set():
            # Graceful shutdown mid-run: hand the run back so the next
            # process resumes it from the persisted results.
            with self.pool.connection() as conn:
                conn.execute(
                    "UPDATE training_runs SET status = 'pending' "
                    "WHERE run_id = %s AND status = 'running'",
                    (run_id,),
                )
            log.info("run %s: paused by shutdown, will resume on next start", run_id)
            return
        with self.pool.connection() as conn:
            conn.execute(
                "UPDATE training_runs SET status = 'completed', finished_at = now() "
                "WHERE run_id = %s AND status = 'running'",
                (run_id,),
            )
            conn.execute(_REFRESH_PROGRESS_SQL, {"run_id": run_id})
        log.info("run %s: completed", run_id)
