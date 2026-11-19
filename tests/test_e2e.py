"""End-to-end tests against a real PostgreSQL, driving the ASGI app in-process.

Requires a reachable PostgreSQL (see TEST_DATABASE_URL). The database is
recreated empty for the test module. Run with:

    TEST_DATABASE_URL=postgresql://app@127.0.0.1:5544/appdb_test2 \
    TEST_ADMIN_URL=postgresql://app@127.0.0.1:5544/postgres \
        pytest tests/test_e2e.py
"""

from __future__ import annotations

import csv
import io
import os
import threading
import time
import unittest

os.environ["DATABASE_URL"] = os.environ.get(
    "TEST_DATABASE_URL", "postgresql://app@127.0.0.1:5544/appdb_test2"
)
os.environ.setdefault("RUN_CHUNK_SIZE", "100")
os.environ.setdefault("RUN_CHUNK_DELAY_MS", "50")
os.environ.setdefault("RUNNER_IDLE_SLEEP_MS", "100")

import psycopg  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from app.csvio import parse_revision_csv, parse_revision_records  # noqa: E402
from app.db import create_pool  # noqa: E402
from app.imports import (  # noqa: E402
    ConflictError,
    import_revision_batch,
    import_revision_reupload,
)
from app.main import app  # noqa: E402

ADMIN_URL = os.environ.get("TEST_ADMIN_URL", "postgresql://app@127.0.0.1:5544/postgres")
DB_NAME = os.environ["DATABASE_URL"].rstrip("/").split("/")[-1]

REV_HEADER = (
    "payment_id,revision_id,account_id,occurred_at_utc,visible_at_utc,"
    "amount_cents,revoked\n"
)
SMP_HEADER = "sample_id,account_id,prediction_at_utc\n"


def make_db() -> None:
    try:
        with psycopg.connect(ADMIN_URL, autocommit=True) as conn:
            conn.execute(f'CREATE DATABASE "{DB_NAME}"')
    except psycopg.errors.DuplicateDatabase:
        pass
    with psycopg.connect(os.environ["DATABASE_URL"], autocommit=True) as conn:
        conn.execute("DROP SCHEMA public CASCADE")
        conn.execute("CREATE SCHEMA public")


def post_csv(client: TestClient, path: str, text: str):
    return client.post(
        path, content=text.encode(), headers={"Content-Type": "text/csv"}
    )


def wait_run(client: TestClient, run_id: int, timeout: float = 120.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = client.get(f"/training-runs/{run_id}")
        assert r.status_code == 200, r.text
        body = r.json()
        if body["status"] in ("completed", "failed"):
            return body
        time.sleep(0.2)
    raise AssertionError(f"run {run_id} did not finish in {timeout}s")


def dataset_rows(client: TestClient, run_id: int) -> list[dict]:
    r = client.get(f"/training-runs/{run_id}/dataset.csv")
    assert r.status_code == 200, r.text
    return list(csv.DictReader(io.StringIO(r.text)))


class E2E(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        make_db()

    def test_01_feature_semantics_and_boundaries(self):
        """Window edges, visibility edge, revocation, account/time corrections."""
        revs = [
            # occurred exactly T-24h -> inside window (lower bound inclusive)
            "bdy-b1,1,acct-A,2026-11-14T12:00:00Z,2026-11-15T11:00:00Z,100,false",
            # occurred exactly T -> outside window (upper bound exclusive)
            "bdy-b2,1,acct-A,2026-11-15T12:00:00Z,2026-11-15T11:00:00Z,200,false",
            # visible exactly at T -> visible (visibility is inclusive)
            "bdy-b3,1,acct-A,2026-11-15T11:59:59Z,2026-11-15T12:00:00Z,300,false",
            # visible 1s after T -> not visible
            "bdy-b4,1,acct-A,2026-11-15T11:00:00Z,2026-11-15T12:00:01Z,400,false",
            # revoked top revision: payment gone, old revision NOT resurrected
            "bdy-b5,1,acct-A,2026-11-15T10:00:00Z,2026-11-15T09:00:00Z,500,false",
            "bdy-b5,2,acct-A,2026-11-15T10:00:00Z,2026-11-15T11:00:00Z,500,true",
            # correction moves payment to another account
            "bdy-b6,1,acct-A,2026-11-15T10:00:00Z,2026-11-15T09:00:00Z,600,false",
            "bdy-b6,2,acct-B,2026-11-15T10:00:00Z,2026-11-15T11:00:00Z,600,false",
            # correction moves occurred_at INTO the window
            "bdy-b7,1,acct-A,2026-10-16T12:00:00Z,2026-11-15T07:00:00Z,700,false",
            "bdy-b7,2,acct-A,2026-11-15T10:00:00Z,2026-11-15T11:00:00Z,700,false",
            # correction moves occurred_at OUT of the window
            "bdy-b8,1,acct-A,2026-11-15T10:00:00Z,2026-11-15T09:00:00Z,800,false",
            "bdy-b8,2,acct-A,2026-10-16T12:00:00Z,2026-11-15T11:00:00Z,800,false",
        ]
        samples = [
            "bdy-t1,acct-A,2026-11-15T12:00:00Z",
            "bdy-t2,acct-B,2026-11-15T12:00:00Z",
            "bdy-t3,acct-NOBODY,2026-11-15T12:00:00Z",
        ]
        with TestClient(app) as client:
            r = post_csv(client, "/payment-revision-batches", REV_HEADER + "\n".join(revs) + "\n")
            self.assertEqual(r.status_code, 201, r.text)
            self.assertEqual(r.json()["rows_inserted"], len(revs))
            r = post_csv(client, "/sample-batches", SMP_HEADER + "\n".join(samples) + "\n")
            self.assertEqual(r.status_code, 201, r.text)
            run = client.post("/training-runs").json()
            final = wait_run(client, run["run_id"])
            self.assertEqual(final["status"], "completed", final)
            rows = {r["sample_id"]: r for r in dataset_rows(client, run["run_id"])}
        # acct-A: b1(100) + b3(300) + b7(700) = 1100, 3 payments
        self.assertEqual(rows["bdy-t1"]["amount_last_24h_cents"], "1100")
        self.assertEqual(rows["bdy-t1"]["payment_count"], "3")
        # acct-B: only b6 after correction
        self.assertEqual(rows["bdy-t2"]["amount_last_24h_cents"], "600")
        self.assertEqual(rows["bdy-t2"]["payment_count"], "1")
        # unknown account: zero row still present
        self.assertEqual(rows["bdy-t3"]["amount_last_24h_cents"], "0")
        self.assertEqual(rows["bdy-t3"]["payment_count"], "0")

    def test_02_idempotent_reuse_and_conflict(self):
        body = REV_HEADER + (
            "idem-p1,1,acct-A,2026-11-15T11:00:00Z,2026-11-15T10:00:00Z,1000,false\n"
            "idem-p2,1,acct-A,2026-11-15T11:00:00Z,2026-11-15T10:00:00Z,2000,false\n"
        )
        with TestClient(app) as client:
            r1 = post_csv(client, "/payment-revision-batches", body)
            self.assertEqual(r1.status_code, 201, r1.text)
            self.assertEqual((r1.json()["rows_inserted"], r1.json()["rows_reused"]), (2, 0))
            # identical re-upload -> everything reused
            r2 = post_csv(client, "/payment-revision-batches", body)
            self.assertEqual(r2.status_code, 201, r2.text)
            self.assertEqual((r2.json()["rows_inserted"], r2.json()["rows_reused"]), (0, 2))
            # same key, different content -> 409, whole batch rejected
            conflict = REV_HEADER + (
                "idem-p1,1,acct-A,2026-11-15T11:00:00Z,2026-11-15T10:00:00Z,9999,false\n"
                "idem-p3,1,acct-A,2026-11-15T11:00:00Z,2026-11-15T10:00:00Z,5,false\n"
            )
            r3 = post_csv(client, "/payment-revision-batches", conflict)
            self.assertEqual(r3.status_code, 409, r3.text)
            stats = client.get("/stats").json()
            # idem-p3 must not exist: batch rolled back
            self.assertEqual(stats["payment_revisions"], 12 + 2)  # 12 boundary + 2 here
            # malformed CSV -> 400
            bad = SMP_HEADER + "sx,acct-A,not-a-timestamp\n"
            r4 = post_csv(client, "/sample-batches", bad)
            self.assertEqual(r4.status_code, 400, r4.text)

    def test_03_watermark_excludes_concurrent_uploads(self):
        """Batches uploaded while a run is in progress belong to the next run."""
        smp1 = SMP_HEADER + "".join(
            f"wm-a{i:04d},acct-A,2026-11-15T12:00:00Z\n" for i in range(3000)
        )
        smp2 = SMP_HEADER + "".join(
            f"wm-b{i:03d},acct-A,2026-11-15T12:00:00Z\n" for i in range(40)
        )
        with TestClient(app) as client:
            before = client.get("/stats").json()["samples"]
            self.assertEqual(post_csv(client, "/sample-batches", smp1).status_code, 201)
            run = client.post("/training-runs").json()
            self.assertEqual(run["total_samples"], before + 3000)
            run_id = run["run_id"]
            # upload while the run is active -> accepted but excluded
            self.assertEqual(post_csv(client, "/sample-batches", smp2).status_code, 201)
            active = client.post("/training-runs")
            self.assertIn(active.status_code, (409, 201))
            if active.status_code == 201:
                # run already finished (fast machine): the "second" run is a
                # new run and must contain the late batch instead
                second_run_id = active.json()["run_id"]
                wait_run(client, second_run_id)
                rows2 = dataset_rows(client, second_run_id)
                self.assertEqual(sum(r["sample_id"].startswith("wm-b") for r in rows2), 40)
            # dataset gated until completion
            final = wait_run(client, run_id)
            self.assertEqual(final["status"], "completed")
            self.assertEqual(final["total_samples"], before + 3000)
            rows = dataset_rows(client, run_id)
            self.assertEqual(sum(r["sample_id"].startswith("wm-a") for r in rows), 3000)
            self.assertEqual(sum(r["sample_id"].startswith("wm-b") for r in rows), 0)
            # next run picks up the late batch
            run2 = client.post("/training-runs").json()
            if run2.get("status") == "pending" or "run_id" in run2:
                final2 = wait_run(client, run2["run_id"])
                self.assertEqual(final2["status"], "completed")
                rows2 = dataset_rows(client, run2["run_id"])
                self.assertEqual(
                    sum(r["sample_id"].startswith("wm-b") for r in rows2), 40
                )

    def test_04_download_gating(self):
        with TestClient(app) as client:
            smp = SMP_HEADER + "".join(
                f"gate-{i:04d},acct-A,2026-11-15T12:00:00Z\n" for i in range(2000)
            )
            self.assertEqual(post_csv(client, "/sample-batches", smp).status_code, 201)
            run = client.post("/training-runs").json()
            run_id = run["run_id"]
            # while pending/running the dataset is not downloadable
            early = client.get(f"/training-runs/{run_id}/dataset.csv")
            if early.status_code != 200:
                self.assertEqual(early.status_code, 409)
            final = wait_run(client, run_id)
            self.assertEqual(final["status"], "completed")
            rows = dataset_rows(client, run_id)
            self.assertEqual(sum(r["sample_id"].startswith("gate-") for r in rows), 2000)

    def test_05_restart_resume_graceful_and_crash(self):
        """Graceful stop mid-run resumes; stale 'running' state is re-queued."""
        smp = SMP_HEADER + "".join(
            f"rs-{i:05d},acct-A,2026-11-15T12:00:00Z\n" for i in range(5000)
        )
        with TestClient(app) as client:
            self.assertEqual(post_csv(client, "/sample-batches", smp).status_code, 201)
            run = client.post("/training-runs").json()
            run_id, total = run["run_id"], run["total_samples"]
            # wait until partially done
            deadline = time.time() + 60
            done = 0
            while time.time() < deadline:
                body = client.get(f"/training-runs/{run_id}").json()
                done = body["done_samples"]
                if 0 < done < total:
                    break
                if body["status"] == "completed":
                    self.fail("run finished before we could observe partial progress")
                time.sleep(0.1)
            self.assertGreater(done, 0)
            self.assertLess(done, total)
        # exiting the client = graceful shutdown (like `docker compose restart`)
        with psycopg.connect(os.environ["DATABASE_URL"], autocommit=True) as conn:
            status, persisted = conn.execute(
                "SELECT status, done_samples FROM training_runs WHERE run_id = %s",
                (run_id,),
            ).fetchone()
            self.assertEqual(status, "pending")  # handed back for resume
            self.assertGreaterEqual(persisted, done)
            # simulate a hard crash: run claimed by a dead worker
            conn.execute(
                "UPDATE training_runs SET status = 'running' WHERE run_id = %s", (run_id,)
            )
        with TestClient(app) as client2:
            body = None
            deadline = time.time() + 30
            while time.time() < deadline:
                body = client2.get(f"/training-runs/{run_id}").json()
                if body["status"] in ("running", "completed"):
                    break
                time.sleep(0.2)
            self.assertIn(body["status"], ("running", "completed"))
            self.assertGreaterEqual(body["done_samples"], persisted)  # no regression
            final = wait_run(client2, run_id)
            self.assertEqual(final["status"], "completed")
            self.assertEqual(final["done_samples"], total)
            rows = dataset_rows(client2, run_id)
            self.assertEqual(len(rows), total)
            self.assertEqual(len({r["sample_id"] for r in rows}), total)  # no duplicates
            self.assertEqual(sum(r["sample_id"].startswith("rs-") for r in rows), 5000)


class ReuploadE2E(unittest.TestCase):
    """The re-upload endpoint: per-payment rejection, persisted receipts,
    correction CSV, and atomicity against the original endpoint."""

    @classmethod
    def setUpClass(cls) -> None:
        make_db()

    def _db_revision_rows(self) -> list[tuple]:
        with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
            return conn.execute(
                "SELECT payment_id, revision_id, amount_cents "
                "FROM payment_revisions ORDER BY payment_id, revision_id"
            ).fetchall()

    def test_01_mixed_batch_rejects_only_conflicting_payments(self):
        base = REV_HEADER + (
            "ru-p1,1,acct-A,2026-11-15T11:00:00Z,2026-11-15T10:00:00Z,1000,false\n"
            "ru-p2,1,acct-A,2026-11-15T11:00:00Z,2026-11-15T10:00:00Z,2000,false\n"
        )
        reup_lines = [
            # db conflict: ru-p1#1 stored with amount 1000
            "ru-p1,1,acct-A,2026-11-15T11:00:00Z,2026-11-15T10:00:00Z,9999,false",
            # same payment, different revision -> rejected together with it
            "ru-p1,2,acct-A,2026-11-15T11:30:00Z,2026-11-15T11:20:00Z,1000,false",
            # identical to the stored row -> reused
            "ru-p2,1,acct-A,2026-11-15T11:00:00Z,2026-11-15T10:00:00Z,2000,false",
            # brand new -> inserted
            "ru-p3,1,acct-A,2026-11-15T11:00:00Z,2026-11-15T10:00:00Z,3000,false",
            # in-file same key, different values -> whole payment rejected
            "ru-p4,1,acct-A,2026-11-15T09:00:00Z,2026-11-15T09:30:00Z,400,false",
            "ru-p4,1,acct-A,2026-11-15T09:00:00Z,2026-11-15T09:30:00Z,401,false",
            "ru-p4,2,acct-A,2026-11-15T09:10:00Z,2026-11-15T09:40:00Z,400,false",
        ]
        with TestClient(app) as client:
            r = post_csv(client, "/payment-revision-batches", base)
            self.assertEqual(r.status_code, 201, r.text)

            r = post_csv(client, "/payment-revision-reuploads",
                         REV_HEADER + "\n".join(reup_lines) + "\n")
            self.assertEqual(r.status_code, 201, r.text)
            body = r.json()
            self.assertEqual(body["rows_total"], 7)
            self.assertEqual(body["rows_inserted"], 1)  # ru-p3#1
            self.assertEqual(body["rows_reused"], 1)  # ru-p2#1
            self.assertEqual(body["rows_rejected"], 5)  # ru-p1 x2 + ru-p4 x3
            self.assertEqual(
                body["rows_total"],
                body["rows_inserted"] + body["rows_reused"] + body["rows_rejected"],
            )
            self.assertEqual(sorted(body["rejected_payments"]), ["ru-p1", "ru-p4"])
            reasons = {c["key"]: c["reason"] for c in body["conflicts"]}
            self.assertEqual(
                reasons, {"ru-p1#1": "db_conflict", "ru-p4#1": "in_file_conflict"}
            )
            db_conflict = next(c for c in body["conflicts"] if c["reason"] == "db_conflict")
            self.assertEqual(db_conflict["existing"]["amount_cents"], 1000)
            self.assertEqual(db_conflict["incoming"]["amount_cents"], 9999)
            batch_id = body["batch_id"]
            self.assertEqual(
                body["rejected_csv_url"],
                f"/payment-revision-reuploads/{batch_id}/rejected.csv",
            )

            # receipt is persisted and queryable by batch id
            g = client.get(f"/payment-revision-reuploads/{batch_id}")
            self.assertEqual(g.status_code, 200, g.text)
            self.assertEqual(g.json()["rows_rejected"], 5)
            self.assertEqual(g.json()["conflicts"], body["conflicts"])
            listing = client.get("/payment-revision-reuploads").json()
            self.assertEqual([b["batch_id"] for b in listing], [batch_id])
            self.assertEqual(listing[0]["rejected_payments"], ["ru-p1", "ru-p4"])

            # correction CSV keeps ALL source lines of the rejected payments,
            # in original file order, and matches the receipt's embedded copy
            expected_csv = REV_HEADER + "\n".join(
                reup_lines[i] for i in (0, 1, 4, 5, 6)
            ) + "\n"
            self.assertEqual(body["rejected_csv"], expected_csv)
            c = client.get(f"/payment-revision-reuploads/{batch_id}/rejected.csv")
            self.assertEqual(c.status_code, 200, c.text)
            self.assertEqual(c.text, expected_csv)
            type(self).rejected_csv_text = c.text

            # good payments stored, rejected payments fully absent
            self.assertEqual(
                self._db_revision_rows(),
                [
                    ("ru-p1", 1, 1000),
                    ("ru-p2", 1, 2000),
                    ("ru-p3", 1, 3000),
                ],
            )

    def test_02_format_error_rejects_whole_batch_and_leaves_no_receipt(self):
        with TestClient(app) as client:
            before = client.get("/payment-revision-reuploads").json()
            stats_before = client.get("/stats").json()
            bad = REV_HEADER + (
                "fe-ok,1,acct-A,2026-11-15T11:00:00Z,2026-11-15T10:00:00Z,5,false\n"
                "fe-bad,1,acct-A,not-a-timestamp,2026-11-15T10:00:00Z,5,false\n"
            )
            r = post_csv(client, "/payment-revision-reuploads", bad)
            self.assertEqual(r.status_code, 400, r.text)
            # no receipt, no rows: the whole batch was rejected
            self.assertEqual(client.get("/payment-revision-reuploads").json(), before)
            self.assertEqual(client.get("/stats").json(), stats_before)

    def test_03_correction_cycle_and_idempotent_reupload(self):
        """Fix the rejected CSV from test_01, re-upload: accepted; the same
        file again is fully reused. Unknown receipt ids are 404."""
        rejected = self.rejected_csv_text.splitlines()
        header, lines = rejected[0], rejected[1:]
        fixed, seen_p4 = [], False
        for line in lines:
            if line.startswith("ru-p1,1,"):
                line = line.replace(",9999,", ",1000,")  # match the stored row
            if line.startswith("ru-p4,1,"):
                if seen_p4:
                    continue  # drop the conflicting duplicate
                seen_p4 = True
            fixed.append(line)
        fixed_csv = header + "\n" + "\n".join(fixed) + "\n"
        with TestClient(app) as client:
            r = post_csv(client, "/payment-revision-reuploads", fixed_csv)
            self.assertEqual(r.status_code, 201, r.text)
            body = r.json()
            self.assertEqual(
                (body["rows_total"], body["rows_inserted"],
                 body["rows_reused"], body["rows_rejected"]),
                (4, 3, 1, 0),
            )
            self.assertEqual(body["rejected_payments"], [])
            self.assertEqual(body["conflicts"], [])
            self.assertEqual(body["rejected_csv"], REV_HEADER)

            # identical re-upload -> everything reused
            r2 = post_csv(client, "/payment-revision-reuploads", fixed_csv)
            self.assertEqual(r2.status_code, 201, r2.text)
            self.assertEqual(
                (r2.json()["rows_inserted"], r2.json()["rows_reused"]), (0, 4)
            )

            self.assertEqual(
                self._db_revision_rows(),
                [
                    ("ru-p1", 1, 1000),
                    ("ru-p1", 2, 1000),
                    ("ru-p2", 1, 2000),
                    ("ru-p3", 1, 3000),
                    ("ru-p4", 1, 400),
                    ("ru-p4", 2, 400),
                ],
            )
            self.assertEqual(
                client.get("/payment-revision-reuploads/999999").status_code, 404
            )
            self.assertEqual(
                client.get("/payment-revision-reuploads/999999/rejected.csv").status_code,
                404,
            )

    def test_04_concurrent_legacy_and_reupload_never_split_a_payment(self):
        """Race the two endpoints on the same payment: afterwards the
        payment's rows come entirely from ONE of the batches — never a mix,
        never a partial set."""
        pool = create_pool(os.environ["DATABASE_URL"], max_size=4)
        try:
            with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
                receipts_before = conn.execute(
                    "SELECT count(*) FROM revision_reupload_batches"
                ).fetchone()[0]
            for round_no in range(8):
                pid = f"cc-{round_no}"
                legacy_text = REV_HEADER + (
                    f"{pid},1,acct-L,2026-11-15T11:00:00Z,2026-11-15T10:00:00Z,100,false\n"
                    f"{pid},2,acct-L,2026-11-15T11:00:00Z,2026-11-15T10:00:00Z,200,false\n"
                )
                reup_text = REV_HEADER + (
                    f"{pid},1,acct-R,2026-11-15T11:00:00Z,2026-11-15T10:00:00Z,999,false\n"
                    f"{pid},3,acct-R,2026-11-15T11:00:00Z,2026-11-15T10:00:00Z,300,false\n"
                )
                rows, dups = parse_revision_csv(legacy_text)
                records = parse_revision_records(reup_text)
                barrier = threading.Barrier(2)
                outcome: dict = {}

                def run_legacy():
                    try:
                        with pool.connection() as conn:
                            barrier.wait(timeout=10)
                            import_revision_batch(conn, rows, dups)
                        outcome["legacy"] = "accepted"
                    except ConflictError:
                        outcome["legacy"] = "rejected"
                    except Exception as exc:  # pragma: no cover - failure aid
                        outcome["legacy_error"] = exc

                def run_reupload():
                    try:
                        with pool.connection() as conn:
                            barrier.wait(timeout=10)
                            outcome["receipt"] = import_revision_reupload(conn, records)
                    except Exception as exc:  # pragma: no cover - failure aid
                        outcome["reupload_error"] = exc

                threads = [
                    threading.Thread(target=run_legacy),
                    threading.Thread(target=run_reupload),
                ]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join(30)
                self.assertFalse(any(t.is_alive() for t in threads), "deadlock?")
                self.assertNotIn("legacy_error", outcome, outcome)
                self.assertNotIn("reupload_error", outcome, outcome)

                with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
                    got = conn.execute(
                        "SELECT revision_id, account_id, amount_cents "
                        "FROM payment_revisions WHERE payment_id = %s "
                        "ORDER BY revision_id",
                        (pid,),
                    ).fetchall()
                legacy_state = [(1, "acct-L", 100), (2, "acct-L", 200)]
                reupload_state = [(1, "acct-R", 999), (3, "acct-R", 300)]
                self.assertIn(
                    got,
                    (legacy_state, reupload_state),
                    f"payment {pid} split across concurrent batches: {got}",
                )
                receipt = outcome["receipt"]
                if got == legacy_state:
                    self.assertEqual(outcome["legacy"], "accepted")
                    self.assertEqual(receipt["rows_inserted"], 0)
                    self.assertEqual(receipt["rows_rejected"], 2)
                    self.assertEqual(receipt["rejected_payments"], [pid])
                else:
                    self.assertEqual(outcome["legacy"], "rejected")
                    self.assertEqual(receipt["rows_inserted"], 2)
                    self.assertEqual(receipt["rows_rejected"], 0)
            with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
                receipts_after = conn.execute(
                    "SELECT count(*) FROM revision_reupload_batches"
                ).fetchone()[0]
            # every re-upload committed exactly one receipt, win or lose
            self.assertEqual(receipts_after - receipts_before, 8)
        finally:
            pool.close()

    def test_05_rows_and_receipt_commit_or_vanish_together(self):
        """A failure after the import work but before commit leaves neither
        revision rows nor a receipt behind."""
        records = parse_revision_records(
            REV_HEADER
            + "atomic-p,1,acct-A,2026-11-15T11:00:00Z,2026-11-15T10:00:00Z,5,false\n"
        )
        pool = create_pool(os.environ["DATABASE_URL"], max_size=2)

        class Boom(Exception):
            pass

        try:
            with self.assertRaises(Boom):
                with pool.connection() as conn:
                    receipt = import_revision_reupload(conn, records)
                    raise Boom  # simulate a failure just before commit
            with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
                n_rows = conn.execute(
                    "SELECT count(*) FROM payment_revisions "
                    "WHERE payment_id = 'atomic-p'"
                ).fetchone()[0]
                n_receipts = conn.execute(
                    "SELECT count(*) FROM revision_reupload_batches "
                    "WHERE batch_id = %s",
                    (receipt["batch_id"],),
                ).fetchone()[0]
            self.assertEqual(n_rows, 0)
            self.assertEqual(n_receipts, 0)
        finally:
            pool.close()


if __name__ == "__main__":
    unittest.main()
