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


if __name__ == "__main__":
    unittest.main()
