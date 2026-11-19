"""FastAPI application: payment-revision intake, sample intake, training runs."""

from __future__ import annotations

import csv
import io
import logging
import os
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from psycopg_pool import ConnectionPool
from pydantic import BaseModel

from .csvio import CsvError, parse_revision_csv, parse_sample_csv
from .db import create_pool, init_schema
from .imports import (
    ACCEPTANCE_LOCK_KEY,
    ConflictError,
    import_revision_batch,
    import_sample_batch,
)
from .runner import RunWorker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("api")

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://app:app@localhost:5432/appdb"
)
RUN_CHUNK_SIZE = int(os.environ.get("RUN_CHUNK_SIZE", "200"))
RUN_CHUNK_DELAY_MS = int(os.environ.get("RUN_CHUNK_DELAY_MS", "0"))
RUNNER_IDLE_SLEEP_MS = int(os.environ.get("RUNNER_IDLE_SLEEP_MS", "500"))
MAX_CSV_BYTES = int(os.environ.get("MAX_CSV_BYTES", str(64 * 1024 * 1024)))


class BatchResult(BaseModel):
    batch_id: int
    rows_total: int
    rows_inserted: int
    rows_reused: int


class RunCreated(BaseModel):
    run_id: int
    status: str
    watermark_seq: int
    total_samples: int
    created_at: datetime


class RunStatus(BaseModel):
    run_id: int
    status: str
    watermark_seq: int
    total_samples: int
    done_samples: int
    created_at: datetime
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    error: Optional[str]


def _utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = create_pool(DATABASE_URL)
    init_schema(pool)
    stop_event = threading.Event()
    worker = RunWorker(
        pool,
        stop_event,
        chunk_size=RUN_CHUNK_SIZE,
        chunk_delay_s=RUN_CHUNK_DELAY_MS / 1000.0,
        idle_sleep_s=RUNNER_IDLE_SLEEP_MS / 1000.0,
    )
    worker.start()
    app.state.pool = pool
    app.state.worker = worker
    log.info("service started (chunk_size=%d)", RUN_CHUNK_SIZE)
    try:
        yield
    finally:
        stop_event.set()
        worker.join(timeout=15)
        pool.close()
        log.info("service stopped")


app = FastAPI(
    title="Point-in-time Payment Feature Builder",
    version="1.0.0",
    description=(
        "Rebuilds the last-24h payment amount per sample exactly as knowable "
        "at prediction time, so late-arriving corrections never leak the "
        "future into training data. Upload payment-revision CSVs and "
        "prediction-sample CSVs, then create a training run and download the "
        "feature dataset once the run is complete."
    ),
    lifespan=lifespan,
)


def get_pool(request: Request) -> ConnectionPool:
    return request.app.state.pool


# ---------------------------------------------------------------- uploads


async def _read_csv_body(request: Request) -> str:
    """Read the CSV payload either as raw body or as multipart file field."""
    ctype = request.headers.get("content-type", "")
    if ctype.startswith("multipart/form-data"):
        form = await request.form()
        upload = form.get("file")
        if upload is None or not hasattr(upload, "read"):
            raise HTTPException(
                status_code=400,
                detail="multipart form must contain a 'file' field with the CSV",
            )
        raw = await upload.read()
    else:
        raw = await request.body()
    if not raw:
        raise HTTPException(status_code=400, detail="empty request body: CSV expected")
    if len(raw) > MAX_CSV_BYTES:
        raise HTTPException(
            status_code=413, detail=f"CSV too large (limit {MAX_CSV_BYTES} bytes)"
        )
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="body must be UTF-8 encoded CSV")


# Documents the upload body in /docs without letting FastAPI consume the
# stream (the endpoint reads it manually to support both raw and multipart).
_UPLOAD_OPENAPI_EXTRA = {
    "requestBody": {
        "required": True,
        "content": {
            "text/csv": {
                "schema": {"type": "string", "format": "binary"},
            },
            "multipart/form-data": {
                "schema": {
                    "type": "object",
                    "properties": {"file": {"type": "string", "format": "binary"}},
                    "required": ["file"],
                }
            },
        },
    }
}


@app.post(
    "/payment-revision-batches",
    status_code=201,
    response_model=BatchResult,
    summary="Import a payment-revision CSV batch",
    description=(
        "Columns: `payment_id,revision_id,account_id,occurred_at_utc,"
        "visible_at_utc,amount_cents,revoked`. Idempotent: a key "
        "(payment_id, revision_id) re-uploaded with identical content is "
        "reused; the same key with different content rejects the ENTIRE "
        "batch (409, nothing stored). Send the CSV as raw body "
        "(`Content-Type: text/csv`) or as multipart file."
    ),
    openapi_extra=_UPLOAD_OPENAPI_EXTRA,
)
async def upload_revision_batch(request: Request):
    text = await _read_csv_body(request)
    try:
        rows, in_file_dups = parse_revision_csv(text)
    except CsvError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    pool = get_pool(request)
    try:
        with pool.connection() as conn:
            batch_id, inserted, reused = import_revision_batch(conn, rows, in_file_dups)
    except ConflictError as exc:
        return JSONResponse(
            status_code=409,
            content={"detail": str(exc), "conflicts": exc.conflicts[:20]},
        )
    return BatchResult(
        batch_id=batch_id,
        rows_total=len(rows) + in_file_dups,
        rows_inserted=inserted,
        rows_reused=reused,
    )


@app.post(
    "/sample-batches",
    status_code=201,
    response_model=BatchResult,
    summary="Import a prediction-sample CSV batch",
    description=(
        "Columns: `sample_id,account_id,prediction_at_utc`. Idempotent per "
        "sample_id (identical re-upload is reused, conflicting content "
        "rejects the entire batch with 409). Batches accepted while a "
        "training run is in progress belong to the NEXT run."
    ),
    openapi_extra=_UPLOAD_OPENAPI_EXTRA,
)
async def upload_sample_batch(request: Request):
    text = await _read_csv_body(request)
    try:
        rows, in_file_dups = parse_sample_csv(text)
    except CsvError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    pool = get_pool(request)
    try:
        with pool.connection() as conn:
            batch_id, inserted, reused = import_sample_batch(conn, rows, in_file_dups)
    except ConflictError as exc:
        return JSONResponse(
            status_code=409,
            content={"detail": str(exc), "conflicts": exc.conflicts[:20]},
        )
    return BatchResult(
        batch_id=batch_id,
        rows_total=len(rows) + in_file_dups,
        rows_inserted=inserted,
        rows_reused=reused,
    )


# ---------------------------------------------------------------- runs


@app.post(
    "/training-runs",
    status_code=201,
    response_model=RunCreated,
    summary="Create a training run over all samples accepted so far",
    description=(
        "Freezes the acceptance watermark: exactly the sample batches "
        "committed before this call are included. Only one active "
        "(pending/running) run is allowed; 409 otherwise."
    ),
)
def create_training_run(request: Request):
    pool = get_pool(request)
    with pool.connection() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (ACCEPTANCE_LOCK_KEY,))
        active = conn.execute(
            "SELECT run_id, status FROM training_runs "
            "WHERE status IN ('pending', 'running') LIMIT 1"
        ).fetchone()
        if active:
            raise HTTPException(
                status_code=409,
                detail=f"training run {active[0]} is still {active[1]}; "
                "wait for it to finish before creating a new one",
            )
        watermark = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) FROM samples"
        ).fetchone()[0]
        total = conn.execute(
            "SELECT count(*) FROM samples WHERE seq <= %s", (watermark,)
        ).fetchone()[0]
        row = conn.execute(
            "INSERT INTO training_runs (watermark_seq, total_samples) "
            "VALUES (%s, %s) RETURNING run_id, status, created_at",
            (watermark, total),
        ).fetchone()
    return RunCreated(
        run_id=row[0],
        status=row[1],
        watermark_seq=watermark,
        total_samples=total,
        created_at=row[2],
    )


def _run_status_row(row) -> RunStatus:
    return RunStatus(
        run_id=row[0],
        status=row[1],
        watermark_seq=row[2],
        total_samples=row[3],
        done_samples=row[4],
        created_at=row[5],
        started_at=row[6],
        finished_at=row[7],
        error=row[8],
    )


_RUN_COLS = (
    "run_id, status, watermark_seq, total_samples, done_samples, "
    "created_at, started_at, finished_at, error"
)


@app.get("/training-runs", response_model=list[RunStatus], summary="List training runs")
def list_training_runs(request: Request):
    pool = get_pool(request)
    with pool.connection() as conn:
        rows = conn.execute(
            f"SELECT {_RUN_COLS} FROM training_runs ORDER BY run_id"
        ).fetchall()
    return [_run_status_row(r) for r in rows]


@app.get(
    "/training-runs/{run_id}",
    response_model=RunStatus,
    summary="Get training-run status and progress",
)
def get_training_run(run_id: int, request: Request):
    pool = get_pool(request)
    with pool.connection() as conn:
        row = conn.execute(
            f"SELECT {_RUN_COLS} FROM training_runs WHERE run_id = %s", (run_id,)
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"training run {run_id} not found")
    return _run_status_row(row)


@app.get(
    "/training-runs/{run_id}/dataset.csv",
    summary="Download the feature dataset CSV",
    description=(
        "Available only once the run is completed — every sample's amount "
        "and the payment revisions it used are settled before anything is "
        "served. Returns 409 while the run is pending/running/failed."
    ),
)
def download_dataset(run_id: int, request: Request):
    pool = get_pool(request)
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT status FROM training_runs WHERE run_id = %s", (run_id,)
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"training run {run_id} not found")
    if row[0] != "completed":
        raise HTTPException(
            status_code=409,
            detail=f"training run {run_id} is '{row[0]}'; the dataset is "
            "downloadable only after the run completes",
        )

    def generate():
        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator="\n")
        writer.writerow(
            [
                "sample_id",
                "account_id",
                "prediction_at_utc",
                "amount_last_24h_cents",
                "payment_count",
            ]
        )
        yield buf.getvalue()
        with pool.connection() as conn:
            with conn.cursor(name=f"dataset_{run_id}") as cur:
                cur.itersize = 1000
                cur.execute(
                    "SELECT sample_id, account_id, prediction_at, amount_cents, "
                    "payment_count FROM run_results WHERE run_id = %s "
                    "ORDER BY sample_seq",
                    (run_id,),
                )
                for sample_id, account_id, prediction_at, amount, count in cur:
                    buf.seek(0)
                    buf.truncate(0)
                    writer.writerow(
                        [sample_id, account_id, _utc(prediction_at), amount, count]
                    )
                    yield buf.getvalue()

    return StreamingResponse(
        generate(),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="training_run_{run_id}.csv"'
        },
    )


# ---------------------------------------------------------------- introspection


@app.get("/sample-batches", summary="List accepted sample batches")
def list_sample_batches(request: Request):
    pool = get_pool(request)
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT batch_id, accepted_at, rows_total, rows_inserted, rows_reused "
            "FROM sample_batches ORDER BY batch_id"
        ).fetchall()
    return [
        {
            "batch_id": r[0],
            "accepted_at": _utc(r[1]),
            "rows_total": r[2],
            "rows_inserted": r[3],
            "rows_reused": r[4],
        }
        for r in rows
    ]


@app.get("/payment-revision-batches", summary="List imported revision batches")
def list_revision_batches(request: Request):
    pool = get_pool(request)
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT batch_id, received_at, rows_total, rows_inserted, rows_reused "
            "FROM revision_batches ORDER BY batch_id"
        ).fetchall()
    return [
        {
            "batch_id": r[0],
            "received_at": _utc(r[1]),
            "rows_total": r[2],
            "rows_inserted": r[3],
            "rows_reused": r[4],
        }
        for r in rows
    ]


@app.get("/stats", summary="Row counts of the main tables")
def stats(request: Request):
    pool = get_pool(request)
    with pool.connection() as conn:
        out: dict[str, Any] = {}
        for table in (
            "payment_revisions",
            "samples",
            "revision_batches",
            "sample_batches",
            "training_runs",
            "run_results",
        ):
            out[table] = conn.execute(
                f"SELECT count(*) FROM {table}"  # noqa: S608 - fixed names
            ).fetchone()[0]
    return out


@app.get("/health", summary="Liveness/readiness probe")
def health(request: Request):
    pool = get_pool(request)
    with pool.connection() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/", summary="Service overview")
def root():
    return {
        "service": "point-in-time payment feature builder",
        "docs": "/docs",
        "endpoints": [
            "POST /payment-revision-batches",
            "POST /sample-batches",
            "POST /training-runs",
            "GET /training-runs",
            "GET /training-runs/{run_id}",
            "GET /training-runs/{run_id}/dataset.csv",
            "GET /sample-batches",
            "GET /payment-revision-batches",
            "GET /stats",
            "GET /health",
        ],
    }
