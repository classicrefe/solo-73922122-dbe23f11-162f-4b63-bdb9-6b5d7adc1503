"""CSV parsing and validation for payment-revision and sample uploads."""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import datetime, timezone

REVISION_COLUMNS = [
    "payment_id",
    "revision_id",
    "account_id",
    "occurred_at_utc",
    "visible_at_utc",
    "amount_cents",
    "revoked",
]

SAMPLE_COLUMNS = ["sample_id", "account_id", "prediction_at_utc"]

_TRUE_VALUES = {"true", "t", "1", "yes", "y"}
_FALSE_VALUES = {"false", "f", "0", "no", "n"}
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


class CsvError(ValueError):
    """Raised when an uploaded CSV is malformed. Message is client-facing."""


@dataclass(frozen=True)
class RevisionRow:
    payment_id: str
    revision_id: int
    account_id: str
    occurred_at: datetime
    visible_at: datetime
    amount_cents: int
    revoked: bool

    def content(self) -> tuple:
        return (
            self.account_id,
            self.occurred_at,
            self.visible_at,
            self.amount_cents,
            self.revoked,
        )


@dataclass(frozen=True)
class SampleRow:
    sample_id: str
    account_id: str
    prediction_at: datetime

    def content(self) -> tuple:
        return (self.account_id, self.prediction_at)


def _parse_int(value: str, field: str, line: int) -> int:
    v = value.strip()
    try:
        return int(v, 10)
    except ValueError:
        raise CsvError(f"line {line}: field '{field}' must be an integer, got {value!r}")


def _parse_bool(value: str, field: str, line: int) -> bool:
    v = value.strip().lower()
    if v in _TRUE_VALUES:
        return True
    if v in _FALSE_VALUES:
        return False
    raise CsvError(
        f"line {line}: field '{field}' must be a boolean "
        f"(true/false/1/0/yes/no), got {value!r}"
    )


def _parse_ts(value: str, field: str, line: int) -> datetime:
    v = value.strip()
    try:
        dt = datetime.fromisoformat(v)
    except ValueError:
        raise CsvError(
            f"line {line}: field '{field}' must be an ISO 8601 timestamp, got {value!r}"
        )
    if dt.tzinfo is None:
        raise CsvError(
            f"line {line}: field '{field}' must include an explicit timezone "
            f"(e.g. '2026-11-15T08:30:00Z' or '+08:00' offset), got {value!r}"
        )
    return dt.astimezone(timezone.utc)


def _parse_text(value: str | None, field: str, line: int) -> str:
    v = (value or "").strip()
    if not v:
        raise CsvError(f"line {line}: field '{field}' must not be empty")
    return v


def _reader(text: str, required: list[str]) -> csv.DictReader:
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        raise CsvError("empty CSV: header row is required")
    fields = [f.strip() for f in reader.fieldnames]
    missing = [c for c in required if c not in fields]
    if missing:
        raise CsvError(
            f"missing required column(s): {', '.join(missing)}; "
            f"expected columns: {', '.join(required)}"
        )
    return reader


def parse_revision_csv(text: str) -> tuple[list[RevisionRow], int]:
    """Parse a payment-revision CSV.

    Returns (rows, in_file_duplicates). Rows are deduplicated by
    (payment_id, revision_id); identical duplicates collapse, conflicting
    duplicates reject the whole file.
    """
    reader = _reader(text, REVISION_COLUMNS)
    rows: list[RevisionRow] = []
    seen: dict[tuple[str, int], RevisionRow] = {}
    in_file_dups = 0
    for raw in reader:
        line = reader.line_num
        payment_id = _parse_text(raw.get("payment_id"), "payment_id", line)
        revision_id = _parse_int(raw.get("revision_id", ""), "revision_id", line)
        if revision_id < 0:
            raise CsvError(f"line {line}: field 'revision_id' must be >= 0")
        amount_cents = _parse_int(raw.get("amount_cents", ""), "amount_cents", line)
        if not (_INT64_MIN <= amount_cents <= _INT64_MAX):
            raise CsvError(f"line {line}: field 'amount_cents' out of int64 range")
        row = RevisionRow(
            payment_id=payment_id,
            revision_id=revision_id,
            account_id=_parse_text(raw.get("account_id"), "account_id", line),
            occurred_at=_parse_ts(raw.get("occurred_at_utc", ""), "occurred_at_utc", line),
            visible_at=_parse_ts(raw.get("visible_at_utc", ""), "visible_at_utc", line),
            amount_cents=amount_cents,
            revoked=_parse_bool(raw.get("revoked", ""), "revoked", line),
        )
        key = (row.payment_id, row.revision_id)
        prev = seen.get(key)
        if prev is not None:
            if prev.content() != row.content():
                raise CsvError(
                    f"line {line}: conflicting duplicate for "
                    f"payment_id={payment_id!r} revision_id={revision_id} "
                    f"within this file"
                )
            in_file_dups += 1
            continue
        seen[key] = row
        rows.append(row)
    return rows, in_file_dups


def parse_sample_csv(text: str) -> tuple[list[SampleRow], int]:
    """Parse a prediction-sample CSV. Same dedupe rules as revisions."""
    reader = _reader(text, SAMPLE_COLUMNS)
    rows: list[SampleRow] = []
    seen: dict[str, SampleRow] = {}
    in_file_dups = 0
    for raw in reader:
        line = reader.line_num
        row = SampleRow(
            sample_id=_parse_text(raw.get("sample_id"), "sample_id", line),
            account_id=_parse_text(raw.get("account_id"), "account_id", line),
            prediction_at=_parse_ts(
                raw.get("prediction_at_utc", ""), "prediction_at_utc", line
            ),
        )
        prev = seen.get(row.sample_id)
        if prev is not None:
            if prev.content() != row.content():
                raise CsvError(
                    f"line {line}: conflicting duplicate for "
                    f"sample_id={row.sample_id!r} within this file"
                )
            in_file_dups += 1
            continue
        seen[row.sample_id] = row
        rows.append(row)
    return rows, in_file_dups
