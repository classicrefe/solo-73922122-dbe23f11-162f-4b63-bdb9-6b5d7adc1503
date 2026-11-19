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


@dataclass(frozen=True)
class RevisionRecord:
    """A parsed revision row plus its exact source text.

    The source text (which may span several physical lines when a quoted
    field embeds newlines) is what the re-upload receipt hands back to the
    channel in the correction CSV.
    """

    row: RevisionRow
    line_no: int
    source: str


@dataclass(frozen=True)
class _RawRecord:
    line_no: int  # 1-based number of the record's last physical line
    source: str  # exact source text of the record
    fields: dict[str, "str | None"]  # header name -> raw value (None if short)


def _parse_int(value: "str | None", field: str, line: int) -> int:
    v = (value or "").strip()
    try:
        return int(v, 10)
    except ValueError:
        raise CsvError(f"line {line}: field '{field}' must be an integer, got {value!r}")


def _parse_bool(value: "str | None", field: str, line: int) -> bool:
    v = (value or "").strip().lower()
    if v in _TRUE_VALUES:
        return True
    if v in _FALSE_VALUES:
        return False
    raise CsvError(
        f"line {line}: field '{field}' must be a boolean "
        f"(true/false/1/0/yes/no), got {value!r}"
    )


def _parse_ts(value: "str | None", field: str, line: int) -> datetime:
    v = (value or "").strip()
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


def _parse_text(value: "str | None", field: str, line: int) -> str:
    v = (value or "").strip()
    if not v:
        raise CsvError(f"line {line}: field '{field}' must not be empty")
    return v


def _read_records(text: str, required: list[str]) -> list[_RawRecord]:
    """Split the CSV into records, keeping each record's exact source text.

    Header names are matched after stripping surrounding whitespace. Blank
    lines are skipped (csv.DictReader semantics). Short rows yield None for
    the missing fields so the field parsers raise a clean CsvError instead
    of crashing.
    """
    stream = io.StringIO(text)
    reader = csv.reader(stream)
    try:
        header = next(reader)
    except StopIteration:
        raise CsvError("empty CSV: header row is required")
    pos = stream.tell()
    names = [f.strip() for f in header]
    missing = [c for c in required if c not in names]
    if missing:
        raise CsvError(
            f"missing required column(s): {', '.join(missing)}; "
            f"expected columns: {', '.join(required)}"
        )
    index: dict[str, int] = {}
    for i, name in enumerate(names):
        index[name] = i  # a duplicated header name maps to its last column
    records: list[_RawRecord] = []
    for rec in reader:
        cur = stream.tell()
        source = text[pos:cur]
        pos = cur
        if not rec:
            continue  # blank line
        fields = {
            name: (rec[i] if i < len(rec) else None) for name, i in index.items()
        }
        records.append(_RawRecord(line_no=reader.line_num, source=source, fields=fields))
    return records


def _revision_row(fields: dict[str, "str | None"], line: int) -> RevisionRow:
    payment_id = _parse_text(fields.get("payment_id"), "payment_id", line)
    revision_id = _parse_int(fields.get("revision_id"), "revision_id", line)
    if revision_id < 0:
        raise CsvError(f"line {line}: field 'revision_id' must be >= 0")
    amount_cents = _parse_int(fields.get("amount_cents"), "amount_cents", line)
    if not (_INT64_MIN <= amount_cents <= _INT64_MAX):
        raise CsvError(f"line {line}: field 'amount_cents' out of int64 range")
    return RevisionRow(
        payment_id=payment_id,
        revision_id=revision_id,
        account_id=_parse_text(fields.get("account_id"), "account_id", line),
        occurred_at=_parse_ts(fields.get("occurred_at_utc"), "occurred_at_utc", line),
        visible_at=_parse_ts(fields.get("visible_at_utc"), "visible_at_utc", line),
        amount_cents=amount_cents,
        revoked=_parse_bool(fields.get("revoked"), "revoked", line),
    )


def _sample_row(fields: dict[str, "str | None"], line: int) -> SampleRow:
    return SampleRow(
        sample_id=_parse_text(fields.get("sample_id"), "sample_id", line),
        account_id=_parse_text(fields.get("account_id"), "account_id", line),
        prediction_at=_parse_ts(fields.get("prediction_at_utc"), "prediction_at_utc", line),
    )


def parse_revision_csv(text: str) -> tuple[list[RevisionRow], int]:
    """Parse a payment-revision CSV.

    Returns (rows, in_file_duplicates). Rows are deduplicated by
    (payment_id, revision_id); identical duplicates collapse, conflicting
    duplicates reject the whole file.
    """
    rows: list[RevisionRow] = []
    seen: dict[tuple[str, int], RevisionRow] = {}
    in_file_dups = 0
    for rec in _read_records(text, REVISION_COLUMNS):
        row = _revision_row(rec.fields, rec.line_no)
        key = (row.payment_id, row.revision_id)
        prev = seen.get(key)
        if prev is not None:
            if prev.content() != row.content():
                raise CsvError(
                    f"line {rec.line_no}: conflicting duplicate for "
                    f"payment_id={row.payment_id!r} revision_id={row.revision_id} "
                    f"within this file"
                )
            in_file_dups += 1
            continue
        seen[key] = row
        rows.append(row)
    return rows, in_file_dups


def parse_revision_records(text: str) -> list[RevisionRecord]:
    """Parse every data line of a payment-revision CSV, keeping sources.

    Field-format problems raise CsvError (the caller rejects the whole
    batch). Unlike parse_revision_csv this neither deduplicates nor raises
    on in-file key conflicts: every data line is returned so the re-upload
    importer can reject whole payments and hand their source lines back to
    the channel for correction.
    """
    return [
        RevisionRecord(row=_revision_row(rec.fields, rec.line_no), line_no=rec.line_no, source=rec.source)
        for rec in _read_records(text, REVISION_COLUMNS)
    ]


def parse_sample_csv(text: str) -> tuple[list[SampleRow], int]:
    """Parse a prediction-sample CSV. Same dedupe rules as revisions."""
    rows: list[SampleRow] = []
    seen: dict[str, SampleRow] = {}
    in_file_dups = 0
    for rec in _read_records(text, SAMPLE_COLUMNS):
        row = _sample_row(rec.fields, rec.line_no)
        prev = seen.get(row.sample_id)
        if prev is not None:
            if prev.content() != row.content():
                raise CsvError(
                    f"line {rec.line_no}: conflicting duplicate for "
                    f"sample_id={row.sample_id!r} within this file"
                )
            in_file_dups += 1
            continue
        seen[row.sample_id] = row
        rows.append(row)
    return rows, in_file_dups
