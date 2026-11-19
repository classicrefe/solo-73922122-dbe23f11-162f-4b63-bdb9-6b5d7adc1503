"""Unit tests for CSV parsing/validation (no server needed)."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.csvio import (
    CsvError,
    parse_revision_csv,
    parse_revision_records,
    parse_sample_csv,
)

REV_HEADER = (
    "payment_id,revision_id,account_id,occurred_at_utc,visible_at_utc,"
    "amount_cents,revoked\n"
)
REV_ROW = "p1,1,acct-A,2026-11-15T11:00:00Z,2026-11-15T10:00:00Z,1000,false\n"


class RevisionCsvTest(unittest.TestCase):
    def test_valid_row(self):
        rows, dups = parse_revision_csv(REV_HEADER + REV_ROW)
        self.assertEqual(len(rows), 1)
        self.assertEqual(dups, 0)
        r = rows[0]
        self.assertEqual((r.payment_id, r.revision_id, r.account_id), ("p1", 1, "acct-A"))
        self.assertEqual(r.amount_cents, 1000)
        self.assertFalse(r.revoked)
        self.assertEqual(r.occurred_at.isoformat(), "2026-11-15T11:00:00+00:00")

    def test_offset_timezone_normalized_to_utc(self):
        rows, _ = parse_revision_csv(
            REV_HEADER + "p1,1,a,2026-11-15T19:00:00+08:00,2026-11-15T10:00:00Z,5,false\n"
        )
        self.assertEqual(rows[0].occurred_at.isoformat(), "2026-11-15T11:00:00+00:00")

    def test_identical_duplicate_in_file_collapses(self):
        rows, dups = parse_revision_csv(REV_HEADER + REV_ROW + REV_ROW)
        self.assertEqual(len(rows), 1)
        self.assertEqual(dups, 1)

    def test_conflicting_duplicate_in_file_rejected(self):
        other = REV_ROW.replace(",1000,", ",1001,")
        with self.assertRaises(CsvError):
            parse_revision_csv(REV_HEADER + REV_ROW + other)

    def test_missing_column(self):
        with self.assertRaisesRegex(CsvError, "missing required column"):
            parse_revision_csv("payment_id,revision_id\np1,1\n")

    def test_empty_file(self):
        with self.assertRaisesRegex(CsvError, "header"):
            parse_revision_csv("")

    def test_bad_boolean(self):
        with self.assertRaisesRegex(CsvError, "boolean"):
            parse_revision_csv(REV_HEADER + REV_ROW.replace("false", "maybe"))

    def test_boolean_spellings(self):
        for token, expect in [("TRUE", True), ("0", False), ("yes", True), ("No", False)]:
            rows, _ = parse_revision_csv(REV_HEADER + REV_ROW.replace("false", token))
            self.assertEqual(rows[0].revoked, expect, token)

    def test_bad_integer(self):
        with self.assertRaisesRegex(CsvError, "integer"):
            parse_revision_csv(REV_HEADER + REV_ROW.replace(",1000,", ",10.5,"))

    def test_negative_amount_allowed(self):
        rows, _ = parse_revision_csv(REV_HEADER + REV_ROW.replace(",1000,", ",-250,"))
        self.assertEqual(rows[0].amount_cents, -250)

    def test_naive_timestamp_rejected(self):
        with self.assertRaisesRegex(CsvError, "timezone"):
            parse_revision_csv(
                REV_HEADER + REV_ROW.replace("2026-11-15T11:00:00Z", "2026-11-15 11:00:00")
            )

    def test_bad_revision_id(self):
        with self.assertRaisesRegex(CsvError, "integer"):
            parse_revision_csv(REV_HEADER + REV_ROW.replace("p1,1,", "p1,one,"))

    def test_negative_revision_id_rejected(self):
        with self.assertRaisesRegex(CsvError, ">= 0"):
            parse_revision_csv(REV_HEADER + REV_ROW.replace("p1,1,", "p1,-1,"))


class RevisionRecordsTest(unittest.TestCase):
    """parse_revision_records: keeps exact source lines, never raises on
    in-file key conflicts (that is the importer's job), still raises
    CsvError on any field-format problem."""

    def test_source_and_line_numbers_captured(self):
        row2 = REV_ROW.replace("p1", "p2")
        recs = parse_revision_records(REV_HEADER + REV_ROW + row2)
        self.assertEqual(len(recs), 2)
        self.assertEqual(recs[0].source, REV_ROW)
        self.assertEqual(recs[1].source, row2)
        self.assertEqual((recs[0].line_no, recs[1].line_no), (2, 3))
        self.assertEqual(recs[1].row.payment_id, "p2")

    def test_quoted_newline_source_spans_physical_lines(self):
        row = 'p1,1,"acct\nA",2026-11-15T11:00:00Z,2026-11-15T10:00:00Z,1000,false\n'
        recs = parse_revision_records(REV_HEADER + row)
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0].source, row)
        self.assertEqual(recs[0].row.account_id, "acct\nA")

    def test_last_line_without_trailing_newline(self):
        row = REV_ROW.rstrip("\n")
        recs = parse_revision_records(REV_HEADER + row)
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0].source, row)

    def test_blank_lines_skipped(self):
        recs = parse_revision_records(REV_HEADER + "\n" + REV_ROW + "\n\n")
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0].source, REV_ROW)
        self.assertEqual(recs[0].line_no, 3)

    def test_in_file_conflict_not_raised_by_parser(self):
        recs = parse_revision_records(
            REV_HEADER + REV_ROW + REV_ROW.replace(",1000,", ",1001,")
        )
        self.assertEqual(len(recs), 2)

    def test_short_row_is_csv_error_not_crash(self):
        short = REV_HEADER + "p1,1,acct-A\n"
        with self.assertRaises(CsvError):
            parse_revision_records(short)
        with self.assertRaises(CsvError):
            parse_revision_csv(short)
        with self.assertRaises(CsvError):
            parse_sample_csv("sample_id,account_id,prediction_at_utc\ns1\n")

    def test_field_errors_still_rejected(self):
        with self.assertRaisesRegex(CsvError, "integer"):
            parse_revision_records(REV_HEADER + REV_ROW.replace("p1,1,", "p1,x,"))
        with self.assertRaisesRegex(CsvError, "timezone"):
            parse_revision_records(
                REV_HEADER + REV_ROW.replace("2026-11-15T11:00:00Z", "2026-11-15")
            )


class SampleCsvTest(unittest.TestCase):
    HEADER = "sample_id,account_id,prediction_at_utc\n"
    ROW = "s1,acct-A,2026-11-15T12:00:00Z\n"

    def test_valid(self):
        rows, dups = parse_sample_csv(self.HEADER + self.ROW)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].sample_id, "s1")

    def test_conflicting_duplicate_in_file_rejected(self):
        with self.assertRaises(CsvError):
            parse_sample_csv(self.HEADER + self.ROW + "s1,acct-B,2026-11-15T12:00:00Z\n")

    def test_identical_duplicate_collapses(self):
        rows, dups = parse_sample_csv(self.HEADER + self.ROW + self.ROW)
        self.assertEqual((len(rows), dups), (1, 1))

    def test_empty_id_rejected(self):
        with self.assertRaisesRegex(CsvError, "empty"):
            parse_sample_csv(self.HEADER + ",acct-A,2026-11-15T12:00:00Z\n")


if __name__ == "__main__":
    unittest.main()
