#!/usr/bin/env python3
"""
Stream a large CSV of lead data into Postgres, resumable on crash.

Reliability model
-----------------
Each batch is loaded with COPY and the checkpoint (last byte offset +
row number) is updated *in the same transaction*. Outcomes are atomic:
either the batch is fully visible AND the checkpoint advances, or
neither happens. So a crash, network blip, or kill -9 at any point is
safe: re-running the script resumes from the last committed offset
with zero double-inserts and zero gaps.

Bad rows (wrong column count, unparseable confidence, unparseable
timestamp, CSV parse errors) are written to rejected_rows.csv with the
row number and reason; the loader does not stop on bad rows.

Usage
-----
    export DATABASE_URL='postgresql://...'
    pip install psycopg2-binary
    python load_leads.py --csv input.csv

Re-run the same command after a crash to resume. Use --reset to
discard the checkpoint (does NOT delete already-loaded rows).

Assumption
----------
Resume uses byte offsets, which assumes no embedded newlines inside
quoted CSV values. This is safe for this dataset (lead fields don't
contain newlines). If that ever changes, switch to row-number resume.
"""

import argparse
import csv
import hashlib
import io
import os
import sys
import time
from datetime import datetime

import psycopg2

EXPECTED_HEADER = [
    "person_name", "person_title_normalized", "person_detailed_function",
    "person_email_analyzed", "person_extrapolated_email_confidence", "person_linkedin_url",
    "person_location_country", "sanitized_organization_name_unanalyzed",
    "modality", "person_vacuumed_at",
]

COPY_SQL = """
    COPY leads (
        person_name, person_title, person_detailed_function,
        person_email, person_email_confidence, person_linkedin_url,
        person_location_country, organization_name,
        modality, vacuumed_at
    ) FROM STDIN WITH (FORMAT CSV, NULL '')
"""


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True, help="Path to input CSV")
    p.add_argument("--rejected", default="rejected_rows.csv",
                   help="Path to write rejected rows (default: rejected_rows.csv)")
    p.add_argument("--batch-rows", type=int, default=10_000,
                   help="Rows per COPY batch (default: 10000)")
    p.add_argument("--reset", action="store_true",
                   help="Discard checkpoint and restart from row 0")
    return p.parse_args()


def get_conn():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        sys.exit("DATABASE_URL env var not set")
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    return conn


def acquire_lock(conn, file_path):
    """Prevent two loaders from running against the same file."""
    key = int(hashlib.md5(file_path.encode()).hexdigest()[:15], 16) & 0x7FFFFFFFFFFFFFFF
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (key,))
        got = cur.fetchone()[0]
    if not got:
        sys.exit(f"Another loader is already running for {file_path} (advisory lock held)")


def detect_delimiter(header_line):
    """Pick tab if the header has more tabs than commas, else comma."""
    return "\t" if header_line.count("\t") > header_line.count(",") else ","


def build_column_map(actual_header):
    """Return (indices_in_source_for_each_expected_col, missing_cols)."""
    indices = []
    missing = []
    for col in EXPECTED_HEADER:
        if col in actual_header:
            indices.append(actual_header.index(col))
        else:
            missing.append(col)
    return indices, missing


def validate_and_clean(row):
    """Return (cleaned_row, None) or (None, error_msg)."""
    if len(row) != 10:
        return None, f"expected 10 columns, got {len(row)}"

    fields = [c.strip() if c is not None else "" for c in row]
    name, title, func, email, conf, linkedin, country, org, modality, vacuumed = fields

    if conf:
        try:
            float(conf)
        except ValueError:
            return None, f"invalid confidence: {conf!r}"

    if vacuumed:
        try:
            datetime.fromisoformat(vacuumed.replace("Z", "+00:00"))
        except ValueError:
            return None, f"invalid timestamp: {vacuumed!r}"

    return [name, title, func, email, conf, linkedin, country, org, modality, vacuumed], None


def flush_batch(conn, batch, file_path, offset, row_number, total_ok, total_bad):
    """COPY rows + advance checkpoint atomically."""
    buf = io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
    for r in batch:
        writer.writerow(r)
    buf.seek(0)
    try:
        with conn.cursor() as cur:
            cur.copy_expert(COPY_SQL, buf)
            cur.execute(
                "UPDATE migration_state SET "
                "last_byte_offset=%s, last_row_number=%s, "
                "rows_inserted=%s, rows_rejected=%s, last_updated=NOW() "
                "WHERE file_path=%s",
                (offset, row_number, total_ok, total_bad, file_path),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def load_checkpoint(conn, file_path, reset):
    with conn.cursor() as cur:
        if reset:
            cur.execute("DELETE FROM migration_state WHERE file_path = %s", (file_path,))
        cur.execute(
            "SELECT last_byte_offset, last_row_number, rows_inserted, rows_rejected, completed_at "
            "FROM migration_state WHERE file_path = %s",
            (file_path,),
        )
        row = cur.fetchone()
        if row is None:
            cur.execute(
                "INSERT INTO migration_state (file_path) VALUES (%s)", (file_path,)
            )
            conn.commit()
            return 0, 0, 0, 0, None
        conn.commit()
        return row


def main():
    args = parse_args()
    abs_path = os.path.abspath(args.csv)
    if not os.path.exists(abs_path):
        sys.exit(f"CSV not found: {abs_path}")
    file_size = os.path.getsize(abs_path)

    conn = get_conn()
    acquire_lock(conn, abs_path)

    last_offset, last_row, total_ok, total_bad, completed_at = load_checkpoint(
        conn, abs_path, args.reset
    )
    if completed_at is not None and not args.reset:
        print(f"This file already completed at {completed_at}. Use --reset to re-run.")
        return

    rejected_mode = "a" if last_offset > 0 else "w"
    rejected_f = open(args.rejected, rejected_mode, newline="", encoding="utf-8")
    rejected_writer = csv.writer(rejected_f)
    if rejected_mode == "w":
        rejected_writer.writerow(["row_number", "reason", "raw_line"])

    f = open(abs_path, "rb")

    # Always read the header (even on resume) to detect delimiter + map columns.
    header_line = f.readline()
    header_text = header_line.decode("utf-8").rstrip("\r\n")
    delimiter = detect_delimiter(header_text)
    actual_header = [
        h.strip() for h in next(csv.reader(io.StringIO(header_text), delimiter=delimiter))
    ]
    column_indices, missing = build_column_map(actual_header)
    if missing:
        sys.exit(
            "Missing required columns in CSV.\n"
            f"  Missing:  {missing}\n"
            f"  Found ({len(actual_header)} cols): {actual_header}"
        )
    header_offset = f.tell()

    if last_offset == 0:
        last_row = 1
        last_offset = header_offset
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE migration_state SET last_byte_offset=%s, last_row_number=%s, "
                "last_updated=NOW() WHERE file_path=%s",
                (last_offset, last_row, abs_path),
            )
            conn.commit()
        print(
            f"Header verified. Delimiter={delimiter!r}, "
            f"{len(actual_header)} source columns, extracting {len(EXPECTED_HEADER)}. "
            f"Starting fresh load of {file_size:,} bytes."
        )
    else:
        f.seek(last_offset)
        print(
            f"Resuming: delimiter={delimiter!r}, byte offset {last_offset:,}/{file_size:,} "
            f"({100*last_offset/file_size:.2f}%), row {last_row:,}, "
            f"prior ok={total_ok:,} bad={total_bad:,}"
        )

    batch = []
    row_number = last_row
    start_time = time.time()
    last_log = start_time
    bytes_at_start = last_offset

    try:
        while True:
            line = f.readline()
            if not line:
                break

            row_number += 1
            text_line = line.decode("utf-8", errors="replace")
            try:
                parsed = next(csv.reader(io.StringIO(text_line), delimiter=delimiter))
            except Exception as e:
                rejected_writer.writerow([row_number, f"csv parse error: {e}", text_line.rstrip("\r\n")])
                rejected_f.flush()
                total_bad += 1
                continue

            try:
                extracted = [parsed[i] for i in column_indices]
            except IndexError:
                rejected_writer.writerow([
                    row_number,
                    f"row has {len(parsed)} cols, need at least {max(column_indices)+1}",
                    text_line.rstrip("\r\n"),
                ])
                rejected_f.flush()
                total_bad += 1
                continue

            cleaned, err = validate_and_clean(extracted)
            if err:
                rejected_writer.writerow([row_number, err, text_line.rstrip("\r\n")])
                rejected_f.flush()
                total_bad += 1
                continue

            batch.append(cleaned)

            if len(batch) >= args.batch_rows:
                offset_after = f.tell()
                flush_batch(
                    conn, batch, abs_path, offset_after, row_number,
                    total_ok + len(batch), total_bad,
                )
                total_ok += len(batch)
                batch = []

                now = time.time()
                if now - last_log >= 5:
                    elapsed = now - start_time
                    rate_mbps = (offset_after - bytes_at_start) / elapsed / 1e6 if elapsed else 0
                    pct = 100 * offset_after / file_size if file_size else 0
                    eta_s = (file_size - offset_after) / (rate_mbps * 1e6) if rate_mbps > 0 else 0
                    print(
                        f"row={row_number:,} offset={offset_after:,}/{file_size:,} "
                        f"({pct:.2f}%) ok={total_ok:,} bad={total_bad:,} "
                        f"rate={rate_mbps:.1f}MB/s eta={eta_s/60:.1f}min"
                    )
                    last_log = now

        if batch:
            offset_after = f.tell()
            flush_batch(
                conn, batch, abs_path, offset_after, row_number,
                total_ok + len(batch), total_bad,
            )
            total_ok += len(batch)

        with conn.cursor() as cur:
            cur.execute(
                "UPDATE migration_state SET completed_at=NOW(), last_updated=NOW() "
                "WHERE file_path=%s",
                (abs_path,),
            )
            conn.commit()

        elapsed = time.time() - start_time
        print(
            f"\nDone. inserted={total_ok:,} rejected={total_bad:,} "
            f"elapsed={elapsed/60:.1f}min"
        )
        if total_bad > 0:
            print(f"Rejected rows written to: {args.rejected}")

    finally:
        f.close()
        rejected_f.close()
        conn.close()


if __name__ == "__main__":
    main()
