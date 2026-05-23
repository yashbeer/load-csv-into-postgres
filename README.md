# load-csv-into-postgres

A single-file Python script that streams a huge CSV into Postgres with
**crash-safe resume**. If the load dies halfway — network blip, OOM,
`kill -9`, laptop lid closed — just re-run the same command. It picks up
from the last committed batch with zero duplicates and zero gaps.

Built for a ~17 GB Apollo lead export into [Neon](https://neon.tech),
but the pattern works for any large CSV into any Postgres.

## Why this exists

The usual options for "load a big CSV into Postgres" all have a sharp
edge:

- `psql \copy` — fast, but if it dies at 80% you start over.
- `pgloader` — powerful, but a heavy dependency and a fiddly config
  file for a one-shot job.
- A custom Python loop with `INSERT` — slow, and rarely resumable
  *correctly* (people forget the checkpoint and the COPY have to be in
  the same transaction).

This script is ~300 lines, has one dependency (`psycopg2`), and gets the
resume semantics right.

## Quick start

```bash
pip install psycopg2-binary

# any Postgres works; Neon is what this was built against
export DATABASE_URL='postgresql://user:pass@host/db?sslmode=require'

# one-time table + checkpoint table setup
psql "$DATABASE_URL" -f schema.sql

# load
python load_leads.py --csv input.csv
```

Crashed? Re-run the exact same command. It resumes.

```
Resuming: delimiter='\t', byte offset 12,884,901,888/17,853,190,760 (72.17%),
          row 41,210,034, prior ok=41,209,000 bad=1,034
```

## How resume works

Every batch does two things in **one transaction**:

1. `COPY` the batch of rows into `leads`.
2. `UPDATE migration_state` with the new byte offset and row number.

Because both are in the same transaction, the outcome is atomic:

- Commit succeeds → rows are visible *and* the checkpoint moved.
- Anything goes wrong → nothing is visible *and* the checkpoint is
  unchanged.

On restart, the loader reads `last_byte_offset` from `migration_state`
and `seek()`s the file there. There is no "did we already insert this?"
guesswork because the database itself is the source of truth for where
we got to.

## What it handles

- **Tab- or comma-delimited** — auto-detected from the header line.
- **Reordered / extra columns** — columns are matched by name against
  the expected header, so source files with extra junk columns are
  fine.
- **Bad rows** — wrong column count, unparseable confidence, bad
  timestamp, CSV parse errors → written to `rejected_rows.csv` with
  the row number and reason. The load does not stop.
- **Concurrent runs on the same file** — a Postgres advisory lock
  (`pg_try_advisory_lock`) prevents two loaders fighting over the same
  CSV.
- **Progress** — logs row count, % complete, MB/s, and ETA every ~5s.

## Flags

```
--csv PATH          Input CSV (required)
--rejected PATH     Where to write bad rows (default: rejected_rows.csv)
--batch-rows N      Rows per COPY batch (default: 10,000)
--reset             Discard the checkpoint and start over.
                    Does NOT delete already-loaded rows — you usually want
                    to TRUNCATE leads yourself before using this.
```

## Schema

The target table and checkpoint table are in [`schema.sql`](schema.sql).
The script is currently hard-coded for an Apollo-style lead schema (10
columns, see `EXPECTED_HEADER` and `COPY_SQL` in `load_leads.py`).
Adapting it to a different schema is mostly: change those two
constants and the validators in `validate_and_clean()`.

Build indexes **after** the load finishes — indexing during `COPY` is
5–10× slower. `schema.sql` lists the suggested `CREATE INDEX
CONCURRENTLY` statements at the bottom.

## Assumptions / limitations

- **No embedded newlines inside quoted CSV fields.** Resume is by
  byte offset, so a `"foo\nbar"` field would throw off the line
  counter on restart. Apollo-style exports don't have these. If yours
  might, switch resume to row-number-based (skip N lines instead of
  `seek()`).
- **One file per `migration_state` row** — keyed by absolute path. If
  you move the file, the loader treats it as new.
- **The schema is currently lead-specific.** PRs welcome to make the
  column mapping config-driven.

## License

MIT. Do whatever you want.
