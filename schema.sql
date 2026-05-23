-- Run this once in the Neon SQL shell before loading.

CREATE TABLE IF NOT EXISTS leads (
    id                       BIGSERIAL PRIMARY KEY,
    person_name              TEXT,
    person_title             TEXT,
    person_detailed_function TEXT,
    person_email             TEXT,
    person_email_confidence  REAL,
    email_domain             TEXT GENERATED ALWAYS AS (
        NULLIF(lower(split_part(person_email, '@', 2)), '')
    ) STORED,
    person_linkedin_url      TEXT,
    person_location_country  TEXT,
    organization_name        TEXT,
    modality                 TEXT,
    vacuumed_at              TIMESTAMPTZ
);

-- Checkpoint table: lets the loader resume from the exact byte offset
-- of the last successfully committed batch after a crash.
CREATE TABLE IF NOT EXISTS migration_state (
    file_path        TEXT PRIMARY KEY,
    last_byte_offset BIGINT      NOT NULL DEFAULT 0,
    last_row_number  BIGINT      NOT NULL DEFAULT 0,
    rows_inserted    BIGINT      NOT NULL DEFAULT 0,
    rows_rejected    BIGINT      NOT NULL DEFAULT 0,
    last_updated     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at     TIMESTAMPTZ
);

-- Build indexes AFTER the bulk load finishes — indexing during COPY
-- slows the load 5-10x. Suggested post-load:
--   CREATE INDEX CONCURRENTLY leads_email_idx        ON leads (person_email);
--   CREATE INDEX CONCURRENTLY leads_email_domain_idx ON leads (email_domain);
--   CREATE INDEX CONCURRENTLY leads_org_idx          ON leads (organization_name);
--   CREATE INDEX CONCURRENTLY leads_vacuumed_at_idx  ON leads (vacuumed_at);
