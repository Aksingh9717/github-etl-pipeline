-- ============================================================
-- GITHUB ETL PROJECT — SQL SETUP SCRIPT
-- Run these queries in pgAdmin in order, top to bottom
-- Database: github_etl
-- ============================================================


-- ── STEP 1: CREATE WATERMARK TABLE ──────────────────────────
-- What is this?
--   A watermark is a bookmark. It stores the last date
--   your ETL successfully processed data up to.
--   Every run reads this date and only fetches newer records.
-- Why do we need it?
--   Without watermark, every run loads ALL data from scratch.
--   With watermark, each run loads ONLY what changed.

CREATE TABLE etl_watermark
(
    process_name    VARCHAR(100),
    last_updated_at TIMESTAMP
);

-- Insert the starting watermark
-- We use year 2000 so the very first run picks up everything
INSERT INTO etl_watermark VALUES ('github_issues', '2000-01-01');

-- Verify
SELECT * FROM etl_watermark;


-- ── STEP 2: CREATE STAGING TABLE ────────────────────────────
-- What is this?
--   A temporary holding area. Every ETL run:
--     1. Wipes this table clean (TRUNCATE)
--     2. Inserts only the new records
--   It acts as a buffer before data goes into the final table.
-- Why do we need it?
--   Staging lets us validate data before it touches the final table.
--   In production, you can run data quality checks on staging
--   before committing to the permanent table.
-- Extra column: load_timestamp
--   Records WHEN this row was loaded by ETL.
--   Useful for debugging and auditing.

CREATE TABLE stg_github_issues
(
    issue_id       BIGINT,
    issue_number   INT,
    title          TEXT,
    state          VARCHAR(20),
    created_at     TIMESTAMP,
    updated_at     TIMESTAMP,
    user_login     VARCHAR(100),
    load_timestamp TIMESTAMP       -- when ETL loaded this row
);

-- Verify
SELECT * FROM stg_github_issues;


-- ── STEP 3: CREATE FINAL TABLE ──────────────────────────────
-- What is this?
--   The permanent table that holds all GitHub issues.
--   This is the table analysts and dashboards read from.
-- Why PRIMARY KEY on issue_id?
--   Enables UPSERT (ON CONFLICT). Without it, we can't do
--   "update if exists, insert if new" logic.
-- Note: No load_timestamp here — analysts don't need it.
--   load_timestamp is only for ETL tracking (stays in staging).

CREATE TABLE github_issues
(
    issue_id     BIGINT PRIMARY KEY,
    issue_number INT,
    title        TEXT,
    state        VARCHAR(20),
    created_at   TIMESTAMP,
    updated_at   TIMESTAMP,
    user_login   VARCHAR(100)
);

-- Verify
SELECT * FROM github_issues;


-- ── STEP 4: THE UPSERT QUERY (SCD TYPE 1) ───────────────────
-- What is UPSERT?
--   UPSERT = UPDATE + INSERT combined.
--   If the issue already exists → update its values.
--   If it's a new issue → insert it fresh.
-- What is SCD Type 1?
--   Slowly Changing Dimension Type 1.
--   Old values get OVERWRITTEN with new ones.
--   No history is kept. Simple and fast.
--   Example: Issue title changed → new title replaces old title.
-- What does EXCLUDED mean?
--   When a conflict happens, EXCLUDED refers to the row
--   that was being inserted (but couldn't because of conflict).
--   So EXCLUDED.title = the new title we tried to insert.

INSERT INTO github_issues
(
    issue_id,
    issue_number,
    title,
    state,
    created_at,
    updated_at,
    user_login
)
SELECT
    issue_id,
    issue_number,
    title,
    state,
    created_at,
    updated_at,
    user_login
FROM stg_github_issues
ON CONFLICT (issue_id)
DO UPDATE SET
    issue_number = EXCLUDED.issue_number,
    title        = EXCLUDED.title,
    state        = EXCLUDED.state,
    created_at   = EXCLUDED.created_at,
    updated_at   = EXCLUDED.updated_at,
    user_login   = EXCLUDED.user_login;


-- ── USEFUL VERIFICATION QUERIES ─────────────────────────────

-- Check current watermark
SELECT * FROM etl_watermark;

-- Check how many rows in final table
SELECT COUNT(*) FROM github_issues;

-- Check for duplicate issue_ids in staging (should return 0 rows)
SELECT issue_id, COUNT(*)
FROM stg_github_issues
GROUP BY issue_id
HAVING COUNT(*) > 1;

-- See the 5 most recently updated issues
SELECT issue_id, title, state, updated_at
FROM github_issues
ORDER BY updated_at DESC
LIMIT 5;

-- Compare staging vs final row count
SELECT 'staging' AS table_name, COUNT(*) AS row_count FROM stg_github_issues
UNION ALL
SELECT 'final',                  COUNT(*)               FROM github_issues;
