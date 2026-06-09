# github-etl-pipeline
This ETL Pipeline discuss about how to fetch data from API into SQL using python automation and window task scheduler

# GitHub Issues ETL Pipeline

A real incremental data pipeline that pulls live issues from the GitHub API,
transforms them, and loads them into PostgreSQL — automatically, every run
only picking up what changed since the last one.

---

## What This Project Does

Every time this pipeline runs, it:

1. Checks the last processed date from the database (the watermark)
2. Calls the GitHub API and fetches the latest issues
3. Filters only records that were updated after that date
4. Loads them into a staging table
5. Upserts them into the final table (new issues added, changed issues updated)
6. Saves the new checkpoint so the next run starts from there

No duplicates. No full reloads. Just the delta — what actually changed.

---

## Why Incremental Loading Matters

Earlier when i write pipelines that load everything every time. That works when
your data is small. The moment your data grows, it becomes a problem.

Incremental loading solves this properly:

- **Run 1** — watermark is year 2000, so it loads all 30 issues (first ever run)
- **Run 2** — watermark is yesterday, so it loads only the 2 issues that changed
- **Run 3** — watermark is today, so it loads 0 issues (nothing changed)

This is how every real production ETL works — from startup pipelines to Fortune 500
data warehouses. The concept is the same.

---

## Project Structure

```
github_etl/
│
├── github_etl.py    ← Main pipeline script (8 functions + try/except)
├── setup.sql        ← All SQL to create tables and verify data
├── APPROACH.md      ← My step by step thinking for any API project
├── run_etl.bat      ← Windows batch file to automate the pipeline
└── README.md        ← This file
```

---

## How It Works — Architecture

```
GitHub API
    │
    ▼
extract_data()           ← HTTP GET request, returns raw JSON
    │
    ▼
transform_data()         ← picks 7 columns, cleans types
    │
    ▼
get_incremental_records() ← filters rows where updated_at > watermark
    │
    ▼
stg_github_issues        ← staging table (TRUNCATE + INSERT every run)
    │
    ▼
github_issues            ← final table (UPSERT — SCD Type 1)
    │
    ▼
etl_watermark            ← watermark updated to max(updated_at) of this run
```

---

## Database Tables

**etl_watermark** — stores the last successful run's checkpoint
```sql
process_name VARCHAR(100)
last_updated_at TIMESTAMP
```

**stg_github_issues** — staging / temporary buffer
```sql
issue_id BIGINT, issue_number INT, title TEXT,
state VARCHAR(20), created_at TIMESTAMP,
updated_at TIMESTAMP, user_login VARCHAR(100),
load_timestamp TIMESTAMP
```

**github_issues** — final permanent table
```sql
issue_id BIGINT PRIMARY KEY, issue_number INT, title TEXT,
state VARCHAR(20), created_at TIMESTAMP,
updated_at TIMESTAMP, user_login VARCHAR(100)
```

---

## Setup Instructions

### Prerequisites
- Python 3.x
- PostgreSQL (pgAdmin)
- pip packages: `requests`, `pandas`, `psycopg2-binary`

### Install Python packages
```bash
pip install requests pandas psycopg2-binary
```

### Create the database
1. Open pgAdmin
2. Create a new database called `github_etl`
3. Open Query Tool and run the full `setup.sql` file top to bottom

### Configure connection
Open `github_etl.py` and update these values in `get_connection()`:
```python
host="localhost"
database="github_etl"
user="postgres"
password="your_password"
```

### Run the pipeline
```bash
python github_etl.py
```

### Automate it (Windows Task Scheduler)
1. Create `run_etl.bat`:
   ```
   cd C:\path\to\your\project
   python github_etl.py
   ```
2. Open Task Scheduler → Create Basic Task
3. Set your schedule (daily, hourly, every 5 minutes)
4. Action → Start a Program → point to `run_etl.bat`

---

## Sample Log Output

```
2026-06-09 15:03:38 - INFO - ==================================================
2026-06-09 15:03:38 - INFO - ETL STARTED
2026-06-09 15:03:39 - INFO - Database connected
2026-06-09 15:03:39 - INFO - Watermark = 2026-06-08 17:35:09
2026-06-09 15:03:40 - INFO - Rows fetched from API = 30
2026-06-09 15:03:40 - INFO - Transform complete
2026-06-09 15:03:40 - INFO - New records to load = 2
2026-06-09 15:03:40 - INFO - Staging loaded with 2 rows
2026-06-09 15:03:40 - INFO - Rows In Staging = 2
2026-06-09 15:03:40 - INFO - Final table loaded via UPSERT (SCD Type 1)
2026-06-09 15:03:40 - INFO - Watermark updated to 2026-06-09 15:01:28
2026-06-09 15:03:40 - INFO - ETL COMPLETED SUCCESSFULLY
2026-06-09 15:03:40 - INFO - ==================================================
```

---

## Key Concepts Demonstrated

| Concept | Where |
|---------|-------|
| Incremental loading with watermark | `get_incremental_records()`, `update_watermark()` |
| Staging → Final pattern | `load_staging()` → `load_final()` |
| SCD Type 1 (UPSERT) | `ON CONFLICT DO UPDATE` in `load_final()` |
| Error handling | `try / except / finally` block |
| Structured logging | `logging` module, writes to `etl.log` |
| Modular functions | 8 single-purpose functions |

---

## What I Would Add Next

- **SCD Type 2** — keep history of changes instead of overwriting
- **Config file** — move DB credentials to a separate `config.py`
- **Email alerts** — send email when ETL fails
- **Multiple tables** — pull labels, comments, milestones alongside issues

---

## Author

Akash Kumar — Senior Data Analyst  
Built as a hands-on learning project to understand real ETL patterns used in production.

