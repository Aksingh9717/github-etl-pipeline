# How I Approach Any API Data Pipeline — Step by Step

This document explains my thinking process from the moment I receive a problem statement
to the moment the pipeline is running automatically. I wrote this for myself so I can
come back to it for any future project.

---

## The Problem Statement (What We Were Asked to Do)

> "Fetch GitHub issues from the Microsoft VSCode repository and store them in a
> PostgreSQL database. The pipeline should be incremental — meaning each run should
> only load new or updated issues, not reload everything from scratch."

That one sentence contains everything we need to figure out:

- **Source:** GitHub API
- **Destination:** PostgreSQL database
- **Challenge:** Incremental loading (don't reload everything every time)

---

## My Step by Step Thinking Process

### Step 1 — Understand the API First (Before Writing Any Code)

The very first thing I do is open the API in a browser or Postman and look at the raw data.

For GitHub issues the URL is:
```
https://api.github.com/repos/microsoft/vscode/issues
```

I look at the JSON response and ask:
- What fields does it return? (id, number, title, state, created_at, updated_at, user...)
- What does each field mean?
- Which field tells me WHEN a record was last changed? → `updated_at`
- Is there a field I can use as a unique key? → `id`

This step takes 10 minutes but saves hours of confusion later.

---

### Step 2 — Decide Which Columns You Actually Need

The API returns 30+ fields per issue. I don't need all of them.

I pick only what's useful for analysis:

| Column | Why I need it |
|--------|---------------|
| `id` | Unique identifier — used as primary key |
| `number` | Issue number visible on GitHub |
| `title` | What the issue is about |
| `state` | Open or closed |
| `created_at` | When the issue was first raised |
| `updated_at` | When it was last changed — used for incremental filtering |
| `user.login` | Who raised the issue |

---

### Step 3 — Design the Database Tables (Before Writing Python)

I always create the database structure BEFORE writing any Python code. This way the
destination is ready when the pipeline runs.

I need 3 tables:

**Table 1: etl_watermark**
Stores the last processed date. This is the brain of incremental loading.
```
process_name | last_updated_at
github_issues | 2000-01-01
```

**Table 2: stg_github_issues (Staging)**
Temporary table. Gets wiped and reloaded every run.
Includes `load_timestamp` so I know exactly when each row was loaded.

**Table 3: github_issues (Final)**
Permanent table. Analysts and dashboards read from here.
Has `issue_id` as PRIMARY KEY to support UPSERT.

---

### Step 4 — Create the Tables in pgAdmin

Open pgAdmin → select your database → open Query Tool → run `setup.sql`

The order matters:
1. Create `etl_watermark` first
2. Insert the starting watermark row (`2000-01-01`) immediately after creating it
3. Create `stg_github_issues`
4. Create `github_issues`

---

### Step 5 — Write the Python Pipeline (Function by Function)

I write one function at a time and test each one before moving to the next.

The 8 functions follow the same order as the data flow:

```
get_connection()           → open database connection
get_watermark()            → read last processed date
extract_data()             → call the API, get raw JSON
transform_data()           → clean the data, pick columns
get_incremental_records()  → filter only new rows
load_staging()             → insert into staging table
load_final()               → UPSERT into final table
update_watermark()         → save the new max date
```

Each function does ONE job. If I can describe it in one sentence, it deserves a function.

---

### Step 6 — Understand Incremental Loading (The Core Concept)

This is the most important idea in the whole project.

**Without incremental loading:**
- Every run fetches all 30 rows and inserts all 30 rows
- After 100 runs you have 3,000 rows — most of them duplicates
- Slow, wasteful, inaccurate

**With incremental loading:**
- Run 1: Watermark = 2000-01-01 → fetches all 30 rows (first ever run)
- Run 2: Watermark = 2026-06-08 → fetches only 2 rows that changed since then
- Run 3: Watermark = 2026-06-09 → fetches only 1 row that changed
- Each run loads only what actually changed

**How it works in code:**
```python
new_records = df2[df2['updated_at'] > pd.Timestamp(watermark)]
```
This single line is incremental loading. It reads: "give me only rows where
updated_at is newer than our last checkpoint."

**After each successful run:**
```sql
UPDATE etl_watermark SET last_updated_at = <max_updated_at>
WHERE process_name = 'github_issues'
```
This moves the checkpoint forward so the next run starts from here.

---

### Step 7 — Understand the Staging → Final Flow

Many beginners ask: why not insert directly into the final table?

**Reason 1: Safety**
Staging is a buffer. If something goes wrong during transform, the final table
is untouched. We can fix the issue and reload staging without corrupting real data.

**Reason 2: UPSERT becomes simple**
We load everything into staging first, then run one SQL command to UPSERT it all.
This is cleaner than doing row-by-row UPSERT in Python.

**Reason 3: Auditing**
Staging has `load_timestamp` — so we can always check what was loaded in any given run.

---

### Step 8 — Add Error Handling and Logging

Every production ETL has:

```python
try:
    # all 8 steps
except Exception as e:
    logging.error(f"ETL FAILED: {e}")
finally:
    cursor.close()
    conn.close()
```

**try** — attempt the full pipeline  
**except** — if anything fails (API down, DB error, bad data) → log the error  
**finally** — always close the connection, even if it crashed halfway

Logging instead of print() because:
- Logs go to a file (`etl.log`) — you can check them later
- Logs have timestamps — you know exactly when each step ran
- In production, nobody is watching the terminal

---

### Step 9 — Test Manually First

Before automating, I always run the script manually 2-3 times and verify:

```sql
-- Watermark moved forward?
SELECT * FROM etl_watermark;

-- Rows in final table correct?
SELECT COUNT(*) FROM github_issues;

-- Any duplicates?
SELECT issue_id, COUNT(*) FROM github_issues
GROUP BY issue_id HAVING COUNT(*) > 1;
```

If all 3 checks pass → safe to automate.

---

### Step 10 — Automate with Windows Task Scheduler

**Step 1:** Create `run_etl.bat` in the same folder:
```
cd C:\Users\akash.kumar1\Documents\github_etl
python github_etl.py
```

**Step 2:** Open Task Scheduler → Create Basic Task

**Step 3:** Set trigger (Daily / Every 5 minutes / Hourly)

**Step 4:** Action → Start a Program → point to `run_etl.bat`

**Step 5:** Save. Done.

The ETL now runs automatically without you touching anything.

---

## The Mental Model I Use for Any New API Project

Every time I see a new API and need to pull data, I ask these questions in order:

1. What does the raw JSON look like? (open it in browser first)
2. Which field is the unique key?
3. Which field tells me when a record was last updated?
4. Which columns do I actually need?
5. What are my 3 tables? (watermark, staging, final)
6. Write functions in data flow order
7. Does incremental loading make sense here? (almost always yes)
8. How do I handle errors?
9. How do I automate it?

These 9 questions work for GitHub, for any REST API, for any database destination.

---

## Files in This Project

| File | What it does |
|------|--------------|
| `github_etl.py` | Main Python pipeline script |
| `setup.sql` | All SQL to create tables and run queries |
| `APPROACH.md` | This file — thinking process and concepts |
| `README.md` | Project overview for GitHub |
| `run_etl.bat` | Windows batch file to run the script |
| `etl.log` | Auto-generated log file after each run |
