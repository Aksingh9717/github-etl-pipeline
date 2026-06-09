# ============================================================
# GITHUB ISSUES ETL PIPELINE
# What this does:
#   1. Connect to PostgreSQL
#   2. Read last processed date (watermark)
#   3. Fetch issues from GitHub API
#   4. Transform raw JSON into clean table
#   5. Filter only NEW records since last run
#   6. Load into staging table
#   7. UPSERT into final table
#   8. Update watermark for next run
# ============================================================

# ── IMPORTS ─────────────────────────────────────────────────
import requests        # sends HTTP requests to GitHub API
import pandas as pd    # converts JSON → DataFrame (table)
import psycopg2        # connects Python to PostgreSQL
import logging         # writes logs to file instead of print
from datetime import datetime  # gets current timestamp for load_time


# ── LOGGING SETUP ───────────────────────────────────────────
# Instead of print(), we use logging.
# This writes messages to a file called etl.log
# AND shows them on screen (because of StreamHandler).
# Format: 2026-06-09 10:00:00 - INFO - ETL Started
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl.log'),   # saves logs to file
        logging.StreamHandler()           # also shows on screen
    ]
)


# ============================================================
# FUNCTION 1: get_connection()
# Job: Connect to PostgreSQL and return the connection object
# Called from: try block → conn = get_connection()
# ============================================================
def get_connection():
    conn = psycopg2.connect(
        host="localhost",
        database="github_etl",
        user="postgres",
        password="root"
    )
    # return sends the connection object back to whoever called this function
    # Think: function produces a value → return sends it back
    return conn


# ============================================================
# FUNCTION 2: get_watermark(cursor)
# Job: Read the last processed date from etl_watermark table
# Parameter: cursor — needed to run SQL queries
# Returns: a datetime value like 2026-06-08 15:11:44
# Called from: try block → watermark = get_watermark(cursor)
# ============================================================
def get_watermark(cursor):
    cursor.execute("""
        SELECT last_updated_at
        FROM etl_watermark
        WHERE process_name = 'github_issues'
    """)
    # cursor.fetchone() returns one row as a tuple: (datetime(2026,6,8,15,11),)
    # [0] takes the first element from that tuple → the actual datetime value
    watermark = cursor.fetchone()[0]
    return watermark


# ============================================================
# FUNCTION 3: extract_data()
# Job: Call GitHub API and return raw JSON data
# Returns: list of issue dictionaries (raw JSON)
# Called from: try block → data = extract_data()
# ============================================================
def extract_data():
    url = "https://api.github.com/repos/microsoft/vscode/issues"
    response = requests.get(url)
    # .json() converts the API response text into a Python list/dict
    data = response.json()
    return data


# ============================================================
# FUNCTION 4: transform_data(data)
# Job: Convert raw JSON into a clean DataFrame with only the columns we need
# Parameter: data — the raw JSON list from extract_data()
# Returns: df2 — a clean DataFrame
# Called from: try block → df2 = transform_data(data)
# ============================================================
def transform_data(data):
    # Convert raw JSON list into a full DataFrame (every field from API)
    df = pd.DataFrame(data)

    # Create a new empty DataFrame — we only want specific columns
    df2 = pd.DataFrame()

    df2['issue_id']     = df['id']
    df2['issue_number'] = df['number']
    df2['title']        = df['title']
    df2['state']        = df['state']
    df2['created_at']   = df['created_at']
    df2['updated_at']   = df['updated_at']

    # df['user'] contains a dict like {'login': 'akash', 'id': 123, ...}
    # .apply(lambda x: x['login']) extracts only the login name from each row
    df2['user_login'] = df['user'].apply(lambda x: x['login'])

    return df2


# ============================================================
# FUNCTION 5: get_incremental_records(df2, watermark)
# Job: Filter only rows that are newer than the watermark
#      This is the CORE of incremental loading
# Parameters:
#   df2       — clean DataFrame from transform_data()
#   watermark — datetime from get_watermark()
# Returns: new_records — only rows with updated_at > watermark
# Called from: try block → new_records = get_incremental_records(df2, watermark)
# ============================================================
def get_incremental_records(df2, watermark):
    # Convert updated_at from string "2026-06-08T09:01:28Z" to datetime object
    # .dt.tz_localize(None) removes timezone info so comparison with watermark works
    df2['updated_at'] = pd.to_datetime(df2['updated_at']).dt.tz_localize(None)

    # pd.Timestamp(watermark) converts the DB datetime into a pandas Timestamp
    # so we can compare two datetime objects correctly
    new_records = df2[df2['updated_at'] > pd.Timestamp(watermark)]
    return new_records


# ============================================================
# FUNCTION 6: load_staging(cursor, conn, new_records)
# Job: Clear staging table and insert only the new records
# Parameters:
#   cursor      — to run SQL
#   conn        — to commit changes
#   new_records — filtered DataFrame from get_incremental_records()
# Called from: try block → load_staging(cursor, conn, new_records)
# ============================================================
def load_staging(cursor, conn, new_records):
    load_time = datetime.now()  # timestamp of when this ETL run happened

    # TRUNCATE clears the staging table completely before inserting fresh data
    cursor.execute("TRUNCATE TABLE stg_github_issues")
    conn.commit()

    # Loop through every row in new_records and insert it into staging
    # iterrows() gives you (index, row) for each row in the DataFrame
    for index, row in new_records.iterrows():
        sql = """
            INSERT INTO stg_github_issues
            (issue_id, issue_number, title, state, created_at, updated_at, user_login, load_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (
            int(row['issue_id']),
            int(row['issue_number']),
            row['title'],
            row['state'],
            row['created_at'],
            row['updated_at'],
            row['user_login'],
            load_time
        ))

    # commit() saves all the inserts permanently to the database
    # Without this, the inserts exist temporarily and disappear
    conn.commit()
    logging.info(f"Staging loaded with {len(new_records)} rows")


# ============================================================
# FUNCTION 7: load_final(cursor, conn)
# Job: UPSERT from staging into the final table
#      UPSERT = UPDATE if exists, INSERT if new
#      This is SCD Type 1 — old values get overwritten
# Parameters: cursor, conn
# Called from: try block → load_final(cursor, conn)
# ============================================================
def load_final(cursor, conn):
    # First check how many rows are in staging
    cursor.execute("SELECT COUNT(*) FROM stg_github_issues")
    count = cursor.fetchone()[0]
    logging.info(f"Rows In Staging = {count}")

    # UPSERT logic:
    # - Try to INSERT the row
    # - If issue_id already exists (ON CONFLICT), UPDATE instead
    # This means: new issues get added, existing ones get their data refreshed
    cursor.execute("""
        INSERT INTO github_issues
        (issue_id, issue_number, title, state, created_at, updated_at, user_login, load_timestamp)
        SELECT issue_id, issue_number, title, state, created_at, updated_at, user_login, load_timestamp
        FROM stg_github_issues
        ON CONFLICT (issue_id)
        DO UPDATE SET
            title        = EXCLUDED.title,
            state        = EXCLUDED.state,
            updated_at   = EXCLUDED.updated_at,
            user_login   = EXCLUDED.user_login,
            load_timestamp = EXCLUDED.load_timestamp
    """)
    conn.commit()
    logging.info("Final table loaded (UPSERT complete)")


# ============================================================
# FUNCTION 8: update_watermark(cursor, conn, max_updated_at)
# Job: Save the newest processed date into etl_watermark
#      So next run starts from this point (no duplicates)
# Parameters:
#   cursor         — to run SQL
#   conn           — to commit
#   max_updated_at — the newest updated_at from new_records
# Called from: try block → update_watermark(cursor, conn, max_updated_at)
# ============================================================
def update_watermark(cursor, conn, max_updated_at):
    cursor.execute("""
        UPDATE etl_watermark
        SET last_updated_at = %s
        WHERE process_name = 'github_issues'
    """, (max_updated_at,))
    conn.commit()
    logging.info(f"Watermark updated to {max_updated_at}")


# ============================================================
# MAIN EXECUTION BLOCK
# This is where all 8 functions are called in order.
# Think of this as the "director" — it calls each function
# one by one and passes results between them.
#
# try    → run the ETL
# except → if ANYTHING fails, log the error (don't crash silently)
# finally→ ALWAYS close cursor and connection, success or failure
# ============================================================

# These are declared outside try so finally can always access them
conn   = None
cursor = None

try:
    logging.info("=" * 50)
    logging.info("ETL STARTED")

    # Step 1: Connect to database
    conn = get_connection()
    cursor = conn.cursor()
    logging.info("Database connected")

    # Step 2: Read watermark (last successful run's max date)
    watermark = get_watermark(cursor)
    logging.info(f"Watermark = {watermark}")

    # Step 3: Fetch data from GitHub API
    data = extract_data()
    logging.info(f"Rows fetched from API = {len(data)}")

    # Step 4: Transform raw JSON into clean DataFrame
    df2 = transform_data(data)
    logging.info("Transform complete")

    # Step 5: Filter only new/updated records since last run
    new_records = get_incremental_records(df2, watermark)
    logging.info(f"New records to load = {len(new_records)}")

    # If no new records, skip loading (nothing to do)
    if len(new_records) == 0:
        logging.info("No new records found. ETL complete.")
    else:
        # Step 6: Load into staging table
        load_staging(cursor, conn, new_records)

        # Step 7: UPSERT from staging into final table
        load_final(cursor, conn)

        # Step 8: Calculate newest date from this run and update watermark
        # This is calculated HERE (not inside update_watermark)
        # because new_records only exists in this try block
        max_updated_at = new_records['updated_at'].max()
        update_watermark(cursor, conn, max_updated_at)

        logging.info(f"Max Updated Date = {max_updated_at}")

    logging.info("ETL COMPLETED SUCCESSFULLY")
    logging.info("=" * 50)

except Exception as e:
    # If anything fails (API down, DB error, bad data), log the error
    # instead of crashing silently
    logging.error(f"ETL FAILED: {e}")

finally:
    # This ALWAYS runs — whether ETL succeeded or failed
    # Closing cursor and connection frees up database resources
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    logging.info("Database connection closed")
