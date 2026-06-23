#!/usr/bin/env python3
"""
Deduplicates Snowflake tables by:
1. Parsing {schema}_ddl.sql to get exact column definitions per table
2. Creating a regular staging table _DEDUP_{table} with the same schema
3. INSERT INTO staging SELECT DISTINCT * FROM original
4. CREATE OR REPLACE TABLE original CLONE staging  (atomic zero-copy swap)
5. DROP TABLE staging

Uses regular tables (not TEMPORARY) because Snowflake temporary tables cannot be cloned.
"""
import os
import re
import time
from pathlib import Path

import yaml
import snowflake.connector

with open(Path(__file__).parent / "config.yml", "r") as f:
    cfg = yaml.safe_load(f)
sf = cfg["snowflake"]

SF_DATABASE = "GTFS_TEST"
EXPORTS_DIR = cfg["exports_dir"]
SCHEMAS = ["SCHEDULE", "TRIP_UPDATES", "WEATHER_API_STAGING"]


POLL_INTERVAL = 30  # seconds between status checks for long-running queries


def connect():
    return snowflake.connector.connect(
        user=sf["user"],
        password=sf["password"],
        account=sf["account"],
        database=sf["database"],
        warehouse=sf["warehouse"],
        network_timeout=120,
    )


def execute_async_poll(conn, cs, sql: str, fetch_results: bool = False):
    """
    Execute SQL asynchronously and poll until completion.
    Avoids the 120s network_timeout cancelling long-running INSERT/SELECT operations.
    Set fetch_results=True when the caller will call cs.fetchone()/fetchall() afterwards.
    Polling uses exponential backoff: 1s → 2s → 4s → … → 30s max.
    """
    cs.execute_async(sql)
    query_id = cs.sfqid
    interval = 1
    while conn.is_still_running(conn.get_query_status(query_id)):
        time.sleep(interval)
        interval = min(interval * 2, POLL_INTERVAL)
        print(f"    [{query_id}] Still running...")
    conn.get_query_status_throw_if_error(query_id)
    if fetch_results:
        cs.get_results_from_sfqid(query_id)


def get_all_tables(cs, schema: str) -> list:
    cs.execute("""
        SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
          AND TABLE_NAME NOT LIKE '_DEDUP_%%'
        ORDER BY TABLE_NAME
    """, (schema,))
    return [row[0] for row in cs.fetchall()]


def check_duplicates(conn, cs, schema: str, table_name: str):
    """Returns (total_rows, distinct_rows). Distinct is computed over all columns."""
    cs.execute(f'SELECT COUNT(*) FROM "{SF_DATABASE}"."{schema}"."{table_name}"')
    total = cs.fetchone()[0]
    if total == 0:
        return 0, 0
    execute_async_poll(conn, cs, f"""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT * FROM "{SF_DATABASE}"."{schema}"."{table_name}"
        )
    """, fetch_results=True)
    distinct = cs.fetchone()[0]
    return total, distinct


def extract_table_ddl(ddl_text: str, table_name: str) -> str:
    """
    Extract the CREATE [OR REPLACE] TABLE ... block for a specific table from the DDL file.
    Returns just the column definition body (the part inside the outer parentheses).
    """
    # Match a CREATE TABLE statement that references this table name
    pattern = rf'create\s+(?:or\s+replace\s+)?table\s+[^\s(]*{re.escape(table_name)}\s*\(([^;]+)\)'
    m = re.search(pattern, ddl_text, re.IGNORECASE | re.DOTALL)
    if not m:
        return None
    return m.group(1).strip()


def dedup_via_temp_clone(conn, cs, schema: str, table_name: str, column_body: str):
    staging = f"_DEDUP_{table_name}"
    fq_orig = f'"{SF_DATABASE}"."{schema}"."{table_name}"'
    fq_stage = f'"{SF_DATABASE}"."{schema}"."{staging}"'

    # Step 1: create staging table with same columns
    cs.execute(f'USE DATABASE "{SF_DATABASE}"')
    cs.execute(f'USE SCHEMA "{schema}"')
    cs.execute(f'DROP TABLE IF EXISTS {fq_stage}')
    cs.execute(f'CREATE TABLE {fq_stage} ({column_body})')

    # Step 2: populate staging with deduplicated rows (async — may run for many minutes)
    execute_async_poll(conn, cs, f'INSERT INTO {fq_stage} SELECT DISTINCT * FROM {fq_orig}')

    # Step 3: atomic zero-copy swap — original is replaced instantly
    cs.execute(f'CREATE OR REPLACE TABLE {fq_orig} CLONE {fq_stage}')

    # Step 4: drop staging
    cs.execute(f'DROP TABLE IF EXISTS {fq_stage}')


def process_schema(conn, schema: str):
    print(f"\n=== Schema: {schema} ===")
    ddl_path = Path(EXPORTS_DIR) / schema / f"{schema}_ddl.sql"
    if not ddl_path.exists():
        print(f"  ⚠ DDL file not found: {ddl_path} — cannot dedup this schema")
        return
    ddl_text = ddl_path.read_text(encoding="utf-8")

    cs = conn.cursor()
    try:
        cs.execute(f'USE DATABASE "{SF_DATABASE}"')
        cs.execute(f'USE SCHEMA "{schema}"')
        tables = get_all_tables(cs, schema)
    finally:
        cs.close()

    for table_name in tables:
        cs = conn.cursor()
        try:
            total, distinct = check_duplicates(conn, cs, schema, table_name)
        finally:
            cs.close()

        if total == 0:
            print(f"  {table_name}: empty — skipping")
            continue
        if total == distinct:
            print(f"  {table_name}: clean ({total} rows)")
            continue

        dupes = total - distinct
        print(f"  {table_name}: {dupes} duplicates ({total} rows → {distinct} distinct) — deduping...")

        column_body = extract_table_ddl(ddl_text, table_name)
        if column_body is None:
            print(f"  ⚠ Could not extract DDL for {table_name} from {ddl_path} — skipping")
            continue

        cs = conn.cursor()
        try:
            dedup_via_temp_clone(conn, cs, schema, table_name, column_body)
        finally:
            cs.close()

        # Verify
        cs = conn.cursor()
        try:
            total_after, _ = check_duplicates(conn, cs, schema, table_name)
        finally:
            cs.close()
        print(f"  {table_name}: done — {total_after} rows remaining")


def main():
    conn = connect()
    try:
        for schema in SCHEMAS:
            process_schema(conn, schema)
    finally:
        try:
            conn.close()
        except Exception:
            pass
    print("\nDedup complete.")


if __name__ == "__main__":
    main()
