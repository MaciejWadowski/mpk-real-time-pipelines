import os
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
import tempfile

import pandas as pd
import yaml
import snowflake.connector
from snowflake.connector import errors as sf_errors

# === CONFIGURATION ===
MODE = "fresh"  # "full" or "fresh"
PARQUET_ENGINE = "pyarrow"

# Retry / network settings
NETWORK_TIMEOUT = 120
MAX_RETRIES = 6
BASE_BACKOFF = 2
RETRYABLE_EXCEPTIONS = (
    sf_errors.OperationalError,
    sf_errors.DatabaseError,
    ConnectionError,
    OSError,
    TimeoutError,
)

# Load config.yml
with open(Path(__file__).parent / "config.yml", "r") as f:
    cfg = yaml.safe_load(f)

sf = cfg["snowflake"]

SF_DATABASE = sf.get("database", "GTFS_TEST")
SCHEMAS = [
    "SCHEDULE",
    "TRIP_UPDATES",
    "WEATHER_API_STAGING",
]
OUTPUT_DIR = r"g:\Archiwum\Projekcik\snowflake_exports"
MAX_SIZE_MB = 50
MIGRATION_META = Path(OUTPUT_DIR) / "migration_meta.json"


def load_last_migration():
    if MIGRATION_META.exists():
        try:
            with open(MIGRATION_META, "r") as f:
                data = json.load(f)
            ts = data.get("last_migration")
            if ts:
                return datetime.fromisoformat(ts)
        except Exception:
            return None
    return None


def save_migration_timestamp(ts: datetime):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(MIGRATION_META, "w") as f:
        json.dump({"last_migration": ts.isoformat()}, f)


def connect():
    return snowflake.connector.connect(
        user=sf["user"],
        password=sf["password"],
        account=sf["account"],
        database=sf["database"],
        warehouse=sf["warehouse"],
        network_timeout=NETWORK_TIMEOUT,
        client_session_keep_alive=True,
    )


def retry_operation(fn, *args, **kwargs):
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except RETRYABLE_EXCEPTIONS as e:
            last_exc = e
            if attempt == MAX_RETRIES:
                raise
            sleep = BASE_BACKOFF * (2 ** (attempt - 1))
            print(f"Transient error ({type(e).__name__}): {e}. Retrying in {sleep}s (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(sleep)
    if last_exc:
        raise last_exc


def save_schema_ddl(conn, schema, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    ddl_file = os.path.join(out_dir, f"{schema}_ddl.sql")
    lines = []
    lines.append(f"CREATE SCHEMA IF NOT EXISTS {SF_DATABASE}.{schema};")
    cs = conn.cursor()
    try:
        cs.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_type = 'BASE TABLE'",
            (schema,),
        )
        tables = [r[0] for r in cs.fetchall()]
    finally:
        cs.close()

    for tbl in tables:
        try:
            cur = conn.cursor()
            try:
                cur.execute(f"SELECT GET_DDL('TABLE', '{SF_DATABASE}.{schema}.{tbl}')")
                ddl_rows = cur.fetchall()
                ddl_text = "\n".join(r[0] for r in ddl_rows if r and r[0])
                if ddl_text:
                    lines.append(f"\n-- DDL for {schema}.{tbl}\n{ddl_text}")
            finally:
                cur.close()
        except Exception as e:
            lines.append(f"\n-- Could not fetch DDL for {schema}.{tbl}: {e}")
    with open(ddl_file, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"  → Saved DDL to {ddl_file}")


def get_tables_with_sizes(conn, schema):
    cs = conn.cursor()
    try:
        cs.execute(
            f"""
            SELECT table_name, bytes
            FROM {sf['database']}.information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            """,
            (schema,),
        )
        return cs.fetchall()
    finally:
        cs.close()


def atomic_write_parquet(df: pd.DataFrame, final_path: str, mode: str = 'incremental'):
    """
    Write parquet atomically: write to .part file in same folder then rename.
    If final_path already exists and mode is incremental, skip and return False.
    Returns True if file was written, False if skipped.
    """
    if os.path.exists(final_path) and mode == 'incremental':
        # Already downloaded previously; skip
        print(f"    → Skipping (exists): {final_path}")
        return False

    # Ensure directory exists
    os.makedirs(os.path.dirname(final_path), exist_ok=True)

    # Use a temporary file in same directory to ensure atomic rename
    dir_name = os.path.dirname(final_path) or "."
    base_tmp = Path(tempfile.mktemp(dir=dir_name))
    try:
        # write to tmp path using parquet engine
        df.to_parquet(str(base_tmp), engine=PARQUET_ENGINE, index=False)
        # Rename to final path (atomic on same filesystem)
        os.replace(str(base_tmp), final_path)
        print(f"    → Wrote {len(df)} rows to {final_path}")
        return True
    finally:
        # cleanup tmp if still present
        try:
            if os.path.exists(str(base_tmp)):
                os.remove(str(base_tmp))
        except Exception:
            pass


def read_sql_with_retry(query: str, conn):
    return retry_operation(pd.read_sql, query, conn)


def export_full_table(conn, table_ref, out_path):
    df = read_sql_with_retry(f"SELECT * FROM {table_ref}", conn)
    if not df.empty:
        atomic_write_parquet(df, out_path, 'overwrite')
    else:
        print(f"    → Full export: no rows for {table_ref}")


def export_incremental_table(conn, table_ref, out_dir, base_name, last_migration_ts):
    if last_migration_ts is None:
        export_by_timestamp_chunks(conn, table_ref, out_dir, base_name)
        return

    start = last_migration_ts + timedelta(microseconds=1)
    qry = f"SELECT MAX(LOAD_TIMESTAMP) FROM {table_ref}"
    try:
        max_ts = read_sql_with_retry(qry, conn).iloc[0, 0]
    except Exception:
        max_ts = None

    if max_ts is None or pd.isnull(max_ts):
        print("    ⚠ Table has no LOAD_TIMESTAMP values or could not determine max; skipping incremental export.")
        return

    current = start
    while current <= max_ts:
        next_day = current + timedelta(days=1)
        chunk_name = current.strftime("%Y-%m-%d")
        final_file = os.path.join(out_dir, f"{base_name}_{chunk_name}.parquet")
        # skip chunk if already downloaded
        if os.path.exists(final_file):
            print(f"    → Chunk {chunk_name} skipped (already exists): {final_file}")
            current = next_day
            continue
        qry = f"""
            SELECT * FROM {table_ref}
            WHERE LOAD_TIMESTAMP > '{last_migration_ts.isoformat()}' AND LOAD_TIMESTAMP >= '{current}' AND LOAD_TIMESTAMP < '{next_day}'
        """
        df = read_sql_with_retry(qry, conn)
        if not df.empty:
            atomic_write_parquet(df, final_file)
        else:
            print(f"    → Chunk {chunk_name}: no new data")
        current = next_day


def export_by_timestamp_chunks(conn, table, out_dir, base_name):
    qry = f"SELECT MIN(LOAD_TIMESTAMP), MAX(LOAD_TIMESTAMP) FROM {table}"
    try:
        min_ts, max_ts = read_sql_with_retry(qry, conn).iloc[0]
    except Exception:
        print(f"    ⚠ Could not determine LOAD_TIMESTAMP range for {table}, skipping chunking.")
        return

    if pd.isnull(min_ts) or pd.isnull(max_ts):
        print(f"    ⚠ Could not determine LOAD_TIMESTAMP range for {table}, skipping chunking.")
        return

    current = min_ts
    while current <= max_ts:
        next_day = current + timedelta(days=1)
        chunk_name = current.strftime("%Y-%m-%d")
        final_file = os.path.join(out_dir, f"{base_name}_{chunk_name}.parquet")
        # skip if already exists
        if os.path.exists(final_file):
            print(f"    → Chunk {chunk_name} skipped (already exists): {final_file}")
            current = next_day
            continue

        qry = f"""
            SELECT * FROM {table}
            WHERE LOAD_TIMESTAMP >= '{current}' AND LOAD_TIMESTAMP < '{next_day}'
        """
        df = read_sql_with_retry(qry, conn)
        if not df.empty:
            atomic_write_parquet(df, final_file)
        else:
            print(f"    → Chunk {chunk_name}: no data")
        current = next_day


def main():
    mode = MODE.lower()
    if mode not in ("full", "fresh"):
        raise ValueError("MODE must be 'full' or 'fresh'")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    last_migration = load_last_migration()
    print(f"Mode: {mode}. Last migration: {last_migration}")

    conn = connect()

    try:
        print("Saving schema DDLs...")
        for schema in SCHEMAS:
            schema_dir = os.path.join(OUTPUT_DIR, schema)
            save_schema_ddl(conn, schema, schema_dir)

        for schema in SCHEMAS:
            print(f"\n--- Schema: {schema}")
            schema_dir = os.path.join(OUTPUT_DIR, schema)
            os.makedirs(schema_dir, exist_ok=True)

            tables = get_tables_with_sizes(conn, schema)
            for table_name, size_bytes in tables:
                size_mb = round(size_bytes / (1024 * 1024), 2) if size_bytes is not None else 0
                print(f"  • {table_name} ({size_mb} MB)")
                table_ref = f"{SF_DATABASE}.{schema}.{table_name}"
                base_path = os.path.join(schema_dir, table_name)

                # Check if table has LOAD_TIMESTAMP column
                cs = conn.cursor()
                try:
                    cs.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s", (schema, table_name))
                    columns = [row[0] for row in cs.fetchall()]
                finally:
                    cs.close()

                has_load_timestamp = "LOAD_TIMESTAMP" in columns

                if mode == "fresh":
                    if not has_load_timestamp:
                        # No LOAD_TIMESTAMP column, always do full export regardless of size
                        out_path = base_path + ".parquet"
                        export_full_table(conn, table_ref, out_path)
                    else:
                        if last_migration is None:
                            print("    → No last migration timestamp found, doing full export for this table.")
                            if size_mb <= MAX_SIZE_MB:
                                out_path = base_path + ".parquet"
                                export_full_table(conn, table_ref, out_path)
                            else:
                                export_by_timestamp_chunks(conn, table_ref, schema_dir, table_name)
                        else:
                            if size_mb <= MAX_SIZE_MB:
                                print("    → Table size is below the limit, doing full export for this table.")
                                out_path = base_path + ".parquet"
                                export_full_table(conn, table_ref, out_path)
                            else:
                                export_incremental_table(conn, table_ref, schema_dir, table_name, last_migration)
                else:  # full
                    if size_mb <= MAX_SIZE_MB:
                        out_path = base_path + ".parquet"
                        export_full_table(conn, table_ref, out_path)
                    else:
                        export_by_timestamp_chunks(conn, table_ref, schema_dir, table_name)

    finally:
        try:
            conn.close()
        except Exception:
            pass

    now = datetime.utcnow()
    save_migration_timestamp(now)
    print(f"\n✅ All exports complete. Saved migration timestamp: {now.isoformat()}")


if __name__ == "__main__":
    main()