#!/usr/bin/env python3
"""
Uploader that:
- Ensures schema exists and runs saved DDL if present (safe: CREATE TABLE IF NOT EXISTS)
- Creates a schema-scoped named stage and a file format preserving Parquet logical types
- PUTs parquet files to the named stage in parallel (ThreadPoolExecutor)
- Runs batch COPY INTO target tables (up to COPY_BATCH_SIZE files per statement)
- Preserves full table names and strips only timestamp suffixes _YYYY-MM-DD or _YYYYmmddTHHMMSSZ

Skip logic (per table):
  - New table (not yet in Snowflake): upload all files
  - Static table (only base .parquet, no date partitions): TRUNCATE + reload
  - Chunked table (only date-partitioned files): skip dates already in Snowflake via
    SELECT DISTINCT DATE(LOAD_TIMESTAMP)
  - Mixed table (base file + date files): date-files use date-based skip; base file
    checked by reading its LOAD_TIMESTAMP dates against what's already loaded

Notes:
- User must have privileges to CREATE FILE FORMAT, CREATE STAGE, PUT to stages, COPY INTO,
  TRUNCATE TABLE, and CREATE TABLE.
- config.yml (in the same dir) must contain snowflake connection settings under key "snowflake"
- Snowflake Python connector is thread-safe; cursors from one connection can be used in parallel
  threads. PUT uses fully-qualified stage names to avoid USE DATABASE/SCHEMA session-state races.
"""
import datetime
import logging
import os
import re
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import yaml
import snowflake.connector

with open(Path(__file__).parent / "config.yml", "r") as f:
    cfg = yaml.safe_load(f)
sf = cfg["snowflake"]

# === CONFIG ===
EXPORTS_DIR = cfg["exports_dir"]
SF_DATABASE = "GTFS_TEST"
FILE_FORMAT = "PARQUET_LOGICAL"
STAGE_SUFFIX = "_PARQUET_STAGE"
PUT_WORKERS = 8       # parallel threads for PUT (S3 uploads are network-bound)
COPY_BATCH_SIZE = 50  # files per COPY INTO statement
_SUFFIX_RE = re.compile(r"(_\d{4}-\d{2}-\d{2}|_\d{8}T\d{6}Z)$", re.IGNORECASE)
# === /CONFIG ===

# === LOGGING ===
_log_dir = Path(__file__).parent / "logs"
_log_dir.mkdir(exist_ok=True)
_log_file = _log_dir / f"uploader_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(_log_file, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)
log.info(f"Log file: {_log_file}")
# === /LOGGING ===


def connect():
    return snowflake.connector.connect(
        user=sf["user"],
        password=sf["password"],
        account=sf["account"],
        database=sf["database"],
        warehouse=sf["warehouse"],
        network_timeout=900,
    )


def filename_to_table_name(fname: str) -> str:
    stem = Path(fname).stem
    return _SUFFIX_RE.sub("", stem)


def date_from_filename(fname: str):
    m = re.search(r'_(\d{4}-\d{2}-\d{2})\.parquet$', fname, re.IGNORECASE)
    return datetime.date.fromisoformat(m.group(1)) if m else None


def ensure_db_schema(cs, schema: str):
    cs.execute(f'USE DATABASE "{SF_DATABASE}"')
    cs.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    cs.execute(f'USE SCHEMA "{schema}"')


def ensure_file_format_and_stage(cs, schema: str):
    cs.execute(f"""
        CREATE FILE FORMAT IF NOT EXISTS "{FILE_FORMAT}"
        TYPE = PARQUET
        USE_LOGICAL_TYPE = TRUE
    """)
    cs.execute(
        f'CREATE STAGE IF NOT EXISTS "{schema + STAGE_SUFFIX}"'
        f" FILE_FORMAT = (FORMAT_NAME = '{FILE_FORMAT}')"
    )
    return schema + STAGE_SUFFIX


def apply_saved_ddl_if_present(cs, schema_dir: str, schema: str):
    ddl_path = Path(schema_dir) / f"{schema}_ddl.sql"
    if not ddl_path.exists():
        return
    ddl_text = ddl_path.read_text(encoding="utf-8")
    stmts = [s.strip() for s in ddl_text.split(";") if s.strip()]
    for stmt in stmts:
        stmt_safe = re.sub(
            r'create\s+or\s+replace\s+table',
            'CREATE TABLE IF NOT EXISTS',
            stmt,
            flags=re.IGNORECASE,
        )
        try:
            cs.execute(stmt_safe)
        except Exception:
            pass


def get_existing_tables(cs, schema: str) -> set:
    cs.execute("""
        SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
    """, (schema,))
    return {row[0] for row in cs.fetchall()}


def get_loaded_dates(cs, schema: str, table_name: str) -> set:
    cs.execute(
        f'SELECT DISTINCT DATE(LOAD_TIMESTAMP) FROM "{SF_DATABASE}"."{schema}"."{table_name}"'
    )
    return {row[0] for row in cs.fetchall() if row[0] is not None}


def get_parquet_dates(filepath: str) -> set:
    df = pd.read_parquet(filepath, columns=['LOAD_TIMESTAMP'])
    return set(pd.to_datetime(df['LOAD_TIMESTAMP']).dt.date.unique())


def put_files_parallel(conn, schema: str, stage_name: str, schema_dir: str, fnames: list) -> list:
    """PUT files to stage in parallel. Returns list of staged filenames for successful PUTs."""
    staged = []

    # Fully-qualified stage avoids USE DATABASE/USE SCHEMA session-state races across threads
    fq_stage = f'@"{SF_DATABASE}"."{schema}"."{stage_name}"'

    def put_one(fname):
        local_path = os.path.join(schema_dir, fname)
        cs = conn.cursor()
        try:
            cs.execute(f"PUT file://{local_path} {fq_stage} PARALLEL = 4 AUTO_COMPRESS = TRUE")
            return Path(local_path).name, None
        except Exception as e:
            return fname, e
        finally:
            try:
                cs.close()
            except Exception:
                pass

    failures = []
    with ThreadPoolExecutor(max_workers=PUT_WORKERS) as executor:
        futures = {executor.submit(put_one, fname): fname for fname in fnames}
        for future in as_completed(futures):
            staged_name, err = future.result()
            if err:
                log.error(f"PUT failed for {futures[future]}: {err}")
                failures.append(futures[future])
            else:
                staged.append(staged_name)

    if failures:
        raise RuntimeError(f"PUT failed for {len(failures)} file(s): {failures}")

    return staged


def copy_batch(cs, schema: str, table_name: str, stage_name: str, staged_files: list):
    """COPY INTO from multiple staged files using async polling (avoids network_timeout on large batches)."""
    cs.execute(f'USE DATABASE "{SF_DATABASE}"')
    cs.execute(f'USE SCHEMA "{schema}"')
    files_clause = ", ".join(f"'{f}'" for f in staged_files)
    cs.execute_async(f"""
        COPY INTO "{SF_DATABASE}"."{schema}"."{table_name}"
        FROM @{stage_name}
        FILES = ({files_clause})
        FILE_FORMAT = (FORMAT_NAME = '{FILE_FORMAT}')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'ABORT_STATEMENT'
    """)
    query_id = cs.sfqid
    conn = cs.connection
    interval = 1
    while conn.is_still_running(conn.get_query_status(query_id)):
        time.sleep(interval)
        interval = min(interval * 2, 30)
    conn.get_query_status_throw_if_error(query_id)


def upload_table_files(
    conn, schema: str, stage_name: str, schema_dir: str, fnames: list, table_name: str
):
    """PUT all files in parallel, then COPY in batches of COPY_BATCH_SIZE."""
    if not fnames:
        return
    log.info(f"Uploading {len(fnames)} file(s) for {table_name}...")
    staged = put_files_parallel(conn, schema, stage_name, schema_dir, fnames)
    cs = conn.cursor()
    try:
        total_batches = (len(staged) + COPY_BATCH_SIZE - 1) // COPY_BATCH_SIZE
        for i in range(0, len(staged), COPY_BATCH_SIZE):
            batch = staged[i:i + COPY_BATCH_SIZE]
            batch_num = i // COPY_BATCH_SIZE + 1
            copy_batch(cs, schema, table_name, stage_name, batch)
            log.info(f"COPY OK: batch {batch_num}/{total_batches} ({len(batch)} files)")
    finally:
        cs.close()


def process_schema(conn, schema_dir: str, schema: str):
    cs = conn.cursor()
    try:
        ensure_db_schema(cs, schema)
        stage_name = ensure_file_format_and_stage(cs, schema)
        apply_saved_ddl_if_present(cs, schema_dir, schema)
        existing_tables = get_existing_tables(cs, schema)
    finally:
        cs.close()

    table_files = defaultdict(lambda: {'base': None, 'date_files': []})
    for fname in sorted(os.listdir(schema_dir)):
        if not fname.lower().endswith('.parquet'):
            continue
        tname = filename_to_table_name(fname)
        if date_from_filename(fname):
            table_files[tname]['date_files'].append(fname)
        else:
            table_files[tname]['base'] = fname

    for table_name, files in sorted(table_files.items()):
        base_file = files['base']
        date_files = sorted(files['date_files'])
        table_exists = table_name in existing_tables
        to_upload = []

        log.info(f"[{schema}.{table_name}] base={base_file}, date_files={len(date_files)}, exists={table_exists}")

        if not table_exists:
            if base_file:
                to_upload.append(base_file)
            to_upload.extend(date_files)

        elif not date_files:
            # Static table — TRUNCATE then reload
            cs2 = conn.cursor()
            try:
                cs2.execute(f'TRUNCATE TABLE "{SF_DATABASE}"."{schema}"."{table_name}"')
                log.info(f"Truncated {table_name}")
            finally:
                cs2.close()
            if base_file:
                to_upload.append(base_file)

        else:
            # Chunked table — skip dates already present in Snowflake
            cs3 = conn.cursor()
            try:
                loaded_dates = get_loaded_dates(cs3, schema, table_name)
            finally:
                cs3.close()
            log.info(f"Loaded dates in Snowflake for {table_name}: {len(loaded_dates)}")

            for fname in date_files:
                file_date = date_from_filename(fname)
                if file_date in loaded_dates:
                    log.info(f"Skipping {fname} (date {file_date} already in Snowflake)")
                else:
                    to_upload.append(fname)

            if base_file:
                base_path = os.path.join(schema_dir, base_file)
                try:
                    base_dates = get_parquet_dates(base_path)
                except Exception as e:
                    log.warning(f"Could not read dates from {base_file}: {e}, uploading anyway")
                    base_dates = set()
                if base_dates and base_dates.issubset(loaded_dates):
                    log.info(f"Skipping {base_file} (all dates already in Snowflake)")
                else:
                    to_upload.append(base_file)

        upload_table_files(conn, schema, stage_name, schema_dir, to_upload, table_name)


def main():
    conn = connect()
    try:
        for schema in sorted(os.listdir(EXPORTS_DIR)):
            schema_dir = os.path.join(EXPORTS_DIR, schema)
            if not os.path.isdir(schema_dir):
                continue
            log.info(f"=== Schema: {schema} ===")
            process_schema(conn, schema_dir, schema)
    finally:
        try:
            conn.close()
        except Exception:
            pass
    log.info("Upload complete.")


if __name__ == "__main__":
    main()
