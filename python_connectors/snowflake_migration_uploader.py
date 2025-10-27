#!/usr/bin/env python3
"""
Uploader that:
- Ensures schema exists and runs saved DDL if present
- Creates a schema-scoped named stage and a file format preserving Parquet logical types
- PUTs parquet files to the named stage
- Runs COPY INTO target tables using FILE_FORMAT with USE_LOGICAL_TYPE = TRUE
  and MATCH_BY_COLUMN_NAME = TRUE so columns are matched by name (no manual column casting)
- Preserves full table names and strips only timestamp suffixes _YYYY-MM-DD or _YYYYmmddTHHMMSSZ
Notes:
- User must have privileges to CREATE FILE FORMAT, CREATE STAGE, PUT to stages, and COPY INTO tables.
- config.yml (in the same dir) must contain snowflake connection settings under key "snowflake"
"""
import os
import re
from pathlib import Path

import yaml
import snowflake.connector

# === CONFIG ===
EXPORTS_DIR = r"g:\Archiwum\Projekcik\snowflake_exports"
SF_DATABASE = "GTFS_TEST"
FILE_FORMAT = "PARQUET_LOGICAL"
# stage name will be created inside the schema: e.g. STAGE_parquet (schema-scoped)
STAGE_SUFFIX = "_PARQUET_STAGE"
# only strip timestamp-like suffixes from filenames when deriving table name
_SUFFIX_RE = re.compile(r"(_\d{4}-\d{2}-\d{2}|_\d{8}T\d{6}Z)$", re.IGNORECASE)
# === /CONFIG ===

# load credentials
with open(Path(__file__).parent / "config.yml", "r") as f:
    cfg = yaml.safe_load(f)
sf = cfg["snowflake"]


def connect():
    return snowflake.connector.connect(
        user=sf["user"],
        password=sf["password"],
        account=sf["account"],
        database=sf["database"],
        warehouse=sf["warehouse"],
        network_timeout=120,
    )


def filename_to_table_name(fname: str) -> str:
    stem = Path(fname).stem
    return _SUFFIX_RE.sub("", stem)


def ensure_db_schema(cs, schema: str):
    cs.execute(f'USE DATABASE "{SF_DATABASE}"')
    cs.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    cs.execute(f'USE SCHEMA "{schema}"')


def ensure_file_format_and_stage(cs, schema: str):
    # Create file format preserving Parquet logical types
    cs.execute(
        f"""
        CREATE FILE FORMAT IF NOT EXISTS "{FILE_FORMAT}"
        TYPE = PARQUET
        USE_LOGICAL_TYPE = TRUE
        """
    )
    # Create a schema-scoped stage name
    stage_name = f'"{schema}"."{schema + STAGE_SUFFIX}"'  # fully qualified for safety if needed later
    # Create stage within current schema (unqualified name will be schema-scoped as well)
    cs.execute(f'CREATE STAGE IF NOT EXISTS "{schema + STAGE_SUFFIX}" FILE_FORMAT = (FORMAT_NAME = \'{FILE_FORMAT}\')')
    # return the unquoted stage name to use with @<stage>
    return schema + STAGE_SUFFIX


def apply_saved_ddl_if_present(cs, schema_dir: str, schema: str):
    ddl_path = Path(schema_dir) / f"{schema}_ddl.sql"
    if not ddl_path.exists():
        return
    ddl_text = ddl_path.read_text(encoding="utf-8")
    stmts = [s.strip() for s in ddl_text.split(";") if s.strip()]
    for stmt in stmts:
        try:
            cs.execute(stmt)
        except Exception:
            # ignore errors (objects may already exist or statements may be qualified)
            pass


def put_file_to_stage(cs, local_path: str, stage_name: str):
    # stage_name is the schema-scoped stage identifier (unquoted)
    # Use session schema, then PUT to @<stage_name>
    # Ensure DB and schema are set in session before calling this
    staged_target = f'@{stage_name}'
    put_sql = f"PUT file://{local_path} {staged_target} PARALLEL = 4 AUTO_COMPRESS = FALSE"
    cs.execute(put_sql)
    # return the staged filename(s) - PUT returns responses but connector doesn't return them easily,
    # we'll use the local filename for FILES clause below
    return Path(local_path).name


def copy_from_stage(cs, schema: str, table_name: str, stage_name: str, staged_file_name: str):
    # Ensure DB and schema in session
    cs.execute(f'USE DATABASE "{SF_DATABASE}"')
    cs.execute(f'USE SCHEMA "{schema}"')
    # Use MATCH_BY_COLUMN_NAME = TRUE so columns from Parquet are matched by name
    copy_sql = f"""
        COPY INTO "{SF_DATABASE}"."{schema}"."{table_name}"
        FROM @{stage_name}
        FILES = ('{staged_file_name}')
        FILE_FORMAT = (FORMAT_NAME = '{FILE_FORMAT}')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'ABORT_STATEMENT'
    """
    cs.execute(copy_sql)


def process_schema(conn, schema_dir: str, schema: str):
    cs = conn.cursor()
    try:
        ensure_db_schema(cs, schema)
        # create file format and stage (in this schema) and apply DDL if present
        stage_name = ensure_file_format_and_stage(cs, schema)
        apply_saved_ddl_if_present(cs, schema_dir, schema)
    finally:
        cs.close()

    for fname in sorted(os.listdir(schema_dir)):
        if not fname.lower().endswith(".parquet"):
            continue
        local_path = os.path.join(schema_dir, fname)
        table_name = filename_to_table_name(fname)
        print(f"Processing {local_path} -> {schema}.{table_name}")

        cs2 = conn.cursor()
        try:
            # Ensure session in proper DB/SCHEMA for PUT and COPY
            cs2.execute(f'USE DATABASE "{SF_DATABASE}"')
            cs2.execute(f'USE SCHEMA "{schema}"')

            try:
                staged_file = put_file_to_stage(cs2, local_path, f'{schema + STAGE_SUFFIX}')
            except Exception as e:
                print(f"  ⚠ PUT failed: {e}")
                continue

            try:
                copy_from_stage(cs2, schema, table_name, f'{schema + STAGE_SUFFIX}', staged_file)
                print("  → COPY OK")
            except Exception as e:
                print(f"  ⚠ COPY failed: {e}")
        finally:
            try:
                cs2.close()
            except Exception:
                pass


def main():
    conn = connect()
    try:
        for schema in sorted(os.listdir(EXPORTS_DIR)):
            schema_dir = os.path.join(EXPORTS_DIR, schema)
            if not os.path.isdir(schema_dir):
                continue
            process_schema(conn, schema_dir, schema)
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
