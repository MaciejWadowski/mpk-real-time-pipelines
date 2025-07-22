#!/usr/bin/env python3
"""
snowflake_migration_uploader.py

Scans CSVs under ./snowflake_exports/<SCHEMA>/ and inserts them
into the matching tables in the target Snowflake account.
Existing data is preserved; no tables are dropped.

This version auto‐casts all object columns to str to avoid PyArrow type errors.
"""

import os
import re
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ─────────────────────────────────────────────────────────────────────────────

# Local directory with exported CSVs
EXPORTS_DIR = './snowflake_exports'
# ─────────────────────────────────────────────────────────────────────────────

# Load config.yml
with open("config.yml", "r") as f:
    cfg = yaml.safe_load(f)

sf = cfg["snowflake"]

def main():
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=sf["user"],
        password=sf["password"],
        account=sf["account"],
        database=sf["database"],
        warehouse=sf["warehouse"]
    )
    cs = conn.cursor()

    for schema in os.listdir(EXPORTS_DIR):
        schema_dir = os.path.join(EXPORTS_DIR, schema)
        if not os.path.isdir(schema_dir):
            continue

        print(f"\n=== SCHEMA: {schema} ===")
        cs.execute(f"USE SCHEMA {SF_DATABASE}.{schema}")

        # find all csv files
        csv_files = [f for f in os.listdir(schema_dir) if f.lower().endswith('.csv')]
        # group by base table name
        pattern = re.compile(r"(?P<base>.+?)(?:_\d{4}-\d{2}-\d{2})?\.csv$")
        groups = {}
        for fname in csv_files:
            m = pattern.match(fname)
            if not m:
                continue
            base = m.group('base')
            groups.setdefault(base, []).append(fname)

        for table_name, files in groups.items():
            # full dump first, else date‐sorted chunks
            full = f"{table_name}.csv"
            job_files = [full] if full in files else sorted(files)

            print(f"\n-- Inserting into {schema}.{table_name} ({len(job_files)} file(s))")
            for fname in job_files:
                path = os.path.join(schema_dir, fname)
                print(f"  • Reading {fname} ... ", end='', flush=True)
                # disable low_memory to consolidate dtype
                df = pd.read_csv(path, low_memory=False)

                # force any object‐dtype column to string
                for col in df.select_dtypes(include=['object']).columns:
                    df[col] = df[col].astype(str)

                # append to existing table
                success, nchunks, nrows, _ = write_pandas(
                    conn,
                    df,
                    table_name=table_name,
                    schema=schema,
                    database=SF_DATABASE,
                    auto_create_table=False,
                    overwrite=False
                )
                status = "OK" if success else "FAILED"
                print(f"{status} | {nrows} rows in {nchunks} chunk(s)")

    cs.close()
    conn.close()
    print("\n✅ All data inserted into Snowflake successfully.")

if __name__ == '__main__':
    main()
