import os
import pandas as pd
import snowflake.connector
from datetime import timedelta
import yaml
import snowflake.connector

# Load config.yml
with open("config.yml", "r") as f:
    cfg = yaml.safe_load(f)

sf = cfg["snowflake"]

SCHEMAS = ['SCHEDULE', 'TRIP_UPDATES']
OUTPUT_DIR = './snowflake_exports'
MAX_SIZE_MB = 50

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    conn = snowflake.connector.connect(
        user=sf["user"],
        password=sf["password"],
        account=sf["account"],
        database=sf["database"],
        warehouse=sf["warehouse"]
    )
    cs = conn.cursor()

    for schema in SCHEMAS:
        print(f"\n--- Schema: {schema}")
        schema_dir = os.path.join(OUTPUT_DIR, schema)
        os.makedirs(schema_dir, exist_ok=True)

        # Get table names and their sizes
        cs.execute(f"""
            SELECT table_name, bytes
            FROM {SF_DATABASE}.information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
        """, (schema,))
        tables = cs.fetchall()

        for table_name, size_bytes in tables:
            size_mb = round(size_bytes / (1024 * 1024), 2)
            print(f"  • {table_name} ({size_mb} MB)")

            table_ref = f"{SF_DATABASE}.{schema}.{table_name}"
            base_path = os.path.join(schema_dir, table_name)

            if size_mb <= MAX_SIZE_MB:
                df = pd.read_sql(f"SELECT * FROM {table_ref}", conn)
                out_path = base_path + ".csv"
                df.to_csv(out_path, index=False)
                print(f"    → Exported to {out_path}")
            else:
                export_by_timestamp_chunks(conn, table_ref, schema_dir, table_name)

    cs.close()
    conn.close()
    print("\n✅ All exports complete.")

def export_by_timestamp_chunks(conn, table, out_dir, base_name):
    # Determine min and max LOAD_TIMESTAMP values
    qry = f"SELECT MIN(LOAD_TIMESTAMP), MAX(LOAD_TIMESTAMP) FROM {table}"
    min_ts, max_ts = pd.read_sql(qry, conn).iloc[0]

    if pd.isnull(min_ts) or pd.isnull(max_ts):
        print(f"    ⚠ Could not determine LOAD_TIMESTAMP range, skipping chunking.")
        return

    current = min_ts
    while current <= max_ts:
        next_day = current + timedelta(days=1)
        chunk_name = current.strftime('%Y-%m-%d')
        out_file = os.path.join(out_dir, f"{base_name}_{chunk_name}.csv")

        qry = f"""
            SELECT * FROM {table}
            WHERE LOAD_TIMESTAMP >= '{current}' AND LOAD_TIMESTAMP < '{next_day}'
        """
        df = pd.read_sql(qry, conn)
        if not df.empty:
            df.to_csv(out_file, index=False)
            print(f"    → Chunk {chunk_name}: {len(df)} rows → {out_file}")
        else:
            print(f"    → Chunk {chunk_name}: no data")
        current = next_day

if __name__ == '__main__':
    main()