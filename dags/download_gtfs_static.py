import os
import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonVirtualenvOperator
import json


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}


def create_table_in_snowflake(ctx, table_name, df, schema):
    """
    Creates a table in Snowflake (in the given schema) based on the DataFrame's structure.
    """
    ddl = generate_create_table_ddl(table_name, df, schema)
    print(f"Creating table {schema}.{table_name.upper()} ...")
    cs_local = ctx.cursor()
    cs_local.execute(ddl)
    cs_local.close()
    print(f"Table {schema}.{table_name.upper()} created.")


def generate_create_table_ddl(table_name, df, schema):
    """
    Generates a CREATE OR REPLACE TABLE DDL command based on a DataFrame's structure.
    Mapping: integer -> NUMBER, float -> FLOAT, datetime -> TIMESTAMP_NTZ, others -> VARCHAR.
    Column names are converted to uppercase.
    Special case: the "load_timestamp" column is always mapped to TIMESTAMP_NTZ.
    """
    col_defs = []
    for col, dtype in df.dtypes.items():
        if col.lower() == "load_timestamp":
            sf_type = "TIMESTAMP_NTZ"
        else:
            dtype_str = str(dtype).lower()
            if "int" in dtype_str:
                sf_type = "NUMBER"
            elif "float" in dtype_str:
                sf_type = "FLOAT"
            elif "datetime" in dtype_str:
                sf_type = "TIMESTAMP_NTZ"
            else:
                sf_type = "VARCHAR"
        col_defs.append(f'"{col.upper()}" {sf_type}')
    ddl = f'CREATE TABLE IF NOT EXISTS {schema}.{table_name.upper()} (\n  ' + ',\n  '.join(col_defs) + '\n);'
    return ddl


def _apply_scd2(cs, stg_table, target_table, columns, pks, logical_date_col, exclude_cols):
    for scd2_col in ("VALID_FROM TIMESTAMP_NTZ", "VALID_TO TIMESTAMP_NTZ", "IS_CURRENT BOOLEAN"):
        cs.execute(f"ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS {scd2_col};")

    cols_to_hash = [c for c in columns if c not in pks and c not in exclude_cols]
    if not cols_to_hash:
        raise ValueError(f"No trackable columns for SCD2 on {target_table}.")

    join = " AND ".join(f"tgt.{pk} = stg.{pk}" for pk in pks)
    join_any = " AND ".join(f"existing.{pk} = stg.{pk}" for pk in pks)
    tgt_hash = "HASH(" + ", ".join(f"tgt.{c}" for c in cols_to_hash) + ")"
    stg_hash = "HASH(" + ", ".join(f"stg.{c}" for c in cols_to_hash) + ")"
    cols_str = ", ".join(columns)
    stg_cols = ", ".join(f"stg.{c}" for c in columns)

    latest_ts_subquery = f"(SELECT MAX(LOAD_TIMESTAMP) FROM {stg_table})"

    update_sql = f"""
        UPDATE {target_table} tgt
        SET tgt.VALID_TO = stg.{logical_date_col}, tgt.IS_CURRENT = FALSE
        FROM {stg_table} stg
        WHERE {join} AND tgt.IS_CURRENT = TRUE AND {tgt_hash} != {stg_hash}
          AND stg.LOAD_TIMESTAMP = {latest_ts_subquery}
    """
    insert_sql = f"""
        INSERT INTO {target_table} ({cols_str}, VALID_FROM, VALID_TO, IS_CURRENT)
        SELECT {stg_cols},
            CASE WHEN NOT EXISTS (
                SELECT 1 FROM {target_table} existing WHERE {join_any}
            ) THEN '1900-01-01'::TIMESTAMP_NTZ
            ELSE stg.{logical_date_col}
            END,
            NULL,
            TRUE
        FROM {stg_table} stg
        WHERE stg.LOAD_TIMESTAMP = {latest_ts_subquery}
          AND NOT EXISTS (
            SELECT 1 FROM {target_table} tgt
            WHERE {join} AND tgt.IS_CURRENT = TRUE AND {tgt_hash} = {stg_hash}
        )
    """
    try:
        cs.execute("BEGIN")
        cs.execute(update_sql)
        cs.execute(insert_sql)
        cs.execute("COMMIT")
    except Exception as e:
        cs.execute("ROLLBACK")
        raise RuntimeError(f"SCD2 transaction failed for {target_table}: {e}") from e


with DAG(
    dag_id='gtfs_download_static',
    default_args=default_args,
    description='Download GTFS data for Kraków and save tables as CSV files',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    def merge_gtfs_feeds(logical_date):
        """
        Downloads GTFS feeds from the provided URLs using gtfs_kit
        and merges the common tables: routes, trips, stop_times, stops,
        calendar (if available) and calendar_dates (if available).
        Additionally, each feed is tagged with a "mode" field using a hardcoded letter.
        """
        import pandas as pd
        import gtfs_kit as gk

        GTFS_FEEDS = [
            ("https://gtfs.ztp.krakow.pl/GTFS_KRK_A.zip", "A"),
            ("https://gtfs.ztp.krakow.pl/GTFS_KRK_M.zip", "M"),
            ("https://gtfs.ztp.krakow.pl/GTFS_KRK_T.zip", "T"),
        ]
        TABLES = ["routes", "trips", "stop_times", "stops", "calendar", "calendar_dates", "shapes"]

        all_tables = {table: [] for table in TABLES}
        first_feed = None

        for url, mode in GTFS_FEEDS:
            print(f"Downloading data from: {url}")
            feed = gk.read_feed(url, dist_units="km")
            if first_feed is None:
                first_feed = feed
            for table in TABLES:
                df = getattr(feed, table, None)
                if df is not None:
                    df["mode"] = mode
                    all_tables[table].append(df)

        for table in TABLES:
            dfs = all_tables[table]
            if not dfs:
                continue
            merged = pd.concat(dfs, ignore_index=True).drop_duplicates()
            merged["load_timestamp"] = logical_date
            setattr(first_feed, table, merged)

        return first_feed


    def save_static_data_to_snowflake(ctx, merged_feed):
        """
        Creates tables in the SCHEDULE schema and uploads static GTFS data.
        The tables are loaded into STAGING tables with _STG suffix.
        Before uploading, the staging tables are truncated.
        """
        cs = ctx.cursor()
        tables = ["routes", "trips", "stop_times", "stops"]
        if hasattr(merged_feed, "calendar"):
            tables.append("calendar")
        if hasattr(merged_feed, "calendar_dates"):
            tables.append("calendar_dates")
        if hasattr(merged_feed, "shapes"):
            tables.append("shapes")
            
        for table in tables:
            df = getattr(merged_feed, table).reset_index(drop=True)
            df = df.rename(columns=lambda x: x.upper())
            
            for col in df.select_dtypes(include=['datetimetz']).columns:
                df[col] = df[col].dt.tz_localize(None)
            
            stg_table = f"{table}_STG"
            
            create_table_in_snowflake(ctx, stg_table, df, "SCHEDULE")
            cs.execute("USE SCHEMA SCHEDULE")

            print(f"Uploading static data for table {stg_table.upper()} to Snowflake...")
            
            success, nchunks, nrows, _ = write_pandas(
                ctx, df, stg_table.upper(), auto_create_table=False, use_logical_type=True
            )
            
            if success:
                print(f"Table {stg_table.upper()} uploaded successfully: {nrows} rows in {nchunks} chunks.\n")
            else:
                print(f"Upload failed for table {stg_table.upper()}.\n")


    def ingest_static_data_to_snowflake(**kwargs):
        """
        Downloads and merges the GTFS feeds and uploads the merged static data to Snowflake staging tables.
        """
        merged_feed = merge_gtfs_feeds(kwargs['logical_date'])
        hook = SnowflakeHook(snowflake_conn_id='my_snowflake_conn')
        conn = hook.get_conn()
        try:
            save_static_data_to_snowflake(conn, merged_feed)
        finally:
            conn.close()


    def load_data_from_staging(**kwargs):
        hook = SnowflakeHook(snowflake_conn_id='my_snowflake_conn')
        conn = hook.get_conn()
        cs = conn.cursor()

        try:
            cs.execute("USE SCHEMA SCHEDULE")

            config_path = os.path.join(os.path.dirname(__file__), "gtfs_static_config.json")
            with open(config_path) as f:
                table_config = json.load(f)

            for table, config in table_config.items():
                stg_table = f"{table}_STG"
                pks = [pk.upper() for pk in config["pks"]]
                use_scd2 = config.get("use_scd2", False)
                target_table = f"{table}_SCD2" if use_scd2 else table

                cs.execute(f"SELECT * FROM {stg_table} LIMIT 1")
                columns = [col[0].upper() for col in cs.description]

                cs.execute(f"CREATE TABLE IF NOT EXISTS {target_table} CLONE {stg_table};")

                if use_scd2:
                    logical_date_col = config.get("logical_date_col", "LOAD_TIMESTAMP").upper()
                    exclude_cols = {col.upper() for col in config.get("scd2_exclude_cols", [])} | {logical_date_col}
                    _apply_scd2(cs, stg_table, target_table, columns, pks, logical_date_col, exclude_cols)
                    print(f"SCD2 applied for {table}.")
                else:
                    cols = ", ".join(columns)
                    cs.execute(f"""
                        INSERT INTO {target_table} ({cols})
                        SELECT {cols} FROM {stg_table}
                        WHERE LOAD_TIMESTAMP = (SELECT MAX(LOAD_TIMESTAMP) FROM {stg_table})
                    """)
                    print(f"Simple INSERT completed for {table}.")
        finally:
            conn.close()
    
    download_gtfs_task = PythonOperator(
        task_id='download_gtfs_data',
        python_callable=ingest_static_data_to_snowflake,
    )

    load_data_from_staging_task = PythonOperator(
        task_id='load_data_from_staging',
        python_callable=load_data_from_staging,
    )

    def run_dbt(**context):
        from dbt.cli.main import dbtRunner, dbtRunnerResult

        runner = dbtRunner()

        result: dbtRunnerResult = runner.invoke([
            "run",
            "--project-dir", "/opt/airflow/dbt/gtfs_project",
            "--profiles-dir", "/opt/airflow/dbt",
            "--vars", f'{{"execution_date": "{context["execution_date"]}"}}',
        ])

        if not result.success:
            raise RuntimeError("dbt run failed")

    bash_dbt_operator = PythonVirtualenvOperator(
        task_id="run_dbt",
        python_callable=run_dbt,
        requirements=[
            "dbt-snowflake==1.9.4"
        ],
        system_site_packages=False,
        op_kwargs={
            "execution_date": "{{ ds }}"
        },
        dag=dag,
    )

    download_gtfs_task >> load_data_from_staging_task >> bash_dbt_operator
