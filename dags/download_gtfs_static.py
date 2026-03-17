import os
import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonVirtualenvOperator


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


with DAG(
    dag_id='gtfs_download_static',
    default_args=default_args,
    description='Download GTFS data for Kraków and save tables as CSV files',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    def merge_gtfs_feeds(logical_date):
        import pandas as pd
        import gtfs_kit as gk
        """
        Downloads GTFS feeds from the provided URLs using gtfs_kit
        and merges the common tables: routes, trips, stop_times, stops,
        calendar (if available) and calendar_dates (if available).
        Additionally, each feed is tagged with a "mode" field using a hardcoded letter.
        """
        GTFS_FEEDS = [
            ("https://gtfs.ztp.krakow.pl/GTFS_KRK_A.zip", "A"),
            ("https://gtfs.ztp.krakow.pl/GTFS_KRK_M.zip", "M"),
            ("https://gtfs.ztp.krakow.pl/GTFS_KRK_T.zip", "T")
        ]
        feeds = []
        for url, mode_letter in GTFS_FEEDS:
            print(f"Downloading data from: {url}")
            feed = gk.read_feed(url, dist_units="km")
            
            if hasattr(feed, "routes"):
                feed.routes["mode"] = mode_letter
            if hasattr(feed, "trips"):
                feed.trips["mode"] = mode_letter
            if hasattr(feed, "stop_times"):
                feed.stop_times["mode"] = mode_letter
            if hasattr(feed, "stops"):
                feed.stops["mode"] = mode_letter
            if hasattr(feed, "calendar"):
                feed.calendar["mode"] = mode_letter
            if hasattr(feed, "calendar_dates"):
                feed.calendar_dates["mode"] = mode_letter
            if hasattr(feed, "shapes"):
                feed.shapes["mode"] = mode_letter

            feeds.append(feed)

        merged_feed = feeds[0]
        for feed in feeds[1:]:
            merged_feed.routes = pd.concat([merged_feed.routes, feed.routes], ignore_index=True).drop_duplicates()
            merged_feed.trips = pd.concat([merged_feed.trips, feed.trips], ignore_index=True).drop_duplicates()
            merged_feed.stop_times = pd.concat([merged_feed.stop_times, feed.stop_times], ignore_index=True).drop_duplicates()
            merged_feed.stops = pd.concat([merged_feed.stops, feed.stops], ignore_index=True).drop_duplicates()

            if hasattr(merged_feed, "calendar") and hasattr(feed, "calendar"):
                merged_feed.calendar = pd.concat([merged_feed.calendar, feed.calendar], ignore_index=True).drop_duplicates()
            elif not hasattr(merged_feed, "calendar") and hasattr(feed, "calendar"):
                merged_feed.calendar = feed.calendar.copy().drop_duplicates()

            if hasattr(feed, "calendar_dates"):
                if hasattr(merged_feed, "calendar_dates"):
                    merged_feed.calendar_dates = pd.concat([merged_feed.calendar_dates, feed.calendar_dates],
                                                           ignore_index=True).drop_duplicates()
                else:
                    merged_feed.calendar_dates = feed.calendar_dates.copy().drop_duplicates()

            if hasattr(feed, "shapes"):
                if hasattr(merged_feed, "shapes"):
                    merged_feed.shapes = pd.concat([merged_feed.shapes, feed.shapes],
                                                   ignore_index=True).drop_duplicates()
                else:
                    merged_feed.shapes = feed.shapes.copy().drop_duplicates()

        # Add a load timestamp to each table
        merged_feed.routes['load_timestamp'] = logical_date
        merged_feed.trips['load_timestamp'] = logical_date
        merged_feed.stop_times['load_timestamp'] = logical_date
        merged_feed.stops['load_timestamp'] = logical_date
        
        if hasattr(merged_feed, "calendar"):
            merged_feed.calendar['load_timestamp'] = logical_date
        
        if hasattr(merged_feed, "calendar_dates"):
            merged_feed.calendar_dates['load_timestamp'] = logical_date

        if hasattr(merged_feed, "shapes"):
            merged_feed.shapes['load_timestamp'] = logical_date

        return merged_feed


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
            cs.execute(f"TRUNCATE TABLE IF EXISTS {stg_table.upper()}")
            
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
        save_static_data_to_snowflake(conn, merged_feed)


    def load_data_from_staging(**kwargs):
        """
        Loads data from STAGING to Target tables based on configuration.
        Applies SCD2 logic. The logical date column is automatically excluded from change tracking.
        """
        hook = SnowflakeHook(snowflake_conn_id='my_snowflake_conn')
        conn = hook.get_conn()
        cs = conn.cursor()
    
        table_config = {
            "ROUTES": {"pks": ["ROUTE_ID"], "use_scd2": True},
            "TRIPS": {"pks": ["TRIP_ID", "MODE"], "use_scd2": False},
            "STOPS": {"pks": ["STOP_ID"], "use_scd2": False},
            "CALENDAR": {"pks": ["SERVICE_ID"], "use_scd2": False},
            "CALENDAR_DATES": {"pks": ["SERVICE_ID", "DATE"], "use_scd2": False},
            "STOP_TIMES": {"pks": ["TRIP_ID", "STOP_SEQUENCE"], "use_scd2": False},
            "SHAPES": {"pks": ["SHAPE_ID", "SHAPE_PT_SEQUENCE"], "use_scd2": False}
        }
    
        cs.execute("USE SCHEMA SCHEDULE")
    
        for table, config in table_config.items():
            pks = [pk.upper() for pk in config["pks"]]
            use_scd2 = config.get("use_scd2", False)
            
            logical_date_col = config.get("logical_date_col", "LOAD_TIMESTAMP").upper()
            
            exclude_cols = [col.upper() for col in config.get("scd2_exclude_cols", [])]
            
            if logical_date_col not in exclude_cols:
                exclude_cols.append(logical_date_col)
                
            stg_table = f"{table}_STG"
            
            cs.execute(f"SHOW TABLES LIKE '{stg_table}' IN SCHEMA SCHEDULE")
            if not cs.fetchone():
                print(f"Staging table {stg_table} does not exist. Skipping for {table}.")
                continue
    
            create_target_ddl = f"CREATE TABLE IF NOT EXISTS {table} CLONE {stg_table};"
            cs.execute(create_target_ddl)
    
            cs.execute(f"SELECT * FROM {stg_table} LIMIT 1")
            columns = [col[0].upper() for col in cs.description]
            cols_str = ", ".join(columns)
    
            if use_scd2:
                print(f"Applying SCD2 logic for {table}...")
            
                try:
                    cs.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS VALID_FROM TIMESTAMP_NTZ;")
                    cs.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS VALID_TO TIMESTAMP_NTZ;")
                    cs.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS IS_ACTIVE BOOLEAN;")
                except Exception:
                    pass
    
                cols_to_hash = [c for c in columns if c not in pks and c not in exclude_cols]
                
                if not cols_to_hash:
                    print(f"Brak kolumn do śledzenia zmian dla {table}. Pomijam SCD2.")
                    continue
    
                tgt_hash_expr = "HASH(" + ", ".join([f"tgt.{c}" for c in cols_to_hash]) + ")"
                stg_hash_expr = "HASH(" + ", ".join([f"stg.{c}" for c in cols_to_hash]) + ")"
    
                join_conditions = " AND ".join([f"tgt.{pk} = stg.{pk}" for pk in pks])
    
                update_sql = f"""
                    UPDATE {table}_scd2 tgt
                    SET tgt.VALID_TO = stg.{logical_date_col},
                        tgt.IS_ACTIVE = FALSE
                    FROM {stg_table} stg
                    WHERE {join_conditions}
                      AND tgt.IS_ACTIVE = TRUE
                      AND {tgt_hash_expr} != {stg_hash_expr}
                """
                cs.execute(update_sql)
                
                insert_sql = f"""
                    INSERT INTO {table}_scd2 ({cols_str}, VALID_FROM, VALID_TO, IS_ACTIVE)
                    SELECT {cols_str}, stg.{logical_date_col}, NULL, TRUE
                    FROM {stg_table} stg
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {table}_scd2 tgt 
                        WHERE {join_conditions} AND tgt.IS_ACTIVE = TRUE
                    )
                """
                cs.execute(insert_sql)
                print(f"SCD2 applied for {table}.")
                
            else:
                insert_sql = f"""
                    INSERT INTO {table} ({cols_str})
                    SELECT {cols_str} FROM {stg_table}
                """
                cs.execute(insert_sql)
                print(f"Simple INSERT completed for {table}.")
    
            cs.execute(f"TRUNCATE TABLE {stg_table}")
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
