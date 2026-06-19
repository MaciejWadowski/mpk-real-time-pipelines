import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


def get_connection() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "GTFS_TEST"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "DBT_WAREHOUSE"),
        role=os.environ.get("SNOWFLAKE_ROLE", "GTFS_UPLOADER_ROLE"),
    )
