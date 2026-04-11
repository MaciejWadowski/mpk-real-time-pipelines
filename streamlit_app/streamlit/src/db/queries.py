import streamlit as st
from src.db.snowflake_conn import SnowflakeDB


@st.cache_resource
def get_db():
    return SnowflakeDB(
        user=st.secrets["user"],
        password=st.secrets["password"],
        account=st.secrets["account"],
        database=st.secrets["database"],
        schema=st.secrets["schema"],
        warehouse=st.secrets["warehouse"],
    )


@st.cache_data(ttl=3600)
def get_stops_coordinates():
    db = get_db()
    return db.query(
        """
        WITH stops_raw AS(
            SELECT
                *,
                ROW_NUMBER() OVER(PARTITION BY STOP_ID ORDER BY LOAD_TIMESTAMP DESC) AS rn
            FROM GTFS_TEST.SCHEDULE.STOPS
            )
        SELECT
            CASE
                WHEN STOP_DESC IS NULL THEN STOP_NAME
                ELSE CONCAT(STOP_NAME, ' ', STOP_DESC)
            END AS STOP_NAME,
            STOP_LAT AS LAT,
            STOP_LON AS LON
        FROM stops_raw
        WHERE rn = 1
        """
    )
