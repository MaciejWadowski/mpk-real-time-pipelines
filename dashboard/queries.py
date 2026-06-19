from __future__ import annotations

import pandas as pd
import streamlit as st

from config import get_connection

DB = "GTFS_TEST"
DELAYS = f"{DB}.SCHEDULE_DATA_MARTS.DELAYS_TABLE"
STOPS = f"{DB}.SCHEDULE.STOPS"
SHAPES = f"{DB}.SCHEDULE.SHAPES"
TRIPS = f"{DB}.SCHEDULE.TRIPS"
ROUTES_SCD2 = f"{DB}.SCHEDULE.ROUTES_SCD2"
HOURLY_WEATHER = f"{DB}.WEATHER_API_STAGING.HOURLY_WEATHER"


# ── helpers ──────────────────────────────────────────────────────────────────

def _run(sql: str, params: tuple = ()) -> pd.DataFrame:
    conn = get_connection()
    try:
        cs = conn.cursor()
        cs.execute(sql, params) if params else cs.execute(sql)
        rows = cs.fetchall()
        cols = [d[0].lower() for d in cs.description]
        return pd.DataFrame(rows, columns=cols)
    finally:
        cs.close()
        conn.close()


def _where(
    modes: tuple,
    line: str | None,
    start: str,
    end: str,
    hour_min: int,
    hour_max: int,
) -> tuple[str, tuple]:
    conds: list[str] = []
    params: list = []

    if modes:
        ph = ", ".join(["%s"] * len(modes))
        conds.append(f"MODE IN ({ph})")
        params.extend(modes)

    if line:
        conds.append("ROUTE_SHORT_NAME = %s")
        params.append(line)

    if start:
        conds.append("EVENT_DATE >= %s")
        params.append(start)
    if end:
        conds.append("EVENT_DATE <= %s")
        params.append(end)

    if hour_min > 0 or hour_max < 23:
        conds.append("HOUR(ACTUAL_ARRIVAL) BETWEEN %s AND %s")
        params.extend([hour_min, hour_max])

    where = ("WHERE " + " AND ".join(conds)) if conds else ""
    return where, tuple(params)


# ── metadata ─────────────────────────────────────────────────────────────────

@st.cache_data(ttl=86400)
def get_last_update() -> pd.Timestamp:
    df = _run(f"SELECT MAX(LOAD_TIMESTAMP) AS TS FROM {DELAYS}")
    return pd.Timestamp(df["ts"].iloc[0])


@st.cache_data(ttl=86400)
def get_date_range() -> tuple[object, object]:
    from datetime import datetime, date as date_type
    df = _run(f"SELECT MIN(EVENT_DATE) AS MIN_DT, MAX(EVENT_DATE) AS MAX_DT FROM {DELAYS}")

    def _parse(val) -> date_type:
        if isinstance(val, date_type):
            return val
        s = str(val).replace("-", "").replace("/", "")[:8]
        return datetime.strptime(s, "%Y%m%d").date()

    return _parse(df["min_dt"].iloc[0]), _parse(df["max_dt"].iloc[0])


@st.cache_data(ttl=86400)
def get_all_routes() -> list[str]:
    df = _run(f"SELECT DISTINCT ROUTE_SHORT_NAME FROM {DELAYS} ORDER BY ROUTE_SHORT_NAME")
    return df["route_short_name"].tolist()


# ── KPI ──────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=86400)
def get_kpi(
    modes: tuple, line: str | None, start: str, end: str, hour_min: int, hour_max: int
) -> dict:
    where, params = _where(modes, line, start, end, hour_min, hour_max)
    conn = get_connection()
    try:
        cs = conn.cursor()
        cs.execute(
            f"""
            SELECT
                ROUND(SUM(CASE WHEN DELAY = 0 THEN 1 ELSE 0 END) * 100.0
                      / NULLIF(COUNT(*), 0), 1) AS pct_on_time,
                COUNT(*) AS n_obs
            FROM {DELAYS} {where}
            """,
            params,
        )
        overall = cs.fetchone()
        cs.execute(
            f"""
            SELECT ROUTE_SHORT_NAME, ROUND(AVG(DELAY), 1) AS avg_delay
            FROM {DELAYS} {where}
            GROUP BY ROUTE_SHORT_NAME HAVING COUNT(*) >= 100
            ORDER BY avg_delay DESC LIMIT 1
            """,
            params,
        )
        worst = cs.fetchone()
    finally:
        cs.close()
        conn.close()

    return {
        "pct_on_time": float(overall[0] or 0),
        "n_obs": int(overall[1] or 0),
        "worst_line": worst[0] if worst else "N/A",
        "worst_delay": float(worst[1]) if worst else 0.0,
    }


# ── map: stops ────────────────────────────────────────────────────────────────

@st.cache_data(ttl=86400)
def get_map_stops(
    modes: tuple, line: str | None, start: str, end: str, hour_min: int, hour_max: int
) -> pd.DataFrame:
    where, params = _where(modes, line, start, end, hour_min, hour_max)
    # Wrap DELAYS in a subquery to avoid ambiguous column names when joining with STOPS
    return _run(
        f"""
        SELECT
            d.STOP_ID,
            d.STOP_NAME,
            ROUND(AVG(d.DELAY), 2) AS avg_delay,
            COUNT(*)               AS n_obs,
            ROUND(SUM(CASE WHEN d.DELAY = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)
                                   AS pct_on_time,
            MAX(s.STOP_LAT)        AS lat,
            MAX(s.STOP_LON)        AS lon
        FROM (
            SELECT STOP_ID, STOP_NAME, DELAY
            FROM {DELAYS}
            {where}
        ) d
        JOIN {STOPS} s ON d.STOP_ID = s.STOP_ID
        GROUP BY d.STOP_ID, d.STOP_NAME
        HAVING COUNT(*) >= 30 AND MAX(s.STOP_LAT) IS NOT NULL
        """,
        params,
    )


# ── map: route shapes ─────────────────────────────────────────────────────────

@st.cache_data(ttl=86400)
def get_route_shapes(modes: tuple) -> pd.DataFrame:
    """One representative shape per route, returned as raw points for pydeck PathLayer."""
    mode_cond = ""
    params: tuple = ()
    if modes and len(modes) < 3:
        ph = ", ".join(["%s"] * len(modes))
        mode_cond = f"AND t.MODE IN ({ph})"
        params = modes

    return _run(
        f"""
        WITH latest_shapes AS (
            SELECT MAX(LOAD_TIMESTAMP) AS ts FROM {SHAPES}
        ),
        latest_trips AS (
            SELECT MAX(LOAD_TIMESTAMP) AS ts FROM {TRIPS}
        ),
        route_shapes AS (
            SELECT
                r.ROUTE_SHORT_NAME,
                t.SHAPE_ID,
                t.MODE,
                COUNT(*) AS trip_count
            FROM {TRIPS} t
            JOIN {ROUTES_SCD2} r
                ON t.ROUTE_ID = r.ROUTE_ID AND t.MODE = r.MODE AND r.IS_CURRENT = TRUE
            WHERE t.LOAD_TIMESTAMP = (SELECT ts FROM latest_trips)
            {mode_cond}
            GROUP BY 1, 2, 3
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ROUTE_SHORT_NAME ORDER BY trip_count DESC) = 1
        )
        SELECT
            rs.ROUTE_SHORT_NAME,
            rs.MODE,
            s.SHAPE_ID,
            s.SHAPE_PT_SEQUENCE,
            s.SHAPE_PT_LAT,
            s.SHAPE_PT_LON
        FROM {SHAPES} s
        JOIN route_shapes rs ON s.SHAPE_ID = rs.SHAPE_ID AND s.MODE = rs.MODE
        WHERE s.LOAD_TIMESTAMP = (SELECT ts FROM latest_shapes)
        ORDER BY rs.ROUTE_SHORT_NAME, s.SHAPE_ID, s.SHAPE_PT_SEQUENCE
        """,
        params,
    )


# ── time heatmap ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=86400)
def get_time_heatmap(
    modes: tuple, line: str | None, start: str, end: str
) -> pd.DataFrame:
    # Intentionally ignores hour filter — the heatmap IS the hour view
    where, params = _where(modes, line, start, end, 0, 23)
    return _run(
        f"""
        SELECT
            HOUR(ACTUAL_ARRIVAL)      AS hour,
            DAYOFWEEK(ACTUAL_ARRIVAL) AS dow,
            ROUND(AVG(DELAY), 2)      AS avg_delay,
            COUNT(*)                  AS n_obs
        FROM {DELAYS} {where}
        GROUP BY 1, 2
        """,
        params,
    )


# ── trend ─────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=86400)
def get_trend(modes: tuple, lines: tuple, start: str, end: str) -> pd.DataFrame:
    if not lines:
        return pd.DataFrame()
    line_ph = ", ".join(["%s"] * len(lines))
    mode_cond = ""
    params: list = list(lines)
    if modes:
        ph = ", ".join(["%s"] * len(modes))
        mode_cond = f"AND MODE IN ({ph})"
        params.extend(modes)
    params.extend([start, end])
    return _run(
        f"""
        SELECT
            TO_DATE(EVENT_DATE, 'YYYYMMDD') AS dt,
            ROUTE_SHORT_NAME,
            ROUND(AVG(DELAY), 2) AS avg_delay,
            COUNT(*)             AS n_obs
        FROM {DELAYS}
        WHERE ROUTE_SHORT_NAME IN ({line_ph})
          {mode_cond}
          AND EVENT_DATE BETWEEN %s AND %s
        GROUP BY 1, 2
        ORDER BY 1
        """,
        tuple(params),
    )


# ── ranking ───────────────────────────────────────────────────────────────────

@st.cache_data(ttl=86400)
def get_ranking_lines(
    modes: tuple, line: str | None, start: str, end: str, hour_min: int, hour_max: int
) -> pd.DataFrame:
    where, params = _where(modes, line, start, end, hour_min, hour_max)
    return _run(
        f"""
        SELECT
            ROUTE_SHORT_NAME,
            MODE,
            ROUND(AVG(DELAY), 2)                     AS avg_delay,
            ROUND(APPROX_PERCENTILE(DELAY, 0.5), 2) AS median_delay,
            ROUND(SUM(CASE WHEN DELAY = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)
                                                     AS pct_on_time,
            COUNT(*)                                 AS n_obs
        FROM {DELAYS} {where}
        GROUP BY ROUTE_SHORT_NAME, MODE
        HAVING COUNT(*) >= 100
        ORDER BY avg_delay DESC
        """,
        params,
    )


@st.cache_data(ttl=86400)
def get_ranking_stops(
    modes: tuple, line: str | None, start: str, end: str, hour_min: int, hour_max: int
) -> pd.DataFrame:
    where, params = _where(modes, line, start, end, hour_min, hour_max)
    return _run(
        f"""
        SELECT
            STOP_NAME,
            ROUND(AVG(DELAY), 2)                     AS avg_delay,
            ROUND(APPROX_PERCENTILE(DELAY, 0.5), 2) AS median_delay,
            ROUND(SUM(CASE WHEN DELAY = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)
                                                     AS pct_on_time,
            COUNT(*)                                 AS n_obs
        FROM {DELAYS} {where}
        GROUP BY STOP_NAME
        HAVING COUNT(*) >= 100
        ORDER BY avg_delay DESC
        """,
        params,
    )


# ── weather correlation ───────────────────────────────────────────────────────

@st.cache_data(ttl=86400)
def get_weather_correlation(
    modes: tuple, line: str | None, start: str, end: str, hour_min: int, hour_max: int
) -> pd.DataFrame:
    where, params = _where(modes, line, start, end, hour_min, hour_max)
    return _run(
        f"""
        WITH city_weather AS (
            SELECT
                TIMESTAMP,
                ROUND(AVG(TEMPERATURE_2M), 1)   AS temp,
                ROUND(AVG(PRECIPITATION), 2)     AS precip,
                ROUND(AVG(WIND_SPEED_10M), 1)    AS wind,
                ROUND(AVG(CLOUD_COVER), 0)       AS cloud_cover,
                ROUND(AVG(WEATHER_CODE))         AS weather_code
            FROM {HOURLY_WEATHER}
            GROUP BY TIMESTAMP
        ),
        delays_hourly AS (
            SELECT
                DATE_TRUNC('hour', ACTUAL_ARRIVAL) AS hour_ts,
                ROUND(AVG(DELAY), 2)               AS avg_delay,
                COUNT(*)                           AS n_obs
            FROM {DELAYS} {where}
            GROUP BY 1
        )
        SELECT
            w.temp, w.precip, w.wind, w.cloud_cover, w.weather_code,
            d.avg_delay, d.n_obs
        FROM city_weather w
        JOIN delays_hourly d ON w.TIMESTAMP = d.hour_ts
        WHERE d.n_obs >= 10
        ORDER BY w.TIMESTAMP
        """,
        params,
    )
