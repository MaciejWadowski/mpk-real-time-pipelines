WITH
    change_indicators AS (
        SELECT
            trip_id,
            stop_sequence,
            stop_id,
            mode,
            delay,
            load_timestamp,
            CASE
                WHEN stop_sequence < LAG(stop_sequence) OVER (
                    PARTITION BY
                        trip_id,
                        mode
                    ORDER BY
                        load_timestamp
                ) THEN 1
                ELSE 0
            END AS change_indicator
        FROM
            GTFS_TEST.SCHEDULE_DATA_MARTS.DELAYS_TABLE
    ),
    sessionize AS (
        SELECT
            trip_id,
            stop_sequence,
            stop_id,
            mode,
            delay,
            load_timestamp,
            SUM(change_indicator) OVER (
                PARTITION BY
                    trip_id,
                    mode
                ORDER BY
                    load_timestamp
            ) AS session_id
        FROM
            change_indicators
    ),

    latest_trip_update AS (
        -- keep the row with the largest delay per (trip_id, stop_sequence, mode) - case when trip prolongs longer than 1 minute
        SELECT
            trip_id,
            session_id,
            stop_sequence,
            stop_id,
            mode,
            delay,
            load_timestamp
        FROM
            sessionize QUALIFY ROW_NUMBER() OVER (
                PARTITION BY
                    trip_id,
                    session_id,
                    stop_sequence,
                    mode
                ORDER BY
                    load_timestamp DESC,
                    delay DESC
            ) = 1
    ),
    paired AS (
        -- pair each stop with the next stop on the same trip and same mode
        SELECT
            trip_id,
            mode,
            stop_sequence,
            stop_id,
            delay,
            load_timestamp,
            LEAD(stop_id) OVER (
                PARTITION BY
                    trip_id,
                    session_id,
                    mode
                ORDER BY
                    stop_sequence
            ) AS next_stop_id,
            LEAD(stop_sequence) OVER (
                PARTITION BY
                    trip_id,
                    session_id,
                    mode
                ORDER BY
                    stop_sequence
            ) AS next_stop_sequence
        FROM
            latest_trip_update
    ),
    valid_pairs AS (
        -- keep only real consecutive pairs (both sides exist) and origin before dest
        SELECT
            stop_id,
            next_stop_id,
            delay,
            DATE_PART('hour', load_timestamp) AS delay_hour,
            TO_DATE(load_timestamp) AS delay_date
        FROM
            paired
        WHERE
            next_stop_id IS NOT NULL -- remove if there is no last stop
            AND stop_sequence + 1 = next_stop_sequence -- remove if we have a gap between stations
    ),
    avg_delays AS (
        SELECT
            COUNT(1) AS cnt,
            AVG(delay) AS avg_delay,
            stop_id,
            next_stop_id,
            delay_hour,
            delay_date
        FROM
            valid_pairs
        GROUP BY
            stop_id,
            next_stop_id,
            delay_hour,
            delay_date
    ),
-- enrich with geolocation, if not needed it could be removed
    avg_delays_geolocation AS (
        SELECT
            a.cnt,
            a.avg_delay,
            a.stop_id,
            b.stop_lat,
            b.stop_lon,
            a.next_stop_id,
            c.stop_lat AS next_stop_lat,
            c.stop_lon AS next_stop_lon,
            a.delay_hour,
            a.delay_date
        FROM
            avg_delays a
            LEFT JOIN GTFS_TEST.SCHEDULE.STOPS b ON a.stop_id = b.stop_id
            AND a.delay_date = TO_DATE(b.load_timestamp)
            LEFT JOIN GTFS_TEST.SCHEDULE.STOPS c ON a.stop_id = c.stop_id
            AND a.delay_date = TO_DATE(c.load_timestamp)
    )
-- example query usage
SELECT
    *
FROM
    avg_delays_geolocation
WHERE
    delay_date between '2026-02-24' and '2026-02-28'
ORDER BY
    stop_id DESC,
    delay_hour;