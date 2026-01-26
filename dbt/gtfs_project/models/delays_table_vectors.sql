{{ config(materialized='table', alias='DELAYS_TABLE_VECTORS')}}

WITH RAW_UPDATES AS (
    SELECT 
        trip_id, 
        mode, 
        stop_id, 
        CONVERT_TIMEZONE('UTC', 'Europe/Warsaw', TO_TIMESTAMP_NTZ(IFNULL(arrival, departure)::NUMBER)) AS actual_time,
        TO_CHAR(actual_time::DATE, 'YYYYMMDD') AS event_date
    FROM {{ source('trip_updates', 'trip_updates') }} 
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY trip_id, mode, stop_id, event_date
        ORDER BY load_timestamp DESC
    ) = 1
),

COMBINED_STOPS AS (
    SELECT 
        s.trip_id, 
        s.mode, 
        s.event_date AS schedule_event_date, 
        s.stop_id, 
        s.stop_sequence,
        n.actual_time,
        TO_TIMESTAMP(TO_DATE(s.event_date, 'YYYYMMDD') || ' ' || s.arrival_time) AS planned_time
    FROM {{ ref('trips_schedule_table') }} s
    LEFT JOIN RAW_UPDATES n 
        ON n.trip_id = s.trip_id 
        AND n.mode = s.mode 
        AND n.stop_id = s.stop_id
        AND (
            (
                n.event_date = s.event_date
                AND NOT (
                    (HOUR(n.actual_time) = 23 AND SPLIT_PART(s.arrival_time, ':', 1) = '00')
                    OR (HOUR(n.actual_time) = 0 AND SPLIT_PART(s.arrival_time, ':', 1) = '23')
                )
            )
            OR (
                HOUR(n.actual_time) = 23 
                AND SPLIT_PART(s.arrival_time, ':', 1) = '00'
                AND n.event_date = TO_CHAR(DATEADD(day, -1, TO_DATE(s.event_date, 'YYYYMMDD')), 'YYYYMMDD')
            )
            OR (
                HOUR(n.actual_time) = 0 
                AND SPLIT_PART(s.arrival_time, ':', 1) = '23'
                AND s.event_date = TO_CHAR(DATEADD(day, -1, TO_DATE(n.event_date, 'YYYYMMDD')), 'YYYYMMDD')
            )
        )
    WHERE s.mode <> 'TR'
)

SELECT 
    trip_id,
    mode,
    schedule_event_date AS event_date,
    stop_id AS start_stop_id,
    stop_sequence AS start_stop_sequence,
    planned_time AS planned_departure,
    actual_time AS actual_departure,
    LEAD(stop_id) OVER (PARTITION BY trip_id, mode, schedule_event_date ORDER BY stop_sequence) AS direction_stop_id,
    LEAD(stop_sequence) OVER (PARTITION BY trip_id, mode, schedule_event_date ORDER BY stop_sequence) AS end_stop_sequence,
    LEAD(planned_time) OVER (PARTITION BY trip_id, mode, schedule_event_date ORDER BY stop_sequence) AS planned_arrival,
    LEAD(actual_time) OVER (PARTITION BY trip_id, mode, schedule_event_date ORDER BY stop_sequence) AS actual_arrival,
    MD5(trip_id || schedule_event_date || stop_sequence || COALESCE(LEAD(stop_sequence) OVER (PARTITION BY trip_id, mode, schedule_event_date ORDER BY stop_sequence)::STRING, 'END')) AS vector_unique_id
FROM COMBINED_STOPS
WHERE actual_time IS NOT NULL
QUALIFY direction_stop_id IS NOT NULL;
