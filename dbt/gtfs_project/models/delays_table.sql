{{ config(materialized='table', alias='DELAYS_TABLE')}}

WITH NEXT_STOP AS (
    SELECT 
        trip_id, 
        stop_id, 
        CONVERT_TIMEZONE('UTC','Europe/Warsaw', to_timestamp_ntz(IFNULL(arrival, departure)::NUMBER)) AS ACTUAL_ARRIVAL, 
        MODE, 
        LOAD_TIMESTAMP,
        TO_CHAR(CONVERT_TIMEZONE('UTC','Europe/Warsaw', to_timestamp_ntz(IFNULL(arrival, departure)::NUMBER))::DATE, 'YYYYMMDD') AS EVENT_DATE
    FROM {{ source('trip_updates', 'trip_updates') }} 
        QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIP_ID, MODE, STOP_ID, EVENT_DATE ORDER BY LOAD_TIMESTAMP DESC) = 1
), JOINED_WITH_SCHEDULE AS (
SELECT 
    S.TRIP_ID, 
    S.STOP_ID,
    S.STOP_NAME,
    S.TRIP_HEADSIGN,
    S.STOP_SEQUENCE,
    N.ACTUAL_ARRIVAL,
    S.ROUTE_SHORT_NAME,
    TO_TIMESTAMP(TO_DATE(S.EVENT_DATE, 'YYYYMMDD') || ' ' || S.ARRIVAL_TIME || '.000') as PLANNED_ARRIVAL,
    S.MODE,
    S.EVENT_DATE,
    N.LOAD_TIMESTAMP,
    S.LOAD_TIMESTAMP AS SCHEDULE_TIMESTAMP
FROM NEXT_STOP as N 
JOIN {{ ref('trips_schedule_table') }} AS S
JOIN {{ ref('trips_schedule_table') }} AS S
    ON N.TRIP_ID = S.TRIP_ID 
    AND N.MODE=S.MODE 
    AND N.STOP_ID=S.STOP_ID 
    AND (
            (
                N.event_date = S.event_date
                AND NOT (
                            (
                                HOUR(N.ACTUAL_ARRIVAL) = 23
                                AND SPLIT_PART(S.ARRIVAL_TIME, ':', '0') = 0
                            )
                        OR  (
                            HOUR(N.ACTUAL_ARRIVAL) = 0
                            AND SPLIT_PART(S.ARRIVAL_TIME, ':', '0') = 23
                            )
                        )
            )
            OR  (
                HOUR(N.ACTUAL_ARRIVAL) = 23
                AND SPLIT_PART(S.ARRIVAL_TIME, ':', '0') = 0
                AND N.EVENT_DATE = TO_CHAR(DATEADD(day, -1, TO_DATE(S.EVENT_DATE, 'YYYYMMDD'))::DATE, 'YYYYMMDD')
            )
            OR (
                HOUR(N.ACTUAL_ARRIVAL) = 0
                AND SPLIT_PART(S.ARRIVAL_TIME, ':', '0') = 23
                AND S.EVENT_DATE = TO_CHAR(DATEADD(day, -1, TO_DATE(N.EVENT_DATE, 'YYYYMMDD'))::DATE, 'YYYYMMDD')
            )

        )
QUALIFY ROW_NUMBER() 
        OVER (PARTITION BY S.trip_id, S.stop_id, S.MODE, S.event_date ORDER BY S.LOAD_TIMESTAMP DESC) = 1
), delays as (
SELECT *, case when datediff('minute',planned_arrival, actual_arrival) < 0 then 0 else datediff('minute',planned_arrival, actual_arrival) end  as delay FROM JOINED_WITH_SCHEDULE
)
select * from delays where mode != 'TR'