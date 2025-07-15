{{ config(materialized='table', alias='DELAYS_TABLE')}}

WITH NEXT_STOP AS (
    SELECT 
        trip_id, 
        stop_id, 
        CONVERT_TIMEZONE('UTC','Europe/Warsaw', to_timestamp_ntz(IFNULL(arrival, departure)::NUMBER)) AS actual_arrival, 
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
    TO_TIMESTAMP(TO_DATE(N.ACTUAL_ARRIVAL) || ' ' || S.ARRIVAL_TIME || '.000') as PLANNED_ARRIVAL,
    S.MODE,
    S.EVENT_DATE,
    N.LOAD_TIMESTAMP,
    S.LOAD_TIMESTAMP AS SCHEDULE_TIMESTAMP
FROM NEXT_STOP as N 
JOIN {{ ref('trips_schedule_table') }} AS S
    ON N.trip_id = S.trip_id 
    and N.MODE=S.MODE 
    and N.stop_id=S.stop_id 
    and N.event_date = S.event_date
    --and S.LOAD_TIMESTAMP <= N.LOAD_TIMESTAMP
QUALIFY ROW_NUMBER() 
        OVER (PARTITION BY S.trip_id, S.stop_id, S.MODE, S.event_date ORDER BY S.LOAD_TIMESTAMP DESC) = 1
), delays as (
SELECT *, case when datediff('minute',planned_arrival, actual_arrival) < 0 then 0 else datediff('minute',planned_arrival, actual_arrival) end  as delay FROM JOINED_WITH_SCHEDULE
)
select * from delays where mode != 'TR'