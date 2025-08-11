{{ config(materialized='table', alias='TRIPS_SCHEDULE_TABLE')}}

WITH calendar AS (
  SELECT
    service_id,
    mode,
    event_date,
    load_timestamp
  FROM {{ ref('aggregated_calendar_table') }}
  WHERE load_timestamp IN (
    SELECT DISTINCT load_timestamp FROM {{ source('schedule', 'stop_times') }}
  )
  QUALIFY ROW_NUMBER() OVER (PARTITION BY MODE, EVENT_DATE ORDER BY LOAD_TIMESTAMP DESC) = 1
)
SELECT 
    A.TRIP_ID,
    A.MODE,
    D.ROUTE_SHORT_NAME, 
    IFF(
         SPLIT_PART(A.ARRIVAL_TIME, ':', '0') > 23,
         CONCAT(TRUNCATE(SPLIT_PART(A.ARRIVAL_TIME, ':', '0') - 24, 0), RIGHT(A.ARRIVAL_TIME, 6)),
         A.ARRIVAL_TIME
    ) AS ARRIVAL_TIME,
    A.STOP_SEQUENCE, 
    A.SHAPE_DIST_TRAVELED, 
    B.TRIP_HEADSIGN, 
    C.STOP_NAME, 
    C.STOP_ID, 
    C.STOP_LAT, 
    C.STOP_LON,
    IFF(
         SPLIT_PART(A.ARRIVAL_TIME, ':', '0') > 23,
         TO_CHAR(DATEADD(day, 1, TO_DATE(E.EVENT_DATE, 'YYYYMMDD'))::DATE, 'YYYYMMDD'),
         E.EVENT_DATE
    ) AS EVENT_DATE,
    IFF(
         SPLIT_PART(A.ARRIVAL_TIME, ':', '0') > 23,
         TRUE,
         FALSE
    ) AS IS_NIGHT_TRIP,
    A.LOAD_TIMESTAMP
FROM {{ source('schedule', 'stop_times') }} A
JOIN {{ source('schedule', 'trips') }} B
  ON A.TRIP_ID = B.TRIP_ID 
  AND A.LOAD_TIMESTAMP = B.LOAD_TIMESTAMP
  AND A.MODE = B.MODE
JOIN {{ source('schedule', 'stops') }} C
  ON A.STOP_ID = C.STOP_ID
  AND A.LOAD_TIMESTAMP = C.LOAD_TIMESTAMP
  AND A.MODE = C.MODE
JOIN {{ source('schedule', 'routes') }} D 
  ON B.ROUTE_ID = D.ROUTE_ID
  AND A.LOAD_TIMESTAMP = D.LOAD_TIMESTAMP
  AND B.MODE = D.MODE
JOIN calendar E
  ON B.SERVICE_ID = E.service_id
  AND B.MODE = E.mode
  AND B.LOAD_TIMESTAMP = E.load_timestamp
WHERE C.STOP_NAME IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY A.TRIP_ID, A.MODE, E.EVENT_DATE, A.STOP_ID ORDER BY A.LOAD_TIMESTAMP DESC) = 1
