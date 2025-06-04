{{ config(materialized='view', alias='VIEW_TRIPS_SCHEDULE') }}

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
    E.EVENT_DATE,
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
JOIN {{ ref('aggregated_calendar') }} E
  ON B.SERVICE_ID = E.service_id
  AND B.MODE = E.mode
  AND B.LOAD_TIMESTAMP = E.load_timestamp
  AND (
    (
      E.load_timestamp = (
          SELECT MAX(F.load_timestamp)
          FROM {{ ref('aggregated_calendar') }} F
          WHERE F.service_id = E.service_id
            AND F.mode = E.mode
      )
      AND E.event_date = TO_CHAR(DATEADD(day, 1, E.load_timestamp), 'YYYYMMDD')
    )
    OR E.event_date = TO_CHAR(E.load_timestamp, 'YYYYMMDD')
  )
WHERE C.STOP_NAME IS NOT NULL 
ORDER BY A.STOP_SEQUENCE ASC
