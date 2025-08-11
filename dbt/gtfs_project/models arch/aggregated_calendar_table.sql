{{ config(materialized='table', alias='AGGREGATED_CALENDAR_TABLE') }}


WITH first_date AS (
  SELECT MIN(TO_DATE(load_timestamp)) AS first_ld
  FROM {{ source('schedule', 'calendar') }}
),
max_date AS (
  SELECT MIN(TO_DATE(END_DATE, 'YYYYMMDD')) AS max_dt
  FROM {{ source('schedule', 'calendar') }}
),
date_series AS (
  SELECT DATEADD(day, seq4(), (SELECT first_ld FROM first_date)) AS generated_date
  FROM TABLE(GENERATOR(ROWCOUNT => 10000))
)
SELECT 
  TO_CHAR(ds.generated_date, 'YYYYMMDD') AS event_date,
  c.service_id,
  c.mode,
  c.load_timestamp
FROM max_date md
JOIN date_series ds
  ON ds.generated_date <= md.max_dt
JOIN {{ source('schedule', 'calendar') }} c
  ON ds.generated_date BETWEEN TO_DATE(c.start_date, 'YYYYMMDD')
                          AND TO_DATE(c.end_date, 'YYYYMMDD')
LEFT JOIN {{ source('schedule', 'calendar_dates') }} cd
  ON c.service_id = cd.service_id
  AND c.mode = cd.mode
  AND c.load_timestamp = cd.load_timestamp
  AND ds.generated_date = TO_DATE(cd."DATE", 'YYYYMMDD')
  AND cd.date <= md.max_dt
WHERE
  (
      (
        CASE DAYOFWEEK(ds.generated_date)
          WHEN 1 THEN c.monday
          WHEN 2 THEN c.tuesday
          WHEN 3 THEN c.wednesday
          WHEN 4 THEN c.thursday
          WHEN 5 THEN c.friday
          WHEN 6 THEN c.saturday
          WHEN 0 THEN c.sunday
        END = 1
        AND (cd.exception_type != 2 OR cd.exception_type IS NULL)
      )
      OR cd.exception_type = 1
  )
ORDER BY ds.generated_date, c.service_id