{{ config(materialized='table', alias='AGGREGATED_CALENDAR_TABLE') }}


with date_series AS (
  SELECT DATEADD(day, seq4(), DATEADD('month', -1, '{{ var("execution_date") }}'::DATE)) AS generated_date
  FROM TABLE(GENERATOR(ROWCOUNT => 10000))
),
 all_calendars as (
SELECT
  TO_CHAR(ds.generated_date, 'YYYYMMDD') AS event_date,
  c.service_id,
  c.mode,
  c.load_timestamp
FROM  {{ source('schedule', 'calendar') }} c
 JOIN date_series ds ON ds.generated_date BETWEEN TO_DATE(c.start_date, 'YYYYMMDD')
                          AND TO_DATE(c.end_date, 'YYYYMMDD')
LEFT JOIN {{ source('schedule', 'calendar_dates') }} cd
  ON c.service_id = cd.service_id
  AND c.mode = cd.mode
  AND c.load_timestamp = cd.load_timestamp
WHERE
  (
      (
        CASE DAYOFWEEK(TO_DATE(cd."DATE", 'YYYYMMDD'))
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
  AND to_date(c.load_timestamp) = '{{ var("execution_date") }}'
ORDER BY c.service_id
), deduplicated as (
    SELECT *, ROW_NUMBER() OVER (partition by event_date, service_id, mode order by load_timestamp desc) as rnk FROM all_calendars
)
SELECT event_date, service_id, mode, load_timestamp from deduplicated where rnk = 1