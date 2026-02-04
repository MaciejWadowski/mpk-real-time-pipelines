{{ config(materialized='table', alias='VEHICLE_POSITIONS')}}
    
SELECT 
    vp.ID AS vehicle_id,
    vp.LATITUDE,
    vp.LONGITUDE,
    vp.BEARING,
    vp.SPEED,
    vp.CURRENT_STATUS,
    vp.CURRENT_STOP_SEQUENCE,
    vp.LOAD_TIMESTAMP,
    vp.TRIP_ID,
    dt.vector_unique_id
FROM {{ source('trip_updates', 'vehicle_positions') }} vp
INNER JOIN {{ ref('delays_table_vectors') }} dt
    ON vp.TRIP_ID = dt.trip_id 
    AND TO_CHAR(vp.LOAD_TIMESTAMP, 'YYYYMMDD') = dt.event_date
    AND vp.CURRENT_STOP_SEQUENCE = dt.end_stop_sequence
    AND vp.MODE = dt.MODE
WHERE 
    vp.CURRENT_STATUS IN (1, 2)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY vp.TRIP_ID, vp.LOAD_TIMESTAMP 
    ORDER BY vp.ID ASC
) = 1;
