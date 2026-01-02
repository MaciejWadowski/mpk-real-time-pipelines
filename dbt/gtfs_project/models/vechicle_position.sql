
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
FROM {{ ref('delays_table') }} vp
INNER JOIN GTFS_TEST.TRIP_UPDATES.DELAY_VECTORS_TABLE dt
    ON vp.TRIP_ID = dt.trip_id 
    AND TO_CHAR(vp.LOAD_TIMESTAMP, 'YYYYMMDD') = dt.event_date
    AND vp.CURRENT_STOP_SEQUENCE = dt.end_stop_sequence
WHERE 
    vp.CURRENT_STATUS IN (1, 2)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY vp.TRIP_ID, vp.LOAD_TIMESTAMP 
    ORDER BY vp.ID ASC
) = 1;
