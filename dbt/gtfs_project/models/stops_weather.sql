{{ config(materialized='table', alias='STOPS_WEATHER')}}

with
stops as (
  select
    stop_id,
    stop_name,
    stop_lat,
    stop_lon
  from {{ source('schedule', 'stops') }}
  where stop_lat is not null and stop_lon is not null
),

district_locations as (
  select
    district_number,
    district_name,
    latitude  as district_lat,
    longitude as district_lon,
    location
  from {{ source('weather_api_staging', 'district_locations') }}
),

hourly_weather as (
  select
    district_name,
    timestamp    as weather_timestamp,
    temperature_2m,
    wind_speed_10m,
    relative_humidity_2m,
    precipitation,
    weather_code,
    pressure_msl,
    cloud_cover,
    visibility,
    is_day
  from {{ source('weather_api_staging', 'hourly_weather') }}
),

-- find nearest district for each stop
stops_nearest as (
  select
    s.stop_id,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    d.district_name,
    st_distance(
      st_geogfromtext('POINT(' || s.stop_lon || ' ' || s.stop_lat || ')'),
      d.location
    ) as distance_meters,
    row_number() over (partition by s.stop_id order by st_distance(
      st_geogfromtext('POINT(' || s.stop_lon || ' ' || s.stop_lat || ')'),
      d.location
    )) as rn
  from stops s
  join district_locations d
    on true
)

, stops_assigned as (
  select *
  from stops_nearest
  where rn = 1
)

select
  sa.stop_id,
  sa.stop_name,
  sa.district_name,
  hw.weather_timestamp,
  hw.temperature_2m,
  hw.wind_speed_10m,
  hw.relative_humidity_2m,
  hw.precipitation,
  hw.weather_code,
  hw.pressure_msl,
  hw.cloud_cover,
  hw.visibility,
  hw.is_day
from stops_assigned sa
join hourly_weather hw
  on hw.district_name = sa.district_name
order by sa.stop_id, hw.weather_timestamp
