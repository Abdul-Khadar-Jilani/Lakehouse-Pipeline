-- =============================================================================
-- Silver Layer: stg_weather_hourly
-- =============================================================================
-- Cleans and types the raw Bronze data:
--   * Parses ISO timestamp string → TIMESTAMP
--   * Renames columns to descriptive snake_case
--   * Filters out rows with null readings
--   * Extracts date and hour for downstream aggregation
--   * DEDUPLICATES: if the DAG is re-triggered, only keeps the latest
--     ingested record per (city, time) to prevent duplicate rows in Gold
-- =============================================================================

with source as (
    select * from {{ source('weather_raw', 'raw_weather_hourly') }}
),

cleaned as (
    select
        city,
        latitude,
        longitude,

        -- Parse the ISO 8601 timestamp string to proper TIMESTAMP
        parse_timestamp('%Y-%m-%dT%H:%M', time) as recorded_at,

        -- Rename and cast weather metrics
        cast(temperature_2m as float64)       as temperature_celsius,
        cast(relative_humidity_2m as float64)  as relative_humidity_pct,

        ingested_at,
        source_file,

        -- Derived fields for easy aggregation
        extract(date from parse_timestamp('%Y-%m-%dT%H:%M', time)) as weather_date,
        extract(hour from parse_timestamp('%Y-%m-%dT%H:%M', time)) as weather_hour,

        -- Deduplication: rank by latest ingestion per (city, time)
        row_number() over (
            partition by city, time
            order by ingested_at desc
        ) as _row_num

    from source
    where
        -- Filter out null / incomplete readings
        temperature_2m       is not null
        and relative_humidity_2m is not null
        and time             is not null
),

-- Keep only the latest record per (city, time)
deduplicated as (
    select * from cleaned
    where _row_num = 1
)

select
    city,
    latitude,
    longitude,
    recorded_at,
    temperature_celsius,
    relative_humidity_pct,
    ingested_at,
    source_file,
    weather_date,
    weather_hour
from deduplicated
