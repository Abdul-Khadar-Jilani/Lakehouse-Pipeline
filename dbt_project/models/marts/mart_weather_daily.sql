-- =============================================================================
-- Gold Layer: mart_weather_daily
-- =============================================================================
-- Daily weather aggregates per city — designed for BI dashboards.
-- Aggregates hourly Silver data into daily min/max/avg metrics.
-- Powers Power BI and Looker Studio reports.
-- =============================================================================

with hourly_data as (
    select * from {{ ref('stg_weather_hourly') }}
),

daily_aggregates as (
    select
        city,
        weather_date,
        latitude,
        longitude,

        -- Temperature aggregations
        min(temperature_celsius)                  as min_temp_celsius,
        max(temperature_celsius)                  as max_temp_celsius,
        round(avg(temperature_celsius), 2)        as avg_temp_celsius,

        -- Humidity aggregations
        min(relative_humidity_pct)                as min_humidity_pct,
        max(relative_humidity_pct)                as max_humidity_pct,
        round(avg(relative_humidity_pct), 2)      as avg_humidity_pct,

        -- Record counts (expect 24 per city per day)
        count(*)                                  as record_count,

        -- Data freshness
        max(ingested_at)                          as last_ingested_at

    from hourly_data
    group by city, weather_date, latitude, longitude
)

select * from daily_aggregates
