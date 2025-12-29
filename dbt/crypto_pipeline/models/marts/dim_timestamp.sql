-- Dimension table for timestamps
{{ config(materialized='table') }}

WITH price_timestamps AS (
    SELECT DISTINCT
        fetch_timestamp as timestamp
    FROM {{ ref('stg_prices') }}
),

sentiment_timestamps AS (
    SELECT DISTINCT
        fetch_timestamp as timestamp
    FROM {{ ref('stg_sentiment') }}
),

all_timestamps AS (
    SELECT timestamp FROM price_timestamps
    UNION DISTINCT
    SELECT timestamp FROM sentiment_timestamps
)

SELECT
    FORMAT_TIMESTAMP('%Y%m%d%H%M%S', timestamp) as timestamp_key,
    timestamp,
    DATE(timestamp) as date,
    EXTRACT(HOUR FROM timestamp) as hour,
    EXTRACT(DAYOFWEEK FROM timestamp) as day_of_week,
    FORMAT_TIMESTAMP('%A', timestamp) as day_name,
    EXTRACT(WEEK FROM timestamp) as week_of_year,
    EXTRACT(MONTH FROM timestamp) as month,
    FORMAT_TIMESTAMP('%B', timestamp) as month_name,
    EXTRACT(QUARTER FROM timestamp) as quarter,
    EXTRACT(YEAR FROM timestamp) as year,
    EXTRACT(DAYOFWEEK FROM timestamp) IN (1, 7) as is_weekend
FROM all_timestamps