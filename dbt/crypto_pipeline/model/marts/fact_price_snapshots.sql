-- Fact table for price snapshots
-- depends_on: {{ ref('dim_timestamp') }}
{{ config(materialized='incremental', unique_key='snapshot_id') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['coin_id', 'fetch_timestamp']) }} as snapshot_id,
    {{ dbt_utils.generate_surrogate_key(['coin_id']) }} as coin_key,
    FORMAT_TIMESTAMP('%Y%m%d%H%M%S', fetch_timestamp) as timestamp_key,
    current_price,
    market_cap,
    market_cap_rank,
    total_volume,
    high_24h,
    low_24h,
    price_change_24h,
    price_change_percentage_24h,
    market_cap_change_24h,
    circulating_supply,
    total_supply,
    max_supply,
    source as data_source
FROM {{ ref('stg_prices') }}

{% if is_incremental() %}
    WHERE fetch_timestamp > (SELECT MAX(timestamp) FROM {{ ref('dim_timestamp') }})
{% endif %}