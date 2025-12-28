-- Clean and standardize price data from raw staging
SELECT
    coin_id,
    symbol,
    name,
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
    TIMESTAMP(last_updated) as last_updated,
    TIMESTAMP(fetch_timestamp) as fetch_timestamp,
    source
FROM {{ source('crypto_pipeline', 'stg_prices_raw') }}
WHERE current_price IS NOT NULL