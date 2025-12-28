-- Dimension table for coins
{{ config(materialized='table') }}

WITH coin_data AS (
    SELECT DISTINCT
        coin_id,
        symbol,
        name
    FROM {{ ref('stg_prices') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['coin_id']) }} as coin_key,
    coin_id,
    symbol,
    name,
    CURRENT_TIMESTAMP() as effective_date,
    TIMESTAMP('2099-12-31') as expiry_date,
    TRUE as is_current
FROM coin_data