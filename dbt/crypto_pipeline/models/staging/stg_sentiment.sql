-- Clean and standardize sentiment data
SELECT
    value,
    value_classification,
    TIMESTAMP(timestamp) as timestamp,
    TIMESTAMP(fetch_timestamp) as fetch_timestamp,
    source
FROM {{ source('crypto_pipeline', 'stg_sentiment_raw') }}
WHERE value IS NOT NULL