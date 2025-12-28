-- Fact table for sentiment
-- depends_on: {{ ref('dim_timestamp') }}
{{ config(materialized='incremental', unique_key='sentiment_id') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['timestamp']) }} as sentiment_id,
    FORMAT_TIMESTAMP('%Y%m%d%H%M%S', fetch_timestamp) as timestamp_key,
    value as sentiment_value,
    value_classification as sentiment_classification,
    source as data_source
FROM {{ ref('stg_sentiment') }}

{% if is_incremental() %}
    WHERE fetch_timestamp > (SELECT MAX(timestamp) FROM {{ ref('dim_timestamp') }})
{% endif %}