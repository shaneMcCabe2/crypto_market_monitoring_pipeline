"""
Initialize dimensional model tables in BigQuery
Creates fact and dimension tables for the data warehouse
"""

from google.cloud import bigquery
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_dimensional_tables(project_id=None, dataset_id='crypto_pipeline'):
    """
    Create fact and dimension tables for the data warehouse
    """
    if project_id is None:
        project_id = os.getenv('GCP_PROJECT_ID')
    
    client = bigquery.Client(project=project_id)
    dataset_ref = f"{project_id}.{dataset_id}"
    
    logger.info(f"Creating dimensional model in {dataset_ref}")
    
    # Dimension: Coin
    dim_coin_schema = [
        bigquery.SchemaField("coin_key", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("coin_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("symbol", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("effective_date", "TIMESTAMP"),
        bigquery.SchemaField("expiry_date", "TIMESTAMP"),
        bigquery.SchemaField("is_current", "BOOLEAN"),
    ]
    
    dim_coin_table_ref = f"{dataset_ref}.dim_coin"
    dim_coin_table = bigquery.Table(dim_coin_table_ref, schema=dim_coin_schema)
    dim_coin_table = client.create_table(dim_coin_table, exists_ok=True)
    logger.info(f"Created table {dim_coin_table_ref}")
    
    # Dimension: Timestamp
    dim_timestamp_schema = [
        bigquery.SchemaField("timestamp_key", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("hour", "INTEGER"),
        bigquery.SchemaField("day_of_week", "INTEGER"),
        bigquery.SchemaField("day_name", "STRING"),
        bigquery.SchemaField("week_of_year", "INTEGER"),
        bigquery.SchemaField("month", "INTEGER"),
        bigquery.SchemaField("month_name", "STRING"),
        bigquery.SchemaField("quarter", "INTEGER"),
        bigquery.SchemaField("year", "INTEGER"),
        bigquery.SchemaField("is_weekend", "BOOLEAN"),
    ]
    
    dim_timestamp_table_ref = f"{dataset_ref}.dim_timestamp"
    dim_timestamp_table = bigquery.Table(dim_timestamp_table_ref, schema=dim_timestamp_schema)
    dim_timestamp_table = client.create_table(dim_timestamp_table, exists_ok=True)
    logger.info(f"Created table {dim_timestamp_table_ref}")
    
    # Fact: Price Snapshots
    fact_price_schema = [
        bigquery.SchemaField("snapshot_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("coin_key", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("timestamp_key", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("current_price", "FLOAT64"),
        bigquery.SchemaField("market_cap", "FLOAT64"),
        bigquery.SchemaField("market_cap_rank", "INTEGER"),
        bigquery.SchemaField("total_volume", "FLOAT64"),
        bigquery.SchemaField("high_24h", "FLOAT64"),
        bigquery.SchemaField("low_24h", "FLOAT64"),
        bigquery.SchemaField("price_change_24h", "FLOAT64"),
        bigquery.SchemaField("price_change_percentage_24h", "FLOAT64"),
        bigquery.SchemaField("market_cap_change_24h", "FLOAT64"),
        bigquery.SchemaField("circulating_supply", "FLOAT64"),
        bigquery.SchemaField("total_supply", "FLOAT64"),
        bigquery.SchemaField("max_supply", "FLOAT64"),
        bigquery.SchemaField("data_source", "STRING"),
    ]
    
    fact_price_table_ref = f"{dataset_ref}.fact_price_snapshots"
    fact_price_table = bigquery.Table(fact_price_table_ref, schema=fact_price_schema)
    fact_price_table = client.create_table(fact_price_table, exists_ok=True)
    logger.info(f"Created table {fact_price_table_ref}")
    
    # Fact: Sentiment
    fact_sentiment_schema = [
        bigquery.SchemaField("sentiment_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("timestamp_key", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("sentiment_value", "INTEGER"),
        bigquery.SchemaField("sentiment_classification", "STRING"),
        bigquery.SchemaField("data_source", "STRING"),
    ]
    
    fact_sentiment_table_ref = f"{dataset_ref}.fact_sentiment"
    fact_sentiment_table = bigquery.Table(fact_sentiment_table_ref, schema=fact_sentiment_schema)
    fact_sentiment_table = client.create_table(fact_sentiment_table, exists_ok=True)
    logger.info(f"Created table {fact_sentiment_table_ref}")
    
    logger.info("All dimensional model tables created successfully")


if __name__ == "__main__":
    create_dimensional_tables()