"""
Initialize BigQuery staging tables for raw data
"""

from google.cloud import bigquery
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_staging_tables(project_id=None, dataset_id='crypto_pipeline'):
    """
    Create staging tables in BigQuery for raw data
    """
    if project_id is None:
        project_id = os.getenv('GCP_PROJECT_ID')
    
    client = bigquery.Client(project=project_id)
    
    # Create dataset if it doesn't exist
    dataset_ref = f"{project_id}.{dataset_id}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    dataset = client.create_dataset(dataset, exists_ok=True)
    logger.info(f"Dataset {dataset_id} ready")
    
    # Staging table for prices
    prices_schema = [
        bigquery.SchemaField("fetch_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("coin_id", "STRING"),
        bigquery.SchemaField("symbol", "STRING"),
        bigquery.SchemaField("name", "STRING"),
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
        bigquery.SchemaField("last_updated", "TIMESTAMP"),
    ]
    
    prices_table_ref = f"{dataset_ref}.stg_prices_raw"
    prices_table = bigquery.Table(prices_table_ref, schema=prices_schema)
    prices_table = client.create_table(prices_table, exists_ok=True)
    logger.info(f"Created table {prices_table_ref}")
    
    # Staging table for sentiment
    sentiment_schema = [
        bigquery.SchemaField("fetch_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("value", "INTEGER"),
        bigquery.SchemaField("value_classification", "STRING"),
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
        bigquery.SchemaField("time_until_update", "INTEGER"),
    ]
    
    sentiment_table_ref = f"{dataset_ref}.stg_sentiment_raw"
    sentiment_table = bigquery.Table(sentiment_table_ref, schema=sentiment_schema)
    sentiment_table = client.create_table(sentiment_table, exists_ok=True)
    logger.info(f"Created table {sentiment_table_ref}")
    
    logger.info("All staging tables created successfully")


if __name__ == "__main__":
    create_staging_tables()