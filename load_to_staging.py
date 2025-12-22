"""
Load data from GCS into BigQuery staging tables
"""

from google.cloud import bigquery
from google.cloud import storage
import logging
import os
import json
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def list_gcs_files(bucket_name, prefix, limit=None):
    """
    List files in GCS bucket with given prefix
    
    Args:
        bucket_name: GCS bucket name
        prefix: Path prefix to filter files
        limit: Maximum number of files to return
        
    Returns:
        list: List of blob objects
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        if limit:
            blobs = blobs[:limit]
        
        logger.info(f"Found {len(blobs)} files with prefix '{prefix}'")
        return blobs
        
    except Exception as e:
        logger.error(f"Failed to list GCS files: {e}")
        return []


def load_prices_to_staging(project_id, dataset_id, bucket_name, limit_files=10):
    """
    Load price data from GCS into BigQuery staging table
    
    Args:
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        bucket_name: GCS bucket name
        limit_files: Number of files to load (for testing)
    """
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset_id}.stg_prices_raw"
    
    logger.info(f"Loading price data into {table_id}")
    
    # List recent price files
    blobs = list_gcs_files(bucket_name, "raw/prices/", limit=limit_files)
    
    if not blobs:
        logger.warning("No price files found in GCS")
        return 0
    
    records_loaded = 0
    
    for blob in blobs:
        try:
            # Download and parse JSON
            content = blob.download_as_text()
            data = json.loads(content)
            
            # Extract records from nested structure
            if 'data' in data and isinstance(data['data'], list):
                fetch_timestamp = data.get('fetch_timestamp')
                source = data.get('source', 'coingecko')
                
                rows_to_insert = []
                for coin in data['data']:
                    row = {
                        'fetch_timestamp': fetch_timestamp,
                        'source': source,
                        'coin_id': coin.get('id'),
                        'symbol': coin.get('symbol'),
                        'name': coin.get('name'),
                        'current_price': coin.get('current_price'),
                        'market_cap': coin.get('market_cap'),
                        'market_cap_rank': coin.get('market_cap_rank'),
                        'total_volume': coin.get('total_volume'),
                        'high_24h': coin.get('high_24h'),
                        'low_24h': coin.get('low_24h'),
                        'price_change_24h': coin.get('price_change_24h'),
                        'price_change_percentage_24h': coin.get('price_change_percentage_24h'),
                        'market_cap_change_24h': coin.get('market_cap_change_24h'),
                        'circulating_supply': coin.get('circulating_supply'),
                        'total_supply': coin.get('total_supply'),
                        'max_supply': coin.get('max_supply'),
                        'last_updated': coin.get('last_updated'),
                    }
                    rows_to_insert.append(row)
                
                # Insert rows
                if rows_to_insert:
                    errors = client.insert_rows_json(table_id, rows_to_insert)
                    if errors:
                        logger.error(f"Errors inserting rows from {blob.name}: {errors}")
                    else:
                        records_loaded += len(rows_to_insert)
                        logger.info(f"Loaded {len(rows_to_insert)} records from {blob.name}")
            
        except Exception as e:
            logger.error(f"Failed to process {blob.name}: {e}")
            continue
    
    logger.info(f"Total price records loaded: {records_loaded}")
    return records_loaded


def load_sentiment_to_staging(project_id, dataset_id, bucket_name, limit_files=10):
    """
    Load sentiment data from GCS into BigQuery staging table
    
    Args:
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        bucket_name: GCS bucket name
        limit_files: Number of files to load (for testing)
    """
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset_id}.stg_sentiment_raw"
    
    logger.info(f"Loading sentiment data into {table_id}")
    
    # List recent sentiment files
    blobs = list_gcs_files(bucket_name, "raw/sentiment/", limit=limit_files)
    
    if not blobs:
        logger.warning("No sentiment files found in GCS")
        return 0
    
    records_loaded = 0
    
    for blob in blobs:
        try:
            # Download and parse JSON
            content = blob.download_as_text()
            data = json.loads(content)
            
            # Extract records from nested structure
            if 'data' in data and isinstance(data['data'], list):
                fetch_timestamp = data.get('fetch_timestamp')
                source = data.get('source', 'feargreed_index')
                
                rows_to_insert = []
                for record in data['data']:
                    row = {
                        'fetch_timestamp': fetch_timestamp,
                        'source': source,
                        'value': int(record.get('value')),
                        'value_classification': record.get('value_classification'),
                        'timestamp': datetime.fromtimestamp(
                            int(record.get('timestamp')), 
                            tz=timezone.utc
                        ).isoformat(),
                        'time_until_update': record.get('time_until_update'),
                    }
                    rows_to_insert.append(row)
                
                # Insert rows
                if rows_to_insert:
                    errors = client.insert_rows_json(table_id, rows_to_insert)
                    if errors:
                        logger.error(f"Errors inserting rows from {blob.name}: {errors}")
                    else:
                        records_loaded += len(rows_to_insert)
                        logger.info(f"Loaded {len(rows_to_insert)} records from {blob.name}")
            
        except Exception as e:
            logger.error(f"Failed to process {blob.name}: {e}")
            continue
    
    logger.info(f"Total sentiment records loaded: {records_loaded}")
    return records_loaded


def run_load_to_staging(limit_files=10):
    """
    Main function to load all data from GCS to BigQuery staging
    
    Args:
        limit_files: Number of files to load per data type (for testing)
    """
    project_id = os.getenv('GCP_PROJECT_ID')
    dataset_id = os.getenv('BQ_DATASET', 'crypto_pipeline')
    bucket_name = os.getenv('GCS_BUCKET_NAME')
    
    if not all([project_id, bucket_name]):
        logger.error("Missing required environment variables")
        return False
    
    logger.info("=" * 60)
    logger.info("Starting GCS to BigQuery staging load")
    logger.info("=" * 60)
    
    # Load prices
    logger.info("\n[1/2] Loading price data...")
    prices_count = load_prices_to_staging(
        project_id, dataset_id, bucket_name, limit_files
    )
    
    # Load sentiment
    logger.info("\n[2/2] Loading sentiment data...")
    sentiment_count = load_sentiment_to_staging(
        project_id, dataset_id, bucket_name, limit_files
    )
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("Load Summary")
    logger.info("=" * 60)
    logger.info(f"Price records loaded: {prices_count}")
    logger.info(f"Sentiment records loaded: {sentiment_count}")
    logger.info("=" * 60)
    
    return True


if __name__ == "__main__":
    # Start with a small number for testing
    # Remove limit_files parameter to load all files
    success = run_load_to_staging(limit_files=10)
    exit(0 if success else 1)