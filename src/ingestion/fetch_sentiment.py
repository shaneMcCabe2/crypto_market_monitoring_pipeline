"""
Sentiment data ingestion script for Fear & Greed Index API
Fetches crypto market sentiment and saves to local JSON and GCS
"""

import requests
import json
import os
from datetime import datetime, timezone
from google.cloud import storage
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fetch_feargreed_sentiment(limit=1):
    """
    Fetch sentiment data from Fear & Greed Index API
    
    Args:
        limit: Number of records to fetch (default 1 for current value)
        
    Returns:
        dict: API response data or None if failed
    """
    url = "https://api.alternative.me/fng/"
    params = {'limit': limit} if limit > 1 else {}
    
    try:
        logger.info(f"Fetching Fear & Greed Index (limit={limit})...")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get('data'):
            logger.info(f"Successfully fetched {len(data['data'])} sentiment record(s)")
            return data['data']
        else:
            logger.error("No data in API response")
            return None
        
    except requests.exceptions.Timeout:
        logger.error("Request timed out")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON response: {e}")
        return None


def save_to_local_json(data, output_dir='data/raw/sentiment'):
    """
    Save data to local JSON file with timestamp partitioning
    
    Args:
        data: Data to save
        output_dir: Base directory for output files
        
    Returns:
        str: Path to saved file or None if failed
    """
    if data is None:
        logger.error("No data to save")
        return None
    
    try:
        # Create timestamp-based directory structure: YYYY/MM/DD/HH/
        now = datetime.now(timezone.utc)
        timestamp_dir = os.path.join(
            output_dir,
            now.strftime('%Y'),
            now.strftime('%m'),
            now.strftime('%d'),
            now.strftime('%H')
        )
        
        # Create directory if it doesn't exist
        os.makedirs(timestamp_dir, exist_ok=True)
        
        # Create filename with full timestamp
        filename = f"sentiment_{now.strftime('%Y%m%d_%H%M%S')}.json"
        filepath = os.path.join(timestamp_dir, filename)
        
        # Save data
        with open(filepath, 'w') as f:
            json.dump({
                'fetch_timestamp': now.isoformat(),
                'source': 'feargreed_index',
                'num_records': len(data),
                'data': data
            }, f, indent=2)
        
        logger.info(f"Saved data to {filepath}")
        return filepath
        
    except Exception as e:
        logger.error(f"Failed to save data: {e}")
        return None


def upload_to_gcs(local_filepath, bucket_name=None):
    """
    Upload file to Google Cloud Storage
    
    Args:
        local_filepath: Path to local file
        bucket_name: GCS bucket name (from env if not provided)
        
    Returns:
        str: GCS path or None if failed
    """
    if not local_filepath or not os.path.exists(local_filepath):
        logger.error(f"Local file not found: {local_filepath}")
        return None
    
    # Get bucket name from environment if not provided
    if bucket_name is None:
        bucket_name = os.getenv('GCS_BUCKET_NAME')
    
    if not bucket_name:
        logger.error("GCS_BUCKET_NAME not set in environment")
        return None
    
    try:
        # Initialize GCS client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # Create GCS path matching local structure
        # Remove 'data/' prefix if present and use raw/ prefix for GCS
        gcs_path = local_filepath.replace('\\', '/')  # Windows path fix
        if gcs_path.startswith('data/'):
            gcs_path = gcs_path[5:]  # Remove 'data/' prefix
        
        # Upload file
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_filepath)
        
        logger.info(f"Uploaded to gs://{bucket_name}/{gcs_path}")
        return f"gs://{bucket_name}/{gcs_path}"
        
    except Exception as e:
        logger.error(f"Failed to upload to GCS: {e}")
        return None


def validate_data(data):
    """
    Basic validation of fetched data
    
    Args:
        data: Data to validate
        
    Returns:
        bool: True if valid, False otherwise
    """
    if not data:
        logger.warning("Data is empty")
        return False
    
    if not isinstance(data, list):
        logger.warning("Data is not a list")
        return False
    
    # Check first record has required fields
    required_fields = ['value', 'value_classification', 'timestamp']
    if data and isinstance(data[0], dict):
        missing_fields = [f for f in required_fields if f not in data[0]]
        if missing_fields:
            logger.warning(f"Missing required fields: {missing_fields}")
            return False
    
    logger.info("Data validation passed")
    return True


def run_ingestion(upload_gcs=True):
    """
    Main ingestion function
    Orchestrates the fetch, save, and upload process
    
    Args:
        upload_gcs: Whether to upload to GCS (default True)
    """
    logger.info("Starting sentiment ingestion...")
    
    # Fetch data (just current value)
    data = fetch_feargreed_sentiment(limit=1)
    
    # Validate data
    if not validate_data(data):
        logger.error("Data validation failed, aborting")
        return False
    
    # Save to local file
    filepath = save_to_local_json(data)
    
    if not filepath:
        logger.error("Failed to save locally, aborting")
        return False
    
    # Upload to GCS if enabled
    if upload_gcs:
        gcs_path = upload_to_gcs(filepath)
        if not gcs_path:
            logger.warning("GCS upload failed, but local save succeeded")
            # Don't fail the whole job if GCS upload fails
    
    logger.info("Ingestion completed successfully")
    return True


if __name__ == "__main__":
    # Can disable GCS upload for testing with: run_ingestion(upload_gcs=False)
    success = run_ingestion()
    exit(0 if success else 1)