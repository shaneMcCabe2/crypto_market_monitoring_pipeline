"""
Orchestration script to run all ingestion jobs
Runs price and sentiment ingestion in sequence
"""

import sys
import logging
from datetime import datetime, timezone

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import ingestion modules
try:
    from fetch_prices import run_ingestion as run_price_ingestion
    from fetch_sentiment import run_ingestion as run_sentiment_ingestion
except ImportError as e:
    logger.error(f"Failed to import ingestion modules: {e}")
    logger.error("Make sure you're running from src/ingestion/ directory")
    sys.exit(1)


def run_all_ingestion():
    """
    Run all ingestion jobs in sequence
    
    Returns:
        dict: Results for each job
    """
    start_time = datetime.now(timezone.utc)
    logger.info("=" * 60)
    logger.info("Starting full ingestion pipeline")
    logger.info(f"Start time: {start_time.isoformat()}")
    logger.info("=" * 60)
    
    results = {
        'prices': False,
        'sentiment': False,
        'start_time': start_time.isoformat(),
        'end_time': None,
        'duration_seconds': None
    }
    
    # Run price ingestion
    logger.info("\n[1/2] Running price ingestion...")
    try:
        results['prices'] = run_price_ingestion(upload_gcs=True)
        if results['prices']:
            logger.info("[1/2] Price ingestion completed successfully")
        else:
            logger.error("[1/2] Price ingestion failed")
    except Exception as e:
        logger.error(f"[1/2] Price ingestion error: {e}")
        results['prices'] = False
    
    # Run sentiment ingestion
    logger.info("\n[2/2] Running sentiment ingestion...")
    try:
        results['sentiment'] = run_sentiment_ingestion(upload_gcs=True)
        if results['sentiment']:
            logger.info("[2/2] Sentiment ingestion completed successfully")
        else:
            logger.error("[2/2] Sentiment ingestion failed")
    except Exception as e:
        logger.error(f"[2/2] Sentiment ingestion error: {e}")
        results['sentiment'] = False
    
    # Calculate duration
    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()
    results['end_time'] = end_time.isoformat()
    results['duration_seconds'] = duration
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("Pipeline Summary")
    logger.info("=" * 60)
    logger.info(f"Price ingestion: {'SUCCESS' if results['prices'] else 'FAILED'}")
    logger.info(f"Sentiment ingestion: {'SUCCESS' if results['sentiment'] else 'FAILED'}")
    logger.info(f"Duration: {duration:.2f} seconds")
    logger.info("=" * 60)
    
    return results


if __name__ == "__main__":
    results = run_all_ingestion()
    
    # Exit with error code if any job failed
    all_success = all([results['prices'], results['sentiment']])
    
    if all_success:
        logger.info("All ingestion jobs completed successfully")
        sys.exit(0)
    else:
        logger.error("Some ingestion jobs failed")
        sys.exit(1)