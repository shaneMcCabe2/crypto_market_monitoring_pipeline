"""
Test script to verify BigQuery connection and environment setup
"""

import os
from google.cloud import bigquery
from google.cloud import storage

def test_bigquery_connection():
    """Test BigQuery connection and basic operations"""
    try:
        # Initialize BigQuery client
        client = bigquery.Client()
        
        print("[OK] BigQuery client initialized successfully")
        print(f"  Project ID: {client.project}")
        
        # Test query - just select 1 to verify connection works
        query = "SELECT 1 as test_value"
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            print(f"[OK] Test query executed successfully: {row.test_value}")
        
        # List datasets in project
        datasets = list(client.list_datasets())
        if datasets:
            print(f"[OK] Found {len(datasets)} dataset(s) in project:")
            for dataset in datasets:
                print(f"  - {dataset.dataset_id}")
        else:
            print("  No datasets found (this is fine for a new project)")
        
        return True
        
    except Exception as e:
        print(f"[FAIL] BigQuery connection failed: {e}")
        return False

def test_gcs_connection():
    """Test Google Cloud Storage connection"""
    try:
        # Initialize GCS client
        storage_client = storage.Client()
        
        print("\n[OK] GCS client initialized successfully")
        
        # List buckets
        buckets = list(storage_client.list_buckets())
        if buckets:
            print(f"[OK] Found {len(buckets)} bucket(s):")
            for bucket in buckets:
                print(f"  - {bucket.name}")
        else:
            print("  No buckets found yet")
        
        return True
        
    except Exception as e:
        print(f"[FAIL] GCS connection failed: {e}")
        return False

def test_environment_variables():
    """Check that required environment variables are set"""
    print("\nChecking environment variables:")
    
    required_vars = [
        'GOOGLE_APPLICATION_CREDENTIALS',
        'GCP_PROJECT_ID',
        'GCS_BUCKET_NAME'
    ]
    
    all_set = True
    for var in required_vars:
        value = os.getenv(var)
        if value:
            # Mask credentials path for security
            display_value = value if var != 'GOOGLE_APPLICATION_CREDENTIALS' else '***set***'
            print(f"[OK] {var}: {display_value}")
        else:
            print(f"[FAIL] {var}: not set")
            all_set = False
    
    return all_set

if __name__ == "__main__":
    print("=" * 60)
    print("Testing BigQuery & GCS Environment Setup")
    print("=" * 60)
    
    # Test environment variables
    env_ok = test_environment_variables()
    
    if not env_ok:
        print("\n[WARNING] Some environment variables are missing")
        print("Make sure your .env file is set up correctly")
        exit(1)
    
    # Test BigQuery
    print("\n" + "-" * 60)
    print("Testing BigQuery Connection")
    print("-" * 60)
    bq_ok = test_bigquery_connection()
    
    # Test GCS
    print("\n" + "-" * 60)
    print("Testing Google Cloud Storage Connection")
    print("-" * 60)
    gcs_ok = test_gcs_connection()
    
    # Summary
    print("\n" + "=" * 60)
    if bq_ok and gcs_ok:
        print("[OK] All tests passed! Environment is ready.")
    else:
        print("[FAIL] Some tests failed. Check the errors above.")
    print("=" * 60)