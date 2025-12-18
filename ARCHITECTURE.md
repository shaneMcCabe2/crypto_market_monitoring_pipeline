# Crypto Market Monitoring Pipeline - Architecture

## Overview
End-to-end data pipeline that ingests cryptocurrency market data from multiple sources, processes it incrementally, and surfaces insights through dashboards.

## Data Flow

```
[Data Sources] → [Landing Zone] → [Staging] → [Data Warehouse] → [Presentation Layer]
```

### 1. Data Sources
- **CoinGecko API**: Price, volume, market cap data (15-min intervals)
- **Reddit API**: Sentiment data from crypto subreddits
- **Future**: On-chain metrics (optional enhancement)

### 2. Landing Zone (GCS Raw Storage)
- **Location**: `gs://crypto-pipeline-raw-data/raw/`
- **Structure**:
  - `/raw/prices/YYYY/MM/DD/HH/data.json`
  - `/raw/sentiment/YYYY/MM/DD/HH/data.json`
- **Purpose**: Immutable raw data storage, enables reprocessing

### 3. Staging Layer (BigQuery)
- **Tables**: `stg_prices_raw`, `stg_sentiment_raw`
- **Purpose**: Initial landing in warehouse, minimal transformation
- **Processing**: Data loaded from GCS, light cleaning and type casting

### 4. Data Warehouse (BigQuery)
- **Schema**: Star schema with fact and dimension tables
- **Fact Tables**:
  - `fact_price_snapshots`: Time-series price/volume data
  - `fact_sentiment`: Aggregated sentiment metrics
- **Dimension Tables**:
  - `dim_coin`: Coin attributes and metadata
  - `dim_timestamp`: Time dimension for analysis
- **Transformation Tool**: dbt for modeling and incremental processing

### 5. Presentation Layer
- **Dashboards**: Tableau/Looker connected to BigQuery
- **Analytics**: Python/Jupyter notebooks for ad-hoc analysis
- **Key Metrics**: Price trends, volatility, sentiment correlation

## Technology Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | Python (requests, praw) |
| Storage | Google Cloud Storage |
| Warehouse | BigQuery |
| Transformation | dbt |
| Orchestration | Cron (initial), Prefect (future) |
| Visualization | Tableau/Looker/Power BI |

## Update Frequency
- **Price data**: Every 15 minutes
- **Sentiment data**: Every 15 minutes
- **Dashboard refresh**: Real-time (streaming) or 15-min cache

## Data Quality Strategy
- Idempotent ingestion (safe reruns)
- Incremental processing (only new data)
- dbt tests on all models (not_null, unique, relationships)
- Anomaly detection for price spikes
- Pipeline monitoring and alerting

## Future Enhancements
- Add on-chain metrics (transaction volumes, wallet activity)
- Implement Prefect for orchestration
- Add machine learning for volatility prediction
- Real-time streaming with Pub/Sub
- Data versioning and lineage tracking
