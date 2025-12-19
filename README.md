# Crypto Market Monitoring Pipeline

An end-to-end data engineering project that ingests cryptocurrency market data from multiple sources, processes it incrementally, and surfaces insights through analytics and dashboards.

## Project Overview

This pipeline demonstrates:
- Multi-source data ingestion (price and sentiment data)
- Cloud-based data storage and warehousing (Google Cloud Platform)
- Incremental data processing with dbt
- Dimensional modeling (star schema)
- Data quality monitoring and validation
- Automated orchestration

## Architecture

```
[CoinGecko API] ──┐
                  ├─> [Python Ingestion] -> [GCS Storage] -> [BigQuery Staging] -> [dbt Transformation] -> [Dashboards]
[Fear & Greed] ───┘
```

### Data Flow
1. **Sources**: CoinGecko API (prices/volume), Fear & Greed Index (market sentiment)
2. **Landing Zone**: Raw JSON files in Google Cloud Storage with timestamp partitioning
3. **Staging**: BigQuery tables for initial data landing
4. **Warehouse**: Star schema with fact and dimension tables
5. **Presentation**: Dashboards and analytics via Tableau/Looker

## Technology Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.11+ |
| Environment | pipenv |
| Cloud Platform | Google Cloud Platform |
| Storage | Google Cloud Storage |
| Data Warehouse | BigQuery |
| Transformation | dbt |
| Orchestration | Python (future: Prefect) |
| Visualization | Tableau/Looker/Power BI |

## Project Structure

```
crypto_market_monitoring_pipeline/
├── src/
│   ├── ingestion/
│   │   ├── fetch_prices.py       # CoinGecko price data ingestion
│   │   ├── fetch_sentiment.py    # Fear & Greed sentiment ingestion
│   │   ├── run_ingestion.py      # Orchestration script
│   │   └── test_apis.py          # API connectivity tests
│   ├── transformations/          # Future: dbt-independent transforms
│   └── quality_checks/           # Future: data quality scripts
├── dbt/                          # Future: dbt project
├── config/
│   └── gcp-service-account.json  # GCP credentials (gitignored)
├── docs/
│   └── ARCHITECTURE.md           # Detailed architecture documentation
├── data/
│   └── raw/                      # Local data storage (gitignored)
├── .env                          # Environment variables (gitignored)
├── .gitignore
├── Pipfile
├── Pipfile.lock
├── README.md
└── test_bigquery_connection.py   # GCP connection test
```

## Setup Instructions

### Prerequisites
- Python 3.11 or higher
- pipenv (`pip install pipenv`)
- Google Cloud Platform account
- Git

### 1. Clone the Repository

```bash
git clone https://github.com/shaneMcCabe2/crypto_market_monitoring_pipeline.git
cd crypto_market_monitoring_pipeline
```

### 2. Set Up Python Environment

```bash
# Create virtual environment and install dependencies
pipenv install

# Activate the environment
pipenv shell
```

### 3. Configure Google Cloud Platform

#### Create GCP Project
1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Create a new project (e.g., "crypto-market-pipeline")
3. Enable BigQuery API and Cloud Storage API

#### Create GCS Bucket
1. Navigate to Cloud Storage → Buckets
2. Create bucket: `crypto-pipeline-raw-data`
3. Choose region matching your BigQuery dataset

#### Create Service Account
1. Go to IAM & Admin → Service Accounts
2. Create service account: `crypto-pipeline-service`
3. Grant roles:
   - BigQuery Data Editor
   - BigQuery Job User
   - Storage Object Admin
4. Create JSON key and download to `config/gcp-service-account.json`

### 4. Configure Environment Variables

Create `.env` file in project root:

```env
# Google Cloud credentials
GOOGLE_APPLICATION_CREDENTIALS=./config/gcp-service-account.json
GCP_PROJECT_ID=your-project-id
GCS_BUCKET_NAME=crypto-pipeline-raw-data

# BigQuery
BQ_DATASET=crypto_pipeline
```

### 5. Test GCP Connection

```bash
pipenv run python test_bigquery_connection.py
```

You should see successful connections to both BigQuery and Cloud Storage.

### 6. Test API Access

```bash
pipenv run python src/ingestion/test_apis.py
```

This verifies connectivity to CoinGecko and Fear & Greed Index APIs.

## Usage

### Manual Ingestion

Run individual ingestion scripts:

```bash
# Fetch price data
pipenv run python src/ingestion/fetch_prices.py

# Fetch sentiment data
pipenv run python src/ingestion/fetch_sentiment.py
```

### Full Pipeline

Run both ingestion jobs together:

```bash
cd src/ingestion
pipenv run python run_ingestion.py
```

### Scheduled Ingestion (Optional)

**Windows (Task Scheduler):**
1. Open Task Scheduler
2. Create Basic Task
3. Trigger: Daily, repeat every 15 minutes
4. Action: `python.exe C:\path\to\crypto_pipeline\src\ingestion\run_ingestion.py`

**Linux/Mac (cron):**
```bash
# Edit crontab
crontab -e

# Add entry to run every 15 minutes
*/15 * * * * cd /path/to/crypto_pipeline/src/ingestion && /path/to/pipenv run python run_ingestion.py
```

## Data Sources

### CoinGecko API
- **Endpoint**: `/coins/markets`
- **Data**: Price, volume, market cap for top 50 cryptocurrencies
- **Rate Limit**: 10-50 calls/min (free tier)
- **Update Frequency**: Every 15 minutes
- **No authentication required**

### Fear & Greed Index
- **Endpoint**: `https://api.alternative.me/fng/`
- **Data**: Market sentiment score (0-100)
- **Rate Limit**: No official limit
- **Update Frequency**: Daily (but queried every 15 min with price data)
- **No authentication required**

## Current Status

**Completed:**
- ✅ Phase 1: Environment setup and API testing
- ✅ Phase 2: Data ingestion layer
  - Price data ingestion from CoinGecko
  - Sentiment data ingestion from Fear & Greed Index
  - Local and GCS storage with timestamp partitioning
  - Orchestration script for running both jobs

**In Progress:**
- 🔄 Phase 3: Data warehouse design (staging tables, dimensional model)

**Planned:**
- ⏳ Phase 4: dbt transformation layer
- ⏳ Phase 5: Data quality monitoring
- ⏳ Phase 6: Dashboards and analytics
- ⏳ Phase 7: Documentation and polish

## Key Features

### Idempotent Ingestion
Scripts can be safely re-run without creating duplicate data. Each run creates a new timestamped file.

### Error Handling
Comprehensive error handling with detailed logging. Failed GCS uploads don't prevent local saves.

### Data Validation
Basic schema validation ensures required fields are present before saving data.

### Timestamp Partitioning
Data organized by `YYYY/MM/DD/HH/` structure for efficient querying and management.

## Future Enhancements

- Implement Prefect for orchestration and monitoring
- Add incremental processing in dbt
- Implement data quality tests with Great Expectations
- Add anomaly detection for price spikes
- Create interactive dashboards
- Add on-chain metrics (transaction volumes, wallet activity)
- Implement data versioning and lineage tracking

## Development

### Running Tests

```bash
# Test GCP connectivity
pipenv run python test_bigquery_connection.py

# Test API connectivity
pipenv run python src/ingestion/test_apis.py
```

### Adding Dependencies

```bash
pipenv install package-name
```

### Code Style
- Follow PEP 8 style guidelines
- Use descriptive variable names
- Add docstrings to all functions
- Include logging for key operations

## Troubleshooting

### GCS Upload Fails
- Verify service account has Storage Object Admin role
- Check `GCS_BUCKET_NAME` in `.env` matches actual bucket name
- Ensure bucket exists and is in correct region

### BigQuery Connection Fails
- Verify service account has BigQuery roles
- Check `GCP_PROJECT_ID` in `.env` is correct
- Ensure BigQuery API is enabled

### API Rate Limits
- CoinGecko free tier: 10-50 calls/min
- If hitting limits, reduce ingestion frequency or add delays

## Contributing

This is a portfolio project, but suggestions are welcome! Please open an issue to discuss proposed changes.

## License

This project is for educational and portfolio purposes.

## Contact

Shane McCabe
- GitHub: [@shaneMcCabe2](https://github.com/shaneMcCabe2)
- Project Link: [crypto_market_monitoring_pipeline](https://github.com/shaneMcCabe2/crypto_market_monitoring_pipeline)

## Acknowledgments

- [CoinGecko](https://www.coingecko.com) for cryptocurrency market data
- [alternative.me](https://alternative.me) for Fear & Greed Index
- Google Cloud Platform for infrastructure
