Batch Fraud Analytics Pipeline
An end-to-end AWS data engineering project that processes 300,000 synthetic credit card transactions to detect fraud patterns using a batch ETL architecture.

ArchitectureCostDashboard

Project Overview
This pipeline ingests raw transaction data, cleans and enriches it using PySpark (AWS Glue), runs SQL-based fraud detection logic (Amazon Athena), and visualizes high-risk users in an interactive dashboard.

Live Dashboard: View Dashboard (Replace with your link)

Architecture
Synthetic Data (Colab) → S3 (Raw/CSV)
↓
AWS Glue Crawler
↓
Glue ETL (PySpark)
↓
S3 (Processed/Parquet/Partitioned)
↓
Amazon Athena (SQL)
↓
Interactive Dashboard (Plotly)
## Tech Stack
- **Storage**: Amazon S3 (Raw & Processed buckets)
- **Compute**: AWS Glue (PySpark ETL, Crawlers)
- **Analytics**: Amazon Athena (Standard SQL)
- **Visualization**: Plotly (Python)
- **Data Format**: Parquet with Snappy compression

## Key Features
1.  **Data Generation**: 300,000 transactions with 1.5% synthetic fraud rate across 4 fraud types (High Amount, Foreign, Card Not Present, Rapid Succession).
2.  **ETL Pipeline**: Automated type casting, null handling, and column enrichment (e.g., `amount_bucket`, `is_foreign_txn`).
3.  **Cost Optimization**: Configured for minimum resource usage (Glue G.1X, 2 workers) keeping total run cost under $1.00.
4.  **Fraud Logic**: Complex SQL window functions to detect rapid succession and user risk scoring.
5.  **Partitioning**: Data is partitioned by `year` and `month` to optimize Athena query performance and cost.

## Dashboard Insights
The dashboard highlights:
- **KPIs**: Total fraud flags, fraud rate, and total fraud amount.
- **Breakdown**: Fraud distribution by type (Pie Chart) and country (Bar Chart).
- **Trends**: Daily fraud volume over time.
- **Leaderboard**: Top 20 high-risk users ranked by a weighted risk score.

## Repository Structure
```bash
.
├── athena_queries/       # SQL scripts for fraud detection
├── glue_jobs/            # PySpark ETL script
├── docs/                 # HTML Dashboard
├── notebooks/            # Colab notebook for data generation
├── .gitignore            # Prevents upload of sensitive data
└── README.md
```

## Setup Instructions

### Prerequisites
- AWS Account with appropriate Glue/S3/Athena permissions.
- Python 3.x environment (or Google Colab).

### Phase 1: Data Generation
1. Open the project notebook in Google Colab.
2. Add AWS Credentials to Colab Secrets (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `S3_SUFFIX`).
3. Run all cells to generate data and upload to S3.

### Phase 2: Glue Crawler
1. Go to AWS Glue → Crawlers.
2. Create a crawler pointing to your `fraud-raw-<suffix>/raw/` S3 bucket.
3. Set target database to `fraud_db`.
4. Run the crawler to create the `raw` table.

### Phase 3: ETL Job
1. Go to AWS Glue → ETL Jobs.
2. Create a Spark job using `glue_jobs/fraud_etl.py`.
3. Set parameters:
   - `--RAW_S3_PATH`: s3://fraud-raw-<suffix>/raw/
   - `--PROCESSED_S3_PATH`: s3://fraud-processed-<suffix>/processed/
   - `--DATABASE_NAME`: fraud_db
4. Run the job (Worker type: G.1X, 2 workers).
5. **Crucial**: Re-run a crawler on the *Processed* bucket to update the catalog.

### Phase 4: Athena Queries
1. Open Athena Query Editor.
2. Set output location to `s3://fraud-processed-<suffix>/athena-results/`.
3. Run the SQL files in `athena_queries/` in order (01 → 04).

### Phase 5: Dashboard
1. Re-open the Colab notebook and run the "Phase 5" cells.
2. Download the generated `dashboard.html`.
3. Place it in the `docs/` folder of this repo.
4. Enable GitHub Pages on the `main` branch `/docs` folder.

## Estimated Cost
- S3 Storage: ~$0.00
- Glue Crawler: $0.00
- Glue ETL Job: ~$0.44 (one run)
- Athena Queries: ~$0.01
- **Total**: <$0.50

## Author
Chulumanco Vumazonke
```
