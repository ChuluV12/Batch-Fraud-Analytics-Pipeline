# Batch Fraud Analytics Pipeline

An end-to-end AWS data engineering project that processes 300,000 synthetic credit card transactions to detect fraud patterns using a batch ETL architecture.

---

## Project Overview

This pipeline ingests raw transaction data, cleans and enriches it using PySpark (AWS Glue), runs SQL-based fraud detection logic using Amazon Athena, and visualizes high-risk users in an interactive dashboard.

Live Dashboard: Replace with your dashboard link

---

## Architecture

```text
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
```

---

## Tech Stack

| Category | Technology |
|---|---|
| Storage | Amazon S3 (Raw & Processed Buckets) |
| Compute | AWS Glue (PySpark ETL, Crawlers) |
| Analytics | Amazon Athena (SQL) |
| Visualization | Plotly (Python) |
| Data Format | Parquet with Snappy Compression |

---

## Key Features

### Synthetic Data Generation

- Generates 300,000 synthetic credit card transactions
- Simulates a 1.5% fraud rate
- Includes multiple fraud patterns:
  - High Amount Fraud
  - Foreign Transactions
  - Card-Not-Present Fraud
  - Rapid Succession Transactions

### ETL Pipeline

The ETL pipeline performs:

- Data type casting
- Null handling
- Data cleaning
- Feature engineering
- Column enrichment

Additional derived fields include:

- `amount_bucket`
- `is_foreign_txn`
- Date and timestamp features

### Fraud Detection Logic

Athena SQL queries are used to:

- Detect rapid succession transactions
- Identify high-risk users
- Calculate fraud metrics
- Generate weighted fraud risk scores

### Partitioned Data Lake

Processed data is partitioned by:

- `year`
- `month`

Partitioning improves Athena query performance and reduces query cost.

### Cost Optimization

The pipeline is configured for low-cost execution using:

- Glue Worker Type: `G.1X`
- 2 workers
- Parquet compression with Snappy

Estimated total run cost is under $1.00.

---

## Dashboard Insights

The dashboard includes:

### KPIs

- Total fraud flags
- Fraud rate
- Total fraud amount

### Fraud Analysis

- Fraud distribution by type
- Fraud distribution by country

### Trend Analysis

- Daily fraud activity over time

### Risk Monitoring

- Top 20 high-risk users ranked by weighted fraud score

---

## Repository Structure

```bash
.
├── athena_queries/       # SQL scripts for fraud detection
├── glue_jobs/            # PySpark ETL scripts
├── docs/                 # HTML dashboard for GitHub Pages
├── notebooks/            # Colab notebook for data generation
├── .gitignore
└── README.md
```

---

## Setup Instructions

### Prerequisites

Before running the project, ensure you have:

- An AWS account
- Permissions for:
  - AWS Glue
  - Amazon S3
  - Amazon Athena
- Python 3.x or Google Colab

---

## Phase 1: Data Generation

1. Open the notebook in Google Colab
2. Add the following credentials to Colab Secrets:
   - `AWS_ACCESS_KEY_ID`
   - `AWS_SECRET_ACCESS_KEY`
   - `S3_SUFFIX`
3. Run all notebook cells
4. The notebook will generate transaction data and upload it to S3

---

## Phase 2: AWS Glue Crawler

1. Open AWS Glue
2. Navigate to Crawlers
3. Create a crawler pointing to:

```text
s3://fraud-raw-<suffix>/raw/
```

4. Set the target database to:

```text
fraud_db
```

5. Run the crawler to create the raw transactions table

---

## Phase 3: ETL Job

1. Open AWS Glue
2. Navigate to ETL Jobs
3. Create a Spark job using:

```text
glue_jobs/fraud_etl.py
```

4. Configure the following job parameters:

```text
--RAW_S3_PATH=s3://fraud-raw-<suffix>/raw/
--PROCESSED_S3_PATH=s3://fraud-processed-<suffix>/processed/
--DATABASE_NAME=fraud_db
```

5. Recommended worker configuration:

```text
Worker Type: G.1X
Workers: 2
```

6. Run the ETL job

After the ETL job completes, run another crawler on the processed bucket to update the Glue Data Catalog.

---

## Phase 4: Athena Queries

1. Open the Athena Query Editor
2. Set the output location to:

```text
s3://fraud-processed-<suffix>/athena-results/
```

3. Run the SQL files inside `athena_queries/` in the following order:

```text
01 → 02 → 03 → 04
```

---

## Phase 5: Dashboard Deployment

1. Re-open the Colab notebook
2. Run the dashboard generation cells
3. Download the generated `dashboard.html`
4. Place the file inside the `docs/` directory
5. Enable GitHub Pages:
   - Branch: `main`
   - Folder: `/docs`

---

## Estimated AWS Cost

| Service | Estimated Cost |
|---|---|
| Amazon S3 Storage | ~$0.00 |
| Glue Crawler | ~$0.00 |
| Glue ETL Job | ~$0.44 |
| Athena Queries | ~$0.01 |
| Total | < $0.50 |

---

## Future Improvements

- Real-time fraud detection with Kafka or Kinesis
- Machine learning fraud scoring
- Workflow orchestration using AWS Step Functions
- CI/CD pipeline integration
- dbt transformation layer
- Data quality monitoring

---

## Author

Chulumanco Vumazonke
