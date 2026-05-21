"""
fraud_etl.py — AWS Glue PySpark ETL Job
Phase 3: Clean raw CSV → Parquet in the processed S3 bucket

Deploy this script via:
  AWS Console → Glue → ETL Jobs → Script editor → paste this file
  OR upload to S3 and reference in job definition.

Job parameters to set in Glue (Job details → Parameters):
  --RAW_S3_PATH       s3://fraud-raw-<suffix>/raw/
  --PROCESSED_S3_PATH s3://fraud-processed-<suffix>/processed/
  --DATABASE_NAME     fraud_db
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import (
    TimestampType, DoubleType, BooleanType, StringType
)

# ── Init ───────────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'RAW_S3_PATH',
    'PROCESSED_S3_PATH',
    'DATABASE_NAME',
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

RAW_PATH       = args['RAW_S3_PATH']        # e.g. s3://fraud-raw-john/raw/
PROCESSED_PATH = args['PROCESSED_S3_PATH']  # e.g. s3://fraud-processed-john/processed/
DATABASE_NAME  = args['DATABASE_NAME']      # e.g. fraud_db

print(f"[ETL] Reading from:  {RAW_PATH}")
print(f"[ETL] Writing to:    {PROCESSED_PATH}")

# ── 1. Read raw CSV ────────────────────────────────────────────────────────────
df_raw = spark.read.option("header", "true").option("inferSchema", "false").csv(RAW_PATH)

print(f"[ETL] Raw row count: {df_raw.count():,}")
print("[ETL] Raw schema:")
df_raw.printSchema()

# ── 2. Cast data types ─────────────────────────────────────────────────────────
df_typed = (
    df_raw
    .withColumn("timestamp",    F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("amount",       F.col("amount").cast(DoubleType()))
    .withColumn("card_present", F.col("card_present").cast(BooleanType()))
    .withColumn("is_fraud",     F.col("is_fraud").cast(BooleanType()))
    # Keep string columns as StringType (already inferred as string)
    .withColumn("transaction_id",    F.col("transaction_id").cast(StringType()))
    .withColumn("user_id",           F.col("user_id").cast(StringType()))
    .withColumn("merchant",          F.col("merchant").cast(StringType()))
    .withColumn("merchant_category", F.col("merchant_category").cast(StringType()))
    .withColumn("country",           F.col("country").cast(StringType()))
    .withColumn("fraud_reason",      F.col("fraud_reason").cast(StringType()))
)

# ── 3. Drop nulls in critical columns ─────────────────────────────────────────
critical_cols = ["transaction_id", "user_id", "timestamp", "amount", "is_fraud"]
df_clean = df_typed.dropna(subset=critical_cols)

dropped = df_typed.count() - df_clean.count()
print(f"[ETL] Rows dropped (null critical cols): {dropped:,}")

# ── 4. Derived columns (useful for Athena queries & QuickSight) ────────────────
df_enriched = (
    df_clean
    # Date parts for partitioning and BI filtering
    .withColumn("txn_date",   F.to_date("timestamp"))
    .withColumn("txn_year",   F.year("timestamp").cast(StringType()))
    .withColumn("txn_month",  F.month("timestamp").cast(StringType()))
    .withColumn("txn_hour",   F.hour("timestamp"))

    # Amount buckets for BI dashboards
    .withColumn("amount_bucket",
        F.when(F.col("amount") < 50,   F.lit("< $50"))
         .when(F.col("amount") < 200,  F.lit("$50–$200"))
         .when(F.col("amount") < 1000, F.lit("$200–$1K"))
         .when(F.col("amount") < 5000, F.lit("$1K–$5K"))
         .otherwise(                   F.lit("> $5K"))
    )

    # High-value flag (used in Athena fraud queries)
    .withColumn("is_high_value", F.col("amount") > 5000)

    # Home-country mismatch flag (US/UK/CA/AU/DE = home; others = foreign)
    .withColumn("is_foreign_txn",
        ~F.col("country").isin("US", "UK", "CA", "AU", "DE")
    )
)

print(f"[ETL] Enriched schema:")
df_enriched.printSchema()
print(f"[ETL] Final row count: {df_enriched.count():,}")

# ── 5. Write to S3 as Parquet (Snappy compression, partitioned by year+month) ──
(
    df_enriched
    .write
    .mode("overwrite")
    .option("compression", "snappy")
    .partitionBy("txn_year", "txn_month")
    .parquet(PROCESSED_PATH)
)

print(f"[ETL] ✅ Written to {PROCESSED_PATH}")

# ── 6. Update Glue Catalog via crawler (triggered separately) ──────────────────
# After this job runs, re-run the Glue Crawler on PROCESSED_PATH
# so Athena picks up the new Parquet schema + partitions.
print("[ETL] ✅ Job complete. Re-run the Glue Crawler on the processed bucket.")

job.commit()
