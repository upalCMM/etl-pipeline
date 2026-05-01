#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL Script - GSC Query-Level Data (Raw Metrics Only)
Extracts Google Search Console data from BigQuery and loads to PostgreSQL
Supports both backfill and incremental updates
"""

import os
import sys

# AUTO-FIX: Add project root to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import logging
import pandas as pd
import certifi
from datetime import datetime, timedelta
from google.cloud import bigquery
from sqlalchemy import create_engine, text
from config import DB_CONFIG, BIGQUERY_CONFIG

# ---------------------------------------------------------------------
# SSL FIX
# ---------------------------------------------------------------------
os.environ["SSL_CERT_FILE"] = certifi.where()
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()

# ---------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("gsc_daily_update.log", encoding="utf-8"),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# CONNECTION SETUP
# ---------------------------------------------------------------------
def pg_engine():
    """Create PostgreSQL connection engine"""
    url = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(url)


def bq_client():
    """Create BigQuery client"""
    return bigquery.Client.from_service_account_json(
        BIGQUERY_CONFIG["credentials_path"],
        project=BIGQUERY_CONFIG["project_id"]
    )


# ---------------------------------------------------------------------
# GET LATEST DATE LOADED IN POSTGRES
# ---------------------------------------------------------------------
def get_latest_date(engine):
    """Get the most recent date already loaded in PostgreSQL"""
    logger.info("Checking latest GSC date already loaded in Postgres...")

    sql = "SELECT MAX(date) FROM google_data.gsc_daily;"
    with engine.connect() as conn:
        result = conn.execute(text(sql)).scalar()

    if result:
        logger.info(f"Latest GSC date found: {result}")
    else:
        logger.info("No existing GSC data - full load will begin from 2025-01-01")

    return result


# ---------------------------------------------------------------------
# RUN BIGQUERY EXTRACT (QUERY-LEVEL DATA, RAW METRICS ONLY)
# ---------------------------------------------------------------------
def load_from_bigquery(start_date, end_date=None):
    """Extract query-level GSC data from BigQuery for date range"""
    client = bq_client()
    
    if end_date is None:
        end_date = datetime.now().date() - timedelta(days=1)  # Yesterday
    
    logger.info(f"Running BigQuery GSC extract from {start_date} to {end_date} ...")

    query = f"""
    SELECT
        data_date AS date,
        query,
        url,
        IFNULL(LOWER(device), 'unknown') AS device,
        IFNULL(LOWER(country), 'unknown') AS country,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        SUM(sum_position) AS sum_position
    FROM `iron-circuit-345510.searchconsole.searchdata_url_impression`
    WHERE data_date >= DATE('{start_date}')
      AND data_date <= DATE('{end_date}')
      AND url IS NOT NULL
    GROUP BY date, query, url, device, country
    ORDER BY date;
    """

    logger.info("Executing BigQuery query...")
    df = client.query(query).to_dataframe()
    logger.info(f"GSC extract complete: {len(df):,} rows loaded from BigQuery.")

    if df.empty:
        logger.info("No data returned from BigQuery for the date range")
        return df

    # Handle NULL queries (Google anonymizes low-volume keywords)
    df["query"] = df["query"].fillna("(not provided)")
    
    # SAFETY: Ensure device/country always populated
    df["device"] = df["device"].fillna("unknown")
    df["country"] = df["country"].fillna("unknown")

    # Convert date properly
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # Add last_updated timestamp
    df["last_updated"] = datetime.now()

    # Add page_url_canonical (copy from url for now, can be enhanced later)
    df["page_url_canonical"] = df["url"]

    # Preview
    logger.info("Sample extracted rows:")
    logger.info(df.head(10).to_string())

    return df


# ---------------------------------------------------------------------
# INSERT INTO POSTGRES (UPSERT BY DATE RANGE)
# ---------------------------------------------------------------------
def upsert_postgres(engine, df):
    """Upsert GSC data into PostgreSQL - delete date range then insert"""
    if df.empty:
        logger.info("No GSC rows to insert - skipping.")
        return 0

    min_d = df["date"].min()
    max_d = df["date"].max()

    logger.info(f"Upserting GSC rows for {min_d} -> {max_d}")

    with engine.begin() as conn:
        # Delete overlapping date range
        delete_result = conn.execute(
            text("""
                DELETE FROM google_data.gsc_daily
                WHERE date BETWEEN :start AND :end;
            """),
            {"start": min_d, "end": max_d}
        )
        logger.info(f"Deleted {delete_result.rowcount} existing rows for date range")

        # Insert new records
        df.to_sql(
            "gsc_daily",
            conn,
            schema="google_data",
            if_exists="append",
            index=False,
        )

    logger.info(f"Inserted {len(df):,} GSC rows into Postgres.")
    return len(df)


# ---------------------------------------------------------------------
# BACKFILL HISTORICAL DATA
# ---------------------------------------------------------------------
def backfill_historical(engine):
    """Backfill all data from 2025-01-01 to yesterday"""
    logger.info("=" * 63)
    logger.info("BACKFILL MODE: Loading all data from 2025-01-01")
    logger.info("=" * 63)
    
    start_date = datetime(2025, 1, 1).date()
    end_date = datetime.now().date() - timedelta(days=1)
    
    logger.info(f"Backfilling from {start_date} to {end_date}")
    
    # Load data in monthly chunks to avoid memory issues
    current_start = start_date
    total_rows = 0
    
    while current_start <= end_date:
        # Calculate end of current month
        if current_start.month == 12:
            current_end = datetime(current_start.year, 12, 31).date()
        else:
            current_end = datetime(current_start.year, current_start.month + 1, 1).date() - timedelta(days=1)
        
        # Don't go beyond end_date
        if current_end > end_date:
            current_end = end_date
        
        logger.info(f"Processing chunk: {current_start} to {current_end}")
        
        df = load_from_bigquery(current_start, current_end)
        
        if not df.empty:
            rows = upsert_postgres(engine, df)
            total_rows += rows
        
        # Move to next month
        if current_start.month == 12:
            current_start = datetime(current_start.year + 1, 1, 1).date()
        else:
            current_start = datetime(current_start.year, current_start.month + 1, 1).date()
    
    logger.info(f"Backfill complete! Total rows loaded: {total_rows:,}")
    return total_rows


# ---------------------------------------------------------------------
# INCREMENTAL UPDATE
# ---------------------------------------------------------------------
def incremental_update(engine, latest_date):
    """Run incremental update from latest_date to yesterday"""
    logger.info("=" * 63)
    logger.info("INCREMENTAL MODE: Loading new data")
    logger.info("=" * 63)
    
    start_date = latest_date + timedelta(days=1)
    end_date = datetime.now().date() - timedelta(days=1)
    
    if start_date > end_date:
        logger.info(f"No new data to fetch (start: {start_date}, end: {end_date})")
        return 0
    
    logger.info(f"Incremental update from {start_date} to {end_date}")
    
    df = load_from_bigquery(start_date, end_date)
    
    if not df.empty:
        return upsert_postgres(engine, df)
    else:
        logger.info("No new data found")
        return 0


# ---------------------------------------------------------------------
# VERIFY TABLE SCHEMA
# ---------------------------------------------------------------------
def verify_schema(engine):
    """Verify the table has the correct schema"""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_schema = 'google_data' 
              AND table_name = 'gsc_daily'
            ORDER BY ordinal_position;
        """))
        
        columns = [row for row in result]
        
        required_columns = ['date', 'query', 'url', 'device', 'country', 
                           'clicks', 'impressions', 'sum_position']
        
        existing_columns = [col[0] for col in columns]
        
        for req_col in required_columns:
            if req_col not in existing_columns:
                logger.error(f"Missing required column: {req_col}")
                logger.error("Please run schema migration first")
                return False
        
        # Check if old calculated columns exist (should be gone)
        old_columns = ['avg_position', 'ctr']
        for old_col in old_columns:
            if old_col in existing_columns:
                logger.warning(f"Old column {old_col} still exists - consider dropping it")
        
        logger.info("Schema verification passed")
        return True


# ---------------------------------------------------------------------
# MAIN PROCESS
# ---------------------------------------------------------------------
def main():
    logger.info("=" * 63)
    logger.info("GSC ETL - Query Level Data (Raw Metrics Only)")
    logger.info("=" * 63)

    try:
        engine = pg_engine()
        
        # Verify schema before proceeding
        if not verify_schema(engine):
            logger.error("Schema verification failed - exiting")
            sys.exit(1)
        
        # Check if we need to backfill or update incrementally
        latest_date = get_latest_date(engine)
        
        if not latest_date:
            # No data - run full backfill
            logger.info("No data found - starting full backfill")
            backfill_historical(engine)
        else:
            # Data exists - run incremental update
            incremental_update(engine, latest_date)
        
        # Final summary
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_rows,
                    MIN(date) as oldest_date,
                    MAX(date) as newest_date,
                    COUNT(DISTINCT date) as days_loaded
                FROM google_data.gsc_daily;
            """))
            row = result.fetchone()
            
            logger.info("=" * 63)
            logger.info("FINAL STATUS")
            logger.info("=" * 63)
            logger.info(f"Total rows in table: {row[0]:,}")
            logger.info(f"Date range: {row[1]} to {row[2]}")
            logger.info(f"Number of days: {row[3]}")
            logger.info("=" * 63)
            logger.info("GSC ETL Completed Successfully")
            logger.info("=" * 63)

    except Exception as e:
        logger.error(f"ETL failed with error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()