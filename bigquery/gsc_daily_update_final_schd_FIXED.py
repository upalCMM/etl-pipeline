#!/usr/bin/env python3
"""
ETL Script - Auto-fixed version
"""
import os
import sys

# AUTO-FIX: Add project root to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)
import os
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
    url = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(url)


def bq_client():
    return bigquery.Client.from_service_account_json(
        BIGQUERY_CONFIG["credentials_path"],
        project=BIGQUERY_CONFIG["project"]
    )


# ---------------------------------------------------------------------
# GET LATEST DATE LOADED IN POSTGRES
# ---------------------------------------------------------------------
def get_latest_date(engine):
    logger.info("Checking latest GSC date already loaded in Postgres...")

    sql = "SELECT MAX(date) FROM google_data.gsc_daily;"
    with engine.connect() as conn:
        result = conn.execute(text(sql)).scalar()

    if result:
        logger.info(f"Latest GSC date found: {result}")
    else:
        logger.info("No existing GSC data — full load will begin from 2025-01-01")

    return result


# ---------------------------------------------------------------------
# RUN BIGQUERY EXTRACT
# ---------------------------------------------------------------------
def load_from_bigquery(start_date):
    client = bq_client()

    logger.info(f"Running BigQuery GSC extract from {start_date} ...")

    query = f"""
    -- Normalize before grouping
    WITH cleaned AS (
        SELECT
            data_date AS date,
            url,
            IFNULL(LOWER(device), 'unknown') AS device,
            IFNULL(LOWER(country), 'unknown') AS country,
            clicks,
            impressions,
            sum_position
        FROM `iron-circuit-345510.searchconsole.searchdata_url_impression`
        WHERE data_date >= DATE('{start_date}')
          AND url IS NOT NULL
    )

    SELECT
        date,
        url,
        device,
        country,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        SAFE_DIVIDE(SUM(sum_position), SUM(impressions)) AS avg_position,
        SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS ctr
    FROM cleaned
    GROUP BY date, url, device, country
    ORDER BY date;
    """

    df = client.query(query).to_dataframe()
    logger.info(f"GSC extract complete: {len(df):,} rows loaded from BigQuery.")

    # SAFETY: Ensure device/country always populated
    df["device"] = df["device"].fillna("unknown")
    df["country"] = df["country"].fillna("unknown")

    # Convert date properly
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # Preview
    if not df.empty:
        logger.info("Sample extracted rows:")
        logger.info(df.head(10).to_string())

    return df


# ---------------------------------------------------------------------
# INSERT INTO POSTGRES (UPSERT BY DATE RANGE)
# ---------------------------------------------------------------------
def upsert_postgres(engine, df):
    if df.empty:
        logger.info("No GSC rows to insert — skipping.")
        return 0

    min_d = df["date"].min()
    max_d = df["date"].max()

    logger.info(f"Upserting GSC rows for {min_d} -> {max_d}")

    with engine.begin() as conn:
        # Delete overlapping range
        conn.execute(
            text("""
                DELETE FROM google_data.gsc_daily
                WHERE date BETWEEN :start AND :end;
            """),
            {"start": min_d, "end": max_d}
        )

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
# MAIN PROCESS
# ---------------------------------------------------------------------
def main():
    logger.info("===============================================================")
    logger.info("🚀 Starting GSC Daily Incremental ETL")
    logger.info("===============================================================")

    engine = pg_engine()
    latest_date = get_latest_date(engine)

    start_date = latest_date + timedelta(days=1) if latest_date else datetime(2025, 1, 1).date()

    df = load_from_bigquery(start_date)
    upsert_postgres(engine, df)

    logger.info("===============================================================")
    logger.info("🎉 GSC Daily ETL Completed Successfully")
    logger.info("===============================================================")


if __name__ == "__main__":
    main()
