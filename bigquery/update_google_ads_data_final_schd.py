#!/usr/bin/env python3
"""
google_ads_fixed_etl.py  (PRODUCTION REWRITE)

Drop-in replacement for your existing production script.

Keeps:
- project_root sys.path auto-fix
- .env loading
- SSL certifi handling
- logging to google_ads_fixed_etl.log
- CLI flags: --start-date, --end-date, --debug
- SAME target table: google_data.google_ads_daily
- SAME PK/upsert key: (date, service, campaign_channel_type, original_name)

Enhances:
- Adds campaign_id + campaign_status into the extract + upsert
- Uses segments_date (true metric date) instead of _DATA_DATE
- Incremental by default:
    - If start-date is NOT provided, uses max(date) in Postgres minus LOOKBACK_DAYS
    - If table empty, starts from FIXED_START_DATE (2025-01-01)

IMPORTANT: Run once in Postgres before deploying:
    ALTER TABLE google_data.google_ads_daily
      ADD COLUMN IF NOT EXISTS campaign_id bigint;

    ALTER TABLE google_data.google_ads_daily
      ADD COLUMN IF NOT EXISTS campaign_status text;
"""
import os
import sys

# ---------------------------------------------------------------------
# AUTO-FIX: Add project root to Python path (keep exactly like prod)
# ---------------------------------------------------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import certifi
import pandas as pd
import logging
from datetime import datetime, timedelta, date
from typing import Optional, Tuple
from dataclasses import dataclass
from google.cloud import bigquery
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

# ---------------------------------------------------------------------
# ENV + SSL
# ---------------------------------------------------------------------
load_dotenv()
os.environ["SSL_CERT_FILE"] = certifi.where()
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()

# ---------------------------------------------------------------------
# LOGGING (keep same file name as prod)
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("google_ads_fixed_etl.log", encoding="utf-8", mode="a"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# CONFIG (matches your production conventions)
# ---------------------------------------------------------------------
SCHEMA_NAME = "google_data"
TABLE_NAME = "google_ads_daily"

FIXED_START_DATE_STR = "2025-01-01"
FIXED_START_DATE = datetime.strptime(FIXED_START_DATE_STR, "%Y-%m-%d").date()

CHUNK_SIZE = int(os.getenv("GOOGLE_ADS_CHUNK_SIZE", "10000"))
LOOKBACK_DAYS = int(os.getenv("GOOGLE_ADS_LOOKBACK_DAYS", "90"))  # refresh window for late adjustments

BIGQUERY_CONFIG = {
    "project": os.getenv("BIGQUERY_PROJECT", "iron-circuit-345510"),
    "credentials_path": os.getenv("BIGQUERY_CREDENTIALS_PATH", "service-account-key.json"),
    "dataset": os.getenv("BIGQUERY_DATASET", "google_ads_mcc"),
}

DB_CONFIG = {
    # keep EXACT same precedence as your production code
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
    "database": os.getenv("PG_DB"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASS"),
}


@dataclass
class ETLStats:
    rows_extracted: int = 0
    rows_transformed: int = 0
    rows_upserted: int = 0
    date_range: Tuple[str, str] = ("", "")
    execution_time: float = 0.0
    error_count: int = 0


class GoogleAdsFixedETL:
    def __init__(self, start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        Production behaviour:
        - If --start-date is provided: use it
        - Else: incremental window:
            start = max(date) in Postgres - LOOKBACK_DAYS
            if empty table: start = 2025-01-01
        - End date default: yesterday (local clock)
        """
        self.bq_client: Optional[bigquery.Client] = None
        self.pg_engine: Optional[Engine] = None

        # end date default is yesterday
        if not end_date:
            end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        self.end_date_str = end_date
        self.end_date = datetime.strptime(self.end_date_str, "%Y-%m-%d").date()

        self.setup_clients()

        if start_date:
            self.start_date_str = start_date
            self.start_date = datetime.strptime(self.start_date_str, "%Y-%m-%d").date()
        else:
            self.start_date = self._compute_incremental_start_date()
            self.start_date_str = self.start_date.strftime("%Y-%m-%d")

        # guard rails
        if self.start_date < FIXED_START_DATE:
            self.start_date = FIXED_START_DATE
            self.start_date_str = FIXED_START_DATE_STR

        if self.end_date < self.start_date:
            logger.warning("End date %s is before start date %s; forcing end_date=start_date", self.end_date, self.start_date)
            self.end_date = self.start_date
            self.end_date_str = self.end_date.strftime("%Y-%m-%d")

        self.stats = ETLStats(date_range=(self.start_date_str, self.end_date_str))

        logger.info("✅ ETL initialized with date range: %s to %s", self.start_date_str, self.end_date_str)
        logger.info("✅ Incremental mode: %s (LOOKBACK_DAYS=%s)", "ON" if not start_date else "OFF (manual start-date)", LOOKBACK_DAYS)
        logger.info("✅ Enhancements: campaign_id + campaign_status; metric date = segments_date")

    def setup_clients(self):
        """Initialize BigQuery and PostgreSQL clients"""
        creds_path = BIGQUERY_CONFIG["credentials_path"]
        if not os.path.exists(creds_path):
            raise FileNotFoundError(f"Credentials file not found: {creds_path}")

        self.bq_client = bigquery.Client.from_service_account_json(
            creds_path,
            project=BIGQUERY_CONFIG["project"],
        )
        logger.info("✅ BigQuery client connected")

        postgres_url = (
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        self.pg_engine = create_engine(
            postgres_url,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            connect_args={"connect_timeout": 30},
        )
        logger.info("✅ PostgreSQL engine created (%s:%s/%s)", DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["database"])

    def _compute_incremental_start_date(self) -> date:
        """
        start = MAX(date) from table - LOOKBACK_DAYS
        if empty -> FIXED_START_DATE
        """
        sql = f"SELECT MAX(date) FROM {SCHEMA_NAME}.{TABLE_NAME};"
        with self.pg_engine.connect() as conn:
            max_dt = conn.execute(text(sql)).scalar()

        if max_dt is None:
            logger.info("No existing rows in %s.%s -> full load from %s", SCHEMA_NAME, TABLE_NAME, FIXED_START_DATE_STR)
            return FIXED_START_DATE

        start_dt = max_dt - timedelta(days=LOOKBACK_DAYS)
        logger.info("Existing MAX(date)=%s -> refreshing from %s (lookback %s days)", max_dt, start_dt, LOOKBACK_DAYS)
        return start_dt

    def extract_from_bigquery(self) -> pd.DataFrame:
        """
        Extract:
        - Uses segments_date as the date
        - Pulls latest campaign metadata for campaign_name + status + channel type
        - Returns campaign-level rows (not device-level)
        """
        start_time = datetime.now()

        project = BIGQUERY_CONFIG["project"]
        dataset = BIGQUERY_CONFIG["dataset"]
        campaign_table = f"{project}.{dataset}.ads_Campaign_2206191174"
        stats_table = f"{project}.{dataset}.ads_CampaignBasicStats_2206191174"

        logger.info("📥 EXTRACT: Fetching Google Ads data from %s to %s...", self.start_date_str, self.end_date_str)

        query = f"""
        WITH latest_campaign_metadata AS (
          SELECT
            campaign_id,
            campaign_advertising_channel_type,
            campaign_name,
            campaign_status,
            TRIM(REGEXP_REPLACE(
              REPLACE(REPLACE(campaign_name, '—', '-'), '–', '-'),
              r'\\s+', ' '
            )) AS clean_name,
            ROW_NUMBER() OVER (
              PARTITION BY campaign_id
              ORDER BY _DATA_DATE DESC
            ) AS rn
          FROM `{campaign_table}`
          WHERE campaign_name IS NOT NULL
        ),
        campaign_metadata AS (
          SELECT
            campaign_id,
            campaign_advertising_channel_type,
            campaign_status,
            clean_name AS original_campaign_name
          FROM latest_campaign_metadata
          WHERE rn = 1
        ),
        daily_campaign_stats AS (
          SELECT
            campaign_id,
            segments_date AS date,
            SUM(metrics_impressions) AS impressions,
            SUM(metrics_clicks) AS clicks,
            SAFE_DIVIDE(SUM(metrics_cost_micros), 1e6) AS cost,
            SUM(metrics_conversions) AS conversions,
            SUM(metrics_conversions_value) AS revenue
          FROM `{stats_table}`
          WHERE segments_date >= DATE('{self.start_date_str}')
            AND segments_date <= DATE('{self.end_date_str}')
          GROUP BY campaign_id, segments_date
        )
        SELECT
          dcs.date,
          dcs.campaign_id,
          COALESCE(cm.original_campaign_name, 'Unknown Campaign') AS original_name,
          COALESCE(cm.campaign_advertising_channel_type, 'UNSPECIFIED') AS campaign_channel_type,
          cm.campaign_status AS campaign_status,

          CASE
            WHEN STRPOS(COALESCE(cm.original_campaign_name, ''), ' - ') > 0
              THEN TRIM(SPLIT(COALESCE(cm.original_campaign_name, ''), ' - ')[OFFSET(0)])
            ELSE COALESCE(cm.original_campaign_name, 'Unknown')
          END AS service,

          CASE
            WHEN ARRAY_LENGTH(SPLIT(COALESCE(cm.original_campaign_name, ''), ' - ')) > 1
              THEN TRIM(SPLIT(COALESCE(cm.original_campaign_name, ''), ' - ')[OFFSET(1)])
            ELSE NULL
          END AS city,

          CASE
            WHEN ARRAY_LENGTH(SPLIT(COALESCE(cm.original_campaign_name, ''), ' - ')) > 2
              THEN TRIM(SPLIT(COALESCE(cm.original_campaign_name, ''), ' - ')[OFFSET(2)])
            ELSE NULL
          END AS campaign_type,

          dcs.impressions,
          dcs.clicks,
          dcs.cost,
          dcs.conversions,
          dcs.revenue,

          1 AS campaign_count
        FROM daily_campaign_stats dcs
        LEFT JOIN campaign_metadata cm
          ON dcs.campaign_id = cm.campaign_id
        WHERE dcs.impressions > 0
        ORDER BY dcs.date DESC, dcs.cost DESC
        """

        df = self.bq_client.query(query).to_dataframe()

        self.stats.rows_extracted = len(df)
        elapsed = (datetime.now() - start_time).total_seconds()

        logger.info("✅ EXTRACT completed in %.1fs | rows=%s", elapsed, f"{len(df):,}")

        if not df.empty:
            unknown = df[df["original_name"] == "Unknown Campaign"].shape[0]
            logger.info("   Date range in extract: %s -> %s", df["date"].min(), df["date"].max())
            logger.info("   Unknown Campaign rows: %s", unknown)
            logger.info("   Sample campaigns:")
            for _, r in df.head(5).iterrows():
                logger.info("     - %s | campaign_id=%s | status=%s", r["original_name"], r["campaign_id"], r.get("campaign_status"))

        return df

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean + enforce types consistent with existing table schema."""
        if df.empty:
            return df

        start_time = datetime.now()
        logger.info("🔄 TRANSFORM: Processing data...")

        out = df.copy()

        # date as datetime (for formatting in load)
        out["date"] = pd.to_datetime(out["date"])

        # campaign_id numeric
        out["campaign_id"] = pd.to_numeric(out["campaign_id"], errors="coerce")

        # clean text fields
        text_columns = [
            "service",
            "city",
            "campaign_type",
            "campaign_channel_type",
            "original_name",
            "campaign_status",
        ]
        for col in text_columns:
            if col in out.columns:
                out[col] = out[col].astype(str).str.strip()
                out[col] = out[col].replace({"nan": None, "None": None, "": None})

        # required not-null fields in table
        out["service"] = out["service"].fillna("Unknown")
        out["campaign_channel_type"] = out["campaign_channel_type"].fillna("UNSPECIFIED")
        out["original_name"] = out["original_name"].fillna("Unknown Campaign")

        # numerics (keep behaviour consistent)
        out["impressions"] = pd.to_numeric(out["impressions"], errors="coerce").fillna(0).astype("int64")
        out["clicks"] = pd.to_numeric(out["clicks"], errors="coerce").fillna(0).astype("int64")
        out["cost"] = pd.to_numeric(out["cost"], errors="coerce").fillna(0).round(2)

        # BQ conversions is FLOAT64 - keep your table as bigint:
        out["conversions"] = pd.to_numeric(out["conversions"], errors="coerce").fillna(0).round(0).astype("int64")
        out["revenue"] = pd.to_numeric(out["revenue"], errors="coerce").fillna(0).round(2)
        out["campaign_count"] = pd.to_numeric(out["campaign_count"], errors="coerce").fillna(0).astype("int64")

        # de-dupe on your PK (same as existing script)
        key_columns = ["date", "service", "campaign_channel_type", "original_name"]
        before = len(out)
        out = out.drop_duplicates(subset=key_columns, keep="first")
        removed = before - len(out)
        if removed > 0:
            logger.warning("   Removed %s duplicate rows (PK de-dupe)", removed)

        self.stats.rows_transformed = len(out)
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info("✅ TRANSFORM completed in %.1fs | rows=%s", elapsed, f"{len(out):,}")
        return out

    def load_with_upsert(self, df: pd.DataFrame) -> int:
        """UPSERT into google_data.google_ads_daily, adding campaign_id + campaign_status."""
        if df.empty:
            logger.warning("No data to load.")
            return 0

        start_time = datetime.now()
        logger.info("💾 LOAD: UPSERTing %s rows to %s.%s...", f"{len(df):,}", SCHEMA_NAME, TABLE_NAME)

        with self.pg_engine.begin() as connection:
            temp_table = f"temp_google_ads_{int(datetime.now().timestamp())}"

            connection.execute(
                text(
                    f"""
                    CREATE TEMP TABLE {temp_table} (
                        date DATE,
                        service TEXT,
                        city TEXT,
                        campaign_type TEXT,
                        campaign_channel_type TEXT,
                        original_name TEXT,
                        impressions BIGINT,
                        clicks BIGINT,
                        cost NUMERIC(15, 2),
                        conversions BIGINT,
                        revenue NUMERIC(15, 2),
                        campaign_count INTEGER,
                        campaign_id BIGINT,
                        campaign_status TEXT
                    ) ON COMMIT DROP
                    """
                )
            )

            columns = [
                "date",
                "service",
                "city",
                "campaign_type",
                "campaign_channel_type",
                "original_name",
                "impressions",
                "clicks",
                "cost",
                "conversions",
                "revenue",
                "campaign_count",
                "campaign_id",
                "campaign_status",
            ]

            temp_df = df[columns].copy()
            temp_df["date"] = temp_df["date"].dt.strftime("%Y-%m-%d")
            temp_df = temp_df.where(pd.notna(temp_df), None)

            temp_df.to_sql(
                temp_table,
                connection,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=CHUNK_SIZE,
            )

            upsert_sql = text(
                f"""
                INSERT INTO {SCHEMA_NAME}.{TABLE_NAME}
                (date, service, city, campaign_type, campaign_channel_type, original_name,
                 impressions, clicks, cost, conversions, revenue, campaign_count,
                 campaign_id, campaign_status)
                SELECT
                    date::DATE,
                    service,
                    city,
                    campaign_type,
                    campaign_channel_type,
                    original_name,
                    impressions,
                    clicks,
                    cost,
                    conversions,
                    revenue,
                    campaign_count,
                    campaign_id,
                    campaign_status
                FROM {temp_table}
                ON CONFLICT (date, service, campaign_channel_type, original_name)
                DO UPDATE SET
                    city = EXCLUDED.city,
                    campaign_type = EXCLUDED.campaign_type,
                    impressions = EXCLUDED.impressions,
                    clicks = EXCLUDED.clicks,
                    cost = EXCLUDED.cost,
                    conversions = EXCLUDED.conversions,
                    revenue = EXCLUDED.revenue,
                    campaign_count = EXCLUDED.campaign_count,
                    campaign_id = EXCLUDED.campaign_id,
                    campaign_status = EXCLUDED.campaign_status,
                    last_updated = CURRENT_TIMESTAMP
                """
            )

            result = connection.execute(upsert_sql)
            rows_upserted = result.rowcount if result.rowcount is not None else 0

        self.stats.rows_upserted = rows_upserted
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info("✅ LOAD completed in %.1fs | rowcount=%s", elapsed, rows_upserted)
        return rows_upserted

    def run_etl(self) -> bool:
        start_time = datetime.now()

        logger.info("=" * 80)
        logger.info("✅ GOOGLE ADS FIXED ETL (PROD) - campaign_id + campaign_status")
        logger.info("=" * 80)
        logger.info("📅 Date range: %s to %s", self.start_date_str, self.end_date_str)
        logger.info("🎯 Target: %s.%s", SCHEMA_NAME, TABLE_NAME)
        logger.info("=" * 80)

        try:
            df_raw = self.extract_from_bigquery()
            if df_raw.empty:
                logger.info("No data to process.")
                return True

            df_clean = self.transform_data(df_raw)
            if df_clean.empty:
                logger.info("No valid data after transformation.")
                return True

            self.load_with_upsert(df_clean)

            self.stats.execution_time = (datetime.now() - start_time).total_seconds()

            logger.info("=" * 80)
            logger.info("✅ ETL COMPLETE")
            logger.info("=" * 80)
            logger.info("⏱️  Total execution time: %.1fs", self.stats.execution_time)
            logger.info("📥 Rows extracted: %s", f"{self.stats.rows_extracted:,}")
            logger.info("🔄 Rows transformed: %s", f"{self.stats.rows_transformed:,}")
            logger.info("💾 Rows upserted: %s", f"{self.stats.rows_upserted:,}")
            logger.info("=" * 80)

            return True

        except Exception as e:
            logger.error("❌ ETL failed: %s", e, exc_info=True)
            return False


def main():
    try:
        import argparse

        parser = argparse.ArgumentParser(description="Google Ads Fixed ETL (Prod) - campaign_id + status")
        parser.add_argument(
            "--start-date",
            type=str,
            default=None,
            help=f"Start date (YYYY-MM-DD). If omitted, uses incremental lookback from MAX(date) - {LOOKBACK_DAYS} days.",
        )
        parser.add_argument(
            "--end-date",
            type=str,
            default=None,
            help="End date (YYYY-MM-DD). Default: yesterday.",
        )
        parser.add_argument("--debug", action="store_true", help="Enable debug logging")

        args = parser.parse_args()
        if args.debug:
            logging.getLogger().setLevel(logging.DEBUG)

        etl = GoogleAdsFixedETL(start_date=args.start_date, end_date=args.end_date)
        success = etl.run_etl()
        return 0 if success else 1

    except KeyboardInterrupt:
        logger.info("ETL interrupted by user")
        return 130
    except Exception as e:
        logger.error("Script failed: %s", e, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
