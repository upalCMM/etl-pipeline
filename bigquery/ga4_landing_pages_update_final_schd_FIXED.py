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
import certifi
import pandas as pd
import logging
from datetime import datetime, timedelta
from google.cloud import bigquery
from sqlalchemy import create_engine, text
from config import DB_CONFIG, BIGQUERY_CONFIG

# -------------------------------------------------------------------
# SSL FIX
# -------------------------------------------------------------------
os.environ["SSL_CERT_FILE"] = certifi.where()
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()

# -------------------------------------------------------------------
# LOGGING
# -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("ga4_landing_pages_update.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

SCHEMA_NAME = "google_data"
TABLE_NAME = "ga4_landing_pages"
MIN_DATE = datetime(2025, 1, 1).date()


# ===================================================================
# MAIN CLASS
# ===================================================================
class GA4LandingPagesUpdate:

    def __init__(self):
        self.bq_client = None
        self.pg_engine = None
        self._setup_clients()

    # ------------------------------------------------------------------
    # SETUP CLIENTS
    # ------------------------------------------------------------------
    def _setup_clients(self):
        try:
            logger.info("Connecting to BigQuery...")
            self.bq_client = bigquery.Client.from_service_account_json(
                BIGQUERY_CONFIG["credentials_path"],
                project=BIGQUERY_CONFIG["project"],
            )
            logger.info("BigQuery connected.")

            logger.info("Connecting to Postgres...")
            pg_url = (
                f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
                f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            )
            self.pg_engine = create_engine(pg_url)
            logger.info("Postgres connected.")
        except Exception as e:
            logger.error(f"Failed to initialize clients: {e}")
            raise

    # ------------------------------------------------------------------
    # GET LATEST DATE IN POSTGRES (FOR INCREMENTAL LOAD)
    # ------------------------------------------------------------------
    def get_latest_date(self):
        try:
            logger.info("Fetching latest date from Postgres...")

            with self.pg_engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT MAX(date) FROM {SCHEMA_NAME}.{TABLE_NAME}")
                )
                latest = result.scalar()

            if latest:
                logger.info(f"Latest date found: {latest}")
            else:
                logger.info("No existing rows → full load needed.")

            return latest

        except Exception as e:
            logger.error(f"Failed to get latest date: {e}")
            raise

    # ------------------------------------------------------------------
    # RUN BIGQUERY QUERY: SESSIONS + ENGAGED + BOUNCED + ENGAGEMENT TIME
    # ------------------------------------------------------------------
    def run_bigquery_query(self, start_date):
        start_suffix = start_date.strftime("%Y%m%d")
        today = datetime.today().date()
        end_suffix = today.strftime("%Y%m%d")

        logger.info(
            f"Running GA4 Query: TABLE_SUFFIX {start_suffix} → {end_suffix}"
        )

        query = f"""
        WITH sessions AS (
          SELECT
            PARSE_DATE('%Y%m%d', event_date) AS date,
            user_pseudo_id,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id')
              AS session_id,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key='page_location')
              AS landing_page,
            device.category AS device_type,
            geo.country AS country
          FROM `iron-circuit-345510.analytics_266654161.events_*`
          WHERE event_name = 'session_start'
            AND _TABLE_SUFFIX BETWEEN '{start_suffix}' AND '{end_suffix}'
        ),

        engagement AS (
          SELECT
            user_pseudo_id,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id')
              AS session_id,
            SUM((SELECT value.int_value FROM UNNEST(event_params) WHERE key='engagement_time_msec'))
              AS engagement_msec
          FROM `iron-circuit-345510.analytics_266654161.events_*`
          WHERE event_name = 'user_engagement'
          GROUP BY user_pseudo_id, session_id
        ),

        pageviews AS (
          SELECT
            user_pseudo_id,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id')
              AS session_id,
            COUNT(*) AS pageviews
          FROM `iron-circuit-345510.analytics_266654161.events_*`
          WHERE event_name = 'page_view'
          GROUP BY user_pseudo_id, session_id
        ),

        scrolls AS (
          SELECT
            user_pseudo_id,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id')
              AS session_id,
            MAX((SELECT value.int_value FROM UNNEST(event_params) WHERE key='percent_scrolled'))
              AS max_scroll
          FROM `iron-circuit-345510.analytics_266654161.events_*`
          WHERE event_name = 'scroll'
          GROUP BY user_pseudo_id, session_id
        ),

        joined AS (
          SELECT
            s.date,
            s.landing_page,
            s.device_type,
            s.country,

            COALESCE(e.engagement_msec, 0) AS engagement_msec,
            COALESCE(p.pageviews, 1) AS pageviews,
            COALESCE(sc.max_scroll, 0) AS max_scroll

          FROM sessions s
          LEFT JOIN engagement e USING (user_pseudo_id, session_id)
          LEFT JOIN pageviews p USING (user_pseudo_id, session_id)
          LEFT JOIN scrolls sc USING (user_pseudo_id, session_id)
        )

        SELECT
          date,
          landing_page,
          device_type,
          country,

          COUNT(*) AS sessions,

          SUM(
            CASE
              WHEN engagement_msec >= 10000 THEN 1
              WHEN pageviews > 1 THEN 1
              WHEN max_scroll >= 90 THEN 1
              ELSE 0
            END
          ) AS engaged_sessions,

          COUNT(*) -
          SUM(
            CASE
              WHEN engagement_msec >= 10000 THEN 1
              WHEN pageviews > 1 THEN 1
              WHEN max_scroll >= 90 THEN 1
              ELSE 0
            END
          ) AS bounced_sessions,

          AVG(engagement_msec) / 1000 AS avg_engagement_seconds

        FROM joined
        WHERE landing_page IS NOT NULL
        GROUP BY date, landing_page, device_type, country
        ORDER BY date DESC, sessions DESC;
        """

        try:
            df = self.bq_client.query(query).to_dataframe()
            logger.info(f"BigQuery returned {len(df)} rows.")
            if not df.empty:
                logger.info("Sample rows:\n%s", df.head().to_string())
            return df

        except Exception as e:
            logger.error(f"BigQuery query failed: {e}")
            raise

    # ------------------------------------------------------------------
    # LOAD INTO POSTGRES
    # ------------------------------------------------------------------
    def load_into_postgres(self, df):
        if df.empty:
            logger.info("No new rows to insert.")
            return 0

        df["date"] = pd.to_datetime(df["date"]).dt.date
        min_d, max_d = df["date"].min(), df["date"].max()

        logger.info(f"Upserting rows for date range {min_d} → {max_d}")

        try:
            with self.pg_engine.begin() as conn:
                # Remove overlapping dates
                conn.execute(
                    text(
                        f"""
                        DELETE FROM {SCHEMA_NAME}.{TABLE_NAME}
                        WHERE date BETWEEN :start AND :end
                        """
                    ),
                    {"start": min_d, "end": max_d},
                )
                logger.info("Deleted overlapping rows.")

                # Insert fresh rows
                df.to_sql(
                    TABLE_NAME,
                    conn,
                    schema=SCHEMA_NAME,
                    if_exists="append",
                    index=False,
                )
                logger.info(f"Inserted {len(df)} rows.")

            return len(df)

        except Exception as e:
            logger.error(f"Insert into Postgres failed: {e}")
            raise

    # ------------------------------------------------------------------
    # MAIN RUN
    # ------------------------------------------------------------------
    def run(self):
        logger.info("=" * 80)
        logger.info("🚀 Starting GA4 Landing Page ETL")

        latest = self.get_latest_date()

        if latest is None:
            start_date = MIN_DATE
            logger.info(f"Full load starting {start_date}")
        else:
            start_date = latest + timedelta(days=1)
            start_date = max(start_date, MIN_DATE)
            logger.info(f"Incremental load from {start_date}")

        if start_date > datetime.today().date():
            logger.info("No new data needed.")
            return

        df = self.run_bigquery_query(start_date)
        inserted = self.load_into_postgres(df)

        logger.info(f"🎉 Completed — Inserted {inserted} rows.")
        logger.info("=" * 80)


# ===================================================================
# ENTRY POINT
# ===================================================================
def main():
    try:
        GA4LandingPagesUpdate().run()
        return 0
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
