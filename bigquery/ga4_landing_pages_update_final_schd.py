#!/usr/bin/env python3
"""
GA4 Landing Pages ETL - 7 Day Compare + Upsert

What it does:
- Queries BigQuery for the last 7 days
- Pulls matching rows from Postgres for the same 7 days
- Compares rows by business key
- Upserts only rows that are missing or changed

This keeps BigQuery read cost low and avoids unnecessary reloads.
"""
#!/usr/bin/env python3
"""
GA4 Landing Pages ETL - 7 Day Compare + Upsert
"""

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

print("RUNNING FILE:", __file__)
print("CURRENT_DIR:", current_dir)
print("PROJECT_ROOT:", project_root)
print("SYS.PATH[0:3]:", sys.path[:3])
print("CONFIG EXISTS:", os.path.exists(os.path.join(project_root, "config.py")))

import certifi
import pandas as pd
import logging
from datetime import datetime, timedelta, date
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
        logging.FileHandler("ga4_landing_pages_compare_upsert.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

SCHEMA_NAME = "google_data"
TABLE_NAME = "ga4_landing_pages"

KEY_COLS = [
    "date",
    "landing_page",
    "device_type",
    "country",
    "session_source",
    "session_medium",
    "session_campaign",
]

COMPARE_COLS = [
    "sessions",
    "engaged_sessions",
    "bounced_sessions",
    "avg_engagement_seconds",
    "avg_pageviews",
    "avg_max_scroll",
    "total_conversions",
    "high_engagement_sessions",
    "multi_page_sessions",
    "traffic_type",
    "page_category",
    "page_type",
    "device_category",
    "landing_page_clean",
]


class GA4LandingPagesCompareUpsertETL:
    def __init__(self):
        self.bq_client = None
        self.pg_engine = None
        self._setup_clients()

    # ------------------------------------------------------------------
    # SETUP
    # ------------------------------------------------------------------
    def _setup_clients(self):
        logger.info("Connecting to BigQuery...")
        self.bq_client = bigquery.Client.from_service_account_json(
            BIGQUERY_CONFIG["credentials_path"],
            project=BIGQUERY_CONFIG.get("project_id") or BIGQUERY_CONFIG.get("project"),
        )
        logger.info("BigQuery connected.")

        logger.info("Connecting to Postgres...")
        pg_url = (
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        self.pg_engine = create_engine(pg_url)
        logger.info("Postgres connected.")

    # ------------------------------------------------------------------
    # TABLE SETUP
    # ------------------------------------------------------------------
    def ensure_table_exists(self):
        with self.pg_engine.begin() as conn:
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
                    date DATE NOT NULL,
                    landing_page TEXT NOT NULL,
                    device_type TEXT NOT NULL,
                    country TEXT NOT NULL,
                    session_source TEXT NOT NULL,
                    session_medium TEXT NOT NULL,
                    session_campaign TEXT NOT NULL,
                    sessions INTEGER NOT NULL,
                    engaged_sessions INTEGER NOT NULL,
                    bounced_sessions INTEGER NOT NULL,
                    avg_engagement_seconds NUMERIC,
                    avg_pageviews NUMERIC,
                    avg_max_scroll NUMERIC,
                    total_conversions INTEGER,
                    high_engagement_sessions INTEGER,
                    multi_page_sessions INTEGER,
                    traffic_type TEXT,
                    page_category TEXT,
                    page_type TEXT,
                    device_category TEXT,
                    landing_page_clean TEXT,
                    last_updated TIMESTAMP NOT NULL DEFAULT NOW(),
                    CONSTRAINT ga4_landing_pages_pk PRIMARY KEY (
                        date,
                        landing_page,
                        device_type,
                        country,
                        session_source,
                        session_medium,
                        session_campaign
                    )
                )
            """))

    # ------------------------------------------------------------------
    # BIGQUERY QUERY
    # ------------------------------------------------------------------
    def run_bigquery_query(self, start_date: date, end_date: date) -> pd.DataFrame:
        start_suffix = start_date.strftime("%Y%m%d")
        end_suffix = end_date.strftime("%Y%m%d")

        logger.info(f"Running BigQuery for {start_date} → {end_date}")

        query = f"""
        WITH base_events AS (
          SELECT
            PARSE_DATE('%Y%m%d', event_date) AS date,
            event_name,
            user_pseudo_id,

            (SELECT ep.value.int_value
             FROM UNNEST(event_params) ep
             WHERE ep.key = 'ga_session_id') AS session_id,

            (SELECT ep.value.string_value
             FROM UNNEST(event_params) ep
             WHERE ep.key = 'page_location') AS page_location,

            device.category AS device_type,
            geo.country AS country,

            COALESCE(traffic_source.source, '(none)') AS user_source,
            COALESCE(traffic_source.medium, '(none)') AS user_medium,
            COALESCE(traffic_source.name, '(not set)') AS user_campaign,

            (SELECT ep.value.int_value
             FROM UNNEST(event_params) ep
             WHERE ep.key = 'engagement_time_msec') AS engagement_time_msec,

            (SELECT ep.value.int_value
             FROM UNNEST(event_params) ep
             WHERE ep.key = 'percent_scrolled') AS percent_scrolled

          FROM `iron-circuit-345510.analytics_266654161.events_*`
          WHERE _TABLE_SUFFIX BETWEEN '{start_suffix}' AND '{end_suffix}'
            AND event_name IN ('session_start', 'page_view', 'user_engagement', 'scroll')
        ),

        session_starts AS (
          SELECT
            date,
            user_pseudo_id,
            session_id,
            page_location AS landing_page,
            device_type,
            country,
            user_source AS session_source,
            user_medium AS session_medium,
            user_campaign AS session_campaign
          FROM base_events
          WHERE event_name = 'session_start'
            AND session_id IS NOT NULL
            AND page_location IS NOT NULL
        ),

        session_metrics AS (
          SELECT
            user_pseudo_id,
            session_id,
            COUNTIF(event_name = 'page_view') AS pageviews,
            SUM(
              CASE
                WHEN event_name = 'user_engagement' THEN COALESCE(engagement_time_msec, 0)
                ELSE 0
              END
            ) AS engagement_msec,
            MAX(
              CASE
                WHEN event_name = 'scroll' THEN COALESCE(percent_scrolled, 0)
                ELSE 0
              END
            ) AS max_scroll
          FROM base_events
          WHERE session_id IS NOT NULL
          GROUP BY user_pseudo_id, session_id
        ),

        session_facts AS (
          SELECT
            s.date,
            s.landing_page,
            s.device_type,
            s.country,
            s.session_source,
            s.session_medium,
            s.session_campaign,

            COALESCE(m.pageviews, 1) AS pageviews,
            COALESCE(m.engagement_msec, 0) AS engagement_msec,
            COALESCE(m.max_scroll, 0) AS max_scroll,

            CASE
              WHEN COALESCE(m.engagement_msec, 0) >= 10000 THEN 1
              WHEN COALESCE(m.pageviews, 1) > 1 THEN 1
              WHEN COALESCE(m.max_scroll, 0) >= 90 THEN 1
              ELSE 0
            END AS is_engaged,

            CASE
              WHEN COALESCE(m.engagement_msec, 0) >= 30000 THEN 1
              ELSE 0
            END AS is_high_engagement,

            CASE
              WHEN COALESCE(m.pageviews, 1) > 1 THEN 1
              ELSE 0
            END AS is_multi_page

          FROM session_starts s
          LEFT JOIN session_metrics m
            ON s.user_pseudo_id = m.user_pseudo_id
           AND s.session_id = m.session_id
        )

        SELECT
          date,
          landing_page,
          device_type,
          country,
          session_source,
          session_medium,
          session_campaign,
          COUNT(*) AS sessions,
          SUM(is_engaged) AS engaged_sessions,
          COUNT(*) - SUM(is_engaged) AS bounced_sessions,
          ROUND(AVG(engagement_msec) / 1000, 3) AS avg_engagement_seconds,
          ROUND(AVG(pageviews), 3) AS avg_pageviews,
          ROUND(AVG(max_scroll), 3) AS avg_max_scroll,
          0 AS total_conversions,
          SUM(is_high_engagement) AS high_engagement_sessions,
          SUM(is_multi_page) AS multi_page_sessions
        FROM session_facts
        GROUP BY
          date,
          landing_page,
          device_type,
          country,
          session_source,
          session_medium,
          session_campaign
        ORDER BY date DESC, sessions DESC
        """

        df = self.bq_client.query(query).to_dataframe()
        logger.info(f"BigQuery returned {len(df)} rows")
        return df

    # ------------------------------------------------------------------
    # TAXONOMY
    # ------------------------------------------------------------------
    def apply_taxonomy(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Applying taxonomy...")

        if df.empty:
            return df

        df = df.copy()

        df["landing_page_clean"] = (
            df["landing_page"]
            .astype(str)
            .str.replace(r"^https?://www\.comparemymove\.com", "", regex=True)
        )

        df["device_category"] = df["device_type"].fillna("unknown")

        df["traffic_type"] = "other"
        df.loc[df["session_medium"].eq("organic"), "traffic_type"] = "organic_search"
        df.loc[df["session_medium"].eq("cpc"), "traffic_type"] = "paid_search"
        df.loc[df["session_medium"].eq("email"), "traffic_type"] = "email"
        df.loc[df["session_medium"].eq("referral"), "traffic_type"] = "referral"

        df["page_category"] = "other"
        df["page_type"] = "other_pages"

        tool_mask = (
            df["landing_page_clean"].str.contains(r"^/conveyancing/[a-z0-9-]+(?:/|$)", na=False, regex=True)
            | df["landing_page_clean"].str.contains(r"^/chartered-surveyors/[a-z0-9-]+(?:/|$)", na=False, regex=True)
            | df["landing_page_clean"].str.contains(r"^/house-removals/[a-z0-9-]+(?:/|$)", na=False, regex=True)
            | df["landing_page_clean"].str.contains(r"^/companies/(?:conveyancing|surveying|removals)/", na=False, regex=True)
        )

        df.loc[tool_mask, "page_category"] = "tool_page"
        df.loc[tool_mask, "page_type"] = "tool_page"

        return df

    # ------------------------------------------------------------------
    # POSTGRES FETCH FOR LAST 7 DAYS
    # ------------------------------------------------------------------
    def fetch_postgres_rows(self, start_date: date, end_date: date) -> pd.DataFrame:
        query = text(f"""
            SELECT
                date,
                landing_page,
                device_type,
                country,
                session_source,
                session_medium,
                session_campaign,
                sessions,
                engaged_sessions,
                bounced_sessions,
                avg_engagement_seconds,
                avg_pageviews,
                avg_max_scroll,
                total_conversions,
                high_engagement_sessions,
                multi_page_sessions,
                traffic_type,
                page_category,
                page_type,
                device_category,
                landing_page_clean
            FROM {SCHEMA_NAME}.{TABLE_NAME}
            WHERE date BETWEEN :start_date AND :end_date
        """)

        with self.pg_engine.connect() as conn:
            df = pd.read_sql(query, conn, params={
                "start_date": start_date,
                "end_date": end_date
            })

        logger.info(f"Postgres returned {len(df)} rows for comparison")
        return df

    # ------------------------------------------------------------------
    # COMPARE BQ VS POSTGRES
    # ------------------------------------------------------------------
    def get_rows_to_upsert(self, bq_df: pd.DataFrame, pg_df: pd.DataFrame) -> pd.DataFrame:
        if bq_df.empty:
            return bq_df

        bq_df = bq_df.copy()
        pg_df = pg_df.copy()

        # Normalize null-ish values
        for col in ["session_source", "session_medium", "session_campaign"]:
            if col in bq_df.columns:
                if col == "session_campaign":
                    bq_df[col] = bq_df[col].fillna("(not set)")
                else:
                    bq_df[col] = bq_df[col].fillna("(none)")
            if col in pg_df.columns:
                if col == "session_campaign":
                    pg_df[col] = pg_df[col].fillna("(not set)")
                else:
                    pg_df[col] = pg_df[col].fillna("(none)")

        # Normalize dates
        bq_df["date"] = pd.to_datetime(bq_df["date"]).dt.date
        if not pg_df.empty:
            pg_df["date"] = pd.to_datetime(pg_df["date"]).dt.date

        # Round numeric compare fields to reduce false diffs
        numeric_compare = ["avg_engagement_seconds", "avg_pageviews", "avg_max_scroll"]
        for col in numeric_compare:
            if col in bq_df.columns:
                bq_df[col] = pd.to_numeric(bq_df[col], errors="coerce").round(3)
            if col in pg_df.columns:
                pg_df[col] = pd.to_numeric(pg_df[col], errors="coerce").round(3)

        # If Postgres has nothing, everything is new
        if pg_df.empty:
            logger.info("No matching Postgres rows found; all BigQuery rows will be upserted.")
            return bq_df

        merged = bq_df.merge(
            pg_df,
            on=KEY_COLS,
            how="left",
            suffixes=("", "_pg"),
            indicator=True
        )

        missing_mask = merged["_merge"].eq("left_only")

        changed_mask = pd.Series(False, index=merged.index)
        for col in COMPARE_COLS:
            changed_mask = changed_mask | (
                merged[col].fillna("__NULL__").astype(str)
                != merged[f"{col}_pg"].fillna("__NULL__").astype(str)
            )

        final_mask = missing_mask | changed_mask

        rows_to_upsert = merged.loc[final_mask, bq_df.columns].copy()

        logger.info(
            "Comparison result: %s missing, %s changed, %s total to upsert",
            int(missing_mask.sum()),
            int((changed_mask & ~missing_mask).sum()),
            len(rows_to_upsert),
        )

        return rows_to_upsert

    # ------------------------------------------------------------------
    # UPSERT
    # ------------------------------------------------------------------
    def upsert_into_postgres(self, df: pd.DataFrame) -> int:
        if df.empty:
            logger.info("No rows to upsert.")
            return 0

        df = df.copy()
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["session_source"] = df["session_source"].fillna("(none)")
        df["session_medium"] = df["session_medium"].fillna("(none)")
        df["session_campaign"] = df["session_campaign"].fillna("(not set)")
        df["last_updated"] = datetime.now()

        rows = df.to_dict(orient="records")

        upsert_sql = text(f"""
            INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} (
                date,
                landing_page,
                device_type,
                country,
                session_source,
                session_medium,
                session_campaign,
                sessions,
                engaged_sessions,
                bounced_sessions,
                avg_engagement_seconds,
                avg_pageviews,
                avg_max_scroll,
                total_conversions,
                high_engagement_sessions,
                multi_page_sessions,
                traffic_type,
                page_category,
                page_type,
                device_category,
                landing_page_clean,
                last_updated
            )
            VALUES (
                :date,
                :landing_page,
                :device_type,
                :country,
                :session_source,
                :session_medium,
                :session_campaign,
                :sessions,
                :engaged_sessions,
                :bounced_sessions,
                :avg_engagement_seconds,
                :avg_pageviews,
                :avg_max_scroll,
                :total_conversions,
                :high_engagement_sessions,
                :multi_page_sessions,
                :traffic_type,
                :page_category,
                :page_type,
                :device_category,
                :landing_page_clean,
                :last_updated
            )
            ON CONFLICT (
                date,
                landing_page,
                device_type,
                country,
                session_source,
                session_medium,
                session_campaign
            )
            DO UPDATE SET
                sessions = EXCLUDED.sessions,
                engaged_sessions = EXCLUDED.engaged_sessions,
                bounced_sessions = EXCLUDED.bounced_sessions,
                avg_engagement_seconds = EXCLUDED.avg_engagement_seconds,
                avg_pageviews = EXCLUDED.avg_pageviews,
                avg_max_scroll = EXCLUDED.avg_max_scroll,
                total_conversions = EXCLUDED.total_conversions,
                high_engagement_sessions = EXCLUDED.high_engagement_sessions,
                multi_page_sessions = EXCLUDED.multi_page_sessions,
                traffic_type = EXCLUDED.traffic_type,
                page_category = EXCLUDED.page_category,
                page_type = EXCLUDED.page_type,
                device_category = EXCLUDED.device_category,
                landing_page_clean = EXCLUDED.landing_page_clean,
                last_updated = EXCLUDED.last_updated
        """)

        with self.pg_engine.begin() as conn:
            conn.execute(upsert_sql, rows)

        logger.info(f"Upserted {len(df)} rows into Postgres.")
        return len(df)

    # ------------------------------------------------------------------
    # RUN
    # ------------------------------------------------------------------
    def run(self, start_date: date = None, end_date: date = None) -> int:
        logger.info("=" * 80)
        logger.info("Starting GA4 compare-and-upsert ETL")

        self.ensure_table_exists()

        today = datetime.today().date()
        yesterday = today - timedelta(days=1)

        if end_date is None:
            end_date = yesterday

        if start_date is None:
            start_date = end_date - timedelta(days=6)  # last 7 days inclusive

        if start_date > end_date:
            logger.info("No valid date range to process.")
            return 0

        logger.info(f"Comparison window: {start_date} → {end_date}")

        bq_df = self.run_bigquery_query(start_date, end_date)
        bq_df = self.apply_taxonomy(bq_df)

        pg_df = self.fetch_postgres_rows(start_date, end_date)

        rows_to_upsert = self.get_rows_to_upsert(bq_df, pg_df)

        upserted = self.upsert_into_postgres(rows_to_upsert)

        logger.info(f"Completed — upserted {upserted} rows.")
        logger.info("=" * 80)
        return upserted


def main():
    try:
        etl = GA4LandingPagesCompareUpsertETL()
        etl.run()
        return 0
    except Exception as e:
        logger.exception(f"ETL failed: {e}")
        return 1


if __name__ == "__main__":
    etl = GA4LandingPagesCompareUpsertETL()
    etl.run()