#!/usr/bin/env python3
"""
PD_sync_mailchimp_final_new.py
Direct Mailchimp -> PostgreSQL sync (paged extraction + retries + visible print progress)
Includes proxy_excluded fields and auto-detects data center from API key
"""

import os
import time
import random
import json
from pathlib import Path

import certifi
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import psycopg2
import pandas as pd
from dotenv import load_dotenv


# ------------------------------------------------------------------------------
# ENV LOADING with absolute path and debugging
# ------------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent.absolute()
ENV_PATH = SCRIPT_DIR / ".env"

print(f"\n{'='*60}")
print("🔧 ENVIRONMENT LOADING")
print(f"{'='*60}")
print(f"Script directory: {SCRIPT_DIR}")
print(f"Looking for .env at: {ENV_PATH}")
print(f".env exists: {ENV_PATH.exists()}")

if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH, override=True)
    print("✅ .env file loaded successfully")
else:
    print("❌ .env file not found!")
    print("Please create .env file at:", ENV_PATH)
    sys.exit(1)

# Show loaded variables (masked)
print("\n📋 Loaded Configuration:")
print(f"MAILCHIMP_API_KEY: {'[SET]' if os.getenv('MAILCHIMP_API_KEY') else 'NOT SET'}")
print(f"MAILCHIMP_DATA_CENTER: {os.getenv('MAILCHIMP_DATA_CENTER', 'NOT SET')}")
print(f"PG_HOST: {os.getenv('PG_HOST', 'NOT SET')}")
print(f"PG_PORT: {os.getenv('PG_PORT', 'NOT SET')}")
print(f"PG_DB: {os.getenv('PG_DB', 'NOT SET')}")
print(f"PG_USER: {os.getenv('PG_USER', 'NOT SET')}")
print(f"PG_PASS: {'[SET]' if os.getenv('PG_PASS') else 'NOT SET'}")
print(f"{'='*60}\n")


# ------------------------------------------------------------------------------
# MAILCHIMP EXTRACT
# ------------------------------------------------------------------------------
def _mailchimp_session() -> requests.Session:
    """
    Requests session with retries for transient API/edge errors.
    """
    retry = Retry(
        total=8,
        connect=8,
        read=8,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_mailchimp_data_direct():
    """
    Fetch Mailchimp reports directly from API (paged + retries + print progress).
    Auto-detects data center from API key.
    Returns: list[dict] in flattened structure.
    """
    print("\n>>> entered get_mailchimp_data_direct() <<<\n")

    api_key = os.getenv("MAILCHIMP_API_KEY")
    
    # Auto-detect data center from API key (most reliable method)
    if api_key and '-' in api_key:
        data_center = api_key.split('-')[-1]
        print(f"✅ Auto-detected data center from API key: {data_center}")
    else:
        data_center = os.getenv("MAILCHIMP_DATA_CENTER", "us1")
        print(f"⚠️ Using configured data center: {data_center}")

    if not api_key:
        print("❌ MAILCHIMP_API_KEY not found in environment variables")
        return None

    since_date = os.getenv("MAILCHIMP_SINCE_SEND_TIME", "2025-08-01T00:00:00Z")
    page_size = int(os.getenv("MAILCHIMP_PAGE_SIZE", "200"))

    url = f"https://{data_center}.api.mailchimp.com/3.0/reports"
    auth = ("anystring", api_key)
    session = _mailchimp_session()

    offset = 0
    total_items = None
    all_reports = []
    page_number = 1

    # Reduce payload size substantially (stability + speed)
    fields = (
        "reports.id,reports.campaign_title,reports.list_name,reports.subject_line,"
        "reports.emails_sent,reports.unsubscribed,reports.send_time,"
        "reports.bounces.hard_bounces,reports.bounces.soft_bounces,"
        "reports.opens.opens_total,reports.opens.unique_opens,reports.opens.open_rate,"
        "reports.opens.proxy_excluded_opens,reports.opens.proxy_excluded_unique_opens,reports.opens.proxy_excluded_open_rate,"
        "reports.clicks.clicks_total,reports.clicks.unique_clicks,reports.clicks.click_rate,"
        "reports._links,"
        "total_items"
    )

    print("\n" + "=" * 70)
    print("🚀 STARTING MAILCHIMP EXTRACTION")
    print(f"Date range: {since_date} → Present")
    print(f"Page size: {page_size}")
    print(f"Data center: {data_center}")
    print("=" * 70)

    while True:
        print(f"\n📥 Requesting page {page_number} | offset={offset}")

        params = {
            "since_send_time": since_date,
            "count": page_size,
            "offset": offset,
            "sort_field": "send_time",
            "sort_dir": "DESC",
            "fields": fields,
        }

        try:
            response = session.get(
                url,
                params=params,
                auth=auth,
                verify=certifi.where(),
                timeout=(10, 120),
                headers={"User-Agent": "pipedrive-mailchimp-sync/1.0"},
            )

            if response.status_code == 504:
                sleep_s = 5 + random.random() * 5
                print(f"⚠️  504 Timeout at offset={offset}. Sleeping {sleep_s:.1f}s and retrying...")
                time.sleep(sleep_s)
                continue

            response.raise_for_status()
            payload = response.json()

        except requests.exceptions.RequestException as e:
            print("❌ API request failed")
            print(f"Error: {e}")
            if getattr(e, "response", None):
                print(f"Status Code: {e.response.status_code}")
                print(f"Response Body: {e.response.text[:1000]}")
            return None

        reports = payload.get("reports", [])

        if total_items is None:
            total_items = payload.get("total_items")
            print(f"📊 Total reports available: {total_items}")

        if not reports:
            print("✅ No more reports returned. Pagination complete.")
            break

        all_reports.extend(reports)

        print(f"✔ Page {page_number} complete")
        print(f"   Fetched this page: {len(reports)}")
        print(f"   Total accumulated: {len(all_reports)}")

        if total_items:
            progress_pct = (len(all_reports) / total_items) * 100
            print(f"   Progress: {progress_pct:.2f}%")

        offset += len(reports)
        page_number += 1

        if total_items and offset >= total_items:
            print("✅ Reached total_items limit.")
            break

    print("\n" + "=" * 70)
    print(f"🎉 EXTRACTION COMPLETE — {len(all_reports)} reports fetched")
    print("=" * 70)

    # Flatten
    processed_data = []
    print("\n🔄 Processing reports...")

    for i, report in enumerate(all_reports, start=1):
        opens_data = report.get("opens", {}) or {}
        bounces_data = report.get("bounces", {}) or {}
        clicks_data = report.get("clicks", {}) or {}

        processed_data.append({
            "id": report.get("id", ""),
            "campaign_title": report.get("campaign_title", ""),
            "list_name": report.get("list_name", ""),
            "subject_line": report.get("subject_line", ""),
            "emails_sent": report.get("emails_sent", 0),
            "unsubscribed": report.get("unsubscribed", 0),
            "send_time": report.get("send_time", ""),
            "hard_bounces": bounces_data.get("hard_bounces", 0),
            "soft_bounces": bounces_data.get("soft_bounces", 0),
            "opens_total": opens_data.get("opens_total", 0),
            "unique_opens": opens_data.get("unique_opens", 0),
            "open_rate": opens_data.get("open_rate", 0),
            "proxy_excluded_opens": opens_data.get("proxy_excluded_opens", 0),
            "proxy_excluded_unique_opens": opens_data.get("proxy_excluded_unique_opens", 0),
            "proxy_excluded_open_rate": opens_data.get("proxy_excluded_open_rate", 0),
            "clicks_total": clicks_data.get("clicks_total", 0),
            "unique_clicks": clicks_data.get("unique_clicks", 0),
            "click_rate": clicks_data.get("click_rate", 0),
            "_links": report.get("_links", {}) or {},
        })

        if i % 50 == 0:
            print(f"   Processed {i}/{len(all_reports)} reports...")

    print("✅ Processing complete.\n")
    return processed_data


# ------------------------------------------------------------------------------
# POSTGRES LOAD
# ------------------------------------------------------------------------------
def ensure_mailchimp_table(cursor):
    """
    Ensure pipedrive.fact_mailchimp exists and has required columns.
    """
    # Create schema if it doesn't exist
    cursor.execute("CREATE SCHEMA IF NOT EXISTS pipedrive;")
    
    check_columns_query = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'pipedrive'
      AND table_name = 'fact_mailchimp'
      AND column_name IN ('proxy_excluded_opens', 'proxy_excluded_unique_opens', 'proxy_excluded_open_rate', '_links');
    """
    cursor.execute(check_columns_query)
    existing_columns = [row[0] for row in cursor.fetchall()]

    required_columns = ['proxy_excluded_opens', 'proxy_excluded_unique_opens', 'proxy_excluded_open_rate', '_links']
    missing_columns = [c for c in required_columns if c not in existing_columns]

    if missing_columns:
        print(f"🛠 Missing columns detected: {missing_columns} — adding...")
        for column in missing_columns:
            if column == "_links":
                alter_query = "ALTER TABLE pipedrive.fact_mailchimp ADD COLUMN IF NOT EXISTS _links JSONB;"
            elif column == "proxy_excluded_open_rate":
                alter_query = "ALTER TABLE pipedrive.fact_mailchimp ADD COLUMN IF NOT EXISTS proxy_excluded_open_rate DECIMAL(5,4);"
            else:
                alter_query = f"ALTER TABLE pipedrive.fact_mailchimp ADD COLUMN IF NOT EXISTS {column} INTEGER DEFAULT 0;"

            cursor.execute(alter_query)
            print(f"✅ Added column: {column}")

    create_table_query = """
    CREATE TABLE IF NOT EXISTS pipedrive.fact_mailchimp (
        id VARCHAR(50) PRIMARY KEY,
        campaign_title TEXT,
        list_name TEXT,
        subject_line TEXT,
        emails_sent INTEGER,
        unsubscribed INTEGER,
        send_time TIMESTAMP,
        hard_bounces INTEGER DEFAULT 0,
        soft_bounces INTEGER DEFAULT 0,
        opens_total INTEGER DEFAULT 0,
        unique_opens INTEGER DEFAULT 0,
        open_rate DECIMAL(5,4),
        clicks_total INTEGER DEFAULT 0,
        unique_clicks INTEGER DEFAULT 0,
        click_rate DECIMAL(5,4),
        proxy_excluded_opens INTEGER DEFAULT 0,
        proxy_excluded_unique_opens INTEGER DEFAULT 0,
        proxy_excluded_open_rate DECIMAL(5,4),
        _links JSONB,
        sync_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT valid_open_rate CHECK (open_rate >= 0 AND open_rate <= 1),
        CONSTRAINT valid_click_rate CHECK (click_rate >= 0 AND click_rate <= 1),
        CONSTRAINT valid_proxy_open_rate CHECK (proxy_excluded_open_rate >= 0 AND proxy_excluded_open_rate <= 1)
    );

    CREATE INDEX IF NOT EXISTS idx_fact_mailchimp_send_time ON pipedrive.fact_mailchimp(send_time);
    CREATE INDEX IF NOT EXISTS idx_fact_mailchimp_list_name ON pipedrive.fact_mailchimp(list_name);
    """
    cursor.execute(create_table_query)
    print("✅ Table pipedrive.fact_mailchimp ensured")
    return True


def load_data_to_postgres(reports_data):
    """
    Load report rows into pipedrive.fact_mailchimp with upsert.
    Uses print() statements for visible progress.
    """
    # Get connection parameters from environment using PG_* variables
    db_host = os.getenv("PG_HOST", "127.0.0.1")
    db_port = os.getenv("PG_PORT", "5432")
    db_name = os.getenv("PG_DB")
    db_user = os.getenv("PG_USER")
    db_password = os.getenv("PG_PASS")
    
    # Validate required parameters
    if not all([db_name, db_user, db_password]):
        print("❌ Missing required database connection parameters")
        print(f"PG_DB: {'[SET]' if db_name else 'NOT SET'}")
        print(f"PG_USER: {'[SET]' if db_user else 'NOT SET'}")
        print(f"PG_PASS: {'[SET]' if db_password else 'NOT SET'}")
        return False
    
    # Build connection parameters - force TCP/IP
    db_config = {
        "host": db_host,
        "port": db_port,
        "database": db_name,
        "user": db_user,
        "password": db_password,
        "connect_timeout": 10,
        "sslmode": "prefer"  # Use SSL if available
    }
    
    # Print connection attempt details (mask password)
    print("\n" + "="*50)
    print("🔌 ATTEMPTING POSTGRES CONNECTION")
    print("="*50)
    print(f"   Host: {db_host}")
    print(f"   Port: {db_port}")
    print(f"   Database: {db_name}")
    print(f"   User: {db_user}")
    print(f"   Password: {'[SET]' if db_password else '[NOT SET]'}")
    print("="*50 + "\n")

    if not reports_data:
        print("⚠️ No data to load into PostgreSQL")
        return True

    print("\n" + "=" * 70)
    print("🐘 STARTING POSTGRES LOAD")
    print(f"Rows to load: {len(reports_data)}")
    print("=" * 70)

    conn = None
    cur = None
    
    try:
        # Attempt connection
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        
        print("✅ Database connection established")

        # Ensure table exists
        ensure_mailchimp_table(cur)

        insert_query = """
        INSERT INTO pipedrive.fact_mailchimp
        (id, campaign_title, list_name, subject_line, emails_sent, unsubscribed,
         send_time, hard_bounces, soft_bounces, opens_total, unique_opens, open_rate,
         clicks_total, unique_clicks, click_rate, proxy_excluded_opens,
         proxy_excluded_unique_opens, proxy_excluded_open_rate, _links)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            campaign_title = EXCLUDED.campaign_title,
            list_name = EXCLUDED.list_name,
            subject_line = EXCLUDED.subject_line,
            emails_sent = EXCLUDED.emails_sent,
            unsubscribed = EXCLUDED.unsubscribed,
            send_time = EXCLUDED.send_time,
            hard_bounces = EXCLUDED.hard_bounces,
            soft_bounces = EXCLUDED.soft_bounces,
            opens_total = EXCLUDED.opens_total,
            unique_opens = EXCLUDED.unique_opens,
            open_rate = EXCLUDED.open_rate,
            clicks_total = EXCLUDED.clicks_total,
            unique_clicks = EXCLUDED.unique_clicks,
            click_rate = EXCLUDED.click_rate,
            proxy_excluded_opens = EXCLUDED.proxy_excluded_opens,
            proxy_excluded_unique_opens = EXCLUDED.proxy_excluded_unique_opens,
            proxy_excluded_open_rate = EXCLUDED.proxy_excluded_open_rate,
            _links = EXCLUDED._links,
            sync_timestamp = CURRENT_TIMESTAMP;
        """

        # Build tuples
        data_tuples = []
        for r in reports_data:
            try:
                send_time = pd.to_datetime(r.get("send_time")).tz_localize(None) if r.get("send_time") else None
                data_tuples.append((
                    r.get("id", ""),
                    r.get("campaign_title", ""),
                    r.get("list_name", ""),
                    r.get("subject_line", ""),
                    int(r.get("emails_sent", 0) or 0),
                    int(r.get("unsubscribed", 0) or 0),
                    send_time,
                    int(r.get("hard_bounces", 0) or 0),
                    int(r.get("soft_bounces", 0) or 0),
                    int(r.get("opens_total", 0) or 0),
                    int(r.get("unique_opens", 0) or 0),
                    float(r.get("open_rate", 0) or 0),
                    int(r.get("clicks_total", 0) or 0),
                    int(r.get("unique_clicks", 0) or 0),
                    float(r.get("click_rate", 0) or 0),
                    int(r.get("proxy_excluded_opens", 0) or 0),
                    int(r.get("proxy_excluded_unique_opens", 0) or 0),
                    float(r.get("proxy_excluded_open_rate", 0) or 0),
                    json.dumps(r.get("_links") or {}),
                ))
            except Exception as e:
                print(f"⚠️ Error processing row {r.get('id', 'unknown')}: {e}")
                continue

        batch_size = int(os.getenv("PG_BATCH_SIZE", "500"))
        total = len(data_tuples)
        
        if total == 0:
            print("⚠️ No valid data to insert after processing")
            return True

        print(f"\n📦 Inserting {total} records in batches of {batch_size}...")

        for start in range(0, total, batch_size):
            end = min(start + batch_size, total)
            cur.executemany(insert_query, data_tuples[start:end])
            conn.commit()
            print(f"✅ Inserted/Upserted rows {start + 1}-{end} of {total}")

        # Get final count
        cur.execute("SELECT COUNT(*) FROM pipedrive.fact_mailchimp")
        count = cur.fetchone()[0]
        print(f"\n📌 Total records now in pipedrive.fact_mailchimp: {count}")

        # Show latest records
        cur.execute("""
            SELECT id, campaign_title, send_time, emails_sent,
                   unique_opens, proxy_excluded_unique_opens,
                   unique_clicks, proxy_excluded_open_rate
            FROM pipedrive.fact_mailchimp
            ORDER BY send_time DESC
            LIMIT 5
        """)
        latest = cur.fetchall()

        print("\n" + "=" * 80)
        print("📊 LATEST 5 RECORDS IN pipedrive.fact_mailchimp")
        print("=" * 80)
        for record in latest:
            print(f"ID: {record[0]}")
            print(f"Campaign: {record[1]}")
            print(f"Sent: {record[2]} | Emails: {record[3]}")
            print(f"Unique Opens: {record[4]} | Proxy Excluded Unique Opens: {record[5]}")
            print(f"Unique Clicks: {record[6]} | Proxy Excluded Open Rate: {record[7]}")
            print("-" * 80)

        print("🎉 POSTGRES LOAD COMPLETE\n")
        return True

    except psycopg2.OperationalError as e:
        print(f"❌ Database connection error: {e}")
        print("\n💡 Troubleshooting tips:")
        print("   1. Check if PostgreSQL is running: sudo systemctl status postgresql")
        print("   2. Verify connection parameters in .env file")
        print("   3. Test connection manually: psql -h 127.0.0.1 -U postgres -d cmm_pipedrive")
        return False
        
    except Exception as e:
        print(f"❌ Error loading data to PostgreSQL: {e}")
        if conn:
            conn.rollback()
        return False

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            print("🔒 Database connection closed")


# ------------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    print("\n" + "="*60)
    print("📧 DIRECT MAILCHIMP TO POSTGRESQL SYNC")
    print("="*60)
    print("INCLUDING: proxy_excluded_opens, proxy_excluded_unique_opens,")
    print("           proxy_excluded_open_rate, _links")
    print(f"Date Range: August 1st, 2025 to present")
    print(f"Table: pipedrive.fact_mailchimp")
    print("="*60 + "\n")

    start_time = time.time()

    # Fetch data from Mailchimp
    reports_data = get_mailchimp_data_direct()

    if reports_data is None:
        print("\n❌ Failed to fetch data from Mailchimp API")
        raise SystemExit(1)

    if not reports_data:
        print("\n⚠️ No data found for the specified date range")
        raise SystemExit(0)

    # Load to PostgreSQL
    success = load_data_to_postgres(reports_data)

    # Summary
    elapsed_time = time.time() - start_time
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)

    print("\n" + "="*60)
    if success:
        print("✅ SYNC COMPLETED SUCCESSFULLY")
        print(f"📊 Synced {len(reports_data)} records")
    else:
        print("❌ SYNC FAILED")
    print(f"⏱️  Time taken: {minutes}m {seconds}s")
    print("="*60 + "\n")

    if success:
        raise SystemExit(0)
    else:
        raise SystemExit(2)