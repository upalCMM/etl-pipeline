#!/usr/bin/env python3
"""
DAILY INCREMENTAL ETL - CORRECTED VERSION
Upserts only new/updated records from MySQL to Postgres
Uses loaded_at to track last successful run
"""
import os
import sys
from dotenv import load_dotenv
import mysql.connector
import psycopg2
import psycopg2.extras
import logging
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('companies_etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

# MySQL Connection
MYSQL_HOST = os.getenv("MARIA_HOST")
MYSQL_PORT = int(os.getenv("MARIA_PORT", 3306))
MYSQL_USER = os.getenv("MARIA_USER")
MYSQL_PASS = os.getenv("MARIA_PASS")
MYSQL_DB = os.getenv("MARIA_DB")

# PostgreSQL Connection
PG_HOST = os.getenv("PG_HOST")
PG_PORT = int(os.getenv("PG_PORT", 5432))
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")

def to_bool(val):
    return bool(val) if val is not None else None

def get_last_etl_run(pg_cursor):
    """Get the timestamp of the last successful ETL run"""
    pg_cursor.execute("""
        SELECT COALESCE(MAX(loaded_at), '1900-01-01'::timestamp) 
        FROM crm_data.partner_master_companies
    """)
    last_run = pg_cursor.fetchone()[0]
    logger.info(f"📅 Last ETL run: {last_run}")
    return last_run

# Connect to MySQL
logger.info(f"Connecting to MySQL: {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}")
mysql_conn = mysql.connector.connect(
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    user=MYSQL_USER,
    password=MYSQL_PASS,
    database=MYSQL_DB,
)
mysql_cursor = mysql_conn.cursor(dictionary=True)
logger.info("✅ Connected to MySQL")

# Connect to PostgreSQL
logger.info(f"Connecting to PostgreSQL: {PG_HOST}:{PG_PORT}/{PG_DB}")
pg_conn = psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    database=PG_DB,
    user=PG_USER,
    password=PG_PASS,
)
pg_cursor = pg_conn.cursor()
logger.info("✅ Connected to PostgreSQL")

# Get last successful run timestamp
last_run = get_last_etl_run(pg_cursor)

# Fetch records updated since last run (with 1 hour buffer to be safe)
buffer_time = last_run - timedelta(hours=1) if last_run.year > 1900 else last_run
logger.info(f"🔍 Fetching records updated since: {buffer_time}")

query = """
    SELECT 
        c.id, c.service_id, s.name AS service_name,
        c.b2b_user_id, c.promo_code_id, c.uuid,
        c.company_name, c.display_name, c.username,
        c.lead_status, c.active, c.account_type, c.payg_method,
        c.http_referrer, c.extra_services,
        c.created_at, c.updated_at, c.deleted_at,
        c.previous_id, c.has_applied, c.reference_number,
        c.directory, c.heard_about, c.start_when,
        c.terms_accepted, c.applied_at, c.established_date,
        c.last_logged_in_at, c.archived_at, c.signup_id
    FROM compmove_core.companies c
    LEFT JOIN compmove_core.services s ON c.service_id = s.id
    WHERE c.updated_at > %s OR c.created_at > %s
    ORDER BY c.id
"""

mysql_cursor.execute(query, (buffer_time, buffer_time))
rows = mysql_cursor.fetchall()
logger.info(f"📊 Found {len(rows)} records to upsert")

if len(rows) == 0:
    logger.info("✅ No new or updated records. ETL complete.")
    pg_cursor.close()
    pg_conn.close()
    mysql_cursor.close()
    mysql_conn.close()
    sys.exit(0)

# Prepare upsert data with current timestamp for loaded_at
current_timestamp = datetime.now()
data = []

for r in rows:
    partner_type = r.get("service_name") or 'Unknown'
    
    data.append((
        partner_type,                                    # partner_type
        r["id"],                                         # id
        r.get("uuid"),                                   # uuid
        r.get("company_name"),                           # company_name
        r.get("display_name"),                           # display_name
        r.get("username"),                               # username
        r.get("lead_status"),                            # lead_status
        to_bool(r.get("active")),                        # active
        r.get("account_type"),                           # account_type
        r.get("payg_method"),                            # payg_method
        r.get("http_referrer"),                          # http_referrer
        r.get("extra_services"),                         # extra_services
        r.get("created_at"),                             # created_at
        r.get("updated_at"),                             # updated_at
        r.get("deleted_at"),                             # deleted_at
        r.get("service_id"),                             # service_id
        r.get("b2b_user_id"),                            # b2b_user_id
        r.get("promo_code_id"),                          # promo_code_id
        current_timestamp,                               # loaded_at (NOW!)
        r.get("previous_id"),                            # previous_id
        to_bool(r.get("has_applied")),                   # has_applied
        r.get("reference_number"),                       # reference_number
        r.get("directory"),                              # directory
        r.get("heard_about"),                            # heard_about
        r.get("start_when"),                             # start_when
        to_bool(r.get("terms_accepted")),                # terms_accepted
        r.get("applied_at"),                             # applied_at
        r.get("established_date"),                       # established_date
        r.get("last_logged_in_at"),                      # last_logged_in_at
        r.get("archived_at"),                            # archived_at
        r.get("signup_id")                               # signup_id
    ))

# Upsert SQL
upsert_sql = """
INSERT INTO crm_data.partner_master_companies (
    partner_type, id, uuid, company_name, display_name, username,
    lead_status, active, account_type, payg_method, http_referrer,
    extra_services, created_at, updated_at, deleted_at, service_id,
    b2b_user_id, promo_code_id, loaded_at,
    previous_id, has_applied, reference_number, directory, heard_about,
    start_when, terms_accepted, applied_at, established_date,
    last_logged_in_at, archived_at, signup_id
) VALUES %s
ON CONFLICT (partner_type, id) DO UPDATE SET
    uuid = EXCLUDED.uuid,
    company_name = EXCLUDED.company_name,
    display_name = EXCLUDED.display_name,
    username = EXCLUDED.username,
    lead_status = EXCLUDED.lead_status,
    active = EXCLUDED.active,
    account_type = EXCLUDED.account_type,
    payg_method = EXCLUDED.payg_method,
    http_referrer = EXCLUDED.http_referrer,
    extra_services = EXCLUDED.extra_services,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at,
    deleted_at = EXCLUDED.deleted_at,
    service_id = EXCLUDED.service_id,
    b2b_user_id = EXCLUDED.b2b_user_id,
    promo_code_id = EXCLUDED.promo_code_id,
    loaded_at = EXCLUDED.loaded_at,
    previous_id = EXCLUDED.previous_id,
    has_applied = EXCLUDED.has_applied,
    reference_number = EXCLUDED.reference_number,
    directory = EXCLUDED.directory,
    heard_about = EXCLUDED.heard_about,
    start_when = EXCLUDED.start_when,
    terms_accepted = EXCLUDED.terms_accepted,
    applied_at = EXCLUDED.applied_at,
    established_date = EXCLUDED.established_date,
    last_logged_in_at = EXCLUDED.last_logged_in_at,
    archived_at = EXCLUDED.archived_at,
    signup_id = EXCLUDED.signup_id
"""

logger.info("🚀 Running upsert...")
psycopg2.extras.execute_values(pg_cursor, upsert_sql, data, page_size=1000)
pg_conn.commit()

logger.info(f"✅ Successfully upserted {len(data)} rows")
logger.info(f"⏰ ETL completed at: {current_timestamp}")

# Show summary of what was updated
pg_cursor.execute("""
    SELECT 
        partner_type,
        COUNT(*) as updated_count
    FROM crm_data.partner_master_companies
    WHERE loaded_at = %s
    GROUP BY partner_type
    ORDER BY partner_type
""", (current_timestamp,))

summary = pg_cursor.fetchall()
if summary:
    logger.info("📊 Summary of updates by partner type:")
    for partner_type, count in summary:
        logger.info(f"  {partner_type}: {count}")

# Cleanup
pg_cursor.close()
pg_conn.close()
mysql_cursor.close()
mysql_conn.close()

logger.info("=" * 60)
logger.info("🎉 INCREMENTAL ETL COMPLETED SUCCESSFULLY!")
logger.info("=" * 60)