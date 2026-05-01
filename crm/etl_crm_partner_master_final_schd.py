#!/usr/bin/env python3
"""
ETL Script - MariaDB -> Postgres
- MariaDB creds loaded from .env (MARIA_* vars)
- Postgres creds loaded from .env (PG_* vars)
"""
import os
import sys

# AUTO-FIX: Add project root to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from dotenv import load_dotenv
import mysql.connector
import psycopg2
import psycopg2.extras

# ---------------------------------------------------------
# LOAD ENVIRONMENT VARIABLES
# ---------------------------------------------------------
load_dotenv()

# Postgres
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB   = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")

# MariaDB (from .env)
MARIA_HOST = os.getenv("MARIA_HOST")
MARIA_PORT = os.getenv("MARIA_PORT" )
MARIA_USER = os.getenv("MARIA_USER")
MARIA_PASS = os.getenv("MARIA_PASS")
MARIA_DB   = os.getenv("MARIA_DB")

def require_env(name: str, value: str | None) -> str:
    if value is None or str(value).strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

# Validate required vars
PG_HOST = require_env("PG_HOST", PG_HOST)
PG_DB   = require_env("PG_DB", PG_DB)
PG_USER = require_env("PG_USER", PG_USER)
PG_PASS = require_env("PG_PASS", PG_PASS)

MARIA_HOST = require_env("MARIA_HOST", MARIA_HOST)
MARIA_USER = require_env("MARIA_USER", MARIA_USER)
MARIA_PASS = require_env("MARIA_PASS", MARIA_PASS)
MARIA_DB   = require_env("MARIA_DB", MARIA_DB)

# Cast ports
PG_PORT = int(PG_PORT)
MARIA_PORT = int(MARIA_PORT)

print(f"[INFO] Connecting to Postgres {PG_HOST}:{PG_PORT}/{PG_DB}")
print(f"[INFO] Connecting to MariaDB  {MARIA_HOST}:{MARIA_PORT}/{MARIA_DB}")

# ---------------------------------------------------------
# HELPER: cast tinyint -> boolean
# ---------------------------------------------------------
def to_bool(val):
    if val is None:
        return None
    return bool(val)

# ---------------------------------------------------------
# MARIADB CONNECTION (FROM .env)
# ---------------------------------------------------------
maria_conn = mysql.connector.connect(
    host=MARIA_HOST,
    port=MARIA_PORT,
    user=MARIA_USER,
    password=MARIA_PASS,
    database=MARIA_DB,
)
maria_cursor = maria_conn.cursor(dictionary=True)
print("[INFO] Connected to MariaDB")

# ---------------------------------------------------------
# UNION ALL QUERY FOR PARTNER TABLES
# ---------------------------------------------------------
union_sql = """
    SELECT 
        'Conveyancing' AS partner_type,
        id,
        uuid,
        company_name,
        display_name,
        username,
        lead_status,
        active,
        account_type,
        payg_method,
        http_referrer,
        extra_services,
        created_at,
        updated_at,
        deleted_at,
        service_id,
        b2b_user_id,
        promo_code_id
    FROM compmove_core.conveyancing_companies

    UNION ALL

    SELECT 
        'Removals',
        id,
        uuid,
        company_name,
        display_name,
        username,
        lead_status,
        active,
        account_type,
        payg_method,
        http_referrer,
        extra_services,
        created_at,
        updated_at,
        deleted_at,
        service_id,
        b2b_user_id,
        promo_code_id
    FROM compmove_core.removal_companies

    UNION ALL

    SELECT 
        'Surveying',
        id,
        uuid,
        company_name,
        display_name,
        username,
        lead_status,
        active,
        account_type,
        payg_method,
        http_referrer,
        extra_services,
        created_at,
        updated_at,
        deleted_at,
        service_id,
        b2b_user_id,
        promo_code_id
    FROM compmove_core.surveying_companies

    UNION ALL

    SELECT 
        'Financial',
        id,
        uuid,
        company_name,
        display_name,
        username,
        lead_status,
        active,
        account_type,
        payg_method,
        http_referrer,
        extra_services,
        created_at,
        updated_at,
        deleted_at,
        service_id,
        b2b_user_id,
        promo_code_id
    FROM compmove_core.companies
"""

maria_cursor.execute(union_sql)
rows = maria_cursor.fetchall()
print(f"[INFO] Fetched {len(rows)} rows from MariaDB")

# ---------------------------------------------------------
# POSTGRES CONNECTION
# ---------------------------------------------------------
pg_conn = psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    database=PG_DB,
    user=PG_USER,
    password=PG_PASS,
)
pg_cursor = pg_conn.cursor()
print("[INFO] Connected to PostgreSQL")

# ---------------------------------------------------------
# UPSERT QUERY
# ---------------------------------------------------------
upsert_sql = """
INSERT INTO crm_data.partner_master_companies (
    partner_type,
    id,
    uuid,
    company_name,
    display_name,
    username,
    lead_status,
    active,
    account_type,
    payg_method,
    http_referrer,
    extra_services,
    created_at,
    updated_at,
    deleted_at,
    service_id,
    b2b_user_id,
    promo_code_id,
    loaded_at
)
VALUES %s
ON CONFLICT (partner_type, id)
DO UPDATE SET
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
    loaded_at = NOW();
"""

# ---------------------------------------------------------
# FORMAT DATA
# ---------------------------------------------------------
data = [
    (
        r["partner_type"],
        r["id"],
        r["uuid"],
        r["company_name"],
        r["display_name"],
        r["username"],
        r["lead_status"],
        to_bool(r["active"]),
        r["account_type"],
        r["payg_method"],
        r["http_referrer"],
        r["extra_services"],
        r["created_at"],
        r["updated_at"],
        r["deleted_at"],
        r["service_id"],
        r["b2b_user_id"],
        r["promo_code_id"],
        None,  # loaded_at set by NOW() in SQL
    )
    for r in rows
]

# ---------------------------------------------------------
# EXECUTE UPSERT
# ---------------------------------------------------------
print("[INFO] Running UPSERT...")
psycopg2.extras.execute_values(pg_cursor, upsert_sql, data)
pg_conn.commit()
print(f"[SUCCESS] Upserted {len(data)} rows into crm_data.partner_master_companies")

# ---------------------------------------------------------
# CLOSE
# ---------------------------------------------------------
pg_cursor.close()
pg_conn.close()
maria_cursor.close()
maria_conn.close()
print("[DONE] Daily upsert ETL completed successfully.")