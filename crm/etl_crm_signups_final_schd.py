#!/usr/bin/env python3
"""
DAILY TRUE UPSERT: Signups from MariaDB to PostgreSQL (non-PII).

What it does:
- Extracts signups from MariaDB (compmove_core.signups) with LEFT JOIN enrichment
- Excludes PII: contact_name, email, phone (not selected, not stored)
- Filters out soft-deleted rows (deleted_at IS NULL)
- Incremental based on change_ts = GREATEST(created_at, COALESCE(updated_at, created_at))
- TRUE UPSERT into Postgres: ON CONFLICT (id) DO UPDATE

Notes:
- MariaDB connection defaults match your WORKING leads script (host 127.0.0.1, port 53080, user/pass db/db).
- Extract failures are FATAL (script exits non-zero).
"""

import os
import sys
from datetime import datetime, date
from dotenv import load_dotenv
import mysql.connector
import psycopg2
import psycopg2.extras

# ------------------------------------------------------------------
# ENV - FIXED VERSION
# ------------------------------------------------------------------
# Get the absolute path to .env (one level up from script)
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')

if not os.path.exists(env_path):
    print(f"[FATAL] .env not found at {env_path}")
    sys.exit(1)

load_dotenv(dotenv_path=env_path)

# PostgreSQL (target)
DB_HOST = os.getenv("PG_HOST")
DB_NAME = os.getenv("PG_DB")
DB_USER = os.getenv("PG_USER")
DB_PASSWORD = os.getenv("PG_PASS")
DB_PORT = os.getenv("PG_PORT")

# MariaDB/MySQL (source) — defaults aligned with your leads script
MARIA_HOST = os.getenv("MARIA_HOST")
MARIA_PORT = os.getenv("MARIA_PORT")
MARIA_USER = os.getenv("MARIA_USER")
MARIA_PASS = os.getenv("MARIA_PASS")
MARIA_DB = os.getenv("MARIA_DB")

# Incremental watermark start (set earlier for backfill)
START_DATE = date(2024, 1, 1)

# ------------------------------------------------------------------
# EXTRACT
# ------------------------------------------------------------------
def extract_signups():
    """
    Extract signups changed since START_DATE, excluding deleted rows.
    Uses LEFT JOINs so missing dimension rows don't drop signups.
    Returns:
      - list[dict] on success
      - None on fatal error
    """
    try:
        conn = mysql.connector.connect(
            host=MARIA_HOST,
            port=int(MARIA_PORT),
            user=MARIA_USER,
            password=MARIA_PASS,
            database=MARIA_DB,
        )
        cursor = conn.cursor(dictionary=True)

        # Optional sanity check (prints one row)
        cursor.execute("SELECT @@hostname AS host, DATABASE() AS db, VERSION() AS version")
        print(f"[INFO] Connected to MariaDB: {cursor.fetchone()}")

        query = """
        SELECT
            sg.id,
            sg.previous_id,
            sg.service_id,
            sg.company_name,
            sg.username,
            sg.token,
            sg.signup_email_state_id,
            sg.website,
            sg.state_id,
            sg.reason_id,
            sg.start_when_id,
            sg.regions_covered,
            sg.also_interested,
            sg.heard_about_source_id,
            sg.additional_details,
            sg.promo_code_id,
            sg.notes,
            sg.type,
            sg.http_referrer,
            sg.referrer_source_id,
            sg.created_at,
            sg.updated_at,
            sg.company_id,
            sg.completed_at,
            sg.deleted_at,

            sc.name  AS service,
            st.name  AS status,
            wh.name  AS start_status,
            hd.name  AS source,
            ref.name AS referrer_source,

            GREATEST(sg.created_at, COALESCE(sg.updated_at, sg.created_at)) AS change_ts

        FROM compmove_core.signups sg
        LEFT JOIN compmove_core.services sc
            ON sg.service_id = sc.id
        LEFT JOIN compmove_core.signup_states st
            ON sg.state_id = st.id
        LEFT JOIN compmove_core.signup_start_when wh
            ON sg.start_when_id = wh.id
        LEFT JOIN compmove_core.heard_about_sources hd
            ON sg.heard_about_source_id = hd.id
        LEFT JOIN compmove_core.referrer_sources ref
            ON sg.referrer_source_id = ref.id
        WHERE GREATEST(sg.created_at, COALESCE(sg.updated_at, sg.created_at)) >= %s
          AND sg.deleted_at IS NULL
        ORDER BY change_ts DESC
        """

        cursor.execute(query, (START_DATE,))

        rows = []
        batch_size = 10000
        total = 0

        while True:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break
            rows.extend(batch)
            total += len(batch)
            if total % 50000 == 0:
                print(f"  Extracted {total:,} rows...")

        cursor.close()
        conn.close()

        print(f"[INFO] Total extracted (changed since {START_DATE}): {len(rows):,}")
        return rows

    except Exception as e:
        print(f"[FATAL] Extract failed: {e}")
        return None

# ------------------------------------------------------------------
# TRANSFORM
# ------------------------------------------------------------------
def transform(rows):
    """
    Convert extracted dict rows into tuples matching the INSERT column order.
    """
    data = []
    for r in rows:
        data.append((
            r["id"],
            r.get("previous_id"),
            r.get("service_id"),
            r.get("company_name"),
            r.get("username"),
            r.get("token"),
            r.get("signup_email_state_id"),
            r.get("website"),
            r.get("state_id"),
            r.get("reason_id"),
            r.get("start_when_id"),
            r.get("regions_covered"),
            r.get("also_interested"),
            r.get("heard_about_source_id"),
            r.get("additional_details"),
            r.get("promo_code_id"),
            r.get("notes"),
            r.get("type"),
            r.get("http_referrer"),
            r.get("referrer_source_id"),
            r.get("company_id"),
            r.get("completed_at"),
            r.get("created_at"),
            r.get("updated_at"),
            r.get("deleted_at"),
            r.get("service"),
            r.get("status"),
            r.get("start_status"),
            r.get("source"),
            r.get("referrer_source"),
            r.get("change_ts"),
        ))
    return data

# ------------------------------------------------------------------
# LOAD (TRUE UPSERT)
# ------------------------------------------------------------------
def upsert_postgres(data):
    if not data:
        print("[INFO] No changed rows to upsert")
        return True

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        cur = conn.cursor()

        sql = """
        INSERT INTO crm_data.signups (
            id, previous_id, service_id, company_name, username, token,
            signup_email_state_id, website, state_id, reason_id,
            start_when_id, regions_covered, also_interested,
            heard_about_source_id, additional_details, promo_code_id,
            notes, type, http_referrer, referrer_source_id,
            company_id, completed_at, created_at, updated_at, deleted_at,
            service, status, start_status, source, referrer_source,
            change_ts
        ) VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            previous_id = EXCLUDED.previous_id,
            service_id = EXCLUDED.service_id,
            company_name = EXCLUDED.company_name,
            username = EXCLUDED.username,
            token = EXCLUDED.token,
            signup_email_state_id = EXCLUDED.signup_email_state_id,
            website = EXCLUDED.website,
            state_id = EXCLUDED.state_id,
            reason_id = EXCLUDED.reason_id,
            start_when_id = EXCLUDED.start_when_id,
            regions_covered = EXCLUDED.regions_covered,
            also_interested = EXCLUDED.also_interested,
            heard_about_source_id = EXCLUDED.heard_about_source_id,
            additional_details = EXCLUDED.additional_details,
            promo_code_id = EXCLUDED.promo_code_id,
            notes = EXCLUDED.notes,
            type = EXCLUDED.type,
            http_referrer = EXCLUDED.http_referrer,
            referrer_source_id = EXCLUDED.referrer_source_id,
            company_id = EXCLUDED.company_id,
            completed_at = EXCLUDED.completed_at,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at,
            deleted_at = EXCLUDED.deleted_at,
            service = EXCLUDED.service,
            status = EXCLUDED.status,
            start_status = EXCLUDED.start_status,
            source = EXCLUDED.source,
            referrer_source = EXCLUDED.referrer_source,
            change_ts = EXCLUDED.change_ts
        """

        psycopg2.extras.execute_values(
            cur,
            sql,
            data,
            page_size=1000,
        )
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM crm_data.signups")
        total = cur.fetchone()[0]

        cur.close()
        conn.close()

        print(f"[SUCCESS] Upserted {len(data):,} rows")
        print(f"[INFO] Total rows in crm_data.signups: {total:,}")
        return True

    except Exception as e:
        print(f"[FATAL] Upsert failed: {e}")
        return False

# ------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------
def main():
    print("=" * 70)
    print(f"SIGNUPS TRUE UPSERT START — {datetime.now()}")
    print("=" * 70)
    print(f"Source: MariaDB ({MARIA_HOST}:{MARIA_PORT}/{MARIA_DB})")
    print(f"Target: PostgreSQL ({DB_HOST}:{DB_PORT}/{DB_NAME})")
    print(f"Incremental: change_ts >= {START_DATE}")
    print("PII excluded: contact_name, email, phone")
    print("=" * 70)

    rows = extract_signups()
    if rows is None:
        # Fatal extract error
        return False

    data = transform(rows)
    ok = upsert_postgres(data)
    return ok

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user")
        sys.exit(2)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)