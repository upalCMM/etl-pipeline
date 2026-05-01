#!/usr/bin/env python3
"""
ETL Script - Partner Company Logs from MariaDB to PostgreSQL

Fixes:
- Correctly handles loaded_at by setting it in SQL as NOW()
  (so VALUES tuples stay at 16 fields and the INSERT has 17 columns).
- Uses server-side cursor for MariaDB to avoid loading 1.1M+ rows in memory.
- Streams + upserts in batches.

Requirements:
- mysql-connector-python
- psycopg2-binary
- python-dotenv

.env expected at: ../.env (one level above this script directory)
PG_* variables:
  PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS

MariaDB variables (optional, have fallbacks):
  MARIA_HOST, MARIA_PORT, MARIA_USER, MARIA_PASS, MARIA_DB
"""

import os
import sys
from datetime import datetime
from dotenv import load_dotenv
import mysql.connector
import psycopg2
import psycopg2.extras


# ---------------------------
# .env loading (project-root)
# ---------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(SCRIPT_DIR, "..", ".env")

if not os.path.exists(ENV_PATH):
    print(f"[ERROR] .env file not found at {ENV_PATH}")
    raise SystemExit(1)

load_dotenv(dotenv_path=ENV_PATH)


# ---------------------------
# Config
# ---------------------------
# PostgreSQL
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB   = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")

missing_pg = [k for k, v in {
    "PG_HOST": PG_HOST,
    "PG_DB": PG_DB,
    "PG_USER": PG_USER,
    "PG_PASS": PG_PASS,
}.items() if not v]

if missing_pg:
    print(f"[ERROR] Missing required PG env vars in {ENV_PATH}: {', '.join(missing_pg)}")
    raise SystemExit(1)

# MariaDB
MARIA_HOST = os.getenv("MARIA_HOST")
MARIA_PORT = int(os.getenv("MARIA_PORT"))
MARIA_USER = os.getenv("MARIA_USER")
MARIA_PASS = os.getenv("MARIA_PASS")
MARIA_DB   = os.getenv("MARIA_DB")


# ---------------------------
# Helpers
# ---------------------------
def to_bool(val):
    """Convert various truthy/falsy MariaDB values to Python bool."""
    if val is None:
        return None
    # mysql-connector can return int(0/1), bool, or bytes sometimes
    if isinstance(val, (bytes, bytearray)):
        try:
            val = val.decode("utf-8")
        except Exception:
            pass
    if isinstance(val, str):
        v = val.strip().lower()
        if v in ("1", "true", "t", "yes", "y"):
            return True
        if v in ("0", "false", "f", "no", "n", ""):
            return False
    return bool(val)


UNION_SQL = """
SELECT
    'Conveyancing' AS partner_type,
    id,
    company_id,
    user_id,
    url,
    section,
    action,
    data,
    notes,
    defcon,
    active,
    created_at,
    obfuscated_at,
    human,
    NULL AS service_id,
    NULL AS previous_id
FROM compmove_core.conveyancing_company_logs

UNION ALL

SELECT
    'Removals' AS partner_type,
    id,
    company_id,
    user_id,
    url,
    section,
    action,
    data,
    notes,
    defcon,
    active,
    created_at,
    obfuscated_at,
    human,
    NULL AS service_id,
    NULL AS previous_id
FROM compmove_core.removal_company_logs

UNION ALL

SELECT
    'Surveying' AS partner_type,
    id,
    company_id,
    user_id,
    url,
    section,
    action,
    data,
    notes,
    defcon,
    active,
    created_at,
    obfuscated_at,
    human,
    NULL AS service_id,
    NULL AS previous_id
FROM compmove_core.surveying_company_logs

UNION ALL

SELECT
    'Financial' AS partner_type,
    id,
    company_id,
    user_id,
    url,
    section,
    action,
    data,
    notes,
    defcon,
    active,
    created_at,
    obfuscated_at,
    human,
    service_id,
    previous_id
FROM compmove_core.company_logs
"""

# IMPORTANT: loaded_at is provided by NOW() in the template
UPSERT_SQL = """
INSERT INTO crm_data.partner_company_logs (
    partner_type,
    id,
    company_id,
    user_id,
    url,
    section,
    action,
    data,
    notes,
    defcon,
    active,
    created_at,
    obfuscated_at,
    human,
    service_id,
    previous_id,
    loaded_at
)
VALUES %s
ON CONFLICT (partner_type, id)
DO UPDATE SET
    company_id = EXCLUDED.company_id,
    user_id = EXCLUDED.user_id,
    url = EXCLUDED.url,
    section = EXCLUDED.section,
    action = EXCLUDED.action,
    data = EXCLUDED.data,
    notes = EXCLUDED.notes,
    defcon = EXCLUDED.defcon,
    active = EXCLUDED.active,
    created_at = EXCLUDED.created_at,
    obfuscated_at = EXCLUDED.obfuscated_at,
    human = EXCLUDED.human,
    service_id = EXCLUDED.service_id,
    previous_id = EXCLUDED.previous_id,
    loaded_at = NOW();
"""

# 16 placeholders + NOW() for loaded_at
VALUES_TEMPLATE = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())"


def main():
    print(f"\n{'='*60}")
    print(f"PARTNER COMPANY LOGS ETL - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    print(f"Source: MariaDB ({MARIA_HOST}:{MARIA_PORT}/{MARIA_DB})")
    print(f"Target: PostgreSQL ({PG_HOST}:{PG_PORT}/{PG_DB})")
    print(f"{'='*60}")

    start_time = datetime.now()

    maria_conn = None
    maria_cursor = None
    pg_conn = None
    pg_cursor = None

    # Tune these as needed
    FETCH_BATCH_SIZE = 10_000   # rows fetched from MariaDB per round-trip
    UPSERT_PAGE_SIZE = 2_000    # rows per execute_values page
    COMMIT_EVERY = 20_000       # commit after this many rows inserted

    try:
        # ---------------------------
        # Connect to MariaDB (streaming)
        # ---------------------------
        print(f"\n[1/4] Connecting to MariaDB...")
        maria_conn = mysql.connector.connect(
            host=MARIA_HOST,
            port=MARIA_PORT,
            user=MARIA_USER,
            password=MARIA_PASS,
            database=MARIA_DB,
        )

        # Unbuffered cursor = stream results (avoid loading all rows)
        maria_cursor = maria_conn.cursor(dictionary=True, buffered=False)
        print("✅ Connected to MariaDB")

        # ---------------------------
        # Execute UNION ALL query
        # ---------------------------
        print(f"\n[2/4] Executing UNION ALL query (streaming)...")
        maria_cursor.execute(UNION_SQL)
        print("✅ Query started")

        # ---------------------------
        # Connect to PostgreSQL
        # ---------------------------
        print(f"\n[3/4] Connecting to PostgreSQL...")
        pg_conn = psycopg2.connect(
            host=PG_HOST,
            port=int(PG_PORT),
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASS,
        )
        pg_conn.autocommit = False
        pg_cursor = pg_conn.cursor()
        print("✅ Connected to PostgreSQL")

        # ---------------------------
        # Stream + upsert in batches
        # ---------------------------
        print(f"\n[4/4] Streaming rows and upserting...")

        total_processed = 0
        total_upserted = 0

        while True:
            batch = maria_cursor.fetchmany(FETCH_BATCH_SIZE)
            if not batch:
                break

            data = []
            for r in batch:
                try:
                    data.append((
                        r.get("partner_type"),
                        r.get("id"),
                        r.get("company_id"),
                        r.get("user_id"),
                        r.get("url"),
                        r.get("section"),
                        r.get("action"),
                        r.get("data"),
                        r.get("notes"),
                        r.get("defcon"),
                        to_bool(r.get("active")),
                        r.get("created_at"),
                        r.get("obfuscated_at"),
                        to_bool(r.get("human")),
                        r.get("service_id"),
                        r.get("previous_id"),
                    ))
                except Exception as e:
                    # Skip bad rows but continue
                    print(f"  ⚠️ Skipping row id={r.get('id')}: {e}")

            if not data:
                continue

            psycopg2.extras.execute_values(
                pg_cursor,
                UPSERT_SQL,
                data,
                template=VALUES_TEMPLATE,
                page_size=UPSERT_PAGE_SIZE,
            )

            total_processed += len(batch)
            total_upserted += len(data)

            # Commit periodically
            if total_upserted % COMMIT_EVERY < len(data):
                pg_conn.commit()
                print(f"✅ Committed. Processed={total_processed:,} Upserted={total_upserted:,}")

        # Final commit
        pg_conn.commit()
        print(f"✅ Final commit. Total processed={total_processed:,} Total upserted={total_upserted:,}")

        # Final count
        pg_cursor.execute("SELECT COUNT(*) FROM crm_data.partner_company_logs")
        total = pg_cursor.fetchone()[0]
        print(f"📊 Total rows in table: {total:,}")

        # Summary
        end_time = datetime.now()
        duration = end_time - start_time

        print(f"\n{'='*60}")
        print("✅ ETL COMPLETED SUCCESSFULLY")
        print(f"{'='*60}")
        print(f"Start: {start_time.strftime('%H:%M:%S')}")
        print(f"End:   {end_time.strftime('%H:%M:%S')}")
        print(f"Duration: {duration}")
        print(f"Rows upserted: {total_upserted:,}")
        print(f"{'='*60}")

        return True

    except mysql.connector.Error as e:
        print(f"\n❌ MariaDB Error: {e}")
        if pg_conn:
            pg_conn.rollback()
        return False
    except psycopg2.Error as e:
        print(f"\n❌ PostgreSQL Error: {e}")
        if pg_conn:
            pg_conn.rollback()
        return False
    except Exception as e:
        print(f"\n❌ Unexpected Error: {e}")
        if pg_conn:
            pg_conn.rollback()
        return False
    finally:
        try:
            if pg_cursor:
                pg_cursor.close()
            if pg_conn:
                pg_conn.close()
        except Exception:
            pass
        try:
            if maria_cursor:
                maria_cursor.close()
            if maria_conn:
                maria_conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        ok = main()
        raise SystemExit(0 if ok else 1)
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user")
        raise SystemExit(2)
    except Exception as e:
        print(f"\n💥 Fatal error: {e}")
        raise SystemExit(1)