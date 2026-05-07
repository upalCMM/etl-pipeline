import os
import time
import random
import requests
import psycopg2
import pandas as pd
import urllib3

from dotenv import load_dotenv
from datetime import datetime

# =========================================================
# REMOVE SSL WARNINGS
# =========================================================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================================================
# ENV
# =========================================================
load_dotenv()

API_TOKEN = os.getenv("PIPEDRIVE_API_TOKEN")

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB   = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")

BASE_URL = "https://comparemymove.pipedrive.com/api/v1"

TARGET_STAGES = {"325", "326"}

DRY_RUN = False

REQUEST_DELAY = 0.4
MAX_RETRIES = 5

# =========================================================
# DB CONNECTION
# =========================================================
def get_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS
    )

# =========================================================
# LOAD OUTBOUND DEALS
# =========================================================
def load_outbound_deals():

    sql = """
        SELECT DISTINCT deal_id
        FROM pipedrive.fact_partner_stu_ttu_deals
        WHERE sales_created_date IS NULL
          AND channel_name = 'Outbound Data Scrape'
          AND pipeline_name IN ('NEW SALES PIPELINE', 'ONBOARDING - SALES')
          AND deal_status IN ('Won','Active')
        ORDER BY deal_id
    """

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(sql)

    deals = [r[0] for r in cur.fetchall()]

    cur.close()
    conn.close()

    print(f"Loaded {len(deals)} outbound deals")

    return deals

# =========================================================
# FETCH FLOW
# =========================================================
def fetch_flow(deal_id):

    start = 0
    limit = 50
    all_items = []

    while True:

        time.sleep(REQUEST_DELAY)

        params = {
            "api_token": API_TOKEN,
            "start": start,
            "limit": limit,
            "items": "change",
            "all_changes": 1
        }

        for attempt in range(MAX_RETRIES):

            try:

                r = requests.get(
                    f"{BASE_URL}/deals/{deal_id}/flow",
                    params=params,
                    timeout=20,
                    verify=False
                )

                if r.status_code == 200:
                    data = r.json()
                    break

                if r.status_code == 429:

                    sleep_time = (2 ** attempt) + random.random()

                    print(
                        f"Rate limited {deal_id} "
                        f"sleeping {sleep_time:.2f}s"
                    )

                    time.sleep(sleep_time)
                    continue

                print(f"API error {r.status_code} for deal {deal_id}")
                return []

            except Exception as e:

                print(f"Request error {deal_id}: {e}")

                time.sleep(
                    (2 ** attempt) + random.random()
                )

        else:
            return []

        items = data.get("data", [])

        if not items:
            break

        all_items.extend(items)

        pagination = (
            data.get("additional_data", {})
                .get("pagination", {})
        )

        if not pagination.get("more_items_in_collection"):
            break

        start += limit

    return all_items

# =========================================================
# EXTRACT EARLIEST TARGET STAGE DATE
# =========================================================
def extract_date(flow):

    matches = []

    for e in flow:

        if e.get("object") != "dealChange":
            continue

        d = e.get("data", {})

        if d.get("field_key") == "stage_id":

            if str(d.get("new_value")) in TARGET_STAGES:

                ts = e.get("timestamp")

                if ts:
                    matches.append(ts)

    if not matches:
        return None

    earliest = min(matches)

    return datetime.fromisoformat(
        earliest.replace("Z", "")
    ).date()

# =========================================================
# UPDATE OUTBOUND DEALS
# =========================================================
def update_outbound(results):

    if not results:
        print("No outbound updates")
        return 0

    conn = get_conn()
    cur = conn.cursor()

    sql = """
        UPDATE pipedrive.fact_partner_stu_ttu_deals
        SET sales_created_date = %s
        WHERE deal_id = %s
          AND sales_created_date IS NULL
    """

    cur.executemany(
        sql,
        [(r["date"], r["deal_id"]) for r in results]
    )

    updated = cur.rowcount

    if DRY_RUN:

        conn.rollback()
        print("DRY RUN rollback")

    else:

        conn.commit()
        print(f"Committed outbound updates: {updated}")

    cur.close()
    conn.close()

    return updated

# =========================================================
# FALLBACK UPDATE
# NON OUTBOUND -> created_date
# =========================================================
def update_non_outbound():

    conn = get_conn()
    cur = conn.cursor()

    sql = """
        UPDATE pipedrive.fact_partner_stu_ttu_deals
        SET sales_created_date = created_date
        WHERE sales_created_date IS NULL
          AND channel_name <> 'Outbound Data Scrape'
          AND created_date IS NOT NULL
    """

    cur.execute(sql)

    updated = cur.rowcount

    if DRY_RUN:

        conn.rollback()
        print("DRY RUN rollback")

    else:

        conn.commit()
        print(f"Committed fallback updates: {updated}")

    cur.close()
    conn.close()

    return updated

# =========================================================
# EXPORT REMAINING NULLS
# =========================================================
def export_remaining_nulls():

    sql = """
        SELECT
            deal_id,
            deal_title,
            pipeline_name,
            channel_name,
            deal_status
        FROM pipedrive.fact_partner_stu_ttu_deals
        WHERE sales_created_date IS NULL
          AND pipeline_name IN (
                'NEW SALES PIPELINE',
                'ONBOARDING - SALES'
          )
        ORDER BY deal_id
    """

    conn = get_conn()

    df = pd.read_sql_query(sql, conn)

    conn.close()

    filename = (
        "remaining_null_sales_created_date_"
        f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )

    df.to_csv(filename, index=False)

    print(f"CSV exported: {filename}")

    return filename, len(df)

# =========================================================
# MAIN
# =========================================================
def main():

    start_time = datetime.now()

    print("=" * 80)
    print("PIPELINE START")
    print("DRY_RUN =", DRY_RUN)
    print("=" * 80)

    # =====================================================
    # STEP 1 - OUTBOUND API MATCHING
    # =====================================================

    deals = load_outbound_deals()

    results = []

    for i, deal_id in enumerate(deals, 1):

        flow = fetch_flow(deal_id)

        date = extract_date(flow)

        if date:

            results.append({
                "deal_id": deal_id,
                "date": date
            })

            print(f"[{i}] {deal_id} -> {date}")

        else:

            print(f"[{i}] {deal_id} -> no match")

    print("=" * 80)
    print("OUTBOUND MATCHES:", len(results))
    print("=" * 80)

    outbound_updates = update_outbound(results)

    # =====================================================
    # STEP 2 - NON OUTBOUND FALLBACK
    # =====================================================

    fallback_updates = update_non_outbound()

    # =====================================================
    # STEP 3 - EXPORT REMAINING NULLS
    # =====================================================

    csv_file, remaining_count = export_remaining_nulls()

    # =====================================================
    # COMPLETE
    # =====================================================

    end_time = datetime.now()

    print("=" * 80)
    print("DONE")
    print(f"Outbound updates : {outbound_updates}")
    print(f"Fallback updates : {fallback_updates}")
    print(f"Remaining NULLs  : {remaining_count}")
    print(f"CSV File         : {csv_file}")
    print(f"Started          : {start_time}")
    print(f"Finished         : {end_time}")
    print("=" * 80)

# =========================================================
# RUN
# =========================================================
if __name__ == "__main__":
    main()