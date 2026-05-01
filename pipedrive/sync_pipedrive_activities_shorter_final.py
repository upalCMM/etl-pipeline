# ===== UNICODE FIX FOR WINDOWS =====
import sys
import io
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
# ==================================================
sys.path.append("..")

import requests
from sqlalchemy import create_engine, text
import logging
import urllib3
from datetime import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

from config import PIPEDRIVE_API_TOKEN, PIPEDRIVE_BASE_URL, DB_CONFIG

# -------------------------------------------------------------------
# If you still want to cap history, keep START_DATE.
# If you truly want "all time", set START_DATE = None.
# -------------------------------------------------------------------
START_DATE = datetime(2025, 1, 1)
END_DATE = datetime.now()


def fetch_all_users():
    """Fetch all users from Pipedrive (paged)."""
    print("👥 Fetching all users...")
    users = []
    start = 0
    limit = 500

    while True:
        url = (
            f"{PIPEDRIVE_BASE_URL}/users"
            f"?api_token={PIPEDRIVE_API_TOKEN}&start={start}&limit={limit}"
        )

        try:
            response = requests.get(url, verify=False, timeout=60)
            data = response.json()

            if not data.get("success"):
                print(f"API error: {data.get('error', 'Unknown error')}")
                break

            batch_users = data.get("data", []) or []
            users.extend(batch_users)
            print(f"📥 Fetched {len(batch_users)} users (Total: {len(users)})")

            pagination = data.get("additional_data", {}).get("pagination", {})
            if not pagination.get("more_items_in_collection", False):
                break

            start = pagination.get("next_start", start + limit)

        except Exception as e:
            print(f"Request failed: {e}")
            break

    print(f"✅ Total users: {len(users)}")
    return users


def fetch_activities_by_user(user_id: int, user_name: str = "Unknown"):
    """
    Fetch activities for a specific user (paged).
    If START_DATE is set, restrict to [START_DATE, END_DATE).
    """
    all_activities = []
    start = 0
    limit = 500

    if START_DATE:
        print(f"👤 Fetching activities for {user_name} (ID: {user_id}) since {START_DATE.strftime('%Y-%m-%d')}...")
    else:
        print(f"👤 Fetching activities for {user_name} (ID: {user_id}) (all time)...")

    while True:
        url = (
            f"{PIPEDRIVE_BASE_URL}/activities"
            f"?api_token={PIPEDRIVE_API_TOKEN}"
            f"&user_id={user_id}"
            f"&start={start}&limit={limit}"
        )

        # Date filter (optional)
        if START_DATE:
            url += f"&since={START_DATE.strftime('%Y-%m-%d')}&until={END_DATE.strftime('%Y-%m-%d')}"

        try:
            response = requests.get(url, verify=False, timeout=60)
            data = response.json()

            if not data.get("success"):
                print(f"API error for user {user_id}: {data.get('error', 'Unknown error')}")
                break

            activities = data.get("data", []) or []
            all_activities.extend(activities)
            print(f"   📥 +{len(activities)} (Total: {len(all_activities)})")

            pagination = data.get("additional_data", {}).get("pagination", {})
            if not pagination.get("more_items_in_collection", False):
                break

            start = pagination.get("next_start", start + limit)

        except Exception as e:
            print(f"Request failed for user {user_id}: {e}")
            break

    return all_activities


def sync_activities_to_database(engine, activities):
    """Sync activities to database using UPSERT."""
    print(f"\n💾 Syncing {len(activities)} activities to database...")

    existing_columns = check_table_schema(engine)
    if not existing_columns or "id" not in existing_columns:
        print("❌ Activities table not found or missing 'id' column")
        return False

    upsert_query, available_columns = build_upsert_query(existing_columns)

    with engine.connect() as conn:
        trans = conn.begin()
        try:
            count_before = conn.execute(
                text("SELECT COUNT(*) FROM pipedrive.fact_pipedrive_activities")
            ).scalar()
            print(f"📊 Activities in database before: {count_before:,}")

            inserted_count = 0
            updated_count = 0
            error_count = 0

            batch_size = 500
            total_batches = (len(activities) + batch_size - 1) // batch_size

            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(activities))
                batch_activities = activities[start_idx:end_idx]

                print(f"   Processing batch {batch_num + 1}/{total_batches} ({len(batch_activities)} activities)...")

                batch_inserted = 0
                batch_updated = 0

                for activity in batch_activities:
                    try:
                        activity_data = extract_activity_data(activity)
                        if activity_data.get("id") is None:
                            continue

                        filtered_data = {k: v for k, v in activity_data.items() if k in available_columns}

                        # Keep existence check so you retain "inserted vs updated" counts
                        existing_record = conn.execute(
                            text("SELECT 1 FROM pipedrive.fact_pipedrive_activities WHERE id = :id"),
                            {"id": activity_data["id"]},
                        ).fetchone()

                        conn.execute(text(upsert_query), filtered_data)

                        if existing_record:
                            batch_updated += 1
                        else:
                            batch_inserted += 1

                    except Exception as e:
                        error_count += 1
                        # If you want visibility, uncomment:
                        # print(f"   ❌ Error on activity id={activity.get('id')}: {e}")

                trans.commit()
                if batch_num < total_batches - 1:
                    trans = conn.begin()

                inserted_count += batch_inserted
                updated_count += batch_updated
                print(f"     ✅ Batch {batch_num + 1}: +{batch_inserted} new, ~{batch_updated} updated")

            count_after = conn.execute(
                text("SELECT COUNT(*) FROM pipedrive.fact_pipedrive_activities")
            ).scalar()

            print("\n🎉 Sync completed:")
            print(f"   - New activities: {inserted_count:,}")
            print(f"   - Updated activities: {updated_count:,}")
            print(f"   - Errors: {error_count:,}")
            print(f"   - Total in database: {count_after:,} (was {count_before:,})")

            return True

        except Exception as e:
            trans.rollback()
            print(f"❌ Sync failed: {e}")
            return False


def update_pipedrive_activities():
    """
    Main function: fetch activities USER-BY-USER (paged), de-duplicate, and sync to Postgres.
    This removes the month-by-month date strategy entirely.
    """
    print("🚀 Starting Pipedrive activities update (user-by-user)...")
    print("=" * 60)

    connection_string = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    engine = create_engine(connection_string)

    users = fetch_all_users()
    if not users:
        print("❌ No users returned; aborting.")
        return False

    all_activities = []

    for idx, user in enumerate(users, start=1):
        user_id = user.get("id")
        user_name = user.get("name", "Unknown")

        if not user_id:
            continue

        print(f"\n[{idx}/{len(users)}] {user_name} (ID: {user_id})")
        user_activities = fetch_activities_by_user(int(user_id), user_name)
        all_activities.extend(user_activities)

    # De-duplicate across users by activity id
    unique_activities = {a["id"]: a for a in all_activities if a and a.get("id") is not None}

    print("\n📊 Combined results:")
    print(f"   - Total activities fetched: {len(all_activities):,}")
    print(f"   - Unique activities: {len(unique_activities):,}")

    # Optional: show top 10 user breakdown
    user_breakdown = {}
    for activity in unique_activities.values():
        uid = activity.get("user_id")
        if isinstance(uid, dict):
            uid = uid.get("value")
        if uid is not None:
            user_breakdown[int(uid)] = user_breakdown.get(int(uid), 0) + 1

    print("\n👥 Activities per user (top 10):")
    for uid, cnt in sorted(user_breakdown.items(), key=lambda x: x[1], reverse=True)[:10]:
        uname = next((u.get("name", "Unknown") for u in users if u.get("id") == uid), "Unknown")
        print(f"   - {uname}: {cnt:,} activities")

    if not unique_activities:
        print("❌ No activities to sync.")
        return False

    success = sync_activities_to_database(engine, list(unique_activities.values()))
    if success:
        print("\n✅ Update completed successfully!")
        return True

    print("\n❌ Update failed.")
    return False


# -----------------------------
# Helper functions
# -----------------------------
def extract_activity_data(activity):
    """Extract and transform activity data for DB."""
    user_data = activity.get("user_id", {})
    org_data = activity.get("org_id", {})
    deal_data = activity.get("deal_id", {})

    user_id = user_data.get("value") if isinstance(user_data, dict) else user_data
    org_id = org_data.get("value") if isinstance(org_data, dict) else org_data
    deal_id = deal_data.get("value") if isinstance(deal_data, dict) else deal_data

    return {
        "id": activity.get("id"),
        "subject": activity.get("subject", "") or "",
        "type": activity.get("type", "") or "",
        "due_date": safe_date_parse(activity.get("due_date")),
        "due_time": safe_time_parse(activity.get("due_time")),
        "duration_minutes": parse_duration_minutes(activity.get("duration")),
        "add_time": safe_datetime_parse(activity.get("add_time")),
        "update_time": safe_datetime_parse(activity.get("update_time")),
        "marked_as_done_time": safe_datetime_parse(activity.get("marked_as_done_time")),
        "user_id": int(user_id) if user_id is not None else 0,
        "org_id": int(org_id) if org_id is not None else 0,
        "deal_id": int(deal_id) if deal_id is not None else 0,
    }


def build_upsert_query(columns):
    """Build UPSERT query for available columns in the table."""
    all_columns = [
        "id",
        "subject",
        "type",
        "due_date",
        "due_time",
        "duration_minutes",
        "add_time",
        "update_time",
        "marked_as_done_time",
        "user_id",
        "org_id",
        "deal_id",
    ]

    available_columns = [col for col in all_columns if col in columns]
    columns_str = ", ".join(available_columns)
    values_str = ", ".join([f":{col}" for col in available_columns])

    update_parts = [f"{col} = EXCLUDED.{col}" for col in available_columns if col != "id"]

    if "sync_timestamp" in columns:
        update_parts.append("sync_timestamp = NOW()")

    update_str = ", ".join(update_parts)

    query = f"""
        INSERT INTO pipedrive.fact_pipedrive_activities (
            {columns_str}
        ) VALUES (
            {values_str}
        )
        ON CONFLICT (id) DO UPDATE SET
            {update_str}
    """
    return query, available_columns


def check_table_schema(engine):
    """Return list of columns in pipedrive.fact_pipedrive_activities."""
    with engine.connect() as conn:
        try:
            result = conn.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'fact_pipedrive_activities'
                      AND table_schema = 'pipedrive'
                    ORDER BY ordinal_position
                    """
                )
            )
            return [col[0] for col in result.fetchall()]
        except Exception as e:
            print(f"Error checking schema: {e}")
            return []


def parse_duration_minutes(duration):
    """
    Parse Pipedrive duration into minutes.
    Accepts:
      - "HH:MM"
      - "HH:MM:SS" (seconds ignored for minute-level storage)
      - integer-like strings
    """
    if not duration:
        return None
    try:
        duration_str = str(duration).strip()
        if ":" in duration_str:
            parts = duration_str.split(":")
            if len(parts) >= 2:
                hh = int(parts[0])
                mm = int(parts[1])
                return hh * 60 + mm
        return int(duration_str)
    except Exception:
        return None


def safe_date_parse(date_string):
    if not date_string:
        return None
    try:
        if isinstance(date_string, str):
            date_part = date_string.split(" ")[0]
            return datetime.strptime(date_part, "%Y-%m-%d").date()
        return date_string
    except Exception:
        return None


def safe_datetime_parse(datetime_string):
    if not datetime_string:
        return None
    try:
        if isinstance(datetime_string, str):
            formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"]
            for fmt in formats:
                try:
                    return datetime.strptime(datetime_string, fmt)
                except Exception:
                    continue
            return datetime.strptime(datetime_string.split(" ")[0], "%Y-%m-%d")
        return datetime_string
    except Exception:
        return None


def safe_time_parse(time_string):
    if not time_string:
        return None
    try:
        if isinstance(time_string, str):
            time_str = time_string.strip()
            if ":" in time_str:
                parts = time_str.split(":")
                if len(parts) == 2:
                    time_str = time_str + ":00"
                return datetime.strptime(time_str, "%H:%M:%S").time()
        return time_string
    except Exception:
        return None


if __name__ == "__main__":
    success = update_pipedrive_activities()
    sys.exit(0 if success else 1)
