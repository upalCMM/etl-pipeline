#!/usr/bin/env python3
"""
GITHUB_update_etl_github_user_final_schd.py - WITH DYNAMIC REPO DISCOVERY

Incremental GitHub PR activity loader.

What this version fixes
-----------------------
- FIXED: activity_key no longer includes metadata, preventing duplicate records
- FIXED: batch deduplication prevents same-key conflicts within a single UPSERT
- NEW: Dynamically discovers ALL repositories in the organization
- Keeps all data but ensures same activity from multiple sources creates single record
- Uses UPSERT with deterministic activity_key
- Supports existing tables by applying schema upgrades in code
- Rolls back failed transactions so one bad insert does not poison the session
- Uses incremental sync state per repo
- Supports full refresh for selected repo(s)

Usage
-----
List all repositories:
    python GITHUB_update_etl_github_user_final_schd.py --list-repos

Incremental run (ALL repos):
    python GITHUB_update_etl_github_user_final_schd.py

Single repo:
    python GITHUB_update_etl_github_user_final_schd.py --repo craft

Full refresh (ALL repos):
    python GITHUB_update_etl_github_user_final_schd.py --full-refresh

Override sync start point:
    python GITHUB_update_etl_github_user_final_schd.py --since 2025-01-01T00:00:00Z

Debug logging:
    python GITHUB_update_etl_github_user_final_schd.py --debug
"""

import os
import sys
import json
import time
import argparse
import logging
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Any

import requests
import psycopg2
from psycopg2.extras import execute_values, Json
from dotenv import load_dotenv, find_dotenv
import certifi


# ============================================================
# Configuration
# ============================================================

load_dotenv(find_dotenv())

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_ORG = os.getenv("GITHUB_ORG", "Compare-My-Move")
BASE_URL = "https://api.github.com"

DB_CONFIG = {
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
    "dbname": os.getenv("PG_DB"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASS"),
}

DEFAULT_START_DATE = datetime(2025, 1, 1, tzinfo=timezone.utc)

REQUEST_TIMEOUT = (10, 60)
REQUEST_DELAY = 0.15
MIN_RATE_LIMIT_BUFFER = 50

PIPELINE_NAME = "github_programmer_activity_tracker_v2"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - programmer_activity - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("programmer_activity")


# ============================================================
# Helpers
# ============================================================

def parse_time(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None


def to_naive_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def hours_between(start: Optional[datetime], end: Optional[datetime]) -> Optional[float]:
    if not start or not end:
        return None
    return (end - start).total_seconds() / 3600


def get_work_hour(dt: Optional[datetime]) -> Optional[int]:
    return None if dt is None else dt.hour


def get_day_of_week(dt: Optional[datetime]) -> Optional[int]:
    return None if dt is None else dt.weekday()


def is_bot_user(username: Optional[str]) -> bool:
    if not username:
        return False
    username_l = username.lower()
    bot_indicators = ["[bot]", "dependabot", "github-actions", "renovate", "bot"]
    return any(token in username_l for token in bot_indicators)


def safe_json(data: Dict[str, Any]) -> str:
    return json.dumps(data, default=str, sort_keys=True)


def activity_key_for(
    repo_name: str,
    pr_id: int,
    username: str,
    activity_type: str,
    activity_time: datetime,
    comment_id: Optional[int],
    in_reply_to_user: Optional[str],
    metadata: Dict[str, Any],  # Parameter kept for compatibility, not used in key
) -> str:
    """
    Creates a deterministic unique key for an activity.
    FIXED: metadata removed from key to prevent duplicates from multiple sources.
    """
    raw = "||".join([
        repo_name or "",
        str(pr_id or ""),
        username or "",
        activity_type or "",
        activity_time.isoformat() if activity_time else "",
        str(comment_id or ""),
        in_reply_to_user or "",
        # metadata intentionally removed to deduplicate same activity from different sources
    ])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def deduplicate_activities(rows: List[Tuple]) -> List[Tuple]:
    """
    Remove duplicate activity_keys from the same batch.
    Keeps the first occurrence, drops subsequent duplicates.
    This prevents the "ON CONFLICT DO UPDATE cannot affect row a second time" error.
    """
    seen_keys = set()
    deduped = []
    for row in rows:
        activity_key = row[0]  # first column is activity_key
        if activity_key not in seen_keys:
            seen_keys.add(activity_key)
            deduped.append(row)
    if len(rows) != len(deduped):
        logger.debug(f"Deduplicated {len(rows) - len(deduped)} rows in batch")
    return deduped


def parse_since_arg(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    dt = parse_time(value)
    if dt is None:
        raise argparse.ArgumentTypeError(
            "Invalid --since value. Use ISO format, e.g. 2025-01-01T00:00:00Z"
        )
    return dt


# ============================================================
# GitHub API Client
# ============================================================

class GitHubClient:
    def __init__(self, token: str, org: str):
        self.org = org
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        })

    def _maybe_sleep_for_rate_limit(self, response: requests.Response) -> None:
        remaining = int(response.headers.get("X-RateLimit-Remaining", "5000"))
        reset_ts = int(response.headers.get("X-RateLimit-Reset", "0"))

        if remaining <= MIN_RATE_LIMIT_BUFFER:
            sleep_for = max(reset_ts - int(time.time()) + 5, 0)
            if sleep_for > 0:
                logger.warning(
                    f"Rate limit low ({remaining}). Sleeping for {sleep_for}s until reset."
                )
                time.sleep(sleep_for)

    def get(self, url: str, max_retries: int = 5) -> Optional[Any]:
        for attempt in range(max_retries):
            try:
                resp = self.session.get(
                    url,
                    timeout=REQUEST_TIMEOUT,
                    verify=certifi.where(),
                )

                if resp.status_code == 200:
                    self._maybe_sleep_for_rate_limit(resp)
                    time.sleep(REQUEST_DELAY)
                    return resp.json()

                if resp.status_code == 404:
                    return None

                if resp.status_code in (403, 429):
                    remaining = int(resp.headers.get("X-RateLimit-Remaining", "0"))
                    reset_ts = int(resp.headers.get("X-RateLimit-Reset", "0"))
                    body = (resp.text or "").lower()

                    if remaining == 0 or "rate limit" in body or "secondary rate limit" in body:
                        sleep_for = max(reset_ts - int(time.time()) + 5, 30)
                        logger.warning(
                            f"GitHub throttled request. status={resp.status_code}, "
                            f"remaining={remaining}. Sleeping {sleep_for}s"
                        )
                        time.sleep(sleep_for)
                        continue

                if resp.status_code >= 500:
                    backoff = 2 ** attempt
                    logger.warning(f"GitHub server error {resp.status_code}. Retrying in {backoff}s")
                    time.sleep(backoff)
                    continue

                logger.error(f"HTTP {resp.status_code}: {resp.text[:300]}")
                return None

            except requests.exceptions.Timeout:
                backoff = 2 ** attempt
                logger.warning(f"Timeout for {url}. Retrying in {backoff}s")
                time.sleep(backoff)
            except Exception as e:
                backoff = 2 ** attempt
                logger.warning(f"Request error for {url}: {e}. Retrying in {backoff}s")
                time.sleep(backoff)

        logger.error(f"Failed after retries: {url}")
        return None

    def get_paged(self, url: str, page_limit: Optional[int] = None) -> List[Dict]:
        items: List[Dict] = []
        page = 1

        while True:
            sep = "&" if "?" in url else "?"
            page_url = f"{url}{sep}per_page=100&page={page}"
            data = self.get(page_url)

            if not data or not isinstance(data, list):
                break

            items.extend(data)

            if len(data) < 100:
                break
            if page_limit is not None and page >= page_limit:
                break

            page += 1

        return items

    def get_all_repositories(self) -> List[str]:
        """Dynamically fetch all repositories from the GitHub organization"""
        logger.info(f"Fetching all repositories from {self.org}...")
        
        all_repos = []
        page = 1
        
        while True:
            url = f"{BASE_URL}/orgs/{self.org}/repos?per_page=100&page={page}&type=all"
            repos = self.get(url)
            
            if not repos or not isinstance(repos, list):
                break
            
            repo_names = [repo['name'] for repo in repos]
            all_repos.extend(repo_names)
            logger.debug(f"  Page {page}: found {len(repo_names)} repos (total so far: {len(all_repos)})")
            
            if len(repos) < 100:
                break
            
            page += 1
        
        logger.info(f"Found {len(all_repos)} total repositories")
        return all_repos

    def get_prs_page(self, repo: str, page: int = 1, state: str = "all") -> Optional[List[Dict]]:
        url = (
            f"{BASE_URL}/repos/{self.org}/{repo}/pulls"
            f"?state={state}&sort=updated&direction=desc&per_page=100&page={page}"
        )
        return self.get(url)

    def get_pr_details(self, repo: str, pr_number: int) -> Optional[Dict]:
        return self.get(f"{BASE_URL}/repos/{self.org}/{repo}/pulls/{pr_number}")

    def get_pr_reviews(self, repo: str, pr_number: int) -> List[Dict]:
        return self.get_paged(f"{BASE_URL}/repos/{self.org}/{repo}/pulls/{pr_number}/reviews")

    def get_issue_comments(self, repo: str, pr_number: int) -> List[Dict]:
        return self.get_paged(f"{BASE_URL}/repos/{self.org}/{repo}/issues/{pr_number}/comments")

    def get_review_comments(self, repo: str, pr_number: int) -> List[Dict]:
        return self.get_paged(f"{BASE_URL}/repos/{self.org}/{repo}/pulls/{pr_number}/comments")

    def get_timeline(self, repo: str, pr_number: int) -> List[Dict]:
        return self.get_paged(f"{BASE_URL}/repos/{self.org}/{repo}/issues/{pr_number}/timeline")

    def get_labels(self, repo: str, pr_number: int) -> List[Dict]:
        data = self.get(f"{BASE_URL}/repos/{self.org}/{repo}/issues/{pr_number}/labels")
        return data if isinstance(data, list) else []


# ============================================================
# Database
# ============================================================

def ensure_schema(conn) -> None:
    cur = conn.cursor()

    cur.execute("CREATE SCHEMA IF NOT EXISTS github_data;")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS github_data.programmer_activity (
            id BIGSERIAL PRIMARY KEY,
            username TEXT NOT NULL,
            repo_name TEXT NOT NULL,
            pr_id INTEGER NOT NULL,
            pr_title TEXT,
            pr_author TEXT NOT NULL,
            activity_type TEXT NOT NULL,
            activity_time TIMESTAMP NOT NULL,
            hours_since_pr_open FLOAT,
            review_state TEXT,
            comment_length INTEGER,
            comment_id BIGINT,
            in_reply_to_user TEXT,
            pr_labels TEXT[],
            is_bot BOOLEAN DEFAULT FALSE,
            work_hour INTEGER,
            day_of_week INTEGER,
            first_action_for_pr BOOLEAN DEFAULT FALSE,
            response_to_review BOOLEAN DEFAULT FALSE,
            metadata JSONB,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)

    # Upgrade old tables in place
    cur.execute("""
        ALTER TABLE github_data.programmer_activity
        ADD COLUMN IF NOT EXISTS activity_key TEXT;
    """)
    cur.execute("""
        ALTER TABLE github_data.programmer_activity
        ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_programmer_activity_activity_key
        ON github_data.programmer_activity(activity_key);
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS github_data.etl_state (
            pipeline_name TEXT NOT NULL,
            repo_name TEXT NOT NULL,
            last_synced_at TIMESTAMP,
            updated_at TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (pipeline_name, repo_name)
        );
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_programmer_activity_repo_pr
        ON github_data.programmer_activity(repo_name, pr_id);
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_programmer_activity_time
        ON github_data.programmer_activity(activity_time);
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_programmer_activity_username
        ON github_data.programmer_activity(username);
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_programmer_activity_type
        ON github_data.programmer_activity(activity_type);
    """)

    conn.commit()
    cur.close()


def get_last_synced_at(conn, repo_name: str) -> Optional[datetime]:
    cur = conn.cursor()
    cur.execute("""
        SELECT last_synced_at
        FROM github_data.etl_state
        WHERE pipeline_name = %s
          AND repo_name = %s
    """, (PIPELINE_NAME, repo_name))
    row = cur.fetchone()
    cur.close()

    if not row or row[0] is None:
        return None
    return row[0].replace(tzinfo=timezone.utc)


def set_last_synced_at(conn, repo_name: str, synced_at: datetime) -> None:
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO github_data.etl_state (
            pipeline_name,
            repo_name,
            last_synced_at,
            updated_at
        )
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (pipeline_name, repo_name)
        DO UPDATE SET
            last_synced_at = EXCLUDED.last_synced_at,
            updated_at = NOW()
    """, (PIPELINE_NAME, repo_name, to_naive_utc(synced_at)))
    conn.commit()
    cur.close()


def delete_existing_range(conn, repo_name: str, since_dt: datetime) -> None:
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM github_data.programmer_activity
        WHERE repo_name = %s
          AND activity_time >= %s
    """, (repo_name, to_naive_utc(since_dt)))
    deleted = cur.rowcount
    conn.commit()
    cur.close()
    logger.info(f"Deleted {deleted} existing activity rows for repo={repo_name} from {since_dt.isoformat()}")


def upsert_activities(conn, rows: List[Tuple]) -> int:
    if not rows:
        return 0

    sql = """
        INSERT INTO github_data.programmer_activity (
            activity_key,
            username,
            repo_name,
            pr_id,
            pr_title,
            pr_author,
            activity_type,
            activity_time,
            hours_since_pr_open,
            review_state,
            comment_length,
            comment_id,
            in_reply_to_user,
            pr_labels,
            is_bot,
            work_hour,
            day_of_week,
            first_action_for_pr,
            response_to_review,
            metadata
        )
        VALUES %s
        ON CONFLICT (activity_key)
        DO UPDATE SET
            pr_title = EXCLUDED.pr_title,
            review_state = EXCLUDED.review_state,
            comment_length = EXCLUDED.comment_length,
            in_reply_to_user = EXCLUDED.in_reply_to_user,
            pr_labels = EXCLUDED.pr_labels,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
    """

    cur = conn.cursor()
    execute_values(cur, sql, rows, page_size=500)
    affected = cur.rowcount
    conn.commit()
    cur.close()
    return affected


# ============================================================
# Activity extraction
# ============================================================

def make_activity_row(
    repo_name: str,
    pr_number: int,
    pr_title: str,
    pr_author: str,
    username: str,
    activity_type: str,
    activity_time: datetime,
    hours_since_pr_open: Optional[float],
    review_state: Optional[str],
    comment_length: Optional[int],
    comment_id: Optional[int],
    in_reply_to_user: Optional[str],
    pr_labels: List[str],
    first_action_for_pr: bool,
    response_to_review: bool,
    metadata: Dict[str, Any],
) -> Tuple:
    activity_key = activity_key_for(
        repo_name=repo_name,
        pr_id=pr_number,
        username=username,
        activity_type=activity_type,
        activity_time=activity_time,
        comment_id=comment_id,
        in_reply_to_user=in_reply_to_user,
        metadata=metadata,  # Passed but not used in key
    )

    activity_time_naive = to_naive_utc(activity_time)

    return (
        activity_key,
        username,
        repo_name,
        pr_number,
        pr_title,
        pr_author,
        activity_type,
        activity_time_naive,
        hours_since_pr_open,
        review_state,
        comment_length,
        comment_id,
        in_reply_to_user,
        pr_labels,
        is_bot_user(username),
        get_work_hour(activity_time_naive),
        get_day_of_week(activity_time_naive),
        first_action_for_pr,
        response_to_review,
        Json(metadata),
    )


def extract_pr_activities(repo_name: str, pr_data: Dict, client: GitHubClient, since_dt: datetime) -> List[Tuple]:
    pr_number = pr_data["number"]
    pr_title = pr_data.get("title", "")
    pr_author = pr_data.get("user", {}).get("login") or "unknown"
    pr_created_at = parse_time(pr_data.get("created_at"))

    if not pr_created_at:
        return []

    logger.info(f"Processing PR #{pr_number} in {repo_name}")

    # Sequential requests inside each PR to reduce rate-limit pressure
    details = client.get_pr_details(repo_name, pr_number) or {}
    reviews = client.get_pr_reviews(repo_name, pr_number) or []
    issue_comments = client.get_issue_comments(repo_name, pr_number) or []
    review_comments = client.get_review_comments(repo_name, pr_number) or []
    timeline = client.get_timeline(repo_name, pr_number) or []
    labels_data = client.get_labels(repo_name, pr_number) or []

    pr_labels = [label.get("name", "") for label in labels_data if label.get("name")]
    activities: List[Tuple] = []

    # 1) PR opened
    if pr_created_at >= since_dt:
        activities.append(make_activity_row(
            repo_name=repo_name,
            pr_number=pr_number,
            pr_title=pr_title,
            pr_author=pr_author,
            username=pr_author,
            activity_type="opened",
            activity_time=pr_created_at,
            hours_since_pr_open=0.0,
            review_state=None,
            comment_length=None,
            comment_id=None,
            in_reply_to_user=None,
            pr_labels=pr_labels,
            first_action_for_pr=True,
            response_to_review=False,
            metadata={"pr_url": pr_data.get("html_url")},
        ))

    # 2) Reviews
    first_review_time = set()
    for review in reviews:
        reviewer = review.get("user", {}).get("login")
        submitted_at = parse_time(review.get("submitted_at"))
        state = review.get("state")
        review_id = review.get("id")

        if not reviewer or not submitted_at or not state or submitted_at < since_dt:
            continue

        if state == "APPROVED":
            act_type = "approved"
        elif state == "CHANGES_REQUESTED":
            act_type = "changes_requested"
        elif state == "COMMENTED":
            act_type = "review_commented"
        else:
            act_type = "reviewed"

        is_first = reviewer not in first_review_time
        first_review_time.add(reviewer)

        activities.append(make_activity_row(
            repo_name=repo_name,
            pr_number=pr_number,
            pr_title=pr_title,
            pr_author=pr_author,
            username=reviewer,
            activity_type=act_type,
            activity_time=submitted_at,
            hours_since_pr_open=hours_between(pr_created_at, submitted_at),
            review_state=state,
            comment_length=None,
            comment_id=review_id,
            in_reply_to_user=None,
            pr_labels=pr_labels,
            first_action_for_pr=is_first,
            response_to_review=False,
            metadata={"review_id": review_id},
        ))

    # 3) Issue comments
    commenters_seen = set()
    for comment in issue_comments:
        commenter = comment.get("user", {}).get("login")
        created_at = parse_time(comment.get("created_at"))
        comment_id = comment.get("id")
        body = comment.get("body", "")

        if not commenter or not created_at or created_at < since_dt:
            continue

        is_first = commenter not in commenters_seen
        commenters_seen.add(commenter)

        activities.append(make_activity_row(
            repo_name=repo_name,
            pr_number=pr_number,
            pr_title=pr_title,
            pr_author=pr_author,
            username=commenter,
            activity_type="commented",
            activity_time=created_at,
            hours_since_pr_open=hours_between(pr_created_at, created_at),
            review_state=None,
            comment_length=len(body) if body is not None else None,
            comment_id=comment_id,
            in_reply_to_user=None,
            pr_labels=pr_labels,
            first_action_for_pr=is_first,
            response_to_review=False,
            metadata={"comment_type": "issue", "comment_id": comment_id},
        ))

    # 4) Review comments
    review_commenters_seen = set(commenters_seen)
    for comment in review_comments:
        commenter = comment.get("user", {}).get("login")
        created_at = parse_time(comment.get("created_at"))
        comment_id = comment.get("id")
        body = comment.get("body", "")
        in_reply_to = comment.get("in_reply_to_id")

        if not commenter or not created_at or created_at < since_dt:
            continue

        is_first = commenter not in review_commenters_seen
        review_commenters_seen.add(commenter)

        activities.append(make_activity_row(
            repo_name=repo_name,
            pr_number=pr_number,
            pr_title=pr_title,
            pr_author=pr_author,
            username=commenter,
            activity_type="review_commented",
            activity_time=created_at,
            hours_since_pr_open=hours_between(pr_created_at, created_at),
            review_state=None,
            comment_length=len(body) if body is not None else None,
            comment_id=comment_id,
            in_reply_to_user=None,
            pr_labels=pr_labels,
            first_action_for_pr=is_first,
            response_to_review=in_reply_to is not None,
            metadata={
                "comment_type": "review",
                "comment_id": comment_id,
                "in_reply_to": in_reply_to,
            },
        ))

    # 5) Timeline events
    type_map = {
        "merged": "merged",
        "closed": "closed",
        "reopened": "reopened",
        "assigned": "assigned",
        "unassigned": "unassigned",
        "review_requested": "review_requested",
        "review_request_removed": "review_request_removed",
        "ready_for_review": "ready_for_review",
        "converted_to_draft": "converted_to_draft",
        "labeled": "labeled",
        "unlabeled": "unlabeled",
        "milestoned": "milestoned",
        "demilestoned": "demilestoned",
    }

    for event in timeline:
        event_type = event.get("event")
        actor = event.get("actor", {}).get("login")
        created_at = parse_time(event.get("created_at"))

        if event_type not in type_map or not actor or not created_at or created_at < since_dt:
            continue

        target_user = None
        if event_type in ("assigned", "unassigned"):
            target_user = event.get("assignee", {}).get("login")
        elif event_type == "review_requested":
            target_user = event.get("requested_reviewer", {}).get("login")

        activities.append(make_activity_row(
            repo_name=repo_name,
            pr_number=pr_number,
            pr_title=pr_title,
            pr_author=pr_author,
            username=actor,
            activity_type=type_map[event_type],
            activity_time=created_at,
            hours_since_pr_open=hours_between(pr_created_at, created_at),
            review_state=None,
            comment_length=None,
            comment_id=None,
            in_reply_to_user=target_user,
            pr_labels=pr_labels,
            first_action_for_pr=False,
            response_to_review=False,
            metadata={"event_id": event.get("id"), "event_type": event_type},
        ))

    # 6) Merge fallback from PR details
    if details.get("merged_by") and details.get("merged_at"):
        merger = details["merged_by"].get("login")
        merged_at = parse_time(details.get("merged_at"))

        if merger and merged_at and merged_at >= since_dt:
            activities.append(make_activity_row(
                repo_name=repo_name,
                pr_number=pr_number,
                pr_title=pr_title,
                pr_author=pr_author,
                username=merger,
                activity_type="merged",
                activity_time=merged_at,
                hours_since_pr_open=hours_between(pr_created_at, merged_at),
                review_state=None,
                comment_length=None,
                comment_id=None,
                in_reply_to_user=None,
                pr_labels=pr_labels,
                first_action_for_pr=False,
                response_to_review=False,
                metadata={"merge_commit": details.get("merge_commit_sha")},
            ))

    logger.info(f"Extracted {len(activities)} activities from PR #{pr_number}")
    return activities


# ============================================================
# Sync logic
# ============================================================

def resolve_since_dt(conn, repo_name: str, full_refresh: bool, cli_since: Optional[datetime]) -> datetime:
    if cli_since is not None:
        return cli_since

    if full_refresh:
        return DEFAULT_START_DATE

    last_synced = get_last_synced_at(conn, repo_name)
    if last_synced is None:
        return DEFAULT_START_DATE

    # 24 hour overlap for safety (increased from 10 minutes)
    return last_synced - timedelta(hours=24)


def iter_prs_since(client: GitHubClient, repo_name: str, since_dt: datetime):
    page = 1

    while True:
        prs = client.get_prs_page(repo_name, page=page, state="all")
        if not prs:
            break

        keep_going = False

        for pr in prs:
            updated_at = parse_time(pr.get("updated_at"))
            if updated_at and updated_at >= since_dt:
                keep_going = True
                yield pr

        if not keep_going:
            break

        if len(prs) < 100:
            break

        page += 1


def process_repo(conn, client: GitHubClient, repo_name: str, since_dt: datetime, full_refresh: bool = False) -> None:
    logger.info(f"Starting repo={repo_name}, since={since_dt.isoformat()}, full_refresh={full_refresh}")

    if full_refresh:
        delete_existing_range(conn, repo_name, since_dt)

    total_prs = 0
    total_rows = 0
    max_seen_updated_at = since_dt

    for pr in iter_prs_since(client, repo_name, since_dt):
        pr_number = pr["number"]
        pr_updated_at = parse_time(pr.get("updated_at"))
        if pr_updated_at and pr_updated_at > max_seen_updated_at:
            max_seen_updated_at = pr_updated_at

        try:
            rows = extract_pr_activities(repo_name, pr, client, since_dt)
            rows = deduplicate_activities(rows)  # <-- BATCH DEDUPLICATION
            upsert_activities(conn, rows)
            total_prs += 1
            total_rows += len(rows)

            if total_prs % 10 == 0:
                logger.info(f"Repo={repo_name} progress: PRs processed={total_prs}, activity rows={total_rows}")

        except Exception as e:
            conn.rollback()
            logger.exception(f"Failed to process repo={repo_name} pr=#{pr_number}: {e}")

    set_last_synced_at(conn, repo_name, max_seen_updated_at or datetime.now(timezone.utc))
    logger.info(f"Completed repo={repo_name}: PRs={total_prs}, activity_rows={total_rows}")


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Track programmer activities from GitHub PRs")
    parser.add_argument("--repo", type=str, help="Process a single repository (overrides auto-discovery)")
    parser.add_argument("--full-refresh", action="store_true", help="Delete and reload data from the sync start point")
    parser.add_argument("--since", type=str, help="Override sync start point, e.g. 2025-01-01T00:00:00Z")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--list-repos", action="store_true", help="List all repositories and exit")

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    if not GITHUB_TOKEN:
        logger.error("GITHUB_TOKEN not found in environment")
        sys.exit(1)

    if not all([DB_CONFIG["host"], DB_CONFIG["dbname"], DB_CONFIG["user"]]):
        logger.error("Database configuration incomplete. Check PG_HOST, PG_DB, PG_USER, PG_PASS in .env")
        sys.exit(1)

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        ensure_schema(conn)
        client = GitHubClient(GITHUB_TOKEN, GITHUB_ORG)
        
        # Handle --list-repos flag
        if args.list_repos:
            repos = client.get_all_repositories()
            print("\n?? All repositories found:")
            for i, repo in enumerate(repos, 1):
                print(f"  {i}. {repo}")
            return
        
        # Determine repos to process
        if args.repo:
            repos = [args.repo]
            logger.info(f"Processing single repository: {args.repo}")
        else:
            repos = client.get_all_repositories()
            logger.info(f"Processing ALL {len(repos)} repositories found")
        
        # Override start date if provided
        cli_since = parse_since_arg(args.since) if args.since else None

        for repo_name in repos:
            since_dt = resolve_since_dt(
                conn=conn,
                repo_name=repo_name,
                full_refresh=args.full_refresh,
                cli_since=cli_since,
            )
            process_repo(
                conn=conn,
                client=client,
                repo_name=repo_name,
                since_dt=since_dt,
                full_refresh=args.full_refresh,
            )

        logger.info("ETL run complete.")

    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()