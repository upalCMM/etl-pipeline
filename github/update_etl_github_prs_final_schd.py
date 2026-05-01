#!/usr/bin/env python3
"""
COMPLETE ETL Script - With Dynamic Repository Discovery
Updates ALL fields including labels, comments, review comments
"""
import os
import sys
import json
import time
import argparse
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from dotenv import load_dotenv, find_dotenv
import certifi

# ============================================================
# Environment / Config
# ============================================================

load_dotenv(find_dotenv())

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_ORG = os.getenv("GITHUB_ORG", "Compare-My-Move")

PGHOST = os.getenv("PGHOST")
PGPORT = os.getenv("PGPORT")
PGDATABASE = os.getenv("PGDATABASE")
PGUSER = os.getenv("PGUSER")
PGPASSWORD = os.getenv("PGPASSWORD")

BASE_URL = "https://api.github.com"
PR_START_DATE = datetime(2025, 1, 1, tzinfo=timezone.utc)

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

REQUEST_DELAY = 0.1
REQUEST_TIMEOUT = (5, 60)

# ============================================================
# DB Helpers
# ============================================================

def get_db_connection():
    conn = psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
    )
    conn.autocommit = False
    return conn

def ensure_state_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS github_data.pull_request_etl_state (
            repo_name TEXT PRIMARY KEY,
            last_processed_pr_updated_at TIMESTAMP
        );
    """)
    conn.commit()
    cur.close()

def load_state(conn):
    cur = conn.cursor()
    cur.execute("SELECT repo_name, last_processed_pr_updated_at FROM github_data.pull_request_etl_state;")
    state = {r[0]: r[1] for r in cur.fetchall()}
    cur.close()
    return state

def update_state(conn, repo_name, timestamp_value):
    if timestamp_value is None:
        return
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO github_data.pull_request_etl_state (repo_name, last_processed_pr_updated_at)
        VALUES (%s, %s)
        ON CONFLICT (repo_name)
        DO UPDATE SET last_processed_pr_updated_at = GREATEST(
            EXCLUDED.last_processed_pr_updated_at,
            github_data.pull_request_etl_state.last_processed_pr_updated_at
        );
    """, (repo_name, timestamp_value))
    conn.commit()
    cur.close()

# ============================================================
# GitHub Helpers
# ============================================================

def get_all_repositories():
    """Dynamically fetch all repositories from the GitHub organization"""
    print(f"\n?? Fetching all repositories from {GITHUB_ORG}...")
    
    all_repos = []
    page = 1
    
    while True:
        url = f"{BASE_URL}/orgs/{GITHUB_ORG}/repos?per_page=100&page={page}&type=all"
        print(f"  Fetching page {page}...")
        
        # Use same request logic as make_request but simplified for this call
        response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        
        if response.status_code != 200:
            print(f"  Error fetching repos: {response.status_code}")
            break
        
        repos = response.json()
        if not repos or len(repos) == 0:
            break
        
        # Extract repository names
        repo_names = [repo['name'] for repo in repos]
        all_repos.extend(repo_names)
        print(f"  Found {len(repo_names)} repos on this page (total so far: {len(all_repos)})")
        
        if len(repos) < 100:
            break
        
        page += 1
        time.sleep(REQUEST_DELAY)
    
    print(f"\n? Found {len(all_repos)} total repositories in {GITHUB_ORG}")
    return all_repos

def make_request(url):
    for attempt in range(3):
        try:
            resp = requests.get(
                url,
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT,
                verify=certifi.where(),
            )
            
            if resp.status_code == 403 and resp.headers.get("X-RateLimit-Remaining") == "0":
                reset_ts = int(resp.headers.get("X-RateLimit-Reset", "0"))
                wait_for = max(reset_ts - int(time.time()) + 5, 30)
                print(f"  Rate limit reached. Waiting {wait_for} seconds.")
                time.sleep(wait_for)
                continue
            
            if resp.status_code == 404:
                return None
                
            if resp.status_code >= 400:
                print(f"  API error {resp.status_code}")
                if attempt < 2:
                    time.sleep(2)
                continue
            
            return resp.json()
            
        except Exception as e:
            print(f"  Request error: {e}")
            if attempt < 2:
                time.sleep(2)
    
    return None

def get_all_pages(url_base):
    all_items = []
    page = 1
    
    while True:
        if '?' in url_base:
            url = f"{url_base}&page={page}"
        else:
            url = f"{url_base}?page={page}"
        
        data = make_request(url)
        if not data:
            break
        
        all_items.extend(data)
        
        if len(data) < 100:
            break
        
        page += 1
        time.sleep(REQUEST_DELAY)
    
    return all_items

# ============================================================
# Data Extraction Helpers
# ============================================================

def to_naive(dt_str):
    if not dt_str:
        return None
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.replace(tzinfo=None)
    except Exception:
        return None

def extract_label_info(pr_data):
    """Extract label information from PR data"""
    labels = pr_data.get("labels", [])
    
    if not labels:
        return {
            "label_names": "",
            "full_labels": json.dumps([]),
            "label_count": 0
        }
    
    label_names_list = [label["name"] for label in labels]
    label_names_str = ", ".join(label_names_list)
    label_count = len(labels)
    
    full_labels_list = [
        {
            "name": label["name"],
            "color": label.get("color", ""),
            "description": label.get("description", "")
        }
        for label in labels
    ]
    
    return {
        "label_names": label_names_str,
        "full_labels": json.dumps(full_labels_list),
        "label_count": label_count
    }

def extract_comments_info(issue_comments, review_comments):
    """Extract comment metrics from issue comments and review comments"""
    
    # Regular comments (on the PR/issue itself)
    regular_comments = issue_comments if issue_comments else []
    regular_comment_authors = list(set([
        comment.get("user", {}).get("login", "unknown")
        for comment in regular_comments
        if comment.get("user", {}).get("login")
    ]))
    
    regular_comment_times = [
        to_naive(comment.get("created_at"))
        for comment in regular_comments
        if comment.get("created_at")
    ]
    regular_comment_times = [t for t in regular_comment_times if t]
    
    # Review comments (inline code comments on PR)
    review_comments_list = review_comments if review_comments else []
    review_comment_authors = list(set([
        comment.get("user", {}).get("login", "unknown")
        for comment in review_comments_list
        if comment.get("user", {}).get("login")
    ]))
    
    review_comment_times = [
        to_naive(comment.get("created_at"))
        for comment in review_comments_list
        if comment.get("created_at")
    ]
    review_comment_times = [t for t in review_comment_times if t]
    
    # Combine for unique authors
    all_comment_authors = list(set(regular_comment_authors + review_comment_authors))
    
    return {
        "regular_comments_count": len(regular_comments),
        "review_comments_count": len(review_comments_list),
        "total_comments_count": len(regular_comments) + len(review_comments_list),
        "regular_comment_authors_count": len(regular_comment_authors),
        "review_comment_authors_count": len(review_comment_authors),
        "unique_comment_authors_count": len(all_comment_authors),
        "regular_comment_authors": ", ".join(sorted(regular_comment_authors)) if regular_comment_authors else "",
        "review_comment_authors": ", ".join(sorted(review_comment_authors)) if review_comment_authors else "",
        "first_regular_comment_time": min(regular_comment_times) if regular_comment_times else None,
        "last_regular_comment_time": max(regular_comment_times) if regular_comment_times else None,
        "first_review_comment_time": min(review_comment_times) if review_comment_times else None,
        "last_review_comment_time": max(review_comment_times) if review_comment_times else None,
    }

# ============================================================
# Main ETL Functions
# ============================================================

def upsert_pull_requests(conn, rows):
    """Upsert PR rows into database"""
    if not rows:
        return
    
    print(f"  Upserting {len(rows)} PR rows...")
    
    sql = """
    INSERT INTO github_data.pull_requests (
        repo_name, pr_id, pr_title, pr_description, author, state, is_draft,
        pr_created_at, pr_updated_at, pr_closed_at, pr_merged_at,
        first_approval_time, last_approval_time, approval_count, all_approval_times,
        first_request_changes_time, last_request_changes_time, request_changes_count, all_request_changes_times,
        draft_events_count, first_draft_event_time, last_draft_event_time, draft_event_types, all_draft_event_times,
        commits_count, first_commit_time, last_commit_time, all_commit_times, last_file_change_time,
        reviews_count, first_review_time, last_review_time, all_review_times, review_states,
        regular_comments_count, review_comments_count, total_comments_count,
        regular_comment_authors_count, review_comment_authors_count, unique_comment_authors_count,
        regular_comment_authors, review_comment_authors,
        first_regular_comment_time, last_regular_comment_time,
        first_review_comment_time, last_review_comment_time,
        additions, deletions, changed_files,
        label_names, full_labels, label_count
    )
    VALUES %s
    ON CONFLICT (repo_name, pr_id)
    DO UPDATE SET
        pr_title = EXCLUDED.pr_title,
        pr_description = EXCLUDED.pr_description,
        author = EXCLUDED.author,
        state = EXCLUDED.state,
        is_draft = EXCLUDED.is_draft,
        pr_created_at = EXCLUDED.pr_created_at,
        pr_updated_at = EXCLUDED.pr_updated_at,
        pr_closed_at = EXCLUDED.pr_closed_at,
        pr_merged_at = EXCLUDED.pr_merged_at,
        first_approval_time = EXCLUDED.first_approval_time,
        last_approval_time = EXCLUDED.last_approval_time,
        approval_count = EXCLUDED.approval_count,
        all_approval_times = EXCLUDED.all_approval_times,
        first_request_changes_time = EXCLUDED.first_request_changes_time,
        last_request_changes_time = EXCLUDED.last_request_changes_time,
        request_changes_count = EXCLUDED.request_changes_count,
        all_request_changes_times = EXCLUDED.all_request_changes_times,
        draft_events_count = EXCLUDED.draft_events_count,
        first_draft_event_time = EXCLUDED.first_draft_event_time,
        last_draft_event_time = EXCLUDED.last_draft_event_time,
        draft_event_types = EXCLUDED.draft_event_types,
        all_draft_event_times = EXCLUDED.all_draft_event_times,
        commits_count = EXCLUDED.commits_count,
        first_commit_time = EXCLUDED.first_commit_time,
        last_commit_time = EXCLUDED.last_commit_time,
        all_commit_times = EXCLUDED.all_commit_times,
        last_file_change_time = EXCLUDED.last_file_change_time,
        reviews_count = EXCLUDED.reviews_count,
        first_review_time = EXCLUDED.first_review_time,
        last_review_time = EXCLUDED.last_review_time,
        all_review_times = EXCLUDED.all_review_times,
        review_states = EXCLUDED.review_states,
        regular_comments_count = EXCLUDED.regular_comments_count,
        review_comments_count = EXCLUDED.review_comments_count,
        total_comments_count = EXCLUDED.total_comments_count,
        regular_comment_authors_count = EXCLUDED.regular_comment_authors_count,
        review_comment_authors_count = EXCLUDED.review_comment_authors_count,
        unique_comment_authors_count = EXCLUDED.unique_comment_authors_count,
        regular_comment_authors = EXCLUDED.regular_comment_authors,
        review_comment_authors = EXCLUDED.review_comment_authors,
        first_regular_comment_time = EXCLUDED.first_regular_comment_time,
        last_regular_comment_time = EXCLUDED.last_regular_comment_time,
        first_review_comment_time = EXCLUDED.first_review_comment_time,
        last_review_comment_time = EXCLUDED.last_review_comment_time,
        additions = EXCLUDED.additions,
        deletions = EXCLUDED.deletions,
        changed_files = EXCLUDED.changed_files,
        label_names = EXCLUDED.label_names,
        full_labels = EXCLUDED.full_labels,
        label_count = EXCLUDED.label_count;
    """
    
    cur = conn.cursor()
    execute_values(cur, sql, rows)
    conn.commit()
    cur.close()

def process_pr(repo_name, pr, skip_timeline=False):
    """Process a single PR and return the row data"""
    number = pr["number"]
    
    # Fetch additional data
    pr_details_url = f"{BASE_URL}/repos/{GITHUB_ORG}/{repo_name}/pulls/{number}"
    details = make_request(pr_details_url) or {}
    
    # Fetch reviews
    reviews_url = f"{BASE_URL}/repos/{GITHUB_ORG}/{repo_name}/pulls/{number}/reviews?per_page=100"
    reviews = get_all_pages(reviews_url)
    
    # Fetch commits
    commits_url = f"{BASE_URL}/repos/{GITHUB_ORG}/{repo_name}/pulls/{number}/commits?per_page=100"
    commits = get_all_pages(commits_url)
    
    # Fetch issue comments
    issue_comments_url = f"{BASE_URL}/repos/{GITHUB_ORG}/{repo_name}/issues/{number}/comments?per_page=100"
    issue_comments = get_all_pages(issue_comments_url)
    
    # Fetch review comments
    review_comments_url = f"{BASE_URL}/repos/{GITHUB_ORG}/{repo_name}/pulls/{number}/comments?per_page=100"
    review_comments = get_all_pages(review_comments_url)
    
    # Fetch timeline (optional)
    timeline = []
    if not skip_timeline:
        timeline_url = f"{BASE_URL}/repos/{GITHUB_ORG}/{repo_name}/issues/{number}/timeline?per_page=100"
        timeline = get_all_pages(timeline_url)
    
    # Build metrics
    approval_times = sorted([
        to_naive(r.get("submitted_at"))
        for r in reviews
        if r.get("state") == "APPROVED" and r.get("submitted_at")
    ])
    
    change_times = sorted([
        to_naive(r.get("submitted_at"))
        for r in reviews
        if r.get("state") == "CHANGES_REQUESTED" and r.get("submitted_at")
    ])
    
    # Draft events from timeline
    draft_events = []
    for e in timeline:
        evt = e.get("event")
        if evt in ("converted_to_draft", "ready_for_review"):
            dt = to_naive(e.get("created_at"))
            if dt:
                draft_events.append((dt, evt))
    
    draft_events.sort(key=lambda x: x[0])
    draft_times = [d[0] for d in draft_events]
    draft_types = [d[1] for d in draft_events]
    
    commit_times = sorted([
        to_naive(c["commit"]["author"]["date"])
        for c in commits
        if c.get("commit", {}).get("author", {}).get("date")
    ])
    
    last_change = commit_times[-1] if commit_times else None
    
    review_times = sorted([
        to_naive(r.get("submitted_at"))
        for r in reviews
        if r.get("submitted_at")
    ])
    review_states = [r.get("state") for r in reviews if r.get("state")]
    
    # Extract label info
    label_info = extract_label_info(pr)
    
    # Extract comment info
    comment_info = extract_comments_info(issue_comments, review_comments)
    
    # Build the complete row
    row = (
        repo_name, number,
        pr.get("title"), pr.get("body"), pr["user"]["login"], pr["state"], pr.get("draft", False),
        to_naive(pr["created_at"]), to_naive(pr["updated_at"]),
        to_naive(pr.get("closed_at")), to_naive(pr.get("merged_at")),
        approval_times[0] if approval_times else None,
        approval_times[-1] if approval_times else None,
        len(approval_times),
        "; ".join(t.isoformat() for t in approval_times) if approval_times else None,
        change_times[0] if change_times else None,
        change_times[-1] if change_times else None,
        len(change_times),
        "; ".join(t.isoformat() for t in change_times) if change_times else None,
        len(draft_times),
        draft_times[0] if draft_times else None,
        draft_times[-1] if draft_times else None,
        "; ".join(draft_types) if draft_types else None,
        "; ".join(t.isoformat() for t in draft_times) if draft_times else None,
        len(commits),
        commit_times[0] if commit_times else None,
        commit_times[-1] if commit_times else None,
        "; ".join(t.isoformat() for t in commit_times) if commit_times else None,
        last_change,
        len(review_times),
        review_times[0] if review_times else None,
        review_times[-1] if review_times else None,
        "; ".join(t.isoformat() for t in review_times) if review_times else None,
        "; ".join(review_states) if review_states else None,
        comment_info["regular_comments_count"],
        comment_info["review_comments_count"],
        comment_info["total_comments_count"],
        comment_info["regular_comment_authors_count"],
        comment_info["review_comment_authors_count"],
        comment_info["unique_comment_authors_count"],
        comment_info["regular_comment_authors"],
        comment_info["review_comment_authors"],
        comment_info["first_regular_comment_time"],
        comment_info["last_regular_comment_time"],
        comment_info["first_review_comment_time"],
        comment_info["last_review_comment_time"],
        details.get("additions", 0),
        details.get("deletions", 0),
        details.get("changed_files", 0),
        label_info["label_names"],
        label_info["full_labels"],
        label_info["label_count"],
    )
    
    return row

def extract_repo(repo_name, last_ts, skip_timeline=False):
    """Extract all PRs for a repository"""
    rows = []
    if last_ts is None:
        last_ts = PR_START_DATE.replace(tzinfo=None)
    
    repo_max_updated = last_ts
    page = 1
    
    while True:
        url = f"{BASE_URL}/repos/{GITHUB_ORG}/{repo_name}/pulls?state=all&sort=updated&direction=desc&per_page=100&page={page}"
        print(f"  Fetching page {page}...")
        prs = make_request(url)
        
        if not prs:
            break
        
        for pr in prs:
            pr_created = datetime.fromisoformat(pr["created_at"].replace("Z", "+00:00"))
            pr_updated = datetime.fromisoformat(pr["updated_at"].replace("Z", "+00:00"))
            
            pr_created_naive = pr_created.replace(tzinfo=None)
            pr_updated_naive = pr_updated.replace(tzinfo=None)
            
            if pr_created < PR_START_DATE:
                continue
            
            if pr_updated_naive <= last_ts:
                continue
            
            print(f"  Processing PR #{pr['number']}...")
            row = process_pr(repo_name, pr, skip_timeline)
            rows.append(row)
            
            if pr_updated_naive > repo_max_updated:
                repo_max_updated = pr_updated_naive
            
            time.sleep(REQUEST_DELAY)
        
        page += 1
    
    # Deduplicate
    unique = {}
    for r in rows:
        key = (r[0], r[1])
        unique[key] = r
    rows = list(unique.values())
    
    return rows, repo_max_updated

# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(description='Complete GitHub PR ETL with dynamic repo discovery')
    parser.add_argument('--repo', type=str, help='Process single repository (overrides auto-discovery)')
    parser.add_argument('--skip-timeline', action='store_true', help='Skip timeline API calls')
    parser.add_argument('--precheck', action='store_true', help='Quick precheck only')
    parser.add_argument('--list-repos', action='store_true', help='List all repositories and exit')
    args = parser.parse_args()
    
    # List repos mode
    if args.list_repos:
        repos = get_all_repositories()
        print("\n?? All repositories found:")
        for i, repo in enumerate(repos, 1):
            print(f"  {i}. {repo}")
        return
    
    # Precheck mode
    if args.precheck:
        if args.repo:
            repos = [args.repo]
        else:
            repos = get_all_repositories()
        
        for repo in repos:
            url = f"{BASE_URL}/repos/{GITHUB_ORG}/{repo}/pulls?state=all&per_page=5"
            data = make_request(url)
            if data:
                print(f"\n{repo}: {len(data)} recent PRs")
                for pr in data:
                    print(f"  #{pr['number']} - {pr['state']} - labels: {len(pr.get('labels', []))}")
        return
    
    # Full ETL mode
    if not all([GITHUB_TOKEN, PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD]):
        print("Missing required environment variables for GitHub or Postgres.")
        return
    
    # Determine repos to process
    if args.repo:
        repos = [args.repo]
        print(f"\n?? Processing single repository: {args.repo}")
    else:
        repos = get_all_repositories()
        print(f"\n?? Processing ALL {len(repos)} repositories found")
    
    print("Starting COMPLETE GitHub PR ETL...")
    conn = get_db_connection()
    ensure_state_table(conn)
    state = load_state(conn)
    
    for repo_name in repos:
        print(f"\n{'='*60}")
        print(f"Processing {repo_name}")
        print(f"{'='*60}")
        
        last_ts = state.get(repo_name)
        print(f"  Last sync timestamp: {last_ts if last_ts else 'Never (starting from {PR_START_DATE})'}")
        
        rows, max_updated = extract_repo(repo_name, last_ts, args.skip_timeline)
        
        if rows:
            upsert_pull_requests(conn, rows)
            print(f"  ? Updated {len(rows)} PRs")
        else:
            print("  ?? No new PRs to process")
        
        if max_updated and max_updated > (last_ts or datetime.min):
            update_state(conn, repo_name, max_updated)
            print(f"  ?? State updated to {max_updated}")
    
    conn.close()
    print("\n? ETL Complete!")

if __name__ == "__main__":
    main()