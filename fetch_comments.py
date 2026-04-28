#!/usr/bin/env python3
"""
Fetch first qualifying comment timestamp for PRs.

Uses the bulk /repos/{owner}/{repo}/issues/comments endpoint to efficiently
scan all comments, then records the first non-author, non-bot comment per PR
into a `pr_first_comment` table.

Only fetches comments from the last ~14 months (to support 1-year charts with
smoothing buffer).

Usage:
    python fetch_comments.py                    # Fetch/update all target repos
    python fetch_comments.py --repos dotnet/runtime  # Specific repo
    python fetch_comments.py --since 2024-06-01      # Custom start date
"""

import sqlite3
import time
import os
import sys
import signal
import argparse
from datetime import datetime, timezone, timedelta

import requests as req

DEFAULT_DB = "pr-dashboard.db"
REQUEST_DELAY = 0.5

# Repos to fetch comments for (skip vscode, rust, go — too large or not needed)
COMMENT_REPOS = [
    "microsoft/aspire",
    "microsoft/vcpkg",
    "dotnet/maui",
    "dotnet/roslyn",
    "dotnet/runtime",
]

# Bot accounts — comments by these are never "first human comment"
BOT_ACCOUNTS = {
    "bors", "rust-bors", "dotnet-bot", "dependabot[bot]", "github-actions[bot]",
    "renovate[bot]", "copilot-swe-agent[bot]", "Copilot",
    "dotnet-maestro[bot]", "msftbot[bot]", "fabricbot[bot]",
    "dnfclas", "dotnet-policy-service[bot]",
    "azure-pipelines[bot]", "codecov[bot]", "codecov-commenter",
}

_shutdown = False


def signal_handler(sig, frame):
    global _shutdown
    if _shutdown:
        sys.exit(1)
    print("\nShutdown requested — finishing current page...")
    _shutdown = True


signal.signal(signal.SIGINT, signal_handler)


def get_token():
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        import subprocess
        try:
            result = subprocess.run(
                ["gh", "auth", "token"], capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0:
                token = result.stdout.strip()
        except Exception:
            pass
    if not token:
        print("ERROR: No GitHub token found.")
        sys.exit(1)
    return token


def init_db(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=60000")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS pr_first_comment (
            repo TEXT NOT NULL,
            number INTEGER NOT NULL,
            first_comment_at TEXT NOT NULL,
            commenter TEXT NOT NULL,
            PRIMARY KEY (repo, number)
        );

        CREATE TABLE IF NOT EXISTS comment_fetch_progress (
            repo TEXT PRIMARY KEY,
            last_comment_id INTEGER NOT NULL DEFAULT 0,
            last_since TEXT,
            updated_at TEXT
        );
    """)
    conn.commit()
    return conn


def fetch_page(session, url, params, max_retries=5):
    """Fetch a single API page with retry and rate-limit handling."""
    rate_limit_retries = 0

    for attempt in range(max_retries):
        if _shutdown:
            return None
        try:
            resp = session.get(url, params=params, timeout=30)
        except req.exceptions.RequestException as e:
            wait = min(4 ** attempt, 120)
            print(f"  Network error: {e}, retry in {wait}s...")
            time.sleep(wait)
            continue

        remaining = int(resp.headers.get("X-RateLimit-Remaining", 9999))
        reset_ts = int(resp.headers.get("X-RateLimit-Reset", 0))

        if resp.status_code == 200:
            if remaining < 100:
                wait = max(reset_ts - time.time(), 0) + 5
                print(f"  Rate limit low ({remaining}). Sleeping {wait:.0f}s...")
                time.sleep(wait)
            return resp

        if resp.status_code == 403:
            rate_limit_retries += 1
            if rate_limit_retries > 10:
                print(f"  FAILED: rate limit retries exhausted.")
                return None
            retry_after = resp.headers.get("Retry-After")
            wait = int(retry_after) + 5 if retry_after else max(reset_ts - time.time(), 60) + 5
            print(f"  Rate limited. Sleeping {wait:.0f}s...")
            time.sleep(wait)
            continue

        if resp.status_code in (500, 502, 503):
            wait = min(4 ** attempt, 120)
            print(f"  Server error {resp.status_code}, retry in {wait}s...")
            time.sleep(wait)
            continue

        print(f"  Unexpected status {resp.status_code}: {resp.text[:200]}")
        return None

    return None


def load_pr_authors(conn, repo):
    """Load PR authors and copilot_requesters for a repo."""
    rows = conn.execute(
        "SELECT number, author, copilot_requester FROM items "
        "WHERE repo = ? AND is_pull_request = 1",
        (repo,)
    ).fetchall()
    pr_info = {}
    for number, author, copilot_requester in rows:
        pr_info[number] = {
            "author": author,
            "copilot_requester": copilot_requester,
        }
    return pr_info


def is_qualifying_comment(commenter, pr_info_entry):
    """Check if a comment qualifies as 'first human comment'."""
    if not commenter or commenter.lower() in {b.lower() for b in BOT_ACCOUNTS}:
        return False
    if not pr_info_entry:
        return False
    # Not by the PR author
    if pr_info_entry["author"] and commenter.lower() == pr_info_entry["author"].lower():
        return False
    # Not by the person who requested copilot to open it
    if pr_info_entry["copilot_requester"] and commenter.lower() == pr_info_entry["copilot_requester"].lower():
        return False
    return True


def fetch_comments_for_repo(session, conn, repo, since_date):
    """Fetch all comments for a repo using the bulk endpoint."""
    owner, name = repo.split("/")
    url = f"https://api.github.com/repos/{owner}/{name}/issues/comments"

    # Check progress
    row = conn.execute(
        "SELECT last_since, last_comment_id FROM comment_fetch_progress WHERE repo = ?",
        (repo,)
    ).fetchone()

    if row and row[0]:
        # Resume from where we left off
        since = row[0]
        last_seen_id = row[1] or 0
        print(f"  Resuming from since={since}, last_id={last_seen_id}")
    else:
        since = since_date
        last_seen_id = 0
        print(f"  Starting fresh from {since}")

    # Load PR info for author matching
    pr_info = load_pr_authors(conn, repo)
    # Also load from predecessor repos for runtime
    if repo == "dotnet/runtime":
        for pred in ("dotnet/coreclr", "dotnet/corefx"):
            pred_info = load_pr_authors(conn, pred)
            # These PRs are in the items table under their original repo
            # but comments come from the runtime repo — skip these
    print(f"  Loaded {len(pr_info)} PR records for author matching")

    # Load existing first comments to avoid duplicating
    existing = set()
    for (num,) in conn.execute(
        "SELECT number FROM pr_first_comment WHERE repo = ?", (repo,)
    ).fetchall():
        existing.add(num)

    params = {
        "sort": "created",
        "direction": "asc",
        "since": since,
        "per_page": 100,
    }

    page = 0
    total_comments = 0
    new_first_comments = 0
    batch = []

    while not _shutdown:
        page += 1
        resp = fetch_page(session, url, params)
        if resp is None:
            break

        comments = resp.json()
        if not comments:
            print(f"  Done — no more comments.")
            break

        for c in comments:
            comment_id = c["id"]
            if comment_id <= last_seen_id:
                continue

            # Extract PR number from issue_url
            issue_url = c.get("issue_url", "")
            try:
                pr_number = int(issue_url.rstrip("/").split("/")[-1])
            except (ValueError, IndexError):
                continue

            # Skip if we already have a first comment for this PR
            if pr_number in existing:
                continue

            user_obj = c.get("user")
            commenter = user_obj.get("login", "") if user_obj else ""
            created_at = c.get("created_at", "")

            info = pr_info.get(pr_number)
            if info is None:
                # Not a PR we know about (could be an issue), skip
                continue

            if is_qualifying_comment(commenter, info):
                batch.append((repo, pr_number, created_at, commenter))
                existing.add(pr_number)
                new_first_comments += 1

            last_seen_id = max(last_seen_id, comment_id)

        total_comments += len(comments)

        # Commit batch periodically
        if batch and page % 10 == 0:
            conn.executemany(
                "INSERT OR IGNORE INTO pr_first_comment (repo, number, first_comment_at, commenter) "
                "VALUES (?, ?, ?, ?)", batch
            )
            last_comment_ts = comments[-1].get("created_at", since)
            conn.execute(
                "INSERT OR REPLACE INTO comment_fetch_progress (repo, last_since, last_comment_id, updated_at) "
                "VALUES (?, ?, ?, ?)",
                (repo, last_comment_ts, last_seen_id,
                 datetime.now(timezone.utc).isoformat())
            )
            conn.commit()
            batch = []

        if page % 20 == 0:
            print(f"  Page {page}: {total_comments} comments scanned, "
                  f"{new_first_comments} first-comments found")

        # Follow pagination
        link = resp.headers.get("Link", "")
        if 'rel="next"' not in link:
            break
        # Extract next URL
        for part in link.split(","):
            if 'rel="next"' in part:
                next_url = part.split(";")[0].strip().strip("<>")
                url = next_url
                params = {}  # params are in the URL now
                break

        time.sleep(REQUEST_DELAY)

    # Final commit
    if batch:
        conn.executemany(
            "INSERT OR IGNORE INTO pr_first_comment (repo, number, first_comment_at, commenter) "
            "VALUES (?, ?, ?, ?)", batch
        )
    conn.execute(
        "INSERT OR REPLACE INTO comment_fetch_progress (repo, last_since, last_comment_id, updated_at) "
        "VALUES (?, ?, ?, ?)",
        (repo, since, last_seen_id, datetime.now(timezone.utc).isoformat())
    )
    conn.commit()

    print(f"  Complete: {total_comments} comments scanned, "
          f"{new_first_comments} new first-comments recorded")
    return new_first_comments


def fetch_review_comments_for_repo(session, conn, repo, since_date):
    """Fetch PR review comments (inline code review) using the bulk endpoint.
    
    This complements fetch_comments_for_repo which only gets issue-style comments.
    PR review comments are the primary feedback mechanism and often arrive before
    any issue-style comment.
    
    Updates pr_first_comment keeping the EARLIEST qualifying comment from either source.
    """
    owner, name = repo.split("/")
    url = f"https://api.github.com/repos/{owner}/{name}/pulls/comments"

    # Check progress for review comments (separate tracking)
    row = conn.execute(
        "SELECT last_since, last_comment_id FROM comment_fetch_progress WHERE repo = ?",
        (repo + "/reviews",)
    ).fetchone()

    if row and row[0]:
        since = row[0]
        last_seen_id = row[1] or 0
        print(f"  [review] Resuming from since={since}, last_id={last_seen_id}")
    else:
        since = since_date
        last_seen_id = 0
        print(f"  [review] Starting fresh from {since}")

    pr_info = load_pr_authors(conn, repo)
    print(f"  [review] Loaded {len(pr_info)} PR records for author matching")

    # Load existing first comments so we can compare timestamps
    existing_comments = {}
    for r_repo, num, ts in conn.execute(
        "SELECT repo, number, first_comment_at FROM pr_first_comment WHERE repo = ?",
        (repo,)
    ).fetchall():
        existing_comments[num] = ts

    params = {
        "sort": "created",
        "direction": "asc",
        "since": since,
        "per_page": 100,
    }

    page = 0
    total_comments = 0
    new_first = 0
    updated_first = 0
    batch_insert = []
    batch_update = []

    while not _shutdown:
        page += 1
        resp = fetch_page(session, url, params)
        if resp is None:
            break

        comments = resp.json()
        if not comments:
            print(f"  [review] Done — no more comments.")
            break

        for c in comments:
            comment_id = c["id"]
            if comment_id <= last_seen_id:
                continue

            # Extract PR number from pull_request_url
            pr_url = c.get("pull_request_url", "")
            try:
                pr_number = int(pr_url.rstrip("/").split("/")[-1])
            except (ValueError, IndexError):
                continue

            user_obj = c.get("user")
            commenter = user_obj.get("login", "") if user_obj else ""
            created_at = c.get("created_at", "")

            info = pr_info.get(pr_number)
            if info is None:
                continue

            if is_qualifying_comment(commenter, info):
                existing_ts = existing_comments.get(pr_number)
                if existing_ts is None:
                    # No existing comment — insert
                    batch_insert.append((repo, pr_number, created_at, commenter))
                    existing_comments[pr_number] = created_at
                    new_first += 1
                elif created_at < existing_ts:
                    # This review comment is earlier — update
                    batch_update.append((created_at, commenter, repo, pr_number))
                    existing_comments[pr_number] = created_at
                    updated_first += 1

            last_seen_id = max(last_seen_id, comment_id)

        total_comments += len(comments)

        # Commit periodically
        if (batch_insert or batch_update) and page % 10 == 0:
            if batch_insert:
                conn.executemany(
                    "INSERT OR IGNORE INTO pr_first_comment (repo, number, first_comment_at, commenter) "
                    "VALUES (?, ?, ?, ?)", batch_insert
                )
            if batch_update:
                conn.executemany(
                    "UPDATE pr_first_comment SET first_comment_at = ?, commenter = ? "
                    "WHERE repo = ? AND number = ?", batch_update
                )
            last_comment_ts = comments[-1].get("created_at", since)
            conn.execute(
                "INSERT OR REPLACE INTO comment_fetch_progress (repo, last_since, last_comment_id, updated_at) "
                "VALUES (?, ?, ?, ?)",
                (repo + "/reviews", last_comment_ts, last_seen_id,
                 datetime.now(timezone.utc).isoformat())
            )
            conn.commit()
            batch_insert = []
            batch_update = []

        if page % 20 == 0:
            print(f"  [review] Page {page}: {total_comments} scanned, "
                  f"{new_first} new + {updated_first} updated")

        # Follow pagination
        link = resp.headers.get("Link", "")
        if 'rel="next"' not in link:
            break
        for part in link.split(","):
            if 'rel="next"' in part:
                next_url = part.split(";")[0].strip().strip("<>")
                url = next_url
                params = {}
                break

        time.sleep(REQUEST_DELAY)

    # Final commit
    if batch_insert:
        conn.executemany(
            "INSERT OR IGNORE INTO pr_first_comment (repo, number, first_comment_at, commenter) "
            "VALUES (?, ?, ?, ?)", batch_insert
        )
    if batch_update:
        conn.executemany(
            "UPDATE pr_first_comment SET first_comment_at = ?, commenter = ? "
            "WHERE repo = ? AND number = ?", batch_update
        )
    conn.execute(
        "INSERT OR REPLACE INTO comment_fetch_progress (repo, last_since, last_comment_id, updated_at) "
        "VALUES (?, ?, ?, ?)",
        (repo + "/reviews", since, last_seen_id, datetime.now(timezone.utc).isoformat())
    )
    conn.commit()

    print(f"  [review] Complete: {total_comments} scanned, "
          f"{new_first} new + {updated_first} updated first-comments")
    return new_first + updated_first


def main():
    parser = argparse.ArgumentParser(description="Fetch first comment timestamps for PRs")
    parser.add_argument("--db", default=DEFAULT_DB, help="Database path")
    parser.add_argument("--repos", nargs="+", help="Specific repos to fetch")
    parser.add_argument("--since", help="Start date (YYYY-MM-DD), default ~14 months ago")
    args = parser.parse_args()

    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), args.db)
    conn = init_db(db_path)

    repos = args.repos or COMMENT_REPOS

    if args.since:
        since_date = args.since + "T00:00:00Z"
    else:
        since_date = (datetime.now(timezone.utc) - timedelta(days=425)).strftime("%Y-%m-%dT00:00:00Z")

    token = get_token()
    session = req.Session()
    session.headers.update({
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    })

    # Check rate limit
    resp = session.get("https://api.github.com/rate_limit")
    if resp.status_code == 200:
        rl = resp.json()["resources"]["core"]
        print(f"Rate limit: {rl['remaining']}/{rl['limit']} "
              f"(resets at {datetime.fromtimestamp(rl['reset']).strftime('%H:%M:%S')})")

    print(f"\nFetching comments since {since_date}")
    print(f"Target repos: {', '.join(repos)}\n")

    total = 0
    for repo in repos:
        if _shutdown:
            break
        print(f"\n{'='*60}")
        print(f"  {repo}")
        print(f"{'='*60}")
        n = fetch_comments_for_repo(session, conn, repo, since_date)
        total += n

    # Second pass: PR review comments (inline code review)
    print(f"\n\n{'#'*60}")
    print(f"  PHASE 2: PR review comments (inline code review)")
    print(f"{'#'*60}")
    for repo in repos:
        if _shutdown:
            break
        print(f"\n{'='*60}")
        print(f"  {repo} [review comments]")
        print(f"{'='*60}")
        n = fetch_review_comments_for_repo(session, conn, repo, since_date)
        total += n

    print(f"\n{'='*60}")
    print(f"  DONE — {total} total new first-comments recorded")
    print(f"{'='*60}")

    # Summary
    for repo in repos:
        count = conn.execute(
            "SELECT COUNT(*) FROM pr_first_comment WHERE repo = ?", (repo,)
        ).fetchone()[0]
        print(f"  {repo}: {count} PRs with first-comment data")

    conn.close()


if __name__ == "__main__":
    main()
