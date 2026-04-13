#!/usr/bin/env python3
"""
GitHub Repo Health Data Fetcher

Fetches all issues and PRs for specified repos, stores in SQLite.
Supports checkpoint/resume, rate limit handling, and unattended operation.

Usage:
    python fetch.py                          # Fetch all repos now
    python fetch.py --wait 14400             # Wait 4 hours then fetch
    python fetch.py --repos dotnet/runtime   # Fetch one repo only
    python fetch.py --db mydata.db           # Custom DB path
"""

import requests as req
import sqlite3
import time
import json
import os
import sys
import signal
from datetime import datetime, timezone
from pathlib import Path

REPOS = [
    "golang/go",          # smallest, good for pipeline validation
    "microsoft/aspire",   # aspire moved from dotnet/ to microsoft/ ~Mar 2026
    "dotnet/maui",
    "dotnet/roslyn",
    "dotnet/runtime",
    "rust-lang/rust",
    "microsoft/vscode",   # largest, fetch last
]

DEFAULT_DB = "pr-dashboard.db"
REQUEST_DELAY = 0.5  # seconds between requests (conservative; ~7200 req/hr headroom)

# Graceful shutdown flag
_shutdown_requested = False


def signal_handler(sig, frame):
    global _shutdown_requested
    if _shutdown_requested:
        print("\nForce quit.")
        sys.exit(1)
    print("\nShutdown requested -- finishing current page and saving checkpoint...")
    _shutdown_requested = True


signal.signal(signal.SIGINT, signal_handler)
if hasattr(signal, "SIGBREAK"):
    signal.signal(signal.SIGBREAK, signal_handler)


def get_token():
    """Get GitHub token from env var or gh CLI."""
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
        print("  Set GITHUB_TOKEN env var, or authenticate with `gh auth login`.")
        sys.exit(1)
    return token


def init_db(db_path):
    """Initialize SQLite database with schema."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS items (
            repo TEXT NOT NULL,
            number INTEGER NOT NULL,
            created_at TEXT,
            closed_at TEXT,
            state TEXT,
            is_pull_request INTEGER NOT NULL DEFAULT 0,
            merged_at TEXT,
            labels TEXT,
            author TEXT,
            merged_by TEXT,
            PRIMARY KEY (repo, number)
        );

        CREATE TABLE IF NOT EXISTS fetch_progress (
            repo TEXT NOT NULL,
            item_type TEXT NOT NULL,
            last_page INTEGER NOT NULL DEFAULT 0,
            items_fetched INTEGER NOT NULL DEFAULT 0,
            total_expected INTEGER,
            updated_at TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            PRIMARY KEY (repo, item_type)
        );

        CREATE INDEX IF NOT EXISTS idx_items_repo_type ON items(repo, is_pull_request);
        CREATE INDEX IF NOT EXISTS idx_items_created ON items(repo, created_at);
    """)
    # Migration: add columns if they don't exist (for DBs created before this change)
    for col in ("author", "merged_by"):
        try:
            conn.execute(f"ALTER TABLE items ADD COLUMN {col} TEXT")
        except sqlite3.OperationalError:
            pass  # column already exists
    conn.commit()
    return conn


def check_rate_limit(response):
    """Check rate limit headers. Returns (remaining, reset_timestamp)."""
    remaining = int(response.headers.get("X-RateLimit-Remaining", 9999))
    reset_ts = int(response.headers.get("X-RateLimit-Reset", 0))
    return remaining, reset_ts


def wait_for_rate_limit(remaining, reset_ts, context=""):
    """Sleep until rate limit resets if remaining is low."""
    if remaining >= 100:
        return
    now = time.time()
    wait = max(reset_ts - now, 0) + 5
    reset_time = datetime.fromtimestamp(reset_ts).strftime("%H:%M:%S")
    print(f"  {context}Rate limit low ({remaining} remaining). "
          f"Sleeping until {reset_time} ({wait:.0f}s)...")
    time.sleep(wait)


def fetch_page(session, url, params, max_retries=5):
    """Fetch a single API page with retry logic and rate limit handling."""
    rate_limit_retries = 0
    max_rate_limit_retries = 10  # cap to avoid infinite loop on persistent 403

    for attempt in range(max_retries):
        if _shutdown_requested:
            return None

        try:
            resp = session.get(url, params=params, timeout=30)
        except req.exceptions.RequestException as e:
            wait = min(4 ** attempt, 120)
            print(f"  Network error: {e}")
            print(f"  Retry {attempt + 1}/{max_retries} in {wait}s...")
            time.sleep(wait)
            continue

        remaining, reset_ts = check_rate_limit(resp)

        if resp.status_code == 200:
            # Proactive throttle if getting low
            wait_for_rate_limit(remaining, reset_ts)
            return resp

        if resp.status_code == 401:
            print(f"  Authentication failed (401). Token may be expired or revoked.")
            print(f"  Response: {resp.text[:300]}")
            return None  # unrecoverable without new token

        if resp.status_code == 403:
            rate_limit_retries += 1
            if rate_limit_retries > max_rate_limit_retries:
                print(f"  FAILED: {max_rate_limit_retries} rate limit retries exhausted. "
                      f"Possible persistent auth/abuse issue.")
                return None

            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                wait = int(retry_after) + 5
                print(f"  Secondary rate limit. Retry-After: {wait}s "
                      f"(attempt {rate_limit_retries}/{max_rate_limit_retries})")
            else:
                wait = max(reset_ts - time.time(), 60) + 5
                print(f"  Primary rate limit hit ({remaining} remaining). "
                      f"(attempt {rate_limit_retries}/{max_rate_limit_retries})")
            print(f"  Sleeping {wait:.0f}s...")
            time.sleep(wait)
            continue  # retry, don't count against max_retries

        if resp.status_code == 304:
            return resp  # Not Modified (conditional request)

        if resp.status_code >= 500:
            wait = min(4 ** attempt, 120)
            print(f"  Server error {resp.status_code}. Retry in {wait}s...")
            time.sleep(wait)
            continue

        if resp.status_code == 422:
            print(f"  Validation error: {resp.text[:300]}")
            return None

        print(f"  Unexpected status {resp.status_code}: {resp.text[:300]}")
        return None

    print(f"  FAILED after {max_retries} retries")
    return None


def fetch_items(conn, session, repo, item_type, request_delay):
    """
    Fetch all issues or PRs for a repo with checkpoint/resume.

    item_type: 'issue' or 'pr'
    """
    owner, name = repo.split("/")

    if item_type == "pr":
        url = f"https://api.github.com/repos/{owner}/{name}/pulls"
    else:
        url = f"https://api.github.com/repos/{owner}/{name}/issues"

    # Check for existing progress
    row = conn.execute(
        "SELECT last_page, items_fetched, status FROM fetch_progress "
        "WHERE repo = ? AND item_type = ?",
        (repo, item_type)
    ).fetchone()

    if row and row[2] == "complete":
        print(f"  Already complete ({row[1]} items). Skipping.")
        return row[1]

    start_page = (row[0] + 1) if row else 1
    items_fetched = row[1] if row else 0

    if start_page > 1:
        print(f"  Resuming from page {start_page} ({items_fetched} items so far)")

    page = start_page
    empty_streak = 0

    while not _shutdown_requested:
        params = {
            "state": "all",
            "per_page": 100,
            "page": page,
            "sort": "created",
            "direction": "asc",
        }

        resp = fetch_page(session, url, params)
        if resp is None:
            # Save checkpoint on failure or shutdown
            save_checkpoint(conn, repo, item_type, page - 1, items_fetched,
                           "interrupted" if _shutdown_requested else "failed")
            return items_fetched

        data = resp.json()

        if not data:
            empty_streak += 1
            if empty_streak >= 2:
                break  # confirmed end of data
            page += 1
            time.sleep(request_delay)
            continue
        empty_streak = 0

        # Parse and insert
        batch = []
        skipped_prs = 0
        for item in data:
            # When fetching issues, the /issues endpoint also returns PRs.
            # Skip them here -- we fetch PRs separately via /pulls for merged_at.
            if item_type == "issue" and "pull_request" in item:
                skipped_prs += 1
                continue

            is_pr = 1 if item_type == "pr" else 0
            labels = json.dumps([lb["name"] for lb in item.get("labels", [])])

            # Extract author and merged_by (nested objects)
            author = None
            user = item.get("user")
            if user and isinstance(user, dict):
                author = user.get("login")

            merged_by_login = None
            mb = item.get("merged_by")
            if mb and isinstance(mb, dict):
                merged_by_login = mb.get("login")

            batch.append((
                repo,
                item["number"],
                item.get("created_at"),
                item.get("closed_at"),
                (item.get("state") or "").upper(),
                is_pr,
                item.get("merged_at"),  # only present from /pulls
                labels,
                author,
                merged_by_login,
            ))

        if batch:
            conn.executemany(
                "INSERT OR REPLACE INTO items "
                "(repo, number, created_at, closed_at, state, is_pull_request, "
                "merged_at, labels, author, merged_by) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                batch
            )
            items_fetched += len(batch)

        # Checkpoint every page
        save_checkpoint(conn, repo, item_type, page, items_fetched, "in_progress")

        # Progress log
        if page % 20 == 0 or page == start_page:
            ts = datetime.now().strftime("%H:%M:%S")
            suffix = f" (skipped {skipped_prs} PRs)" if skipped_prs else ""
            print(f"  [{ts}] page {page}: {items_fetched} {item_type}s{suffix}")

        # End of data?
        if len(data) < 100:
            break

        page += 1
        time.sleep(request_delay)

    if _shutdown_requested:
        save_checkpoint(conn, repo, item_type, page, items_fetched, "interrupted")
    else:
        save_checkpoint(conn, repo, item_type, page, items_fetched, "complete")
        print(f"  Complete: {items_fetched} {item_type}s")

    return items_fetched


def save_checkpoint(conn, repo, item_type, page, items_fetched, status):
    """Persist progress to database."""
    conn.execute(
        "INSERT OR REPLACE INTO fetch_progress "
        "(repo, item_type, last_page, items_fetched, total_expected, updated_at, status) "
        "VALUES (?, ?, ?, ?, NULL, ?, ?)",
        (repo, item_type, page, items_fetched,
         datetime.now(timezone.utc).isoformat(), status)
    )
    conn.commit()


def print_rate_limit(session):
    """Print current rate limit status."""
    try:
        resp = session.get("https://api.github.com/rate_limit", timeout=10)
        rl = resp.json()["resources"]["core"]
        reset_time = datetime.fromtimestamp(rl["reset"]).strftime("%H:%M:%S")
        print(f"Rate limit: {rl['remaining']}/{rl['limit']} "
              f"(resets at {reset_time})")
        return rl["remaining"]
    except Exception as e:
        print(f"Could not check rate limit: {e}")
        return None


def print_summary(conn, repos):
    """Print final summary of fetched data."""
    total = conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
    print(f"\nTotal items in database: {total:,}")
    print()
    for repo in repos:
        issues = conn.execute(
            "SELECT COUNT(*) FROM items WHERE repo = ? AND is_pull_request = 0",
            (repo,)
        ).fetchone()[0]
        prs = conn.execute(
            "SELECT COUNT(*) FROM items WHERE repo = ? AND is_pull_request = 1",
            (repo,)
        ).fetchone()[0]
        progress = conn.execute(
            "SELECT item_type, status, items_fetched FROM fetch_progress WHERE repo = ?",
            (repo,)
        ).fetchall()
        status_str = ", ".join(f"{r[0]}:{r[1]}" for r in progress)
        print(f"  {repo}: {issues:,} issues, {prs:,} PRs [{status_str}]")


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Fetch GitHub issue/PR data for repo health analysis"
    )
    parser.add_argument(
        "--db", default=DEFAULT_DB,
        help=f"SQLite database path (default: {DEFAULT_DB})"
    )
    parser.add_argument(
        "--repos", nargs="*",
        help="Override repo list (e.g., dotnet/runtime golang/go)"
    )
    parser.add_argument(
        "--delay", type=float, default=REQUEST_DELAY,
        help=f"Delay between requests in seconds (default: {REQUEST_DELAY})"
    )
    parser.add_argument(
        "--wait", type=int, default=0,
        help="Wait N seconds before starting (e.g., --wait 14400 for 4 hours)"
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="Clear all progress checkpoints and re-fetch from scratch"
    )
    args = parser.parse_args()

    repos = args.repos or REPOS
    db_path = str(Path(args.db).resolve())

    # Banner
    print("=" * 60)
    print("  GitHub Repo Health Data Fetcher")
    print("=" * 60)
    print(f"  Database : {db_path}")
    print(f"  Repos    : {', '.join(repos)}")
    print(f"  Delay    : {args.delay}s between requests")
    if args.wait > 0:
        print(f"  Wait     : {args.wait}s ({args.wait/3600:.1f} hours)")
    print("=" * 60)

    # Wait if requested
    if args.wait > 0:
        launch_time = datetime.fromtimestamp(time.time() + args.wait)
        print(f"\nWaiting until ~{launch_time.strftime('%H:%M:%S')}...")
        # Sleep in 30s increments so Ctrl+C works
        remaining = args.wait
        while remaining > 0 and not _shutdown_requested:
            chunk = min(remaining, 30)
            time.sleep(chunk)
            remaining -= chunk
        if _shutdown_requested:
            print("Cancelled during wait.")
            return

    # Auth
    token = get_token()
    session = req.Session()
    session.headers.update({
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "pr-dashboard-fetcher/1.0",
    })

    # Validate token and check limits
    print()
    remaining = print_rate_limit(session)
    if remaining is not None and remaining < 200:
        print("WARNING: Rate limit is low. Script will auto-pause when needed.")
    print()

    # Init DB
    conn = init_db(db_path)

    if args.reset:
        print("Resetting all progress checkpoints...")
        conn.execute("DELETE FROM fetch_progress")
        conn.commit()

    start_time = time.time()

    # Fetch each repo
    for i, repo in enumerate(repos):
        if _shutdown_requested:
            break

        print(f"\n{'='*60}")
        print(f"[{i+1}/{len(repos)}] {repo}")
        print(f"{'='*60}")

        print(f"\n  --- Pull Requests ---")
        pr_count = fetch_items(conn, session, repo, "pr", args.delay)
        if _shutdown_requested:
            break

        print(f"\n  --- Issues ---")
        issue_count = fetch_items(conn, session, repo, "issue", args.delay)
        if _shutdown_requested:
            break

        print(f"\n  Repo total: {issue_count:,} issues + {pr_count:,} PRs")

    # Summary
    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    if _shutdown_requested:
        print(f"  INTERRUPTED after {elapsed/60:.1f} minutes")
        print(f"  Progress saved -- re-run to resume from checkpoint")
    else:
        print(f"  COMPLETE in {elapsed/60:.1f} minutes")
    print(f"{'='*60}")

    print_summary(conn, repos)
    print_rate_limit(session)

    conn.close()


if __name__ == "__main__":
    main()
