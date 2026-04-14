#!/usr/bin/env python3
"""
GitHub Repo Health Data Fetcher

Fetches all issues and PRs for specified repos, stores in SQLite.
Supports checkpoint/resume, rate limit handling, and unattended operation.

Usage:
    python fetch.py                          # Resume/complete initial fetch
    python fetch.py --update                 # Incremental update (~5-10 min)
    python fetch.py --reset                  # Full re-fetch from scratch (hours)
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
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Schema version — bump when DB schema changes in incompatible ways.
# --update refuses to run against a different version; use --reset to rebuild.
SCHEMA_VERSION = 1

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
    conn.execute("PRAGMA busy_timeout=60000")
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
            sync_started_at TEXT,
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
    # Migration: add sync_started_at to fetch_progress
    try:
        conn.execute("ALTER TABLE fetch_progress ADD COLUMN sync_started_at TEXT")
    except sqlite3.OperationalError:
        pass  # column already exists
    # Migration: add next_url for Link-header pagination resume
    try:
        conn.execute("ALTER TABLE fetch_progress ADD COLUMN next_url TEXT")
    except sqlite3.OperationalError:
        pass  # column already exists
    conn.commit()

    # Set schema version on fresh DB (user_version defaults to 0)
    current_version = conn.execute("PRAGMA user_version").fetchone()[0]
    if current_version == 0:
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
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

    Uses Link-header (cursor-based) pagination to avoid GitHub's 422 error
    that occurs with page-based pagination beyond ~10K results.

    item_type: 'issue' or 'pr'
    """
    owner, name = repo.split("/")

    base_url = (f"https://api.github.com/repos/{owner}/{name}/pulls"
                if item_type == "pr" else
                f"https://api.github.com/repos/{owner}/{name}/issues")

    # Check for existing progress
    row = conn.execute(
        "SELECT last_page, items_fetched, status, next_url FROM fetch_progress "
        "WHERE repo = ? AND item_type = ?",
        (repo, item_type)
    ).fetchone()

    if row and row[2] == "complete":
        print(f"  Already complete ({row[1]} items). Skipping.")
        return row[1]

    page = (row[0] + 1) if row else 1
    items_fetched = row[1] if row else 0
    # Resume from saved Link URL if available, otherwise start fresh
    saved_next_url = row[3] if row else None

    if saved_next_url:
        next_url = saved_next_url
        next_params = {}  # URL already contains all params
        print(f"  Resuming from page {page} ({items_fetched} items so far) via saved cursor")
    else:
        next_url = base_url
        next_params = {
            "state": "all",
            "per_page": 100,
            "sort": "created",
            "direction": "asc",
        }
        if page > 1:
            # Fallback: if we have a page but no URL (old checkpoint), use page param
            next_params["page"] = page
            print(f"  Resuming from page {page} ({items_fetched} items so far)")

    empty_streak = 0

    while not _shutdown_requested:
        resp = fetch_page(session, next_url, next_params)
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

            batch.append(parse_item(repo, item, is_pr=(item_type == "pr")))

        if batch:
            conn.executemany(UPSERT_SQL, batch)
            items_fetched += len(batch)

        # Follow Link header for next page (cursor-based pagination)
        link_header = resp.headers.get("Link", "")
        link_next = None
        for part in link_header.split(","):
            if 'rel="next"' in part:
                link_next = part.split(";")[0].strip().strip("<>")
                break

        # Checkpoint every page (save the next Link URL for resume)
        save_checkpoint(conn, repo, item_type, page, items_fetched, "in_progress",
                        next_url=link_next)

        # Progress log
        if page % 20 == 0 or page == 1:
            ts = datetime.now().strftime("%H:%M:%S")
            suffix = f" (skipped {skipped_prs} PRs)" if skipped_prs else ""
            print(f"  [{ts}] page {page}: {items_fetched} {item_type}s{suffix}")

        # End of data?
        if not link_next or len(data) < 100:
            break

        next_url = link_next
        next_params = {}  # Link URL already contains all params
        page += 1
        time.sleep(request_delay)

    if _shutdown_requested:
        save_checkpoint(conn, repo, item_type, page, items_fetched, "interrupted")
    else:
        save_checkpoint(conn, repo, item_type, page, items_fetched, "complete")
        print(f"  Complete: {items_fetched} {item_type}s")

    return items_fetched


def save_checkpoint(conn, repo, item_type, page, items_fetched, status,
                    next_url=None):
    """Persist progress to database."""
    conn.execute(
        "INSERT OR REPLACE INTO fetch_progress "
        "(repo, item_type, last_page, items_fetched, total_expected, updated_at, status, next_url) "
        "VALUES (?, ?, ?, ?, NULL, ?, ?, ?)",
        (repo, item_type, page, items_fetched,
         datetime.now(timezone.utc).isoformat(), status, next_url)
    )
    conn.commit()


# --- Upsert helper (shared by full fetch and update) ---

UPSERT_SQL = (
    "INSERT INTO items "
    "(repo, number, created_at, closed_at, state, is_pull_request, "
    "merged_at, labels, author, merged_by) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
    "ON CONFLICT(repo, number) DO UPDATE SET "
    "  created_at      = COALESCE(items.created_at, excluded.created_at), "
    "  is_pull_request = MAX(items.is_pull_request, excluded.is_pull_request), "
    "  closed_at       = excluded.closed_at, "
    "  state           = excluded.state, "
    "  merged_at       = COALESCE(excluded.merged_at, items.merged_at), "
    "  labels          = excluded.labels, "
    "  author          = COALESCE(items.author, excluded.author), "
    "  merged_by       = COALESCE(excluded.merged_by, items.merged_by)"
)


def parse_item(repo, item, is_pr):
    """Parse a GitHub API item into an upsert tuple."""
    labels = json.dumps([lb["name"] for lb in item.get("labels", [])])
    author = None
    user = item.get("user")
    if user and isinstance(user, dict):
        author = user.get("login")
    merged_by_login = None
    mb = item.get("merged_by")
    if mb and isinstance(mb, dict):
        merged_by_login = mb.get("login")
    return (
        repo,
        item["number"],
        item.get("created_at"),
        item.get("closed_at"),
        (item.get("state") or "").upper(),
        1 if is_pr else 0,
        item.get("merged_at"),
        labels,
        author,
        merged_by_login,
    )


def update_repo(conn, session, repo, request_delay):
    """
    Incremental update for a completed repo using /issues?since=.

    Uses the issues endpoint (which returns both issues and PRs) filtered by
    updated_at >= watermark. PRs discovered this way are hydrated individually
    via /pulls/{number} to get merged_at and merged_by.

    Returns (issues_updated, prs_updated) or None on failure.
    """
    owner, name = repo.split("/")

    # Get the oldest watermark across both issue and pr progress rows.
    # Both must be "complete" for --update to apply.
    rows = conn.execute(
        "SELECT item_type, status, sync_started_at FROM fetch_progress WHERE repo = ?",
        (repo,)
    ).fetchall()

    status_map = {r[0]: (r[1], r[2]) for r in rows}
    if not all(status_map.get(t, (None,))[0] == "complete" for t in ("issue", "pr")):
        print(f"  Not fully fetched yet — skipping update (run without --update first)")
        return None

    # Use oldest watermark minus 2-day overlap for safety
    watermarks = [s for _, (_, s) in status_map.items() if s]
    if not watermarks:
        print(f"  No watermark found — run full fetch first (no --update)")
        return None

    oldest_watermark = min(watermarks)
    since_dt = datetime.fromisoformat(oldest_watermark.replace("Z", "+00:00")) - timedelta(days=2)
    since_str = since_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"  Watermark: {oldest_watermark}")
    print(f"  Fetching changes since: {since_str} (2-day overlap)")

    # Fetch all changed items via /issues?since=
    # Use Link header pagination (not page numbers) to handle large result sets.
    # GitHub returns 422 for page-based pagination beyond ~10K results.
    next_url = f"https://api.github.com/repos/{owner}/{name}/issues"
    next_params = {
        "state": "all",
        "per_page": 100,
        "sort": "updated",
        "direction": "asc",
        "since": since_str,
    }
    page = 0
    issues_updated = 0
    prs_to_hydrate = set()

    while not _shutdown_requested:
        resp = fetch_page(session, next_url, next_params)
        if resp is None:
            return None

        data = resp.json()
        if not data:
            break

        page += 1
        batch = []
        for item in data:
            is_pr = "pull_request" in item
            if is_pr:
                # Check if we need to hydrate this PR
                number = item["number"]
                existing = conn.execute(
                    "SELECT state, merged_at FROM items WHERE repo = ? AND number = ?",
                    (repo, number)
                ).fetchone()
                new_state = (item.get("state") or "").upper()
                needs_hydration = (
                    existing is None  # new PR
                    or existing[0] != new_state  # state changed
                    or (new_state == "CLOSED" and not existing[1])  # closed but no merged_at
                )
                if needs_hydration:
                    prs_to_hydrate.add(number)
                else:
                    # Still upsert the non-PR-specific fields (labels, etc.)
                    batch.append(parse_item(repo, item, is_pr=True))
            else:
                batch.append(parse_item(repo, item, is_pr=False))
                issues_updated += 1

        if batch:
            conn.executemany(UPSERT_SQL, batch)
            conn.commit()

        if page == 1 or page % 10 == 0:
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"  [{ts}] page {page}: {issues_updated} issues, "
                  f"{len(prs_to_hydrate)} PRs queued for hydration")

        # Follow Link header for next page (cursor-based pagination)
        link_header = resp.headers.get("Link", "")
        next_link = None
        for part in link_header.split(","):
            if 'rel="next"' in part:
                next_link = part.split(";")[0].strip().strip("<>")
                break

        if not next_link or len(data) < 100:
            break

        next_url = next_link
        next_params = {}  # URL already contains all params
        time.sleep(request_delay)

    if _shutdown_requested:
        return None

    # Hydrate PRs that need merged_at/merged_by
    prs_updated = 0
    prs_to_hydrate = sorted(prs_to_hydrate)
    if prs_to_hydrate:
        print(f"  Hydrating {len(prs_to_hydrate)} PRs...")
        for i, number in enumerate(prs_to_hydrate):
            if _shutdown_requested:
                return None

            pr_url = f"https://api.github.com/repos/{owner}/{name}/pulls/{number}"
            resp = fetch_page(session, pr_url, {})
            if resp is None:
                print(f"  WARNING: Failed to hydrate PR #{number}, skipping")
                continue

            pr_data = resp.json()
            row = parse_item(repo, pr_data, is_pr=True)
            conn.execute(UPSERT_SQL, row)
            prs_updated += 1

            if (i + 1) % 50 == 0:
                conn.commit()
                ts = datetime.now().strftime("%H:%M:%S")
                print(f"  [{ts}] hydrated {i + 1}/{len(prs_to_hydrate)} PRs")

            time.sleep(request_delay)

        conn.commit()

    print(f"  Updated: {issues_updated} issues, {prs_updated} PRs")
    return issues_updated, prs_updated


def hydrate_merged_by(conn, session, repo, request_delay):
    """
    Backfill merged_by for merged PRs that have NULL merged_by.

    The /pulls list endpoint doesn't return merged_by — only the individual
    /pulls/{number} endpoint does. This function fetches each such PR
    individually to populate the field.

    Returns the number of PRs hydrated, or None on interruption.
    """
    owner, name = repo.split("/")

    rows = conn.execute(
        "SELECT number FROM items "
        "WHERE repo = ? AND is_pull_request = 1 AND merged_at IS NOT NULL "
        "AND merged_by IS NULL ORDER BY number",
        (repo,)
    ).fetchall()

    total = len(rows)
    if total == 0:
        print(f"  No merged PRs with NULL merged_by — nothing to hydrate")
        return 0

    print(f"  {total:,} merged PRs need merged_by hydration")
    hydrated = 0
    failed = 0

    for i, (number,) in enumerate(rows):
        if _shutdown_requested:
            print(f"  Interrupted after hydrating {hydrated}/{total}")
            conn.commit()
            return None

        url = f"https://api.github.com/repos/{owner}/{name}/pulls/{number}"
        resp = fetch_page(session, url, {})
        if resp is None:
            failed += 1
            if failed > 20:
                print(f"  Too many failures ({failed}), stopping hydration")
                conn.commit()
                return None
            continue

        pr_data = resp.json()
        row = parse_item(repo, pr_data, is_pr=True)
        for _attempt in range(5):
            try:
                conn.execute(UPSERT_SQL, row)
                break
            except sqlite3.OperationalError as e:
                if "locked" in str(e) and _attempt < 4:
                    time.sleep(2 ** _attempt)
                    continue
                raise
        hydrated += 1

        if hydrated % 100 == 0:
            for _attempt in range(5):
                try:
                    conn.commit()
                    break
                except sqlite3.OperationalError as e:
                    if "locked" in str(e) and _attempt < 4:
                        time.sleep(2 ** _attempt)
                        continue
                    raise
            ts = datetime.now().strftime("%H:%M:%S")
            remaining = total - i - 1
            rate = hydrated / max((time.time() - hydrate_merged_by._start_time), 1)
            eta_min = remaining / max(rate, 0.01) / 60
            print(f"  [{ts}] hydrated {hydrated}/{total} "
                  f"({failed} failed, ~{eta_min:.0f} min remaining)")

        time.sleep(request_delay)

    conn.commit()
    if failed:
        print(f"  Hydrated {hydrated}/{total} ({failed} failed)")
    else:
        print(f"  Hydrated {hydrated}/{total}")
    return hydrated


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
    parser.add_argument(
        "--update", action="store_true",
        help="Incremental update: fetch only items changed since last sync (~5-10 min)"
    )
    parser.add_argument(
        "--hydrate", action="store_true",
        help="Backfill merged_by for merged PRs (fetches individual PR details)"
    )
    args = parser.parse_args()

    if args.reset and args.update:
        print("ERROR: --reset and --update are mutually exclusive.")
        sys.exit(1)

    repos = args.repos or REPOS
    db_path = str(Path(args.db).resolve())
    mode = "hydrate" if args.hydrate else ("update" if args.update else ("reset" if args.reset else "fetch"))

    # Banner
    print("=" * 60)
    print("  GitHub Repo Health Data Fetcher")
    print("=" * 60)
    print(f"  Mode     : {mode}")
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

    # Schema version check (for --update only)
    if args.update:
        db_version = conn.execute("PRAGMA user_version").fetchone()[0]
        if db_version != SCHEMA_VERSION:
            print(f"ERROR: DB schema version {db_version} != expected {SCHEMA_VERSION}.")
            print(f"  Run with --reset to rebuild the database.")
            conn.close()
            sys.exit(1)

    if args.reset:
        print("Resetting all progress checkpoints...")
        conn.execute("DELETE FROM fetch_progress")
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        conn.commit()

    start_time = time.time()
    sync_started_at = datetime.now(timezone.utc).isoformat()

    if args.hydrate:
        # --- Hydrate merged_by mode ---
        hydrate_merged_by._start_time = time.time()
        for i, repo in enumerate(repos):
            if _shutdown_requested:
                break

            print(f"\n{'='*60}")
            print(f"[{i+1}/{len(repos)}] {repo} (hydrate merged_by)")
            print(f"{'='*60}")

            hydrate_merged_by(conn, session, repo, args.delay)

    elif args.update:
        # --- Incremental update mode ---
        any_failed = False
        for i, repo in enumerate(repos):
            if _shutdown_requested:
                any_failed = True
                break

            print(f"\n{'='*60}")
            print(f"[{i+1}/{len(repos)}] {repo} (update)")
            print(f"{'='*60}")

            result = update_repo(conn, session, repo, args.delay)
            if result is None:
                if _shutdown_requested:
                    any_failed = True
                    break
                # Distinguish "skipped (not complete)" from "failed"
                # update_repo prints the reason; either way don't advance watermark
                any_failed = True

        # Only persist watermark if no repos failed/were interrupted
        if not any_failed:
            for repo in repos:
                for item_type in ("issue", "pr"):
                    conn.execute(
                        "UPDATE fetch_progress SET sync_started_at = ? "
                        "WHERE repo = ? AND item_type = ?",
                        (sync_started_at, repo, item_type)
                    )
            conn.commit()
            print(f"\n  Watermark advanced to {sync_started_at}")
        else:
            print(f"\n  Watermark NOT advanced (incomplete or failed run)")
    else:
        # --- Full fetch mode ---
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

        # Set watermark for completed repos (full fetch)
        if not _shutdown_requested:
            for repo in repos:
                for item_type in ("issue", "pr"):
                    row = conn.execute(
                        "SELECT status FROM fetch_progress WHERE repo = ? AND item_type = ?",
                        (repo, item_type)
                    ).fetchone()
                    if row and row[0] == "complete":
                        conn.execute(
                            "UPDATE fetch_progress SET sync_started_at = ? "
                            "WHERE repo = ? AND item_type = ?",
                            (sync_started_at, repo, item_type)
                        )
            conn.commit()

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
