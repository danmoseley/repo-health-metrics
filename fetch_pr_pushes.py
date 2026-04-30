#!/usr/bin/env python3
"""
Fetch CI-trigger ("push") events for PRs.

For each PR, queries the GitHub REST timeline endpoint and records:
  - 'committed' events       — keyed by commit SHA, timestamp = committer.date
  - 'head_ref_force_pushed'  — keyed by event id, timestamp = created_at

Caveat: 'committed' events expose committer.date (the local-commit timestamp),
not the push timestamp. For developers who rebase before pushing this is within
minutes of the actual push; for local-commit-then-push-later workflows it can
diverge by hours/days. Force-push events DO carry the actual push timestamp.

This data lets us cluster events into "pushes" (≥ N-min gap → new push)
elsewhere; storing raw events keeps the threshold tunable without re-fetching.

Usage:
    python fetch_pr_pushes.py                                   # default repos
    python fetch_pr_pushes.py --repos dotnet/runtime
    python fetch_pr_pushes.py --since 2024-11-01
    python fetch_pr_pushes.py --refresh-open                    # re-fetch open
                                                                  + recently
                                                                  closed PRs
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
REQUEST_DELAY = 0.4

# Repos to fetch push events for. Start with the dotnet 4; vcpkg has heavy
# bot/auto-rebase activity that would skew the metric and is intentionally
# excluded from comparison.
DEFAULT_REPOS = [
    "dotnet/runtime",
    "dotnet/roslyn",
    "dotnet/maui",
    "microsoft/aspire",
]

# How recent a closed PR must be to still get re-fetched (catches late events).
RECENT_CLOSED_DAYS = 7

_shutdown = False


def signal_handler(sig, frame):
    global _shutdown
    if _shutdown:
        sys.exit(1)
    print("\nShutdown requested — finishing current PR...")
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
        CREATE TABLE IF NOT EXISTS pr_push_events (
            repo TEXT NOT NULL,
            number INTEGER NOT NULL,
            event_id TEXT NOT NULL,         -- commit SHA or numeric event id
            kind TEXT NOT NULL,             -- 'committed' or 'force_pushed'
            ts TEXT NOT NULL,               -- ISO timestamp
            PRIMARY KEY (repo, number, event_id)
        );
        CREATE INDEX IF NOT EXISTS idx_pr_push_events_repo_number
            ON pr_push_events(repo, number);

        CREATE TABLE IF NOT EXISTS pr_push_progress (
            repo TEXT NOT NULL,
            number INTEGER NOT NULL,
            last_fetched_at TEXT NOT NULL,
            is_complete INTEGER NOT NULL DEFAULT 1,  -- 1 if we walked all pages
            PRIMARY KEY (repo, number)
        );
    """)
    conn.commit()
    return conn


def fetch_page(session, url, params, max_retries=5):
    """Fetch a single API page. Rate-limit responses don't consume max_retries."""
    rate_limit_retries = 0
    attempt = 0

    while attempt < max_retries:
        if _shutdown:
            return None
        try:
            resp = session.get(url, params=params, timeout=30)
        except req.exceptions.RequestException as e:
            wait = min(4 ** attempt, 120)
            print(f"  Network error: {e}, retry in {wait}s...")
            time.sleep(wait)
            attempt += 1
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
            attempt += 1
            continue

        if resp.status_code == 404:
            # PR may have been deleted; treat as no events
            return resp

        print(f"  Unexpected status {resp.status_code}: {resp.text[:200]}")
        return None

    return None


def fetch_pr_timeline(session, repo, number):
    """Walk all timeline pages for a PR and yield (event_id, kind, ts) tuples.
    Returns (events_list, is_complete). is_complete=False if we hit an
    unrecoverable error mid-fetch."""
    owner, name = repo.split("/")
    url = f"https://api.github.com/repos/{owner}/{name}/issues/{number}/timeline"
    page = 1
    events = []
    while True:
        if _shutdown:
            return events, False
        params = {"per_page": 100, "page": page}
        resp = fetch_page(session, url, params)
        if resp is None:
            return events, False
        if resp.status_code == 404:
            return events, True  # treat missing PR as "complete" (no events)
        items = resp.json()
        if not items:
            break
        for item in items:
            ev = item.get("event")
            if ev == "committed":
                sha = item.get("sha")
                committer = item.get("committer") or {}
                ts = committer.get("date")
                if sha and ts:
                    events.append((sha, "committed", ts))
            elif ev == "head_ref_force_pushed":
                eid = item.get("id")
                ts = item.get("created_at")
                if eid is not None and ts:
                    events.append((str(eid), "force_pushed", ts))
        if len(items) < 100:
            break
        page += 1
        time.sleep(REQUEST_DELAY)
    return events, True


def select_prs_to_fetch(conn, repo, since_iso, refresh_open):
    """Return list of PR numbers to fetch.
    - All merged PRs created on/after since_iso, not yet recorded as complete
    - Plus open PRs and recently-closed PRs if refresh_open=True"""
    rows = conn.execute(
        "SELECT number, state, merged_at, closed_at FROM items "
        "WHERE repo = ? AND is_pull_request = 1 AND created_at >= ?",
        (repo, since_iso),
    ).fetchall()

    progress = dict(conn.execute(
        "SELECT number, is_complete FROM pr_push_progress WHERE repo = ?",
        (repo,)
    ).fetchall())

    recent_cutoff = (datetime.now(timezone.utc) - timedelta(days=RECENT_CLOSED_DAYS)).isoformat()

    result = []
    for number, state, merged_at, closed_at in rows:
        already_complete = progress.get(number) == 1
        is_merged = bool(merged_at)
        is_open = state == "OPEN" or (state == "open")
        if refresh_open and is_open:
            result.append(number)
            continue
        if refresh_open and closed_at and closed_at >= recent_cutoff:
            result.append(number)
            continue
        if already_complete:
            continue
        # Otherwise: fetch if we don't have it yet
        result.append(number)
    return sorted(result)


def main():
    parser = argparse.ArgumentParser(description="Fetch PR push/CI-trigger events.")
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--repos", nargs="+", default=DEFAULT_REPOS)
    parser.add_argument("--since", default=None,
                        help="ISO date; default = 18 months ago")
    parser.add_argument("--refresh-open", action="store_true",
                        help="Re-fetch open PRs and recently closed PRs")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max PRs to fetch per repo (debug)")
    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f"ERROR: DB not found: {args.db}")
        sys.exit(1)

    if args.since:
        since_iso = args.since
        if "T" not in since_iso:
            since_iso += "T00:00:00Z"
    else:
        since_iso = (datetime.now(timezone.utc) - timedelta(days=18 * 30)).isoformat()

    conn = init_db(args.db)
    token = get_token()
    session = req.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    })

    for repo in args.repos:
        if _shutdown:
            break
        print(f"\n=== {repo} (since {since_iso[:10]}) ===")
        prs = select_prs_to_fetch(conn, repo, since_iso, args.refresh_open)
        if args.limit:
            prs = prs[:args.limit]
        print(f"  {len(prs):,} PRs to fetch")

        batch = []
        completed_progress = []
        for i, number in enumerate(prs, 1):
            if _shutdown:
                break
            events, complete = fetch_pr_timeline(session, repo, number)
            for event_id, kind, ts in events:
                batch.append((repo, number, event_id, kind, ts))
            completed_progress.append((repo, number,
                                       datetime.now(timezone.utc).isoformat(),
                                       1 if complete else 0))

            if i % 25 == 0 or i == len(prs):
                print(f"  [{i}/{len(prs)}] events buffered: {len(batch)}")
                if batch:
                    conn.executemany(
                        "INSERT OR IGNORE INTO pr_push_events "
                        "(repo, number, event_id, kind, ts) VALUES (?,?,?,?,?)",
                        batch,
                    )
                    batch.clear()
                if completed_progress:
                    conn.executemany(
                        "INSERT INTO pr_push_progress "
                        "(repo, number, last_fetched_at, is_complete) "
                        "VALUES (?,?,?,?) "
                        "ON CONFLICT(repo, number) DO UPDATE SET "
                        "  last_fetched_at = excluded.last_fetched_at, "
                        "  is_complete = excluded.is_complete",
                        completed_progress,
                    )
                    completed_progress.clear()
                conn.commit()
            time.sleep(REQUEST_DELAY)

        # Flush any tail
        if batch:
            conn.executemany(
                "INSERT OR IGNORE INTO pr_push_events "
                "(repo, number, event_id, kind, ts) VALUES (?,?,?,?,?)",
                batch,
            )
        if completed_progress:
            conn.executemany(
                "INSERT INTO pr_push_progress "
                "(repo, number, last_fetched_at, is_complete) "
                "VALUES (?,?,?,?) "
                "ON CONFLICT(repo, number) DO UPDATE SET "
                "  last_fetched_at = excluded.last_fetched_at, "
                "  is_complete = excluded.is_complete",
                completed_progress,
            )
        conn.commit()

    # Summary
    print("\n=== Summary ===")
    for repo in args.repos:
        n_prs = conn.execute(
            "SELECT COUNT(DISTINCT number) FROM pr_push_events WHERE repo = ?",
            (repo,)
        ).fetchone()[0]
        n_events = conn.execute(
            "SELECT COUNT(*) FROM pr_push_events WHERE repo = ?",
            (repo,)
        ).fetchone()[0]
        print(f"  {repo}: {n_prs:,} PRs, {n_events:,} events")
    conn.close()


if __name__ == "__main__":
    main()
