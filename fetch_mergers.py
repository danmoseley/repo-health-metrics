#!/usr/bin/env python3
"""
Fetch merged_by data via GraphQL for all merged PRs.

The REST /pulls list endpoint doesn't return merged_by — only the individual
PR detail endpoint does. GraphQL can return mergedBy in list queries efficiently.

This script supplements fetch.py by filling in the merged_by column.
"""

import sqlite3
import json
import time
import os
import sys
import subprocess
import signal
from datetime import datetime, timezone

REQUEST_DELAY = 0.1
_shutdown = False

def signal_handler(sig, frame):
    global _shutdown
    if _shutdown:
        sys.exit(1)
    print("\nShutdown requested — saving after current page...")
    _shutdown = True

signal.signal(signal.SIGINT, signal_handler)


def get_token():
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        try:
            result = subprocess.run(["gh", "auth", "token"], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                token = result.stdout.strip()
        except Exception:
            pass
    if not token:
        print("ERROR: No GitHub token found.")
        sys.exit(1)
    return token


def build_pull_requests_query(numbers):
    """Build a GraphQL query that fetches specific PR numbers in one request."""
    fields = []
    for i, number in enumerate(numbers):
        fields.append(
            f"""
      pr{i}: pullRequest(number: {number}) {{
        number
        mergedBy {{ login }}
        author {{ login }}
      }}
"""
        )
    joined = "".join(fields)
    return f"""
query($owner: String!, $name: String!) {{
  repository(owner: $owner, name: $name) {{
{joined}
  }}
  rateLimit {{ remaining resetAt }}
}}
"""


def graphql_request(session, token, query, variables):
    """Execute a GraphQL query."""
    import requests
    resp = session.post(
        "https://api.github.com/graphql",
        json={"query": query, "variables": variables},
        headers={
            "Authorization": f"bearer {token}",
            "Content-Type": "application/json",
        },
        timeout=30,
    )
    return resp


def ensure_schema(conn):
    """Ensure merged_by_checked exists for older DBs and normalize legacy sentinel rows."""
    try:
        conn.execute("ALTER TABLE items ADD COLUMN merged_by_checked INTEGER NOT NULL DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    conn.execute("UPDATE items SET merged_by = NULL, merged_by_checked = 1 WHERE merged_by = ''")
    conn.execute(
        "UPDATE items SET merged_by_checked = CASE "
        "WHEN merged_by IS NOT NULL THEN 1 "
        "ELSE COALESCE(merged_by_checked, 0) END"
    )
    conn.commit()


def fetch_merged_by(conn, session, token, repo):
    """Fetch merged_by only for PRs that are still missing it."""
    owner, name = repo.split("/")

    missing_numbers = [
        row[0] for row in conn.execute(
            "SELECT number FROM items WHERE repo=? AND is_pull_request=1 "
            "AND merged_at IS NOT NULL AND merged_at != '' AND merged_by_checked = 0 "
            "ORDER BY number ASC",
            (repo,),
        ).fetchall()
    ]
    need = len(missing_numbers)
    if need == 0:
        print(f"  {repo}: no merged PRs still needing merged_by")
        return

    total_merged = conn.execute(
        "SELECT COUNT(*) FROM items WHERE repo=? AND is_pull_request=1 "
        "AND merged_at IS NOT NULL AND merged_at != ''",
        (repo,)
    ).fetchone()[0]

    print(f"  {repo}: {need:,} of {total_merged:,} merged PRs need merged_by")

    updated = 0
    failed_chunks = 0
    chunk_size = 20
    chunks_done = 0
    total_chunks = (need + chunk_size - 1) // chunk_size

    for idx in range(0, need, chunk_size):
        if _shutdown:
            break

        chunk = missing_numbers[idx:idx + chunk_size]
        query = build_pull_requests_query(chunk)
        variables = {"owner": owner, "name": name}
        while not _shutdown:
            for attempt in range(5):
                try:
                    resp = graphql_request(session, token, query, variables)
                    break
                except Exception as e:
                    wait = min(4 ** attempt, 120)
                    print(f"    Error: {e}, retry in {wait}s...")
                    time.sleep(wait)
            else:
                print(f"    FAILED after 5 retries")
                failed_chunks += 1
                break

            if resp.status_code != 200:
                body = resp.text[:300]
                if resp.status_code == 403 or "rate limit" in body.lower():
                    payload = {}
                    try:
                        payload = resp.json()
                    except Exception:
                        pass
                    rl = (payload.get("data") or {}).get("rateLimit", {})
                    reset_at = rl.get("resetAt", "unknown")
                    print(f"    Rate limited, resets at {reset_at}")
                    # Parse reset time and sleep
                    try:
                        reset = datetime.fromisoformat(reset_at.replace("Z", "+00:00"))
                        wait = max((reset - datetime.now(timezone.utc)).total_seconds(), 0) + 10
                    except Exception:
                        wait = 600
                    print(f"    Sleeping {wait:.0f}s...")
                    time.sleep(wait)
                    continue
                print(f"    HTTP {resp.status_code}: {body}")
                failed_chunks += 1
                break

            data = resp.json()
            if "errors" in data:
                err_text = str(data["errors"]).lower()
                if "rate limit" in err_text or "throttle" in err_text:
                    rl = (data.get("data") or {}).get("rateLimit", {})
                    reset_at = rl.get("resetAt", "unknown")
                    print(f"    GraphQL rate-limited, resets at {reset_at}")
                    try:
                        reset = datetime.fromisoformat(reset_at.replace("Z", "+00:00"))
                        wait = max((reset - datetime.now(timezone.utc)).total_seconds(), 0) + 10
                    except Exception:
                        wait = 600
                    print(f"    Sleeping {wait:.0f}s...")
                    time.sleep(wait)
                    continue
                print(f"    GraphQL errors: {data['errors']}")
                failed_chunks += 1
                break

            payload = data.get("data") or {}
            repo_data = payload.get("repository")
            if not repo_data:
                print("    GraphQL returned no repository data")
                failed_chunks += 1
                break
            rl = payload.get("rateLimit", {})

            checked_batch = []
            merged_batch = []
            for i in range(len(chunk)):
                number = chunk[i]
                node = repo_data.get(f"pr{i}")
                checked_batch.append((1, repo, number))
                if not node:
                    continue
                merged_by = None
                if node.get("mergedBy"):
                    merged_by = node["mergedBy"].get("login")
                author = None
                if node.get("author"):
                    author = node["author"].get("login")
                if merged_by:
                    merged_batch.append((merged_by, author, repo, number))

            if checked_batch:
                conn.executemany(
                    "UPDATE items SET merged_by_checked=? "
                    "WHERE repo=? AND number=?",
                    checked_batch
                )
            if merged_batch:
                conn.executemany(
                    "UPDATE items SET merged_by=COALESCE(?, merged_by), "
                    "author=COALESCE(author, ?) "
                    "WHERE repo=? AND number=?",
                    merged_batch
                )
                updated += len(merged_batch)

            chunks_done += 1
            if chunks_done % 25 == 0 or chunks_done == total_chunks:
                conn.commit()
                ts = datetime.now().strftime("%H:%M:%S")
                checked = min(idx + len(chunk), need)
                rl_remaining = rl.get("remaining", "?")
                print(
                    f"    [{ts}] chunk {chunks_done}/{total_chunks}: "
                    f"{checked:,}/{need:,} checked, {updated:,} with merged_by "
                    f"(RL: {rl_remaining})"
                )

            # Proactive rate limit check
            rl_remaining = rl.get("remaining", 0)
            if int(rl_remaining) < 50:
                reset_at = rl.get("resetAt", "unknown")
                try:
                    reset = datetime.fromisoformat(reset_at.replace("Z", "+00:00"))
                    wait = max((reset - datetime.now(timezone.utc)).total_seconds(), 0) + 10
                except Exception:
                    wait = 600
                print(f"    Rate limit low ({rl_remaining}), sleeping {wait:.0f}s...")
                time.sleep(wait)

            time.sleep(REQUEST_DELAY)
            break

    conn.commit()
    print(
        f"  {repo}: set merged_by on {updated:,} PRs"
        + (f", failed chunks: {failed_chunks}" if failed_chunks else "")
    )


def main():
    import requests
    import argparse

    parser = argparse.ArgumentParser(description="Fetch merged_by data via GraphQL")
    parser.add_argument("--db", default="pr-dashboard.db")
    parser.add_argument("--repos", nargs="*")
    args = parser.parse_args()

    token = get_token()
    conn = sqlite3.connect(args.db)
    ensure_schema(conn)
    session = requests.Session()

    if args.repos:
        repos = args.repos
    else:
        repos = [r[0] for r in conn.execute(
            "SELECT DISTINCT repo FROM items ORDER BY repo"
        )]

    print(f"Fetching merged_by via GraphQL for: {', '.join(repos)}\n")

    for repo in repos:
        if _shutdown:
            break
        fetch_merged_by(conn, session, token, repo)
        print()

    conn.close()
    print("Done!")


if __name__ == "__main__":
    main()
