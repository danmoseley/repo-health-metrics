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

REQUEST_DELAY = 0.5
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


QUERY = """
query($owner: String!, $name: String!, $cursor: String) {
  repository(owner: $owner, name: $name) {
    pullRequests(first: 100, after: $cursor, states: MERGED, orderBy: {field: CREATED_AT, direction: ASC}) {
      totalCount
      pageInfo { hasNextPage endCursor }
      nodes {
        number
        mergedBy { login }
        author { login }
      }
    }
  }
  rateLimit { remaining resetAt }
}
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


def fetch_merged_by(conn, session, token, repo):
    """Fetch merged_by for all merged PRs in a repo via GraphQL."""
    owner, name = repo.split("/")

    # Check how many we still need
    need = conn.execute(
        "SELECT COUNT(*) FROM items WHERE repo=? AND is_pull_request=1 "
        "AND merged_at IS NOT NULL AND merged_at != '' AND (merged_by IS NULL OR merged_by = '')",
        (repo,)
    ).fetchone()[0]

    if need == 0:
        print(f"  {repo}: all merged PRs already have merged_by")
        return

    total_merged = conn.execute(
        "SELECT COUNT(*) FROM items WHERE repo=? AND is_pull_request=1 "
        "AND merged_at IS NOT NULL AND merged_at != ''",
        (repo,)
    ).fetchone()[0]

    print(f"  {repo}: {need:,} of {total_merged:,} merged PRs need merged_by")

    cursor = None
    updated = 0
    page = 0

    while not _shutdown:
        variables = {"owner": owner, "name": name, "cursor": cursor}

        for attempt in range(5):
            try:
                resp = graphql_request(session, token, QUERY, variables)
                break
            except Exception as e:
                wait = min(4 ** attempt, 120)
                print(f"    Error: {e}, retry in {wait}s...")
                time.sleep(wait)
        else:
            print(f"    FAILED after 5 retries")
            break

        if resp.status_code != 200:
            body = resp.text[:300]
            if resp.status_code == 403 or "rate limit" in body.lower():
                rl = resp.json().get("data", {}).get("rateLimit", {})
                reset_at = rl.get("resetAt", "unknown")
                print(f"    Rate limited, resets at {reset_at}")
                # Parse reset time and sleep
                try:
                    reset = datetime.fromisoformat(reset_at.replace("Z", "+00:00"))
                    wait = max((reset - datetime.now(timezone.utc)).total_seconds(), 0) + 10
                except:
                    wait = 600
                print(f"    Sleeping {wait:.0f}s...")
                time.sleep(wait)
                continue
            print(f"    HTTP {resp.status_code}: {body}")
            break

        data = resp.json()
        if "errors" in data:
            print(f"    GraphQL errors: {data['errors']}")
            break

        prs_data = data["data"]["repository"]["pullRequests"]
        page_info = prs_data["pageInfo"]
        nodes = prs_data["nodes"]
        rl = data["data"]["rateLimit"]

        batch = []
        for node in nodes:
            merged_by = None
            if node.get("mergedBy"):
                merged_by = node["mergedBy"].get("login")
            author = None
            if node.get("author"):
                author = node["author"].get("login")
            if merged_by:
                batch.append((merged_by, author, repo, node["number"]))

        if batch:
            conn.executemany(
                "UPDATE items SET merged_by=?, author=COALESCE(author, ?) "
                "WHERE repo=? AND number=?",
                batch
            )
            updated += len(batch)

        page += 1
        if page % 10 == 0:
            conn.commit()
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"    [{ts}] page {page}: {updated:,} updated "
                  f"(RL: {rl['remaining']})")

        if not page_info["hasNextPage"]:
            break

        cursor = page_info["endCursor"]

        # Proactive rate limit check
        if int(rl["remaining"]) < 50:
            reset_at = rl["resetAt"]
            try:
                reset = datetime.fromisoformat(reset_at.replace("Z", "+00:00"))
                wait = max((reset - datetime.now(timezone.utc)).total_seconds(), 0) + 10
            except:
                wait = 600
            print(f"    Rate limit low ({rl['remaining']}), sleeping {wait:.0f}s...")
            time.sleep(wait)

        time.sleep(REQUEST_DELAY)

    conn.commit()
    print(f"  {repo}: updated {updated:,} PRs with merged_by")


def main():
    import requests
    import argparse

    parser = argparse.ArgumentParser(description="Fetch merged_by data via GraphQL")
    parser.add_argument("--db", default="pr-dashboard.db")
    parser.add_argument("--repos", nargs="*")
    args = parser.parse_args()

    token = get_token()
    conn = sqlite3.connect(args.db)
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
