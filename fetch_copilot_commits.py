#!/usr/bin/env python3
"""Fetch first commit message for recent PRs and detect Co-authored-by: Copilot trailer.

For each PR created in the last 12 months across all graphed repos, fetches the
first commit message via GraphQL and checks for a Co-authored-by trailer mentioning
Copilot. Stores result in copilot_trailer column (1 = found, 0 = not found).

Uses batched GraphQL queries (~50 PRs per request) with checkpoint/resume support.
"""

import sqlite3
import subprocess
import json
import time
import re
import sys
from datetime import datetime, timedelta, timezone

DB_PATH = "pr-dashboard.db"
BATCH_SIZE = 50

# Match known Copilot co-author trailers (GitHub noreply addresses)
TRAILER_RE = re.compile(
    r"co-authored-by:\s*copilot\s*<[^>]*@users\.noreply\.github\.com>",
    re.IGNORECASE,
)


def run_graphql(query):
    """Execute a GraphQL query via gh CLI. Returns parsed JSON or None."""
    result = subprocess.run(
        ["gh", "api", "graphql", "-f", f"query={query}"],
        capture_output=True, encoding="utf-8", errors="replace"
    )
    if result.returncode != 0:
        stderr_text = result.stderr.strip()
        stdout_text = result.stdout.strip()
        print(f"  GraphQL error: {stderr_text}", file=sys.stderr)
        # Try to parse response body (may contain errors with rate limit info)
        for payload in (stdout_text, stderr_text):
            if not payload:
                continue
            try:
                parsed = json.loads(payload)
                if isinstance(parsed, dict):
                    return parsed  # caller checks for "data" key
            except json.JSONDecodeError:
                continue
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as e:
        print(f"  JSON parse error: {e}", file=sys.stderr)
        return None


def has_copilot_trailer(message):
    """Check if a commit message contains a Co-authored-by: Copilot trailer."""
    if not message:
        return False
    return bool(TRAILER_RE.search(message))


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--repos", nargs="*", help="Repos to process (default: all in DB)")
    parser.add_argument("--db", default=DB_PATH)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)

    # Ensure copilot_trailer column exists
    try:
        conn.execute("ALTER TABLE items ADD COLUMN copilot_trailer INTEGER")
        conn.commit()
        print("Added copilot_trailer column")
    except sqlite3.OperationalError:
        pass  # column already exists

    # Create checkpoint table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS copilot_trailer_progress (
            repo TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'pending'
        )
    """)
    conn.commit()

    if args.repos:
        repos = args.repos
    else:
        repos = [r[0] for r in conn.execute(
            "SELECT DISTINCT repo FROM items ORDER BY repo"
        ).fetchall()]

    # Cutoff: PRs created in last 12 months
    cutoff = (datetime.now(timezone.utc) - timedelta(days=365)).strftime("%Y-%m-%dT%H:%M:%SZ")

    for repo in repos:
        owner, name = repo.split("/")

        # Check/init checkpoint
        row = conn.execute(
            "SELECT status FROM copilot_trailer_progress WHERE repo=?",
            (repo,)
        ).fetchone()
        if row and row[0] == "complete":
            print(f"\n{repo}: already complete, skipping")
            continue

        if not row:
            conn.execute(
                "INSERT INTO copilot_trailer_progress (repo, status) VALUES (?, 'in_progress')",
                (repo,)
            )
        else:
            conn.execute(
                "UPDATE copilot_trailer_progress SET status='in_progress' WHERE repo=?",
                (repo,)
            )
        conn.commit()

        # Get all recent PRs that haven't been checked yet — resume-safe since
        # we only query NULL rows and update them as we go
        prs = conn.execute(
            "SELECT number FROM items "
            "WHERE repo=? AND is_pull_request=1 AND created_at>=? AND copilot_trailer IS NULL "
            "ORDER BY number",
            (repo, cutoff)
        ).fetchall()
        pr_numbers = [r[0] for r in prs]

        total = len(pr_numbers)
        print(f"\n{repo}: {total} PRs to check")

        if total == 0:
            conn.execute(
                "UPDATE copilot_trailer_progress SET status='complete' WHERE repo=?",
                (repo,)
            )
            conn.commit()
            continue

        found = 0
        processed = 0
        consecutive_failures = 0

        for batch_start in range(0, total, BATCH_SIZE):
            batch = pr_numbers[batch_start:batch_start + BATCH_SIZE]
            parts = []
            for i, num in enumerate(batch):
                parts.append(
                    f'pr{i}: pullRequest(number:{num}) {{ '
                    f'number '
                    f'commits(first:1) {{ nodes {{ commit {{ message }} }} }} '
                    f'}}'
                )
            query = f'{{ repository(owner:"{owner}",name:"{name}") {{ {" ".join(parts)} }} }}'

            data = run_graphql(query)
            if not data or "data" not in data:
                # Check for rate limit
                errors = (data or {}).get("errors", [])
                is_rate_limit = any("rate" in str(e).lower() for e in errors)
                if is_rate_limit:
                    print("  Rate limited — waiting 60s...")
                    time.sleep(60)
                    data = run_graphql(query)

                if not data or "data" not in data:
                    print(f"  Batch failed at offset {batch_start}, retrying in 10s...")
                    time.sleep(10)
                    data = run_graphql(query)
                    if not data or "data" not in data:
                        print(f"  Batch failed again, skipping {len(batch)} PRs")
                        consecutive_failures += 1
                        if consecutive_failures >= 3:
                            print(f"  3 consecutive failures — skipping {repo}")
                            break
                        continue

            consecutive_failures = 0

            repo_data = data["data"].get("repository")
            if not repo_data:
                print(f"  Repository not found in response, skipping batch")
                continue

            batch_found = 0
            for i, num in enumerate(batch):
                pr_data = repo_data.get(f"pr{i}")
                if not pr_data:
                    # PR may have been deleted — mark as checked (0)
                    conn.execute(
                        "UPDATE items SET copilot_trailer=0 WHERE repo=? AND number=?",
                        (repo, num)
                    )
                    continue

                commits = pr_data.get("commits", {}).get("nodes", [])
                if commits:
                    message = commits[0].get("commit", {}).get("message", "")
                    trailer = 1 if has_copilot_trailer(message) else 0
                else:
                    trailer = 0  # No commits — mark as checked

                conn.execute(
                    "UPDATE items SET copilot_trailer=? WHERE repo=? AND number=?",
                    (trailer, repo, num)
                )
                if trailer:
                    batch_found += 1

            conn.commit()
            found += batch_found
            processed += len(batch)

            done = min(batch_start + BATCH_SIZE, total)
            print(f"  {done}/{total} — {batch_found} trailers in batch ({found} total)")

            # Brief pause between batches
            time.sleep(0.3)

        # Only mark complete if all PRs have been checked
        remaining = conn.execute(
            "SELECT count(*) FROM items "
            "WHERE repo=? AND is_pull_request=1 AND created_at>=? AND copilot_trailer IS NULL",
            (repo, cutoff)
        ).fetchone()[0]
        if remaining == 0:
            conn.execute(
                "UPDATE copilot_trailer_progress SET status='complete' WHERE repo=?",
                (repo,)
            )
        else:
            print(f"  {remaining} PRs still unchecked (failures) — not marking complete")
        conn.commit()
        print(f"  Done: {found} PRs with Copilot trailer out of {processed} checked")

    # Summary
    total_trailer = conn.execute(
        "SELECT count(*) FROM items WHERE copilot_trailer=1"
    ).fetchone()[0]
    total_checked = conn.execute(
        "SELECT count(*) FROM items WHERE copilot_trailer IS NOT NULL"
    ).fetchone()[0]
    print(f"\nOverall: {total_trailer} PRs with Copilot trailer out of {total_checked} checked")

    conn.close()


if __name__ == "__main__":
    main()
