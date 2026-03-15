#!/usr/bin/env python3
"""Backfill issue authors from GitHub REST API.

Re-fetches issues from /issues endpoint to populate the author field
for issues that were fetched before the author column was added.

Uses conditional requests (If-Modified-Since) where possible and
respects rate limits. Typically completes in ~1 hour for ~490K issues.
"""

import os
import sys
import time
import sqlite3
import requests

DB_PATH = "pr-dashboard.db"
REPOS = [
    "dotnet/aspire",
    "dotnet/maui",
    "dotnet/roslyn",
    "dotnet/runtime",
    "golang/go",
    "microsoft/vscode",
    "rust-lang/rust",
]

def get_session():
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        # Try gh CLI
        import subprocess
        result = subprocess.run(["gh", "auth", "token"], capture_output=True, text=True)
        if result.returncode == 0:
            token = result.stdout.strip()
    if not token:
        print("ERROR: No GitHub token found. Set GITHUB_TOKEN or use gh auth login.")
        sys.exit(1)
    s = requests.Session()
    s.headers.update({
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    })
    return s


def check_rate_limit(session):
    resp = session.get("https://api.github.com/rate_limit")
    if resp.status_code == 200:
        data = resp.json()
        core = data["resources"]["core"]
        remaining = core["remaining"]
        reset_at = core["reset"]
        return remaining, reset_at
    return None, None


def wait_for_rate_limit(session):
    remaining, reset_at = check_rate_limit(session)
    if remaining is not None and remaining < 100:
        wait = max(0, reset_at - time.time()) + 5
        print(f"  Rate limit low ({remaining} remaining). Waiting {wait:.0f}s...")
        time.sleep(wait)


def backfill_repo(conn, session, repo):
    owner, name = repo.split("/")
    
    # Count missing
    missing = conn.execute(
        "SELECT COUNT(*) FROM items WHERE repo = ? AND is_pull_request = 0 AND author IS NULL",
        (repo,)
    ).fetchone()[0]
    
    if missing == 0:
        print(f"  {repo}: all issues have authors, skipping")
        return
    
    print(f"  {repo}: {missing:,} issues missing author")
    
    # Fetch all issues page by page
    url = f"https://api.github.com/repos/{owner}/{name}/issues"
    page = 1
    updated = 0
    skipped_prs = 0
    
    while True:
        wait_for_rate_limit(session)
        
        params = {
            "state": "all",
            "per_page": 100,
            "page": page,
            "sort": "created",
            "direction": "asc",
        }
        
        resp = session.get(url, params=params)
        if resp.status_code == 403:
            print(f"  Rate limited at page {page}, waiting...")
            reset = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
            time.sleep(max(0, reset - time.time()) + 5)
            continue
        
        if resp.status_code != 200:
            print(f"  Error {resp.status_code} at page {page}: {resp.text[:200]}")
            break
        
        data = resp.json()
        if not data:
            break
        
        batch = []
        for item in data:
            # /issues endpoint includes PRs — skip them
            if "pull_request" in item:
                skipped_prs += 1
                continue
            
            user = item.get("user")
            author = user.get("login") if user and isinstance(user, dict) else None
            if author:
                batch.append((author, repo, item["number"]))
        
        if batch:
            conn.executemany(
                "UPDATE items SET author = ? WHERE repo = ? AND number = ? AND is_pull_request = 0",
                batch
            )
            updated += len(batch)
        
        # Checkpoint every 50 pages
        if page % 50 == 0:
            conn.commit()
            remaining = missing - updated
            print(f"    page {page}: {updated:,} updated, ~{remaining:,} remaining")
        
        # Check if we've reached the end
        link = resp.headers.get("Link", "")
        if 'rel="next"' not in link:
            break
        
        page += 1
        time.sleep(0.1)  # Small delay to be polite
    
    conn.commit()
    
    # Verify
    still_missing = conn.execute(
        "SELECT COUNT(*) FROM items WHERE repo = ? AND is_pull_request = 0 AND author IS NULL",
        (repo,)
    ).fetchone()[0]
    
    print(f"  {repo}: updated {updated:,} authors ({still_missing:,} still missing — likely deleted users)")


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    session = get_session()
    
    # Check rate limit before starting
    remaining, _ = check_rate_limit(session)
    print(f"Rate limit remaining: {remaining}")
    print(f"Need ~{sum(1 for r in REPOS for _ in [1]) * 50}+ API calls per 1K issues\n")
    
    for repo in REPOS:
        backfill_repo(conn, session, repo)
        print()
    
    # Summary
    total = conn.execute("SELECT COUNT(*) FROM items WHERE is_pull_request = 0").fetchone()[0]
    with_author = conn.execute(
        "SELECT COUNT(*) FROM items WHERE is_pull_request = 0 AND author IS NOT NULL"
    ).fetchone()[0]
    print(f"Done! {with_author:,}/{total:,} issues now have authors ({100*with_author/total:.1f}%)")
    
    conn.close()


if __name__ == "__main__":
    main()
