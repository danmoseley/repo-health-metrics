#!/usr/bin/env python3
"""
Fetch PR review timeline data via GraphQL for Copilot code review analysis.

For each merged PR in the target repos (last 12 months by default), fetches:
  - Reviews: who reviewed (Copilot bot vs human), when, state
  - Per-commit stats: additions, deletions, committedDate, message
  - Review threads: isResolved, resolvedBy, comment bodies (for suggestion detection)
  - PR title: stored in items table for revert detection

Stores data locally so future charts can be built without re-querying.
NOTE: New tables are not yet in the CSV backup/restore flow (backup_csvs.py / load_csv.py).
Data is only in the local DB until those scripts are updated.

Uses GraphQL timelineItems + reviewThreads queries. Resumable via checkpoint table.

Usage:
    python fetch_reviews.py                              # default: dotnet/runtime
    python fetch_reviews.py --repos dotnet/runtime dotnet/roslyn
    python fetch_reviews.py --since 2024-06-01           # custom start date
    python fetch_reviews.py --db mydata.db               # custom DB path
"""

import sqlite3
import json
import time
import os
import sys
import signal
import argparse
from datetime import datetime, timezone, timedelta

import requests as req

DEFAULT_DB = "pr-dashboard.db"
REQUEST_DELAY = 0.8  # seconds between GraphQL requests (conservative)

DEFAULT_REPOS = [
    "dotnet/runtime",
]

# How many months of merged PRs to fetch review data for
DEFAULT_LOOKBACK_DAYS = 365

_shutdown = False


def signal_handler(sig, frame):
    global _shutdown
    if _shutdown:
        sys.exit(1)
    print("\nShutdown requested — saving after current PR...")
    _shutdown = True


signal.signal(signal.SIGINT, signal_handler)


def get_token():
    """Get GitHub token from environment or gh CLI."""
    import subprocess
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        try:
            result = subprocess.run(
                ["gh", "auth", "token"], capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0:
                token = result.stdout.strip()
        except Exception:
            pass
    if not token:
        print("ERROR: No GitHub token found. Set GITHUB_TOKEN or run 'gh auth login'.")
        sys.exit(1)
    return token


# --- GraphQL query ---
# Fetches timeline items (commits + reviews + force pushes) and review threads
# for a single PR in one request.

TIMELINE_QUERY = """
query($owner: String!, $name: String!, $number: Int!,
      $timelineCursor: String, $threadCursor: String) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      title
      timelineItems(first: 100, after: $timelineCursor, itemTypes: [
        PULL_REQUEST_COMMIT,
        PULL_REQUEST_REVIEW,
        HEAD_REF_FORCE_PUSHED_EVENT
      ]) {
        pageInfo { hasNextPage endCursor }
        nodes {
          __typename
          ... on PullRequestCommit {
            commit {
              oid
              committedDate
              additions
              deletions
              message
            }
          }
          ... on PullRequestReview {
            databaseId
            submittedAt
            state
            commit { oid }
            author { login __typename }
            comments(first: 100) {
              nodes {
                databaseId
                body
                path
                createdAt
                author { login __typename }
              }
            }
          }
          ... on HeadRefForcePushedEvent {
            createdAt
            actor { login }
            beforeCommit { oid }
            afterCommit { oid }
          }
        }
      }
      reviewThreads(first: 50, after: $threadCursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          isResolved
          isOutdated
          resolvedBy { login }
          comments(first: 10) {
            nodes {
              body
              author { login __typename }
              createdAt
            }
          }
        }
      }
    }
  }
  rateLimit { remaining resetAt cost }
}
"""


def ensure_schema(conn):
    """Create tables if they don't exist. Idempotent."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS pr_reviews (
            repo TEXT NOT NULL,
            number INTEGER NOT NULL,
            review_id INTEGER NOT NULL,
            author TEXT,
            author_type TEXT,
            state TEXT,
            submitted_at TEXT,
            commit_sha TEXT,
            PRIMARY KEY (repo, number, review_id)
        );

        CREATE TABLE IF NOT EXISTS pr_review_comments (
            repo TEXT NOT NULL,
            number INTEGER NOT NULL,
            comment_id INTEGER NOT NULL,
            review_id INTEGER,
            author TEXT,
            author_type TEXT,
            body_has_suggestion INTEGER DEFAULT 0,
            path TEXT,
            created_at TEXT,
            is_resolved INTEGER,
            is_outdated INTEGER,
            PRIMARY KEY (repo, number, comment_id)
        );

        CREATE TABLE IF NOT EXISTS pr_commit_stats (
            repo TEXT NOT NULL,
            number INTEGER NOT NULL,
            sha TEXT NOT NULL,
            committed_date TEXT,
            additions INTEGER,
            deletions INTEGER,
            message TEXT,
            PRIMARY KEY (repo, number, sha)
        );

        CREATE TABLE IF NOT EXISTS review_fetch_progress (
            repo TEXT NOT NULL,
            number INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            fetched_at TEXT,
            PRIMARY KEY (repo, number)
        );
        CREATE INDEX IF NOT EXISTS idx_pr_reviews_repo_number
            ON pr_reviews(repo, number);
        CREATE INDEX IF NOT EXISTS idx_pr_review_comments_repo_number
            ON pr_review_comments(repo, number);
        CREATE INDEX IF NOT EXISTS idx_pr_commit_stats_repo_number
            ON pr_commit_stats(repo, number);

        CREATE TABLE IF NOT EXISTS pr_copilot_issue_comments (
            repo TEXT NOT NULL,
            number INTEGER NOT NULL,
            comment_id INTEGER NOT NULL,
            author TEXT,
            created_at TEXT,
            body_length INTEGER,
            PRIMARY KEY (repo, number, comment_id)
        );
    """)

    # Ensure title column exists on items table
    try:
        conn.execute("ALTER TABLE items ADD COLUMN title TEXT")
        conn.commit()
        print("Added title column to items table")
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower():
            pass  # column already exists
        elif "no such table" in str(e).lower():
            print("Warning: items table not found — run fetch.py or load_csv.py first")
        else:
            raise

    conn.commit()


def has_suggestion(body):
    """Check if a review comment body contains a GitHub suggestion block."""
    if not body:
        return False
    return "```suggestion" in body


def graphql_request(session, token, query, variables):
    """Execute a GraphQL query. Returns raw requests.Response (caller must handle .json(), .status_code)."""
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


def handle_rate_limit(rl_data, min_remaining=50):
    """Sleep if rate limit is low. Returns True if we had to sleep."""
    if not rl_data:
        return False
    remaining = rl_data.get("remaining", 999)
    if remaining < min_remaining:
        reset_at = rl_data.get("resetAt", "")
        try:
            reset = datetime.fromisoformat(reset_at.replace("Z", "+00:00"))
            wait = max((reset - datetime.now(timezone.utc)).total_seconds(), 0) + 10
        except (ValueError, AttributeError):
            wait = 600
        print(f"  Rate limit low ({remaining}), sleeping {wait:.0f}s until {reset_at}...")
        time.sleep(wait)
        return True
    return False


def fetch_pr_review_data(session, token, owner, name, number):
    """Fetch all review timeline data for a single PR.

    Returns (title, reviews, review_comments, commits, threads) or None on failure.
    Handles pagination for both timelineItems and reviewThreads.
    """
    all_reviews = []
    all_comments = []
    all_commits = []
    all_threads = []
    title = None

    timeline_cursor = None
    thread_cursor = None
    timeline_done = False
    threads_done = False
    rate_limit_retries = 0

    while not (timeline_done and threads_done):
        if rate_limit_retries >= 3:
            print(f"    Rate-limited 3 times for PR #{number} — giving up")
            return None
        variables = {
            "owner": owner,
            "name": name,
            "number": number,
            "timelineCursor": timeline_cursor if not timeline_done else None,
            "threadCursor": thread_cursor if not threads_done else None,
        }

        for attempt in range(5):
            try:
                resp = graphql_request(session, token, TIMELINE_QUERY, variables)
                # Retry on transient server errors (502, 503, etc.)
                if resp.status_code in (502, 503, 504):
                    wait = min(2 ** attempt * 2, 60)
                    print(f"    HTTP {resp.status_code} for PR #{number}, retry {attempt+1}/5 in {wait}s...")
                    time.sleep(wait)
                    continue
                break
            except Exception as e:
                wait = min(4 ** attempt, 120)
                print(f"    Error fetching PR #{number}: {e}, retry in {wait}s...")
                time.sleep(wait)
        else:
            print(f"    FAILED after 5 retries for PR #{number}")
            return None

        if resp.status_code != 200:
            body_text = resp.text[:300]
            if resp.status_code == 403 or resp.status_code == 429 or "rate limit" in body_text.lower():
                # Parse reset time from response or headers
                wait = 600
                try:
                    rl = resp.json().get("data", {}).get("rateLimit", {})
                    reset_at = rl.get("resetAt") or resp.headers.get("x-ratelimit-reset", "")
                    if reset_at:
                        if reset_at.isdigit():
                            reset = datetime.fromtimestamp(int(reset_at), tz=timezone.utc)
                        else:
                            reset = datetime.fromisoformat(reset_at.replace("Z", "+00:00"))
                        wait = max((reset - datetime.now(timezone.utc)).total_seconds(), 0) + 10
                except Exception:
                    pass
                print(f"    Rate limited on PR #{number}, sleeping {wait:.0f}s...")
                time.sleep(wait)
                rate_limit_retries += 1
                continue  # retry this PR's current page
            print(f"    HTTP {resp.status_code} for PR #{number}: {body_text}")
            return None

        data = resp.json()
        if "errors" in data:
            errors = data["errors"]
            # Some PRs may not be accessible (deleted repos, etc.)
            error_types = [e.get("type", "") for e in errors]
            if "NOT_FOUND" in error_types:
                return ("", [], [], [], [])  # PR deleted/inaccessible — return empty
            # Check for rate limit errors in GraphQL response
            is_rate_limit = any("rate" in str(e).lower() for e in errors)
            if is_rate_limit:
                rl = data.get("data", {}).get("rateLimit", {})
                if not handle_rate_limit(rl, min_remaining=0):
                    print(f"    Rate limit error for PR #{number}, sleeping 600s...")
                    time.sleep(600)
                rate_limit_retries += 1
                continue  # retry
            print(f"    GraphQL errors for PR #{number}: {errors}")
            return None

        pr_data = data.get("data", {}).get("repository", {}).get("pullRequest")
        if not pr_data:
            # PR may have been deleted
            return ("", [], [], [], [])

        if title is None:
            title = pr_data.get("title", "")

        rl = data.get("data", {}).get("rateLimit", {})
        handle_rate_limit(rl)

        # --- Process timeline items ---
        if not timeline_done:
            tl = pr_data.get("timelineItems", {})
            for node in tl.get("nodes", []):
                typename = node.get("__typename", "")
                if typename == "PullRequestCommit":
                    commit = node.get("commit", {})
                    all_commits.append({
                        "sha": commit.get("oid", ""),
                        "committed_date": commit.get("committedDate", ""),
                        "additions": commit.get("additions", 0),
                        "deletions": commit.get("deletions", 0),
                        "message": commit.get("message", ""),
                    })
                elif typename == "PullRequestReview":
                    author = node.get("author") or {}
                    review = {
                        "review_id": node.get("databaseId", 0),
                        "author": author.get("login", ""),
                        "author_type": author.get("__typename", ""),
                        "state": node.get("state", ""),
                        "submitted_at": node.get("submittedAt", ""),
                        "commit_sha": (node.get("commit") or {}).get("oid", ""),
                    }
                    all_reviews.append(review)

                    # Extract inline review comments from this review
                    for cnode in (node.get("comments", {}).get("nodes", []) or []):
                        # Use per-comment author if available, fall back to review author
                        comment_author = cnode.get("author") or {}
                        c_login = comment_author.get("login", "") or review["author"]
                        c_type = comment_author.get("__typename", "") or review["author_type"]
                        all_comments.append({
                            "comment_id": cnode.get("databaseId", 0),
                            "review_id": review["review_id"],
                            "author": c_login,
                            "author_type": c_type,
                            "body_has_suggestion": 1 if has_suggestion(cnode.get("body")) else 0,
                            "path": cnode.get("path", ""),
                            "created_at": cnode.get("createdAt", ""),
                        })
                # HeadRefForcePushedEvent — we already have these in pr_push_events,
                # so we skip them here to avoid duplication.

            pi = tl.get("pageInfo", {})
            if pi.get("hasNextPage"):
                timeline_cursor = pi["endCursor"]
            else:
                timeline_done = True

        # --- Process review threads ---
        if not threads_done:
            rt = pr_data.get("reviewThreads", {})
            for tnode in rt.get("nodes", []):
                is_resolved = tnode.get("isResolved", False)
                is_outdated = tnode.get("isOutdated", False)
                resolved_by = (tnode.get("resolvedBy") or {}).get("login", "")
                thread_comments = tnode.get("comments", {}).get("nodes", []) or []

                # Identify the thread author (first comment) and check for suggestions
                first_comment = thread_comments[0] if thread_comments else {}
                thread_author = (first_comment.get("author") or {}).get("login", "")
                thread_author_type = (first_comment.get("author") or {}).get("__typename", "")

                all_threads.append({
                    "thread_id": tnode.get("id", ""),
                    "is_resolved": is_resolved,
                    "is_outdated": is_outdated,
                    "resolved_by": resolved_by,
                    "author": thread_author,
                    "author_type": thread_author_type,
                    "created_at": first_comment.get("createdAt", ""),
                    "has_suggestion": any(
                        has_suggestion(c.get("body")) for c in thread_comments
                    ),
                })

            pi = rt.get("pageInfo", {})
            if pi.get("hasNextPage"):
                thread_cursor = pi["endCursor"]
            else:
                threads_done = True

        # If both are done on first page, break immediately
        if timeline_done and threads_done:
            break

        time.sleep(REQUEST_DELAY)

    return (title, all_reviews, all_comments, all_commits, all_threads)


def store_pr_data(conn, repo, number, title, reviews, comments, commits, threads):
    """Store fetched review data into the database."""
    # Update PR title in items table
    if title:
        conn.execute(
            "UPDATE items SET title = ? WHERE repo = ? AND number = ?",
            (title, repo, number)
        )

    # Store reviews
    for r in reviews:
        conn.execute(
            "INSERT OR REPLACE INTO pr_reviews "
            "(repo, number, review_id, author, author_type, state, submitted_at, commit_sha) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (repo, number, r["review_id"], r["author"], r["author_type"],
             r["state"], r["submitted_at"], r["commit_sha"])
        )

    # Build a lookup from review comments to thread resolution status.
    # Thread comments are matched by author + created_at since we don't have
    # a direct comment_id ↔ thread_id link from the API.
    thread_resolution = {}
    for t in threads:
        for key in _thread_comment_keys(t):
            thread_resolution[key] = (
                1 if t["is_resolved"] else 0,
                1 if t["is_outdated"] else 0,
            )

    # Store review comments
    for c in comments:
        key = (c["author"], c["created_at"])
        is_resolved, is_outdated = thread_resolution.get(key, (None, None))
        conn.execute(
            "INSERT OR REPLACE INTO pr_review_comments "
            "(repo, number, comment_id, review_id, author, author_type, "
            "body_has_suggestion, path, created_at, is_resolved, is_outdated) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (repo, number, c["comment_id"], c["review_id"], c["author"],
             c["author_type"], c["body_has_suggestion"], c["path"],
             c["created_at"], is_resolved, is_outdated)
        )

    # Store commit stats
    for cm in commits:
        conn.execute(
            "INSERT OR REPLACE INTO pr_commit_stats "
            "(repo, number, sha, committed_date, additions, deletions, message) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (repo, number, cm["sha"], cm["committed_date"],
             cm["additions"], cm["deletions"], cm["message"])
        )


def _thread_comment_keys(thread):
    """Generate lookup keys for matching thread comments to review comments.
    Matches on (author, created_at) which is usually unique per comment."""
    author = thread.get("author", "")
    created_at = thread.get("created_at", "")
    if author and created_at:
        return [(author, created_at)]
    return []


def fetch_repo_reviews(conn, session, token, repo, since_date):
    """Fetch review data for all recent merged PRs in a repo."""
    owner, name = repo.split("/")

    # Preflight: ensure items table exists and has data for this repo
    try:
        item_count = conn.execute(
            "SELECT COUNT(*) FROM items WHERE repo = ? AND is_pull_request = 1",
            (repo,)
        ).fetchone()[0]
    except sqlite3.OperationalError:
        print(f"  {repo}: items table not found — run load_csv.py or fetch.py first")
        return
    if item_count == 0:
        print(f"  {repo}: no PR data in items table — run fetch.py first")
        return

    # Get merged PRs that need fetching (not yet in progress table or not complete)
    since_str = since_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    prs = conn.execute(
        "SELECT number FROM items "
        "WHERE repo = ? AND is_pull_request = 1 "
        "AND merged_at IS NOT NULL AND merged_at != '' "
        "AND merged_at >= ? "
        "AND number NOT IN ("
        "  SELECT number FROM review_fetch_progress "
        "  WHERE repo = ? AND status = 'complete'"
        ") "
        "ORDER BY number",
        (repo, since_str, repo)
    ).fetchall()

    pr_numbers = [r[0] for r in prs]
    total = len(pr_numbers)
    if total == 0:
        print(f"  {repo}: all recent merged PRs already fetched")
        return

    print(f"  {repo}: {total:,} merged PRs to fetch review data for")

    fetched = 0
    total_reviews = 0
    total_comments = 0
    total_commits = 0
    consecutive_failures = 0

    for number in pr_numbers:
        if _shutdown:
            print(f"  Shutdown — saved progress at PR #{number}")
            break

        result = fetch_pr_review_data(session, token, owner, name, number)
        if result is None:
            consecutive_failures += 1
            # Mark as failed so we can retry later with --retry-failed
            conn.execute(
                "INSERT OR REPLACE INTO review_fetch_progress "
                "(repo, number, status, fetched_at) VALUES (?, ?, 'failed', ?)",
                (repo, number, datetime.now(timezone.utc).isoformat())
            )
            conn.commit()
            if consecutive_failures >= 10:
                print(f"  10 consecutive failures — stopping {repo}")
                break
            continue

        consecutive_failures = 0
        title, reviews, comments, commits, threads = result

        store_pr_data(conn, repo, number, title, reviews, comments, commits, threads)

        # Mark as complete — commit data and progress atomically
        conn.execute(
            "INSERT OR REPLACE INTO review_fetch_progress "
            "(repo, number, status, fetched_at) VALUES (?, ?, 'complete', ?)",
            (repo, number, datetime.now(timezone.utc).isoformat())
        )
        conn.commit()

        fetched += 1
        total_reviews += len(reviews)
        total_comments += len(comments)
        total_commits += len(commits)

        if fetched % 50 == 0 or fetched == 1:
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"    [{ts}] {fetched}/{total} PRs "
                  f"({total_reviews} reviews, {total_comments} comments, "
                  f"{total_commits} commits)")

        time.sleep(REQUEST_DELAY)

    ts = datetime.now().strftime("%H:%M:%S")
    print(f"  [{ts}] {repo}: done — {fetched} PRs fetched "
          f"({total_reviews} reviews, {total_comments} comments, {total_commits} commits)")


COPILOT_COMMENTS_SEARCH = """
query($query: String!, $cursor: String) {
  search(query: $query, type: ISSUE, first: 30, after: $cursor) {
    issueCount
    pageInfo { hasNextPage endCursor }
    nodes {
      ... on PullRequest {
        number
        comments(first: 100) {
          nodes {
            databaseId
            author { login }
            createdAt
            bodyText
          }
        }
      }
    }
  }
  rateLimit { remaining resetAt }
}
"""


def fetch_copilot_issue_comments(conn, session, token, repo, since_date):
    """Fetch github-actions[bot] 'Copilot Code Review' issue comments.

    These are posted by a GitHub Actions workflow that runs Copilot agentic
    code review, distinct from the built-in copilot-pull-request-reviewer bot.
    We identify them by author=github-actions AND body containing 'Copilot Code Review'.
    """
    owner, name = repo.split("/")
    since_str = since_date.strftime("%Y-%m-%d")
    search_query = (f"repo:{repo} is:pr is:merged merged:>={since_str} "
                    f"commenter:app/github-actions")

    cursor = None
    total_found = 0
    total_stored = 0
    page = 0

    print(f"  Fetching Copilot agentic review comments for {repo}...")

    while True:
        if _shutdown:
            break
        variables = {"query": search_query, "cursor": cursor}
        try:
            resp = graphql_request(session, token, COPILOT_COMMENTS_SEARCH, variables)
        except Exception as e:
            print(f"    Error fetching comments page {page}: {e}")
            break

        if resp.status_code != 200:
            print(f"    HTTP {resp.status_code}: {resp.text[:200]}")
            if resp.status_code in (403, 429):
                wait = 600
                try:
                    reset_ts = resp.headers.get("x-ratelimit-reset", "")
                    if reset_ts and reset_ts.isdigit():
                        reset = datetime.fromtimestamp(int(reset_ts), tz=timezone.utc)
                        wait = max((reset - datetime.now(timezone.utc)).total_seconds(), 0) + 10
                except Exception:
                    pass
                print(f"    Rate limited, sleeping {wait:.0f}s...")
                time.sleep(wait)
                continue
            break

        result = resp.json()
        if "errors" in result:
            print(f"    GraphQL errors: {result['errors'][0].get('message', '')}")
            break

        rl = result.get("data", {}).get("rateLimit")
        if rl:
            handle_rate_limit(rl)

        search_data = result.get("data", {}).get("search", {})
        if page == 0:
            print(f"    {search_data.get('issueCount', '?')} PRs have github-actions comments")

        for pr_node in search_data.get("nodes", []):
            if not pr_node:
                continue
            pr_num = pr_node["number"]
            for comment in pr_node.get("comments", {}).get("nodes", []):
                if not comment or not comment.get("author"):
                    continue
                if comment["author"]["login"] != "github-actions":
                    continue
                body = comment.get("bodyText", "")
                if "Copilot Code Review" not in body:
                    continue
                # This is a Copilot agentic review comment
                total_found += 1
                comment_id = comment["databaseId"]
                try:
                    conn.execute(
                        "INSERT OR REPLACE INTO pr_copilot_issue_comments "
                        "(repo, number, comment_id, author, created_at, body_length) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (repo, pr_num, comment_id, "github-actions[bot]",
                         comment["createdAt"], len(body))
                    )
                    total_stored += 1
                except sqlite3.Error:
                    pass

        conn.commit()
        page += 1

        page_info = search_data.get("pageInfo", {})
        if page_info.get("hasNextPage") and page_info.get("endCursor"):
            cursor = page_info["endCursor"]
            time.sleep(REQUEST_DELAY)
        else:
            break

    print(f"    Found {total_found} Copilot agentic review comments, stored {total_stored}")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch PR review timeline data via GraphQL"
    )
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite database path")
    parser.add_argument("--repos", nargs="*", help="Repos to fetch (default: dotnet/runtime)")
    parser.add_argument(
        "--since",
        help="Fetch PRs merged since this date (YYYY-MM-DD). Default: 12 months ago.",
    )
    parser.add_argument(
        "--retry-failed", action="store_true",
        help="Retry PRs that previously failed",
    )
    args = parser.parse_args()

    token = get_token()
    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA journal_mode=WAL")
    ensure_schema(conn)

    repos = args.repos or DEFAULT_REPOS
    if args.since:
        since_date = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        since_date = datetime.now(timezone.utc) - timedelta(days=DEFAULT_LOOKBACK_DAYS)

    if args.retry_failed:
        for repo in repos:
            n = conn.execute(
                "DELETE FROM review_fetch_progress WHERE repo = ? AND status = 'failed'",
                (repo,)
            ).rowcount
            if n:
                print(f"  Reset {n} failed PRs for {repo}")
        conn.commit()

    session = req.Session()
    print(f"Fetching review data via GraphQL for: {', '.join(repos)}")
    print(f"  Since: {since_date.strftime('%Y-%m-%d')}")
    print()

    for repo in repos:
        if _shutdown:
            break
        fetch_repo_reviews(conn, session, token, repo, since_date)
        print()

    # Also fetch Copilot agentic review comments (github-actions[bot])
    print("Fetching Copilot agentic review comments (github-actions[bot])...")
    for repo in repos:
        if _shutdown:
            break
        fetch_copilot_issue_comments(conn, session, token, repo, since_date)
    print()

    conn.close()
    print("Done!")


if __name__ == "__main__":
    main()
