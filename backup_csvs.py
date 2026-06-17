#!/usr/bin/env python3
"""Export all tables to CSVs in data/ for git tracking.

This is the *companion* to load_csv.py. After fetching new data
(fetch.py / fetch_comments.py / fetch_pr_pushes.py / fetch_mergers.py /
fetch_copilot_requesters.py / fetch_reviews.py), run this to refresh the
committed CSV backups so the data survives DB deletion/clones.

Tables exported:
  - items                    -> data/items.csv.gz (main data)
  - pr_first_comment          -> data/pr_first_comment.csv
  - pr_push_events            -> data/pr_push_events.csv.gz
  - pr_push_progress          -> data/pr_push_progress.csv
  - pr_reviews                -> data/pr_reviews.csv.gz
  - pr_review_comments        -> data/pr_review_comments.csv.gz
  - pr_commit_stats           -> data/pr_commit_stats.csv.gz
  - review_fetch_progress     -> data/review_fetch_progress.csv
  - pr_copilot_issue_comments -> data/pr_copilot_issue_comments.csv

Usage:
    python backup_csvs.py            # default DB path
    python backup_csvs.py --db ...
"""

import argparse
import csv
import gzip
import os
import sqlite3
import sys

DEFAULT_DB = "pr-dashboard.db"
DATA_DIR = "data"

# (table_name, csv_filename, columns, order_by) — gzip if filename ends in .gz
EXPORTS = [
    ("items",
     "items.csv.gz",
     ["repo", "number", "created_at", "closed_at", "state", "is_pull_request",
      "merged_at", "labels", "author", "merged_by", "merged_by_checked",
      "copilot_requester", "copilot_trailer", "title"],
     "repo, number"),
    ("pr_first_comment",
     "pr_first_comment.csv",
     ["repo", "number", "first_comment_at", "commenter"],
     "repo, number"),
    ("pr_push_events",
     "pr_push_events.csv.gz",
     ["repo", "number", "event_id", "kind", "ts"],
     "repo, number, ts, event_id"),
    ("pr_push_progress",
     "pr_push_progress.csv",
     ["repo", "number", "last_fetched_at", "is_complete"],
     "repo, number"),
    ("fetch_progress",
     "fetch_progress.csv",
     ["repo", "item_type", "last_page", "items_fetched", "total_expected",
      "updated_at", "status", "sync_started_at", "next_url"],
     "repo, item_type"),
    ("pr_reviews",
     "pr_reviews.csv.gz",
     ["repo", "number", "review_id", "author", "author_type",
      "state", "submitted_at", "commit_sha"],
     "repo, number, review_id"),
    ("pr_review_comments",
     "pr_review_comments.csv.gz",
     ["repo", "number", "comment_id", "review_id", "author", "author_type",
      "body_has_suggestion", "path", "created_at", "is_resolved", "is_outdated"],
     "repo, number, comment_id"),
    ("pr_commit_stats",
     "pr_commit_stats.csv.gz",
     ["repo", "number", "sha", "committed_date", "additions", "deletions", "message"],
     "repo, number, committed_date, sha"),
    ("review_fetch_progress",
     "review_fetch_progress.csv",
     ["repo", "number", "status", "fetched_at"],
     "repo, number"),
    ("pr_copilot_issue_comments",
     "pr_copilot_issue_comments.csv",
     ["repo", "number", "comment_id", "author", "created_at", "body_length"],
     "repo, number, comment_id"),
]


def table_exists(conn, name):
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def get_table_columns(conn, table):
    """Return the set of column names that actually exist in a table."""
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def main():
    p = argparse.ArgumentParser(description="Export all tables to CSV.")
    p.add_argument("--db", default=DEFAULT_DB)
    args = p.parse_args()

    if not os.path.exists(args.db):
        print(f"ERROR: DB not found: {args.db}")
        sys.exit(1)

    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)

    for table, fname, cols, order in EXPORTS:
        if not table_exists(conn, table):
            print(f"  (skipping {table}: table does not exist)")
            continue
        # Only export columns that exist in this DB (handles partial migrations)
        existing = get_table_columns(conn, table)
        actual_cols = [c for c in cols if c in existing]
        if not actual_cols:
            print(f"  (skipping {table}: no matching columns)")
            continue
        path = os.path.join(DATA_DIR, fname)
        col_list = ", ".join(actual_cols)
        rows = conn.execute(f"SELECT {col_list} FROM {table} ORDER BY {order}")
        opener = gzip.open if path.endswith(".gz") else open
        n = 0
        with opener(path, "wt", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(actual_cols)
            for r in rows:
                w.writerow(["" if v is None else v for v in r])
                n += 1
        print(f"  wrote {n:,} rows -> {path}")

    conn.close()


if __name__ == "__main__":
    main()
