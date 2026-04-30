#!/usr/bin/env python3
"""Export all auxiliary tables to CSVs in data/ for git tracking.

This is the *companion* to load_csv.py. After fetching new data
(fetch.py / fetch_comments.py / fetch_pr_pushes.py / fetch_mergers.py /
fetch_copilot_requesters.py), run this to refresh the committed CSV
backups so the data survives DB deletion/clones.

Items table is exported by a separate process (data/items.csv.gz is large
and updated less often). This script handles the smaller auxiliary tables:
  - pr_first_comment      -> data/pr_first_comment.csv
  - pr_push_events        -> data/pr_push_events.csv.gz
  - pr_push_progress      -> data/pr_push_progress.csv

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

# (table_name, csv_filename, columns) — gzip if filename ends in .gz
EXPORTS = [
    ("pr_first_comment",
     "pr_first_comment.csv",
     ["repo", "number", "first_comment_at", "commenter"]),
    ("pr_push_events",
     "pr_push_events.csv.gz",
     ["repo", "number", "event_id", "kind", "ts"]),
    ("pr_push_progress",
     "pr_push_progress.csv",
     ["repo", "number", "last_fetched_at", "is_complete"]),
]


def table_exists(conn, name):
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def main():
    p = argparse.ArgumentParser(description="Export auxiliary tables to CSV.")
    p.add_argument("--db", default=DEFAULT_DB)
    args = p.parse_args()

    if not os.path.exists(args.db):
        print(f"ERROR: DB not found: {args.db}")
        sys.exit(1)

    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)

    for table, fname, cols in EXPORTS:
        if not table_exists(conn, table):
            print(f"  (skipping {table}: table does not exist)")
            continue
        path = os.path.join(DATA_DIR, fname)
        col_list = ", ".join(cols)
        rows = conn.execute(
            f"SELECT {col_list} FROM {table} ORDER BY {cols[0]}, {cols[1]}"
            if len(cols) >= 2 else
            f"SELECT {col_list} FROM {table} ORDER BY {cols[0]}"
        )
        opener = gzip.open if path.endswith(".gz") else open
        n = 0
        with opener(path, "wt", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(cols)
            for r in rows:
                w.writerow(["" if v is None else v for v in r])
                n += 1
        print(f"  wrote {n:,} rows -> {path}")

    conn.close()


if __name__ == "__main__":
    main()
