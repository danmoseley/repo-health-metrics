#!/usr/bin/env python3
"""Rebuild the SQLite database from the compressed CSV export."""

import sqlite3
import csv
import gzip
import sys
from pathlib import Path

DB_PATH = "pr-dashboard.db"
CSV_PATH = "data/items.csv.gz"


def nullify(row):
    """Convert empty strings to None (SQL NULL)."""
    return [None if v == "" else v for v in row]


def main():
    csv_path = Path(CSV_PATH)
    if not csv_path.exists():
        print(f"ERROR: {CSV_PATH} not found")
        sys.exit(1)

    db_path = Path(DB_PATH)
    if db_path.exists():
        print(f"WARNING: {DB_PATH} already exists. Delete it first to rebuild.")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript("""
        CREATE TABLE items (
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
        CREATE INDEX idx_items_repo_type ON items(repo, is_pull_request);
        CREATE INDEX idx_items_created ON items(repo, created_at);

        CREATE TABLE fetch_progress (
            repo TEXT NOT NULL,
            item_type TEXT NOT NULL,
            last_page INTEGER NOT NULL DEFAULT 0,
            items_fetched INTEGER NOT NULL DEFAULT 0,
            total_expected INTEGER,
            updated_at TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            PRIMARY KEY (repo, item_type)
        );
    """)

    print(f"Loading {CSV_PATH}...")
    count = 0
    with gzip.open(csv_path, "rt", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        ncols = len(header)
        batch = []
        for row in reader:
            row = nullify(row)
            # Pad with None if CSV has fewer columns than table (e.g., no author/merged_by)
            while len(row) < 10:
                row.append(None)
            batch.append(row[:10])
            if len(batch) >= 10000:
                conn.executemany(
                    "INSERT OR REPLACE INTO items "
                    "(repo, number, created_at, closed_at, state, is_pull_request, "
                    "merged_at, labels, author, merged_by) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)", batch
                )
                count += len(batch)
                batch = []
                if count % 100000 == 0:
                    print(f"  {count:,} rows...")
        if batch:
            conn.executemany(
                "INSERT OR REPLACE INTO items "
                "(repo, number, created_at, closed_at, state, is_pull_request, "
                "merged_at, labels, author, merged_by) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)", batch
            )
            count += len(batch)

    conn.commit()

    # Mark all repos as complete in fetch_progress
    # But only mark a type as complete if we actually have items of that type
    repos = conn.execute("SELECT DISTINCT repo FROM items").fetchall()
    for (repo,) in repos:
        for item_type in ("issue", "pr"):
            is_pr = 1 if item_type == "pr" else 0
            n = conn.execute(
                "SELECT COUNT(*) FROM items WHERE repo=? AND is_pull_request=?",
                (repo, is_pr),
            ).fetchone()[0]
            if n > 0:
                conn.execute(
                    "INSERT OR REPLACE INTO fetch_progress "
                    "(repo, item_type, last_page, items_fetched, status) "
                    "VALUES (?, ?, 0, ?, 'complete')",
                    (repo, item_type, n),
                )
    conn.commit()
    conn.close()

    print(f"Done! {count:,} rows loaded into {DB_PATH}")


if __name__ == "__main__":
    main()
