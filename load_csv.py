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


# (table_name, csv_filename, schema, columns)
# Schema is the CREATE TABLE statement; gzip if filename ends in .gz.
AUX_TABLES = [
    (
        "pr_first_comment",
        "data/pr_first_comment.csv",
        """CREATE TABLE IF NOT EXISTS pr_first_comment (
            repo TEXT NOT NULL,
            number INTEGER NOT NULL,
            first_comment_at TEXT NOT NULL,
            commenter TEXT NOT NULL,
            PRIMARY KEY (repo, number)
        );""",
        ["repo", "number", "first_comment_at", "commenter"],
    ),
    (
        "pr_push_events",
        "data/pr_push_events.csv.gz",
        """CREATE TABLE IF NOT EXISTS pr_push_events (
            repo TEXT NOT NULL,
            number INTEGER NOT NULL,
            event_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            ts TEXT NOT NULL,
            PRIMARY KEY (repo, number, kind, event_id)
        );
        CREATE INDEX IF NOT EXISTS idx_pr_push_events_repo_number
            ON pr_push_events(repo, number);""",
        ["repo", "number", "event_id", "kind", "ts"],
    ),
    (
        "pr_push_progress",
        "data/pr_push_progress.csv",
        """CREATE TABLE IF NOT EXISTS pr_push_progress (
            repo TEXT NOT NULL,
            number INTEGER NOT NULL,
            last_fetched_at TEXT NOT NULL,
            is_complete INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (repo, number)
        );""",
        ["repo", "number", "last_fetched_at", "is_complete"],
    ),
]


def load_aux_tables(conn):
    """Load auxiliary CSV tables (skipped silently if files missing)."""
    for table, csv_rel, schema, cols in AUX_TABLES:
        csv_path = Path(csv_rel)
        if not csv_path.exists():
            print(f"  (skipping {table}: {csv_rel} not found)")
            continue
        conn.executescript(schema)
        opener = gzip.open if str(csv_path).endswith(".gz") else open
        placeholders = ",".join("?" * len(cols))
        col_list = ",".join(cols)
        n = 0
        with opener(csv_path, "rt", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            next(reader)  # header
            batch = []
            for row in reader:
                row = nullify(row)
                while len(row) < len(cols):
                    row.append(None)
                batch.append(row[:len(cols)])
                if len(batch) >= 5000:
                    conn.executemany(
                        f"INSERT OR REPLACE INTO {table} ({col_list}) VALUES ({placeholders})",
                        batch,
                    )
                    n += len(batch)
                    batch = []
            if batch:
                conn.executemany(
                    f"INSERT OR REPLACE INTO {table} ({col_list}) VALUES ({placeholders})",
                    batch,
                )
                n += len(batch)
        conn.commit()
        print(f"  loaded {n:,} rows into {table} (from {csv_rel})")


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
            copilot_requester TEXT,
            copilot_trailer INTEGER,
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

    # Column list must match the CREATE TABLE above — update both together
    ITEMS_COLUMNS = [
        "repo", "number", "created_at", "closed_at", "state", "is_pull_request",
        "merged_at", "labels", "author", "merged_by", "copilot_requester", "copilot_trailer",
    ]
    col_list = ",".join(ITEMS_COLUMNS)
    placeholders = ",".join("?" * len(ITEMS_COLUMNS))
    insert_sql = f"INSERT OR REPLACE INTO items ({col_list}) VALUES ({placeholders})"

    print(f"Loading {CSV_PATH}...")
    count = 0
    with gzip.open(csv_path, "rt", newline="") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        batch = []
        for row in reader:
            row = nullify(row)
            while len(row) < len(ITEMS_COLUMNS):
                row.append(None)
            batch.append(row[:len(ITEMS_COLUMNS)])
            if len(batch) >= 10000:
                conn.executemany(insert_sql, batch)
                count += len(batch)
                batch = []
                if count % 100000 == 0:
                    print(f"  {count:,} rows...")
        if batch:
            conn.executemany(insert_sql, batch)
            count += len(batch)

    conn.commit()

    # ─── Auxiliary tables (preserved data: comments, pushes, etc.) ───
    load_aux_tables(conn)

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
