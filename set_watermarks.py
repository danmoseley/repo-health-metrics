"""Set initial watermarks for existing complete repos based on their max item timestamps."""
import sqlite3
from datetime import datetime, timezone

DB = r'C:\git\repo-health-metrics\pr-dashboard.db'
conn = sqlite3.connect(DB)

# Add sync_started_at column if missing
try:
    conn.execute("ALTER TABLE fetch_progress ADD COLUMN sync_started_at TEXT")
    print("Added sync_started_at column")
except sqlite3.OperationalError:
    print("sync_started_at column already exists")

# Set schema version
ver = conn.execute("PRAGMA user_version").fetchone()[0]
if ver == 0:
    conn.execute("PRAGMA user_version = 1")
    print("Set schema version to 1")
else:
    print(f"Schema version already {ver}")

# For each complete repo, set sync_started_at to the max created_at of its items
# (conservative — this means the first --update will re-check items from before this date
# due to the 2-day overlap, but that's safe with upserts)
rows = conn.execute(
    "SELECT DISTINCT repo FROM fetch_progress WHERE status='complete'"
).fetchall()

for (repo,) in rows:
    existing = conn.execute(
        "SELECT sync_started_at FROM fetch_progress WHERE repo=? AND item_type='pr'",
        (repo,)
    ).fetchone()
    if existing and existing[0]:
        print(f"  {repo:25s} already has watermark: {existing[0]}")
        continue

    max_ts = conn.execute(
        "SELECT MAX(created_at) FROM items WHERE repo=?", (repo,)
    ).fetchone()[0]
    if max_ts:
        # Use the max created_at as initial watermark
        for item_type in ('issue', 'pr'):
            conn.execute(
                "UPDATE fetch_progress SET sync_started_at = ? WHERE repo = ? AND item_type = ?",
                (max_ts, repo, item_type)
            )
        print(f"  {repo:25s} watermark set to {max_ts}")
    else:
        print(f"  {repo:25s} no items, skipping")

conn.commit()
conn.close()
print("\nDone.")
