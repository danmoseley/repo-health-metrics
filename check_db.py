import sqlite3
conn = sqlite3.connect(r'C:\git\repo-health-metrics\pr-dashboard.db')
rows = conn.execute("SELECT repo, item_type, updated_at FROM fetch_progress WHERE status='complete' ORDER BY updated_at DESC LIMIT 10").fetchall()
for r in rows:
    print(f'{r[0]:25s} {r[1]:8s} completed={r[2]}')

print()
for repo in ['dotnet/runtime', 'microsoft/aspire', 'microsoft/vscode']:
    r = conn.execute("SELECT MAX(created_at) FROM items WHERE repo=?", (repo,)).fetchone()
    print(f'{repo:25s} max_created_at={r[0]}')
