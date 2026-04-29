# Repo Health Metrics

Sustainability and health metrics for large GitHub repositories, comparing dotnet/runtime, dotnet/roslyn, dotnet/maui, microsoft/vscode, microsoft/vcpkg, microsoft/aspire, rust-lang/rust, and golang/go.

**[View the charts](https://danmoseley.github.io/repo-health-metrics/)**

## Data preservation

> **Always commit fetched data back to the repo.** GitHub API calls are slow and rate-limited (a full fetch is hours-long), so re-querying for new chart work is wasteful. The committed CSVs let anyone reproduce the dashboard locally without hitting GitHub.

After running any fetcher (`fetch.py`, `fetch_comments.py`, `fetch_pr_pushes.py`, `fetch_mergers.py`, `fetch_copilot_requesters.py`):

```bash
python backup_csvs.py            # exports auxiliary tables to data/*.csv[.gz]
# (items.csv.gz is exported separately — see analysis.md)
git add data/*.csv*
git commit -m "Refresh data backups"
```

To restore on a fresh clone:

```bash
python load_csv.py               # reads data/items.csv.gz + auxiliary CSVs
python analyze.py                # generates charts
```

The `pr-dashboard.db` SQLite file is **gitignored**; the CSVs are the source of truth in git.

