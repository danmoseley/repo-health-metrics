# Copilot Instructions for repo-health-metrics

## Repository Basics

- Default branch is `master` (not main).
- Squash merge only — merge commits are disabled.
- DB `pr-dashboard.db` is gitignored; CSVs in `data/` are the source of truth.
- Restore DB: `python load_csv.py --force`
- Backup DB: `python backup_csvs.py`
- Generate charts: `python analyze.py`

## Data Preservation

**Always commit fetched data back to the repo.** GitHub API calls are slow and
rate-limited (a full fetch is hours-long), so re-querying for new chart work is
wasteful. The committed CSVs let anyone reproduce the dashboard locally without
hitting GitHub.

After running any fetcher (`fetch.py`, `fetch_comments.py`, `fetch_pr_pushes.py`,
`fetch_mergers.py`, `fetch_copilot_requesters.py`):

```bash
python backup_csvs.py            # exports auxiliary tables to data/*.csv[.gz]
# (items.csv.gz is exported separately — see analysis.md)
git add data/*.csv*
git commit -m "Refresh data backups"
```

## GitHub CLI Auth

This repo is owned by `danmoseley`. If `gh` commands return 403, run
`gh auth switch --user danmoseley` — multiple accounts may be configured.

## Chart & Analysis Guidelines

### Visual Clarity

- All series must be clearly distinguishable by eye. Use per-repo colors
  (from `REPO_COLORS` in analyze.py) consistently.
- When a chart shows two dimensions (e.g., Copilot vs Human *per repo*), use
  the repo color for both lines and distinguish with line style: solid vs
  dashed. Thicker solid (2.5) for the primary, thinner dashed (1.5) for the
  secondary. Include the repo short name in each legend entry
  (e.g., "runtime Copilot", "runtime Human").
- Single-dimension-per-repo charts should use `get_color(repo)` and
  `label_line_ends()` for direct labeling.

### Insight Boxes

- Insight text must accurately describe what the data **actually shows**, not
  what we hope it shows. Never claim a trend unless the chart confirms it.
- If data is noisy/volatile, say so: "Volatile — watch for sustained shifts."
- Include a one-line explanation of what the metric measures and why it matters.

### Data Integrity

- Don't mix apples and oranges. If a chart title says "per reviewed PR", don't
  include PRs that weren't reviewed.
- When computing rolling averages that include zeros for non-events, be explicit
  about it or filter them out depending on what the chart is meant to show.

### Conventions

- Use 4-week rolling windows for weekly data to smooth noise.
- Require minimum sample size (≥3–5 data points per window) before plotting.
- Cap outliers sensibly (e.g., time-to-feedback capped at 168 hours / 1 week).
- Use `robust_ylim()` for y-axis scaling where appropriate.
- Keep chart titles single-line (no `\n`). Titles are written to
  `charts/chart_index.tsv`; embedded newlines corrupt the TSV.
- Keep chart labels/text honest and synchronized with implementation:
  title, y-axis units, insight bullets, and PR description must match the actual
  windowing/bucketing/math (no stale claims like "52-week avg" on daily charts).
- Avoid hardcoded narrative claims in insights (e.g., specific trend causes or
  fixed percentages) unless directly supported by the rendered data.

### Chart Change Workflow (Recurring Feedback)

- For chart-only changes, prefer existing local data (`load_csv.py` + `analyze.py`);
  do not re-fetch from GitHub unless explicitly needed.
- Avoid artificial trailing drops from incomplete buckets. End each series at the
  latest date with real data (or explicitly extrapolate), never at synthetic
  future buckets.
- Keep PR scope tight: when adding/modifying one chart, avoid unrelated chart or
  file churn. If extra files appear, clean up before requesting review.
- Regenerate and commit required artifacts for the changed chart(s), including
  all changed `charts/*.png` outputs and `charts/chart_index.tsv` when
  titles/charts are added or changed. Special case: when adding a new chart,
  include the new PNG + index entry in the same PR (not only script changes).
- If reviewer feedback reveals a recurring graph/report mistake that is now fixed,
  update this file in the same PR so future agents inherit the guidance.

## Hydration Scripts

- Scripts typically accept `--db` and optional `--repos`.
- If `--repos` is omitted they auto-discover repos via
  `SELECT DISTINCT repo FROM items ORDER BY repo`.

## Workflow

- The weekly-refresh workflow runs Monday 03:17 UTC.
- It restores DB → fetches → backs up → generates charts → creates a PR.
- The workflow uses `GITHUB_TOKEN` (1,000 req/hour for public repos).
