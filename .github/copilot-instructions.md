# Copilot Instructions for repo-health-metrics

## Repository Basics

- Default branch is `master` (not main).
- Squash merge only — merge commits are disabled.
- DB `pr-dashboard.db` is gitignored; CSVs in `data/` are the source of truth.
- Restore DB: `python load_csv.py --force`
- Backup DB: `python backup_csvs.py`
- Generate charts: `python analyze.py`

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

## Hydration Scripts

- Scripts typically accept `--db` and optional `--repos`.
- If `--repos` is omitted they auto-discover repos via
  `SELECT DISTINCT repo FROM items ORDER BY repo`.

## Workflow

- The weekly-refresh workflow runs Monday 03:17 UTC.
- It restores DB → fetches → backs up → generates charts → creates a PR.
- The workflow uses `GITHUB_TOKEN` (1,000 req/hour for public repos).
