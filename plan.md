# Runtime Repo Health Metrics: Feasibility Proposal

## Goal

Answer sustainability questions about dotnet/runtime and comparable repos using historical GitHub data:
- Is the open-issue/PR backlog growing, stable, or shrinking?
- Is PR merge rate keeping up with issue inflow?
- Are there seasonal patterns or long-term trends?
- Is progress (PRs merged) slowing?
- How does runtime compare to peer repos in the same and other ecosystems?

## Comparison Repos

| Repo | Role in comparison | Why this repo |
|------|-------------------|---------------|
| **dotnet/runtime** | Primary subject | The repo we care about |
| **dotnet/roslyn** | Same org sibling | Same team culture, release cadence, similar scale (~60-70K items). If runtime is struggling but roslyn isn't, that's a signal. |
| **microsoft/vscode** | Same company, different ecosystem | Enormous scale (~180K+ issues), TypeScript, huge community inflow. Shows what "issue tsunami" looks like at extreme scale. |
| **rust-lang/rust** | External benchmark (community-governed) | Language runtime, ~90K+ issues, famously rigorous triage. "What does healthy look like?" |
| **golang/go** | External benchmark (corporate-governed) | Near-identical character to runtime: language runtime + stdlib in one repo, ~70K+ issues, small centralized Google core team + community. Conservative triage (issues stay open by design). |

This set gives us: two within dotnet (sibling comparison), one MS-but-different-ecosystem, and two external language-runtime benchmarks (one community-driven, one corporate-driven). A Go vs Rust vs .NET runtime-to-runtime comparison is about as clean an apples-to-apples sustainability benchmark as exists.

## The Core Insight

GitHub doesn't provide historical snapshots, but it **does** give you event timestamps on every issue and PR: `created_at`, `closed_at`, `merged_at`. With the full set of events, you can **reconstruct the state at any point in time**. For any date T:

```
open_issues(T) = count of issues where created_at <= T AND (closed_at IS NULL OR closed_at > T)
open_prs(T)    = count of PRs where created_at <= T AND (merged_at IS NULL AND closed_at IS NULL, OR closed/merged > T)
```

This gives you a complete time series going back to the repo's creation (May 2020, though it inherits history from coreclr/corefx).

## Scale Estimates (All 5 Repos)

| Repo | Est. Issues | Est. PRs | Est. /issues Pages | Est. /pulls Pages | Total API Pages |
|------|-------------|----------|-------------------|-------------------|-----------------|
| dotnet/runtime | ~70,500 | ~40,000 | ~1,105 | ~400 | ~1,505 |
| dotnet/roslyn | ~70,000 | ~35,000 | ~1,050 | ~350 | ~1,400 |
| microsoft/vscode | ~180,000 | ~15,000 | ~1,950 | ~150 | ~2,100 |
| rust-lang/rust | ~90,000 | ~50,000 | ~1,400 | ~500 | ~1,900 |
| golang/go | ~70,000 | ~5,000 | ~750 | ~50 | ~800 |
| **Total** | | | | | **~7,700** |

Note: The `/issues` endpoint returns both issues AND PRs (mixed), so its page count = (issues + PRs) / 100. PRs are fetched separately via `/pulls` to get `merged_at`, and skipped client-side in the `/issues` response. This means PRs are "seen" twice but only stored once (via `/pulls`).

At 5,000 REST requests/hour, the full 5-repo fetch takes ~90-120 minutes (with 350ms pacing). **A single evening's work.**

## API Strategy: REST (Implemented)

The script uses the REST API for simplicity:
- **PRs**: `/repos/{owner}/{name}/pulls?state=all` -- gives `created_at`, `closed_at`, `merged_at`
- **Issues**: `/repos/{owner}/{name}/issues?state=all` -- gives `created_at`, `closed_at`, `state`; PRs are filtered out client-side (fetched separately for `merged_at`)
- Rate limit: **5,000 requests/hour** (authenticated)
- Request pacing: 350ms between requests (well under secondary limits)
- Pagination: 100 items per page, sorted by `created` ascending (stable ordering)

## Resilience: Rate Limiting, Errors, and Recovery

Fetching ~6,250 pages across 5 repos is not a single fragile operation -- the script is designed to be robust.

### Rate Limit Handling (Implemented)

- **Proactive throttling**: Checks `X-RateLimit-Remaining` on every response. If < 100, sleeps until reset.
- **Reactive backoff**: On 403, checks `Retry-After` header (secondary limit) or `X-RateLimit-Reset` (primary). Sleeps and retries automatically. Rate limit retries are NOT counted against the retry budget.
- **Pacing**: 350ms between requests by default (configurable via `--delay`). At this pace, fetching 1,100 pages takes ~6-7 minutes of wall time.

### Error Recovery (Implemented)

**Page-level checkpointing** -- after every page, progress is committed to SQLite:
- `fetch_progress` table tracks: repo, item_type, last completed page, items fetched, status
- On any failure (network, 5xx, crash, Ctrl+C), re-running the script **resumes from the last checkpoint**
- Zero wasted work on restart

**Retry logic:**
- Transient errors (5xx, network timeout): Up to 5 retries with exponential backoff (1s, 4s, 16s, 64s, 120s cap)
- 403 rate limit: Sleep until reset, retry indefinitely (not counted as retry)
- 422 validation error: Log and skip
- Ctrl+C / SIGINT: Finishes current page, saves checkpoint, exits cleanly. Second Ctrl+C force-quits.

### Worst-Case Scenarios

| Scenario | Impact | Recovery |
|----------|--------|----------|
| Hit primary rate limit mid-fetch | Paused ~30-60 min | Auto-sleeps until reset, resumes |
| Hit secondary rate limit | Paused ~2 min | Auto-backoff via Retry-After |
| Network drops mid-fetch | Lost 1 page | Retries with backoff; checkpoint saves prior pages |
| Process crashes | Lost current page | Re-run resumes from last checkpoint |
| GitHub outage (hours) | Delayed | Retries with backoff until success |
| Token expires mid-fetch | 401 errors | Re-run with fresh token; resumes from checkpoint |
| Ctrl+C during fetch | Graceful stop | Checkpoint saved; re-run resumes |
| Fetch interrupted 3 repos in | 2 repos remain | Re-run skips completed repos, resumes interrupted one |

## Data Model

SQLite database (`pr-dashboard.db`). Three tables:

```sql
CREATE TABLE items (
    repo TEXT NOT NULL,           -- e.g., 'dotnet/runtime'
    number INTEGER NOT NULL,
    created_at TEXT,
    closed_at TEXT,
    state TEXT,                   -- OPEN, CLOSED
    is_pull_request INTEGER,      -- 0 or 1
    merged_at TEXT,               -- NULL for issues, set for merged PRs
    labels TEXT,                  -- JSON array of label names
    PRIMARY KEY (repo, number)
);

CREATE TABLE fetch_progress (
    repo TEXT NOT NULL,
    item_type TEXT NOT NULL,       -- 'issue' or 'pr'
    last_page INTEGER,
    items_fetched INTEGER,
    total_expected INTEGER,
    updated_at TEXT,
    status TEXT,                   -- 'pending', 'in_progress', 'complete', 'failed', 'interrupted'
    PRIMARY KEY (repo, item_type)
);

CREATE TABLE repo_snapshots (
    repo TEXT,
    week_start TEXT,
    open_issues INTEGER,
    open_prs INTEGER,
    issues_opened_this_week INTEGER,
    issues_closed_this_week INTEGER,
    prs_opened_this_week INTEGER,
    prs_merged_this_week INTEGER,
    prs_closed_unmerged_this_week INTEGER,
    PRIMARY KEY (repo, week_start)
);
```

## Script Usage

```bash
cd c:\git\pr-dashboard
pip install -r requirements.txt

# Fetch all 5 repos now
python fetch.py

# Wait 4 hours (midnight) then fetch
python fetch.py --wait 14400

# Fetch just one repo (for testing)
python fetch.py --repos golang/go

# Resume after interruption (automatic -- just re-run)
python fetch.py

# Re-fetch from scratch (clear checkpoints)
python fetch.py --reset

# Custom DB path and request pacing
python fetch.py --db mydata.db --delay 0.5
```

## Analyses Enabled

With this data, you can compute:

### Time Series (weekly or monthly granularity)
1. **Open issue count over time** -- is the backlog growing?
2. **Open PR count over time** -- are PRs piling up?
3. **Issue inflow rate** -- issues opened per week
4. **Issue closure rate** -- issues closed per week
5. **PR merge rate** -- PRs merged per week
6. **Net flow** (inflow - outflow) -- the "sustainability number"

### Derived Metrics
7. **Median time-to-merge** for PRs (by month) -- is review getting slower?
8. **Median time-to-close** for issues -- are issues aging?
9. **Issue/PR ratio** -- how much reported work vs. completed work?
10. **"Stale" counts** -- issues/PRs open > 1 year with no recent activity
11. **Seasonal patterns** -- .NET release cycles should show up clearly

### Comparisons (all 5 repos)
12. **Cross-repo overlays** -- same metric, all repos on one chart (normalized by scale where appropriate)
13. **Relative sustainability score** -- net flow normalized by repo size
14. **Triage effectiveness** -- how quickly do different projects close/merge vs. accumulate?
15. **Cultural signatures** -- Go's "leave issues open" philosophy vs. Rust's aggressive triage vs. .NET's patterns

## What You CAN'T Get (Limitations)

- **Reopen events**: If an issue was closed then reopened, you only see the final state. This slightly understates historical closure rates. (Workaround: fetch timeline events, but this is 1 API call per issue -- prohibitively expensive at 70K issues.)
- **Label changes over time**: You see current labels, not historical. Can't track "how many bugs were open in 2022" unless labels haven't changed.
- **PR review time**: Would need review events (extra API calls per PR).
- **Contributor data**: Would need to fetch author info (adds a field to GraphQL, or an extra API call per issue for REST). Cheap in GraphQL though.
- **Transfer/migration artifacts**: Some issues may have been transferred from other repos, which distorts early dates.

These limitations are minor for the sustainability questions you're asking.

## Implementation Plan

### Phase 1: Data Collection (DONE -- fetch.py)
- `fetch.py` paginates through all issues+PRs for all 5 repos
- Built-in rate limit detection, exponential backoff, and page-level checkpointing
- Store raw data in SQLite (`items` table with `repo` column)
- Fetch order: smallest repo first (golang/go) to validate the pipeline, then the rest
- Graceful shutdown on Ctrl+C; re-run resumes automatically

### Phase 2: Analysis & Visualization (TODO)
- Compute weekly time series from raw events (populate `repo_snapshots`)
- Generate charts (matplotlib or plotly) -- one set per repo, plus cross-repo overlays
- Key charts: open issues over time, open PRs over time, weekly inflow/outflow, net flow, time-to-merge distributions

### Phase 3: Incremental Updates & Iteration (TODO)
- Add `--update` mode to fetch only items changed since last run
- Schedule weekly refresh
- Iterate on metrics based on what the initial charts reveal

## Cost Summary (All 5 Repos)

| Resource | Cost |
|----------|------|
| API calls (one-time full fetch) | ~7,700 REST requests |
| Time to fetch | ~90-120 minutes (with 350ms pacing) |
| Rate limit budget used | ~1.5-2 hours of hourly quota |
| Incremental updates (daily, all repos) | ~100-500 requests |
| Local storage | ~50-150 MB (SQLite) |

## Files

```
c:\git\pr-dashboard\
    plan.md              -- this document
    fetch.py             -- data collection script (Phase 1)
    requirements.txt     -- Python dependencies
    pr-dashboard.db      -- SQLite database (created at runtime)
```

## Key Takeaway

**This is very feasible.** The entire history of all 5 repos (~625K issues+PRs) can be fetched in a single evening, stored in a modest SQLite database, and analyzed locally. No sampling needed. The checkpoint/resume design means rate limits and errors are pauses, not failures. Incremental updates are cheap. The main work is writing the fetch script and the analysis/charting code, not fighting API limits.
