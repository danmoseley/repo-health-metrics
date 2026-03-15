#!/usr/bin/env python3
"""
Repo Health Analysis & Chart Generation

Reads the SQLite database populated by fetch.py and generates:
1. Open issues over time (per repo)
2. Open PRs over time (per repo)
3. Weekly inflow/outflow (issues opened vs closed)
4. Weekly PR merge rate
5. Net flow (sustainability number)
6. Cross-repo comparison overlays

Usage:
    python analyze.py                      # Generate all charts
    python analyze.py --repos dotnet/runtime  # Single repo
    python analyze.py --db mydata.db       # Custom DB
    python analyze.py --output ./charts    # Custom output dir
"""

import sqlite3
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

try:
    import matplotlib
    matplotlib.use("Agg")  # non-interactive backend
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.ticker import FuncFormatter
except ImportError:
    print("ERROR: matplotlib is required. Install with: pip install matplotlib")
    sys.exit(1)

DEFAULT_DB = "pr-dashboard.db"
DEFAULT_OUTPUT = "charts"

# Consistent color palette across charts
REPO_COLORS = {
    "dotnet/runtime": "#512BD4",   # .NET purple
    "dotnet/roslyn": "#E91E63",    # pink/magenta
    "dotnet/maui": "#FF8F00",      # amber/orange
    "microsoft/vscode": "#007ACC", # VS Code blue
    "rust-lang/rust": "#B7410E",   # rust red-brown
    "golang/go": "#00897B",        # teal
}

REPO_SHORT = {
    "dotnet/runtime": "runtime",
    "dotnet/roslyn": "roslyn",
    "dotnet/maui": "maui",
    "microsoft/vscode": "vscode",
    "rust-lang/rust": "rust",
    "golang/go": "go",
}

# Go uses Gerrit for code review, not GitHub PRs — exclude from PR charts
GERRIT_REPOS = {"golang/go"}

# Repos with early migration artifacts — trim chart data before this date
# Repos with early migration artifacts — trim chart data before this date
# Go migrated from Google Code in Q4 2014; mass-closed 8K issues on import
# Runtime (coreclr/corefx) started late 2014; early months have startup noise
REPO_START_DATE = {
    "golang/go": "2015-01-01",
    "dotnet/runtime": "2015-01-01",
}

# Repos where a bot merges all PRs — merged_by is useless for maintainer analysis
BOT_MERGER_REPOS = {"rust-lang/rust"}

# Known bot accounts to exclude from maintainer counts
BOT_ACCOUNTS = {"bors", "rust-bors", "dotnet-bot", "dependabot[bot]", "github-actions[bot]",
                "renovate[bot]", "copilot-swe-agent[bot]", "Copilot",
                "dotnet-maestro[bot]"}

# Repo lineage: map display repo -> predecessor repos whose PRs should be included.
# Issues from predecessors were transferred to the successor repo, so only PRs need merging.
REPO_LINEAGE = {
    "dotnet/runtime": ["dotnet/coreclr", "dotnet/corefx"],
}

# Repos that are predecessors — don't display as standalone lines/charts
LEGACY_REPOS = set()
for _preds in REPO_LINEAGE.values():
    LEGACY_REPOS.update(_preds)


def effective_author(item):
    """For bot-authored PRs, attribute to the human requester.
    Uses copilot_requester (from ASSIGNED_EVENT), falls back to merged_by."""
    author = item.get("author")
    if author and author.lower() in {b.lower() for b in BOT_ACCOUNTS} | {"copilot"}:
        requester = item.get("copilot_requester")
        if requester:
            return requester
        merger = item.get("merged_by")
        if merger and merger.lower() not in {b.lower() for b in BOT_ACCOUNTS}:
            return merger
        return None
    return author


def get_color(repo):
    return REPO_COLORS.get(repo, "#888888")


def get_short(repo):
    return REPO_SHORT.get(repo, repo)


def load_items(conn, repo):
    """Load all items for a repo, sorted by created_at.
    
    For repos with lineage (e.g., dotnet/runtime), also loads PR items from
    predecessor repos (coreclr, corefx). Issues are NOT loaded from predecessors
    because they were transferred and already appear under the successor repo.
    """
    repos_to_load = [(repo, False)]  # (repo_name, prs_only)
    for predecessor in REPO_LINEAGE.get(repo, []):
        repos_to_load.append((predecessor, True))

    items = []
    for load_repo, prs_only in repos_to_load:
        if prs_only:
            sql = ("SELECT number, created_at, closed_at, state, is_pull_request, merged_at, "
                   "author, merged_by, copilot_requester "
                   "FROM items WHERE repo = ? AND is_pull_request = 1 ORDER BY created_at")
        else:
            sql = ("SELECT number, created_at, closed_at, state, is_pull_request, merged_at, "
                   "author, merged_by, copilot_requester "
                   "FROM items WHERE repo = ? ORDER BY created_at")
        rows = conn.execute(sql, (load_repo,)).fetchall()
        for r in rows:
            items.append({
                "number": r[0],
                "created_at": r[1],
                "closed_at": r[2],
                "state": r[3],
                "is_pr": bool(r[4]),
                "merged_at": r[5],
                "author": r[6],
                "merged_by": r[7],
                "copilot_requester": r[8],
            })

    # Fix transferred issue dates for repos with lineage
    if repo in REPO_LINEAGE:
        items = _fix_transferred_issue_dates(items)

    # For Gerrit repos (Go), treat closed_at as merged_at for PRs.
    # Go PRs are auto-closed when the Gerrit CL lands, so closed_at ≈ merge date.
    if repo in GERRIT_REPOS:
        for item in items:
            if item["is_pr"] and not item["merged_at"] and item["closed_at"]:
                item["merged_at"] = item["closed_at"]

    # Sort combined items by created_at
    items.sort(key=lambda x: x["created_at"] or "")

    # Trim items before repo start date (migration artifacts)
    start = REPO_START_DATE.get(repo)
    if start:
        items = [i for i in items if (i["created_at"] or "") >= start]

    return items


def _fix_transferred_issue_dates(items):
    """Fix artifact from coreclr/corefx -> runtime issue transfer.

    Both open AND closed issues were transferred from coreclr/corefx to runtime
    in Jan 2020. Closed issues lost their original closed_at dates — GitHub
    stamped them with the transfer date (Jan 30-31, 2020). ~25K issues show as
    "closed on Jan 31" when they were actually closed years earlier.

    Fix: for issues created before the transfer with closed_at in the transfer
    window (Jan 29 - Feb 2, 2020), set closed_at to created_at. This means they
    contribute zero to the running open-issue count (they were never meaningfully
    "open" in the runtime repo — they arrived already closed).
    """
    from datetime import date as date_type

    TRANSFER_START = date_type(2020, 1, 29)
    TRANSFER_END = date_type(2020, 2, 2)
    REPO_START = date_type(2019, 9, 1)  # runtime repo created ~Sep 2019

    n_fixed = 0
    for item in items:
        if item["is_pr"]:
            continue
        cd = parse_date(item["created_at"])
        cld = parse_date(item["closed_at"])
        if not cd or not cld:
            continue
        # Issue created before runtime existed, closed during the transfer window
        if cd < REPO_START and TRANSFER_START <= cld <= TRANSFER_END:
            # Was already closed in source repo — set closed_at = created_at
            # so it never counts as "open" in the running tally
            item["closed_at"] = item["created_at"]
            n_fixed += 1

    if n_fixed:
        print(f"    (neutralized {n_fixed:,} pre-closed transferred issues — "
              f"closed_at set to created_at)")
    return items


def parse_date(s):
    """Parse ISO date string to date object."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except (ValueError, AttributeError):
        return None


def week_start(d):
    """Get Monday of the week containing date d."""
    return d - timedelta(days=d.weekday())


def compute_weekly_series(items, end_date=None):
    """
    From raw items, compute weekly time series.
    Returns dict with weekly data points.
    """
    if not items:
        return {}

    if end_date is None:
        end_date = datetime.now().date()

    # Collect events by week
    created_issues = defaultdict(int)
    closed_issues = defaultdict(int)
    created_prs = defaultdict(int)
    merged_prs = defaultdict(int)
    closed_prs_unmerged = defaultdict(int)

    for item in items:
        cd = parse_date(item["created_at"])
        if not cd:
            continue
        w = week_start(cd)

        if item["is_pr"]:
            created_prs[w] += 1
            md = parse_date(item["merged_at"])
            if md:
                merged_prs[week_start(md)] += 1
            else:
                cld = parse_date(item["closed_at"])
                if cld:
                    closed_prs_unmerged[week_start(cld)] += 1
        else:
            created_issues[w] += 1
            cld = parse_date(item["closed_at"])
            if cld:
                closed_issues[week_start(cld)] += 1

    # Build list of all weeks
    all_dates = set()
    for d in [created_issues, closed_issues, created_prs, merged_prs, closed_prs_unmerged]:
        all_dates.update(d.keys())
    if not all_dates:
        return {}

    first_week = min(all_dates)
    last_week = week_start(end_date)
    weeks = []
    w = first_week
    while w <= last_week:
        weeks.append(w)
        w += timedelta(weeks=1)

    # Compute running open counts
    open_issues = []
    open_prs = []
    issue_opened = []
    issue_closed = []
    pr_opened = []
    pr_merged = []
    net_issue_flow = []
    net_pr_flow = []

    running_open_issues = 0
    running_open_prs = 0

    for w in weeks:
        ci = created_issues.get(w, 0)
        cli = closed_issues.get(w, 0)
        cp = created_prs.get(w, 0)
        mp = merged_prs.get(w, 0)
        cpu = closed_prs_unmerged.get(w, 0)

        running_open_issues += ci - cli
        running_open_prs += cp - mp - cpu

        open_issues.append(running_open_issues)
        open_prs.append(running_open_prs)
        issue_opened.append(ci)
        issue_closed.append(cli)
        pr_opened.append(cp)
        pr_merged.append(mp)
        net_issue_flow.append(ci - cli)
        net_pr_flow.append(cp - mp - cpu)

    return {
        "weeks": weeks,
        "open_issues": open_issues,
        "open_prs": open_prs,
        "issue_opened": issue_opened,
        "issue_closed": issue_closed,
        "pr_opened": pr_opened,
        "pr_merged": pr_merged,
        "net_issue_flow": net_issue_flow,
        "net_pr_flow": net_pr_flow,
    }


def compute_monthly_time_to_merge(items):
    """
    Compute 75th-percentile time-to-merge (in days) per month for merged PRs.
    P75 gives better resolution than median (which clusters on 0-3 days) while
    being more robust than mean against extreme outliers.
    Returns (months, p75s) lists.
    """
    merge_times_by_month = defaultdict(list)

    for item in items:
        if not item["is_pr"]:
            continue
        cd = parse_date(item["created_at"])
        md = parse_date(item["merged_at"])
        if not cd or not md:
            continue
        days = (md - cd).days
        if days < 0:
            continue
        # Bin by merge month
        month_key = md.replace(day=1)
        merge_times_by_month[month_key].append(days)

    if not merge_times_by_month:
        return [], []

    months = sorted(merge_times_by_month.keys())
    p75s = []
    for m in months:
        vals = sorted(merge_times_by_month[m])
        idx = int(len(vals) * 0.75)
        p75s.append(vals[min(idx, len(vals) - 1)])
    return months, p75s


def compute_monthly_maintainer_stats(items):
    """
    Compute monthly maintainer stats with a 2-month rolling window.

    Returns (months, active_maintainers, prs_per_maintainer, pr_count) lists.
    "Active maintainer" = distinct person who merged >=1 PR in the month or prior month.
    """
    # Collect mergers per month
    mergers_by_month = defaultdict(set)
    merges_by_month = defaultdict(int)

    for item in items:
        if not item["is_pr"]:
            continue
        md = parse_date(item["merged_at"])
        merger = item.get("merged_by")
        if not md or not merger:
            continue
        if merger in BOT_ACCOUNTS:
            continue
        month_key = md.replace(day=1)
        mergers_by_month[month_key].add(merger)
        merges_by_month[month_key] += 1

    if not mergers_by_month:
        return [], [], [], []

    months = sorted(mergers_by_month.keys())
    active_maintainers = []
    prs_per_maintainer = []
    pr_counts = []

    for i, m in enumerate(months):
        # 2-month rolling window: this month + prior month
        window_mergers = set(mergers_by_month[m])
        if i > 0:
            prev = months[i - 1]
            # Only include if exactly 1 calendar month apart
            prev_month_diff = (m.year - prev.year) * 12 + (m.month - prev.month)
            if prev_month_diff == 1:
                window_mergers |= mergers_by_month[prev]

        n_maintainers = len(window_mergers)
        n_prs = merges_by_month[m]
        active_maintainers.append(n_maintainers)
        prs_per_maintainer.append(n_prs / n_maintainers if n_maintainers > 0 else 0)
        pr_counts.append(n_prs)

    return months, active_maintainers, prs_per_maintainer, pr_counts


def smooth(data, window=4):
    """Simple moving average smoother."""
    if len(data) < window:
        return data
    smoothed = []
    for i in range(len(data)):
        start = max(0, i - window + 1)
        smoothed.append(sum(data[start:i+1]) / (i - start + 1))
    return smoothed


def robust_ylim(data_series_list, padding=1.3, symmetric=False, percentile=0.95):
    """Compute a y-axis limit that clips outlier spikes.
    
    data_series_list: list of lists of numeric values
    percentile: which percentile to use for clamping (default 0.95)
    Returns (ymin, ymax) tuple.
    
    For symmetric mode, also computes a per-series p95 and uses the second-highest
    series to set the limit, preventing one extreme repo from distorting the axis.
    """
    all_vals = []
    for series in data_series_list:
        all_vals.extend(v for v in series if v is not None)
    if not all_vals:
        return (0, None)
    all_vals.sort()
    pval = all_vals[int(len(all_vals) * percentile)]
    ymax = pval * padding
    if symmetric:
        neg_vals = [v for v in all_vals if v < 0]
        if neg_vals:
            p_low = neg_vals[max(0, int(len(neg_vals) * (1 - percentile)))]
            ymin = p_low * padding
        else:
            ymin = -ymax
        return (ymin, ymax)
    return (0, max(ymax, 1))


def thousands_formatter(x, pos):
    if abs(x) >= 1000:
        val = x / 1000
        # Use 1 decimal if it would disambiguate (e.g. 3.5K vs 4K)
        if val != int(val):
            return f"{val:.1f}K"
        return f"{val:.0f}K"
    return f"{x:.0f}"


def label_line_ends(ax, lines_info):
    """Add repo name labels near the right end of each plotted line.
    
    lines_info: list of (dates, values, repo_name, color) tuples.
    Adjusts vertical positions to avoid overlapping labels.
    """
    if not lines_info:
        return

    # Collect end points
    endpoints = []
    for dates, values, name, color in lines_info:
        if not dates or not values:
            continue
        # Use the last non-None value
        for i in range(len(values) - 1, -1, -1):
            if values[i] is not None:
                endpoints.append((dates[i], values[i], name, color))
                break

    if not endpoints:
        return

    # Sort by y-value to help space labels
    endpoints.sort(key=lambda e: e[1])

    # Get axis limits for spacing calculation
    ylim = ax.get_ylim()
    y_range = ylim[1] - ylim[0] if ylim[1] and ylim[0] is not None else 1
    min_gap = y_range * 0.03  # minimum 3% of y-range between labels

    # Nudge overlapping labels apart
    adjusted_y = [e[1] for e in endpoints]
    for i in range(1, len(adjusted_y)):
        if adjusted_y[i] - adjusted_y[i - 1] < min_gap:
            adjusted_y[i] = adjusted_y[i - 1] + min_gap

    for (x, orig_y, name, color), adj_y in zip(endpoints, adjusted_y):
        ax.annotate(f" {name}", xy=(x, orig_y), xytext=(x, adj_y),
                    fontsize=8, color=color, fontweight="bold",
                    va="center", ha="left",
                    annotation_clip=False)


def setup_axes(ax, title, ylabel):
    ax.set_title(title, fontsize=13, fontweight="bold", pad=10)
    ax.set_ylabel(ylabel, fontsize=10)
    # Major ticks on Jan 1 (grid lines), minor ticks at mid-year (labels)
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter(""))  # no label on Jan 1 ticks
    ax.xaxis.set_minor_locator(mdates.YearLocator(month=7))  # mid-year
    ax.xaxis.set_minor_formatter(mdates.DateFormatter("%Y"))
    ax.tick_params(axis="x", which="minor", length=0)  # no tick mark for labels
    ax.yaxis.set_major_formatter(FuncFormatter(thousands_formatter))
    ax.grid(True, alpha=0.3, which="major")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


def add_insight_box(ax, lines, loc="lower right"):
    """Add a small text box with observation bullets to the chart.
    lines: list of short strings. loc: 'lower right', 'upper right', 'lower left', 'upper left'."""
    text = "\n".join(f"• {l}" for l in lines)
    x = {"lower right": 0.98, "upper right": 0.98, "lower left": 0.02, "upper left": 0.02}[loc]
    y = {"lower right": 0.03, "upper right": 0.97, "lower left": 0.03, "upper left": 0.97}[loc]
    ha = "right" if "right" in loc else "left"
    va = "bottom" if "lower" in loc else "top"
    ax.text(x, y, text, transform=ax.transAxes, fontsize=7.5,
            va=va, ha=ha, family="sans-serif",
            bbox=dict(boxstyle="round,pad=0.4", facecolor="white", edgecolor="#cccccc",
                      alpha=0.9))


def series_pct_change(dates, values, years_back=2):
    """Compute % change in a series over the last N years using averages of the
    first and last quarter to reduce noise. Returns (pct_change, start_year) or None."""
    if not dates or not values or len(dates) < 52:
        return None
    from datetime import date as dt
    end = dates[-1]
    if isinstance(end, datetime):
        end = end.date()
    start_target = dt(end.year - years_back, end.month, end.day)
    # Find index closest to start
    best_i = 0
    for i, d in enumerate(dates):
        dd = d.date() if isinstance(d, datetime) else d
        if dd <= start_target:
            best_i = i
    if best_i >= len(dates) - 13:
        return None
    # Average over ~3 month windows at start and end
    window = min(13, (len(dates) - best_i) // 4)
    if window < 4:
        return None
    old_avg = sum(values[best_i:best_i + window]) / window
    new_avg = sum(values[-window:]) / window
    if old_avg == 0:
        return None
    pct = 100.0 * (new_avg - old_avg) / old_avg
    return pct, end.year - years_back


def series_latest_avg(values, window=13):
    """Average of the last `window` values."""
    if not values or len(values) < window:
        return None
    return sum(values[-window:]) / window


def _add_yearly_net_bars(ax, weeks, inflow, outflow):
    """Add semi-transparent yearly net bars (inflow - outflow) to an axes."""
    from datetime import date as date_type
    yearly_net = defaultdict(float)
    for w, i, o in zip(weeks, inflow, outflow):
        yearly_net[w.year] += (i - o)
    if not yearly_net:
        return
    years = sorted(yearly_net.keys())
    # Skip partial first/last years
    if len(years) > 2:
        years = years[1:-1]
    centers = [date_type(y, 7, 1) for y in years]
    nets = [yearly_net[y] / 52 for y in years]  # normalize to per-week average
    bar_colors = ["#3498DB" if n >= 0 else "#E67E22" for n in nets]
    ax.bar(centers, nets, width=300, alpha=0.25, color=bar_colors,
           label="Yearly net (avg/wk)", zorder=1)


def chart_open_issues_comparison(all_series, output_dir):
    """Open issues over time, all repos overlaid. Y-axis clamped to p95."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Open Issues Over Time", "Open Issues")

    visible_data = []
    line_ends = []
    for repo, series in all_series.items():
        if not series:
            continue
        s = smooth(series["open_issues"], window=4)
        ax.plot(series["weeks"], s,
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(s)
        line_ends.append((series["weeks"], s, get_short(repo), get_color(repo)))

    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(ymin, ymax)
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    # Insights
    insights = []
    for repo, series in all_series.items():
        if not series:
            continue
        s = smooth(series["open_issues"], window=4)
        r = series_pct_change(series["weeks"], s, years_back=3)
        if r:
            direction = "up" if r[0] > 0 else "down"
            insights.append((abs(r[0]), f"{get_short(repo)}: {direction} {abs(r[0]):.0f}% since {r[1]}"))
    insights.sort(reverse=True)
    if insights:
        lines = [i[1] for i in insights[:4]]
        add_insight_box(ax, lines, loc="lower right")
    fig.tight_layout()
    path = os.path.join(output_dir, "open_issues_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_open_prs_comparison(all_series, output_dir):
    """Open PRs over time, all repos overlaid. Excludes Gerrit repos."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Open Pull Requests Over Time", "Open PRs")

    visible_data = []
    line_ends = []
    for repo, series in all_series.items():
        if not series:
            continue
        s = smooth(series["open_prs"], window=13)
        ax.plot(series["weeks"], s,
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(s)
        line_ends.append((series["weeks"], s, get_short(repo), get_color(repo)))

    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(ymin, ymax)
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    # Insights
    insights = []
    for repo, series in all_series.items():
        if not series:
            continue
        s = smooth(series["open_prs"], window=13)
        r = series_pct_change(series["weeks"], s, years_back=3)
        if r:
            direction = "up" if r[0] > 0 else "down"
            insights.append((abs(r[0]), f"{get_short(repo)}: {direction} {abs(r[0]):.0f}% since {r[1]}"))
    insights.sort(reverse=True)
    if insights:
        lines = [i[1] for i in insights[:3]]
        lines.append("Open PR backlogs growing across all major repos")
        add_insight_box(ax, lines, loc="lower right")
    fig.tight_layout()
    path = os.path.join(output_dir, "open_prs_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_net_flow_comparison(all_series, output_dir):
    """Net issue flow (opened - closed per week), smoothed, Y-axis clamped."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Net Issue Flow (Opened - Closed per Week, 26-week avg)",
               "Net Issues / Week")

    ax.axhline(y=0, color="black", linewidth=0.5, alpha=0.5)

    visible_data = []
    line_ends = []
    for repo, series in all_series.items():
        if not series:
            continue
        smoothed = smooth(series["net_issue_flow"], window=26)
        ax.plot(series["weeks"], smoothed,
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(smoothed)
        line_ends.append((series["weeks"], smoothed, get_short(repo), get_color(repo)))

    # Fixed symmetric range — shows the interesting variation without extreme spikes
    ax.set_ylim(-100, 100)
    ax.annotate("Y-axis clamped to [-100, +100] to exclude bulk closure spikes",
                xy=(0.02, 0.02), xycoords="axes fraction", fontsize=8,
                color="#888888", style="italic")
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    # Insights: who's currently above/below zero
    above = []
    below = []
    for repo, series in all_series.items():
        if not series:
            continue
        s = smooth(series["net_issue_flow"], window=26)
        avg = series_latest_avg(s, window=13)
        if avg is not None:
            (above if avg > 0 else below).append((avg, get_short(repo)))
    lines = []
    if above:
        names = ", ".join(n for _, n in sorted(above, reverse=True))
        lines.append(f"Currently accumulating: {names}")
    if below:
        names = ", ".join(n for _, n in sorted(below))
        lines.append(f"Currently reducing backlog: {names}")
    if lines:
        add_insight_box(ax, lines, loc="upper right")
    fig.tight_layout()
    path = os.path.join(output_dir, "net_issue_flow_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_pr_merge_rate_comparison(all_series, output_dir):
    """PR merge rate (merged per week), smoothed."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "PR Merge Rate (Merged per Week, 26-week avg)",
               "PRs Merged / Week")

    visible_data = []
    line_ends = []
    for repo, series in all_series.items():
        if not series:
            continue
        smoothed = smooth(series["pr_merged"], window=26)
        ax.plot(series["weeks"], smoothed,
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(smoothed)
        line_ends.append((series["weeks"], smoothed, get_short(repo), get_color(repo)))

    ymin, ymax = robust_ylim(visible_data, percentile=0.99)
    ax.set_ylim(ymin, max(ymax, 300))
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    # Insights
    lines = []
    for repo, series in all_series.items():
        if not series:
            continue
        s = smooth(series["pr_merged"], window=26)
        r = series_pct_change(series["weeks"], s, years_back=3)
        if r and abs(r[0]) > 30:
            direction = "up" if r[0] > 0 else "down"
            lines.append(f"{get_short(repo)}: {direction} {abs(r[0]):.0f}% since {r[1]}")
    if lines:
        add_insight_box(ax, lines[:4], loc="upper right")
    fig.tight_layout()
    path = os.path.join(output_dir, "pr_merge_rate_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_per_repo_dashboard(repo, series, output_dir):
    """4-panel dashboard for a single repo."""
    if not series:
        return

    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle(f"{repo} — Health Dashboard", fontsize=15, fontweight="bold", y=0.98)
    color = get_color(repo)
    weeks = series["weeks"]

    # Panel 1: Open issues
    ax = axes[0, 0]
    setup_axes(ax, "Open Issues", "Count")
    ax.plot(weeks, series["open_issues"], color=color, linewidth=1.5)
    ax.fill_between(weeks, series["open_issues"], alpha=0.15, color=color)

    # Panel 2: Open PRs
    ax = axes[0, 1]
    setup_axes(ax, "Open PRs", "Count")
    open_prs_smooth = smooth(series["open_prs"], window=26)
    ax.plot(weeks, open_prs_smooth, color=color, linewidth=1.5)
    ax.fill_between(weeks, open_prs_smooth, alpha=0.15, color=color)
    if repo in GERRIT_REPOS:
        ax.annotate("PR merge inferred from close date (Gerrit workflow)",
                    xy=(0.02, 0.02), xycoords="axes fraction", fontsize=7,
                    color="#888888", style="italic")

    # Panel 3: Issue inflow vs outflow (smoothed, clamped) + yearly net bars
    ax = axes[1, 0]
    setup_axes(ax, "Issues: Opened vs Closed (26-week avg)", "Per Week")
    opened_smooth = smooth(series["issue_opened"], window=26)
    closed_smooth = smooth(series["issue_closed"], window=26)
    ax.plot(weeks, opened_smooth, color="#E74C3C",
            label="Opened", linewidth=1.2, alpha=0.8)
    ax.plot(weeks, closed_smooth, color="#27AE60",
            label="Closed", linewidth=1.2, alpha=0.8)
    # Yearly net bars
    _add_yearly_net_bars(ax, weeks, series["issue_opened"], series["issue_closed"])
    # Clamp to p95 to exclude mass-closure spikes
    all_vals = opened_smooth + closed_smooth
    if all_vals:
        p95 = sorted(all_vals)[int(len(all_vals) * 0.95)]
        ax.set_ylim(-p95 * 0.4, p95 * 1.5)
    ax.legend(fontsize=9)

    # Panel 4: PRs opened vs merged (smoothed, clamped) + yearly net bars
    ax = axes[1, 1]
    setup_axes(ax, "PRs: Opened vs Merged (26-week avg)", "Per Week")
    pr_open_smooth = smooth(series["pr_opened"], window=26)
    pr_merge_smooth = smooth(series["pr_merged"], window=26)
    ax.plot(weeks, pr_open_smooth, color="#E74C3C",
            label="Opened", linewidth=1.2, alpha=0.8)
    ax.plot(weeks, pr_merge_smooth, color="#27AE60",
            label="Merged", linewidth=1.2, alpha=0.8)
    _add_yearly_net_bars(ax, weeks, series["pr_opened"], series["pr_merged"])
    all_vals = pr_open_smooth + pr_merge_smooth
    if all_vals:
        p95 = sorted(all_vals)[int(len(all_vals) * 0.95)]
        ax.set_ylim(-p95 * 0.4, p95 * 1.5)
    ax.legend(fontsize=9)

    fig.tight_layout(rect=[0, 0, 1, 0.96])
    safe_name = repo.replace("/", "_")
    path = os.path.join(output_dir, f"dashboard_{safe_name}.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_sustainability_score(all_series, output_dir):
    """
    Rolling close ratio: issues closed / issues opened over a trailing window.
    Above 100% = working down backlog. Below 100% = falling behind.
    """
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Issue Close Ratio (Closed / Opened, Trailing 12-Month Window)",
               "Close Ratio")

    WINDOW = 52  # 52 weeks = ~12 months

    ax.axhline(y=100, color="black", linewidth=1, alpha=0.4, linestyle="--")

    line_ends = []
    for repo, series in all_series.items():
        if not series:
            continue
        weeks = series["weeks"]
        opened = series["issue_opened"]
        closed = series["issue_closed"]

        ratios = []
        for i in range(len(weeks)):
            start = max(0, i - WINDOW + 1)
            win_opened = sum(opened[start:i + 1])
            win_closed = sum(closed[start:i + 1])
            if win_opened > 20:  # need enough data for meaningful ratio
                ratios.append(100.0 * win_closed / win_opened)
            else:
                ratios.append(None)

        # Filter to non-None for plotting
        valid = [(w, r) for w, r in zip(weeks, ratios) if r is not None]
        if not valid:
            continue
        vw, vr = zip(*valid)
        smoothed = smooth(list(vr), 8)
        ax.plot(list(vw), smoothed,
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        line_ends.append((list(vw), smoothed, get_short(repo), get_color(repo)))

    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}%"))
    # Clamp y-axis but ensure 100% line is visible
    ax.set_ylim(40, 180)
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    # Place labels just above/below the 100% reference line in data coords
    xlim = ax.get_xlim()
    x_pos = mdates.num2date(xlim[0] + (xlim[1] - xlim[0]) * 0.02)
    ax.text(x_pos, 103, "▲ shrinking backlog", fontsize=9,
            color="#888888", style="italic", va="bottom")
    ax.text(x_pos, 97, "▼ growing backlog", fontsize=9,
            color="#888888", style="italic", va="top")
    # Insights: current close ratio for each repo
    ratios_now = []
    for repo, series in all_series.items():
        if not series:
            continue
        s = smooth(series["issue_closed"], window=52)
        o = smooth(series["issue_opened"], window=52)
        recent_closed = sum(s[-13:]) if len(s) >= 13 else None
        recent_opened = sum(o[-13:]) if len(o) >= 13 else None
        if recent_opened and recent_opened > 0:
            ratio = 100 * recent_closed / recent_opened
            ratios_now.append((ratio, get_short(repo)))
    if ratios_now:
        ratios_now.sort(reverse=True)
        lines = [f"{n}: {r:.0f}%" for r, n in ratios_now]
        lines.insert(0, "Current 12-month close ratio:")
        add_insight_box(ax, lines, loc="lower left")
    fig.tight_layout()
    path = os.path.join(output_dir, "sustainability_score.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_time_to_merge(all_ttm, output_dir):
    """Median time-to-merge (days) per month, all repos. Excludes Gerrit repos."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Time to Merge PRs — 75th Percentile (Monthly, 4-month avg)", "Days")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}"))

    visible_data = []
    line_ends = []
    for repo, (months, medians) in all_ttm.items():
        if not months or repo in GERRIT_REPOS:
            continue
        smoothed = smooth(medians, window=4)
        ax.plot(months, smoothed,
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(smoothed)
        line_ends.append((months, smoothed, get_short(repo), get_color(repo)))

    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(ymin, ymax)
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    # Insights: current p75 TTM for each repo
    ttm_now = []
    for repo, (months, medians) in all_ttm.items():
        if not months or repo in GERRIT_REPOS:
            continue
        recent = medians[-3:] if len(medians) >= 3 else medians
        if recent:
            ttm_now.append((sum(recent) / len(recent), get_short(repo)))
    if ttm_now:
        ttm_now.sort()
        lines = [f"{n}: {d:.0f} days" for d, n in ttm_now]
        lines.insert(0, "Current p75 time to merge:")
        add_insight_box(ax, lines, loc="upper right")
    fig.tight_layout()
    path = os.path.join(output_dir, "time_to_merge_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_active_maintainers(all_maint, output_dir):
    """Active maintainers per month (2-month rolling window). Excludes Gerrit and bot-merger repos."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Active Maintainers (Distinct Mergers, 2-Month Rolling Window)",
               "People")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}"))

    excluded = GERRIT_REPOS | BOT_MERGER_REPOS
    visible_data = []
    line_ends = []
    for repo, (months, maintainers, _, _) in all_maint.items():
        if not months or repo in excluded:
            continue
        s = smooth(maintainers, 3)
        ax.plot(months, s, color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(s)
        line_ends.append((months, s, get_short(repo), get_color(repo)))

    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(ymin, ymax)
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    # Insights: % change in maintainers
    insights = []
    for repo, (months, maintainers, _, _) in all_maint.items():
        if not months or repo in excluded:
            continue
        s = smooth(maintainers, 3)
        r = series_pct_change(months, s, years_back=2)
        if r:
            direction = "up" if r[0] > 0 else "down"
            insights.append((r[0], f"{get_short(repo)}: {direction} {abs(r[0]):.0f}% since {r[1]}"))
    insights.sort(key=lambda x: x[0])  # most declining first
    if insights:
        lines = [i[1] for i in insights[:4]]
        add_insight_box(ax, lines, loc="upper right")
    fig.tight_layout()
    path = os.path.join(output_dir, "active_maintainers_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_prs_per_maintainer(all_maint, output_dir):
    """PRs merged per active maintainer per month. Excludes Gerrit and bot-merger repos."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "PRs Merged per Active Maintainer (Monthly, 3-month avg)",
               "PRs / Maintainer / Month")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}"))

    excluded = GERRIT_REPOS | BOT_MERGER_REPOS
    visible_data = []
    line_ends = []
    for repo, (months, _, prs_per, _) in all_maint.items():
        if not months or repo in excluded:
            continue
        s = smooth(prs_per, 3)
        ax.plot(months, s, color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(s)
        line_ends.append((months, s, get_short(repo), get_color(repo)))

    ymin, ymax = robust_ylim(visible_data, percentile=0.99)
    ax.set_ylim(ymin, ymax)
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    # Insights: current PRs/maintainer for each repo
    rates = []
    for repo, (months, _, prs_per, _) in all_maint.items():
        if not months or repo in excluded:
            continue
        recent = prs_per[-3:] if len(prs_per) >= 3 else prs_per
        if recent:
            rates.append((sum(recent) / len(recent), get_short(repo)))
    if rates:
        rates.sort(reverse=True)
        lines = [f"{n}: {r:.0f} PRs/person/mo" for r, n in rates]
        lines.insert(0, "Current workload per maintainer:")
        if rates[0][0] > rates[-1][0] * 1.5:
            lines.append(f"{rates[0][1]} has {rates[0][0]/rates[-1][0]:.1f}x the load of {rates[-1][1]}")
        add_insight_box(ax, lines, loc="upper right")
    fig.tight_layout()
    path = os.path.join(output_dir, "prs_per_maintainer_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def _interpolate_maintainers_to_weeks(weeks, maint_months, maint_counts):
    """Map monthly maintainer counts to weekly dates via nearest-month lookup."""
    if not maint_months or not weeks:
        return None
    month_map = {m: c for m, c in zip(maint_months, maint_counts)}
    result = []
    for w in weeks:
        # Find nearest month
        wm = w.replace(day=1)
        count = month_map.get(wm)
        if count is None:
            # Try prior month
            if wm.month == 1:
                prev = wm.replace(year=wm.year - 1, month=12)
            else:
                prev = wm.replace(month=wm.month - 1)
            count = month_map.get(prev, 0)
        result.append(count)
    return result


def chart_open_issues_per_maintainer(all_series, all_maint, output_dir):
    """Open issues divided by active maintainers — shows burden per person."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Open Issues per Active Maintainer", "Issues / Maintainer")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}"))

    excluded = GERRIT_REPOS | BOT_MERGER_REPOS
    visible_data = []
    line_ends = []
    for repo, series in all_series.items():
        if not series or repo in excluded:
            continue
        maint_data = all_maint.get(repo)
        if not maint_data or not maint_data[0]:
            continue
        months, maintainers, _, _ = maint_data
        weekly_maint = _interpolate_maintainers_to_weeks(series["weeks"], months, maintainers)
        if not weekly_maint:
            continue
        ratio = [oi / max(m, 1) for oi, m in zip(series["open_issues"], weekly_maint)]
        s = smooth(ratio, window=13)
        ax.plot(series["weeks"], s, color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(s)
        line_ends.append((series["weeks"], s, get_short(repo), get_color(repo)))

    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(ymin, ymax)
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    # Insights
    insights = []
    for dates, vals, name, color in line_ends:
        r = series_pct_change(dates, vals, years_back=2)
        if r:
            direction = "up" if r[0] > 0 else "down"
            insights.append((r[0], f"{name}: {direction} {abs(r[0]):.0f}% since {r[1]}"))
    insights.sort(reverse=True)
    if insights:
        lines = [i[1] for i in insights[:4]]
        lines.insert(0, "Growing = each maintainer responsible for more issues")
        add_insight_box(ax, lines, loc="lower right")
    fig.tight_layout()
    path = os.path.join(output_dir, "open_issues_per_maintainer.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_open_prs_per_maintainer(all_series, all_maint, output_dir):
    """Open PRs divided by active maintainers — shows review burden per person."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Open PRs per Active Maintainer", "PRs / Maintainer")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}"))

    excluded = GERRIT_REPOS | BOT_MERGER_REPOS
    visible_data = []
    line_ends = []
    for repo, series in all_series.items():
        if not series or repo in excluded:
            continue
        maint_data = all_maint.get(repo)
        if not maint_data or not maint_data[0]:
            continue
        months, maintainers, _, _ = maint_data
        weekly_maint = _interpolate_maintainers_to_weeks(series["weeks"], months, maintainers)
        if not weekly_maint:
            continue
        ratio = [op / max(m, 1) for op, m in zip(series["open_prs"], weekly_maint)]
        s = smooth(ratio, window=13)
        ax.plot(series["weeks"], s, color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(s)
        line_ends.append((series["weeks"], s, get_short(repo), get_color(repo)))

    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(ymin, ymax)
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    # Insights
    insights = []
    for dates, vals, name, color in line_ends:
        r = series_pct_change(dates, vals, years_back=2)
        if r:
            direction = "up" if r[0] > 0 else "down"
            insights.append((r[0], f"{name}: {direction} {abs(r[0]):.0f}% since {r[1]}"))
    insights.sort(reverse=True)
    if insights:
        lines = [i[1] for i in insights[:4]]
        lines.insert(0, "Growing = each maintainer has more PRs to review")
        add_insight_box(ax, lines, loc="lower right")
    fig.tight_layout()
    path = os.path.join(output_dir, "open_prs_per_maintainer.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_contributor_diversity(all_items, output_dir):
    """Distinct PR authors per month (2-month rolling window) — measures community breadth."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Active Community (Distinct PR Authors, 2-Month Rolling Window)",
               "Unique Authors")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}"))

    visible_data = []
    line_ends = []
    for repo, items in all_items.items():
        authors_by_month = defaultdict(set)
        for item in items:
            if not item["is_pr"]:
                continue
            cd = parse_date(item["created_at"])
            author = effective_author(item)
            if not cd or not author:
                continue
            authors_by_month[cd.replace(day=1)].add(author)

        if not authors_by_month:
            continue
        months = sorted(authors_by_month.keys())
        # 2-month rolling window (same as maintainer chart)
        counts = []
        for i, m in enumerate(months):
            window_authors = set(authors_by_month[m])
            if i > 0:
                prev = months[i - 1]
                prev_diff = (m.year - prev.year) * 12 + (m.month - prev.month)
                if prev_diff == 1:
                    window_authors |= authors_by_month[prev]
            counts.append(len(window_authors))

        s = smooth(counts, 6)
        ax.plot(months, s,
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(s)
        line_ends.append((months, s, get_short(repo), get_color(repo)))

    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(ymin, ymax)
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    # Insights: % change in community size
    insights = []
    for repo, items in all_items.items():
        # Recompute for insight (reuse line_ends data)
        pass
    # Use line_ends data directly for % change
    for dates, vals, name, color in line_ends:
        r = series_pct_change(dates, vals, years_back=2)
        if r:
            direction = "up" if r[0] > 0 else "down"
            insights.append((r[0], f"{name}: {direction} {abs(r[0]):.0f}% since {r[1]}"))
    insights.sort(key=lambda x: x[0])  # most declining first
    if insights:
        lines = [i[1] for i in insights[:4]]
        lines.append("Copilot PRs attributed to their human requester")
        add_insight_box(ax, lines, loc="upper right")
    fig.tight_layout()
    path = os.path.join(output_dir, "contributor_diversity_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_issue_close_rate(all_series, output_dir):
    """Percentage of issues closed within 30 days of opening, by month."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Issue Responsiveness (% Closed Within 30 Days, 6-month avg)",
               "% Closed <30d")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}%"))

    # For repos with lineage, responsiveness data before the merge is unreliable
    # because transferred issues lost their original closed_at dates
    from datetime import date as date_type
    LINEAGE_CUTOFF = {repo: date_type(2020, 1, 1) for repo in REPO_LINEAGE}

    line_ends = []
    for repo, items in all_series.items():
        cutoff = LINEAGE_CUTOFF.get(repo)
        monthly_total = defaultdict(int)
        monthly_fast = defaultdict(int)
        for item in items:
            if item["is_pr"]:
                continue
            cd = parse_date(item["created_at"])
            cld = parse_date(item["closed_at"])
            if not cd:
                continue
            if cutoff and cd < cutoff:
                continue
            month = cd.replace(day=1)
            monthly_total[month] += 1
            if cld and (cld - cd).days <= 30:
                monthly_fast[month] += 1

        if not monthly_total:
            continue
        months = sorted(monthly_total.keys())
        pcts = [100.0 * monthly_fast.get(m, 0) / monthly_total[m]
                if monthly_total[m] > 10 else None for m in months]
        # Filter out None months (too few issues)
        valid = [(m, p) for m, p in zip(months, pcts) if p is not None]
        if not valid:
            continue
        vm, vp = zip(*valid)
        smoothed = smooth(list(vp), 6)
        ax.plot(list(vm), smoothed,
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        line_ends.append((list(vm), smoothed, get_short(repo), get_color(repo)))

    ax.set_ylim(0, 100)
    ax.legend(loc="upper right", fontsize=10)
    label_line_ends(ax, line_ends)
    # Insights: current responsiveness
    resp_now = []
    for dates, vals, name, color in line_ends:
        avg = series_latest_avg(vals, window=6)
        if avg is not None:
            resp_now.append((avg, name))
    if resp_now:
        resp_now.sort(reverse=True)
        lines = [f"{n}: {r:.0f}%" for r, n in resp_now]
        lines.insert(0, "Current % closed within 30 days:")
        add_insight_box(ax, lines, loc="lower left")
    if LINEAGE_CUTOFF:
        cutoff_names = ", ".join(get_short(r) for r in LINEAGE_CUTOFF)
        ax.annotate(f"Note: {cutoff_names} shown from 2020 (pre-merge close dates unreliable)",
                    xy=(0.02, 0.02), xycoords="axes fraction", fontsize=8,
                    color="#888888", style="italic")
    fig.tight_layout()
    path = os.path.join(output_dir, "issue_responsiveness_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Generate repo health charts")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite database path")
    parser.add_argument("--repos", nargs="*", help="Repos to analyze (default: all in DB)")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output directory for charts")
    args = parser.parse_args()

    db_path = str(Path(args.db).resolve())
    if not os.path.exists(db_path):
        print(f"ERROR: Database not found: {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)

    # Discover repos in DB
    if args.repos:
        repos = args.repos
    else:
        repos = [r[0] for r in conn.execute(
            "SELECT DISTINCT repo FROM items ORDER BY repo"
        ).fetchall()]
        # Exclude legacy predecessor repos — their data is merged into successor lines
        repos = [r for r in repos if r not in LEGACY_REPOS]

    if not repos:
        print("ERROR: No data in database. Run fetch.py first.")
        sys.exit(1)

    # Stats
    print(f"Database: {db_path}")
    print(f"Repos: {', '.join(repos)}")
    for repo in repos:
        counts = conn.execute(
            "SELECT is_pull_request, COUNT(*) FROM items WHERE repo = ? GROUP BY is_pull_request",
            (repo,)
        ).fetchall()
        count_map = dict(counts)
        lineage = REPO_LINEAGE.get(repo, [])
        suffix = ""
        if lineage:
            extra_prs = sum(
                conn.execute("SELECT COUNT(*) FROM items WHERE repo = ? AND is_pull_request = 1",
                             (p,)).fetchone()[0]
                for p in lineage
            )
            suffix = f" (+{extra_prs:,} PRs from {', '.join(lineage)})"
        print(f"  {repo}: {count_map.get(0, 0):,} issues, {count_map.get(1, 0):,} PRs{suffix}")
    print()

    # Compute series
    print("Computing weekly time series...")
    all_series = {}
    all_items = {}
    for repo in repos:
        print(f"  {repo}...")
        items = load_items(conn, repo)
        all_items[repo] = items
        all_series[repo] = compute_weekly_series(items)

    print("Computing time-to-merge...")
    all_ttm = {}
    for repo in repos:
        all_ttm[repo] = compute_monthly_time_to_merge(all_items[repo])

    print("Computing maintainer stats...")
    all_maint = {}
    has_maintainer_data = False
    for repo in repos:
        all_maint[repo] = compute_monthly_maintainer_stats(all_items[repo])
        if all_maint[repo][0]:  # has months
            has_maintainer_data = True
    print()

    # Generate charts
    output_dir = args.output
    os.makedirs(output_dir, exist_ok=True)

    print("Generating charts...")

    # Cross-repo comparison charts
    if len(repos) > 1:
        chart_open_issues_comparison(all_series, output_dir)
        chart_open_prs_comparison(all_series, output_dir)
        chart_net_flow_comparison(all_series, output_dir)
        chart_pr_merge_rate_comparison(all_series, output_dir)
        chart_sustainability_score(all_series, output_dir)
        chart_time_to_merge(all_ttm, output_dir)
        chart_issue_close_rate(all_items, output_dir)
        if has_maintainer_data:
            chart_active_maintainers(all_maint, output_dir)
            chart_prs_per_maintainer(all_maint, output_dir)
            chart_open_issues_per_maintainer(all_series, all_maint, output_dir)
            chart_open_prs_per_maintainer(all_series, all_maint, output_dir)
            chart_contributor_diversity(all_items, output_dir)
        else:
            print("  (skipping maintainer charts — no author/merged_by data yet)")

    # Per-repo dashboards
    for repo in repos:
        chart_per_repo_dashboard(repo, all_series.get(repo), output_dir)

    conn.close()
    print(f"\nDone! Charts saved to {os.path.abspath(output_dir)}/")


if __name__ == "__main__":
    main()
