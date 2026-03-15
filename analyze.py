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
    plt.rcParams['mathtext.fontset'] = 'dejavusans'
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
    "dotnet/aspire": "#4CAF50",    # green
    "microsoft/vscode": "#007ACC", # VS Code blue
    "rust-lang/rust": "#B7410E",   # rust red-brown
    "golang/go": "#00897B",        # teal
}

REPO_SHORT = {
    "dotnet/runtime": "runtime",
    "dotnet/roslyn": "roslyn",
    "dotnet/maui": "maui",
    "dotnet/aspire": "aspire",
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
    """Gaussian-weighted trailing moving average.
    Uses a half-Gaussian kernel so recent points are weighted most heavily
    and there's no step artifact when points enter/leave the window."""
    import math
    if len(data) < window:
        return data
    # Precompute Gaussian weights (sigma = window/3 gives ~99.7% within window)
    sigma = window / 3.0
    weights = [math.exp(-0.5 * (d / sigma) ** 2) for d in range(window)]
    smoothed = []
    for i in range(len(data)):
        start = max(0, i - window + 1)
        span = data[start:i + 1]
        w = weights[:len(span)][::-1]  # most recent gets weights[0]=1.0
        smoothed.append(sum(v * wt for v, wt in zip(span, w)) / sum(w))
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
        # Clamp label position to within axis limits so tight_layout isn't distorted
        clamped_y = max(ylim[0], min(adj_y, ylim[1]))
        ax.annotate(f" {name}", xy=(x, min(orig_y, ylim[1])), xytext=(x, clamped_y),
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


def add_insight_box(ax, lines, loc="upper center"):
    """Add a small text box with observation bullets to the chart.
    loc: 'upper center' (below title), 'lower left', 'lower right', 'upper left', 'upper right'.
    Repo short names are auto-bolded via mathtext."""
    import re
    # Auto-bold known repo short names (sorted longest-first to avoid partial matches)
    bold_names = sorted(set(REPO_SHORT.values()), key=len, reverse=True)
    text = "\n".join(f"• {l}" for l in lines)
    for name in bold_names:
        pattern = r'\b' + re.escape(name) + r'\b'
        bold = '$\\mathbf{' + name + '}$'
        text = re.sub(pattern, lambda m, b=bold: b, text)
    positions = {
        "upper center": (0.50, 0.97, "center", "top"),
        "lower left":   (0.02, 0.03, "left",   "bottom"),
        "lower right":  (0.98, 0.03, "right",  "bottom"),
        "upper left":   (0.02, 0.97, "left",   "top"),
        "upper right":  (0.98, 0.97, "right",  "top"),
    }
    x, y, ha, va = positions[loc]
    ax.text(x, y, text, transform=ax.transAxes, fontsize=9.5,
            va=va, ha=ha, multialignment="left", family="sans-serif", zorder=10,
            bbox=dict(boxstyle="round,pad=0.4", facecolor="white", edgecolor="#cccccc",
                      alpha=0.92))


def add_direction_arrow(ax, direction="up", x=0.06):
    """Add a 'Better' arrow in chart whitespace. direction: 'up' or 'down'."""
    if direction == "up":
        xy, xytext = (x, 0.55), (x, 0.35)
    else:
        xy, xytext = (x, 0.35), (x, 0.55)
    ax.annotate("", xy=xy, xytext=xytext, xycoords="axes fraction",
                arrowprops=dict(arrowstyle="-|>,head_width=0.6,head_length=0.4",
                                color="black", lw=3))
    label_y = 0.57 if direction == "up" else 0.30
    label_va = "bottom" if direction == "up" else "top"
    ax.text(x, label_y, "Better", transform=ax.transAxes, fontsize=9,
            ha="center", va=label_va, color="black", style="italic")


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
    setup_axes(ax, "Open Issues Over Time (6-month rolling avg)", "Open Issues")

    visible_data = []
    line_ends = []
    for repo, series in all_series.items():
        if not series:
            continue
        s = smooth(series["open_issues"], window=26)
        ax.plot(series["weeks"], s,
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(s)
        line_ends.append((series["weeks"], s, get_short(repo), get_color(repo)))

    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(ymin, ymax)
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_insight_box(ax, [
        "Issue backlogs grow monotonically across all repos — none has reversed this",
        "vscode triages ~3K issues every December (end-of-year housekeeping)\n  but the upward trend still dominates",
        "go's flat backlog reflects disciplined triage — open/close rates\n  stay balanced, unlike most repos where backlogs grow unchecked",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "open_issues_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_open_prs_comparison(all_series, output_dir):
    """Open PRs over time, all repos overlaid. Excludes Gerrit repos."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Open Pull Requests Over Time (6-month rolling avg)", "Open PRs")

    visible_data = []
    line_ends = []
    for repo, series in all_series.items():
        if not series:
            continue
        s = smooth(series["open_prs"], window=26)
        ax.plot(series["weeks"], s,
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(s)
        line_ends.append((series["weeks"], s, get_short(repo), get_color(repo)))

    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(ymin, ymax)
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_insight_box(ax, [
        "PR backlogs rise over time in every repo — a universal pattern",
        "vscode's 3x jump in 2022 was a workflow change to smaller PRs,\n  not team growth — same ~175 authors making 3x more PRs",
        "rust's high open PR count reflects rigorous review culture\n  — many PRs await RFC review or validation results for weeks",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "open_prs_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_net_flow_comparison(all_series, output_dir):
    """Net issue flow (opened - closed per week), smoothed, Y-axis clamped."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Net Issue Flow (Opened − Closed per Week, 2-year avg)",
               "Net Issues / Week")

    ax.axhline(y=0, color="black", linewidth=0.5, alpha=0.5)

    visible_data = []
    line_ends = []
    for repo, series in all_series.items():
        if not series:
            continue
        # Smooth the raw net flow directly with a wide window.
        # Smoothing opened/closed separately then subtracting stays noisy
        # because correlated weekly spikes don't cancel in the difference.
        smoothed = smooth(series["net_issue_flow"], window=104)
        alpha = 0.4 if repo == "microsoft/vscode" else 0.85
        lw = 1.2 if repo == "microsoft/vscode" else 1.5
        ax.plot(series["weeks"], smoothed,
                color=get_color(repo), label=get_short(repo),
                linewidth=lw, alpha=alpha)
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
        so = smooth(series["issue_opened"], window=26)
        sc = smooth(series["issue_closed"], window=26)
        s = smooth(series["net_issue_flow"], window=104)
        avg = series_latest_avg(s, window=13)
        if avg is not None:
            (above if avg > 0 else below).append((avg, get_short(repo)))
    lines = [
        "All repos oscillate near zero — none losing ground long-term",
        "Dips below zero often precede releases (focused triage sprints)",
        "vscode shows regular December dips — annual housekeeping triage\n  closes thousands of stale issues each year-end",
        "go stays flattest — disciplined triage keeps open/close rates balanced",
    ]
    add_insight_box(ax, lines)
    fig.tight_layout()
    path = os.path.join(output_dir, "net_issue_flow_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_pr_merge_rate_comparison(all_series, output_dir):
    """PR merge rate (merged per week), smoothed."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "PR Merge Rate (Merged per Week, 52-week rolling avg)",
               "PRs Merged / Week")

    visible_data = []
    line_ends = []
    for repo, series in all_series.items():
        if not series:
            continue
        smoothed = smooth(series["pr_merged"], window=52)
        ax.plot(series["weeks"], smoothed,
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(smoothed)
        line_ends.append((series["weeks"], smoothed, get_short(repo), get_color(repo)))

    ymin, ymax = robust_ylim(visible_data, percentile=0.99)
    ax.set_ylim(ymin, max(ymax, 300))
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "up")
    add_insight_box(ax, [
        "dotnet repos dip each Nov — freeze before annual .NET release",
        "runtime merge rate declining since late 2024 — likely driven\n  by ~10% drop in active maintainers over same period",
        "vscode 3x jump mid-2022 was workflow shift to smaller PRs,\n  not a staffing increase (same ~175 authors)",
        "rust's steady ~250/wk powered by bors merge bot — 98% of merges\n  automated, removing human bottleneck from the merge step",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "pr_merge_rate_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def _dashboard_insight(ax, text, loc="upper center"):
    """Small insight annotation for dashboard sub-panels, matching main chart style."""
    positions = {
        "upper center": (0.50, 0.97, "center", "top"),
        "lower left":   (0.02, 0.03, "left",   "bottom"),
        "lower right":  (0.98, 0.03, "right",  "bottom"),
        "upper left":   (0.02, 0.97, "left",   "top"),
        "upper right":  (0.98, 0.97, "right",  "top"),
    }
    x, y, ha, va = positions.get(loc, positions["upper center"])
    ax.text(x, y, f"• {text}", transform=ax.transAxes, fontsize=8,
            va=va, ha=ha, multialignment="left", family="sans-serif",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="#cccccc",
                      alpha=0.92))


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
    oi = series["open_issues"]
    if len(oi) >= 52:
        delta = oi[-1] - oi[-52]
        if delta > 0:
            _dashboard_insight(ax, f"Backlog growing — typical pattern across all repos ({delta:+,}/yr)")
        else:
            _dashboard_insight(ax, f"Backlog shrinking — unusual and positive ({delta:+,}/yr)")

    # Panel 2: Open PRs
    ax = axes[0, 1]
    setup_axes(ax, "Open PRs (6-month avg)", "Count")
    open_prs_smooth = smooth(series["open_prs"], window=26)
    ax.plot(weeks, open_prs_smooth, color=color, linewidth=1.5)
    ax.fill_between(weeks, open_prs_smooth, alpha=0.15, color=color)
    if repo in GERRIT_REPOS:
        ax.annotate("PR merge inferred from close date (Gerrit workflow)",
                    xy=(0.02, 0.02), xycoords="axes fraction", fontsize=7,
                    color="#888888", style="italic")
    elif len(series["open_prs"]) >= 52:
        delta = series["open_prs"][-1] - series["open_prs"][-52]
        if delta > 50:
            _dashboard_insight(ax, f"Review queue growing — may need more reviewers ({delta:+,}/yr)")
        elif delta < -50:
            _dashboard_insight(ax, f"Review queue shrinking — team clearing backlog ({delta:+,}/yr)")
        else:
            _dashboard_insight(ax, "Review queue stable")

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
    if len(series["issue_opened"]) >= 52:
        recent_opened = sum(series["issue_opened"][-52:])
        recent_closed = sum(series["issue_closed"][-52:])
        if recent_opened > 0:
            ratio = recent_closed / recent_opened
            if ratio > 1.05:
                _dashboard_insight(ax, f"Closing faster than opening — actively reducing debt ({ratio:.0%})")
            elif ratio > 0.95:
                _dashboard_insight(ax, f"Roughly keeping pace — typical for mature repos ({ratio:.0%})")
            else:
                _dashboard_insight(ax, f"Opening faster than closing — debt accumulating ({ratio:.0%})")

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
    if len(series["pr_merged"]) >= 104:
        recent = sum(series["pr_merged"][-52:])
        prior = sum(series["pr_merged"][-104:-52])
        if prior > 0:
            change = (recent - prior) / prior
            if change > 0.1:
                _dashboard_insight(ax, f"Merge rate accelerating — team throughput up {change:+.0%} YoY")
            elif change < -0.1:
                _dashboard_insight(ax, f"Merge rate declining — may reflect fewer contributors ({change:+.0%} YoY)")
            else:
                _dashboard_insight(ax, "Merge rate steady — sustainable pace")

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
    add_insight_box(ax, [
        "Ratio >100% means closing more than opening (shrinking backlog)",
        "runtime rose to ~115% in 2025 despite fewer maintainers — driven\n  by falling issue inflow (maturing product) not faster triage",
        "roslyn spikes (~2022-Q4, 2024-Q4, 2025-Q4) are deliberate stale issue\n  housekeeping — bulk-closing old Area-IDE issues (avg age 3-5 years)",
        "Most repos hover near 100% — roughly keeping pace",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "sustainability_score.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_time_to_merge(all_ttm, output_dir):
    """Median time-to-merge (days) per month, all repos. Excludes Gerrit repos."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Time to Merge PRs — 75th Percentile (18-month avg)", "Days")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}"))

    visible_data = []
    line_ends = []
    for repo, (months, medians) in all_ttm.items():
        if not months or repo in GERRIT_REPOS:
            continue
        smoothed = smooth(medians, window=18)
        ax.plot(months, smoothed,
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(smoothed)
        line_ends.append((months, smoothed, get_short(repo), get_color(repo)))

    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(ymin, ymax)
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "down")
    # Insights: current p75 TTM for each repo
    add_insight_box(ax, [
        "runtime and roslyn: fast merges (<5d p75) — strong review culture",
        "maui p75 is 9x others — many partner/community PRs sit in queue",
        "maui has Syncfusion contributors with 20-30 day median review waits",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "time_to_merge_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_open_pr_age(all_items, output_dir):
    """Median age (days) of open PRs at each monthly snapshot — shows backlog staleness."""
    from statistics import median
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Median Age of Open PRs (Monthly Snapshot, 6-month avg)", "Days")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}"))

    visible_data = []
    line_ends = []
    for repo, items in all_items.items():
        if repo in GERRIT_REPOS:
            continue
        # Collect PRs with created/closed dates
        prs = []
        for item in items:
            if not item["is_pr"]:
                continue
            cd = parse_date(item["created_at"])
            if not cd:
                continue
            close = parse_date(item.get("merged_at") or item.get("closed_at"))
            prs.append((cd, close))
        if not prs:
            continue
        prs.sort(key=lambda x: x[0])

        # Monthly snapshots
        first_month = prs[0][0].replace(day=1)
        last_month = datetime.now().date().replace(day=1)
        months = []
        medians = []
        m = first_month
        while m <= last_month:
            snapshot = m + timedelta(days=15)  # mid-month
            ages = []
            for created, closed in prs:
                cd_date = created if isinstance(created, type(snapshot)) else created.date() if hasattr(created, 'date') else created
                if cd_date > snapshot:
                    break  # sorted by created_at
                cl_date = None
                if closed:
                    cl_date = closed if isinstance(closed, type(snapshot)) else closed.date() if hasattr(closed, 'date') else closed
                if cl_date is None or cl_date > snapshot:
                    ages.append((snapshot - cd_date).days)
            if ages:
                months.append(m)
                medians.append(median(ages))
            m = (m + timedelta(days=32)).replace(day=1)

        if not months:
            continue
        s = smooth(medians, 6)
        ax.plot(months, s,
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(s)
        line_ends.append((months, s, get_short(repo), get_color(repo)))

    if not visible_data:
        plt.close(fig)
        return

    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(ymin, ymax)
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "down")
    add_insight_box(ax, [
        "Complements TTM — TTM shows merged PRs, this shows the unmerged backlog",
        "roslyn's rising age driven by ~630 stale PRs (68% over 1yr old)\n  — mostly maintainer PRs (66%), not abandoned community work",
        "vscode age dropping recently — team actively closing old PRs",
        "maui's high age reflects long-lived Syncfusion/partner PRs in queue",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "open_pr_age.png")
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
    add_insight_box(ax, [
        "runtime maintainers dropping since late 2023 (.NET 8 timeframe)\n  — may reflect org restructuring or natural attrition",
        "vscode steadily growing — largest maintainer pool by far",
        "maui volatile — tiny team (6-11 people), sensitive to individual changes",
    ])
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
    add_direction_arrow(ax, "up")
    add_insight_box(ax, [
        "maui: 2-3 people merge nearly all PRs (rmarinho ~50%)",
        "vscode maintainers handle ~2x the PR volume of dotnet repos",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "prs_per_maintainer_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def _interpolate_maintainers_to_weeks(weeks, maint_months, maint_counts):
    """Map monthly maintainer counts to weekly dates via nearest-month lookup.
    Returns None for weeks before the first maintainer month."""
    if not maint_months or not weeks:
        return None
    month_map = {m: c for m, c in zip(maint_months, maint_counts)}
    first_month = min(maint_months)
    result = []
    for w in weeks:
        wm = w.replace(day=1)
        if wm < first_month:
            result.append(None)
            continue
        count = month_map.get(wm)
        if count is None:
            if wm.month == 1:
                prev = wm.replace(year=wm.year - 1, month=12)
            else:
                prev = wm.replace(month=wm.month - 1)
            count = month_map.get(prev)
        if count is None or count < 1:
            result.append(None)
        else:
            result.append(count)
    return result


def chart_open_issues_per_maintainer(all_series, all_maint, output_dir):
    """Open issues divided by active maintainers — shows burden per person."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Open Issues per Active Maintainer (3-month rolling avg)", "Issues / Maintainer")
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
        # Filter to weeks with valid maintainer data
        valid = [(w, oi, m) for w, oi, m in zip(series["weeks"], series["open_issues"], weekly_maint) if m is not None]
        if not valid:
            continue
        vw, voi, vm = zip(*valid)
        ratio = [oi / m for oi, m in zip(voi, vm)]
        s = smooth(ratio, window=13)
        ax.plot(list(vw), s, color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(s)
        line_ends.append((list(vw), s, get_short(repo), get_color(repo)))

    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(ymin, ymax)
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "down")
    add_insight_box(ax, [
        "Across all repos, issues per maintainer trend upward over time\n  — issue backlogs grow faster than teams do",
        "runtime burden growing — maintainer count dropped while issues held steady",
        "vscode's large team keeps per-person load relatively flat",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "open_issues_per_maintainer.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_open_prs_per_maintainer(all_series, all_maint, output_dir):
    """Open PRs divided by active maintainers — shows review burden per person."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Open PRs per Active Maintainer (3-month rolling avg)", "PRs / Maintainer")
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
        valid = [(w, op, m) for w, op, m in zip(series["weeks"], series["open_prs"], weekly_maint) if m is not None]
        if not valid:
            continue
        vw, vop, vm = zip(*valid)
        ratio = [op / m for op, m in zip(vop, vm)]
        s = smooth(ratio, window=13)
        ax.plot(list(vw), s, color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(s)
        line_ends.append((list(vw), s, get_short(repo), get_color(repo)))

    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(ymin, ymax)
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "down")
    add_insight_box(ax, [
        "Same upward trend as issues per maintainer — maintainer\n  workload is increasing across all repos",
        "maui's tiny merge team (2-3 people) drives high per-person load",
        "roslyn rising sharply — 630+ open PRs (68% over 1yr old),\n  stale community PRs accumulating without being closed",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "open_prs_per_maintainer.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_contributor_diversity(all_items, output_dir):
    """Distinct PR authors per month (2-month rolling window) — measures community breadth."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Active Community Contributors (Distinct PR Authors, 2-Month Window)",
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
    add_direction_arrow(ax, "up")
    add_insight_box(ax, [
        "runtime community authors declining ~22% but PR volume held steady\n  — fewer people contributing more each (7.4 to 8.8 PRs/person)",
        "maui jumped mid-2024 — ~22 Syncfusion engineers began dedicated\n  contributions (74% of community PRs since Aug 2024)",
        "vscode jumped in 2025 — likely Copilot-driven (total PRs also surged)",
        "rust has broadest contributor base despite niche language",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "contributor_diversity_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_issue_community(all_items, output_dir):
    """Distinct community issue openers per month (non-maintainers, 2-month window)."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Distinct Community Issue Openers (Non-Maintainers, 2-Month Window, 6-month avg)",
               "Unique Community Openers")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}"))

    visible_data = []
    line_ends = []
    has_data = False
    for repo, items in all_items.items():
        # Build maintainer set (anyone who ever merged a PR)
        maintainers = set()
        for item in items:
            if item["is_pr"] and item.get("merged_by"):
                maintainers.add(item["merged_by"])
        maintainers |= BOT_ACCOUNTS

        authors_by_month = defaultdict(set)
        for item in items:
            if item["is_pr"]:
                continue
            cd = parse_date(item["created_at"])
            author = item.get("author")
            if not cd or not author or author in maintainers:
                continue
            authors_by_month[cd.replace(day=1)].add(author)

        if not authors_by_month:
            continue
        months = sorted(authors_by_month.keys())
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
        has_data = True

    if not has_data:
        plt.close(fig)
        print("  (skipping issue community chart — no issue author data)")
        return

    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(ymin, ymax)
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "up")
    add_insight_box(ax, [
        "Excludes maintainers — shows external community engagement only",
        "vscode dominates due to massive user base reporting bugs",
        "runtime/maui declining — likely product maturation (fewer novel bugs)\n  and better self-service (docs, Stack Overflow, Discord)",
        "Could also signal community disengagement if issues feel ignored\n  — open backlog % is rising (runtime 14% to 30% since 2022)\n  though initial turnaround has held steady",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "issue_community_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_community_issue_volume(all_items, output_dir):
    """Monthly count of issues opened by community (non-maintainer) members."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Issues Opened by Community (Non-Maintainers, 6-month avg)",
               "Issues / Month")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}"))

    visible_data = []
    line_ends = []
    has_data = False
    for repo, items in all_items.items():
        maintainers = set()
        for item in items:
            if item["is_pr"] and item.get("merged_by"):
                maintainers.add(item["merged_by"])
        maintainers |= BOT_ACCOUNTS

        issues_by_month = defaultdict(int)
        for item in items:
            if item["is_pr"]:
                continue
            cd = parse_date(item["created_at"])
            author = item.get("author")
            if not cd or not author or author in maintainers:
                continue
            issues_by_month[cd.replace(day=1)] += 1

        if not issues_by_month:
            continue
        months = sorted(issues_by_month.keys())
        counts = [issues_by_month[m] for m in months]

        s = smooth(counts, 6)
        ax.plot(months, s,
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(s)
        line_ends.append((months, s, get_short(repo), get_color(repo)))
        has_data = True

    if not has_data:
        plt.close(fig)
        print("  (skipping community issue volume chart — no issue author data)")
        return

    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(ymin, ymax)
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_insight_box(ax, [
        "runtime volume declining since 2022 — but community share of new issues\n  is rising (55% to 62%) as team files fewer issues",
        "vscode volume tracks product adoption — dwarfs all other repos",
        "Declining volume + rising community % = healthy maturation pattern",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "community_issue_volume.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_community_issue_share(all_items, output_dir):
    """% of issues opened by community (non-maintainers) per month."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Community Share of Issues (% Non-Maintainer, 6-month avg)",
               "% Community")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}%"))

    visible_data = []
    line_ends = []
    has_data = False
    for repo, items in all_items.items():
        maintainers = set()
        for item in items:
            if item["is_pr"] and item.get("merged_by"):
                maintainers.add(item["merged_by"])
        maintainers |= BOT_ACCOUNTS

        total_by_month = defaultdict(int)
        community_by_month = defaultdict(int)
        for item in items:
            if item["is_pr"]:
                continue
            cd = parse_date(item["created_at"])
            author = item.get("author")
            if not cd or not author:
                continue
            m = cd.replace(day=1)
            total_by_month[m] += 1
            if author not in maintainers:
                community_by_month[m] += 1

        if not total_by_month:
            continue
        months = sorted(total_by_month.keys())
        pcts = [100.0 * community_by_month.get(m, 0) / total_by_month[m]
                if total_by_month[m] >= 10 else None for m in months]
        valid = [(m, p) for m, p in zip(months, pcts) if p is not None]
        if len(valid) < 6:
            continue
        vm, vp = zip(*valid)
        s = smooth(list(vp), 6)
        ax.plot(list(vm), s, color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(s)
        line_ends.append((list(vm), s, get_short(repo), get_color(repo)))
        has_data = True

    if not has_data:
        plt.close(fig)
        print("  (skipping community issue share — no author data)")
        return

    ax.set_ylim(0, 100)
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "up")
    add_insight_box(ax, [
        "runtime share rising (53% to 62%) even as volume drops\n  — team filing fewer issues, community holding steady",
        "maui near 90% — UI framework hits many device/platform edge cases;\n  community issues are 88% bug reports vs 11% feature requests",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "community_issue_share.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_community_pr_share(all_items, output_dir):
    """% of PRs opened by community (non-maintainers) per month."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Community Share of PRs (% Non-Maintainer, 6-month avg)",
               "% Community")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}%"))

    visible_data = []
    line_ends = []
    for repo, items in all_items.items():
        if repo in GERRIT_REPOS:
            continue
        maintainers = set()
        for item in items:
            if item["is_pr"] and item.get("merged_by"):
                maintainers.add(item["merged_by"])
        maintainers |= BOT_ACCOUNTS

        total_by_month = defaultdict(int)
        community_by_month = defaultdict(int)
        for item in items:
            if not item["is_pr"]:
                continue
            cd = parse_date(item["created_at"])
            author = effective_author(item)
            if not cd or not author:
                continue
            m = cd.replace(day=1)
            total_by_month[m] += 1
            if author not in maintainers:
                community_by_month[m] += 1

        if not total_by_month:
            continue
        months = sorted(total_by_month.keys())
        pcts = [100.0 * community_by_month.get(m, 0) / total_by_month[m]
                if total_by_month[m] >= 10 else None for m in months]
        valid = [(m, p) for m, p in zip(months, pcts) if p is not None]
        if len(valid) < 6:
            continue
        vm, vp = zip(*valid)
        s = smooth(list(vp), 6)
        ax.plot(list(vm), s, color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(s)
        line_ends.append((list(vm), s, get_short(repo), get_color(repo)))

    if not visible_data:
        plt.close(fig)
        return

    ax.set_ylim(0, 100)
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_insight_box(ax, [
        "vscode ~92% community PRs — almost entirely external contributors",
        "runtime ~70% community — healthy mix of team + external",
        "maui community share surged mid-2024 with Syncfusion partnership\n  — 22 dedicated engineers now contributing regularly",
        "rust near 100% is misleading — we detect maintainers via merged_by,\n  but bors merges everything, so real team members look like community",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "community_pr_share.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


COPILOT_AUTHORS = {"copilot-swe-agent[bot]", "Copilot"}

def chart_copilot_adoption(all_items, output_dir):
    """Copilot-authored PRs as % of all PRs, weekly with 2-week smoothing."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Copilot PRs as % of All PRs (4-week avg)", "% of PRs")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}%"))

    line_ends = []
    for repo, items in all_items.items():
        total_by_week = defaultdict(int)
        copilot_by_week = defaultdict(int)
        for item in items:
            if not item["is_pr"]:
                continue
            cd = parse_date(item["created_at"])
            if not cd:
                continue
            # ISO week start (Monday)
            week = cd - timedelta(days=cd.weekday())
            author = item.get("author") or ""
            total_by_week[week] += 1
            if author in COPILOT_AUTHORS:
                copilot_by_week[week] += 1

        if not total_by_week:
            continue
        weeks = sorted(total_by_week.keys())
        # Only show weeks where Copilot existed (2024+)
        weeks = [w for w in weeks if w.year >= 2024]
        if not weeks:
            continue
        # Drop last week if it's partial (less than 7 days old)
        from datetime import date as _date
        today = _date.today()
        if (today - weeks[-1]).days < 7:
            weeks = weeks[:-1]
        if not weeks:
            continue
        pcts = [100.0 * copilot_by_week.get(w, 0) / total_by_week[w]
                if total_by_week[w] >= 5 else None for w in weeks]
        # Fill None with 0 for smoothing
        pcts_clean = [p if p is not None else 0.0 for p in pcts]
        smoothed = smooth(pcts_clean, 4)
        ax.plot(weeks, smoothed,
                color=get_color(repo), label=get_short(repo),
                linewidth=2, alpha=0.85)
        line_ends.append((weeks, smoothed, get_short(repo), get_color(repo)))

    ax.set_ylim(0, None)
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "up")
    add_insight_box(ax, [
        "Shows adoption of Copilot SWE Agent for PR creation",
        "runtime is early/aggressive adopter — reflects team investment",
        "Rapid month-over-month growth across all dotnet repos",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "copilot_adoption.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_issue_close_rate(all_series, output_dir):
    """Percentage of issues closed within 30 days of opening, by month."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Issue Turnaround (% Closed Within 30 Days, 6-month avg)",
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
    add_direction_arrow(ax, "up")
    add_insight_box(ax, [
        "vscode closes ~60% within 30 days — aggressive bot-assisted triage",
        "go historically most responsive — small focused team",
        "runtime starts at 2020 (pre-merge data unreliable) — has held\n  steady at ~40-50%, respectable for its issue volume",
    ])
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


def chart_community_responsiveness(all_items, all_maint, output_dir):
    """Issue responsiveness for community-filed issues only.
    Community = anyone who has never merged a PR in that repo."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Community Issue Turnaround (% Closed Within 30 Days, 6-month avg)",
               "% Closed <30d")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}%"))

    from datetime import date as date_type
    LINEAGE_CUTOFF = {repo: date_type(2020, 1, 1) for repo in REPO_LINEAGE}

    line_ends = []
    for repo, items in all_items.items():
        cutoff = LINEAGE_CUTOFF.get(repo)

        # Build set of known maintainers (anyone who ever merged a PR)
        maintainers = set()
        for item in items:
            if item["is_pr"] and item.get("merged_by"):
                maintainers.add(item["merged_by"])
        maintainers |= BOT_ACCOUNTS

        monthly_total = defaultdict(int)
        monthly_fast = defaultdict(int)
        for item in items:
            if item["is_pr"]:
                continue
            author = item.get("author")
            if not author or author in maintainers:
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
    add_direction_arrow(ax, "up")
    add_insight_box(ax, [
        "Most repos hold steady over time — community turnaround is consistent",
        "runtime holding steady despite fewer maintainers — sustainable so far",
        "aspire and maui recently declining — possible team bandwidth pressure",
        "Lower than overall turnaround — team issues always get faster triage",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "community_responsiveness_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_community_time_to_close(all_items, output_dir):
    """P75 time-to-close (days) for community-filed issues, by month."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Community Issue Time-to-Close — 75th Percentile (12-month avg)", "Days")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}"))

    from datetime import date as date_type
    LINEAGE_CUTOFF = {repo: date_type(2020, 1, 1) for repo in REPO_LINEAGE}

    visible_data = []
    line_ends = []
    has_data = False
    for repo, items in all_items.items():
        cutoff = LINEAGE_CUTOFF.get(repo)

        maintainers = set()
        for item in items:
            if item["is_pr"] and item.get("merged_by"):
                maintainers.add(item["merged_by"])
        maintainers |= BOT_ACCOUNTS

        # Collect close times by month
        close_times_by_month = defaultdict(list)
        for item in items:
            if item["is_pr"]:
                continue
            author = item.get("author")
            if not author or author in maintainers:
                continue
            cd = parse_date(item["created_at"])
            cld = parse_date(item["closed_at"])
            if not cd or not cld:
                continue
            if cutoff and cd < cutoff:
                continue
            days = (cld - cd).days
            close_times_by_month[cd.replace(day=1)].append(days)

        if not close_times_by_month:
            continue
        months = sorted(close_times_by_month.keys())
        import numpy as np
        p75s = []
        valid_months = []
        for m in months:
            times = close_times_by_month[m]
            if len(times) >= 10:
                p75s.append(float(np.percentile(times, 75)))
                valid_months.append(m)

        if len(valid_months) < 6:
            continue
        s = smooth(p75s, 12)
        ax.plot(valid_months, s,
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(s)
        line_ends.append((valid_months, s, get_short(repo), get_color(repo)))
        has_data = True

    if not has_data:
        plt.close(fig)
        print("  (skipping community time-to-close — no issue author data)")
        return

    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(ymin, ymax)
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "down")
    add_insight_box(ax, [
        "roslyn dwarfs others — confirmed by label data: feature requests\n  take 248d median vs 70d for bugs (p75 is 3 years vs 1.9 years)",
        "roslyn's bulk housekeeping closures (2022, 2024) of old issues\n  push p75 even higher in those years",
        "go and rust will appear once issue author backfill completes",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "community_time_to_close.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_community_issue_age(all_items, output_dir):
    """Median age (days) of open community-filed issues at each monthly snapshot."""
    from statistics import median
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Median Age of Open Community Issues (Monthly Snapshot, 6-month avg)", "Days")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}"))

    visible_data = []
    line_ends = []
    has_data = False
    for repo, items in all_items.items():
        maintainers = set()
        for item in items:
            if item["is_pr"] and item.get("merged_by"):
                maintainers.add(item["merged_by"])
        maintainers |= BOT_ACCOUNTS

        # Collect community issues with dates
        issues = []
        for item in items:
            if item["is_pr"]:
                continue
            author = item.get("author")
            if not author or author in maintainers:
                continue
            cd = parse_date(item["created_at"])
            if not cd:
                continue
            close = parse_date(item["closed_at"])
            issues.append((cd, close))
        if not issues:
            continue
        issues.sort(key=lambda x: x[0])

        # Monthly snapshots
        first_month = issues[0][0].replace(day=1)
        last_month = datetime.now().date().replace(day=1)
        months = []
        medians = []
        m = first_month
        while m <= last_month:
            snapshot = m + timedelta(days=15)
            ages = []
            for created, closed in issues:
                cd_date = created if isinstance(created, type(snapshot)) else created.date() if hasattr(created, 'date') else created
                if cd_date > snapshot:
                    break
                cl_date = None
                if closed:
                    cl_date = closed if isinstance(closed, type(snapshot)) else closed.date() if hasattr(closed, 'date') else closed
                if cl_date is None or cl_date > snapshot:
                    ages.append((snapshot - cd_date).days)
            if len(ages) >= 5:
                months.append(m)
                medians.append(median(ages))
            m = (m + timedelta(days=32)).replace(day=1)

        if len(months) < 6:
            continue
        s = smooth(medians, 6)
        ax.plot(months, s,
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(s)
        line_ends.append((months, s, get_short(repo), get_color(repo)))
        has_data = True

    if not has_data:
        plt.close(fig)
        print("  (skipping community issue age — no issue author data)")
        return

    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(ymin, ymax)
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "down")
    add_insight_box(ax, [
        "Shows staleness of unresolved community issue backlog",
        "go's flat line is partial data (author backfill incomplete past 2015)",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "community_issue_age.png")
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
        chart_open_pr_age(all_items, output_dir)
        chart_issue_close_rate(all_items, output_dir)
        if has_maintainer_data:
            chart_active_maintainers(all_maint, output_dir)
            chart_prs_per_maintainer(all_maint, output_dir)
            chart_open_issues_per_maintainer(all_series, all_maint, output_dir)
            chart_open_prs_per_maintainer(all_series, all_maint, output_dir)
            chart_contributor_diversity(all_items, output_dir)
            chart_copilot_adoption(all_items, output_dir)
            chart_issue_community(all_items, output_dir)
            chart_community_issue_volume(all_items, output_dir)
            chart_community_issue_share(all_items, output_dir)
            chart_community_pr_share(all_items, output_dir)
            chart_community_responsiveness(all_items, all_maint, output_dir)
            chart_community_time_to_close(all_items, output_dir)
            chart_community_issue_age(all_items, output_dir)
        else:
            print("  (skipping maintainer charts — no author/merged_by data yet)")

    # Per-repo dashboards
    for repo in repos:
        chart_per_repo_dashboard(repo, all_series.get(repo), output_dir)

    conn.close()
    print(f"\nDone! Charts saved to {os.path.abspath(output_dir)}/")


if __name__ == "__main__":
    main()
