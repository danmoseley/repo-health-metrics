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
from datetime import date, datetime, timedelta
from pathlib import Path
from collections import defaultdict

try:
    import matplotlib
    matplotlib.use("Agg")  # non-interactive backend
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.ticker import FuncFormatter, MaxNLocator, MultipleLocator
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
    "dotnet/aspire": "#4CAF50",    # green (transferred to microsoft/aspire)
    "microsoft/aspire": "#4CAF50", # green
    "Azure/azure-sdk-for-js": "#007FFF",  # azure blue
    "microsoft/vscode": "#007ACC", # VS Code blue
    "microsoft/vcpkg": "#6A1B9A",  # deep purple
    "rust-lang/rust": "#B7410E",   # rust red-brown
    "golang/go": "#00897B",        # teal
}

REPO_SHORT = {
    "dotnet/runtime": "runtime",
    "dotnet/roslyn": "roslyn",
    "dotnet/maui": "maui",
    "dotnet/aspire": "aspire",
    "microsoft/aspire": "aspire",
    "Azure/azure-sdk-for-js": "azure-sdk-js",
    "microsoft/vscode": "vscode",
    "microsoft/vcpkg": "vcpkg",
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
    "dotnet/maui": "2021-03-01",
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

    # Detect available columns once (schema is per-DB, not per-repo)
    col_info = conn.execute("PRAGMA table_info(items)").fetchall()
    available_cols = {row[1] for row in col_info}
    has_copilot_trailer = "copilot_trailer" in available_cols
    has_title = "title" in available_cols
    copilot_col = "copilot_trailer" if has_copilot_trailer else "NULL AS copilot_trailer"
    title_col = "title" if has_title else "NULL AS title"

    items = []
    for load_repo, prs_only in repos_to_load:
        pr_filter = " AND is_pull_request = 1" if prs_only else ""
        sql = (f"SELECT number, created_at, closed_at, state, is_pull_request, merged_at, "
               f"author, merged_by, copilot_requester, {copilot_col}, {title_col} "
               f"FROM items WHERE repo = ?{pr_filter} ORDER BY created_at")
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
                "copilot_trailer": r[9],
                "title": r[10] or "",
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
    # Cap at the latest week actually present in the data.  When the database
    # hasn't been refreshed recently, extending to end_date appends trailing
    # all-zero weeks that create an artificial dip in rolling-average charts.
    latest_data_week = max(all_dates)
    if latest_data_week < last_week:
        last_week = latest_data_week
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
        # Use the last non-None, non-NaN value
        for i in range(len(values) - 1, -1, -1):
            v = values[i]
            if v is not None and v == v:  # NaN != NaN
                endpoints.append((dates[i], v, name, color))
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


_CHART_REGISTRY = {}  # hash -> title


def _chart_id(title):
    """Generate a stable 7-char hex ID from the chart title."""
    import hashlib
    h = hashlib.sha1(title.encode()).hexdigest()[:7]
    _CHART_REGISTRY[h] = title
    return h


def _data_date():
    """Return the last-modified date of the DB file as a 'YYYY-MM-DD' string."""
    import os
    # Try common locations relative to the script and cwd
    for candidate in [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "pr-dashboard.db"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "pr-dashboard.db"),
        "pr-dashboard.db",
    ]:
        if os.path.exists(candidate):
            mtime = os.path.getmtime(candidate)
            return datetime.fromtimestamp(mtime).strftime("%Y-%m-%d")
    return "?"


def _stamp_chart(ax, title):
    """Add chart ID + data date outside axes, bottom-left of the figure.
    Idempotent per figure: subsequent calls on additional axes are no-ops
    so multi-axes figures aren't stamped multiple times in the same spot."""
    label = f"{_chart_id(title)}  {_data_date()}"
    fig = ax.get_figure()
    if getattr(fig, "_chart_stamped", False):
        return
    fig._chart_stamped = True
    fig.text(0.01, 0.005, label, fontsize=7, color="black", va="bottom", ha="left",
             fontfamily="monospace")


def write_chart_registry(output_dir):
    """Write the hash->title registry to a TSV file alongside the charts."""
    import os
    path = os.path.join(output_dir, "chart_index.tsv")
    with open(path, "w", encoding="utf-8") as f:
        f.write("hash\ttitle\n")
        for h in sorted(_CHART_REGISTRY):
            title = _CHART_REGISTRY[h].replace("\n", " — ")
            f.write(f"{h}\t{title}\n")
    print(f"  {path}")


def setup_axes(ax, title, ylabel):
    ax.set_title(title, fontsize=13, fontweight="bold", pad=10)
    ax.set_ylabel(ylabel, fontsize=10)
    _stamp_chart(ax, title)
    # Major ticks on Jan 1 (year labels + grid lines), minor ticks quarterly
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.xaxis.set_minor_locator(mdates.MonthLocator(bymonth=[4, 7, 10]))
    ax.xaxis.set_minor_formatter(mdates.DateFormatter(""))
    ax.yaxis.set_major_formatter(FuncFormatter(thousands_formatter))
    ax.grid(True, alpha=0.3, which="major")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


def _pad_date_xlim(fig):
    """Snap x-axis limits to Jan 1 boundaries so every year in the data range
    gets a visible tick label. Uses ax.dataLim (actual data extent) to avoid
    being fooled by matplotlib's auto-margin overshooting past a Jan 1."""
    for ax in fig.get_axes():
        # Only adjust axes that use the year-label scheme from setup_axes
        major_loc = ax.xaxis.get_major_locator()
        if not isinstance(major_loc, mdates.YearLocator):
            continue
        # Use actual data extent, not the margin-inflated xlim
        data_lo = ax.dataLim.x0
        data_hi = ax.dataLim.x1
        lo_date = mdates.num2date(data_lo).replace(tzinfo=None)
        hi_date = mdates.num2date(data_hi).replace(tzinfo=None)
        jan1_first = datetime(lo_date.year, 1, 1)
        # Next Jan 1 at or after the last data point
        if hi_date.month == 1 and hi_date.day == 1:
            jan1_after_last = hi_date
        else:
            jan1_after_last = datetime(hi_date.year + 1, 1, 1)
        ax.set_xlim(mdates.date2num(jan1_first), mdates.date2num(jan1_after_last))


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
        "Issue backlogs grow monotonically across all repos — none have reversed this",
        "maui's backlog has flattened since mid-2024 — inflow and closures now roughly balanced",
        "vscode triages ~3K issues every December (end-of-year housekeeping)\n  but the upward trend still dominates",
        "go's backlog is flattest — open/close rates stay balanced over time",
    ])
    _pad_date_xlim(fig)
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
        "PR backlogs rise over time in every repo — a universal pattern as old PRs go idle rather than close",
        "vscode's 3x jump in 2022 was a workflow change to smaller PRs,\n  not team growth — same ~175 authors making 3x more PRs",
        "rust's high open PR count reflects its large contributor base (5,000+ community authors)\n  and rigorous multi-stage review process",
    ])
    _pad_date_xlim(fig)
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

    # Fixed range — shows the interesting variation without extreme spikes
    ax.set_ylim(-75, 100)
    ax.annotate("Y-axis clamped to [-75, +100] to exclude bulk closure spikes",
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
        "go stays flattest — open/close rates stay closely balanced over time",
    ]
    add_insight_box(ax, lines)
    add_direction_arrow(ax, "down")
    _pad_date_xlim(fig)
    fig.tight_layout()
    path = os.path.join(output_dir, "net_issue_flow_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_net_pr_flow_comparison(all_series, output_dir):
    """Net PR flow (opened - merged per week), full history, smoothed, Y-axis clamped."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Net PR Flow (Opened − Merged per Week, 2-year avg)",
               "Net PRs / Week")

    ax.axhline(y=0, color="black", linewidth=0.5, alpha=0.5)

    visible_data = []
    line_ends = []
    for repo, series in all_series.items():
        if not series or repo in GERRIT_REPOS:
            continue
        # Opened − merged (per user request); closures-without-merge are NOT subtracted here
        net = [o - m for o, m in zip(series["pr_opened"], series["pr_merged"])]
        smoothed = smooth(net, window=104)
        alpha = 0.4 if repo == "microsoft/vscode" else 0.85
        lw = 1.2 if repo == "microsoft/vscode" else 1.5
        ax.plot(series["weeks"], smoothed,
                color=get_color(repo), label=get_short(repo),
                linewidth=lw, alpha=alpha)
        visible_data.append(smoothed)
        line_ends.append((series["weeks"], smoothed, get_short(repo), get_color(repo)))

    # Y-axis tuned to typical variation; positive = backlog growing
    ax.set_ylim(-20, 100)
    ax.annotate("Y-axis clamped to [-20, +100]; positive = backlog growing",
                xy=(0.02, 0.02), xycoords="axes fraction", fontsize=8,
                color="#888888", style="italic")
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "down")
    add_insight_box(ax, [
        "Net = opened − merged (PRs closed without merging are NOT subtracted)",
        "Negative = repo is merging at least as fast as new PRs arrive",
        "Sustained positive = open-PR list growing if closures-without-merge stay flat",
        "vcpkg's positive trend reflects bot-managed merge queue + many stale auto-closed PRs",
    ])
    _pad_date_xlim(fig)
    fig.tight_layout()
    path = os.path.join(output_dir, "net_pr_flow_comparison.png")
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
        "runtime merge rate declining since late 2024 — coincides\n  with ~28% drop in active maintainers since 2023 peak?",
        "vscode 3x jump mid-2022 was workflow shift to smaller PRs,\n  not a staffing increase (same ~175 authors)",
        "rust's steady ~160/wk — high volume driven by 1,000+ active contributors;\n  bors automates the merge step but review/iteration is still human",
    ])
    _pad_date_xlim(fig)
    fig.tight_layout()
    path = os.path.join(output_dir, "pr_merge_rate_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_pr_merge_rate_12m(all_items, output_dir):
    """PR merge rate over last 12 months, weekly data points with 28-day trailing sum."""
    import numpy as np

    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "PR Merge Rate — last 12 months (28-day trailing sum)",
               "PRs Merged / Week (28-day rolling sum, ÷4)")

    today = datetime.now().date()

    visible_data = []
    line_ends = []
    for repo, items in all_items.items():
        if not items or repo in GERRIT_REPOS:
            continue
        latest_merged_day = max(
            (parse_date(it.get("merged_at"))
             for it in items
             if it.get("is_pr") and it.get("merged_at")),
            default=None,
        )
        if latest_merged_day is None:
            continue
        last_day = min(today - timedelta(days=1), latest_merged_day)
        cutoff = last_day - timedelta(days=364)
        # Need 27 days of pre-window history so the first plotted point has a full 28-day window
        fetch_start = cutoff - timedelta(days=27)
        # Daily merged counts within the (extended) window
        daily = defaultdict(int)
        for it in items:
            if not it.get("is_pr"):
                continue
            md = parse_date(it.get("merged_at"))
            if md and fetch_start <= md <= last_day:
                daily[md] += 1
        if not daily:
            continue
        # Build weekly series on Mondays and compute 28-day trailing sum at each point
        # Divide by 4 to express as PRs/week average
        weeks = []
        rolling = []
        d = week_start(cutoff)
        if d < cutoff:
            d += timedelta(days=7)
        while d <= last_day:
            s = sum(daily.get(d - timedelta(days=k), 0) for k in range(28)) / 4.0
            weeks.append(d)
            rolling.append(s)
            d += timedelta(days=7)
        if not any(rolling):
            continue
        ax.plot(weeks, rolling,
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        if len(weeks) >= 2:
            x = mdates.date2num(weeks)
            slope, intercept = np.polyfit(x, rolling, 1)
            trend = slope * x + intercept
            ax.plot(weeks, trend,
                    color=get_color(repo), linestyle=":",
                    linewidth=1.5, alpha=0.35)
            # Compute % change per month from the regression line
            mid_val = trend[len(trend) // 2]
            if mid_val > 0:
                pct_per_day = slope / mid_val * 100
                pct_per_month = pct_per_day * 30.44
                direction = "+" if pct_per_month >= 0 else "\u2212"
                label = f"{direction}{abs(pct_per_month):.1f}%/mo"
                # Place at 40% along the trend line to avoid line-end labels
                idx = max(1, len(weeks) * 2 // 5)
                ax.annotate(label, xy=(weeks[idx], trend[idx]),
                            fontsize=7, color=get_color(repo), alpha=0.7,
                            ha="center", va="bottom",
                            xytext=(0, 4), textcoords="offset points")
        visible_data.append(rolling)
        line_ends.append((weeks, rolling, get_short(repo), get_color(repo)))

    if not visible_data:
        plt.close(fig)
        return

    ymin, ymax = robust_ylim(visible_data, percentile=0.99)
    ax.set_ylim(ymin, max(ymax, 50))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    ax.xaxis.set_minor_locator(mdates.WeekdayLocator(byweekday=0))
    ax.xaxis.set_minor_formatter(mdates.DateFormatter(""))
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    total_start = sum(series[0] for series in visible_data)
    total_end = sum(series[-1] for series in visible_data)
    add_direction_arrow(ax, "up" if total_end >= total_start else "down")
    add_insight_box(ax, [
        "12-month view of PR merge rate — weekly points, each = 28-day trailing avg (÷4)",
        "Bridges the long-term 52-week rolling chart and shorter-window views",
        "Each point = average PRs merged per week over the preceding 4 weeks",
        "Each line ends at that repo's latest merged date in the DB",
        "Weekly cadence smooths day-to-day noise while preserving medium-term shifts",
    ])
    _pad_date_xlim(fig)
    fig.tight_layout()
    path = os.path.join(output_dir, "pr_merge_rate_12m.png")
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
            _dashboard_insight(ax, f"Backlog shrinking ({delta:+,}/yr)")

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
            _dashboard_insight(ax, f"Review queue growing ({delta:+,}/yr)")
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
                _dashboard_insight(ax, f"Closing faster than opening ({ratio:.0%})")
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

    _pad_date_xlim(fig)
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
        "runtime rose to ~115% in 2025 despite fewer maintainers — driven\n  by falling issue inflow (maturing product) not faster triage",
        "roslyn spikes (~2022-Q4, 2024-Q4, 2025-Q4) are deliberate stale issue\n  housekeeping — bulk-closing old Area-IDE issues (avg age 3-5 years)",
        "Most repos hover near 100% — roughly keeping pace",
    ])
    add_direction_arrow(ax, "up")
    _pad_date_xlim(fig)
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
        "runtime p75 ~6d, roslyn p75 ~3d — among the fastest",
        "maui p75 rose sharply mid-2024 when Syncfusion partnership ramped up",
    ])
    _pad_date_xlim(fig)
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

        # Monthly snapshots: cap at the latest date actually in the data so
        # months after the DB cutoff don't show artificially inflated ages
        # (stale DB is missing recent merges, so closed PRs look still-open).
        latest_event = max(
            (d for d in (cd for cd, _ in prs) if d is not None),
            default=None,
        )
        latest_close = max(
            (cl for _, cl in prs if cl is not None),
            default=None,
        )
        if latest_close and (latest_event is None or latest_close > latest_event):
            latest_event = latest_close
        first_month = prs[0][0].replace(day=1)
        last_month = min(
            datetime.now().date().replace(day=1),
            latest_event.replace(day=1) if latest_event else datetime.now().date().replace(day=1),
        )
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
    _pad_date_xlim(fig)
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
        "runtime maintainers dropping since late 2023 (.NET 8 timeframe)",
        "vscode steadily growing — largest maintainer pool by far",
        "maui volatile — small team (6-11 people), sensitive to individual changes",
    ])
    _pad_date_xlim(fig)
    fig.tight_layout()
    path = os.path.join(output_dir, "active_maintainers_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_prs_per_maintainer(all_maint, output_dir):
    """PRs merged per active maintainer per month. Excludes Gerrit and bot-merger repos."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "PRs Merged per Active Maintainer (Monthly, 4-month avg)",
               "PRs / Maintainer / Month")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}"))

    excluded = GERRIT_REPOS | BOT_MERGER_REPOS
    visible_data = []
    line_ends = []
    for repo, (months, _, prs_per, _) in all_maint.items():
        if not months or repo in excluded:
            continue
        s = smooth(prs_per, 4)
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
    _pad_date_xlim(fig)
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
    _pad_date_xlim(fig)
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
        "maui's small merge team (2-3 people) drives high per-person load",
        "roslyn rising sharply — 630+ open PRs (68% over 1yr old),\n  mostly maintainer-authored PRs going stale",
    ])
    _pad_date_xlim(fig)
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
        "runtime active contributors declining from 2022 peak (~25% after smoothing)\n  — includes both community and maintainer authors",
        "maui jumped mid-2024 — Syncfusion engineers began dedicated\n  contributions, now a large share of community PRs",
        "vscode jumped in 2025 — likely Copilot-driven (total PRs also surged)?",
        "rust has broadest contributor base of all repos tracked",
    ])
    _pad_date_xlim(fig)
    fig.tight_layout()
    path = os.path.join(output_dir, "contributor_diversity_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_issue_community(all_items, output_dir):
    """Distinct community issue openers per month (non-maintainers, 2-month window)."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Distinct Community Issue Openers (2-Month Window, 6-month avg)",
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
        "runtime/maui declining — product maturation (fewer novel bugs)\n  and better self-service (docs, Stack Overflow, Discord)?",
        "Could also signal community disengagement if issues feel ignored?\n  — open backlog % rising (runtime 14% to 21% for 2022→2024 cohorts)\n  though initial turnaround has held steady",
    ])
    _pad_date_xlim(fig)
    fig.tight_layout()
    path = os.path.join(output_dir, "issue_community_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_community_issue_volume(all_items, output_dir):
    """Monthly count of issues opened by community (non-maintainer) members."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Issues Opened by Community (6-month avg)",
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
        "All dotnet repos show the same pattern: community issue volume peaked ~2022 and is declining",
        "runtime volume declining since 2022 — but community share of new issues\n  is rising (~58% to 62%) as team files fewer issues",
        "vscode volume tracks product adoption — dwarfs all other repos",
    ])
    _pad_date_xlim(fig)
    fig.tight_layout()
    path = os.path.join(output_dir, "community_issue_volume.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_community_issue_share(all_items, output_dir):
    """% of issues opened by community (non-maintainers) per month."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Community Share of Issues (6-month avg)",
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
        "maui near 90% — UI framework hits many device/platform edge cases;\n  community issues are ~86% bug reports, only 2% feature requests",
    ])
    _pad_date_xlim(fig)
    fig.tight_layout()
    path = os.path.join(output_dir, "community_issue_share.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_community_pr_share(all_items, output_dir):
    """% of PRs opened by community (non-maintainers) per month."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Community Share of PRs (6-month avg)",
               "% Community")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}%"))

    visible_data = []
    line_ends = []
    for repo, items in all_items.items():
        if repo in GERRIT_REPOS or repo in BOT_MERGER_REPOS:
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
        "vscode ~24% community PRs — mostly built by maintainer team",
        "runtime ~35% community — rising trend in recent years",
        "maui community share surged mid-2024 with Syncfusion partnership",
    ])
    _pad_date_xlim(fig)
    fig.tight_layout()
    path = os.path.join(output_dir, "community_pr_share.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


COPILOT_AUTHORS = {"copilot-swe-agent[bot]", "Copilot"}

def _classify_copilot(item):
    """Classify a PR's Copilot involvement.
    Returns: 'cca', 'assisted', 'none', or 'unknown'.
    - 'cca': author is a known Copilot bot (regardless of trailer)
    - 'assisted': human author with Co-authored-by: Copilot trailer
    - 'none': human author, checked, no trailer
    - 'unknown': not yet checked (copilot_trailer IS NULL)
    """
    author = item.get("author") or ""
    if author in COPILOT_AUTHORS:
        return "cca"
    trailer = item.get("copilot_trailer")
    if trailer is None:
        return "unknown"
    if trailer == 1:
        return "assisted"
    return "none"

def chart_copilot_adoption(all_items, output_dir):
    """Two Copilot charts: aggregated % and CCA vs Copilot-assisted split."""
    from datetime import date as _date
    today = _date.today()

    # Determine which repos have sufficient trailer coverage (>= 80% of recent PRs checked)
    # Only check PRs in the fetch window (last 12 months) to match what was actually fetched
    # Without this, repos with partial data (e.g., SAML failures) show misleading percentages
    coverage_cutoff = today - timedelta(days=365)
    repos_with_coverage = set()
    for repo, items in all_items.items():
        recent_prs = []
        for i in items:
            if not i["is_pr"]:
                continue
            cd = parse_date(i["created_at"])
            if cd and cd >= coverage_cutoff:
                recent_prs.append(i)
        if not recent_prs:
            continue
        checked = sum(1 for i in recent_prs if i.get("copilot_trailer") is not None)
        coverage = checked / len(recent_prs)
        if coverage >= 0.8:
            repos_with_coverage.add(repo)
        else:
            print(f"  Skipping {repo} from Copilot charts: only {coverage:.0%} trailer coverage")

    # Determine common end date across repos so all lines end at the same point.
    # Without this, repos fetched at different times have different line lengths.
    latest_weeks_per_repo = []
    for repo, items in all_items.items():
        if repo not in repos_with_coverage:
            continue
        max_cd = None
        for item in items:
            if not item["is_pr"]:
                continue
            cd = parse_date(item["created_at"])
            if cd and (max_cd is None or cd > max_cd):
                max_cd = cd
        if max_cd:
            latest_weeks_per_repo.append(max_cd - timedelta(days=max_cd.weekday()))
    # Cap at the earliest repo's latest week so no line extends beyond what others can show
    common_end_week = min(latest_weeks_per_repo) if latest_weeks_per_repo else None

    # --- Chart 1: All Copilot PRs as % of All PRs ---
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Copilot PRs as % of All PRs (4-week avg)", "% of PRs")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}%"))

    line_ends = []
    for repo, items in all_items.items():
        if repo not in repos_with_coverage:
            continue
        total_by_week = defaultdict(int)
        copilot_by_week = defaultdict(int)
        for item in items:
            if not item["is_pr"]:
                continue
            cls = _classify_copilot(item)
            if cls == "unknown":
                continue  # exclude unchecked PRs
            cd = parse_date(item["created_at"])
            if not cd:
                continue
            week = cd - timedelta(days=cd.weekday())
            total_by_week[week] += 1
            if cls in ("cca", "assisted"):
                copilot_by_week[week] += 1

        if not total_by_week:
            continue
        weeks = sorted(total_by_week.keys())
        weeks = [w for w in weeks if w >= coverage_cutoff]
        if common_end_week:
            weeks = [w for w in weeks if w <= common_end_week]
        if not weeks:
            continue
        if (today - weeks[-1]).days < 7:
            weeks = weeks[:-1]
        if not weeks:
            continue
        pcts = [100.0 * copilot_by_week.get(w, 0) / total_by_week[w]
                if total_by_week[w] >= 5 else None for w in weeks]
        pcts_clean = [p if p is not None else 0.0 for p in pcts]
        smoothed = smooth(pcts_clean, 4)
        ax.plot(weeks, smoothed,
                color=get_color(repo), label=get_short(repo),
                linewidth=2, alpha=0.85)
        line_ends.append((weeks, smoothed, get_short(repo), get_color(repo)))

    ax.set_ylim(0, 100)
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "up")
    add_insight_box(ax, [
        "Each point = 4-week rolling average of % of PRs created that week with Copilot involvement",
        "Combines CCA (bot-authored) + Copilot-assisted (human + Co-authored-by trailer)",
        "Excludes PRs where trailer status is unknown (not yet checked)",
        "First-commit-only heuristic — may undercount Copilot usage",
    ])
    _pad_date_xlim(fig)
    fig.tight_layout()
    path = os.path.join(output_dir, "copilot_adoption.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")

    # --- Chart 2: CCA vs Copilot-Assisted split (separate lines per repo) ---
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Copilot PRs by Type (4-week avg)", "PRs / week")

    line_ends = []
    for repo, items in all_items.items():
        if repo not in repos_with_coverage:
            continue
        cca_by_week = defaultdict(int)
        assisted_by_week = defaultdict(int)
        for item in items:
            if not item["is_pr"]:
                continue
            cls = _classify_copilot(item)
            cd = parse_date(item["created_at"])
            if not cd:
                continue
            week = cd - timedelta(days=cd.weekday())
            if cls == "cca":
                cca_by_week[week] += 1
            elif cls == "assisted":
                assisted_by_week[week] += 1

        # Only plot if there's meaningful data
        all_weeks = sorted(set(list(cca_by_week.keys()) + list(assisted_by_week.keys())))
        all_weeks = [w for w in all_weeks if w >= coverage_cutoff]
        if common_end_week:
            all_weeks = [w for w in all_weeks if w <= common_end_week]
        if not all_weeks:
            continue
        if (today - all_weeks[-1]).days < 7:
            all_weeks = all_weeks[:-1]
        if not all_weeks:
            continue

        cca_vals = [cca_by_week.get(w, 0) for w in all_weeks]
        assisted_vals = [assisted_by_week.get(w, 0) for w in all_weeks]

        cca_smooth = smooth(cca_vals, 4)
        assisted_smooth = smooth(assisted_vals, 4)

        color = get_color(repo)
        short = get_short(repo)

        # CCA as solid line, assisted as dashed
        if any(v > 0 for v in cca_smooth):
            ax.plot(all_weeks, cca_smooth,
                    color=color, label=f"{short} CCA",
                    linewidth=2, alpha=0.85)
            line_ends.append((all_weeks, cca_smooth, f"{short} CCA", color))
        if any(v > 0 for v in assisted_smooth):
            ax.plot(all_weeks, assisted_smooth,
                    color=color, label=f"{short} assisted",
                    linewidth=2, alpha=0.85, linestyle="--")
            line_ends.append((all_weeks, assisted_smooth, f"{short} assisted", color))

    ax.set_ylim(0, None)
    ax.legend(loc="upper left", fontsize=9, ncol=2)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "up")
    add_insight_box(ax, [
        "Each point = 4-week rolling average of PRs created that week, by Copilot involvement type",
        "CCA = Copilot Cloud Agent (bot-authored PRs, solid lines)",
        "Assisted = human author with Co-authored-by: Copilot trailer (dashed)",
        "Shows whether growth comes from CCA, local Copilot use, or both",
    ])
    _pad_date_xlim(fig)
    fig.tight_layout()
    path = os.path.join(output_dir, "copilot_by_type.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")

    # --- Chart 3: CCA vs Assisted aggregated across dotnet repos ---
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Copilot PRs by Type — dotnet repos aggregated (4-week avg)", "PRs / week")

    # Track per-week trailer-check coverage so we can drop weeks where
    # the backfill hasn't caught up (otherwise Assisted is artifactually low).
    cca_total = defaultdict(int)
    assisted_total = defaultdict(int)
    week_total = defaultdict(int)     # all PRs in week (including unknown)
    week_checked = defaultdict(int)   # PRs in week with copilot_trailer set

    DOTNET_AGGREGATE_REPOS = {"dotnet/runtime", "dotnet/roslyn", "dotnet/maui", "microsoft/aspire"}
    aggregate_repos = [r for r in DOTNET_AGGREGATE_REPOS if r in repos_with_coverage]

    for repo in aggregate_repos:
        items = all_items.get(repo) or []
        for item in items:
            if not item["is_pr"]:
                continue
            cd = parse_date(item["created_at"])
            if not cd:
                continue
            week = cd - timedelta(days=cd.weekday())
            week_total[week] += 1
            if item.get("copilot_trailer") is not None:
                week_checked[week] += 1
            cls = _classify_copilot(item)
            if cls == "cca":
                cca_total[week] += 1
            elif cls == "assisted":
                assisted_total[week] += 1

    # Drop weeks where < 90% of PRs have been trailer-checked (data lag censoring)
    MIN_WEEK_COVERAGE = 0.90
    all_weeks = sorted(set(list(cca_total.keys()) + list(assisted_total.keys())))
    all_weeks = [w for w in all_weeks if w >= coverage_cutoff]
    if common_end_week:
        all_weeks = [w for w in all_weeks if w <= common_end_week]
    all_weeks = [w for w in all_weeks
                 if week_total.get(w, 0) > 0 and
                 week_checked[w] / week_total[w] >= MIN_WEEK_COVERAGE]
    if all_weeks and (today - all_weeks[-1]).days < 7:
        all_weeks = all_weeks[:-1]

    if all_weeks:
        cca_vals = smooth([cca_total.get(w, 0) for w in all_weeks], 4)
        assisted_vals = smooth([assisted_total.get(w, 0) for w in all_weeks], 4)
        ax.plot(all_weeks, cca_vals, color="#e74c3c", label="CCA (bot-authored)",
                linewidth=2.5, alpha=0.9)
        ax.plot(all_weeks, assisted_vals, color="#3498db", label="Assisted (Co-authored-by trailer)",
                linewidth=2.5, alpha=0.9)
        ax.set_ylim(0, 200)
        # Use the actual data range for x-axis (avoid massive forward padding)
        ax.set_xlim(all_weeks[0], all_weeks[-1])
        ax.legend(loc="upper left", fontsize=10)
        add_direction_arrow(ax, "up")
        repos_str = ", ".join(get_short(r) for r in sorted(aggregate_repos))
        add_insight_box(ax, [
            f"Total weekly PR count across {len(aggregate_repos)} dotnet repos: {repos_str}",
            "Useful for spotting whether dotnet Copilot usage is dominated by CCA or human-assisted",
            "Other repos (vscode, vcpkg, rust, go, pyright) excluded to keep focus on the dotnet stack",
            "Weeks with <90% per-week trailer-check coverage dropped (data-collection lag)",
        ], loc="lower right")
    fig.tight_layout()
    path = os.path.join(output_dir, "copilot_by_type_aggregate.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_copilot_merge_success(all_items, output_dir):
    """Line chart: monthly merge rate for Copilot vs human PRs (runtime only)."""
    from datetime import date as _date
    today = _date.today()
    coverage_cutoff = today - timedelta(days=365)
    MIN_RESOLVED_MONTH = 10  # minimum resolved PRs per month to plot a point

    repo = "dotnet/runtime"
    if repo not in all_items:
        print("  (skipping copilot merge rate trend — runtime not in data)")
        return

    items = all_items[repo]

    # Bucket by creation month, split Copilot (CCA+assisted) vs Human
    # Track total PRs (including open) to compute resolution rate for censoring check
    monthly = defaultdict(lambda: {"cca_m": 0, "cca_r": 0, "cca_open": 0,
                                    "asst_m": 0, "asst_r": 0, "asst_open": 0,
                                    "hum_m": 0, "hum_r": 0, "hum_open": 0})
    for item in items:
        if not item["is_pr"]:
            continue
        cd = parse_date(item["created_at"])
        if not cd or cd < coverage_cutoff:
            continue
        cls = _classify_copilot(item)
        if cls == "unknown":
            continue
        if cls == "cca":
            pfx = "cca"
        elif cls == "assisted":
            pfx = "asst"
        else:
            pfx = "hum"
        month = cd.replace(day=1)
        if item.get("merged_at"):
            monthly[month][pfx + "_m"] += 1
        elif item.get("state") == "CLOSED":
            monthly[month][pfx + "_r"] += 1
        else:
            monthly[month][pfx + "_open"] += 1

    months = sorted(monthly.keys())
    # Drop current month if incomplete (< 14 days in)
    if months and (today - months[-1]).days < 14:
        months = months[:-1]
    # Drop months where < 90% of PRs are resolved (right-censoring bias)
    MIN_RESOLUTION_RATE = 0.75
    def _resolved(d):
        return d["cca_m"] + d["cca_r"] + d["asst_m"] + d["asst_r"] + d["hum_m"] + d["hum_r"]
    def _all(d):
        return _resolved(d) + d["cca_open"] + d["asst_open"] + d["hum_open"]
    months = [m for m in months if _resolved(monthly[m]) / max(1, _all(monthly[m])) >= MIN_RESOLUTION_RATE]
    if not months:
        print("  (skipping copilot merge rate trend — no data)")
        return

    cca_rates, asst_rates, hum_rates = [], [], []
    cca_ns, asst_ns, hum_ns = [], [], []
    for m in months:
        d = monthly[m]
        cca_r = d["cca_m"] + d["cca_r"]
        asst_r = d["asst_m"] + d["asst_r"]
        hum_r = d["hum_m"] + d["hum_r"]
        cca_rates.append(100 * d["cca_m"] / cca_r if cca_r >= MIN_RESOLVED_MONTH else float("nan"))
        asst_rates.append(100 * d["asst_m"] / asst_r if asst_r >= MIN_RESOLVED_MONTH else float("nan"))
        hum_rates.append(100 * d["hum_m"] / hum_r if hum_r >= MIN_RESOLVED_MONTH else float("nan"))
        cca_ns.append(cca_r)
        asst_ns.append(asst_r)
        hum_ns.append(hum_r)

    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "PR Merge Rate: Copilot vs Human — dotnet/runtime (Monthly)", "Merge Rate (%)")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}%"))
    # Override default year-based ticks — our range is ~12 months
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())
    ax.xaxis.set_minor_formatter(mdates.DateFormatter(""))

    ax.plot(months, hum_rates, color="#3498db", label="Human",
            linewidth=2.5, alpha=0.9, marker="s", markersize=4)
    ax.plot(months, cca_rates, color="#e74c3c", label="CCA (bot-authored)",
            linewidth=2.5, alpha=0.9, marker="o", markersize=5)
    ax.plot(months, asst_rates, color="#9b59b6", label="Assisted (Co-authored-by)",
            linewidth=2.5, alpha=0.9, marker="^", markersize=5)

    ax.set_ylim(0, 109)
    ax.legend(loc="lower right", fontsize=11)
    add_insight_box(ax, [
        "Each point = merge rate of PRs created that month (merged / resolved)",
        "Months with <75% resolution rate excluded; recent months may show inflated merge rates as still-open PRs eventually merge or close to avoid censoring bias",
        "CCA = bot-authored; Assisted = human + Co-authored-by trailer; Human = no trailer",
        "Trailer is a lower-bound proxy — not all Copilot use leaves a trailer",
    ])
    _pad_date_xlim(fig)
    fig.tight_layout()
    path = os.path.join(output_dir, "copilot_merge_success.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def _chart_copilot_time_to_merge_for_repo(all_items, output_dir, repo, output_file):
    """Line chart: monthly median TTM for Copilot vs human PRs for one repo."""
    from datetime import date as _date
    today = _date.today()
    coverage_cutoff = today - timedelta(days=365)
    MIN_MERGED_MONTH = 10  # minimum merged PRs per month to plot a point

    if repo not in all_items:
        print(f"  (skipping copilot TTM trend — {repo} not in data)")
        return

    items = all_items[repo]

    # Bucket by creation month, collect TTM values and total PR counts for censoring
    monthly_ttm = defaultdict(lambda: {"cca": [], "asst": [], "hum": []})
    monthly_resolved = defaultdict(lambda: {"cca": 0, "asst": 0, "hum": 0})
    monthly_total = defaultdict(lambda: {"cca": 0, "asst": 0, "hum": 0})
    for item in items:
        if not item["is_pr"]:
            continue
        cd = parse_date(item["created_at"])
        if not cd or cd < coverage_cutoff:
            continue
        cls = _classify_copilot(item)
        if cls == "unknown":
            continue
        if cls == "cca":
            key = "cca"
        elif cls == "assisted":
            key = "asst"
        else:
            key = "hum"
        month = cd.replace(day=1)
        monthly_total[month][key] += 1
        if item.get("merged_at"):
            monthly_resolved[month][key] += 1
            try:
                created_dt = datetime.fromisoformat(item["created_at"].replace("Z", "+00:00"))
                merged_dt = datetime.fromisoformat(item["merged_at"].replace("Z", "+00:00"))
                days = (merged_dt - created_dt).total_seconds() / 86400
                if days >= 0:
                    monthly_ttm[month][key].append(days)
            except (ValueError, AttributeError):
                pass
        elif item.get("state") == "CLOSED":
            monthly_resolved[month][key] += 1

    months = sorted(monthly_total.keys())
    if months and (today - months[-1]).days < 14:
        months = months[:-1]
    # Drop months where < 90% of PRs are resolved (merged or closed) to avoid survivorship bias
    MIN_RESOLUTION_RATE = 0.75
    def _resolved(d):
        return d["cca"] + d["asst"] + d["hum"]
    months = [m for m in months if (
        _resolved(monthly_resolved[m]) / max(1, _resolved(monthly_total[m]))
    ) >= MIN_RESOLUTION_RATE]
    if not months:
        print("  (skipping copilot TTM trend — no data)")
        return

    import numpy as np
    cca_p50, asst_p50, hum_p50 = [], [], []
    cca_ns, asst_ns, hum_ns = [], [], []
    # Use a lower MIN for the smaller-N copilot lines so they stay visible when data is thin
    MIN_CCA_ASST = 3
    for m in months:
        d = monthly_ttm[m]
        for key, p50_list, n_list, min_n in [
            ("cca", cca_p50, cca_ns, MIN_CCA_ASST),
            ("asst", asst_p50, asst_ns, MIN_CCA_ASST),
            ("hum", hum_p50, hum_ns, MIN_MERGED_MONTH),
        ]:
            vals = sorted(d[key])
            n_list.append(len(vals))
            if len(vals) >= min_n:
                p50_list.append(float(np.median(vals)))
            else:
                p50_list.append(float("nan"))

    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, f"Median Time-to-Merge: Copilot vs Human — {repo} (Monthly)", "Days")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.1f}"))
    ax.yaxis.set_major_locator(MaxNLocator(nbins=8))
    # Override default year-based ticks — our range is ~12 months
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())
    ax.xaxis.set_minor_formatter(mdates.DateFormatter(""))

    ax.plot(months, hum_p50, color="#3498db", label="Human p50",
            linewidth=2.5, alpha=0.9, marker="s", markersize=4)
    ax.plot(months, cca_p50, color="#e74c3c", label="CCA (bot-authored) p50",
            linewidth=2.5, alpha=0.9, marker="o", markersize=5)
    ax.plot(months, asst_p50, color="#9b59b6", label="Assisted (Co-authored-by) p50",
            linewidth=2.5, alpha=0.9, marker="^", markersize=5)

    ax.set_ylim(0, None)
    ax.legend(loc="upper left", fontsize=10)
    add_insight_box(ax, [
        "Each point = median TTM of PRs created that month (merged PRs only)",
        "Months with <75% resolution rate excluded; recent months may be slightly biased as still-open PRs continue to resolve",
        "CCA = bot-authored; Assisted = human + Co-authored-by trailer; Human = no trailer",
        "Min sample size: 3 PRs for CCA/Assisted (small populations); 10 for Human",
    ])
    _pad_date_xlim(fig)
    fig.tight_layout()
    path = os.path.join(output_dir, output_file)
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_copilot_time_to_merge(all_items, output_dir):
    """Line chart: monthly median TTM for Copilot vs human PRs (runtime only)."""
    _chart_copilot_time_to_merge_for_repo(
        all_items,
        output_dir,
        "dotnet/runtime",
        "copilot_time_to_merge.png",
    )


def chart_copilot_time_to_merge_azure(all_items, output_dir):
    """Line chart: monthly median TTM for Copilot vs human PRs (Azure repo only)."""
    _chart_copilot_time_to_merge_for_repo(
        all_items,
        output_dir,
        "Azure/azure-sdk-for-js",
        "copilot_time_to_merge_azure.png",
    )


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
        "vscode closes ~68% within 30 days — bot-assisted triage",
        "go historically most responsive — small focused team",
        "runtime starts at 2020 (pre-merge data unreliable) — has held\n  steady at ~40-50%, respectable for its issue volume",
    ])
    if LINEAGE_CUTOFF:
        cutoff_names = ", ".join(get_short(r) for r in LINEAGE_CUTOFF)
        ax.annotate(f"Note: {cutoff_names} shown from 2020 (pre-merge close dates unreliable)",
                    xy=(0.02, 0.02), xycoords="axes fraction", fontsize=8,
                    color="#888888", style="italic")
    _pad_date_xlim(fig)
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
        "runtime holding steady despite fewer maintainers",
        "aspire and maui recently declining — team bandwidth pressure?",
        "Lower than overall turnaround — team issues always get faster triage",
    ])
    _pad_date_xlim(fig)
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
    ymax = min(ymax, 400)          # crop to 400 days max
    ax.set_ylim(ymin, ymax)
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "down")
    add_insight_box(ax, [
        "roslyn has long-tail close times driven by enhancement requests",
    ])
    _pad_date_xlim(fig)
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
    ])
    _pad_date_xlim(fig)
    fig.tight_layout()
    path = os.path.join(output_dir, "community_issue_age.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_community_pareto(all_items, output_dir):
    """Community PR contribution concentration curve per repo."""
    import numpy as np
    fig, ax = plt.subplots(figsize=(14, 7))
    ax.set_title("Community PR Concentration", fontsize=14, fontweight="bold", pad=12)
    _stamp_chart(ax, "Community PR Concentration")
    ax.set_xlabel("Cumulative % of Community Authors (ranked by PR count)", fontsize=10)
    ax.set_ylabel("Cumulative % of Merged PRs", fontsize=10)
    ax.grid(True, alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    gini_labels = []
    for repo, items in all_items.items():
        if repo in GERRIT_REPOS:
            continue
        maintainers = set()
        for item in items:
            if item["is_pr"] and item.get("merged_by"):
                maintainers.add(item["merged_by"])
        maintainers |= BOT_ACCOUNTS

        # Count merged PRs per community author
        author_counts = defaultdict(int)
        for item in items:
            if not item["is_pr"] or not item.get("merged_at"):
                continue
            author = effective_author(item)
            if not author or author in maintainers:
                continue
            author_counts[author] += 1
        if len(author_counts) < 10:
            continue

        # Sort ascending for Lorenz curve
        vals = sorted(author_counts.values())
        n = len(vals)
        cum = np.cumsum(vals)
        total = cum[-1]
        x = np.arange(1, n + 1) / n * 100
        y = cum / total * 100

        # Prepend origin
        x = np.insert(x, 0, 0)
        y = np.insert(y, 0, 0)

        # Gini coefficient
        gini = (2 * sum((i+1)*v for i,v in enumerate(vals)) - (n+1)*sum(vals)) / (n * sum(vals))

        # Top 10% share
        top10_n = max(1, n // 10)
        top10_prs = sum(vals[-top10_n:])
        top10_pct = 100.0 * top10_prs / total

        short = get_short(repo)
        ax.plot(x, y, color=get_color(repo), label=f"{short} (Gini={gini:.2f})",
                linewidth=1.8, alpha=0.85)
        gini_labels.append((short, gini, n, len([v for v in vals if v == 1]), top10_pct))

    # Perfect equality line
    ax.plot([0, 100], [0, 100], 'k--', alpha=0.3, linewidth=1, label="Perfect equality")
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.legend(loc="upper left", fontsize=9)

    # Insight
    insights = []
    dotnet_repos = [g for g in gini_labels if g[0] in ("runtime","roslyn","maui","aspire")]
    if dotnet_repos:
        avg_gini = sum(g[1] for g in dotnet_repos) / len(dotnet_repos)
        avg_onetime = sum(g[3]/g[2] for g in dotnet_repos) / len(dotnet_repos)
        runtime_entry = [g for g in dotnet_repos if g[0] == "runtime"]
        if runtime_entry:
            r = runtime_entry[0]
            top10_n = max(1, r[2] // 10)
            insights.append(f"runtime: top 10% ({top10_n} people) produced {r[4]:.0f}% of all community merged PRs")
        insights.append(f"~{avg_onetime*100:.0f}% of community authors across dotnet repos make exactly 1 PR")
    vscode = [g for g in gini_labels if g[0] == "vscode"]
    if vscode:
        insights.append(f"vscode is most egalitarian (Gini={vscode[0][1]:.2f}) — broader but shallower community, perhaps fixing their own pet issue?")
    add_insight_box(ax, insights)

    _pad_date_xlim(fig)
    fig.tight_layout()
    path = os.path.join(output_dir, "community_pareto.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_community_retention(all_items, output_dir):
    """First-time contributor retention: % who return for a 2nd merged PR within 12 months."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "First-Time Community Contributor Retention (% returning within 12 months)", "%")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}%"))

    visible_data = []
    line_ends = []
    has_data = False
    for repo, items in all_items.items():
        if repo in GERRIT_REPOS:
            continue
        maintainers = set()
        for item in items:
            if item["is_pr"] and item.get("merged_by"):
                maintainers.add(item["merged_by"])
        maintainers |= BOT_ACCOUNTS

        # Build per-author merged PR date lists
        author_dates = defaultdict(list)
        for item in items:
            if not item["is_pr"] or not item.get("merged_at"):
                continue
            author = effective_author(item)
            if not author or author in maintainers:
                continue
            md = parse_date(item["merged_at"])
            if md:
                author_dates[author].append(md)
        for a in author_dates:
            author_dates[a].sort()

        # Group by quarter cohort
        from collections import Counter
        cohorts = defaultdict(lambda: {"total": 0, "returned": 0})
        for author, dates in author_dates.items():
            first = dates[0]
            q = (first.month - 1) // 3 + 1
            key = (first.year, q)
            if first.year < 2019 or first.year > 2024:
                continue
            cohorts[key]["total"] += 1
            if len(dates) >= 2:
                days_to_second = (dates[1] - first).days
                if days_to_second <= 365:
                    cohorts[key]["returned"] += 1

        if len(cohorts) < 4:
            continue

        sorted_keys = sorted(cohorts.keys())
        from datetime import date
        # Place each quarter at its midpoint
        q_months = {1: 2, 2: 5, 3: 8, 4: 11}
        plot_dates = [date(y, q_months[q], 15) for y, q in sorted_keys]
        rates = [100.0 * cohorts[k]["returned"] / max(1, cohorts[k]["total"]) for k in sorted_keys]

        # Skip last cohorts if too recent (< 12 months to observe)
        cutoff = datetime.now().date() - timedelta(days=365)
        while plot_dates and plot_dates[-1] > cutoff:
            plot_dates.pop()
            rates.pop()
        if len(rates) < 4:
            continue

        s = smooth(rates, 4)
        ax.plot(plot_dates, s, color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(s)
        line_ends.append((plot_dates, s, get_short(repo), get_color(repo)))
        has_data = True

    if not has_data:
        plt.close(fig)
        print("  (skipping community retention — no data)")
        return

    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(max(0, ymin), min(100, ymax))
    ax.legend(loc="upper right", fontsize=10)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "up")
    add_insight_box(ax, [
        "runtime retention dropped from 40% to 22% (2020→2024) — fewer first-timers come back",
        "rust retains best (~33-44%), possibly due to mentoring programs and automated tooling?",
        "vscode lowest (~20%) — large drive-by contributor pool, few repeat",
    ])
    _pad_date_xlim(fig)
    fig.tight_layout()
    path = os.path.join(output_dir, "community_retention.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_community_merge_latency(all_items, output_dir):
    """Median days-to-merge for community vs maintainer PRs, rolling 6-month window."""
    fig, axes = plt.subplots(2, 1, figsize=(14, 10), sharex=True)
    ax_comm = axes[0]
    ax_ratio = axes[1]

    setup_axes(ax_comm, "Community PR Merge Latency (median days, 6-month rolling)", "Days")
    ax_comm.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}"))
    setup_axes(ax_ratio, "Community vs Maintainer Merge Speed (ratio, lower = more equal)", "Ratio (community ÷ maintainer)")
    ax_ratio.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.1f}×"))

    visible_comm = []
    visible_ratio = []
    line_ends_comm = []
    line_ends_ratio = []
    has_data = False
    for repo, items in all_items.items():
        if repo in GERRIT_REPOS:
            continue
        maintainers = set()
        for item in items:
            if item["is_pr"] and item.get("merged_by"):
                maintainers.add(item["merged_by"])
        maintainers |= BOT_ACCOUNTS

        # Collect merge latencies by month
        from statistics import median
        comm_by_month = defaultdict(list)
        maint_by_month = defaultdict(list)
        for item in items:
            if not item["is_pr"] or not item.get("merged_at"):
                continue
            cd = parse_date(item["created_at"])
            md = parse_date(item["merged_at"])
            if not cd or not md:
                continue
            days = (md - cd).days
            if days < 0 or days > 365:
                continue
            month_key = cd.replace(day=1)
            author = effective_author(item)
            if not author:
                continue
            if author in maintainers:
                maint_by_month[month_key].append(days)
            else:
                comm_by_month[month_key].append(days)

        # Build rolling 6-month medians
        all_months = sorted(set(comm_by_month.keys()) | set(maint_by_month.keys()))
        if len(all_months) < 12:
            continue

        plot_months = []
        comm_medians = []
        ratio_vals = []
        for i, m in enumerate(all_months):
            # 6-month lookback
            window_months = [am for am in all_months[max(0,i-5):i+1]]
            c_vals = []
            m_vals = []
            for wm in window_months:
                c_vals.extend(comm_by_month.get(wm, []))
                m_vals.extend(maint_by_month.get(wm, []))
            if len(c_vals) >= 10 and len(m_vals) >= 10:
                c_med = median(c_vals)
                m_med = median(m_vals)
                plot_months.append(m)
                comm_medians.append(c_med)
                ratio_vals.append(c_med / max(0.5, m_med))

        if len(plot_months) < 6:
            continue

        s_comm = smooth(comm_medians, 4)
        s_ratio = smooth(ratio_vals, 4)
        short = get_short(repo)
        color = get_color(repo)
        ax_comm.plot(plot_months, s_comm, color=color, label=short,
                     linewidth=1.5, alpha=0.85)
        ax_ratio.plot(plot_months, s_ratio, color=color, label=short,
                      linewidth=1.5, alpha=0.85)
        visible_comm.append(s_comm)
        visible_ratio.append(s_ratio)
        line_ends_comm.append((plot_months, s_comm, short, color))
        line_ends_ratio.append((plot_months, s_ratio, short, color))
        has_data = True

    if not has_data:
        plt.close(fig)
        print("  (skipping community merge latency — no data)")
        return

    ymin_c, ymax_c = robust_ylim(visible_comm)
    ax_comm.set_ylim(max(0, ymin_c), ymax_c)
    ax_comm.legend(loc="upper right", fontsize=9)
    label_line_ends(ax_comm, line_ends_comm)
    add_direction_arrow(ax_comm, "down")

    ymin_r, ymax_r = robust_ylim(visible_ratio)
    ax_ratio.set_ylim(max(0.5, ymin_r), min(ymax_r, 20))
    ax_ratio.axhline(y=1.0, color='gray', linestyle='--', alpha=0.5, linewidth=1)
    ax_ratio.legend(loc="upper right", fontsize=9)
    label_line_ends(ax_ratio, line_ends_ratio)

    add_insight_box(ax_comm, [
        "maui community PRs wait ~8 days median vs <1 day for maintainers — 8× gap",
        "rust achieves near-parity — large reviewer pool and explicit reviewer assignment via rustbot",
        "Long merge latency potentially drives the retention drop in maui and runtime — first-timers don't wait?",
    ])
    add_insight_box(ax_ratio, [
        "Maintainer PRs are generally merged faster than community PRs across all repos",
        "Possible factors: longer wait for first review, more revision cycles, less familiarity with codebase?",
    ])

    _pad_date_xlim(fig)
    fig.tight_layout()
    path = os.path.join(output_dir, "community_merge_latency.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_gini_over_time(all_items, output_dir):
    """Gini coefficient of community PR concentration over time (half-year buckets)."""
    import numpy as np
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Community PR Concentration Over Time (Gini Coefficient, 6-month windows)", "Gini Coefficient")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.2f}"))

    visible_data = []
    line_ends = []
    has_data = False
    for repo, items in all_items.items():
        if repo in GERRIT_REPOS:
            continue
        maintainers = set()
        for item in items:
            if item["is_pr"] and item.get("merged_by"):
                maintainers.add(item["merged_by"])
        maintainers |= BOT_ACCOUNTS

        # Count PRs per community author per half-year
        from datetime import date
        half_authors = defaultdict(lambda: defaultdict(int))
        for item in items:
            if not item["is_pr"] or not item.get("merged_at"):
                continue
            author = effective_author(item)
            if not author or author in maintainers:
                continue
            md = parse_date(item["merged_at"])
            if not md or md.year < 2018:
                continue
            half = 1 if md.month <= 6 else 2
            half_authors[(md.year, half)][author] += 1

        plot_dates = []
        ginis = []
        for key in sorted(half_authors):
            y, h = key
            vals = sorted(half_authors[key].values())
            n = len(vals)
            if n < 10:
                continue
            gini = (2 * sum((i+1)*v for i,v in enumerate(vals)) - (n+1)*sum(vals)) / (n * sum(vals))
            plot_dates.append(date(y, 4 if h == 1 else 10, 1))
            ginis.append(gini)

        if len(plot_dates) < 4:
            continue

        short = get_short(repo)
        color = get_color(repo)
        ax.plot(plot_dates, ginis, color=color, label=short,
                linewidth=1.5, alpha=0.85)
        visible_data.append(ginis)
        line_ends.append((plot_dates, ginis, short, color))
        has_data = True

    if not has_data:
        plt.close(fig)
        print("  (skipping Gini over time — no data)")
        return

    ax.set_ylim(0.2, 1.0)
    ax.legend(loc="lower left", fontsize=10)
    label_line_ends(ax, line_ends)
    # Custom arrow — higher Gini means fewer people do more work (not clearly better/worse)
    ax.annotate("", xy=(0.06, 0.55), xytext=(0.06, 0.35), xycoords="axes fraction",
                arrowprops=dict(arrowstyle="-|>,head_width=0.6,head_length=0.4",
                                color="black", lw=3))
    ax.text(0.06, 0.57, "Fewer people\ndo more work", transform=ax.transAxes, fontsize=8,
            ha="center", va="bottom", color="black", style="italic")
    add_insight_box(ax, [
        "maui spiked 0.40→0.64 in 2024 (Syncfusion partnership effect)",
        "vscode stays low (~0.40) — most evenly distributed community",
        "runtime stable ~0.67 — concentrated but not worsening",
    ])
    _pad_date_xlim(fig)
    fig.tight_layout()
    path = os.path.join(output_dir, "community_gini.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


# ── Zoomed Charts (120-day x-axis) ──────────────────────────────────────────


def chart_pr_merge_rate_zoomed(all_items, output_dir):
    """PR merge rate over last 120 days, daily granularity with 7-day rolling sum."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "PR Merge Rate — last 4 months (7-day trailing sum)", "PRs Merged / Week (7-day rolling sum)")

    today = datetime.now().date()
    cutoff = today - timedelta(days=120)
    # Need 6 days of pre-window history so the first plotted point has a full 7-day window
    fetch_start = cutoff - timedelta(days=6)
    # Drop today (incomplete) to avoid a false trailing dip.  Also cap at the
    # latest merged_at date actually present in the data: when the database
    # hasn't been refreshed recently the tail of the window would otherwise be
    # all-zeros, producing an artificial "dropping in May"-style cliff.
    _latest_merge = max(
        (parse_date(it.get("merged_at"))
         for items in all_items.values()
         for it in items
         if it.get("is_pr") and it.get("merged_at")),
        default=None,
    )
    last_day = min(today - timedelta(days=1), _latest_merge) if _latest_merge else today - timedelta(days=1)

    visible_data = []
    line_ends = []
    for repo, items in all_items.items():
        if not items or repo in GERRIT_REPOS:
            continue
        # Daily merged counts within the (extended) window
        daily = defaultdict(int)
        for it in items:
            if not it.get("is_pr"):
                continue
            md = parse_date(it.get("merged_at"))
            if md and fetch_start <= md <= last_day:
                daily[md] += 1
        if not daily:
            continue
        # Build daily series and compute 7-day trailing sum at each day in [cutoff, last_day]
        days = []
        rolling = []
        d = cutoff
        while d <= last_day:
            s = sum(daily.get(d - timedelta(days=k), 0) for k in range(7))
            days.append(d)
            rolling.append(s)
            d += timedelta(days=1)
        if not any(rolling):
            continue
        ax.plot(days, rolling,
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(rolling)
        line_ends.append((days, rolling, get_short(repo), get_color(repo)))

    if not visible_data:
        plt.close(fig)
        return

    ymin, ymax = robust_ylim(visible_data, percentile=0.99)
    ax.set_ylim(ymin, max(ymax, 50))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    ax.xaxis.set_minor_locator(mdates.WeekdayLocator(byweekday=0))
    ax.xaxis.set_minor_formatter(mdates.DateFormatter(""))
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "up")
    add_insight_box(ax, [
        "Zoomed view of the long-term merge rate chart — last 4 months at daily resolution",
        "Useful for spotting recent dips/spikes (releases, on-call rotations, holiday slowdowns)",
        "Each point = PRs merged in the trailing 7 days, evaluated daily",
        "Chart ends at the latest merged date in the DB — refresh the DB to extend the window",
        "vscode, rust, runtime, vcpkg, roslyn relatively flat; maui and aspire trending down (−73% and −42% from peak)",
    ])
    _pad_date_xlim(fig)
    fig.tight_layout()
    path = os.path.join(output_dir, "pr_merge_rate_zoomed.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_pr_opened_vs_merged_zoomed(all_items, output_dir):
    """Net PR Flow (Opened - Merged) over last 120 days, lightly smoothed."""
    today = datetime.now().date()
    cutoff = today - timedelta(days=120)
    fetch_start = cutoff - timedelta(days=6)
    # Drop today (incomplete) to avoid a false trailing dip.  Also cap at the
    # latest date actually present in the data (max of created_at / merged_at)
    # so a stale database doesn't produce an artificial cliff of all-zeros.
    _latest_date = max(
        (
            max(d for d in (parse_date(it.get("created_at")), parse_date(it.get("merged_at"))) if d is not None)
            for items in all_items.values()
            for it in items
            if it.get("is_pr") and (it.get("created_at") or it.get("merged_at"))
        ),
        default=None,
    )
    last_day = min(today - timedelta(days=1), _latest_date) if _latest_date else today - timedelta(days=1)

    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Net PR Flow — last 4 months (7-day trailing sum, lightly smoothed)",
               "Opened − Merged / Week (7-day rolling sum, smoothed)")

    ax.axhline(y=0, color="black", linewidth=0.5, alpha=0.5)

    visible_data = []
    line_ends = []
    for repo, items in all_items.items():
        if not items or repo in GERRIT_REPOS:
            continue
        opened = defaultdict(int)
        merged = defaultdict(int)
        for it in items:
            if not it.get("is_pr"):
                continue
            cd = parse_date(it.get("created_at"))
            if cd and fetch_start <= cd <= last_day:
                opened[cd] += 1
            md = parse_date(it.get("merged_at"))
            if md and fetch_start <= md <= last_day:
                merged[md] += 1
        if not opened and not merged:
            continue
        days, rolling = [], []
        d = cutoff
        while d <= last_day:
            o = sum(opened.get(d - timedelta(days=k), 0) for k in range(7))
            m = sum(merged.get(d - timedelta(days=k), 0) for k in range(7))
            days.append(d)
            rolling.append(o - m)
            d += timedelta(days=1)
        smoothed = smooth(rolling, window=5)
        ax.plot(days, smoothed,
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(smoothed)
        line_ends.append((days, smoothed, get_short(repo), get_color(repo)))

    if not visible_data:
        plt.close(fig)
        return

    ax.set_ylim(-100, 250)
    ax.yaxis.set_major_locator(MultipleLocator(50))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    ax.xaxis.set_minor_locator(mdates.WeekdayLocator(byweekday=0))
    ax.xaxis.set_minor_formatter(mdates.DateFormatter(""))
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "down")
    add_insight_box(ax, [
        "Zoomed view of net PR flow — last 4 months at daily resolution",
        "Spikes upward = PRs piling up; downward = team draining backlog",
        "Each point = (opened − merged) in the trailing 7 days, lightly smoothed",
        "vscode and rust show large positive swings (backlog churn); maui swings sharply both directions",
    ])
    _pad_date_xlim(fig)
    fig.tight_layout()
    path = os.path.join(output_dir, "pr_opened_vs_merged_zoomed.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


# ── Comment-Based Charts ────────────────────────────────────────────────────

# Repos to include in comment-based charts
COMMENT_CHART_REPOS = (
    "dotnet/runtime", "dotnet/roslyn", "dotnet/maui",
    "microsoft/aspire", "Azure/azure-sdk-for-js",
)


def load_first_comments(conn, repo):
    """Load first-comment data from pr_first_comment table.
    Returns dict: PR number -> {'first_comment_at': date, 'first_comment_dt': datetime, 'commenter': str}
    """
    try:
        rows = conn.execute(
            "SELECT number, first_comment_at, commenter FROM pr_first_comment WHERE repo = ?",
            (repo,)
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    result = {}
    for number, fc_at, commenter in rows:
        d = parse_date(fc_at)
        dt = None
        try:
            if fc_at:
                dt = datetime.fromisoformat(fc_at.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            dt = None
        if d:
            result[number] = {"first_comment_at": d, "first_comment_dt": dt, "commenter": commenter}
    return result


def chart_time_to_comment(all_items, all_first_comments, output_dir):
    """P50 time-to-first-comment in HOURS, weekly with 4-week rolling window, multi-repo, 12-month x-axis."""
    from statistics import median
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Time to First Comment — P50 (weekly, 4-week rolling)", "Hours")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.1f}"))
    ax.yaxis.set_major_locator(MaxNLocator(nbins=8))

    today = datetime.now().date()
    cutoff = today - timedelta(days=365)
    last_complete_week = week_start(today) - timedelta(weeks=1)

    visible_data = []
    line_ends = []
    for repo in COMMENT_CHART_REPOS:
        items = all_items.get(repo)
        fc_map = all_first_comments.get(repo)
        if not items or not fc_map:
            continue
        # Bucket TTC values (in hours, sub-hour precision) by PR-creation week
        ttc_by_week = defaultdict(list)
        for item in items:
            if not item.get("is_pr"):
                continue
            fc = fc_map.get(item["number"])
            if not fc:
                continue
            cd = parse_date(item["created_at"])
            if not cd:
                continue
            fc_dt = fc.get("first_comment_dt")
            try:
                created_dt = datetime.fromisoformat(item["created_at"].replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                continue
            if not fc_dt:
                continue
            # Skip comments that arrived after merge (not review latency)
            try:
                md_dt = datetime.fromisoformat(item["merged_at"].replace("Z", "+00:00")) if item.get("merged_at") else None
            except (ValueError, AttributeError):
                md_dt = None
            if md_dt and fc_dt > md_dt:
                continue
            hours = (fc_dt - created_dt).total_seconds() / 3600
            if hours < 0:
                continue
            ttc_by_week[week_start(cd)].append(hours)
        if not ttc_by_week:
            continue
        # Evaluate weekly with 4-week trailing window
        weeks_x, p50s = [], []
        w = max(min(ttc_by_week.keys()), cutoff)
        w = week_start(w)
        while w <= last_complete_week:
            window_vals = []
            for k in range(4):
                window_vals.extend(ttc_by_week.get(w - timedelta(weeks=k), []))
            if len(window_vals) >= 3:
                weeks_x.append(w)
                p50s.append(median(window_vals))
            w += timedelta(weeks=1)
        if not weeks_x:
            continue
        ax.plot(weeks_x, p50s,
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)
        visible_data.append(p50s)
        line_ends.append((weeks_x, p50s, get_short(repo), get_color(repo)))

    if not visible_data:
        plt.close(fig)
        print("  (skipping time-to-comment — no comment data)")
        return

    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(0, ymax)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "down")
    add_insight_box(ax, [
        "How quickly a PR gets human attention — proxy for review responsiveness",
        "P50 = median hours from PR creation to first non-author, non-bot comment",
        "Lower = more responsive review culture; spikes often align with holidays/releases",
        "Includes all PRs (not just merged); excludes post-merge comments",
        "Review responsiveness roughly stable over last 4 months for aspire and runtime",
    ])
    _pad_date_xlim(fig)
    fig.tight_layout()
    path = os.path.join(output_dir, "time_to_comment.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_copilot_time_to_comment(all_items, all_first_comments, output_dir):
    """Per-repo P50 delta (Copilot − Human) for hours-to-first-comment, weekly 4-week rolling."""
    import numpy as np
    today = datetime.now().date()
    coverage_cutoff = today - timedelta(days=365)
    WINDOW_DAYS = 28
    MIN_PRS = 3  # required in BOTH cohorts before computing delta

    # Per-repo data: repo -> list of (create_date, hours, is_copilot)
    per_repo = {r: [] for r in COMMENT_CHART_REPOS}
    for repo in COMMENT_CHART_REPOS:
        items = all_items.get(repo)
        fc_map = all_first_comments.get(repo)
        if not items or not fc_map:
            continue
        for item in items:
            if not item["is_pr"]:
                continue
            cd = parse_date(item["created_at"])
            if not cd or cd < coverage_cutoff:
                continue
            cls = _classify_copilot(item)
            if cls == "unknown":
                continue
            fc = fc_map.get(item["number"])
            if not fc:
                continue
            fc_dt = fc.get("first_comment_dt")
            try:
                created_dt = datetime.fromisoformat(item["created_at"].replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                continue
            if not fc_dt:
                continue
            try:
                md_dt = datetime.fromisoformat(item["merged_at"].replace("Z", "+00:00")) if item.get("merged_at") else None
            except (ValueError, AttributeError):
                md_dt = None
            if md_dt and fc_dt > md_dt:
                continue
            hours = (fc_dt - created_dt).total_seconds() / 3600
            if hours < 0:
                continue
            per_repo[repo].append((cd, hours, cls in ("cca", "assisted")))

    # Weekly evaluation points
    weeks = []
    w = week_start(coverage_cutoff) + timedelta(weeks=4)
    last_full = week_start(today) - timedelta(weeks=1)
    while w <= last_full:
        weeks.append(w)
        w += timedelta(weeks=1)

    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Time to First Comment Delta (Human − Copilot, positive = copilot sooner) — per repo (4-week rolling P50)", "Hours")
    ax.axhline(y=0, color="black", linewidth=0.7, alpha=0.6)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())

    visible_data = []
    line_ends = []
    repos_with_data = []
    for repo in sorted(COMMENT_CHART_REPOS):
        dp = per_repo.get(repo) or []
        if not dp:
            continue
        deltas = []
        for wk in weeks:
            window_start = wk - timedelta(days=WINDOW_DAYS)
            cop_vals = [h for cd, h, is_cop in dp if is_cop and window_start <= cd < wk + timedelta(days=7)]
            hum_vals = [h for cd, h, is_cop in dp if not is_cop and window_start <= cd < wk + timedelta(days=7)]
            if len(cop_vals) >= MIN_PRS and len(hum_vals) >= MIN_PRS:
                deltas.append(float(np.median(hum_vals) - np.median(cop_vals)))
            else:
                deltas.append(float("nan"))
        if all(d != d for d in deltas):  # all NaN
            continue
        repos_with_data.append(repo)
        ax.plot(weeks, deltas, color=get_color(repo), label=get_short(repo),
                linewidth=2.0, alpha=0.9)
        finite = [d for d in deltas if d == d]
        visible_data.extend(finite)
        line_ends.append((weeks, deltas, get_short(repo), get_color(repo)))

    if not visible_data:
        plt.close(fig)
        print("  (skipping copilot time-to-comment delta — insufficient data)")
        return

    # Y-axis tuned to typical hour-scale variation
    ax.set_ylim(-25, 35)
    ax.yaxis.set_major_locator(MaxNLocator(nbins=8))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}"))
    ax.legend(loc="upper right", fontsize=10)
    label_line_ends(ax, line_ends)

    # Custom dual arrows (no "Better" label — direction is informational, not normative)
    arrow_x = 0.965
    ax.annotate("", xy=(arrow_x, 0.78), xytext=(arrow_x, 0.55),
                xycoords="axes fraction",
                arrowprops=dict(arrowstyle="-|>,head_width=0.5,head_length=0.4",
                                color="#888", lw=2))
    ax.text(arrow_x - 0.005, 0.80, "copilot PRs get human\ncomment sooner",
            transform=ax.transAxes, fontsize=8, ha="right", va="bottom",
            color="#666", style="italic")
    ax.annotate("", xy=(arrow_x, 0.22), xytext=(arrow_x, 0.45),
                xycoords="axes fraction",
                arrowprops=dict(arrowstyle="-|>,head_width=0.5,head_length=0.4",
                                color="#888", lw=2))
    ax.text(arrow_x - 0.005, 0.20, "human PRs get human\ncomment sooner",
            transform=ax.transAxes, fontsize=8, ha="right", va="top",
            color="#666", style="italic")

    add_insight_box(ax, [
        "Per-repo P50 hours: human PRs minus copilot PRs to first non-author comment",
        "Positive → copilot PRs get human comment sooner; Negative → human PRs sooner",
        "Early indications that Copilot PRs are starting to get non-author human attention sooner",
    ])
    _pad_date_xlim(fig)
    fig.tight_layout()
    path = os.path.join(output_dir, "copilot_time_to_comment.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_time_comment_to_merge(all_items, all_first_comments, output_dir):
    """P50 time from first human comment to merge, multi-repo, weekly with 4-week rolling window."""
    import numpy as np
    today = datetime.now().date()
    coverage_cutoff = today - timedelta(days=365)
    WINDOW_DAYS = 28
    MIN_PRS = 5

    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "First Human Comment to Merge — P50 (weekly, 4-week rolling)", "Days")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.1f}"))
    ax.yaxis.set_major_locator(MaxNLocator(nbins=8))

    # Generate weekly evaluation points
    weeks = []
    w = week_start(coverage_cutoff) + timedelta(weeks=4)
    last_full = week_start(today) - timedelta(weeks=1)
    while w <= last_full:
        weeks.append(w)
        w += timedelta(weeks=1)

    visible_data = []
    line_ends = []
    for repo in COMMENT_CHART_REPOS:
        items = all_items.get(repo)
        fc = all_first_comments.get(repo)
        if not items or not fc:
            continue
        # Collect (create_date, days_comment_to_merge) tuples (sub-day precision)
        data_points = []
        for item in items:
            if not item["is_pr"] or not item.get("merged_at"):
                continue
            fci = fc.get(item["number"])
            if not fci:
                continue
            cd = parse_date(item["created_at"])
            if not cd or cd < coverage_cutoff:
                continue
            fc_dt = fci.get("first_comment_dt")
            try:
                merged_dt = datetime.fromisoformat(item["merged_at"].replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                continue
            if not fc_dt:
                continue
            if fc_dt > merged_dt:
                continue
            days = (merged_dt - fc_dt).total_seconds() / 86400
            if days < 0:
                continue
            data_points.append((cd, days))

        if not data_points:
            continue

        p50s = []
        for wk in weeks:
            window_start = wk - timedelta(days=WINDOW_DAYS)
            vals = [d for cd, d in data_points if window_start <= cd < wk + timedelta(days=7)]
            p50s.append(float(np.median(vals)) if len(vals) >= MIN_PRS else float("nan"))

        color = get_color(repo)
        short = get_short(repo)
        ax.plot(weeks, p50s, color=color, label=short, linewidth=1.5, alpha=0.85)
        visible_data.append([v for v in p50s if not (v != v)])  # exclude NaN
        line_ends.append((weeks, p50s, short, color))

    if not visible_data:
        plt.close(fig)
        print("  (skipping comment-to-merge — no comment data)")
        return

    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(0, ymax)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "down")
    add_insight_box(ax, [
        "How long PRs sit between getting first feedback and actually merging",
        "Proxy for review-cycle efficiency — high values may indicate stalled discussions or back-and-forth iteration",
        "P50 = median days from first non-author comment to merge (merged PRs only)",
        "Roughly stable for runtime, roslyn, aspire over the last 4 months",
    ])
    _pad_date_xlim(fig)
    fig.tight_layout()
    path = os.path.join(output_dir, "time_comment_to_merge.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_copilot_time_comment_to_merge(all_items, all_first_comments, output_dir):
    """Per-repo P50 delta (Copilot − Human) for first-comment-to-merge days, weekly 4-week rolling.
    Restricted to PRs created from 2025-07-01 onward (earlier copilot data too sparse)."""
    import numpy as np
    today = datetime.now().date()
    coverage_cutoff = max(today - timedelta(days=365), date(2025, 7, 1))
    WINDOW_DAYS = 28
    MIN_PRS = 3

    per_repo = {r: [] for r in COMMENT_CHART_REPOS}
    for repo in COMMENT_CHART_REPOS:
        items = all_items.get(repo)
        fc = all_first_comments.get(repo)
        if not items or not fc:
            continue
        for item in items:
            if not item["is_pr"] or not item.get("merged_at"):
                continue
            cd = parse_date(item["created_at"])
            if not cd or cd < coverage_cutoff:
                continue
            cls = _classify_copilot(item)
            if cls == "unknown":
                continue
            fc_entry = fc.get(item["number"])
            if not fc_entry:
                continue
            fc_dt = fc_entry.get("first_comment_dt")
            try:
                merged_dt = datetime.fromisoformat(item["merged_at"].replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                continue
            if not fc_dt or fc_dt > merged_dt:
                continue
            days = (merged_dt - fc_dt).total_seconds() / 86400
            if days < 0:
                continue
            per_repo[repo].append((cd, days, cls in ("cca", "assisted")))

    # Weekly evaluation points
    weeks = []
    w = week_start(coverage_cutoff) + timedelta(weeks=4)
    last_full = week_start(today) - timedelta(weeks=1)
    while w <= last_full:
        weeks.append(w)
        w += timedelta(weeks=1)

    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "First Comment to Merge Delta (Human − Copilot, positive = copilot sooner) — per repo (4-week rolling P50)", "Days")
    ax.axhline(y=0, color="black", linewidth=0.7, alpha=0.6)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())

    visible_data = []
    line_ends = []
    for repo in sorted(COMMENT_CHART_REPOS):
        dp = per_repo.get(repo) or []
        if not dp:
            continue
        deltas = []
        for wk in weeks:
            window_start = wk - timedelta(days=WINDOW_DAYS)
            cop_vals = [d for cd, d, is_cop in dp if is_cop and window_start <= cd < wk + timedelta(days=7)]
            hum_vals = [d for cd, d, is_cop in dp if not is_cop and window_start <= cd < wk + timedelta(days=7)]
            if len(cop_vals) >= MIN_PRS and len(hum_vals) >= MIN_PRS:
                deltas.append(float(np.median(hum_vals) - np.median(cop_vals)))
            else:
                deltas.append(float("nan"))
        if all(d != d for d in deltas):
            continue
        ax.plot(weeks, deltas, color=get_color(repo), label=get_short(repo),
                linewidth=2.0, alpha=0.9)
        finite = [d for d in deltas if d == d]
        visible_data.extend(finite)
        line_ends.append((weeks, deltas, get_short(repo), get_color(repo)))

    if not visible_data:
        plt.close(fig)
        print("  (skipping copilot comment-to-merge delta — insufficient data)")
        return

    ax.set_ylim(-10, 10)
    ax.yaxis.set_major_locator(MaxNLocator(nbins=8))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.1f}"))
    ax.legend(loc="upper right", fontsize=10)
    label_line_ends(ax, line_ends)

    arrow_x = 0.965
    ax.annotate("", xy=(arrow_x, 0.78), xytext=(arrow_x, 0.55),
                xycoords="axes fraction",
                arrowprops=dict(arrowstyle="-|>,head_width=0.5,head_length=0.4",
                                color="#888", lw=2))
    ax.text(arrow_x - 0.005, 0.80, "copilot PRs merge sooner\nafter they are first looked at",
            transform=ax.transAxes, fontsize=8, ha="right", va="bottom",
            color="#666", style="italic")
    ax.annotate("", xy=(arrow_x, 0.22), xytext=(arrow_x, 0.45),
                xycoords="axes fraction",
                arrowprops=dict(arrowstyle="-|>,head_width=0.5,head_length=0.4",
                                color="#888", lw=2))
    ax.text(arrow_x - 0.005, 0.20, "human PRs merge sooner\nafter they are first looked at",
            transform=ax.transAxes, fontsize=8, ha="right", va="top",
            color="#666", style="italic")

    add_insight_box(ax, [
        "Per-repo P50 days: human PRs minus copilot PRs, first comment → merge",
        "Positive → copilot PRs merge sooner after feedback; Negative → human PRs sooner",
        "PRs created from Jul 2025 onward only (earlier copilot data too sparse)",
        "Copilot PRs merge about as quickly as human PRs, with early indications they may begin to merge quicker",
    ])
    _pad_date_xlim(fig)
    fig.tight_layout()
    path = os.path.join(output_dir, "copilot_time_comment_to_merge.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


# ── Push-events Chart ───────────────────────────────────────────────────────

# Repos with push-event data fetched (mirrors fetch_pr_pushes.py DEFAULT_REPOS).
PUSH_CHART_REPOS = (
    "dotnet/runtime", "dotnet/roslyn", "dotnet/maui", "microsoft/aspire",
    "Azure/azure-sdk-for-js",
)

# Cluster events whose consecutive timestamps differ by ≤ this many minutes
# into a single "push" (= one CI trigger).
PUSH_CLUSTER_GAP_MINUTES = 5
# Cap per-PR push counts to avoid stuck/auto-merge-loop PRs (we've seen
# roslyn PRs with 200+ pushes from bot-driven rebase loops) dominating
# the weekly mean. PRs above this cap still count as MAX_PUSHES_PER_PR.
MAX_PUSHES_PER_PR = 30


def load_push_events(conn, repo):
    """Load push events from pr_push_events.
    Returns dict: PR number -> sorted list of datetime objects."""
    try:
        rows = conn.execute(
            "SELECT number, ts FROM pr_push_events WHERE repo = ? ORDER BY number, ts",
            (repo,),
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    by_pr = defaultdict(list)
    for number, ts in rows:
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            continue
        by_pr[number].append(dt)
    return dict(by_pr)


def cluster_pushes(timestamps, gap_minutes=PUSH_CLUSTER_GAP_MINUTES):
    """Given a sorted list of datetimes, return the number of clusters where
    consecutive members within `gap_minutes` collapse into one cluster."""
    if not timestamps:
        return 0
    gap = timedelta(minutes=gap_minutes)
    count = 1
    last = timestamps[0]
    for t in timestamps[1:]:
        if t - last > gap:
            count += 1
        last = t
    return count


def chart_pushes_per_pr_over_time(all_items, all_push_events, output_dir):
    """Mean CI-triggering pushes per merged PR, bucketed by merge week.
    Mean (rather than median) gives a continuous y-axis that responds to small
    distribution shifts. Plotted as scatter dots (per-week means) with a
    per-repo linear regression line, plus a first-3mo vs last-3mo stats table."""
    import numpy as np
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(
        ax,
        "CI-Triggering Pushes per PR — weekly means with linear trend",
        "Pushes per merged PR",
    )
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.1f}"))
    ax.yaxis.set_major_locator(MaxNLocator(nbins=8))

    today = datetime.now().date()
    cutoff = today - timedelta(days=18 * 30)
    MIN_PRS_PER_WEEK = 20
    MIN_COVERAGE = 0.70

    visible_data = []
    line_ends = []
    weeks_global = set()
    any_data = False

    # Collect per-PR data (not just weekly aggregated) so we can compute
    # first-3mo vs last-3mo stats for the comparison table.
    repo_pr_data = {}  # repo -> [(merge_date, n_pushes), ...]

    for repo in PUSH_CHART_REPOS:
        items = all_items.get(repo)
        events = all_push_events.get(repo)
        if not items or not events:
            continue

        per_pr = []
        push_counts = defaultdict(list)   # week -> [n_pushes per PR, ...]
        merged_total = defaultdict(int)
        for item in items:
            if not item.get("is_pr") or not item.get("merged_at"):
                continue
            md = parse_date(item["merged_at"])
            if not md or md < cutoff:
                continue
            wk = week_start(md)
            merged_total[wk] += 1
            ts_list = events.get(item["number"])
            if not ts_list:
                continue
            n_pushes = cluster_pushes(ts_list)
            if n_pushes <= 0:
                continue
            n_pushes = min(n_pushes, MAX_PUSHES_PER_PR)
            push_counts[wk].append(n_pushes)
            per_pr.append((md, n_pushes))

        repo_pr_data[repo] = per_pr

        if not push_counts:
            continue
        any_data = True

        x, mean_v = [], []
        cur_week = week_start(today)
        for w in sorted(push_counts):
            if w >= cur_week:
                continue
            vals = push_counts[w]
            if len(vals) < MIN_PRS_PER_WEEK:
                continue
            coverage = len(vals) / max(1, merged_total[w])
            if coverage < MIN_COVERAGE:
                continue
            x.append(w)
            mean_v.append(float(np.mean(vals)))
        if not x:
            continue

        # Plot as a scatter of weekly means + linear regression line
        # (much cleaner than connected lines for a trend question)
        color = get_color(repo)
        short = get_short(repo)
        x_dt = mdates.date2num(x)
        ax.scatter(x, mean_v, color=color, s=22, alpha=0.55, zorder=3,
                   label=f"{short} weekly mean")
        # Linear regression on weekly means
        if len(x) >= 4:
            slope, intercept = np.polyfit(x_dt, mean_v, 1)
            xfit = np.array([x_dt[0], x_dt[-1]])
            yfit = slope * xfit + intercept
            ax.plot(mdates.num2date(xfit), yfit, color=color, linewidth=2.2,
                    alpha=0.9, zorder=4)
        visible_data.extend(mean_v)
        line_ends.append((x, mean_v, short, color))
        for w in x:
            weeks_global.add(w)

    if not any_data or not weeks_global:
        plt.close(fig)
        print("  (skipping pushes-per-pr — no complete-coverage weeks)")
        return

    if visible_data:
        # Cap y using the 95th percentile of the visible series values
        # (weekly means) to keep the trend readable; weekly outlier means
        # extend outside the visible area on purpose.
        import numpy as np
        ymax = float(np.percentile(visible_data, 95)) * 1.4
        ax.set_ylim(0, max(ymax, 5))

    xmin = min(weeks_global) - timedelta(days=3)
    xmax = max(weeks_global) + timedelta(days=10)
    ax.set_xlim(xmin, xmax)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    ax.xaxis.set_minor_locator(mdates.WeekdayLocator(byweekday=0))

    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "down")

    # Compare-periods stats table (top right): mean ± std for first 3 mo vs
    # last 3 mo of the visible window, with Welch's t-test significance marker.
    if weeks_global and repo_pr_data:
        win_start = min(weeks_global)
        win_end = max(weeks_global) + timedelta(days=6)
        first_end = win_start + timedelta(days=90)
        last_start = win_end - timedelta(days=90)
        table_rows = [["repo", "first 3mo (mean ± std)", "last 3mo (mean ± std)", "Δ ± 95% CI"]]
        for repo in PUSH_CHART_REPOS:
            data = repo_pr_data.get(repo) or []
            first_vals = [n for d, n in data if win_start <= d < first_end]
            last_vals = [n for d, n in data if last_start <= d <= win_end]
            if len(first_vals) < 20 or len(last_vals) < 20:
                table_rows.append([get_short(repo), "—", "—", "—"])
                continue
            n1, n2 = len(first_vals), len(last_vals)
            m1 = float(np.mean(first_vals))
            m2 = float(np.mean(last_vals))
            s1 = float(np.std(first_vals, ddof=1))
            s2 = float(np.std(last_vals, ddof=1))
            # Welch's t-test (no scipy needed)
            se_diff = (s1 ** 2 / n1 + s2 ** 2 / n2) ** 0.5
            if se_diff > 0:
                t = (m2 - m1) / se_diff
                num = (s1 ** 2 / n1 + s2 ** 2 / n2) ** 2
                den = (s1 ** 4 / (n1 ** 2 * (n1 - 1))
                       + s2 ** 4 / (n2 ** 2 * (n2 - 1)))
                df = num / den if den > 0 else min(n1, n2) - 1
                from math import erf, sqrt
                z = abs(t)
                pval = 2 * (1 - 0.5 * (1 + erf(z / sqrt(2))))
            else:
                pval = 1.0
            sig = ("***" if pval < 0.001
                   else "**" if pval < 0.01
                   else "*" if pval < 0.05 else "")
            delta = m2 - m1
            sign = "+" if delta >= 0 else ""
            # 95% CI on the difference: with df > 100 the t-critical is ~1.96
            ci95 = 1.96 * se_diff
            table_rows.append([
                get_short(repo),
                f"{m1:.2f} ± {s1:.1f} (n={n1})",
                f"{m2:.2f} ± {s2:.1f} (n={n2})",
                f"{sign}{delta:.2f} ± {ci95:.2f}{sig}",
            ])
        tbl = ax.table(
            cellText=table_rows[1:],
            colLabels=table_rows[0],
            loc="upper right",
            cellLoc="left",
            colLoc="left",
            bbox=[0.50, 0.66, 0.49, 0.30],
        )
        tbl.auto_set_font_size(False)
        tbl.set_fontsize(8)
        for (r, c), cell in tbl.get_celld().items():
            cell.set_edgecolor("#cccccc")
            if r == 0:
                cell.set_facecolor("#f0f0f0")
                cell.set_text_props(weight="bold")
            else:
                cell.set_facecolor("#ffffff")
        ax.text(0.99, 0.65,
                "± std shows PR-to-PR spread; ± 95% CI shows uncertainty of the difference. "
                "Welch's t-test: * p<0.05, ** p<0.01, *** p<0.001.",
                transform=ax.transAxes, fontsize=7, color="#666",
                ha="right", va="top", style="italic", wrap=True)

    add_insight_box(ax, [
        "Dots = per-week mean pushes/PR; line = per-repo linear regression",
        f"A 'push' = git push event triggering CI. Commits within "
        f"{PUSH_CLUSTER_GAP_MINUTES} min are clustered as one push.",
        "Counts both regular pushes and force-pushes (timeline events)",
        f"Per-PR push count capped at {MAX_PUSHES_PER_PR} to suppress stuck/auto-merge-loop PRs",
        f"Weeks with <{MIN_PRS_PER_WEEK} merged PRs or <70% event-coverage are dropped",
        "Caveat: committed-event timestamps reflect commit time (close to push "
        "after rebase, can diverge if commits are made locally then pushed later)",
    ], loc="lower right")
    fig.tight_layout()
    path = os.path.join(output_dir, "pushes_per_pr.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


# ============================================================================
# REVIEW METRICS — Copilot Code Review effectiveness charts
# ============================================================================

REVIEW_CHART_REPOS = (
    "dotnet/runtime", "dotnet/roslyn", "dotnet/maui", "microsoft/aspire",
    "Azure/azure-sdk-for-js",
)


def load_review_data(conn, repo):
    """Load all review-related data for a repo from the review tables.
    Returns dict with keys: reviews_by_pr, comments_by_pr, commits_by_pr, or empty dict if tables missing."""
    try:
        reviews = conn.execute(
            "SELECT number, author, author_type, state, submitted_at "
            "FROM pr_reviews WHERE repo = ? ORDER BY number, submitted_at",
            (repo,)
        ).fetchall()
        comments = conn.execute(
            "SELECT number, author, author_type, body_has_suggestion, is_resolved, created_at, path "
            "FROM pr_review_comments WHERE repo = ? ORDER BY number, created_at",
            (repo,)
        ).fetchall()
        commits = conn.execute(
            "SELECT number, sha, committed_date, additions, deletions, message "
            "FROM pr_commit_stats WHERE repo = ? ORDER BY number, committed_date",
            (repo,)
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    # Organize by PR number
    from collections import defaultdict
    reviews_by_pr = defaultdict(list)
    for num, author, atype, state, ts in reviews:
        reviews_by_pr[num].append({
            "author": author, "author_type": atype, "state": state, "submitted_at": ts
        })
    comments_by_pr = defaultdict(list)
    for num, author, atype, has_sugg, resolved, ts, path in comments:
        comments_by_pr[num].append({
            "author": author, "author_type": atype,
            "body_has_suggestion": has_sugg, "is_resolved": resolved,
            "created_at": ts, "path": path or ""
        })
    commits_by_pr = defaultdict(list)
    for num, sha, cdate, adds, dels, msg in commits:
        commits_by_pr[num].append({
            "sha": sha, "committed_date": cdate,
            "additions": adds or 0, "deletions": dels or 0, "message": msg or ""
        })
    return {
        "reviews_by_pr": dict(reviews_by_pr),
        "comments_by_pr": dict(comments_by_pr),
        "commits_by_pr": dict(commits_by_pr),
    }


def _is_copilot_reviewer(author):
    """Check if an author is the Copilot code review bot."""
    return author and "copilot" in author.lower() and "swe" not in author.lower()


def _first_human_review_ts(reviews):
    """Get timestamp of first human review (any state) on a PR."""
    for r in reviews:
        if r["author_type"] == "User":
            return r["submitted_at"]
    return None


def _first_copilot_review_ts(reviews):
    """Get timestamp of first Copilot review on a PR."""
    for r in reviews:
        if _is_copilot_reviewer(r["author"]):
            return r["submitted_at"]
    return None


# Minimum Copilot-reviewed merged PRs for a repo to appear in comparison charts
_MIN_COPILOT_PRS = 50


def _repos_with_copilot_activity(all_items, review_data):
    """Return repos from REVIEW_CHART_REPOS with meaningful Copilot review activity."""
    result = []
    for repo in REVIEW_CHART_REPOS:
        items = all_items.get(repo)
        rd = review_data.get(repo)
        if not items or not rd:
            continue
        reviews_by_pr = rd["reviews_by_pr"]
        copilot_count = sum(
            1 for i in items
            if i.get("is_pr") and i.get("merged_at")
            and any(_is_copilot_reviewer(r["author"])
                    for r in reviews_by_pr.get(i["number"], []))
        )
        if copilot_count >= _MIN_COPILOT_PRS:
            result.append(repo)
    return result


def _set_review_month_zoom_xaxis(ax):
    # Dense weekly labels for short-window review charts.
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0, interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.xaxis.set_minor_locator(mdates.DayLocator(interval=1))


_REVIEW_LATEST_CREATED_CACHE = {}


def _review_latest_created(all_items):
    cache_key = id(all_items)
    cached = _REVIEW_LATEST_CREATED_CACHE.get(cache_key)
    if cached is not None:
        return cached

    latest_created = None
    for repo in REVIEW_CHART_REPOS:
        items = all_items.get(repo)
        if not items:
            continue
        for it in items:
            if not it.get("is_pr") or not it.get("merged_at") or not it.get("created_at"):
                continue
            d = parse_date(it.get("created_at"))
            if d is not None and (latest_created is None or d > latest_created):
                latest_created = d
    _REVIEW_LATEST_CREATED_CACHE[cache_key] = latest_created
    return latest_created


def _review_effective_today(all_items, month_zoom):
    today = datetime.now().date()
    if not month_zoom:
        return today
    latest_created = _review_latest_created(all_items)
    if latest_created is None:
        return today
    # Keep the same "exclude partial today" behavior while anchoring to data freshness.
    return min(today, latest_created + timedelta(days=1))


def _review_bucket_date(d, month_zoom):
    return d if month_zoom else week_start(d)


def _review_window_values(series_by_bucket, anchor, month_zoom):
    vals = []
    if month_zoom:
        for k in range(28):
            vals.extend(series_by_bucket.get(anchor - timedelta(days=k), []))
    else:
        for k in range(4):
            vals.extend(series_by_bucket.get(anchor - timedelta(weeks=k), []))
    return vals


def _review_window_sum(series_by_bucket, anchor, month_zoom):
    if month_zoom:
        return sum(series_by_bucket.get(anchor - timedelta(days=k), 0) for k in range(28))
    return sum(series_by_bucket.get(anchor - timedelta(weeks=k), 0) for k in range(4))


def chart_review_churn_before_human(
        all_items, review_data, output_dir, *,
        cutoff_days=365,
        title="Code Changes Completed Before Human Review (4-week rolling P50)",
        output_file="review_churn_before_human.png",
        insight_lines=None,
        month_zoom=False):
    """⭐ % of code changes completed before first human review.
    For Copilot-reviewed PRs, what fraction of total lines changed were already
    done before the first human touched the PR? Ideally approaches 100% as
    Copilot review gets good enough that humans barely need to request changes."""
    from statistics import median

    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, title, "% of lines changed before human review")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}%"))

    today = _review_effective_today(all_items, month_zoom)
    cutoff = today - timedelta(days=cutoff_days)
    last_complete = (today - timedelta(days=1)) if month_zoom else (week_start(today) - timedelta(weeks=1))
    step = timedelta(days=3 if month_zoom else 7)

    visible_data = []
    line_ends = []
    active_repos = _repos_with_copilot_activity(all_items, review_data)
    for repo in active_repos:
        items = all_items.get(repo)
        rd = review_data.get(repo)
        if not items or not rd:
            continue
        commits_by_pr = rd["commits_by_pr"]
        reviews_by_pr = rd["reviews_by_pr"]

        ratio_by_week = defaultdict(list)
        for item in items:
            if not item.get("is_pr") or not item.get("merged_at"):
                continue
            num = item["number"]
            commits = commits_by_pr.get(num, [])
            reviews = reviews_by_pr.get(num, [])
            if len(commits) < 2:
                continue  # Need at least 2 commits to measure before/after
            # Only include PRs that had Copilot review
            if not any(_is_copilot_reviewer(r.get("author", "")) for r in reviews):
                continue
            first_human_ts = _first_human_review_ts(reviews)
            if not first_human_ts:
                continue  # No human review — can't split

            # Sum lines before vs after first human review
            total_lines = 0
            before_lines = 0
            for c in commits:
                lines = c["additions"] + c["deletions"]
                total_lines += lines
                if c["committed_date"] and c["committed_date"] <= first_human_ts:
                    before_lines += lines
            if total_lines < 10 or total_lines > 10000:
                continue  # skip trivial and bulk PRs
            pct_before = 100.0 * before_lines / total_lines

            cd = parse_date(item["created_at"])
            if cd and cd >= cutoff:
                ratio_by_week[_review_bucket_date(cd, month_zoom)].append(pct_before)

        if not ratio_by_week:
            continue
        # P50 over a rolling 4-week window (evaluated weekly or every 3 days for month zoom)
        weeks_x, p50s = [], []
        w = max(min(ratio_by_week.keys()), cutoff)
        if not month_zoom:
            w = week_start(w)
        while w <= last_complete:
            window_vals = _review_window_values(ratio_by_week, w, month_zoom)
            if len(window_vals) >= 5:
                weeks_x.append(w)
                p50s.append(median(window_vals))
            w += step
        if not weeks_x:
            continue
        ax.plot(weeks_x, p50s, color=get_color(repo), label=get_short(repo),
                linewidth=2, alpha=0.85)
        visible_data.append(p50s)
        line_ends.append((weeks_x, p50s, get_short(repo), get_color(repo)))

    if not visible_data:
        plt.close(fig)
        print("  (skipping review churn — no data)")
        return
    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(max(0, ymin - 5), 105)
    ax.axhline(y=100, color="green", linestyle="--", alpha=0.3, linewidth=1)
    if month_zoom:
        _set_review_month_zoom_xaxis(ax)
    else:
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
        ax.xaxis.set_minor_locator(mdates.MonthLocator())
    ax.legend(loc="lower left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "up")
    add_insight_box(ax, insight_lines or [
        "Of all lines changed in a Copilot-reviewed PR, what % were done",
        "BEFORE the first human review comment/approval?",
        "Higher = humans find less to change = Copilot caught issues early",
        "Goal: approach 100% — humans just glance and merge",
        "P50 of per-PR ratios; PRs with <2 commits, <10 or >10K LOC excluded",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, output_file)
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_review_copilot_comment_density(
        all_items, review_data, output_dir, *,
        cutoff_days=365,
        title="Copilot Review Comments per 100 Lines Changed (4-week rolling mean)",
        output_file="review_copilot_comment_density.png",
        insight_lines=None,
        month_zoom=False):
    """Copilot Review Comment Density — comments per 100 lines changed, weekly trend."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, title, "Comments / 100 LOC")

    today = _review_effective_today(all_items, month_zoom)
    cutoff = today - timedelta(days=cutoff_days)
    last_complete = (today - timedelta(days=1)) if month_zoom else (week_start(today) - timedelta(weeks=1))
    step = timedelta(days=3 if month_zoom else 7)

    visible_data = []
    line_ends = []
    combined_density_by_week = defaultdict(list)
    active_repos = _repos_with_copilot_activity(all_items, review_data)
    for repo in active_repos:
        items = all_items.get(repo)
        rd = review_data.get(repo)
        if not items or not rd:
            continue
        comments_by_pr = rd["comments_by_pr"]
        commits_by_pr = rd["commits_by_pr"]

        density_by_week = defaultdict(list)
        for item in items:
            if not item.get("is_pr") or not item.get("merged_at"):
                continue
            num = item["number"]
            copilot_comments = [c for c in comments_by_pr.get(num, [])
                                if _is_copilot_reviewer(c["author"])]
            if not copilot_comments:
                continue  # only PRs Copilot actually reviewed
            commits = commits_by_pr.get(num, [])
            loc = sum(c["additions"] + c["deletions"] for c in commits)
            if loc < 10:
                continue  # skip trivial PRs
            cd = parse_date(item["created_at"])
            if cd and cd >= cutoff:
                density = len(copilot_comments) * 100.0 / loc
                wk = _review_bucket_date(cd, month_zoom)
                density_by_week[wk].append(density)
                combined_density_by_week[wk].append(density)

        if not density_by_week:
            continue
        weeks_x, means = [], []
        w = max(min(density_by_week.keys()), cutoff)
        if not month_zoom:
            w = week_start(w)
        while w <= last_complete:
            window_vals = _review_window_values(density_by_week, w, month_zoom)
            if len(window_vals) >= 5:
                weeks_x.append(w)
                means.append(sum(window_vals) / len(window_vals))
            w += step
        if not weeks_x:
            continue
        ax.plot(weeks_x, means, color=get_color(repo), label=get_short(repo),
                linewidth=2, alpha=0.85)
        visible_data.append(means)
        line_ends.append((weeks_x, means, get_short(repo), get_color(repo)))

    if not visible_data:
        plt.close(fig)
        print("  (skipping copilot comment density — no data)")
        return

    # Combined linear regression across all repos
    import numpy as np
    combined_x, combined_y = [], []
    w = min(combined_density_by_week.keys()) if combined_density_by_week else cutoff
    if not month_zoom:
        w = week_start(w)
    while w <= last_complete:
        window_vals = _review_window_values(combined_density_by_week, w, month_zoom)
        if len(window_vals) >= 5:
            combined_x.append(w)
            combined_y.append(sum(window_vals) / len(window_vals))
        w += step
    if len(combined_x) >= 2:
        x_numeric = np.array([(d - combined_x[0]).days for d in combined_x], dtype=float)
        y_arr = np.array(combined_y, dtype=float)
        coeffs = np.polyfit(x_numeric, y_arr, 1)
        trend_y = np.polyval(coeffs, x_numeric)
        ax.plot(combined_x, trend_y, color="gray", linestyle=":", linewidth=1.5,
                alpha=0.7, label="Combined trend")

    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(0, ymax)
    if month_zoom and ymax <= 5:
        ax.yaxis.set_major_locator(MultipleLocator(0.5))
        ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.1f}"))
    if month_zoom:
        _set_review_month_zoom_xaxis(ax)
    else:
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
        ax.xaxis.set_minor_locator(mdates.MonthLocator())
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_insight_box(ax, insight_lines or [
        "Copilot review comments per 100 lines changed (additions + deletions)",
        "Normalized by PR size so large PRs don't inflate the metric",
        "Only Copilot-reviewed PRs with ≥10 LOC; coverage tracked separately",
        "Rising = Copilot finding more per line; declining = cleaner code or lighter touch",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, output_file)
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_review_human_comments_comparison(
        all_items, review_data, output_dir, *,
        cutoff_days=365,
        title="Human Review Comments — Copilot-Reviewed vs Not (4-week rolling mean)",
        output_file="review_human_comments_comparison.png",
        insight_lines=None,
        month_zoom=False):
    """Human comments on Copilot-reviewed PRs vs non-Copilot-reviewed PRs."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, title, "Human comments / PR")

    today = _review_effective_today(all_items, month_zoom)
    cutoff = today - timedelta(days=cutoff_days)
    last_complete = (today - timedelta(days=1)) if month_zoom else (week_start(today) - timedelta(weeks=1))
    step = timedelta(days=3 if month_zoom else 7)

    active_repos = _repos_with_copilot_activity(all_items, review_data)
    for repo in active_repos:
        items = all_items.get(repo)
        rd = review_data.get(repo)
        if not items or not rd:
            continue
        comments_by_pr = rd["comments_by_pr"]
        reviews_by_pr = rd["reviews_by_pr"]

        copilot_by_week = defaultdict(list)
        no_copilot_by_week = defaultdict(list)

        for item in items:
            if not item.get("is_pr") or not item.get("merged_at"):
                continue
            num = item["number"]
            reviews = reviews_by_pr.get(num, [])
            has_copilot = any(_is_copilot_reviewer(r["author"]) for r in reviews)
            human_count = sum(1 for c in comments_by_pr.get(num, [])
                             if c["author_type"] == "User")
            cd = parse_date(item["created_at"])
            if not cd or cd < cutoff:
                continue
            wk = _review_bucket_date(cd, month_zoom)
            if has_copilot:
                copilot_by_week[wk].append(human_count)
            else:
                no_copilot_by_week[wk].append(human_count)

        # Plot both lines using repo color; solid = copilot-reviewed, dashed = not
        repo_color = get_color(repo)
        short = get_short(repo)
        for label_suffix, data, ls, lw, min_pts in [
            ("w/ Copilot", copilot_by_week, "-", 2.5, 10),
            ("w/o Copilot", no_copilot_by_week, "--", 1.5, 5),
        ]:
            if not data:
                continue
            weeks_x, means = [], []
            w = max(min(data.keys()), cutoff)
            if not month_zoom:
                w = week_start(w)
            while w <= last_complete:
                window_vals = _review_window_values(data, w, month_zoom)
                if len(window_vals) >= min_pts:
                    weeks_x.append(w)
                    means.append(sum(window_vals) / len(window_vals))
                w += step
            if weeks_x:
                ax.plot(weeks_x, means, color=repo_color,
                        label=f"{short} {label_suffix}",
                        linewidth=lw, alpha=0.85, linestyle=ls)

    ax.set_ylim(0, None)
    if month_zoom:
        ymax = ax.get_ylim()[1]
        if ymax <= 5:
            ax.yaxis.set_major_locator(MultipleLocator(0.5))
            ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.1f}"))
    if month_zoom:
        _set_review_month_zoom_xaxis(ax)
    else:
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
        ax.xaxis.set_minor_locator(mdates.MonthLocator())
    ax.legend(loc="upper left", fontsize=10)
    add_direction_arrow(ax, "down")
    add_insight_box(ax, insight_lines or [
        "Human review comments per merged PR, split by whether Copilot also reviewed",
        "Solid = w/ Copilot review; Dashed = without (same color = same repo)",
        "Copilot-reviewed PRs may show MORE human comments due to selection bias",
        "Better metric: trend within Copilot-reviewed PRs over time (TODO)",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, output_file)
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_review_suggestion_rate(
        all_items, review_data, output_dir, *,
        cutoff_days=365,
        title="Copilot Suggestion Rate — % of Comments with Code Suggestions (4-week rolling)",
        output_file="review_suggestion_rate.png",
        insight_lines=None,
        month_zoom=False):
    """Copilot Review Suggestion Rate — % of Copilot review comments containing code suggestions."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, title, "% with suggestions")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}%"))

    today = _review_effective_today(all_items, month_zoom)
    cutoff = today - timedelta(days=cutoff_days)
    last_complete = (today - timedelta(days=1)) if month_zoom else (week_start(today) - timedelta(weeks=1))
    step = timedelta(days=3 if month_zoom else 7)

    visible_data = []
    line_ends = []
    combined_total_by_week = defaultdict(int)
    combined_sugg_by_week = defaultdict(int)
    active_repos = _repos_with_copilot_activity(all_items, review_data)
    for repo in active_repos:
        items = all_items.get(repo)
        rd = review_data.get(repo)
        if not items or not rd:
            continue
        comments_by_pr = rd["comments_by_pr"]

        by_week_total = defaultdict(int)
        by_week_sugg = defaultdict(int)
        for item in items:
            if not item.get("is_pr") or not item.get("merged_at"):
                continue
            num = item["number"]
            cd = parse_date(item["created_at"])
            if not cd or cd < cutoff:
                continue
            wk = _review_bucket_date(cd, month_zoom)
            for c in comments_by_pr.get(num, []):
                if _is_copilot_reviewer(c["author"]):
                    by_week_total[wk] += 1
                    combined_total_by_week[wk] += 1
                    if c["body_has_suggestion"]:
                        by_week_sugg[wk] += 1
                        combined_sugg_by_week[wk] += 1

        if not by_week_total:
            continue
        weeks_x, pcts = [], []
        w = max(min(by_week_total.keys()), cutoff)
        if not month_zoom:
            w = week_start(w)
        while w <= last_complete:
            total = _review_window_sum(by_week_total, w, month_zoom)
            sugg = _review_window_sum(by_week_sugg, w, month_zoom)
            if total >= 5:
                weeks_x.append(w)
                pcts.append(100.0 * sugg / total)
            w += step
        if not weeks_x:
            continue
        plot_kwargs = dict(color=get_color(repo), label=get_short(repo), linewidth=2, alpha=0.85)
        if month_zoom:
            plot_kwargs["marker"] = "o"
            plot_kwargs["markersize"] = 3
        ax.plot(weeks_x, pcts, **plot_kwargs)
        visible_data.append(pcts)
        line_ends.append((weeks_x, pcts, get_short(repo), get_color(repo)))

    if not visible_data:
        plt.close(fig)
        print("  (skipping suggestion rate — no data)")
        return

    # Combined linear regression across all repos
    import numpy as np
    combined_x, combined_y = [], []
    w = min(combined_total_by_week.keys()) if combined_total_by_week else cutoff
    if not month_zoom:
        w = week_start(w)
    while w <= last_complete:
        total = _review_window_sum(combined_total_by_week, w, month_zoom)
        sugg = _review_window_sum(combined_sugg_by_week, w, month_zoom)
        if total >= 5:
            combined_x.append(w)
            combined_y.append(100.0 * sugg / total)
        w += step
    if len(combined_x) >= 2:
        x_numeric = np.array([(d - combined_x[0]).days for d in combined_x], dtype=float)
        y_arr = np.array(combined_y, dtype=float)
        coeffs = np.polyfit(x_numeric, y_arr, 1)
        trend_y = np.polyval(coeffs, x_numeric)
        ax.plot(combined_x, trend_y, color="gray", linestyle=":", linewidth=1.5,
                alpha=0.7, label="Combined trend")

    if month_zoom:
        ymax = max(max(series) for series in visible_data) if visible_data else 0
        upper = 10 if ymax <= 10 else 100
        lower = -0.5 if ymax <= 1 else 0
        ax.set_ylim(lower, upper)
        if upper <= 10:
            ax.yaxis.set_major_locator(MultipleLocator(1))
            ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.1f}%"))
    else:
        ax.set_ylim(0, 100)
    if month_zoom:
        _set_review_month_zoom_xaxis(ax)
    else:
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
        ax.xaxis.set_minor_locator(mdates.MonthLocator())
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_insight_box(ax, insight_lines or [
        "What % of Copilot review comments include a concrete code suggestion?",
        "Higher = more actionable feedback (developers can click 'Apply')",
        "Suggestions detected via ```suggestion``` blocks in comment body",
        "Trend reflects Copilot's ability to propose specific fixes, not just flag issues",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, output_file)
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_review_time_to_first_feedback(
        all_items, review_data, output_dir, *,
        cutoff_days=365,
        title="Time to First Review Feedback — Copilot vs Human (P50 hours, 4-week rolling)",
        output_file="review_time_to_first_feedback.png",
        insight_lines=None,
        month_zoom=False):
    """Time to First Actionable Feedback — Copilot vs Human, P50 hours."""
    from statistics import median

    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, title, "Hours")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.1f}"))

    today = _review_effective_today(all_items, month_zoom)
    cutoff = today - timedelta(days=cutoff_days)
    last_complete = (today - timedelta(days=1)) if month_zoom else (week_start(today) - timedelta(weeks=1))
    step = timedelta(days=3 if month_zoom else 7)

    active_repos = _repos_with_copilot_activity(all_items, review_data)
    for repo in active_repos:
        items = all_items.get(repo)
        rd = review_data.get(repo)
        if not items or not rd:
            continue
        reviews_by_pr = rd["reviews_by_pr"]

        copilot_by_week = defaultdict(list)
        human_by_week = defaultdict(list)

        for item in items:
            if not item.get("is_pr") or not item.get("merged_at"):
                continue
            num = item["number"]
            reviews = reviews_by_pr.get(num, [])
            cd = parse_date(item["created_at"])
            if not cd or cd < cutoff:
                continue
            try:
                created_dt = datetime.fromisoformat(item["created_at"].replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                continue
            wk = _review_bucket_date(cd, month_zoom)

            first_copilot = _first_copilot_review_ts(reviews)
            first_human = _first_human_review_ts(reviews)

            if first_copilot:
                try:
                    dt = datetime.fromisoformat(first_copilot.replace("Z", "+00:00"))
                    hours = (dt - created_dt).total_seconds() / 3600
                    if 0 <= hours < 168:  # Cap at 1 week
                        copilot_by_week[wk].append(hours)
                except (ValueError, AttributeError):
                    pass
            if first_human:
                try:
                    dt = datetime.fromisoformat(first_human.replace("Z", "+00:00"))
                    hours = (dt - created_dt).total_seconds() / 3600
                    if 0 <= hours < 168:
                        human_by_week[wk].append(hours)
                except (ValueError, AttributeError):
                    pass

        repo_color = get_color(repo)
        short = get_short(repo)
        for label_suffix, data, ls, lw in [
            ("Copilot", copilot_by_week, "-", 2.5),
            ("Human", human_by_week, "--", 1.5),
        ]:
            if not data:
                continue
            weeks_x, p50s = [], []
            w = max(min(data.keys()), cutoff)
            if not month_zoom:
                w = week_start(w)
            while w <= last_complete:
                window_vals = _review_window_values(data, w, month_zoom)
                if len(window_vals) >= 3:
                    weeks_x.append(w)
                    p50s.append(median(window_vals))
                w += step
            if weeks_x:
                ax.plot(weeks_x, p50s, color=repo_color,
                        label=f"{short} {label_suffix}",
                        linewidth=lw, alpha=0.85, linestyle=ls)

    ax.set_ylim(0, None)
    if month_zoom:
        _set_review_month_zoom_xaxis(ax)
    else:
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
        ax.xaxis.set_minor_locator(mdates.MonthLocator())
    ax.legend(loc="upper left", fontsize=10)
    add_direction_arrow(ax, "down")
    add_insight_box(ax, insight_lines or [
        "How quickly does a PR get its first review feedback?",
        "Solid = first Copilot review; Dashed = first human review (same color = same repo)",
        "Copilot reviews are near-instant (minutes); humans take hours/days",
        "The gap shows Copilot's 24/7 coverage advantage — feedback while you sleep",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, output_file)
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_review_copilot_to_human_approval(
        all_items, review_data, output_dir, *,
        cutoff_days=365,
        title="Copilot Review → Human Approval (P50 hours, 4-week rolling)",
        output_file="review_copilot_to_approval.png",
        insight_lines=None,
        month_zoom=False):
    """Copilot Review → Human Approval — time from Copilot review to human APPROVED."""
    from statistics import median

    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, title, "Hours")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.1f}"))

    today = _review_effective_today(all_items, month_zoom)
    cutoff = today - timedelta(days=cutoff_days)
    last_complete = (today - timedelta(days=1)) if month_zoom else (week_start(today) - timedelta(weeks=1))
    step = timedelta(days=3 if month_zoom else 7)

    visible_data = []
    line_ends = []
    combined_hours_by_week = defaultdict(list)
    active_repos = _repos_with_copilot_activity(all_items, review_data)
    for repo in active_repos:
        items = all_items.get(repo)
        rd = review_data.get(repo)
        if not items or not rd:
            continue
        reviews_by_pr = rd["reviews_by_pr"]

        hours_by_week = defaultdict(list)
        for item in items:
            if not item.get("is_pr") or not item.get("merged_at"):
                continue
            num = item["number"]
            reviews = reviews_by_pr.get(num, [])
            cd = parse_date(item["created_at"])
            if not cd or cd < cutoff:
                continue

            first_copilot = _first_copilot_review_ts(reviews)
            if not first_copilot:
                continue
            # Find first human APPROVED after Copilot review
            first_approval = None
            for r in reviews:
                if (r["author_type"] == "User" and r["state"] == "APPROVED"
                        and r["submitted_at"] and r["submitted_at"] > first_copilot):
                    first_approval = r["submitted_at"]
                    break
            if not first_approval:
                continue
            try:
                cop_dt = datetime.fromisoformat(first_copilot.replace("Z", "+00:00"))
                app_dt = datetime.fromisoformat(first_approval.replace("Z", "+00:00"))
                hours = (app_dt - cop_dt).total_seconds() / 3600
                if 0 < hours < 168:  # 0-7 days
                    wk = _review_bucket_date(cd, month_zoom)
                    hours_by_week[wk].append(hours)
                    combined_hours_by_week[wk].append(hours)
            except (ValueError, AttributeError):
                continue

        if not hours_by_week:
            continue
        weeks_x, p50s = [], []
        w = max(min(hours_by_week.keys()), cutoff)
        if not month_zoom:
            w = week_start(w)
        while w <= last_complete:
            window_vals = _review_window_values(hours_by_week, w, month_zoom)
            if len(window_vals) >= 3:
                weeks_x.append(w)
                p50s.append(median(window_vals))
            w += step
        if not weeks_x:
            continue
        ax.plot(weeks_x, p50s, color=get_color(repo), label=get_short(repo),
                linewidth=2, alpha=0.85)
        visible_data.append(p50s)
        line_ends.append((weeks_x, p50s, get_short(repo), get_color(repo)))

    if not visible_data:
        plt.close(fig)
        print("  (skipping copilot-to-approval — no data)")
        return

    # Combined linear regression across all repos
    import numpy as np
    combined_x, combined_y = [], []
    w = min(combined_hours_by_week.keys()) if combined_hours_by_week else cutoff
    if not month_zoom:
        w = week_start(w)
    while w <= last_complete:
        window_vals = _review_window_values(combined_hours_by_week, w, month_zoom)
        if len(window_vals) >= 3:
            combined_x.append(w)
            combined_y.append(median(window_vals))
        w += step
    if len(combined_x) >= 2:
        x_numeric = np.array([(d - combined_x[0]).days for d in combined_x], dtype=float)
        y_arr = np.array(combined_y, dtype=float)
        coeffs = np.polyfit(x_numeric, y_arr, 1)
        trend_y = np.polyval(coeffs, x_numeric)
        ax.plot(combined_x, trend_y, color="gray", linestyle=":", linewidth=1.5,
                alpha=0.7, label="Combined trend")

    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(0, ymax)
    if month_zoom:
        _set_review_month_zoom_xaxis(ax)
    else:
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
        ax.xaxis.set_minor_locator(mdates.MonthLocator())
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "down")
    add_insight_box(ax, insight_lines or [
        "After Copilot reviews a PR, how long until a human approves it?",
        "Shrinking gap = humans trust Copilot's pre-screen and approve faster",
        "Only includes PRs where Copilot reviewed AND human later approved",
        "Declining trend suggests growing trust in Copilot as first-pass reviewer",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, output_file)
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_review_human_participation(
        all_items, review_data, output_dir, *,
        cutoff_days=365,
        title="Human Reviewers per PR (4-week rolling mean)",
        output_file="review_human_participation.png",
        insight_lines=None,
        month_zoom=False):
    """Human Reviewer Participation Rate — distinct human reviewers per PR."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, title, "Distinct reviewers / PR")

    today = _review_effective_today(all_items, month_zoom)
    cutoff = today - timedelta(days=cutoff_days)
    last_complete = (today - timedelta(days=1)) if month_zoom else (week_start(today) - timedelta(weeks=1))
    step = timedelta(days=3 if month_zoom else 7)

    visible_data = []
    line_ends = []
    active_repos = _repos_with_copilot_activity(all_items, review_data)
    for repo in active_repos:
        items = all_items.get(repo)
        rd = review_data.get(repo)
        if not items or not rd:
            continue
        reviews_by_pr = rd["reviews_by_pr"]

        count_by_week = defaultdict(list)
        for item in items:
            if not item.get("is_pr") or not item.get("merged_at"):
                continue
            num = item["number"]
            reviews = reviews_by_pr.get(num, [])
            human_reviewers = set(r["author"] for r in reviews
                                  if r["author_type"] == "User" and r["author"])
            cd = parse_date(item["created_at"])
            if cd and cd >= cutoff:
                count_by_week[_review_bucket_date(cd, month_zoom)].append(len(human_reviewers))

        if not count_by_week:
            continue
        weeks_x, means = [], []
        w = max(min(count_by_week.keys()), cutoff)
        if not month_zoom:
            w = week_start(w)
        while w <= last_complete:
            window_vals = _review_window_values(count_by_week, w, month_zoom)
            if len(window_vals) >= 5:
                weeks_x.append(w)
                means.append(sum(window_vals) / len(window_vals))
            w += step
        if not weeks_x:
            continue
        ax.plot(weeks_x, means, color=get_color(repo), label=get_short(repo),
                linewidth=2, alpha=0.85)
        visible_data.append(means)
        line_ends.append((weeks_x, means, get_short(repo), get_color(repo)))

    if not visible_data:
        plt.close(fig)
        print("  (skipping human participation — no data)")
        return

    # Combined linear regression across all repos
    import numpy as np
    all_weeks_combined = defaultdict(list)
    for repo in active_repos:
        items = all_items.get(repo)
        rd = review_data.get(repo)
        if not items or not rd:
            continue
        reviews_by_pr = rd["reviews_by_pr"]
        for item in items:
            if not item.get("is_pr") or not item.get("merged_at"):
                continue
            num = item["number"]
            reviews = reviews_by_pr.get(num, [])
            human_reviewers = set(r["author"] for r in reviews
                                  if r["author_type"] == "User" and r["author"])
            cd = parse_date(item["created_at"])
            if cd and cd >= cutoff:
                all_weeks_combined[_review_bucket_date(cd, month_zoom)].append(len(human_reviewers))
    combined_x, combined_y = [], []
    w = min(all_weeks_combined.keys()) if all_weeks_combined else cutoff
    if not month_zoom:
        w = week_start(w)
    while w <= last_complete:
        window_vals = _review_window_values(all_weeks_combined, w, month_zoom)
        if len(window_vals) >= 5:
            combined_x.append(w)
            combined_y.append(sum(window_vals) / len(window_vals))
        w += step
    if len(combined_x) >= 2:
        x_numeric = np.array([(d - combined_x[0]).days for d in combined_x], dtype=float)
        y_arr = np.array(combined_y, dtype=float)
        coeffs = np.polyfit(x_numeric, y_arr, 1)
        trend_y = np.polyval(coeffs, x_numeric)
        ax.plot(combined_x, trend_y, color="gray", linestyle=":", linewidth=1.5,
                alpha=0.7, label="Combined trend")

    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(0, ymax)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.1f}"))
    if month_zoom and ymax <= 5:
        ax.yaxis.set_major_locator(MultipleLocator(0.5))
    if month_zoom:
        _set_review_month_zoom_xaxis(ax)
    else:
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
        ax.xaxis.set_minor_locator(mdates.MonthLocator())
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_insight_box(ax, insight_lines or [
        "Average distinct human reviewers who leave reviews per merged PR",
        "If Copilot review is trusted, fewer humans may need to pile on",
        "Stable or slight decline = healthy — one trusted reviewer is enough",
        "Sharp decline could indicate review abandonment (watch alongside merge rate)",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, output_file)
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_review_copilot_coverage(
        all_items, review_data, output_dir, *,
        cutoff_days=365,
        title="Copilot Review Coverage — % of Merged PRs Reviewed (4-week rolling)",
        output_file="review_copilot_coverage.png",
        insight_lines=None,
        month_zoom=False):
    """Copilot Review Coverage — % of merged PRs that received Copilot review."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, title, "% of PRs")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}%"))

    today = _review_effective_today(all_items, month_zoom)
    cutoff = today - timedelta(days=cutoff_days)
    last_complete = (today - timedelta(days=1)) if month_zoom else (week_start(today) - timedelta(weeks=1))
    step = timedelta(days=3 if month_zoom else 7)

    visible_data = []
    line_ends = []
    combined_total_by_week = defaultdict(int)
    combined_copilot_by_week = defaultdict(int)
    for repo in REVIEW_CHART_REPOS:
        items = all_items.get(repo)
        rd = review_data.get(repo)
        if not items or not rd:
            continue
        reviews_by_pr = rd["reviews_by_pr"]

        total_by_week = defaultdict(int)
        copilot_by_week = defaultdict(int)
        for item in items:
            if not item.get("is_pr") or not item.get("merged_at"):
                continue
            num = item["number"]
            cd = parse_date(item["created_at"])
            if not cd or cd < cutoff:
                continue
            wk = _review_bucket_date(cd, month_zoom)
            total_by_week[wk] += 1
            combined_total_by_week[wk] += 1
            reviews = reviews_by_pr.get(num, [])
            if any(_is_copilot_reviewer(r["author"]) for r in reviews):
                copilot_by_week[wk] += 1
                combined_copilot_by_week[wk] += 1

        if not total_by_week:
            continue
        weeks_x, pcts = [], []
        w = max(min(total_by_week.keys()), cutoff)
        if not month_zoom:
            w = week_start(w)
        while w <= last_complete:
            total = _review_window_sum(total_by_week, w, month_zoom)
            cop = _review_window_sum(copilot_by_week, w, month_zoom)
            if total >= 5:
                weeks_x.append(w)
                pcts.append(100.0 * cop / total)
            w += step
        if not weeks_x:
            continue
        ax.plot(weeks_x, pcts, color=get_color(repo), label=get_short(repo),
                linewidth=2, alpha=0.85)
        visible_data.append(pcts)
        line_ends.append((weeks_x, pcts, get_short(repo), get_color(repo)))

    if not visible_data:
        plt.close(fig)
        print("  (skipping copilot coverage — no data)")
        return

    # Combined linear regression across all repos
    import numpy as np
    combined_x, combined_y = [], []
    w = min(combined_total_by_week.keys()) if combined_total_by_week else cutoff
    if not month_zoom:
        w = week_start(w)
    while w <= last_complete:
        total = _review_window_sum(combined_total_by_week, w, month_zoom)
        cop = _review_window_sum(combined_copilot_by_week, w, month_zoom)
        if total >= 5:
            combined_x.append(w)
            combined_y.append(100.0 * cop / total)
        w += step
    if len(combined_x) >= 2:
        x_numeric = np.array([(d - combined_x[0]).days for d in combined_x], dtype=float)
        y_arr = np.array(combined_y, dtype=float)
        coeffs = np.polyfit(x_numeric, y_arr, 1)
        trend_y = np.polyval(coeffs, x_numeric)
        ax.plot(combined_x, trend_y, color="gray", linestyle=":", linewidth=1.5,
                alpha=0.7, label="Combined trend")

    ax.set_ylim(0, 100)
    if month_zoom:
        _set_review_month_zoom_xaxis(ax)
    else:
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
        ax.xaxis.set_minor_locator(mdates.MonthLocator())
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "up")
    add_insight_box(ax, insight_lines or [
        "What % of merged PRs received at least one Copilot code review?",
        "Higher = broader coverage, more PRs getting automated first-pass review",
        "Rapid adoption since May 2025; now covers majority of runtime PRs",
        "100% coverage not expected — some PRs (bot-authored, trivial) skip review",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, output_file)
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_review_copilot_coverage_last_month(all_items, review_data, output_dir):
    chart_review_copilot_coverage(
        all_items, review_data, output_dir,
        cutoff_days=35,
        title="Copilot Review Coverage — Last Month (4-week rolling)",
        output_file="review_copilot_coverage_last_month.png",
        month_zoom=True,
        insight_lines=[
            "Last-month zoom of Copilot review coverage (% of merged PRs reviewed by Copilot)",
            "Use this to watch week-over-week movement instead of 6–12 month trends",
            "Expect some volatility from small weekly sample sizes",
            "Most useful for spotting sudden adoption drops or spikes in runtime",
        ],
    )


def chart_review_copilot_comment_density_last_month(all_items, review_data, output_dir):
    chart_review_copilot_comment_density(
        all_items, review_data, output_dir,
        cutoff_days=35,
        title="Copilot Comment Density — Last Month (4-week rolling mean)",
        output_file="review_copilot_comment_density_last_month.png",
        month_zoom=True,
        insight_lines=[
            "Last-month zoom of Copilot review comments per 100 LOC on reviewed PRs",
            "Rising values mean denser Copilot feedback; falling means lighter touch",
            "This short view is tuned for week-over-week runtime swings",
            "Interpret with coverage chart to separate intensity from adoption",
        ],
    )


def chart_review_suggestion_rate_last_month(all_items, review_data, output_dir):
    chart_review_suggestion_rate(
        all_items, review_data, output_dir,
        cutoff_days=35,
        title="Suggestion Rate — Last Month (% with suggestions, 4-week rolling)",
        output_file="review_suggestion_rate_last_month.png",
        month_zoom=True,
        insight_lines=[
            "Last-month zoom of the % of Copilot review comments that include code suggestions",
            "Higher values mean more comments are directly actionable",
            "Week-over-week shifts can indicate prompt/model behavior changes",
            "Volatile series — watch for sustained movement over multiple weeks",
        ],
    )


def chart_review_human_comments_comparison_last_month(all_items, review_data, output_dir):
    chart_review_human_comments_comparison(
        all_items, review_data, output_dir,
        cutoff_days=35,
        title="Human Comments Comparison — Last Month (4-week rolling mean)",
        output_file="review_human_comments_comparison_last_month.png",
        month_zoom=True,
        insight_lines=[
            "Last-month zoom of human comments per PR: with Copilot (solid) vs without (dashed)",
            "Same-color lines are the same repo; line style is the comparison dimension",
            "Short-term divergence highlights where Copilot-reviewed PRs need more/less human follow-up",
            "Use alongside coverage to distinguish mix shifts from behavior shifts",
        ],
    )


def chart_review_churn_before_human_last_month(all_items, review_data, output_dir):
    chart_review_churn_before_human(
        all_items, review_data, output_dir,
        cutoff_days=35,
        title="Code Completeness Before Human Review — Last Month (4-week rolling P50)",
        output_file="review_churn_before_human_last_month.png",
        month_zoom=True,
        insight_lines=[
            "Last-month zoom of % of PR code changes completed before first human review",
            "Higher values suggest Copilot catches issues earlier, reducing post-review churn",
            "Runtime week-over-week dips can flag bursts of late rework",
            "Small windows are noisy — confirm trends over consecutive weeks",
        ],
    )


def chart_review_time_to_first_feedback_last_month(all_items, review_data, output_dir):
    chart_review_time_to_first_feedback(
        all_items, review_data, output_dir,
        cutoff_days=35,
        title="Time to First Feedback — Last Month (P50 hours, 4-week rolling)",
        output_file="review_time_to_first_feedback_last_month.png",
        month_zoom=True,
        insight_lines=[
            "Last-month zoom of first-feedback latency: Copilot (solid) vs human (dashed)",
            "Lower values are faster; Copilot should remain near-immediate",
            "Week-over-week human shifts expose review-bandwidth changes quickly",
            "Cap remains 1 week to prevent long-tail outliers from dominating",
        ],
    )


def chart_review_copilot_to_human_approval_last_month(all_items, review_data, output_dir):
    chart_review_copilot_to_human_approval(
        all_items, review_data, output_dir,
        cutoff_days=35,
        title="Copilot→Human Approval — Last Month (P50 hours, 4-week rolling)",
        output_file="review_copilot_to_approval_last_month.png",
        month_zoom=True,
        insight_lines=[
            "Last-month zoom of hours from first Copilot review to first human approval",
            "Lower is faster handoff from AI triage to human sign-off",
            "Useful for spotting week-over-week trust/throughput changes in runtime",
            "Only includes PRs that had both Copilot review and later human approval",
        ],
    )


def chart_review_human_participation_last_month(all_items, review_data, output_dir):
    chart_review_human_participation(
        all_items, review_data, output_dir,
        cutoff_days=35,
        title="Human Reviewer Participation — Last Month (4-week rolling mean)",
        output_file="review_human_participation_last_month.png",
        month_zoom=True,
        insight_lines=[
            "Last-month zoom of distinct human reviewers per merged PR",
            "Lower can mean efficiency gains; sharp drops can also indicate under-review",
            "Week-over-week movement helps validate whether changes are sustained",
            "Cross-check with merge rate and revert-rate charts for safety context",
        ],
    )


def chart_review_rubber_stamp_rate(all_items, review_data, output_dir):
    """Rubber Stamp Rate — % of Copilot-reviewed PRs where human approves with 0 comments.
    Rising = humans trust Copilot review more, find nothing to add."""

    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Human 'Rubber Stamp' Rate on Copilot-Reviewed PRs (4-week rolling)",
               "% of PRs approved with 0 human comments")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}%"))

    today = datetime.now().date()
    cutoff = today - timedelta(days=365)
    last_complete_week = week_start(today) - timedelta(weeks=1)

    visible_data = []
    line_ends = []
    combined_stamp_by_week = defaultdict(lambda: [0, 0])  # week -> [stamp_count, total_count]
    active_repos = _repos_with_copilot_activity(all_items, review_data)
    for repo in active_repos:
        items = all_items.get(repo)
        rd = review_data.get(repo)
        if not items or not rd:
            continue
        reviews_by_pr = rd["reviews_by_pr"]
        comments_by_pr = rd["comments_by_pr"]

        # For each Copilot-reviewed merged PR: was human's action just "approve, no comments"?
        stamp_by_week = defaultdict(lambda: [0, 0])  # week -> [stamp_count, total_count]
        for item in items:
            if not item.get("is_pr") or not item.get("merged_at"):
                continue
            num = item["number"]
            reviews = reviews_by_pr.get(num, [])
            if not any(_is_copilot_reviewer(r["author"]) for r in reviews):
                continue
            # Must have at least one human review action
            human_reviews = [r for r in reviews if r["author_type"] == "User"]
            if not human_reviews:
                continue
            cd = parse_date(item["created_at"])
            if not cd or cd < cutoff:
                continue

            # Count human comments on this PR
            human_comments = [c for c in comments_by_pr.get(num, [])
                              if not _is_copilot_reviewer(c["author"])
                              and c.get("author_type") == "User"]
            # A rubber stamp = human approved with no inline comments AND no
            # substantive review states (CHANGES_REQUESTED or COMMENTED)
            has_substantive_review = any(
                r["state"] in ("CHANGES_REQUESTED", "COMMENTED")
                for r in human_reviews
            )
            is_stamp = (len(human_comments) == 0
                        and not has_substantive_review
                        and any(r["state"] == "APPROVED" for r in human_reviews))
            wk = week_start(cd)
            stamp_by_week[wk][1] += 1
            combined_stamp_by_week[wk][1] += 1
            if is_stamp:
                stamp_by_week[wk][0] += 1
                combined_stamp_by_week[wk][0] += 1

        if not stamp_by_week:
            continue
        weeks_x, pcts = [], []
        w = max(min(stamp_by_week.keys()), cutoff)
        w = week_start(w)
        while w <= last_complete_week:
            window_stamps = 0
            window_total = 0
            for k in range(4):
                s, t = stamp_by_week.get(w - timedelta(weeks=k), (0, 0))
                window_stamps += s
                window_total += t
            if window_total >= 5:
                weeks_x.append(w)
                pcts.append(100.0 * window_stamps / window_total)
            w += timedelta(weeks=1)
        if not weeks_x:
            continue
        ax.plot(weeks_x, pcts, color=get_color(repo), label=get_short(repo),
                linewidth=2, alpha=0.85)
        visible_data.append(pcts)
        line_ends.append((weeks_x, pcts, get_short(repo), get_color(repo)))

    if not visible_data:
        plt.close(fig)
        print("  (skipping rubber stamp rate — no data)")
        return

    # Combined linear regression across all repos
    import numpy as np
    combined_x, combined_y = [], []
    w = min(combined_stamp_by_week.keys()) if combined_stamp_by_week else cutoff
    w = week_start(w)
    while w <= last_complete_week:
        window_stamps = 0
        window_total = 0
        for k in range(4):
            s, t = combined_stamp_by_week.get(w - timedelta(weeks=k), (0, 0))
            window_stamps += s
            window_total += t
        if window_total >= 5:
            combined_x.append(w)
            combined_y.append(100.0 * window_stamps / window_total)
        w += timedelta(weeks=1)
    if len(combined_x) >= 2:
        x_numeric = np.array([(d - combined_x[0]).days for d in combined_x], dtype=float)
        y_arr = np.array(combined_y, dtype=float)
        coeffs = np.polyfit(x_numeric, y_arr, 1)
        trend_y = np.polyval(coeffs, x_numeric)
        ax.plot(combined_x, trend_y, color="gray", linestyle=":", linewidth=1.5,
                alpha=0.7, label="Combined trend")

    ax.set_ylim(0, 100)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())
    ax.legend(loc="lower left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "up")
    add_insight_box(ax, [
        "Of Copilot-reviewed PRs, what % did the human approve with ZERO comments?",
        "Higher = humans trust Copilot's review, find nothing to add",
        "Only includes PRs where a human actually submitted a review",
        "Rising trend = growing confidence in automated first-pass review",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "review_rubber_stamp_rate.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_review_human_approval_speed(all_items, review_data, output_dir):
    """Human Approval Speed — hours from PR creation to first human APPROVED.
    Compares Copilot-reviewed PRs vs non-Copilot-reviewed PRs.
    If Copilot review is effective, humans approve faster on Copilot-reviewed PRs."""
    from statistics import median

    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Time to Human Approval (P50 hours, 4-week rolling)",
               "Hours from PR creation to first human APPROVED")

    today = datetime.now().date()
    cutoff = today - timedelta(days=365)
    last_complete_week = week_start(today) - timedelta(weeks=1)

    visible_data = []
    line_ends = []
    active_repos = _repos_with_copilot_activity(all_items, review_data)
    for repo in active_repos:
        items = all_items.get(repo)
        rd = review_data.get(repo)
        if not items or not rd:
            continue
        reviews_by_pr = rd["reviews_by_pr"]

        # Two series per repo: Copilot-reviewed and non-Copilot-reviewed
        hours_copilot_by_week = defaultdict(list)
        hours_nocopilot_by_week = defaultdict(list)
        for item in items:
            if not item.get("is_pr") or not item.get("merged_at"):
                continue
            num = item["number"]
            reviews = reviews_by_pr.get(num, [])
            human_reviews = [r for r in reviews if r["author_type"] == "User"]
            if not human_reviews:
                continue
            cd = parse_date(item["created_at"])
            if not cd or cd < cutoff:
                continue

            # First APPROVED by a human
            first_approval = None
            for r in human_reviews:
                if r["state"] == "APPROVED" and r["submitted_at"]:
                    first_approval = r["submitted_at"]
                    break
            if not first_approval or not item.get("created_at"):
                continue
            try:
                created_dt = datetime.fromisoformat(item["created_at"].replace("Z", "+00:00"))
                a_dt = datetime.fromisoformat(first_approval.replace("Z", "+00:00"))
                hours = (a_dt - created_dt).total_seconds() / 3600
                if hours < 0 or hours > 336:  # Cap at 2 weeks
                    continue
            except (ValueError, AttributeError):
                continue

            has_copilot = any(_is_copilot_reviewer(r["author"]) for r in reviews)
            wk = week_start(cd)
            if has_copilot:
                hours_copilot_by_week[wk].append(hours)
            else:
                hours_nocopilot_by_week[wk].append(hours)

        # Plot Copilot line (solid)
        for label_suffix, data_by_week, ls, lw in [
            ("Copilot", hours_copilot_by_week, "-", 2.5),
            ("No Copilot", hours_nocopilot_by_week, "--", 1.5),
        ]:
            if not data_by_week:
                continue
            weeks_x, p50s = [], []
            w = max(min(data_by_week.keys()), cutoff)
            w = week_start(w)
            while w <= last_complete_week:
                window_vals = []
                for k in range(4):
                    window_vals.extend(data_by_week.get(w - timedelta(weeks=k), []))
                if len(window_vals) >= 5:
                    weeks_x.append(w)
                    p50s.append(median(window_vals))
                w += timedelta(weeks=1)
            if not weeks_x:
                continue
            lbl = f"{get_short(repo)} {label_suffix}"
            ax.plot(weeks_x, p50s, color=get_color(repo), linestyle=ls,
                    linewidth=lw, label=lbl, alpha=0.85)
            visible_data.append(p50s)
            line_ends.append((weeks_x, p50s, lbl, get_color(repo)))

    if not visible_data:
        plt.close(fig)
        print("  (skipping human approval speed — no data)")
        return
    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(0, ymax)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())
    ax.legend(loc="upper right", fontsize=9)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "down")
    add_insight_box(ax, [
        "Total time from PR creation to first human APPROVED review (P50)",
        "Solid = Copilot-reviewed PRs; Dashed = non-Copilot PRs",
        "If Copilot pre-screens effectively, humans approve faster (less to check)",
        "Includes Copilot review time + author fix time + human review time",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "review_human_approval_speed.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_review_iteration_count(all_items, review_data, output_dir):
    """Review Iteration Rate — % of PRs that receive ≥1 CHANGES_REQUESTED before merge.
    Compares Copilot-reviewed PRs vs non-Copilot-reviewed PRs."""

    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "% of PRs Receiving CHANGES_REQUESTED (4-week rolling)",
               "% of merged PRs with ≥1 change request")

    today = datetime.now().date()
    cutoff = today - timedelta(days=365)
    last_complete_week = week_start(today) - timedelta(weeks=1)

    visible_data = []
    line_ends = []
    active_repos = _repos_with_copilot_activity(all_items, review_data)
    for repo in active_repos:
        items = all_items.get(repo)
        rd = review_data.get(repo)
        if not items or not rd:
            continue
        reviews_by_pr = rd["reviews_by_pr"]

        # Track total and "has changes_requested" per week
        copilot_total_by_week = defaultdict(int)
        copilot_cr_by_week = defaultdict(int)
        nocopilot_total_by_week = defaultdict(int)
        nocopilot_cr_by_week = defaultdict(int)

        for item in items:
            if not item.get("is_pr") or not item.get("merged_at"):
                continue
            num = item["number"]
            reviews = reviews_by_pr.get(num, [])
            human_reviews = [r for r in reviews if r["author_type"] == "User"]
            if not human_reviews:
                continue
            cd = parse_date(item["created_at"])
            if not cd or cd < cutoff:
                continue

            has_cr = any(r["state"] == "CHANGES_REQUESTED" for r in human_reviews)
            has_copilot = any(_is_copilot_reviewer(r["author"]) for r in reviews)
            wk = week_start(cd)
            if has_copilot:
                copilot_total_by_week[wk] += 1
                if has_cr:
                    copilot_cr_by_week[wk] += 1
            else:
                nocopilot_total_by_week[wk] += 1
                if has_cr:
                    nocopilot_cr_by_week[wk] += 1

        for label_suffix, total_by_week, cr_by_week, ls, lw in [
            ("Copilot", copilot_total_by_week, copilot_cr_by_week, "-", 2.5),
            ("No Copilot", nocopilot_total_by_week, nocopilot_cr_by_week, "--", 1.5),
        ]:
            if not total_by_week:
                continue
            weeks_x, rates = [], []
            w = max(min(total_by_week.keys()), cutoff)
            w = week_start(w)
            while w <= last_complete_week:
                total = sum(total_by_week.get(w - timedelta(weeks=k), 0) for k in range(4))
                cr = sum(cr_by_week.get(w - timedelta(weeks=k), 0) for k in range(4))
                if total >= 5:
                    weeks_x.append(w)
                    rates.append(100 * cr / total)
                w += timedelta(weeks=1)
            if not weeks_x:
                continue
            lbl = f"{get_short(repo)} {label_suffix}"
            ax.plot(weeks_x, rates, color=get_color(repo), linestyle=ls,
                    linewidth=lw, label=lbl, alpha=0.85)
            visible_data.append(rates)
            line_ends.append((weeks_x, rates, lbl, get_color(repo)))

    if not visible_data:
        plt.close(fig)
        print("  (skipping review iteration count — no data)")
        return
    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(0, min(ymax, 100))
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())
    ax.legend(loc="upper right", fontsize=9)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "down")
    add_insight_box(ax, [
        "% of merged PRs where a human requested changes at least once",
        "Solid = Copilot-reviewed PRs; Dashed = non-Copilot PRs",
        "If Copilot catches issues upfront, fewer PRs need human change requests",
        "Lower = smoother path to merge, less back-and-forth",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "review_iteration_count.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_review_revert_rate(all_items, review_data, output_dir):
    """Revert/Fix-Recent Rate — PRs that appear to revert or fix a recently merged PR.
    Detected via title patterns: 'Revert "..."', 'revert #N', 'fix #N' referencing
    a PR merged within the last 7 days. Lower = higher quality merges."""
    import re

    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Revert / Fix-Recent Rate (4-week rolling, per 100 merged PRs)",
               "Reverts+fix-recent per 100 merged PRs")

    today = datetime.now().date()
    cutoff = today - timedelta(days=365)
    last_complete_week = week_start(today) - timedelta(weeks=1)

    # Patterns that indicate a revert or fix of a recent PR
    revert_re = re.compile(r'^Revert\b', re.IGNORECASE)
    fix_pr_re = re.compile(r'\bfix(?:es|ed)?\s*#(\d+)\b', re.IGNORECASE)

    visible_data = []
    line_ends = []
    for repo in REVIEW_CHART_REPOS:
        items = all_items.get(repo)
        if not items:
            continue

        # Build lookup: PR number -> merged_at date
        merged_prs = {}
        for item in items:
            if item.get("is_pr") and item.get("merged_at"):
                md = parse_date(item["merged_at"])
                if md:
                    merged_prs[item["number"]] = md

        reverts_by_week = defaultdict(int)
        total_by_week = defaultdict(int)
        for item in items:
            if not item.get("is_pr") or not item.get("merged_at"):
                continue
            md = parse_date(item["merged_at"])
            if not md or md < cutoff:
                continue
            wk = week_start(md)
            total_by_week[wk] += 1

            title = item.get("title", "")
            if not title:
                continue
            is_regression = False
            # Check for "Revert" in title
            if revert_re.search(title):
                is_regression = True
            else:
                # Check for "fix #N" where N was merged within 7 days
                match = fix_pr_re.search(title)
                if match:
                    ref_num = int(match.group(1))
                    ref_merged = merged_prs.get(ref_num)
                    if ref_merged and 0 <= (md - ref_merged).days <= 7:
                        is_regression = True

            if is_regression:
                reverts_by_week[wk] += 1

        if not total_by_week:
            continue
        weeks_x, rates = [], []
        w = max(min(total_by_week.keys()), cutoff)
        w = week_start(w)
        while w <= last_complete_week:
            window_reverts = 0
            window_total = 0
            for k in range(4):
                window_reverts += reverts_by_week.get(w - timedelta(weeks=k), 0)
                window_total += total_by_week.get(w - timedelta(weeks=k), 0)
            if window_total >= 10:
                weeks_x.append(w)
                rates.append(100.0 * window_reverts / window_total)
            w += timedelta(weeks=1)
        if not weeks_x:
            continue
        ax.plot(weeks_x, rates, color=get_color(repo), label=get_short(repo),
                linewidth=2, alpha=0.85)
        visible_data.append(rates)
        line_ends.append((weeks_x, rates, get_short(repo), get_color(repo)))

    if not visible_data:
        plt.close(fig)
        print("  (skipping revert rate — no data)")
        return
    ymin, ymax = robust_ylim(visible_data)
    ax.set_ylim(0, max(ymax, 2))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())
    ax.legend(loc="upper left", fontsize=10)
    label_line_ends(ax, line_ends)
    add_direction_arrow(ax, "down")
    add_insight_box(ax, [
        "PRs that revert or fix a recently-merged PR (within 7 days)",
        "Detected via title: 'Revert ...', 'fix #N' where #N merged <7d ago",
        "Proxy for defect escape rate — lower = higher quality at merge time",
        "Speculative: title-based detection is imperfect but directional",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "review_revert_rate.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_review_change_attribution(all_items, review_data, output_dir):
    """Attribute lines changed to: Copilot feedback, Human feedback, or Author-initiated.

    For each commit after the first review on a PR, find the most recent preceding
    review comment. If from Copilot → Copilot-driven. If from a human → human-driven.
    If no preceding comment (or >24h gap) → author-initiated.
    Commits before any review are excluded (those are the initial PR).
    """
    fig, ax = plt.subplots(figsize=(14, 6))
    title = "Change Attribution: Who Drives Post-Review Work?"
    ax.set_title(title, fontsize=14)
    _stamp_chart(ax, title)
    ax.set_ylabel("% of lines changed (4-week rolling)")

    repos = _repos_with_copilot_activity(all_items, review_data)
    # Aggregate across all repos for a single stacked view
    week_copilot = defaultdict(int)
    week_human = defaultdict(int)
    week_author = defaultdict(int)

    for repo in repos:
        rd = review_data.get(repo)
        if not rd:
            continue
        items_by_num = {it["number"]: it for it in all_items.get(repo, [])}
        comments_by_pr = rd.get("comments_by_pr", {})
        commits_by_pr = rd.get("commits_by_pr", {})
        reviews_by_pr = rd.get("reviews_by_pr", {})

        for num, commits in commits_by_pr.items():
            if num not in items_by_num:
                continue
            reviews = reviews_by_pr.get(num, [])
            comments = comments_by_pr.get(num, [])
            if not reviews and not comments:
                continue

            # Build timeline of all review comments (with source)
            comment_timeline = []
            for c in comments:
                if not c["created_at"]:
                    continue
                is_copilot = _is_copilot_reviewer(c["author"])
                comment_timeline.append((c["created_at"], is_copilot))
            comment_timeline.sort(key=lambda x: x[0])

            # Find first review of any kind to skip initial commits
            first_review_ts = None
            for r in reviews:
                if not r["submitted_at"]:
                    continue
                if first_review_ts is None or r["submitted_at"] < first_review_ts:
                    first_review_ts = r["submitted_at"]
            for c in comments:
                if not c["created_at"]:
                    continue
                if first_review_ts is None or c["created_at"] < first_review_ts:
                    first_review_ts = c["created_at"]

            if not first_review_ts:
                continue

            for commit in commits:
                cdate = commit["committed_date"]
                if not cdate or cdate <= first_review_ts:
                    continue  # Skip initial PR commits
                lines = commit["additions"] + commit["deletions"]
                if lines == 0:
                    continue

                week = week_start(datetime.fromisoformat(cdate.replace("Z", "+00:00")).date()).strftime("%Y-%m-%d")

                # Find most recent comment before this commit
                preceding_comment = None
                for cts, is_copilot in reversed(comment_timeline):
                    if cts < cdate:
                        preceding_comment = (cts, is_copilot)
                        break

                if preceding_comment is None:
                    week_author[week] += lines
                else:
                    # Check gap — if >24h since last comment, likely author-initiated
                    try:
                        t_comment = datetime.fromisoformat(preceding_comment[0].replace("Z", "+00:00"))
                        t_commit = datetime.fromisoformat(cdate.replace("Z", "+00:00"))
                        gap_hours = (t_commit - t_comment).total_seconds() / 3600
                    except (ValueError, TypeError):
                        gap_hours = 0

                    if gap_hours > 24:
                        week_author[week] += lines
                    elif preceding_comment[1]:  # is_copilot
                        week_copilot[week] += lines
                    else:
                        week_human[week] += lines

    # Build rolling 4-week percentages — start from Apr 2025 (sparse data before)
    cutoff_week = "2025-03-24"
    all_weeks = sorted(w for w in set(week_copilot) | set(week_human) | set(week_author)
                       if w >= cutoff_week)
    if len(all_weeks) < 4:
        plt.close(fig)
        print("  (skipping change attribution — insufficient data)")
        return

    dates, pct_copilot, pct_human, pct_author = [], [], [], []
    for i in range(3, len(all_weeks)):
        window = all_weeks[i-3:i+1]
        c = sum(week_copilot.get(w, 0) for w in window)
        h = sum(week_human.get(w, 0) for w in window)
        a = sum(week_author.get(w, 0) for w in window)
        total = c + h + a
        if total < 100:  # Minimum threshold
            continue
        dates.append(datetime.strptime(all_weeks[i], "%Y-%m-%d"))
        pct_copilot.append(100 * c / total)
        pct_human.append(100 * h / total)
        pct_author.append(100 * a / total)

    if not dates:
        plt.close(fig)
        print("  (skipping change attribution — insufficient data)")
        return

    ax.stackplot(dates, pct_copilot, pct_human, pct_author,
                 labels=["After Copilot comment", "After human comment", "Author-initiated (>24h gap)"],
                 colors=["#7c3aed", "#2563eb", "#94a3b8"], alpha=0.8)
    ax.set_ylim(0, 100)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())
    ax.legend(loc="upper right", fontsize=10)
    add_insight_box(ax, [
        "Attributes post-review commits by most recent preceding comment author",
        "Copilot-driven: commit within 24h of a Copilot comment",
        "Human-driven: commit within 24h of a human comment",
        "Author-initiated: >24h since last comment or no preceding comment",
        "Goal: Copilot-driven share grows → Copilot catches issues humans would have",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "review_change_attribution.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_review_thread_depth(all_items, review_data, output_dir):
    """Average comments per review thread, Copilot-initiated vs human-initiated.

    Threads are approximated by grouping comments on the same PR+file path.
    A thread is "Copilot-initiated" if the first comment on that path is from Copilot.
    """
    fig, ax = plt.subplots(figsize=(14, 6))
    title = "Review Thread Depth: Copilot vs Human-Initiated"
    ax.set_title(title, fontsize=14)
    _stamp_chart(ax, title)
    ax.set_ylabel("Avg comments per thread (4-week rolling)")

    repos = _repos_with_copilot_activity(all_items, review_data)
    week_cop_depths = defaultdict(list)  # week -> list of thread depths
    week_hum_depths = defaultdict(list)

    for repo in repos:
        rd = review_data.get(repo)
        if not rd:
            continue
        items_by_num = {it["number"]: it for it in all_items.get(repo, [])}
        comments_by_pr = rd.get("comments_by_pr", {})

        for num, comments in comments_by_pr.items():
            if num not in items_by_num:
                continue
            it = items_by_num[num]
            if not it.get("merged_at"):
                continue

            # Group comments by file path to approximate threads
            threads = defaultdict(list)
            for c in comments:
                threads[c["path"]].append(c)

            merge_week = week_start(datetime.fromisoformat(
                it["merged_at"].replace("Z", "+00:00")).date()).strftime("%Y-%m-%d")

            for path, thread_comments in threads.items():
                if not thread_comments:
                    continue
                depth = len(thread_comments)
                first_author = thread_comments[0]
                if _is_copilot_reviewer(first_author["author"]):
                    week_cop_depths[merge_week].append(depth)
                elif first_author["author_type"] == "User":
                    week_hum_depths[merge_week].append(depth)

    all_weeks = sorted(set(week_cop_depths) | set(week_hum_depths))
    if len(all_weeks) < 4:
        plt.close(fig)
        print("  (skipping thread depth — insufficient data)")
        return

    dates, cop_avgs, hum_avgs = [], [], []
    for i in range(3, len(all_weeks)):
        window = all_weeks[i-3:i+1]
        cop_vals = [d for w in window for d in week_cop_depths.get(w, [])]
        hum_vals = [d for w in window for d in week_hum_depths.get(w, [])]
        if len(cop_vals) < 5 and len(hum_vals) < 5:
            continue
        dates.append(datetime.strptime(all_weeks[i], "%Y-%m-%d"))
        cop_avgs.append(sum(cop_vals) / len(cop_vals) if cop_vals else None)
        hum_avgs.append(sum(hum_vals) / len(hum_vals) if hum_vals else None)

    if not dates:
        plt.close(fig)
        print("  (skipping thread depth — insufficient data)")
        return

    if any(v is not None for v in cop_avgs):
        ax.plot(dates, cop_avgs, color="#7c3aed", linewidth=2.5,
                label="Copilot-initiated threads")
    if any(v is not None for v in hum_avgs):
        ax.plot(dates, hum_avgs, color="#2563eb", linewidth=2.5,
                label="Human-initiated threads")

    visible = [v for v in cop_avgs + hum_avgs if v is not None]
    if visible:
        ymin, ymax = robust_ylim([visible])
        ax.set_ylim(max(0, ymin - 0.5), ymax + 0.5)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())
    ax.legend(loc="upper right", fontsize=10)
    add_direction_arrow(ax, "down")
    add_insight_box(ax, [
        "Threads approximated by comments on same PR + file path",
        "Copilot-initiated = first comment on file from Copilot",
        "Shallower Copilot threads = actionable, no back-and-forth needed",
        "Goal: Copilot threads converge to depth ~1 (comment → fix, done)",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "review_thread_depth.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_review_suggestion_velocity(all_items, review_data, output_dir):
    """Time from suggestion comment to next commit on the PR.

    Compares Copilot suggestions vs human suggestions.
    Only considers comments where body_has_suggestion is true.
    """
    fig, ax = plt.subplots(figsize=(14, 6))
    title = "Suggestion Response Time: Copilot vs Human"
    ax.set_title(title, fontsize=14)
    _stamp_chart(ax, title)
    ax.set_ylabel("P50 hours from suggestion to next commit (4-week rolling)")

    repos = _repos_with_copilot_activity(all_items, review_data)
    week_cop_hours = defaultdict(list)
    week_hum_hours = defaultdict(list)

    for repo in repos:
        rd = review_data.get(repo)
        if not rd:
            continue
        items_by_num = {it["number"]: it for it in all_items.get(repo, [])}
        comments_by_pr = rd.get("comments_by_pr", {})
        commits_by_pr = rd.get("commits_by_pr", {})

        for num, comments in comments_by_pr.items():
            if num not in items_by_num:
                continue
            it = items_by_num[num]
            if not it.get("merged_at"):
                continue
            commits = commits_by_pr.get(num, [])
            if not commits:
                continue

            merge_week = week_start(datetime.fromisoformat(
                it["merged_at"].replace("Z", "+00:00")).date()).strftime("%Y-%m-%d")

            # Only suggestion comments
            suggestions = [c for c in comments
                           if c.get("body_has_suggestion") and c["created_at"]]

            for sugg in suggestions:
                sugg_ts = sugg["created_at"]
                # Find next commit after this suggestion
                next_commit_ts = None
                for commit in commits:
                    if commit["committed_date"] and commit["committed_date"] > sugg_ts:
                        next_commit_ts = commit["committed_date"]
                        break
                if not next_commit_ts:
                    continue
                try:
                    t_sugg = datetime.fromisoformat(sugg_ts.replace("Z", "+00:00"))
                    t_commit = datetime.fromisoformat(next_commit_ts.replace("Z", "+00:00"))
                    hours = (t_commit - t_sugg).total_seconds() / 3600
                except (ValueError, TypeError):
                    continue
                if hours < 0 or hours > 168:  # Cap at 1 week
                    continue

                if _is_copilot_reviewer(sugg["author"]):
                    week_cop_hours[merge_week].append(hours)
                elif sugg["author_type"] == "User":
                    week_hum_hours[merge_week].append(hours)

    all_weeks = sorted(set(week_cop_hours) | set(week_hum_hours))
    if len(all_weeks) < 4:
        plt.close(fig)
        print("  (skipping suggestion velocity — insufficient data)")
        return

    dates, cop_p50, hum_p50 = [], [], []
    for i in range(3, len(all_weeks)):
        window = all_weeks[i-3:i+1]
        cop_vals = sorted(h for w in window for h in week_cop_hours.get(w, []))
        hum_vals = sorted(h for w in window for h in week_hum_hours.get(w, []))
        if len(cop_vals) < 3 and len(hum_vals) < 3:
            continue
        dates.append(datetime.strptime(all_weeks[i], "%Y-%m-%d"))
        cop_p50.append(cop_vals[len(cop_vals)//2] if cop_vals else None)
        hum_p50.append(hum_vals[len(hum_vals)//2] if hum_vals else None)

    if not dates:
        plt.close(fig)
        print("  (skipping suggestion velocity — insufficient data)")
        return

    if any(v is not None for v in cop_p50):
        ax.plot(dates, cop_p50, color="#7c3aed", linewidth=2.5,
                label="Copilot suggestions")
    if any(v is not None for v in hum_p50):
        ax.plot(dates, hum_p50, color="#2563eb", linewidth=2.5,
                label="Human suggestions")

    visible = [v for v in cop_p50 + hum_p50 if v is not None]
    if visible:
        ymin, ymax = robust_ylim([visible])
        ax.set_ylim(0, max(ymax, 1))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())
    ax.legend(loc="upper right", fontsize=10)
    add_direction_arrow(ax, "down")
    add_insight_box(ax, [
        "Time from a suggestion comment to the next commit on the PR",
        "Only includes comments with code suggestions (body_has_suggestion)",
        "Faster response = author finds suggestions actionable and trustworthy",
        "Copilot suggestions acted on quickly → high-quality, low-friction feedback",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "review_suggestion_velocity.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_review_first_response_time(all_items, review_data, output_dir):
    """Time from PR creation to first review feedback — Copilot vs Human.

    Shows 24/7 coverage benefit: Copilot responds in minutes, humans in hours.
    """
    fig, ax = plt.subplots(figsize=(14, 6))
    title = "First Review Response Time (P50 hours)"
    ax.set_title(title, fontsize=14)
    _stamp_chart(ax, title)
    ax.set_ylabel("Hours from PR creation to first review (4-week rolling)")

    repos = _repos_with_copilot_activity(all_items, review_data)
    week_cop_first = defaultdict(list)
    week_hum_first = defaultdict(list)

    for repo in repos:
        rd = review_data.get(repo)
        if not rd:
            continue
        items_by_num = {it["number"]: it for it in all_items.get(repo, [])}
        reviews_by_pr = rd.get("reviews_by_pr", {})
        comments_by_pr = rd.get("comments_by_pr", {})

        for num, it in items_by_num.items():
            if not it.get("merged_at") or not it.get("created_at"):
                continue

            created_week = week_start(datetime.fromisoformat(
                it["created_at"].replace("Z", "+00:00")).date()).strftime("%Y-%m-%d")

            reviews = reviews_by_pr.get(num, [])
            comments = comments_by_pr.get(num, [])

            # Find first Copilot and first human activity
            first_cop_ts = None
            first_hum_ts = None

            for r in reviews:
                ts = r.get("submitted_at")
                if not ts:
                    continue
                if _is_copilot_reviewer(r["author"]):
                    if first_cop_ts is None or ts < first_cop_ts:
                        first_cop_ts = ts
                elif r["author_type"] == "User":
                    if first_hum_ts is None or ts < first_hum_ts:
                        first_hum_ts = ts

            for c in comments:
                ts = c.get("created_at")
                if not ts:
                    continue
                if _is_copilot_reviewer(c["author"]):
                    if first_cop_ts is None or ts < first_cop_ts:
                        first_cop_ts = ts
                elif c["author_type"] == "User":
                    if first_hum_ts is None or ts < first_hum_ts:
                        first_hum_ts = ts

            created = it["created_at"]
            try:
                t_created = datetime.fromisoformat(created.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                continue

            if first_cop_ts:
                try:
                    t_cop = datetime.fromisoformat(first_cop_ts.replace("Z", "+00:00"))
                    hours = (t_cop - t_created).total_seconds() / 3600
                    if 0 <= hours <= 168:
                        week_cop_first[created_week].append(hours)
                except (ValueError, TypeError):
                    pass
            if first_hum_ts:
                try:
                    t_hum = datetime.fromisoformat(first_hum_ts.replace("Z", "+00:00"))
                    hours = (t_hum - t_created).total_seconds() / 3600
                    if 0 <= hours <= 168:
                        week_hum_first[created_week].append(hours)
                except (ValueError, TypeError):
                    pass

    all_weeks = sorted(set(week_cop_first) | set(week_hum_first))
    if len(all_weeks) < 4:
        plt.close(fig)
        print("  (skipping first response time — insufficient data)")
        return

    dates, cop_p50, hum_p50 = [], [], []
    for i in range(3, len(all_weeks)):
        window = all_weeks[i-3:i+1]
        cop_vals = sorted(h for w in window for h in week_cop_first.get(w, []))
        hum_vals = sorted(h for w in window for h in week_hum_first.get(w, []))
        if len(cop_vals) < 5 and len(hum_vals) < 5:
            continue
        dates.append(datetime.strptime(all_weeks[i], "%Y-%m-%d"))
        cop_p50.append(cop_vals[len(cop_vals)//2] if cop_vals else None)
        hum_p50.append(hum_vals[len(hum_vals)//2] if hum_vals else None)

    if not dates:
        plt.close(fig)
        print("  (skipping first response time — insufficient data)")
        return

    if any(v is not None for v in cop_p50):
        ax.plot(dates, cop_p50, color="#7c3aed", linewidth=2.5,
                label="Copilot first review")
    if any(v is not None for v in hum_p50):
        ax.plot(dates, hum_p50, color="#2563eb", linewidth=2.5,
                label="Human first review")

    visible = [v for v in cop_p50 + hum_p50 if v is not None]
    if visible:
        ymin, ymax = robust_ylim([visible])
        ax.set_ylim(0, max(ymax, 1))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())
    ax.legend(loc="upper right", fontsize=10)
    add_direction_arrow(ax, "down")
    add_insight_box(ax, [
        "Hours from PR creation to first review activity (review or comment)",
        "Copilot provides near-instant 24/7 first-pass feedback",
        "Human reviewers constrained by working hours & timezone",
        "Gap = author wait time that Copilot eliminates",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "review_first_response_time.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_review_rubber_stamp_safety(all_items, review_data, output_dir):
    """Safety check: among Copilot-reviewed PRs, do those without substantive
    human review have higher defect rates?

    Compares revert/fix-recent rate for Copilot-reviewed PRs only:
    - Copilot-only: no substantive human review (no human comments, no CHANGES_REQUESTED/COMMENTED)
    - Human + Copilot: has substantive human review alongside Copilot
    Non-Copilot PRs are excluded. If Copilot-only rates are similar or lower,
    Copilot review alone is sufficient for those PRs.
    """
    fig, ax = plt.subplots(figsize=(14, 6))
    title = "Defect Rate: Copilot-Only vs Human+Copilot Review"
    ax.set_title(title, fontsize=14)
    _stamp_chart(ax, title)
    ax.set_ylabel("Reverts + fix-recent per 100 merged PRs (4-week rolling)")

    repos = _repos_with_copilot_activity(all_items, review_data)
    import re
    revert_re = re.compile(r'^Revert\b', re.IGNORECASE)
    fix_re = re.compile(r'(?:fix|fixes|fixed|close|closes|closed|resolve|resolves|resolved)\s+#(\d+)',
                        re.IGNORECASE)

    week_cop_only_total = defaultdict(int)
    week_cop_only_defect = defaultdict(int)
    week_human_total = defaultdict(int)
    week_human_defect = defaultdict(int)

    for repo in repos:
        rd = review_data.get(repo)
        if not rd:
            continue
        items = all_items.get(repo, [])
        items_by_num = {it["number"]: it for it in items}
        reviews_by_pr = rd.get("reviews_by_pr", {})
        comments_by_pr = rd.get("comments_by_pr", {})

        # Build lookup: recently merged PRs for fix-recent detection
        merged_items = [it for it in items if it.get("merged_at")]
        merged_by_num = {}
        for it in merged_items:
            merged_by_num[it["number"]] = it

        for it in merged_items:
            num = it["number"]
            title = it.get("title", "") or ""
            merged_at = it["merged_at"]
            merge_week = week_start(datetime.fromisoformat(
                merged_at.replace("Z", "+00:00")).date()).strftime("%Y-%m-%d")

            reviews = reviews_by_pr.get(num, [])
            comments = comments_by_pr.get(num, [])

            # Classify review type
            has_copilot_review = any(_is_copilot_reviewer(r["author"]) for r in reviews)
            has_copilot_comment = any(_is_copilot_reviewer(c["author"]) for c in comments)
            has_human_comment = any(c["author_type"] == "User" for c in comments)
            has_human_review = any(r["author_type"] == "User" for r in reviews)

            if not has_copilot_review and not has_copilot_comment:
                continue  # Not Copilot-reviewed at all

            # Copilot-only = no human inline comments AND no substantive
            # human reviews (CHANGES_REQUESTED or COMMENTED)
            has_substantive_human = has_human_comment or any(
                r["author_type"] == "User" and r["state"] in ("CHANGES_REQUESTED", "COMMENTED")
                for r in reviews
            )
            is_copilot_only = not has_substantive_human

            # Detect defect
            is_defect = False
            if revert_re.search(title):
                is_defect = True
            for m in fix_re.finditer(title):
                ref_num = int(m.group(1))
                ref_item = merged_by_num.get(ref_num)
                if ref_item and ref_item.get("merged_at"):
                    try:
                        ref_merged = datetime.fromisoformat(
                            ref_item["merged_at"].replace("Z", "+00:00"))
                        this_merged = datetime.fromisoformat(
                            merged_at.replace("Z", "+00:00"))
                        if (this_merged - ref_merged).days <= 7:
                            is_defect = True
                    except (ValueError, TypeError):
                        pass

            if is_copilot_only:
                week_cop_only_total[merge_week] += 1
                if is_defect:
                    week_cop_only_defect[merge_week] += 1
            else:
                week_human_total[merge_week] += 1
                if is_defect:
                    week_human_defect[merge_week] += 1

    all_weeks = sorted(set(week_cop_only_total) | set(week_human_total))
    if len(all_weeks) < 4:
        plt.close(fig)
        print("  (skipping rubber stamp safety — insufficient data)")
        return

    dates, cop_rates, hum_rates = [], [], []
    for i in range(3, len(all_weeks)):
        window = all_weeks[i-3:i+1]
        cop_tot = sum(week_cop_only_total.get(w, 0) for w in window)
        cop_def = sum(week_cop_only_defect.get(w, 0) for w in window)
        hum_tot = sum(week_human_total.get(w, 0) for w in window)
        hum_def = sum(week_human_defect.get(w, 0) for w in window)
        if cop_tot < 3 and hum_tot < 3:
            continue
        dates.append(datetime.strptime(all_weeks[i], "%Y-%m-%d"))
        cop_rates.append(100 * cop_def / cop_tot if cop_tot else None)
        hum_rates.append(100 * hum_def / hum_tot if hum_tot else None)

    if not dates:
        plt.close(fig)
        print("  (skipping rubber stamp safety — insufficient data)")
        return

    if any(v is not None for v in cop_rates):
        ax.plot(dates, cop_rates, color="#7c3aed", linewidth=2.5,
                label="Copilot-only reviewed")
    if any(v is not None for v in hum_rates):
        ax.plot(dates, hum_rates, color="#2563eb", linewidth=2.5,
                label="Human + Copilot reviewed")

    visible = [v for v in cop_rates + hum_rates if v is not None]
    if visible:
        ymin, ymax = robust_ylim([visible])
        ax.set_ylim(0, max(ymax, 1))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())
    ax.legend(loc="upper right", fontsize=10)
    add_direction_arrow(ax, "down")
    add_insight_box(ax, [
        "Among Copilot-reviewed PRs only (non-Copilot PRs excluded)",
        "Copilot-only = no human comments or substantive reviews",
        "Human+Copilot = has human comments/CHANGES_REQUESTED/COMMENTED",
        "If Copilot-only rate is higher: human review adds safety value",
        "⚠️ Selection bias: Copilot-only PRs may differ in complexity",
    ])
    fig.tight_layout()
    path = os.path.join(output_dir, "review_rubber_stamp_safety.png")
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

    print("Loading first-comment data...")
    all_first_comments = {}
    for repo in repos:
        if repo in COMMENT_CHART_REPOS:
            fc = load_first_comments(conn, repo)
            if fc:
                all_first_comments[repo] = fc
                print(f"  {repo}: {len(fc)} PRs with comment data")
            else:
                print(f"  {repo}: no comment data")
    print()

    print("Loading push-event data...")
    all_push_events = {}
    for repo in repos:
        if repo in PUSH_CHART_REPOS:
            pe = load_push_events(conn, repo)
            if pe:
                all_push_events[repo] = pe
                print(f"  {repo}: {len(pe)} PRs with push events")
    print()

    print("Loading review data...")
    all_review_data = {}
    for repo in repos:
        if repo in REVIEW_CHART_REPOS:
            rd = load_review_data(conn, repo)
            if rd:
                all_review_data[repo] = rd
                print(f"  {repo}: {len(rd['reviews_by_pr'])} PRs with reviews, "
                      f"{len(rd['commits_by_pr'])} with commit stats")
            else:
                print(f"  {repo}: no review data")
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
        chart_pr_merge_rate_12m(all_items, output_dir)
        chart_pr_merge_rate_zoomed(all_items, output_dir)
        chart_net_pr_flow_comparison(all_series, output_dir)
        chart_pr_opened_vs_merged_zoomed(all_items, output_dir)
        chart_sustainability_score(all_series, output_dir)
        chart_time_to_merge(all_ttm, output_dir)
        if all_first_comments:
            chart_time_to_comment(all_items, all_first_comments, output_dir)
            chart_copilot_time_to_comment(all_items, all_first_comments, output_dir)
            chart_time_comment_to_merge(all_items, all_first_comments, output_dir)
            chart_copilot_time_comment_to_merge(all_items, all_first_comments, output_dir)
        if all_push_events:
            chart_pushes_per_pr_over_time(all_items, all_push_events, output_dir)
        chart_open_pr_age(all_items, output_dir)
        chart_issue_close_rate(all_items, output_dir)
        if has_maintainer_data:
            chart_active_maintainers(all_maint, output_dir)
            chart_prs_per_maintainer(all_maint, output_dir)
            chart_open_issues_per_maintainer(all_series, all_maint, output_dir)
            chart_open_prs_per_maintainer(all_series, all_maint, output_dir)
            chart_contributor_diversity(all_items, output_dir)
            chart_copilot_adoption(all_items, output_dir)
            chart_copilot_merge_success(all_items, output_dir)
            chart_copilot_time_to_merge(all_items, output_dir)
            chart_copilot_time_to_merge_azure(all_items, output_dir)
            chart_issue_community(all_items, output_dir)
            chart_community_issue_volume(all_items, output_dir)
            chart_community_issue_share(all_items, output_dir)
            chart_community_pr_share(all_items, output_dir)
            chart_community_responsiveness(all_items, all_maint, output_dir)
            chart_community_time_to_close(all_items, output_dir)
            chart_community_issue_age(all_items, output_dir)
            chart_community_pareto(all_items, output_dir)
            chart_community_retention(all_items, output_dir)
            chart_community_merge_latency(all_items, output_dir)
            chart_gini_over_time(all_items, output_dir)
        else:
            print("  (skipping maintainer charts — no author/merged_by data yet)")

    # Per-repo dashboards
    for repo in repos:
        chart_per_repo_dashboard(repo, all_series.get(repo), output_dir)

    # Review metric charts (Copilot Code Review effectiveness)
    if all_review_data:
        print("\n  --- Review Metrics (Copilot Code Review) ---")
        chart_review_copilot_coverage(all_items, all_review_data, output_dir)
        chart_review_copilot_coverage_last_month(all_items, all_review_data, output_dir)
        chart_review_copilot_comment_density(all_items, all_review_data, output_dir)
        chart_review_copilot_comment_density_last_month(all_items, all_review_data, output_dir)
        chart_review_suggestion_rate(all_items, all_review_data, output_dir)
        chart_review_suggestion_rate_last_month(all_items, all_review_data, output_dir)
        chart_review_human_comments_comparison(all_items, all_review_data, output_dir)
        chart_review_human_comments_comparison_last_month(all_items, all_review_data, output_dir)
        chart_review_churn_before_human(all_items, all_review_data, output_dir)
        chart_review_churn_before_human_last_month(all_items, all_review_data, output_dir)
        chart_review_time_to_first_feedback(all_items, all_review_data, output_dir)
        chart_review_time_to_first_feedback_last_month(all_items, all_review_data, output_dir)
        chart_review_copilot_to_human_approval(all_items, all_review_data, output_dir)
        chart_review_copilot_to_human_approval_last_month(all_items, all_review_data, output_dir)
        chart_review_human_participation(all_items, all_review_data, output_dir)
        chart_review_human_participation_last_month(all_items, all_review_data, output_dir)
        chart_review_rubber_stamp_rate(all_items, all_review_data, output_dir)
        chart_review_human_approval_speed(all_items, all_review_data, output_dir)
        chart_review_iteration_count(all_items, all_review_data, output_dir)
        chart_review_revert_rate(all_items, all_review_data, output_dir)
        chart_review_change_attribution(all_items, all_review_data, output_dir)
        chart_review_thread_depth(all_items, all_review_data, output_dir)
        chart_review_suggestion_velocity(all_items, all_review_data, output_dir)
        chart_review_first_response_time(all_items, all_review_data, output_dir)
        chart_review_rubber_stamp_safety(all_items, all_review_data, output_dir)

    write_chart_registry(output_dir)
    conn.close()
    print(f"\nDone! Charts saved to {os.path.abspath(output_dir)}/")


if __name__ == "__main__":
    main()
