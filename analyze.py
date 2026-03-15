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
    "dotnet/roslyn": "#68217A",    # VS purple
    "dotnet/maui": "#1E88E5",      # MAUI blue
    "microsoft/vscode": "#007ACC", # VS Code blue
    "rust-lang/rust": "#DEA584",   # Rust orange
    "golang/go": "#00ADD8",        # Go cyan
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

# Repos where a bot merges all PRs — merged_by is useless for maintainer analysis
BOT_MERGER_REPOS = {"rust-lang/rust"}

# Known bot accounts to exclude from maintainer counts
BOT_ACCOUNTS = {"bors", "rust-bors", "dotnet-bot", "dependabot[bot]", "github-actions[bot]",
                "renovate[bot]", "copilot-swe-agent[bot]"}


def get_color(repo):
    return REPO_COLORS.get(repo, "#888888")


def get_short(repo):
    return REPO_SHORT.get(repo, repo)


def load_items(conn, repo):
    """Load all items for a repo, sorted by created_at."""
    rows = conn.execute(
        "SELECT number, created_at, closed_at, state, is_pull_request, merged_at, "
        "author, merged_by "
        "FROM items WHERE repo = ? ORDER BY created_at",
        (repo,)
    ).fetchall()
    items = []
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
        })
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
    Compute median time-to-merge (in days) per month for merged PRs.
    Returns (months, medians) lists.
    """
    from statistics import median

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
    medians = [median(merge_times_by_month[m]) for m in months]
    return months, medians


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
            # Only include prior month if it's actually adjacent
            if (m - prev).days <= 62:
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


def thousands_formatter(x, pos):
    if x >= 1000:
        return f"{x/1000:.0f}K"
    return f"{x:.0f}"


def setup_axes(ax, title, ylabel):
    ax.set_title(title, fontsize=13, fontweight="bold", pad=10)
    ax.set_ylabel(ylabel, fontsize=10)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.yaxis.set_major_formatter(FuncFormatter(thousands_formatter))
    ax.grid(True, alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


def chart_open_issues_comparison(all_series, output_dir):
    """Open issues over time, all repos overlaid."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Open Issues Over Time", "Open Issues")

    for repo, series in all_series.items():
        if not series:
            continue
        ax.plot(series["weeks"], series["open_issues"],
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)

    ax.legend(loc="upper left", fontsize=10)
    fig.tight_layout()
    path = os.path.join(output_dir, "open_issues_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_open_prs_comparison(all_series, output_dir):
    """Open PRs over time, all repos overlaid. Excludes Gerrit repos."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Open Pull Requests Over Time", "Open PRs")

    for repo, series in all_series.items():
        if not series or repo in GERRIT_REPOS:
            continue
        ax.plot(series["weeks"], series["open_prs"],
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)

    ax.legend(loc="upper left", fontsize=10)
    ax.annotate("Note: golang/go excluded (uses Gerrit, not GitHub PRs)",
                xy=(0.02, 0.02), xycoords="axes fraction", fontsize=8,
                color="#888888", style="italic")
    fig.tight_layout()
    path = os.path.join(output_dir, "open_prs_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_net_flow_comparison(all_series, output_dir):
    """Net issue flow (opened - closed per week), smoothed, Y-axis clamped."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Net Issue Flow (Opened - Closed per Week, 4-week avg)",
               "Net Issues / Week")

    ax.axhline(y=0, color="black", linewidth=0.5, alpha=0.5)

    for repo, series in all_series.items():
        if not series:
            continue
        smoothed = smooth(series["net_issue_flow"], window=4)
        ax.plot(series["weeks"], smoothed,
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)

    # Clamp Y-axis to exclude one-time mass-closure spikes
    ax.set_ylim(-200, 200)
    ax.annotate("Y-axis clamped to [-200, 200] to exclude one-time mass closures",
                xy=(0.02, 0.02), xycoords="axes fraction", fontsize=8,
                color="#888888", style="italic")
    ax.legend(loc="upper left", fontsize=10)
    fig.tight_layout()
    path = os.path.join(output_dir, "net_issue_flow_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_pr_merge_rate_comparison(all_series, output_dir):
    """PR merge rate (merged per week), smoothed. Excludes Gerrit repos."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "PR Merge Rate (Merged per Week, 4-week avg)",
               "PRs Merged / Week")

    for repo, series in all_series.items():
        if not series or repo in GERRIT_REPOS:
            continue
        smoothed = smooth(series["pr_merged"], window=4)
        ax.plot(series["weeks"], smoothed,
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)

    ax.legend(loc="upper left", fontsize=10)
    ax.annotate("Note: golang/go excluded (uses Gerrit, not GitHub PRs)",
                xy=(0.02, 0.02), xycoords="axes fraction", fontsize=8,
                color="#888888", style="italic")
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
    ax.plot(weeks, series["open_prs"], color=color, linewidth=1.5)
    ax.fill_between(weeks, series["open_prs"], alpha=0.15, color=color)

    # Panel 3: Issue inflow vs outflow (smoothed, clamped)
    ax = axes[1, 0]
    setup_axes(ax, "Issues: Opened vs Closed (4-week avg)", "Per Week")
    opened_smooth = smooth(series["issue_opened"])
    closed_smooth = smooth(series["issue_closed"])
    ax.plot(weeks, opened_smooth, color="#E74C3C",
            label="Opened", linewidth=1.2, alpha=0.8)
    ax.plot(weeks, closed_smooth, color="#27AE60",
            label="Closed", linewidth=1.2, alpha=0.8)
    # Clamp to p95 to exclude mass-closure spikes
    all_vals = opened_smooth + closed_smooth
    if all_vals:
        p95 = sorted(all_vals)[int(len(all_vals) * 0.95)]
        ax.set_ylim(0, p95 * 1.5)
    ax.legend(fontsize=9)

    # Panel 4: PRs opened vs merged (smoothed, clamped)
    ax = axes[1, 1]
    setup_axes(ax, "PRs: Opened vs Merged (4-week avg)", "Per Week")
    pr_open_smooth = smooth(series["pr_opened"])
    pr_merge_smooth = smooth(series["pr_merged"])
    ax.plot(weeks, pr_open_smooth, color="#E74C3C",
            label="Opened", linewidth=1.2, alpha=0.8)
    ax.plot(weeks, pr_merge_smooth, color="#27AE60",
            label="Merged", linewidth=1.2, alpha=0.8)
    all_vals = pr_open_smooth + pr_merge_smooth
    if all_vals:
        p95 = sorted(all_vals)[int(len(all_vals) * 0.95)]
        ax.set_ylim(0, p95 * 1.5)
    ax.legend(fontsize=9)

    fig.tight_layout(rect=[0, 0, 1, 0.96])
    safe_name = repo.replace("/", "_")
    path = os.path.join(output_dir, f"dashboard_{safe_name}.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_sustainability_score(all_series, output_dir):
    """
    Cumulative net flow normalized: shows whether backlog is growing
    as a percentage of total items ever opened.
    """
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Issue Backlog Growth (Open Issues as % of Total Ever Opened)",
               "% Still Open")

    for repo, series in all_series.items():
        if not series:
            continue
        weeks = series["weeks"]
        cumulative_opened = []
        total = 0
        for v in series["issue_opened"]:
            total += v
            cumulative_opened.append(total)

        pct_open = []
        for i, oi in enumerate(series["open_issues"]):
            if cumulative_opened[i] > 0:
                pct_open.append(100.0 * oi / cumulative_opened[i])
            else:
                pct_open.append(0)

        ax.plot(weeks, smooth(pct_open, 4),
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)

    ax.set_ylim(0, None)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}%"))
    ax.legend(loc="upper right", fontsize=10)
    fig.tight_layout()
    path = os.path.join(output_dir, "sustainability_score.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_time_to_merge(all_ttm, output_dir):
    """Median time-to-merge (days) per month, all repos. Excludes Gerrit repos."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Median Time to Merge PRs (Monthly, 3-month avg)", "Days")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}"))

    for repo, (months, medians) in all_ttm.items():
        if not months or repo in GERRIT_REPOS:
            continue
        smoothed = smooth(medians, window=3)
        ax.plot(months, smoothed,
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)

    ax.set_ylim(0, None)
    ax.legend(loc="upper left", fontsize=10)
    ax.annotate("Note: golang/go excluded (uses Gerrit, not GitHub PRs)",
                xy=(0.02, 0.02), xycoords="axes fraction", fontsize=8,
                color="#888888", style="italic")
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
    for repo, (months, maintainers, _, _) in all_maint.items():
        if not months or repo in excluded:
            continue
        ax.plot(months, smooth(maintainers, 3),
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)

    ax.set_ylim(0, None)
    ax.legend(loc="upper left", fontsize=10)
    notes = "Maintainer = anyone who merged a PR in the month or prior month (bots excluded)"
    excl_names = ", ".join(sorted(get_short(r) for r in excluded))
    notes += f"\nExcluded: {excl_names} (Gerrit/bot-merger workflows)"
    ax.annotate(notes, xy=(0.02, 0.02), xycoords="axes fraction", fontsize=8,
                color="#888888", style="italic")
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
    for repo, (months, _, prs_per, _) in all_maint.items():
        if not months or repo in excluded:
            continue
        ax.plot(months, smooth(prs_per, 3),
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)

    ax.set_ylim(0, None)
    ax.legend(loc="upper left", fontsize=10)
    notes = "Higher = more throughput per person (or fewer maintainers doing more work)"
    excl_names = ", ".join(sorted(get_short(r) for r in excluded))
    notes += f"\nExcluded: {excl_names} (Gerrit/bot-merger workflows)"
    ax.annotate(notes, xy=(0.02, 0.02), xycoords="axes fraction", fontsize=8,
                color="#888888", style="italic")
    fig.tight_layout()
    path = os.path.join(output_dir, "prs_per_maintainer_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_contributor_diversity(all_items, output_dir):
    """Distinct PR authors per month — measures community contribution breadth."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Distinct PR Authors per Month (3-month avg)",
               "Unique Authors")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}"))

    for repo, items in all_items.items():
        if repo in GERRIT_REPOS:
            continue
        authors_by_month = defaultdict(set)
        for item in items:
            if not item["is_pr"]:
                continue
            cd = parse_date(item["created_at"])
            author = item.get("author")
            if not cd or not author:
                continue
            authors_by_month[cd.replace(day=1)].add(author)

        if not authors_by_month:
            continue
        months = sorted(authors_by_month.keys())
        counts = [len(authors_by_month[m]) for m in months]
        ax.plot(months, smooth(counts, 3),
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)

    ax.set_ylim(0, None)
    ax.legend(loc="upper left", fontsize=10)
    fig.tight_layout()
    path = os.path.join(output_dir, "contributor_diversity_comparison.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  {path}")


def chart_issue_close_rate(all_series, output_dir):
    """Percentage of issues closed within 30 days of opening, by month."""
    fig, ax = plt.subplots(figsize=(14, 7))
    setup_axes(ax, "Issue Responsiveness (% Closed Within 30 Days, 3-month avg)",
               "% Closed <30d")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"{x:.0f}%"))

    for repo, items in all_series.items():
        monthly_total = defaultdict(int)
        monthly_fast = defaultdict(int)
        for item in items:
            if item["is_pr"]:
                continue
            cd = parse_date(item["created_at"])
            cld = parse_date(item["closed_at"])
            if not cd:
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
        ax.plot(list(vm), smooth(list(vp), 3),
                color=get_color(repo), label=get_short(repo),
                linewidth=1.5, alpha=0.85)

    ax.set_ylim(0, 100)
    ax.legend(loc="upper right", fontsize=10)
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
        print(f"  {repo}: {count_map.get(0, 0):,} issues, {count_map.get(1, 0):,} PRs")
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
