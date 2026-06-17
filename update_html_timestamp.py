#!/usr/bin/env python3
"""Update the data-freshness timestamp in charts/index.html"""

import csv
import re
from pathlib import Path
from datetime import datetime, timezone

TIMESTAMP_ATTR = "data-gathered-up-to"
SUBTITLE_RE = re.compile(r'<p class="subtitle">([^<]+)</p>')


def parse_iso_timestamp(value):
    """Parse an ISO-8601 timestamp into a timezone-aware datetime."""
    if not value:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def get_displayed_repos(html_content):
    """Return the repos listed in the page subtitle."""
    match = SUBTITLE_RE.search(html_content)
    if not match:
        raise RuntimeError("Failed to find the repo subtitle in charts/index.html")
    return {repo.strip() for repo in match.group(1).split("·") if repo.strip()}


def get_data_gathered_up_to(data_path, html_path):
    """Return the least-fresh complete fetch watermark for repos shown on the page."""
    with open(html_path, "r", encoding="utf-8") as f:
        displayed_repos = get_displayed_repos(f.read())

    latest_by_repo = {}
    with open(data_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("status") != "complete":
                continue
            repo = row.get("repo", "")
            if repo not in displayed_repos:
                continue
            parsed = parse_iso_timestamp(row.get("sync_started_at", ""))
            if parsed is None:
                continue
            current = latest_by_repo.get(repo)
            if current is None or parsed > current:
                latest_by_repo[repo] = parsed

    missing_repos = displayed_repos - latest_by_repo.keys()
    if missing_repos:
        missing = ", ".join(sorted(missing_repos))
        raise RuntimeError(f"Missing complete sync_started_at data for displayed repos: {missing}")
    if not latest_by_repo:
        raise RuntimeError(f"Failed to find a parseable complete sync_started_at in {data_path}")
    return min(latest_by_repo.values()).isoformat()


def update_timestamp(html_path, data_timestamp):
    """Update the data freshness attribute in the HTML file."""

    # Read the current HTML
    with open(html_path, "r", encoding="utf-8") as f:
        html_content = f.read()

    if f'{TIMESTAMP_ATTR}=' in html_content:
        html_content, replacements = re.subn(
            rf'{TIMESTAMP_ATTR}="[^"]*"',
            f'{TIMESTAMP_ATTR}="{data_timestamp}"',
            html_content
        )
        if replacements == 0:
            raise RuntimeError(f"Failed to update {TIMESTAMP_ATTR} in {html_path}")
    elif 'data-updated=' in html_content:
        html_content, replacements = re.subn(
            r'data-updated="[^"]*"',
            f'{TIMESTAMP_ATTR}="{data_timestamp}"',
            html_content
        )
        if replacements == 0:
            raise RuntimeError(f"Failed to replace data-updated in {html_path}")
    else:
        def add_data_attr(match):
            if TIMESTAMP_ATTR in match.group(0):
                return match.group(0)
            return match.group(1) + f' {TIMESTAMP_ATTR}="{data_timestamp}"' + match.group(2)

        html_content, replacements = re.subn(
            r'(<p class="attribution")([^>]*>)',
            add_data_attr,
            html_content,
            count=1
        )
        if replacements == 0:
            raise RuntimeError(f"Failed to add {TIMESTAMP_ATTR} in {html_path}")

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"Updated data freshness timestamp in {html_path} to {data_timestamp}")

if __name__ == "__main__":
    repo_root = Path(__file__).resolve().parent
    html_path = repo_root / "charts" / "index.html"
    data_path = repo_root / "data" / "fetch_progress.csv"
    update_timestamp(html_path, get_data_gathered_up_to(data_path, html_path))
