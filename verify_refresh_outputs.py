#!/usr/bin/env python3
"""Sanity-check refreshed data and chart outputs before auto-merging.

Compares the current working tree outputs against the last committed baseline
(from a git ref, defaulting to HEAD) and rejects obvious regressions such as
shrinking item history, per-repo date coverage moving backwards, missing chart
files, or empty chart files.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple


def parse_iso_timestamp(value: str) -> Optional[datetime]:
    """Parse an ISO-8601 timestamp into a tz-aware UTC datetime.

    Tolerates the two variants this repo produces: a trailing 'Z' or an
    explicit '+00:00' offset, with or without fractional seconds. Returns
    None for empty/unparseable input so callers can skip rather than crash.
    """
    if not value:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def git_show_bytes(ref: str, repo_relative_path: str) -> Optional[bytes]:
    result = subprocess.run(
        ["git", "show", f"{ref}:{repo_relative_path}"],
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    return result.stdout


def open_csv_from_git(ref: str, repo_relative_path: str) -> Optional[io.TextIOBase]:
    raw = git_show_bytes(ref, repo_relative_path)
    if raw is None:
        return None
    if repo_relative_path.endswith(".gz"):
        return io.TextIOWrapper(gzip.GzipFile(fileobj=io.BytesIO(raw)), encoding="utf-8", newline="")
    return io.StringIO(raw.decode("utf-8"), newline="")


def open_csv_from_worktree(path: Path) -> io.TextIOBase:
    if str(path).endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", newline="")
    return path.open("r", encoding="utf-8", newline="")


def summarize_items(reader: csv.DictReader[str]) -> Tuple[int, Dict[str, str]]:
    total = 0
    max_by_repo: Dict[str, Tuple[datetime, str]] = {}
    for row in reader:
        total += 1
        repo = row.get("repo", "")
        created_at = row.get("created_at", "")
        if not repo or not created_at:
            continue
        parsed = parse_iso_timestamp(created_at)
        if parsed is None:
            continue
        current = max_by_repo.get(repo)
        if current is None or parsed > current[0]:
            max_by_repo[repo] = (parsed, created_at)
    return total, {repo: original for repo, (_, original) in max_by_repo.items()}


def summarize_progress(reader: csv.DictReader[str], timestamp_field: str) -> Tuple[int, str]:
    total = 0
    max_dt: Optional[datetime] = None
    max_original = ""
    for row in reader:
        total += 1
        timestamp = row.get(timestamp_field, "")
        parsed = parse_iso_timestamp(timestamp)
        if parsed is None:
            continue
        if max_dt is None or parsed > max_dt:
            max_dt = parsed
            max_original = timestamp
    return total, max_original


def list_tracked_files(ref: str, prefix: str) -> list[str]:
    result = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", ref, prefix],
        capture_output=True,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip()
        print(
            f"Warning: could not list tracked files at {ref}:{prefix}"
            + (f" ({stderr})" if stderr else "")
        )
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def compare_items(baseline_ref: str, repos: Iterable[str]) -> list[str]:
    problems: list[str] = []
    current_path = Path("data/items.csv.gz")
    if not current_path.exists():
        return ["Current data/items.csv.gz is missing"]

    with open_csv_from_worktree(current_path) as current_file:
        current_total, current_max = summarize_items(csv.DictReader(current_file))

    baseline_file = open_csv_from_git(baseline_ref, "data/items.csv.gz")
    if baseline_file is None:
        print("No baseline data/items.csv.gz found; skipping item-history comparison.")
        return problems
    with baseline_file:
        baseline_total, baseline_max = summarize_items(csv.DictReader(baseline_file))

    print(f"items.csv.gz rows: {baseline_total:,} -> {current_total:,}")
    if current_total < baseline_total:
        problems.append(f"items.csv.gz row count regressed: {baseline_total} -> {current_total}")

    for repo in repos:
        before = baseline_max.get(repo, "")
        after = current_max.get(repo, "")
        before_dt = parse_iso_timestamp(before)
        after_dt = parse_iso_timestamp(after)
        if before_dt and after_dt and after_dt < before_dt:
            problems.append(f"{repo} max created_at regressed: {before} -> {after}")
        elif before and not after:
            problems.append(f"{repo} lost all items (baseline max created_at {before})")
        else:
            print(f"{repo}: max created_at {before or '(none)'} -> {after or '(none)'}")

    return problems


def compare_progress_csv(baseline_ref: str, repo_relative_path: str, timestamp_field: str) -> list[str]:
    problems: list[str] = []
    current_path = Path(repo_relative_path)
    if not current_path.exists():
        return [f"Current {repo_relative_path} is missing"]

    with open_csv_from_worktree(current_path) as current_file:
        current_total, current_max = summarize_progress(csv.DictReader(current_file), timestamp_field)

    baseline_file = open_csv_from_git(baseline_ref, repo_relative_path)
    if baseline_file is None:
        print(f"No baseline {repo_relative_path} found; skipping progress comparison.")
        return problems
    with baseline_file:
        baseline_total, baseline_max = summarize_progress(csv.DictReader(baseline_file), timestamp_field)

    print(f"{repo_relative_path}: rows {baseline_total:,} -> {current_total:,}, max {timestamp_field} {baseline_max or '(none)'} -> {current_max or '(none)'}")
    if current_total < baseline_total:
        problems.append(f"{repo_relative_path} row count regressed: {baseline_total} -> {current_total}")
    baseline_dt = parse_iso_timestamp(baseline_max)
    current_dt = parse_iso_timestamp(current_max)
    if baseline_dt and current_dt and current_dt < baseline_dt:
        problems.append(f"{repo_relative_path} max {timestamp_field} regressed: {baseline_max} -> {current_max}")
    elif baseline_dt and not current_dt:
        problems.append(
            f"{repo_relative_path} lost all parseable {timestamp_field} values (baseline max {baseline_max})"
        )

    return problems


def compare_charts(baseline_ref: str) -> list[str]:
    problems: list[str] = []
    tracked: set[str] = set(list_tracked_files(baseline_ref, "charts"))

    # Also include chart files generated in this run that aren't tracked yet,
    # so a freshly-added empty chart still trips the sanity check.
    current: set[str] = set()
    charts_dir = Path("charts")
    if charts_dir.is_dir():
        for path in charts_dir.rglob("*"):
            if path.is_file():
                current.add(path.as_posix())

    files_to_check = tracked | current
    if not files_to_check:
        print("No chart files at baseline or in worktree; skipping chart-file comparison.")
        return problems

    missing = []
    empty = []
    for repo_relative_path in sorted(files_to_check):
        current_path = Path(repo_relative_path)
        if not current_path.exists():
            missing.append(repo_relative_path)
            continue
        size = current_path.stat().st_size
        if size <= 0:
            empty.append(repo_relative_path)

    print(f"charts tracked at baseline: {len(tracked)}, in worktree: {len(current)}")
    if missing:
        problems.append(f"Missing chart files: {', '.join(missing[:10])}")
    if empty:
        problems.append(f"Empty chart files: {', '.join(empty[:10])}")
    return problems


def compare_rowcount(baseline_ref: str, repo_relative_path: str) -> list[str]:
    """Flag if an append-only data CSV lost rows versus the committed baseline.

    These tables only ever grow, so a smaller row count signals a truncated or
    corrupted export. Counting with csv.reader (not raw lines) is robust to
    embedded newlines in quoted fields; the header is counted in both sides, so
    the comparison stays consistent.
    """
    problems: list[str] = []
    baseline_file = open_csv_from_git(baseline_ref, repo_relative_path)
    if baseline_file is None:
        print(f"No baseline {repo_relative_path} found; skipping row-count check.")
        return problems
    with baseline_file:
        baseline_total = sum(1 for _ in csv.reader(baseline_file))

    current_path = Path(repo_relative_path)
    if not current_path.exists():
        problems.append(f"Current {repo_relative_path} is missing (baseline had {baseline_total} lines)")
        return problems
    with open_csv_from_worktree(current_path) as current_file:
        current_total = sum(1 for _ in csv.reader(current_file))

    print(f"{repo_relative_path}: lines {baseline_total:,} -> {current_total:,}")
    if current_total < baseline_total:
        problems.append(f"{repo_relative_path} row count regressed: {baseline_total} -> {current_total}")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description="Sanity-check refreshed outputs against the committed baseline.")
    parser.add_argument("--baseline-ref", default="HEAD", help="Git ref to compare against (default: HEAD)")
    parser.add_argument("--repos", nargs="+", required=True, help="Repos whose max created_at coverage should not move backwards")
    args = parser.parse_args()

    os.chdir(Path(__file__).resolve().parent)

    problems: list[str] = []
    problems.extend(compare_items(args.baseline_ref, args.repos))
    problems.extend(compare_progress_csv(args.baseline_ref, "data/fetch_progress.csv", "sync_started_at"))
    problems.extend(compare_progress_csv(args.baseline_ref, "data/pr_push_progress.csv", "last_fetched_at"))
    problems.extend(compare_progress_csv(args.baseline_ref, "data/review_fetch_progress.csv", "fetched_at"))
    # Append-only data tables: a shrinking row count means a truncated/corrupt
    # export, which must not be merged unattended.
    for data_csv in (
        "data/pr_first_comment.csv",
        "data/pr_push_events.csv.gz",
        "data/pr_reviews.csv.gz",
        "data/pr_review_comments.csv.gz",
        "data/pr_commit_stats.csv.gz",
        "data/pr_copilot_issue_comments.csv",
    ):
        problems.extend(compare_rowcount(args.baseline_ref, data_csv))
    problems.extend(compare_charts(args.baseline_ref))

    if problems:
        print("\nSanity check failed:")
        for problem in problems:
            print(f"- {problem}")
        return 1

    print("\nSanity check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
