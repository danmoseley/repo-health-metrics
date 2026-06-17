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
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple


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
    return io.StringIO(raw.decode("utf-8"))


def open_csv_from_worktree(path: Path) -> io.TextIOBase:
    if str(path).endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", newline="")
    return path.open("r", encoding="utf-8", newline="")


def summarize_items(reader: csv.DictReader[str]) -> Tuple[int, Dict[str, str]]:
    total = 0
    max_created_at_by_repo: Dict[str, str] = {}
    for row in reader:
        total += 1
        repo = row.get("repo", "")
        created_at = row.get("created_at", "")
        if repo and created_at and created_at > max_created_at_by_repo.get(repo, ""):
            max_created_at_by_repo[repo] = created_at
    return total, max_created_at_by_repo


def summarize_progress(reader: csv.DictReader[str], timestamp_field: str) -> Tuple[int, str]:
    total = 0
    max_timestamp = ""
    for row in reader:
        total += 1
        timestamp = row.get(timestamp_field, "")
        if timestamp and timestamp > max_timestamp:
            max_timestamp = timestamp
    return total, max_timestamp


def list_tracked_files(ref: str, prefix: str) -> list[str]:
    result = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", ref, prefix],
        capture_output=True,
        check=True,
        text=True,
    )
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
        if before and after and after < before:
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
    if baseline_max and current_max and current_max < baseline_max:
        problems.append(f"{repo_relative_path} max {timestamp_field} regressed: {baseline_max} -> {current_max}")

    return problems


def compare_charts(baseline_ref: str) -> list[str]:
    problems: list[str] = []
    tracked = list_tracked_files(baseline_ref, "charts")
    if not tracked:
        print("No baseline charts tracked; skipping chart-file comparison.")
        return problems

    missing = []
    empty = []
    for repo_relative_path in tracked:
        current_path = Path(repo_relative_path)
        if not current_path.exists():
            missing.append(repo_relative_path)
            continue
        size = current_path.stat().st_size
        if size <= 0:
            empty.append(repo_relative_path)

    print(f"charts tracked at baseline: {len(tracked)}")
    if missing:
        problems.append(f"Missing chart files: {', '.join(missing[:10])}")
    if empty:
        problems.append(f"Empty chart files: {', '.join(empty[:10])}")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description="Sanity-check refreshed outputs against the committed baseline.")
    parser.add_argument("--baseline-ref", default="HEAD", help="Git ref to compare against (default: HEAD)")
    parser.add_argument("--repos", nargs="+", required=True, help="Repos whose max created_at coverage should not move backwards")
    args = parser.parse_args()

    os.chdir(Path(__file__).resolve().parent)

    problems: list[str] = []
    problems.extend(compare_items(args.baseline_ref, args.repos))
    problems.extend(compare_progress_csv(args.baseline_ref, "data/pr_push_progress.csv", "last_fetched_at"))
    problems.extend(compare_progress_csv(args.baseline_ref, "data/review_fetch_progress.csv", "fetched_at"))
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
