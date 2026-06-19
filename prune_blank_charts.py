#!/usr/bin/env python3
"""Drop blank charts from the working tree before they are committed.

A chart can render effectively blank when its underlying data window comes back
empty (e.g. a metric with no activity in the lookback period). A single bad
chart should never block the entire weekly refresh, so rather than failing we:

  * restore the previously committed version of the chart, so it simply keeps
    its old data, or
  * omit the chart entirely if it is brand new and has no committed version.

Every other chart and all data updates still proceed and merge normally. The
script always exits 0; blank charts produce a warning, not a failure.

Detection finds the dominant (background) color and treats anything
sufficiently different from it as "ink". An image with almost no ink is blank.
The threshold is deliberately tiny so even a sparse chart (just axes and
labels) clears it, keeping false positives near zero.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple


def find_blank_pngs(png_paths: List[Path], min_ink_fraction: float = 0.001) -> List[Tuple[Path, float]]:
    """Return (path, ink_fraction) for each PNG that is effectively blank.

    Imaging libraries are imported lazily; if they're unavailable we skip the
    check rather than disturb a refresh.
    """
    try:
        import numpy as np
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.image as mpimg
    except Exception as exc:  # pragma: no cover - environment-dependent
        print(f"Warning: cannot import imaging libraries for blank-PNG check ({exc}); skipping.")
        return []

    blank: List[Tuple[Path, float]] = []
    for path in png_paths:
        # Guard the whole per-image analysis: a single unreadable or oddly-shaped
        # chart must never raise out of here, or it would defeat the purpose of
        # this script (one bad chart should not block the weekly refresh).
        try:
            img = mpimg.imread(path)
            arr = np.asarray(img)
            if arr.ndim == 2:
                arr = arr[:, :, None]
            if arr.shape[2] >= 3:
                arr = arr[:, :, :3]  # drop alpha
            else:
                arr = np.repeat(arr[:, :, :1], 3, axis=2)  # grayscale -> RGB
            if arr.dtype != np.uint8:
                arr = np.clip(arr.astype(np.float64) * 255.0, 0, 255)
            # Quantize each channel into 8 coarse levels so anti-aliasing noise
            # does not register as ink, fold the channels into one bucket id, and
            # find the most common (background) bucket with an O(n) histogram.
            # "Ink" is any pixel outside that dominant bucket; a near-uniform
            # image has almost none.
            levels = np.clip((arr.astype(np.float64) / 255.0 * 7.0).round(), 0, 7).astype(np.int64)
            codes = (levels[:, :, 0] * 64 + levels[:, :, 1] * 8 + levels[:, :, 2]).reshape(-1)
            counts = np.bincount(codes, minlength=512)
            ink_fraction = 1.0 - float(counts.max()) / float(codes.size)
        except Exception as exc:
            print(f"Warning: could not analyze {path} for blank check ({exc}); skipping.")
            continue
        if ink_fraction < min_ink_fraction:
            blank.append((path, ink_fraction))
    return blank


def tracked_at(ref: str, prefix: str) -> set[str]:
    result = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", ref, prefix],
        capture_output=True,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        return set()
    return {line.strip() for line in result.stdout.splitlines() if line.strip()}


def main() -> int:
    parser = argparse.ArgumentParser(description="Drop blank charts before commit.")
    parser.add_argument("--baseline-ref", default="HEAD", help="Git ref holding the previously committed charts (default: HEAD)")
    parser.add_argument("--charts-dir", default="charts", help="Directory containing chart PNGs (default: charts)")
    parser.add_argument("--report-file", default=None, help="Append a Markdown bullet list of pruned charts to this file (for the PR body)")
    args = parser.parse_args()

    os.chdir(Path(__file__).resolve().parent)

    charts_dir = Path(args.charts_dir)
    if not charts_dir.is_dir():
        print(f"No charts directory at {charts_dir}; nothing to prune.")
        return 0

    png_paths = sorted(p for p in charts_dir.rglob("*.png") if p.is_file())
    blanks = find_blank_pngs(png_paths)
    if not blanks:
        print(f"Checked {len(png_paths)} chart(s); none are blank.")
        return 0

    committed = tracked_at(args.baseline_ref, args.charts_dir)
    report_lines: List[str] = []
    for path, ink_fraction in blanks:
        posix = path.as_posix()
        if posix in committed:
            restored = subprocess.run(
                ["git", "checkout", args.baseline_ref, "--", posix],
                capture_output=True,
                check=False,
                text=True,
            )
            if restored.returncode == 0:
                print(
                    f"::warning::Chart {posix} rendered blank (ink={ink_fraction:.4%}); "
                    "kept the previously committed version."
                )
                report_lines.append(
                    f"- `{posix}` rendered blank (ink {ink_fraction:.4%}); kept the previously committed version."
                )
            else:
                print(
                    f"::warning::Chart {posix} rendered blank (ink={ink_fraction:.4%}) and "
                    f"could not be restored from {args.baseline_ref}: {restored.stderr.strip()}"
                )
                report_lines.append(
                    f"- `{posix}` rendered blank (ink {ink_fraction:.4%}); could NOT be restored "
                    f"from baseline — please check this chart."
                )
        else:
            try:
                path.unlink()
                print(
                    f"::warning::New chart {posix} rendered blank (ink={ink_fraction:.4%}); "
                    "omitted from this refresh."
                )
                report_lines.append(
                    f"- `{posix}` is a new chart that rendered blank (ink {ink_fraction:.4%}); "
                    "omitted from this refresh."
                )
            except OSError as exc:
                print(f"::warning::Could not remove blank chart {posix}: {exc}")
                report_lines.append(
                    f"- `{posix}` rendered blank (ink {ink_fraction:.4%}) but could not be removed: {exc}"
                )

    if args.report_file and report_lines:
        try:
            with open(args.report_file, "a", encoding="utf-8") as handle:
                handle.write("\n".join(report_lines) + "\n")
        except OSError as exc:
            print(f"Warning: could not write report file {args.report_file} ({exc}); continuing.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
