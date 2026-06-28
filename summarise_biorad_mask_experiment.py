#!/usr/bin/env python3
"""Summarise a BioDAR mask threshold experiment run.

The report is descriptive rather than a hard automatic selector. It records how
much each profile masked and how its VP output changed relative to the baseline
profile for the same input file path.
"""

from __future__ import annotations

import argparse
import csv
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Iterable


DEFAULT_ROOT = Path(
    "/gws/ssde/j25a/ncas_radar/vol2/avocet/experiments/"
    "biorad_mask_threshold_scan"
)


def read_tsv(path: Path) -> Iterable[dict[str, str]]:
    with path.open(newline="") as handle:
        yield from csv.DictReader(handle, delimiter="\t")


def write_tsv(path: Path, rows: list[dict[str, object]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def as_float(value: str | None) -> float:
    if value is None:
        return math.nan
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan


def safe_sum(values: list[float]) -> float:
    return sum(v for v in values if math.isfinite(v))


def median(values: list[float]) -> float:
    vals = [v for v in values if math.isfinite(v)]
    if not vals:
        return math.nan
    return statistics.median(vals)


def mean(values: list[float]) -> float:
    vals = [v for v in values if math.isfinite(v)]
    if not vals:
        return math.nan
    return statistics.fmean(vals)


def infer_radar_date(rel_csv: Path) -> tuple[str, str]:
    parts = rel_csv.parts
    # Expected: single-site/<radar>/<year>/<date>/vpts_csv/<file>
    if len(parts) >= 4 and parts[0] == "single-site":
        return parts[1], parts[3]
    for part in parts:
        if len(part) == 8 and part.isdigit():
            return "", part
    return "", ""


def csv_metrics(path: Path) -> dict[str, float | int]:
    rows = 0
    numeric_sums: defaultdict[str, float] = defaultdict(float)
    numeric_nonzero: defaultdict[str, int] = defaultdict(int)
    candidate_cols = {
        "eta",
        "dens",
        "density",
        "DBZH",
        "dbz",
        "ff",
        "dd",
        "u",
        "v",
        "w",
        "sd_vvp",
      }
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows += 1
            for col, value in row.items():
                if col not in candidate_cols:
                    continue
                val = as_float(value)
                if not math.isfinite(val):
                    continue
                numeric_sums[col] += val
                if val != 0:
                    numeric_nonzero[col] += 1
    metrics: dict[str, float | int] = {"rows": rows}
    for col in sorted(candidate_cols):
        metrics[f"{col}_sum"] = numeric_sums.get(col, 0.0)
        metrics[f"{col}_nonzero_rows"] = numeric_nonzero.get(col, 0)
    return metrics


def collect_vp_outputs(run_dir: Path) -> list[dict[str, object]]:
    vp_root = run_dir / "vp"
    rows: list[dict[str, object]] = []
    if not vp_root.exists():
        return rows
    for profile_dir in sorted(p for p in vp_root.iterdir() if p.is_dir()):
        profile = profile_dir.name
        for csv_path in sorted(profile_dir.rglob("*_vp.csv")):
            rel = csv_path.relative_to(profile_dir)
            radar, date = infer_radar_date(rel)
            metrics = csv_metrics(csv_path)
            rows.append(
                {
                    "profile_id": profile,
                    "radar": radar,
                    "date": date,
                    "relative_csv": str(rel),
                    "csv_path": str(csv_path),
                    **metrics,
                }
            )
    return rows


def collect_mask_diagnostics(run_dir: Path) -> list[dict[str, object]]:
    diag_root = run_dir / "diagnostics"
    out: list[dict[str, object]] = []
    if not diag_root.exists():
        return out
    for path in sorted(diag_root.rglob("*_mask_diagnostics.tsv")):
        for row in read_tsv(path):
            gates_total = as_float(row.get("gates_total"))
            combined_bad = as_float(row.get("combined_bad"))
            row_out: dict[str, object] = dict(row)
            row_out["diagnostics_file"] = str(path)
            row_out["combined_bad_fraction"] = (
                combined_bad / gates_total if gates_total and math.isfinite(gates_total) else math.nan
            )
            out.append(row_out)
    return out


def group_summary(rows: list[dict[str, object]], diag_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    by_profile: defaultdict[str, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        by_profile[str(row["profile_id"])].append(row)

    diag_by_profile: defaultdict[str, list[float]] = defaultdict(list)
    for row in diag_rows:
        diag_by_profile[str(row.get("profile_id", ""))].append(as_float(str(row.get("combined_bad_fraction", ""))))

    summary = []
    for profile, profile_rows in sorted(by_profile.items()):
        eta_sums = [as_float(str(r.get("eta_sum", ""))) for r in profile_rows]
        dens_sums = [as_float(str(r.get("dens_sum", ""))) for r in profile_rows]
        density_sums = [as_float(str(r.get("density_sum", ""))) for r in profile_rows]
        summary.append(
            {
                "profile_id": profile,
                "vp_file_count": len(profile_rows),
                "eta_sum_total": safe_sum(eta_sums),
                "eta_sum_median": median(eta_sums),
                "dens_sum_total": safe_sum(dens_sums),
                "density_sum_total": safe_sum(density_sums),
                "combined_bad_fraction_mean": mean(diag_by_profile.get(profile, [])),
                "combined_bad_fraction_median": median(diag_by_profile.get(profile, [])),
            }
        )
    return summary


def baseline_comparison(rows: list[dict[str, object]], baseline_profile: str) -> list[dict[str, object]]:
    keyed: dict[tuple[str, str], dict[str, object]] = {}
    for row in rows:
        keyed[(str(row["profile_id"]), str(row["relative_csv"]))] = row

    out = []
    for row in rows:
        profile = str(row["profile_id"])
        if profile == baseline_profile:
            continue
        rel = str(row["relative_csv"])
        base = keyed.get((baseline_profile, rel))
        if not base:
            continue
        for metric in ("eta_sum", "dens_sum", "density_sum", "rows"):
            value = as_float(str(row.get(metric, "")))
            base_value = as_float(str(base.get(metric, "")))
            ratio = value / base_value if base_value and math.isfinite(base_value) else math.nan
            out.append(
                {
                    "profile_id": profile,
                    "baseline_profile_id": baseline_profile,
                    "relative_csv": rel,
                    "radar": row.get("radar", ""),
                    "date": row.get("date", ""),
                    "metric": metric,
                    "value": value,
                    "baseline_value": base_value,
                    "ratio_to_baseline": ratio,
                }
            )
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment-root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--baseline-profile", default="baseline_sqi020_dbzh_only")
    args = parser.parse_args()

    run_dir = args.experiment_root / "runs" / args.run_id
    if not run_dir.exists():
        raise SystemExit(f"Run dir does not exist: {run_dir}")
    report_dir = run_dir / "reports"

    vp_rows = collect_vp_outputs(run_dir)
    diag_rows = collect_mask_diagnostics(run_dir)
    summary_rows = group_summary(vp_rows, diag_rows)
    comparison_rows = baseline_comparison(vp_rows, args.baseline_profile)

    write_tsv(
        report_dir / "vp_file_summary.tsv",
        vp_rows,
        [
            "profile_id",
            "radar",
            "date",
            "relative_csv",
            "csv_path",
            "rows",
            "eta_sum",
            "eta_nonzero_rows",
            "dens_sum",
            "dens_nonzero_rows",
            "density_sum",
            "density_nonzero_rows",
            "DBZH_sum",
            "dbz_sum",
            "sd_vvp_sum",
        ],
    )
    write_tsv(
        report_dir / "profile_summary.tsv",
        summary_rows,
        [
            "profile_id",
            "vp_file_count",
            "eta_sum_total",
            "eta_sum_median",
            "dens_sum_total",
            "density_sum_total",
            "combined_bad_fraction_mean",
            "combined_bad_fraction_median",
        ],
    )
    write_tsv(
        report_dir / "baseline_comparison.tsv",
        comparison_rows,
        [
            "profile_id",
            "baseline_profile_id",
            "relative_csv",
            "radar",
            "date",
            "metric",
            "value",
            "baseline_value",
            "ratio_to_baseline",
        ],
    )
    print(report_dir)
    print(f"VP files: {len(vp_rows)}")
    print(f"Diagnostic rows: {len(diag_rows)}")
    print(f"Profile summaries: {len(summary_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
