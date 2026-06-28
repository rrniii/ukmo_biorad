#!/usr/bin/env python3
"""Create the default threshold grid for BioDAR mask experiments."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


DEFAULT_ROWS = [
    {
        "profile_id": "baseline_sqi020_dbzh_only",
        "mask_mode": "baseline",
        "sqi_thr": "0.20",
        "ncp_thr": "",
        "floor_quantile": "",
        "floor_margin_db": "",
        "clutter_dbz_min": "",
        "clutter_vrad_abs_max": "",
        "clutter_persistence_min": "",
        "clutter_min_gates": "",
        "notes": "Current production-style baseline: SQI/SQIH masks DBZH only.",
    },
    {
        "profile_id": "noise_sqi020_ncp020_floor005_3db",
        "mask_mode": "noise_only",
        "sqi_thr": "0.20",
        "ncp_thr": "0.20",
        "floor_quantile": "0.05",
        "floor_margin_db": "3",
        "clutter_dbz_min": "",
        "clutter_vrad_abs_max": "",
        "clutter_persistence_min": "",
        "clutter_min_gates": "",
        "notes": "Moderate noise mask applied to all loaded fields.",
    },
    {
        "profile_id": "noise_sqi030_ncp030_floor010_3db",
        "mask_mode": "noise_only",
        "sqi_thr": "0.30",
        "ncp_thr": "0.30",
        "floor_quantile": "0.10",
        "floor_margin_db": "3",
        "clutter_dbz_min": "",
        "clutter_vrad_abs_max": "",
        "clutter_persistence_min": "",
        "clutter_min_gates": "",
        "notes": "Stricter noise mask; expected to under-count rather than keep marginal gates.",
    },
    {
        "profile_id": "clutter_dbz5_vrad1_persist035",
        "mask_mode": "clutter_only",
        "sqi_thr": "0.20",
        "ncp_thr": "",
        "floor_quantile": "",
        "floor_margin_db": "",
        "clutter_dbz_min": "5",
        "clutter_vrad_abs_max": "1.0",
        "clutter_persistence_min": "0.35",
        "clutter_min_gates": "20",
        "notes": "Static clutter candidate: persistent high-reflectivity low-velocity range bins.",
    },
    {
        "profile_id": "combined_sqi025_ncp025_floor005_3db_clutter5_1_035",
        "mask_mode": "combined",
        "sqi_thr": "0.25",
        "ncp_thr": "0.25",
        "floor_quantile": "0.05",
        "floor_margin_db": "3",
        "clutter_dbz_min": "5",
        "clutter_vrad_abs_max": "1.0",
        "clutter_persistence_min": "0.35",
        "clutter_min_gates": "20",
        "notes": "Recommended first combined candidate.",
    },
    {
        "profile_id": "combined_sqi030_ncp030_floor010_4db_clutter5_1_030",
        "mask_mode": "combined",
        "sqi_thr": "0.30",
        "ncp_thr": "0.30",
        "floor_quantile": "0.10",
        "floor_margin_db": "4",
        "clutter_dbz_min": "5",
        "clutter_vrad_abs_max": "1.0",
        "clutter_persistence_min": "0.30",
        "clutter_min_gates": "20",
        "notes": "Aggressive candidate for the user preference to under-count rather than retain noise/clutter.",
    },
    {
        "profile_id": "combined_sqi035_ncp035_floor010_5db_clutter3_1_025",
        "mask_mode": "combined",
        "sqi_thr": "0.35",
        "ncp_thr": "0.35",
        "floor_quantile": "0.10",
        "floor_margin_db": "5",
        "clutter_dbz_min": "3",
        "clutter_vrad_abs_max": "1.0",
        "clutter_persistence_min": "0.25",
        "clutter_min_gates": "20",
        "notes": "Very aggressive exploratory candidate.",
    },
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        type=Path,
        default=Path(
            "/gws/ssde/j25a/ncas_radar/vol2/avocet/experiments/"
            "biorad_mask_threshold_scan/configs/default_threshold_grid.tsv"
        ),
        help="Output TSV path.",
    )
    args = parser.parse_args()
    args.out.parent.mkdir(parents=True, exist_ok=True)

    fields = list(DEFAULT_ROWS[0].keys())
    with args.out.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(DEFAULT_ROWS)
    print(args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
