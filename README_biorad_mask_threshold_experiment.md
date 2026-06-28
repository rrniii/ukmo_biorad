# BioDAR Mask Threshold Experiment

This workflow tests noise and clutter masking before `calculate_vp()` without
changing pvol, aggregate, VP, or VPTS production files. All outputs live outside
the catalog under:

`/gws/ssde/j25a/ncas_radar/vol2/avocet/experiments/biorad_mask_threshold_scan`

## Package Baseline

The JASMIN user R library used by these jobs is:

`/home/users/rrniii/R/library`

`bioRad` has been updated there from `0.11.0` to the current CRAN release
`0.12.0`. `bioRad 0.12.0` requires `vol2birdR >= 1.3.0`, so `vol2birdR`
has also been updated from `1.2.1` to `1.3.0`.

The old package directories were backed up as:

`/home/users/rrniii/R/library/bioRad_0.11.0_backup_20260628`

`/home/users/rrniii/R/library/vol2birdR_1.2.1_backup_20260628`

`vol2birdR 1.3.0` was installed with explicit PROJ discovery because the
plain CRAN install detected the header but did not link `-lproj` on this
JASMIN module. The successful install used:

`configure.args=c(vol2birdR="--with-proj=/apps/jasmin/jaspy/miniforge_envs/jasr4.4/mf3-23.11.0-0/envs/jasr4.4-mf3-23.11.0-0-v20250902")`

## What Is Tested

The experiment has four mask modes:

- `baseline`: current production-style SQI/SQIH mask applied to `DBZH` only.
- `noise_only`: SQI/SQIH, normalised coherent power, and a per-range-bin low
  reflectivity floor mask applied to all loaded fields.
- `clutter_only`: persistent high-reflectivity, low-radial-velocity gates
  applied to all loaded fields.
- `combined`: noise and clutter masks combined, applied to all loaded fields.

The source pvol HDF5 files are only read. The mask is applied in memory before
calling `bioRad::calculate_vp()`.

## Files Added

- `make_biorad_mask_threshold_grid.py`: writes the default threshold grid.
- `run_biorad_vp_mask_experiment_for_date.R`: per-day masked VP runner.
- `submit_biorad_mask_experiment.sh`: Slurm launcher for VP threshold scans.
- `submit_biorad_mask_vpts.sh`: Slurm launcher for VPTS from experiment VP CSVs.
- `summarise_biorad_mask_experiment.py`: builds profile and baseline comparison
  reports.
- `external_bird_day_labels.template.tsv`: label-file schema for externally
  supported high-bird or control dates.

## Typical Run

Create the default grid:

```bash
cd /home/users/rrniii/ncas_radar_smf_rrniii/BioDAR/ukmo_biorad
python make_biorad_mask_threshold_grid.py
```

Create a label file by copying
`external_bird_day_labels.template.tsv` to the experiment `configs/` directory
and replacing the example row with source-backed radar/date cases.

Submit a canary:

```bash
./submit_biorad_mask_experiment.sh \
  --label-file /gws/ssde/j25a/ncas_radar/vol2/avocet/experiments/biorad_mask_threshold_scan/configs/external_bird_day_labels.tsv \
  --threshold-grid /gws/ssde/j25a/ncas_radar/vol2/avocet/experiments/biorad_mask_threshold_scan/configs/default_threshold_grid.tsv \
  --run-id canary_$(date -u +%Y%m%dT%H%M%SZ) \
  --profiles baseline_sqi020_dbzh_only,combined_sqi025_ncp025_floor005_3db_clutter5_1_035,combined_sqi030_ncp030_floor010_4db_clutter5_1_030 \
  --max-cases 3 \
  --max-active 20 \
  -f
```

After VP jobs finish, build VPTS for the same run:

```bash
./submit_biorad_mask_vpts.sh --run-id <run_id> --max-active 20 -f
```

Then summarise:

```bash
python summarise_biorad_mask_experiment.py --run-id <run_id>
```

Reports are written under `<run_dir>/reports/`.

## Decision Rule

Do not automatically choose a threshold from a single score. Inspect:

- the masked gate fraction by profile;
- VP/VPTS changes relative to baseline for the same radar/date/file;
- externally supported high-bird and low-bird/control dates;
- known weather/clutter periods;
- quick diagnostic plots from the generated VP/VPTS CSVs.

The preferred bias is conservative for biological signal: remove likely
background noise and clutter even if this under-counts some birds. Production
VP/VPTS generation should only be changed after the experiment identifies a
stable profile across several radars, years, pulse types, and migration regimes.
