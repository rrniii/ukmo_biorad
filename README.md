# ukmo_biorad
Tools to convert UKMO aggregated HDF5 into bioRad/vol2bird inputs and compute
biological vertical profiles (VPs) per radar and day.

This repo provides a two-stage pipeline:
1) Split aggregated UKMO HDF5 into smaller ODIM H5 files (one file per
   pulse type and time group).
2) Run bioRad/vol2bird on those ODIM H5 files to produce VP outputs (CSV + H5).

The scripts are designed for JASMIN, use SLURM, and default to a shared GWS
directory tree.

## High-level overview: what the pipeline does and why
UKMO aggregated HDF5 files bundle many scans and time groups into a single
file. vol2bird expects standard ODIM HDF5 inputs that correspond to individual
volumes. The pipeline:
- Splits aggregated HDF5 into ODIM H5 files per pulse type (lp/sp) and time
  group (e.g., 0340, 1700).
- Runs bioRad/vol2bird on each ODIM H5 to compute vertical profiles of
  biological scatterers.

This makes the output suitable for ecological/bird migration analysis and
aligns with bioRad expectations.

## Upstream inputs (CEDA + Nimrod conversion)
The raw inputs for this repo are aggregated ODIM HDF5 files created by the
Nimrod conversion workflow in a separate repo:
- Repo path: `/home/users/rrniii/bin/Nimrod_convert_and_aggregate`
- Key scripts: `convert_all_files.sh`, `convert_and_aggregate.sh`, `extract.sh`,
  and `convert_and_aggregate.py`.

That repo:
- Reads Met Office UKMO Nimrod single-site radar data from the CEDA archive.
  Dataset: https://catalogue.ceda.ac.uk/uuid/82adec1f896af6169112d09cc1174499/
- Uses the raw archive on JASMIN (e.g. `/badc/ukmo-nimrod/data/single-site/...`)
  to extract `.dat.gz.tar` files.
- Converts and aggregates those raw scans into daily ODIM HDF5 under
  `raw_h5_data_final/single-site/<radar>/<year>/`.

This repo assumes those aggregated files already exist and are the inputs for
`submit_biorad_vol2birdinput.sh`. Make sure the base path used by the Nimrod
conversion repo matches the base path configured here.

## Directory layout and naming conventions
Default base: `/work/scratch-pw4/rrniii`

Raw input:
- `raw_h5_data_final/single-site/<radar>/<year>/<YYYYMMDD>_..._aggregate.h5`

vol2bird inputs (split files):
- `vol2birdinput/single-site/<radar>/<year>/<YYYYMMDD>/lp/<base>_lp_<time>.h5`
- `vol2birdinput/single-site/<radar>/<year>/<YYYYMMDD>/sp/<base>_sp_<time>.h5`

VP outputs:
- CSV: `biorad_vp/single-site/<radar>/<year>/<YYYYMMDD>/vpts_csv/*_vp.csv`
- H5:  `biorad_vp/single-site/<radar>/<year>/<YYYYMMDD>/vp_h5/*_vp.h5`

Logs:
- `vol2birdinput_logs/submit_biorad_vol2birdinput_submission_<timestamp>/...`
- `biorad_vp_logs/submit_biorad_vp_submission_<timestamp>/...`

## Components and how they fit together
### Upstream: `Nimrod_convert_and_aggregate` (external)
- Produces the aggregated ODIM HDF5 files consumed by this repo.
- Run `convert_all_files.sh` there to generate:
  `raw_h5_data_final/single-site/<radar>/<year>/<YYYYMMDD>_..._aggregate.h5`.

### `submit_biorad_vol2birdinput.sh`
- Scans the raw tree for aggregated HDF5 files.
- For each day, submits a SLURM job that runs `ukmo2bioradinput.py`.
- Skips submission if expected outputs already exist unless `-f` is used.
- Key defaults (can be overridden with flags):
  - `RAW_ROOT`: raw aggregated inputs
  - `OUTPUT_ROOT`: vol2bird input outputs
  - `LOG_ROOT`: log output directory

### `ukmo2bioradinput.py`
- Reads a single aggregated HDF5 file.
- Splits `lp` and `sp` groups and child time groups into standalone ODIM H5
  files, preserving attributes and internal structure.
- Output naming: `<base>_<pulse>_<time>.h5`.

### `submit_biorad_vp.sh`
- Scans the `vol2birdinput` tree and submits one SLURM job per date directory.
- Passes `--input-dir` to ensure each job only processes its radar/day.
- Defaults to disabling HDF5 file locking for stability on GWS.
- Key defaults (can be overridden with flags):
  - `INPUT_ROOT`: vol2bird input root
  - `OUTPUT_ROOT`: VP output root
  - `LOG_ROOT`: VP log root

### `run_biorad_vp_for_date.R`
- Runs bioRad/vol2bird for all ODIM H5 files in a date directory.
- Writes both VPTS CSV and ODIM VP H5 (unless `--csv-only`).
- Supports debug modes:
  - `--input-dir /path/to/YYYYMMDD`
  - `--input-file /path/to/file.h5`
- Per-file tuning:
  - `nyquist_min = 1` for `lp` files.
  - `sp` files use bioRad defaults.

## HDF5 file locking (important)
On GWS, HDF5 file locking can hang and stall processes in uninterruptible I/O
sleep. For stability, file locking is disabled by default:
- Default behavior: `HDF5_USE_FILE_LOCKING=FALSE`
- Override with:
  - `--enable-hdf5-locking`
  - or `DISABLE_HDF5_LOCKING=0`

This is safe in this pipeline because each job reads a file and writes its own
distinct output files.

## R environment and bioRad installation
- The scripts use `module load jasr`.
- `bioRad` is expected in `~/R/library`.
- `run_biorad_vp_for_date.R` auto-adds `~/R/library` to `.libPaths()` if it
  contains `bioRad`.

## Typical usage
### Step 1: Split aggregated inputs
```
./submit_biorad_vol2birdinput.sh -s 20250101 -e 20250131
```

### Step 2: Run VP calculations
```
./submit_biorad_vp.sh -s 20250101 -e 20250131 --disable-hdf5-locking
```

### Example: single radar/day
```
./submit_biorad_vp.sh -r castor-bay -s 20250122 -e 20250122 --disable-hdf5-locking
```

### Debug a single file
```
module load jasr
Rscript run_biorad_vp_for_date.R 20250122 \
  --input-file /work/scratch-pw4/rrniii/vol2birdinput/single-site/castor-bay/2025/20250122/lp/20250122_polar_pl_radar07_aggregate_lp_0440.h5 \
  --disable-hdf5-locking
```

### CSV-only mode (skip VP H5)
```
./submit_biorad_vp.sh -s 20250122 -e 20250122 --csv-only
```

## Outputs and file contents
- `*_vp.csv`: VPTS CSV output (vertical profile time series).
- `*_vp.h5`: ODIM HDF5 VP output for downstream tools.
- Output filenames mirror input basenames and add `_vp`.

## Known issues and troubleshooting
- Errors like `no valid scans found in polar volume` indicate a file that
  cannot be processed (e.g., missing/invalid scans). The pipeline continues.
- If jobs appear "stuck":
  - Confirm HDF5 locking is disabled.
  - Check log timestamps for progress.
  - Use `--input-file` to isolate a problematic file.

## Related files
- `submit_biorad_vol2birdinput.sh`: stage 1 submission script.
- `ukmo2bioradinput.py`: stage 1 converter.
- `submit_biorad_vp.sh`: stage 2 submission script.
- `run_biorad_vp_for_date.R`: stage 2 runner.
