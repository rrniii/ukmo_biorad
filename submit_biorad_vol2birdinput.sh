#!/usr/bin/env bash
# submit_biorad_vol2birdinput.sh
# --------------------------------
# Scan aggregated UKMO HDF5 files under raw_h5_data_final and submit one SLURM
# job per file/day to run ukmo2bioradinput.py. Before submitting, the script
# checks whether all expected outputs already exist (lp/sp pulse types, all
# time child groups). If everything is present and --force is not set, the day
# is skipped. Jobs use the standard partition, short QoS, and a 30-minute
# time limit by default. Scan failures skip submission and are logged.

set -uo pipefail

RAW_ROOT="/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/raw_h5_data_final"
OUTPUT_ROOT="/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/vol2birdinput"
LOG_ROOT="/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/vol2birdinput_logs"
# Default to the nimrod env that has h5py; can override with --python
PYTHON_BIN="/gws/smf/j04/ncas_radar/software/miniconda3_radar_group_20200519/envs/nimrod/bin/python"
PARTITION="standard"
QOS="short"
TIME_LIMIT="00:30:00"
START_DATE="00000000"  # inclusive YYYYMMDD
END_DATE="99999999"    # inclusive YYYYMMDD
RADAR_FILTER=""        # if set, only process this radar name (matches directory)
FORCE=0                # 1 => overwrite/submit even if outputs exist

usage() {
    cat <<EOF
Usage: $(basename "$0") [options]
  Options:
    -r RADAR        Only process this radar (matches folder under single-site).
    -s START_DATE   Inclusive start date YYYYMMDD (default: all).
    -e END_DATE     Inclusive end date YYYYMMDD (default: all).
    -f              Force submit even if outputs exist.
    -p PARTITION    SLURM partition (default: ${PARTITION}).
    -q QOS          SLURM QoS (default: ${QOS}).
    -t TIME         SLURM time limit (default: ${TIME_LIMIT}).
    --python PATH   Python executable to run ukmo2bioradinput.py (default: ${PYTHON_BIN}).
    --raw-root DIR  Override raw root (default: ${RAW_ROOT}).
    --out-root DIR  Override output root (default: ${OUTPUT_ROOT}).
    --log-root DIR  Override log root (default: ${LOG_ROOT}).
    -h              Show this help.
EOF
}

# Parse args
RUN_TS=$(date +"%Y%m%dT%H%M%S")
ORIG_CMD="$0 $*"
while [[ $# -gt 0 ]]; do
    case "$1" in
        -r) RADAR_FILTER="$2"; shift 2 ;;
        -s) START_DATE="$2"; shift 2 ;;
        -e) END_DATE="$2"; shift 2 ;;
        -f) FORCE=1; shift ;;
        -p) PARTITION="$2"; shift 2 ;;
        -q) QOS="$2"; shift 2 ;;
        -t) TIME_LIMIT="$2"; shift 2 ;;
        --python) PYTHON_BIN="$2"; shift 2 ;;
        --raw-root) RAW_ROOT="$2"; shift 2 ;;
        --out-root) OUTPUT_ROOT="$2"; shift 2 ;;
        --log-root) LOG_ROOT="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
done

# Validate dates
for d in "$START_DATE" "$END_DATE"; do
    if ! [[ "$d" =~ ^[0-9]{8}$ ]]; then
        echo "Dates must be YYYYMMDD; got '$d'" >&2
        exit 1
    fi
done
if [[ "$END_DATE" < "$START_DATE" ]]; then
    echo "End date $END_DATE is before start date $START_DATE" >&2
    exit 1
fi

# Ensure roots exist
mkdir -p "$LOG_ROOT"

RUN_DIR="${LOG_ROOT}/submit_biorad_vol2birdinput_submission_${RUN_TS}"
SLURM_ROOT="${RUN_DIR}/slurm_logs"
mkdir -p "$RUN_DIR"
RUN_LOG="${RUN_DIR}/submit_biorad_vol2birdinput_submission_${RUN_TS}.log"
{
    echo "Started: $(date +"%Y-%m-%d %H:%M:%S %Z")"
    echo "Command: ${ORIG_CMD}"
} >> "$RUN_LOG"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONVERTER="${SCRIPT_DIR}/ukmo2bioradinput.py"
if [[ ! -f "$CONVERTER" ]]; then
    echo "Cannot find ukmo2bioradinput.py at $CONVERTER" >&2
    exit 1
fi
# Ensure Python can import h5py before proceeding
if ! "$PYTHON_BIN" - <<'PY' >/dev/null 2>&1
import h5py
PY
then
    echo "Python at $PYTHON_BIN cannot import h5py. Override with --python or install h5py." >&2
    exit 1
fi

# Enumerate candidate input files: raw_h5_data_final/single-site/<radar>/<year>/<day>_polar_pl_..._aggregate.h5
INPUT_BASE="${RAW_ROOT}/single-site"
if [[ -n "$RADAR_FILTER" ]]; then
    RADARS=("$RADAR_FILTER")
else
    mapfile -t RADARS < <(find "$INPUT_BASE" -mindepth 1 -maxdepth 1 -type d -printf "%f\n" | sort)
fi

total_submitted=0
total_skipped=0

for radar in "${RADARS[@]}"; do
    radar_dir="${INPUT_BASE}/${radar}"
    [[ -d "$radar_dir" ]] || continue
    mapfile -t files < <(find "$radar_dir" -mindepth 2 -maxdepth 2 -type f -name "*_aggregate.h5" | sort)
    for infile in "${files[@]}"; do
        day=$(basename "$infile" | cut -d'_' -f1)
        if [[ "$day" < "$START_DATE" || "$day" > "$END_DATE" ]]; then
            continue
        fi

        # Compute expected outputs by inspecting the pulse/time groups in the input file
        scan_out=""
        scan_out=$("$PYTHON_BIN" - "$infile" "$OUTPUT_ROOT" "$RAW_ROOT" 2>>"$RUN_LOG" <<'PY'
import h5py, os, sys, traceback
in_path, out_root, raw_root = sys.argv[1:4]
abs_in = os.path.abspath(in_path)
abs_raw = os.path.abspath(raw_root)
base = os.path.splitext(os.path.basename(abs_in))[0]
day = base.split("_")[0]
try:
    rel_parent = os.path.relpath(os.path.dirname(abs_in), abs_raw)
    if rel_parent.startswith(".."):
        rel_parent = ""
except Exception:
    rel_parent = ""
out_base = os.path.join(os.path.abspath(out_root), rel_parent, day)
outs = []
try:
    with h5py.File(abs_in, "r") as f:
        for pulse in ("lp", "sp"):
            if pulse not in f:
                continue
            for key in f[pulse].keys():
                outs.append(os.path.join(out_base, pulse, f"{base}_{pulse}_{key}.h5"))
    print("\n".join(outs))
except Exception as e:
    sys.stderr.write(f"SCAN_FAILED {abs_in}: {e}\n")
    sys.exit(1)
PY
        )
        scan_status=$?
        if [[ $scan_status -ne 0 ]]; then
            echo "Failed to scan input (skipping submission): ${infile}" | tee -a "$RUN_LOG" >&2
            ((total_skipped++))
            continue
        fi
        mapfile -t expected_outs <<< "$scan_out"

        missing=()
        for o in "${expected_outs[@]}"; do
            [[ -f "$o" ]] || missing+=("$o")
        done

        if [[ ${#expected_outs[@]} -gt 0 && ${#missing[@]} -eq 0 && $FORCE -eq 0 ]]; then
            echo "Skip (all outputs present): $infile"
            ((total_skipped++))
            continue
        fi

        # Prepare log paths
        log_dir="${SLURM_ROOT}/${radar}/${day}"
        mkdir -p "$log_dir"
        job_name="biorad_${radar}_${day}"

        # Submit SLURM job to run the converter
        if JOB_STR=$(sbatch \
            --account=ncas_radar \
            --partition="${PARTITION}" \
            --qos="${QOS}" \
            --time="${TIME_LIMIT}" \
            -o "${log_dir}/${job_name}-%j.out" \
            -e "${log_dir}/${job_name}-%j.err" \
            --job-name="${job_name}" \
            --wrap="\"${PYTHON_BIN}\" \"${CONVERTER}\" -i \"${infile}\" --raw-root \"${RAW_ROOT}\" --output-root \"${OUTPUT_ROOT}\""); then
            JOB_ID=$(echo "$JOB_STR" | grep -o '[0-9][0-9]*' | tail -n1)
            echo "Submitted: ${infile} -> job ${JOB_ID}" | tee -a "$RUN_LOG"
            ((total_submitted++))
        else
            echo "Failed to submit job for ${infile}" | tee -a "$RUN_LOG" >&2
            ((total_skipped++))
        fi
    done
done

echo "Submitted: ${total_submitted}, Skipped: ${total_skipped}" | tee -a "$RUN_LOG"
