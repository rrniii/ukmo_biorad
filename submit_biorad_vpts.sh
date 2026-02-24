#!/usr/bin/env bash
# submit_biorad_vpts.sh
# ---------------------
# Scan biorad_vp day directories and submit one SLURM job per day to build
# daily VPTS time-series CSV outputs (separate lp/sp) under biorad_vpts,
# organized like raw_h5_data_final (single-site/<radar>/<year>/YYYYMMDD_*.csv).

set -uo pipefail

INPUT_ROOT="/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/biorad_vp"
OUTPUT_ROOT="/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/biorad_vpts"
LOG_ROOT="/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/biorad_vpts_logs"
PARTITION="standard"
QOS="standard"
TIME_LIMIT="01:00:00"
START_DATE="00000000"  # inclusive YYYYMMDD
END_DATE="99999999"    # inclusive YYYYMMDD
RADAR_FILTER=""        # if set, only process this radar name
FORCE=0                # 1 => re-run even if outputs exist
MODULE_NAME="jasr"
R_BIN="Rscript"
R_LIBS_USER_OVERRIDE=""
CHUNK_SIZE=100

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
    --in-root DIR   Override input root (default: ${INPUT_ROOT}).
    --out-root DIR  Override output root (default: ${OUTPUT_ROOT}).
    --log-root DIR  Override log root (default: ${LOG_ROOT}).
    --module NAME   Environment module to load for R (default: ${MODULE_NAME}).
    --r-libs-user   Override R_LIBS_USER (default: keep current).
    --rscript PATH  Rscript executable to use (default: ${R_BIN}).
    --chunk-size N  Number of CSV files to read per chunk in R (default: ${CHUNK_SIZE}).
    -h              Show this help.
EOF
}

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
        --in-root) INPUT_ROOT="$2"; shift 2 ;;
        --out-root) OUTPUT_ROOT="$2"; shift 2 ;;
        --log-root) LOG_ROOT="$2"; shift 2 ;;
        --module) MODULE_NAME="$2"; shift 2 ;;
        --r-libs-user) R_LIBS_USER_OVERRIDE="$2"; shift 2 ;;
        --rscript) R_BIN="$2"; shift 2 ;;
        --chunk-size) CHUNK_SIZE="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
done

INPUT_ROOT="${INPUT_ROOT%/}"
OUTPUT_ROOT="${OUTPUT_ROOT%/}"
LOG_ROOT="${LOG_ROOT%/}"

if [[ -z "$R_LIBS_USER_OVERRIDE" && -d "${HOME}/R/library/bioRad" ]]; then
    R_LIBS_USER_OVERRIDE="${HOME}/R/library"
fi

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
if ! [[ "$CHUNK_SIZE" =~ ^[0-9]+$ ]] || [[ "$CHUNK_SIZE" -lt 1 ]]; then
    echo "--chunk-size must be a positive integer; got '$CHUNK_SIZE'" >&2
    exit 1
fi

if [[ ! -d "$INPUT_ROOT" ]]; then
    echo "Input root does not exist: $INPUT_ROOT" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
R_SCRIPT="${SCRIPT_DIR}/run_biorad_vpts_for_date.R"
if [[ ! -f "$R_SCRIPT" ]]; then
    echo "Cannot find run_biorad_vpts_for_date.R at $R_SCRIPT" >&2
    exit 1
fi

mkdir -p "$LOG_ROOT"
RUN_DIR="${LOG_ROOT}/submit_biorad_vpts_submission_${RUN_TS}"
SLURM_ROOT="${RUN_DIR}/slurm_logs"
mkdir -p "$SLURM_ROOT"
RUN_LOG="${RUN_DIR}/submit_biorad_vpts_submission_${RUN_TS}.log"
{
    echo "Started: $(date +"%Y-%m-%d %H:%M:%S %Z")"
    echo "Command: ${ORIG_CMD}"
} >> "$RUN_LOG"

SCAN_BASE="$INPUT_ROOT"
if [[ -d "${INPUT_ROOT}/single-site" ]]; then
    SCAN_BASE="${INPUT_ROOT}/single-site"
fi

mapfile -t day_dirs < <(find "$SCAN_BASE" -type d -name "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]" | sort)
if [[ ${#day_dirs[@]} -eq 0 ]]; then
    echo "No date directories found under ${SCAN_BASE}" | tee -a "$RUN_LOG"
    exit 0
fi

total_submitted=0
total_skipped=0

for day_dir in "${day_dirs[@]}"; do
    day=$(basename "$day_dir")

    if [[ "$day" < "$START_DATE" || "$day" > "$END_DATE" ]]; then
        continue
    fi
    if [[ -n "$RADAR_FILTER" && "$day_dir" != *"/${RADAR_FILTER}/"* ]]; then
        continue
    fi

    csv_dir="${day_dir}/vpts_csv"
    [[ -d "$csv_dir" ]] || csv_dir="$day_dir"
    lp_count=$(find "$csv_dir" -type f -name "*_lp_*_vp.csv" | wc -l | tr -d ' ')
    sp_count=$(find "$csv_dir" -type f -name "*_sp_*_vp.csv" | wc -l | tr -d ' ')
    if [[ "$lp_count" -eq 0 && "$sp_count" -eq 0 ]]; then
        continue
    fi

    rel_day_dir="${day_dir#${INPUT_ROOT}/}"
    rel_parent="${rel_day_dir%/*}"
    out_parent="${OUTPUT_ROOT}/${rel_parent}"
    expected=()
    if [[ "$lp_count" -gt 0 ]]; then
        expected+=("${out_parent}/${day}_lp_vpts.csv")
    fi
    if [[ "$sp_count" -gt 0 ]]; then
        expected+=("${out_parent}/${day}_sp_vpts.csv")
    fi

    if [[ $FORCE -eq 0 ]]; then
        all_present=1
        for f in "${expected[@]}"; do
            if [[ ! -f "$f" ]]; then
                all_present=0
                break
            fi
        done
        if [[ "$all_present" -eq 1 ]]; then
            echo "Skip (outputs present): $day_dir"
            ((total_skipped++))
            continue
        fi
    fi

    radar="all"
    if [[ "$day_dir" == *"/single-site/"* ]]; then
        radar="${day_dir#*/single-site/}"
        radar="${radar%%/*}"
    fi

    log_dir="${SLURM_ROOT}/${radar}/${day}"
    mkdir -p "$log_dir"
    job_name="biorad_vpts_${radar}_${day}"

    export_env="ALL,RADAR_IN=${INPUT_ROOT},RADAR_OUT=${OUTPUT_ROOT},FORCE=${FORCE},CHUNK_SIZE=${CHUNK_SIZE}"
    if [[ -n "$R_LIBS_USER_OVERRIDE" ]]; then
        export_env="${export_env},R_LIBS_USER=${R_LIBS_USER_OVERRIDE}"
    fi

    extra_args=""
    if [[ $FORCE -eq 1 ]]; then
        extra_args=" --force"
    fi

    wrap_cmd="bash -lc 'module load ${MODULE_NAME}; ${R_BIN} \"${R_SCRIPT}\" \"${day}\" --input-dir \"${day_dir}\" --input-root \"${INPUT_ROOT}\" --output-root \"${OUTPUT_ROOT}\" --chunk-size \"${CHUNK_SIZE}\"${extra_args}'"

    if JOB_STR=$(sbatch \
        --account=ncas_radar \
        --partition="${PARTITION}" \
        --qos="${QOS}" \
        --time="${TIME_LIMIT}" \
        --export="${export_env}" \
        -o "${log_dir}/${job_name}-%j.out" \
        -e "${log_dir}/${job_name}-%j.err" \
        --job-name="${job_name}" \
        --wrap="${wrap_cmd}"); then
        JOB_ID=$(echo "$JOB_STR" | grep -o '[0-9][0-9]*' | tail -n1)
        echo "Submitted: ${day_dir} -> job ${JOB_ID}" | tee -a "$RUN_LOG"
        ((total_submitted++))
    else
        echo "Failed to submit job for ${day_dir}" | tee -a "$RUN_LOG" >&2
        ((total_skipped++))
    fi
done

echo "Submitted: ${total_submitted}, Skipped: ${total_skipped}" | tee -a "$RUN_LOG"
