#!/usr/bin/env bash
# submit_biorad_vits.sh
# ---------------------
# Scan biorad_vpts daily files and submit one SLURM job per radar/day to build
# daily VITS CSV outputs (separate lp/sp) under biorad_vits, organized like
# raw_h5_data_final (single-site/<radar>/<year>/YYYYMMDD_*.csv).

set -uo pipefail

INPUT_ROOT="/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/biorad_vpts"
OUTPUT_ROOT="/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/biorad_vits"
LOG_ROOT="/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/biorad_vits_logs"
PARTITION="standard"
QOS="standard"
TIME_LIMIT="00:30:00"
START_DATE="00000000"  # inclusive YYYYMMDD
END_DATE="99999999"    # inclusive YYYYMMDD
RADAR_FILTER=""        # if set, only process this radar name
FORCE=0                # 1 => re-run even if outputs exist
MODULE_NAME="jasr"
R_BIN="Rscript"
R_LIBS_USER_OVERRIDE=""
ALT_MIN="200"
ALT_MAX="4000"

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
    --alt-min M     VITS integration minimum altitude in m (default: ${ALT_MIN}).
    --alt-max M     VITS integration maximum altitude in m (default: ${ALT_MAX}).
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
        --alt-min) ALT_MIN="$2"; shift 2 ;;
        --alt-max) ALT_MAX="$2"; shift 2 ;;
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

# numeric check for alt bounds
if ! awk "BEGIN {exit !($ALT_MIN+0 < $ALT_MAX+0)}"; then
    echo "--alt-min must be less than --alt-max; got ${ALT_MIN} >= ${ALT_MAX}" >&2
    exit 1
fi

if [[ ! -d "$INPUT_ROOT" ]]; then
    echo "Input root does not exist: $INPUT_ROOT" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
R_SCRIPT="${SCRIPT_DIR}/run_biorad_vits_for_date.R"
if [[ ! -f "$R_SCRIPT" ]]; then
    echo "Cannot find run_biorad_vits_for_date.R at $R_SCRIPT" >&2
    exit 1
fi

mkdir -p "$LOG_ROOT"
RUN_DIR="${LOG_ROOT}/submit_biorad_vits_submission_${RUN_TS}"
SLURM_ROOT="${RUN_DIR}/slurm_logs"
mkdir -p "$SLURM_ROOT"
RUN_LOG="${RUN_DIR}/submit_biorad_vits_submission_${RUN_TS}.log"
{
    echo "Started: $(date +"%Y-%m-%d %H:%M:%S %Z")"
    echo "Command: ${ORIG_CMD}"
} >> "$RUN_LOG"

SCAN_BASE="$INPUT_ROOT"
if [[ -d "${INPUT_ROOT}/single-site" ]]; then
    SCAN_BASE="${INPUT_ROOT}/single-site"
fi

mapfile -t vpts_files < <(find "$SCAN_BASE" -type f -name "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_*_vpts.csv" | sort)
if [[ ${#vpts_files[@]} -eq 0 ]]; then
    echo "No daily VPTS files found under ${SCAN_BASE}" | tee -a "$RUN_LOG"
    exit 0
fi

declare -A keys=()
for f in "${vpts_files[@]}"; do
    base=$(basename "$f")
    day="${base%%_*}"
    [[ "$day" =~ ^[0-9]{8}$ ]] || continue
    parent=$(dirname "$f")
    key="${parent}__SEP__${day}"
    keys["$key"]=1
done

total_submitted=0
total_skipped=0

mapfile -t sorted_keys < <(printf '%s\n' "${!keys[@]}" | sort)
for key in "${sorted_keys[@]}"; do
    parent="${key%__SEP__*}"
    day="${key##*__SEP__}"

    if [[ "$day" < "$START_DATE" || "$day" > "$END_DATE" ]]; then
        continue
    fi
    if [[ -n "$RADAR_FILTER" && "$parent" != *"/${RADAR_FILTER}/"* ]]; then
        continue
    fi

    lp_in="${parent}/${day}_lp_vpts.csv"
    sp_in="${parent}/${day}_sp_vpts.csv"
    [[ -f "$lp_in" || -f "$sp_in" ]] || continue

    rel_parent="${parent#${INPUT_ROOT}/}"
    out_parent="${OUTPUT_ROOT}/${rel_parent}"
    expected=()
    if [[ -f "$lp_in" ]]; then
        expected+=("${out_parent}/${day}_lp_vits.csv")
    fi
    if [[ -f "$sp_in" ]]; then
        expected+=("${out_parent}/${day}_sp_vits.csv")
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
            echo "Skip (outputs present): ${parent} (${day})"
            ((total_skipped++))
            continue
        fi
    fi

    radar="all"
    if [[ "$parent" == *"/single-site/"* ]]; then
        radar="${parent#*/single-site/}"
        radar="${radar%%/*}"
    fi

    log_dir="${SLURM_ROOT}/${radar}/${day}"
    mkdir -p "$log_dir"
    job_name="biorad_vits_${radar}_${day}"

    export_env="ALL,RADAR_IN=${INPUT_ROOT},RADAR_OUT=${OUTPUT_ROOT},FORCE=${FORCE},ALT_MIN=${ALT_MIN},ALT_MAX=${ALT_MAX}"
    if [[ -n "$R_LIBS_USER_OVERRIDE" ]]; then
        export_env="${export_env},R_LIBS_USER=${R_LIBS_USER_OVERRIDE}"
    fi

    extra_args=""
    if [[ $FORCE -eq 1 ]]; then
        extra_args=" --force"
    fi

    wrap_cmd="bash -lc 'module load ${MODULE_NAME}; ${R_BIN} \"${R_SCRIPT}\" \"${day}\" --input-dir \"${parent}\" --input-root \"${INPUT_ROOT}\" --output-root \"${OUTPUT_ROOT}\" --alt-min \"${ALT_MIN}\" --alt-max \"${ALT_MAX}\"${extra_args}'"

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
        echo "Submitted: ${parent} (${day}) -> job ${JOB_ID}" | tee -a "$RUN_LOG"
        ((total_submitted++))
    else
        echo "Failed to submit job for ${parent} (${day})" | tee -a "$RUN_LOG" >&2
        ((total_skipped++))
    fi
done

echo "Submitted: ${total_submitted}, Skipped: ${total_skipped}" | tee -a "$RUN_LOG"
