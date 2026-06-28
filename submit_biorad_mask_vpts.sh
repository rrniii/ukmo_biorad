#!/usr/bin/env bash
# Build experiment VPTS CSVs from completed mask-experiment VP CSVs.

set -uo pipefail

EXPERIMENT_ROOT="/gws/ssde/j25a/ncas_radar/vol2/avocet/experiments/biorad_mask_threshold_scan"
RUN_ID=""
PROFILES="all"
START_DATE="00000000"
END_DATE="99999999"
RADAR_FILTER=""
MAX_ACTIVE=100
SLEEP_SECONDS=60
FORCE=0
PARTITION="standard"
QOS="standard"
TIME_LIMIT="01:00:00"
MODULE_NAME="jasr"
R_BIN="Rscript"
R_LIBS_USER_OVERRIDE="${HOME}/R/library"
CHUNK_SIZE=100

usage() {
    cat <<EOF
Usage: $(basename "$0") --run-id ID [options]
  --profiles LIST       Comma-separated profile IDs or "all" (default: all).
  -r RADAR              Only process this radar.
  -s START_DATE         Inclusive start date YYYYMMDD.
  -e END_DATE           Inclusive end date YYYYMMDD.
  -f                    Force VPTS outputs to be regenerated.
  --max-active N        Max active maskvpts_* jobs (default: ${MAX_ACTIVE}).
  --sleep-seconds N     Wait interval when throttled (default: ${SLEEP_SECONDS}).
  --experiment-root DIR Experiment root (default: ${EXPERIMENT_ROOT}).
  -p PARTITION          Slurm partition (default: ${PARTITION}).
  -q QOS                Slurm QoS (default: ${QOS}).
  -t TIME               Slurm time limit (default: ${TIME_LIMIT}).
  --chunk-size N        VP CSV chunk size for read_vpts (default: ${CHUNK_SIZE}).
  --module NAME         R module to load (default: ${MODULE_NAME}).
  --rscript PATH        Rscript executable (default: ${R_BIN}).
  --r-libs-user DIR     R user library (default: ${R_LIBS_USER_OVERRIDE}).
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --run-id) RUN_ID="$2"; shift 2 ;;
        --profiles) PROFILES="$2"; shift 2 ;;
        -r) RADAR_FILTER="$2"; shift 2 ;;
        -s) START_DATE="$2"; shift 2 ;;
        -e) END_DATE="$2"; shift 2 ;;
        -f) FORCE=1; shift ;;
        --max-active) MAX_ACTIVE="$2"; shift 2 ;;
        --sleep-seconds) SLEEP_SECONDS="$2"; shift 2 ;;
        --experiment-root) EXPERIMENT_ROOT="$2"; shift 2 ;;
        -p) PARTITION="$2"; shift 2 ;;
        -q) QOS="$2"; shift 2 ;;
        -t) TIME_LIMIT="$2"; shift 2 ;;
        --chunk-size) CHUNK_SIZE="$2"; shift 2 ;;
        --module) MODULE_NAME="$2"; shift 2 ;;
        --rscript) R_BIN="$2"; shift 2 ;;
        --r-libs-user) R_LIBS_USER_OVERRIDE="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
done

if [[ -z "$RUN_ID" ]]; then
    usage >&2
    exit 1
fi
for d in "$START_DATE" "$END_DATE"; do
    if ! [[ "$d" =~ ^[0-9]{8}$ ]]; then
        echo "Dates must be YYYYMMDD; got '$d'" >&2
        exit 1
    fi
done
for n in "$MAX_ACTIVE" "$SLEEP_SECONDS" "$CHUNK_SIZE"; do
    if ! [[ "$n" =~ ^[0-9]+$ ]] || [[ "$n" -lt 1 ]]; then
        echo "Numeric options must be positive integers; got '$n'" >&2
        exit 1
    fi
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
R_SCRIPT="${SCRIPT_DIR}/run_biorad_vpts_for_date.R"
if [[ ! -f "$R_SCRIPT" ]]; then
    echo "Cannot find $R_SCRIPT" >&2
    exit 1
fi

EXPERIMENT_ROOT="${EXPERIMENT_ROOT%/}"
RUN_DIR="${EXPERIMENT_ROOT}/runs/${RUN_ID}"
VP_ROOT="${RUN_DIR}/vp"
VPTS_ROOT="${RUN_DIR}/vpts"
LOG_ROOT="${RUN_DIR}/logs"
MANIFEST_ROOT="${RUN_DIR}/manifests"
if [[ ! -d "$VP_ROOT" ]]; then
    echo "VP root does not exist: $VP_ROOT" >&2
    exit 1
fi
mkdir -p "$VPTS_ROOT" "$LOG_ROOT/slurm_vpts" "$MANIFEST_ROOT"

if [[ "$PROFILES" == "all" ]]; then
    mapfile -t PROFILE_LIST < <(find "$VP_ROOT" -mindepth 1 -maxdepth 1 -type d -printf '%f\n' | sort)
else
    IFS=',' read -r -a PROFILE_LIST <<< "$PROFILES"
fi

MANIFEST="${MANIFEST_ROOT}/submitted_vpts_jobs.tsv"
printf "profile_id\tradar\tdate\tinput_dir\tjob_id\n" > "$MANIFEST"
SUBMIT_LOG="${LOG_ROOT}/submit_mask_vpts.log"

active_vpts_jobs() {
    squeue -u "$USER" -h -o "%j" | awk '$1 ~ /^maskvpts_/ {n++} END {print n + 0}'
}

submitted=0
skipped=0

for profile in "${PROFILE_LIST[@]}"; do
    profile_in="${VP_ROOT}/${profile}"
    [[ -d "$profile_in" ]] || continue
    mapfile -t day_dirs < <(find "$profile_in" -type d -name "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]" | sort)
    for day_dir in "${day_dirs[@]}"; do
        day="$(basename "$day_dir")"
        if [[ "$day" < "$START_DATE" || "$day" > "$END_DATE" ]]; then
            continue
        fi
        radar="all"
        if [[ "$day_dir" == *"/single-site/"* ]]; then
            radar="${day_dir#*/single-site/}"
            radar="${radar%%/*}"
        fi
        if [[ -n "$RADAR_FILTER" && "$radar" != "$RADAR_FILTER" ]]; then
            continue
        fi

        csv_dir="${day_dir}/vpts_csv"
        [[ -d "$csv_dir" ]] || csv_dir="$day_dir"
        csv_count=$(find "$csv_dir" -type f -name "*_vp.csv" | wc -l | tr -d ' ')
        if [[ "$csv_count" -eq 0 ]]; then
            ((skipped++))
            continue
        fi

        while [[ "$(active_vpts_jobs)" -ge "$MAX_ACTIVE" ]]; do
            echo "Wait: active_vpts_jobs=$(active_vpts_jobs) max_active=${MAX_ACTIVE}" | tee -a "$SUBMIT_LOG"
            sleep "$SLEEP_SECONDS"
        done

        log_dir="${LOG_ROOT}/slurm_vpts/${profile}/${radar}/${day}"
        mkdir -p "$log_dir"
        job_profile="$(printf '%s' "$profile" | cut -c1-34)"
        job_name="maskvpts_${job_profile}_${radar}_${day}"
        export_env="ALL,RADAR_IN=${profile_in},RADAR_OUT=${VPTS_ROOT}/${profile},FORCE=${FORCE},CHUNK_SIZE=${CHUNK_SIZE},R_LIBS_USER=${R_LIBS_USER_OVERRIDE}"
        extra_args=""
        if [[ "$FORCE" -eq 1 ]]; then
            extra_args=" --force"
        fi
        wrap_cmd="bash -lc 'module load ${MODULE_NAME}; ${R_BIN} \"${R_SCRIPT}\" \"${day}\" --input-dir \"${day_dir}\" --input-root \"${profile_in}\" --output-root \"${VPTS_ROOT}/${profile}\" --chunk-size \"${CHUNK_SIZE}\"${extra_args}'"

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
            job_id=$(echo "$JOB_STR" | grep -o '[0-9][0-9]*' | tail -n1)
            printf "%s\t%s\t%s\t%s\t%s\n" "$profile" "$radar" "$day" "$day_dir" "$job_id" >> "$MANIFEST"
            echo "Submitted: $profile $radar $day -> $job_id" | tee -a "$SUBMIT_LOG"
            ((submitted++))
        else
            ((skipped++))
        fi
    done
done

echo "Submitted: $submitted, skipped: $skipped, run_dir: $RUN_DIR" | tee -a "$SUBMIT_LOG"
