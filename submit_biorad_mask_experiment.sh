#!/usr/bin/env bash
# Submit BioDAR VP masking threshold experiments to Slurm.
#
# Inputs are existing pvol HDF5 files. Outputs are written only under the
# experiment root and source pvol files are never modified.

set -uo pipefail

INPUT_ROOT="/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/vol2birdinput"
EXPERIMENT_ROOT="/gws/ssde/j25a/ncas_radar/vol2/avocet/experiments/biorad_mask_threshold_scan"
LABEL_FILE=""
THRESHOLD_GRID=""
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
PROFILES="all"
START_DATE="00000000"
END_DATE="99999999"
RADAR_FILTER=""
MAX_CASES=0
MAX_ACTIVE=200
SLEEP_SECONDS=60
FORCE=0
WRITE_H5=0
PARTITION="standard"
QOS="standard"
TIME_LIMIT="02:00:00"
MODULE_NAME="jasr"
R_BIN="Rscript"
R_LIBS_USER_OVERRIDE="${HOME}/R/library"

usage() {
    cat <<EOF
Usage: $(basename "$0") --label-file TSV --threshold-grid TSV [options]

Options:
  --run-id ID          Run identifier under EXPERIMENT_ROOT/runs (default UTC timestamp).
  --profiles LIST     Comma-separated profile IDs, or "all" (default: all).
  -r RADAR            Only process one radar.
  -s START_DATE       Inclusive start date YYYYMMDD.
  -e END_DATE         Inclusive end date YYYYMMDD.
  --max-cases N       Limit submitted radar/date rows after filters (0 = no limit).
  --max-active N      Max active maskvp_* Slurm jobs before waiting (default: ${MAX_ACTIVE}).
  --sleep-seconds N   Wait interval while throttled (default: ${SLEEP_SECONDS}).
  -f                  Force VP outputs to be regenerated.
  --write-h5          Also write VP HDF5 outputs. Default is CSV-only to save space.
  --in-root DIR       pvol input root (default: ${INPUT_ROOT}).
  --experiment-root D Experiment root (default: ${EXPERIMENT_ROOT}).
  -p PARTITION        Slurm partition (default: ${PARTITION}).
  -q QOS              Slurm QoS (default: ${QOS}).
  -t TIME             Slurm time limit (default: ${TIME_LIMIT}).
  --module NAME       R module to load (default: ${MODULE_NAME}).
  --rscript PATH      Rscript executable (default: ${R_BIN}).
  --r-libs-user DIR   R user library (default: ${R_LIBS_USER_OVERRIDE}).
  -h                  Show help.

Label TSV columns required: radar, date. Other columns are copied through.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --label-file) LABEL_FILE="$2"; shift 2 ;;
        --threshold-grid) THRESHOLD_GRID="$2"; shift 2 ;;
        --run-id) RUN_ID="$2"; shift 2 ;;
        --profiles) PROFILES="$2"; shift 2 ;;
        -r) RADAR_FILTER="$2"; shift 2 ;;
        -s) START_DATE="$2"; shift 2 ;;
        -e) END_DATE="$2"; shift 2 ;;
        --max-cases) MAX_CASES="$2"; shift 2 ;;
        --max-active) MAX_ACTIVE="$2"; shift 2 ;;
        --sleep-seconds) SLEEP_SECONDS="$2"; shift 2 ;;
        -f) FORCE=1; shift ;;
        --write-h5) WRITE_H5=1; shift ;;
        --in-root) INPUT_ROOT="$2"; shift 2 ;;
        --experiment-root) EXPERIMENT_ROOT="$2"; shift 2 ;;
        -p) PARTITION="$2"; shift 2 ;;
        -q) QOS="$2"; shift 2 ;;
        -t) TIME_LIMIT="$2"; shift 2 ;;
        --module) MODULE_NAME="$2"; shift 2 ;;
        --rscript) R_BIN="$2"; shift 2 ;;
        --r-libs-user) R_LIBS_USER_OVERRIDE="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
done

if [[ -z "$LABEL_FILE" || -z "$THRESHOLD_GRID" ]]; then
    usage >&2
    exit 1
fi
if [[ ! -f "$LABEL_FILE" ]]; then
    echo "Label file does not exist: $LABEL_FILE" >&2
    exit 1
fi
if [[ ! -f "$THRESHOLD_GRID" ]]; then
    echo "Threshold grid does not exist: $THRESHOLD_GRID" >&2
    exit 1
fi
if [[ ! -d "$INPUT_ROOT" ]]; then
    echo "Input root does not exist: $INPUT_ROOT" >&2
    exit 1
fi
for d in "$START_DATE" "$END_DATE"; do
    if ! [[ "$d" =~ ^[0-9]{8}$ ]]; then
        echo "Dates must be YYYYMMDD; got '$d'" >&2
        exit 1
    fi
done
for n in "$MAX_CASES" "$MAX_ACTIVE" "$SLEEP_SECONDS"; do
    if ! [[ "$n" =~ ^[0-9]+$ ]]; then
        echo "Numeric options must be non-negative integers; got '$n'" >&2
        exit 1
    fi
done
if [[ "$MAX_ACTIVE" -lt 1 || "$SLEEP_SECONDS" -lt 1 ]]; then
    echo "--max-active and --sleep-seconds must be >= 1" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
R_SCRIPT="${SCRIPT_DIR}/run_biorad_vp_mask_experiment_for_date.R"
if [[ ! -f "$R_SCRIPT" ]]; then
    echo "Cannot find $R_SCRIPT" >&2
    exit 1
fi

INPUT_ROOT="${INPUT_ROOT%/}"
EXPERIMENT_ROOT="${EXPERIMENT_ROOT%/}"
RUN_DIR="${EXPERIMENT_ROOT}/runs/${RUN_ID}"
VP_ROOT="${RUN_DIR}/vp"
DIAG_ROOT="${RUN_DIR}/diagnostics"
LOG_ROOT="${RUN_DIR}/logs"
MANIFEST_ROOT="${RUN_DIR}/manifests"
mkdir -p "$VP_ROOT" "$DIAG_ROOT" "$LOG_ROOT/slurm" "$MANIFEST_ROOT" "${RUN_DIR}/configs"

cp -p "$LABEL_FILE" "${RUN_DIR}/configs/$(basename "$LABEL_FILE")"
cp -p "$THRESHOLD_GRID" "${RUN_DIR}/configs/$(basename "$THRESHOLD_GRID")"

SUBMIT_LOG="${LOG_ROOT}/submit_mask_experiment.log"
MANIFEST="${MANIFEST_ROOT}/submitted_vp_jobs.tsv"
SKIPPED="${MANIFEST_ROOT}/skipped_vp_jobs.tsv"
printf "profile_id\tradar\tdate\tinput_dir\tjob_id\n" > "$MANIFEST"
printf "profile_id\tradar\tdate\treason\tinput_dir\n" > "$SKIPPED"

if [[ "$PROFILES" == "all" ]]; then
    mapfile -t PROFILE_LIST < <(awk -F '\t' 'NR > 1 && $1 != "" {print $1}' "$THRESHOLD_GRID")
else
    IFS=',' read -r -a PROFILE_LIST <<< "$PROFILES"
fi
if [[ ${#PROFILE_LIST[@]} -eq 0 ]]; then
    echo "No profiles selected." >&2
    exit 1
fi

active_mask_jobs() {
    squeue -u "$USER" -h -o "%j" | awk '$1 ~ /^maskvp_/ {n++} END {print n + 0}'
}

find_day_dir() {
    local radar="$1"
    local date="$2"
    local year="${date:0:4}"
    local scan_base="$INPUT_ROOT"
    if [[ -d "${INPUT_ROOT}/single-site" ]]; then
        scan_base="${INPUT_ROOT}/single-site"
    fi
    local expected="${scan_base}/${radar}/${year}/${date}"
    if [[ -d "$expected" ]]; then
        printf '%s\n' "$expected"
        return 0
    fi
    find "${scan_base}/${radar}" -type d -name "$date" -print -quit 2>/dev/null
}

submitted=0
skipped=0
case_count=0

{
    echo "Started: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo "Run dir: $RUN_DIR"
    echo "Input root: $INPUT_ROOT"
    echo "Label file: $LABEL_FILE"
    echo "Threshold grid: $THRESHOLD_GRID"
    echo "Profiles: ${PROFILE_LIST[*]}"
    echo "bioRad target version: 0.12.0"
} | tee -a "$SUBMIT_LOG"

while IFS=$'\t' read -r radar date rest; do
    [[ "$radar" == "radar" ]] && continue
    [[ -z "$radar" || -z "$date" ]] && continue
    if ! [[ "$date" =~ ^[0-9]{8}$ ]]; then
        printf "all\t%s\t%s\tbad_date\t\n" "$radar" "$date" >> "$SKIPPED"
        ((skipped++))
        continue
    fi
    if [[ "$date" < "$START_DATE" || "$date" > "$END_DATE" ]]; then
        continue
    fi
    if [[ -n "$RADAR_FILTER" && "$radar" != "$RADAR_FILTER" ]]; then
        continue
    fi
    if [[ "$MAX_CASES" -gt 0 && "$case_count" -ge "$MAX_CASES" ]]; then
        break
    fi
    day_dir="$(find_day_dir "$radar" "$date")"
    if [[ -z "$day_dir" || ! -d "$day_dir" ]]; then
        for profile in "${PROFILE_LIST[@]}"; do
            printf "%s\t%s\t%s\tinput_missing\t%s\n" "$profile" "$radar" "$date" "$day_dir" >> "$SKIPPED"
        done
        ((skipped++))
        continue
    fi

    ((case_count++))
    for profile in "${PROFILE_LIST[@]}"; do
        while [[ "$(active_mask_jobs)" -ge "$MAX_ACTIVE" ]]; do
            echo "Wait: active_mask_jobs=$(active_mask_jobs) max_active=${MAX_ACTIVE}" | tee -a "$SUBMIT_LOG"
            sleep "$SLEEP_SECONDS"
        done

        log_dir="${LOG_ROOT}/slurm/${profile}/${radar}/${date}"
        mkdir -p "$log_dir"
        job_profile="$(printf '%s' "$profile" | cut -c1-36)"
        job_name="maskvp_${job_profile}_${radar}_${date}"

        export_env="ALL,RADAR_IN=${INPUT_ROOT},RADAR_OUT=${VP_ROOT}/${profile},DIAGNOSTICS_ROOT=${DIAG_ROOT}/${profile},FORCE=${FORCE},MASK_PROFILE_ID=${profile},MASK_THRESHOLD_GRID=${THRESHOLD_GRID},PARAMS_TO_READ=all,R_LIBS_USER=${R_LIBS_USER_OVERRIDE}"
        extra_args=" --disable-hdf5-locking"
        if [[ "$FORCE" -eq 1 ]]; then
            extra_args="${extra_args} --force"
        fi
        if [[ "$WRITE_H5" -eq 0 ]]; then
            extra_args="${extra_args} --csv-only"
        fi

        wrap_cmd="bash -lc 'module load ${MODULE_NAME}; ${R_BIN} \"${R_SCRIPT}\" \"${date}\" --input-dir \"${day_dir}\" --input-root \"${INPUT_ROOT}\" --output-root \"${VP_ROOT}/${profile}\" --diagnostics-root \"${DIAG_ROOT}/${profile}\" --profile-id \"${profile}\" --threshold-grid \"${THRESHOLD_GRID}\"${extra_args}'"

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
            printf "%s\t%s\t%s\t%s\t%s\n" "$profile" "$radar" "$date" "$day_dir" "$job_id" >> "$MANIFEST"
            echo "Submitted: $profile $radar $date -> $job_id" | tee -a "$SUBMIT_LOG"
            ((submitted++))
        else
            printf "%s\t%s\t%s\tsbatch_failed\t%s\n" "$profile" "$radar" "$date" "$day_dir" >> "$SKIPPED"
            ((skipped++))
        fi
    done
done < "$LABEL_FILE"

echo "Submitted: $submitted, skipped: $skipped, run_dir: $RUN_DIR" | tee -a "$SUBMIT_LOG"
