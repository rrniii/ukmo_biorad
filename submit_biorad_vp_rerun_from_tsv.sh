#!/usr/bin/env bash
# submit_biorad_vp_rerun_from_tsv.sh
# -----------------------------------
# Submit reruns for incomplete BioRad VP day jobs listed in a TSV file.
# The TSV must include at least: site, date. Optional columns: cause, input_dir.
#
# Default walltime is set to 02:00:00 (double the normal 01:00:00 used by
# submit_biorad_vp.sh).

set -uo pipefail

INPUT_ROOT="/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/vol2birdinput"
OUTPUT_ROOT="/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/biorad_vp"
LOG_ROOT="/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/biorad_vp_logs"
RERUN_TSV=""
PARTITION="standard"
QOS="standard"
TIME_LIMIT="02:00:00"
DISABLE_HDF5_LOCKING=1
MODULE_NAME="jasr"
R_BIN="Rscript"
R_LIBS_USER_OVERRIDE=""
DRY_RUN=0
LIMIT=0

usage() {
    cat <<EOF
Usage: $(basename "$0") --rerun-tsv FILE [options]
  Required:
    --rerun-tsv FILE   TSV with incomplete jobs (needs columns: site, date).
  Options:
    -p PARTITION       SLURM partition (default: ${PARTITION}).
    -q QOS             SLURM QoS (default: ${QOS}).
    -t TIME            SLURM time limit (default: ${TIME_LIMIT}).
    --limit N          Submit at most N unique site/date rows (default: all).
    --dry-run          Print actions but do not submit.
    --disable-hdf5-locking  Disable HDF5 file locking (default).
    --enable-hdf5-locking   Enable HDF5 file locking.
    --in-root DIR      Override input root (default: ${INPUT_ROOT}).
    --out-root DIR     Override output root (default: ${OUTPUT_ROOT}).
    --log-root DIR     Override log root (default: ${LOG_ROOT}).
    --module NAME      Environment module to load for R (default: ${MODULE_NAME}).
    --r-libs-user DIR  Override R_LIBS_USER for submitted jobs.
    --rscript PATH     Rscript executable (default: ${R_BIN}).
    -h                 Show help.
EOF
}

ORIG_CMD="$0 $*"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --rerun-tsv) RERUN_TSV="$2"; shift 2 ;;
        -p) PARTITION="$2"; shift 2 ;;
        -q) QOS="$2"; shift 2 ;;
        -t) TIME_LIMIT="$2"; shift 2 ;;
        --limit) LIMIT="$2"; shift 2 ;;
        --dry-run) DRY_RUN=1; shift ;;
        --disable-hdf5-locking) DISABLE_HDF5_LOCKING=1; shift ;;
        --enable-hdf5-locking) DISABLE_HDF5_LOCKING=0; shift ;;
        --in-root) INPUT_ROOT="$2"; shift 2 ;;
        --out-root) OUTPUT_ROOT="$2"; shift 2 ;;
        --log-root) LOG_ROOT="$2"; shift 2 ;;
        --module) MODULE_NAME="$2"; shift 2 ;;
        --r-libs-user) R_LIBS_USER_OVERRIDE="$2"; shift 2 ;;
        --rscript) R_BIN="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
done

if [[ -z "${RERUN_TSV}" ]]; then
    echo "Missing required option: --rerun-tsv FILE" >&2
    usage
    exit 1
fi
if [[ ! -f "${RERUN_TSV}" ]]; then
    echo "TSV not found: ${RERUN_TSV}" >&2
    exit 1
fi
if ! [[ "${LIMIT}" =~ ^[0-9]+$ ]]; then
    echo "--limit must be a non-negative integer; got '${LIMIT}'" >&2
    exit 1
fi

INPUT_ROOT="${INPUT_ROOT%/}"
OUTPUT_ROOT="${OUTPUT_ROOT%/}"
LOG_ROOT="${LOG_ROOT%/}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
R_SCRIPT="${SCRIPT_DIR}/run_biorad_vp_for_date.R"
if [[ ! -f "${R_SCRIPT}" ]]; then
    echo "Cannot find run_biorad_vp_for_date.R at ${R_SCRIPT}" >&2
    exit 1
fi

read -r header_line < "${RERUN_TSV}" || {
    echo "Could not read header from ${RERUN_TSV}" >&2
    exit 1
}
IFS=$'\t' read -r -a headers <<< "${header_line}"

site_idx=-1
date_idx=-1
cause_idx=-1
input_dir_idx=-1
for i in "${!headers[@]}"; do
    case "${headers[$i]}" in
        site) site_idx="$i" ;;
        date) date_idx="$i" ;;
        cause) cause_idx="$i" ;;
        input_dir) input_dir_idx="$i" ;;
    esac
done

if [[ "${site_idx}" -lt 0 || "${date_idx}" -lt 0 ]]; then
    echo "TSV must contain 'site' and 'date' columns: ${RERUN_TSV}" >&2
    exit 1
fi

RUN_TS=$(date +"%Y%m%dT%H%M%S")
mkdir -p "${LOG_ROOT}"
RUN_DIR="${LOG_ROOT}/submit_biorad_vp_rerun_${RUN_TS}"
SLURM_ROOT="${RUN_DIR}/slurm_logs"
mkdir -p "${SLURM_ROOT}"
RUN_LOG="${RUN_DIR}/submit_biorad_vp_rerun_${RUN_TS}.log"

{
    echo "Started: $(date +"%Y-%m-%d %H:%M:%S %Z")"
    echo "Command: ${ORIG_CMD}"
    echo "TSV: ${RERUN_TSV}"
    echo "Partition: ${PARTITION}"
    echo "QoS: ${QOS}"
    echo "Time limit: ${TIME_LIMIT}"
    echo "Dry run: ${DRY_RUN}"
} >> "${RUN_LOG}"

total_rows=0
total_unique=0
total_submitted=0
total_skipped=0
total_failed=0

declare -A seen

while IFS=$'\t' read -r -a row; do
    ((total_rows++))

    site="${row[$site_idx]:-}"
    day="${row[$date_idx]:-}"
    cause="UNKNOWN"
    if [[ "${cause_idx}" -ge 0 ]]; then
        cause="${row[$cause_idx]:-UNKNOWN}"
    fi

    if [[ -z "${site}" || -z "${day}" || ! "${day}" =~ ^[0-9]{8}$ ]]; then
        echo "Skip (invalid row): site='${site}' date='${day}'" | tee -a "${RUN_LOG}"
        ((total_skipped++))
        continue
    fi

    key="${site}|${day}"
    if [[ -n "${seen[$key]+x}" ]]; then
        continue
    fi
    if [[ "${LIMIT}" -gt 0 && "${total_unique}" -ge "${LIMIT}" ]]; then
        break
    fi
    seen["$key"]=1
    ((total_unique++))

    if [[ "${input_dir_idx}" -ge 0 && -n "${row[$input_dir_idx]:-}" ]]; then
        day_dir="${row[$input_dir_idx]}"
    else
        day_dir="${INPUT_ROOT}/single-site/${site}/${day:0:4}/${day}"
    fi

    if [[ ! -d "${day_dir}" ]]; then
        echo "Skip (missing input dir): ${site} ${day} ${day_dir}" | tee -a "${RUN_LOG}"
        ((total_skipped++))
        continue
    fi

    input_count=$(find "${day_dir}" -type f -name "*.h5" | wc -l | tr -d ' ')
    if [[ "${input_count}" -eq 0 ]]; then
        echo "Skip (no input files): ${site} ${day} ${day_dir}" | tee -a "${RUN_LOG}"
        ((total_skipped++))
        continue
    fi

    log_dir="${SLURM_ROOT}/${site}/${day}"
    mkdir -p "${log_dir}"
    job_name="biorad_vp_${site}_${day}_rerun"

    export_env="ALL,RADAR_IN=${INPUT_ROOT},RADAR_OUT=${OUTPUT_ROOT}"
    if [[ -n "${R_LIBS_USER_OVERRIDE}" ]]; then
        export_env="${export_env},R_LIBS_USER=${R_LIBS_USER_OVERRIDE}"
    fi

    extra_args=""
    if [[ "${DISABLE_HDF5_LOCKING}" -eq 1 ]]; then
        extra_args=" --disable-hdf5-locking"
    else
        extra_args=" --enable-hdf5-locking"
    fi

    wrap_cmd="bash -lc 'module load ${MODULE_NAME}; ${R_BIN} \"${R_SCRIPT}\" \"${day}\" --input-dir \"${day_dir}\"${extra_args}'"

    if [[ "${DRY_RUN}" -eq 1 ]]; then
        echo "DRY-RUN submit: site=${site} day=${day} cause=${cause} time=${TIME_LIMIT}" | tee -a "${RUN_LOG}"
        ((total_submitted++))
        continue
    fi

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
        JOB_ID=$(echo "${JOB_STR}" | grep -o '[0-9][0-9]*' | tail -n 1)
        echo "Submitted: ${site} ${day} cause=${cause} -> job ${JOB_ID}" | tee -a "${RUN_LOG}"
        ((total_submitted++))
    else
        echo "Failed submit: ${site} ${day} cause=${cause}" | tee -a "${RUN_LOG}" >&2
        ((total_failed++))
    fi
done < <(tail -n +2 "${RERUN_TSV}")

{
    echo "Rows read: ${total_rows}"
    echo "Unique site/date rows: ${total_unique}"
    echo "Submitted: ${total_submitted}"
    echo "Skipped: ${total_skipped}"
    echo "Submit failures: ${total_failed}"
    echo "Finished: $(date +"%Y-%m-%d %H:%M:%S %Z")"
} | tee -a "${RUN_LOG}"
