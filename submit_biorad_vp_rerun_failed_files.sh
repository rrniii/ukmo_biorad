#!/usr/bin/env bash
# submit_biorad_vp_rerun_failed_files.sh
# --------------------------------------
# Submit file-level reruns for retryable failed VP targets.
# Input TSV must contain at least: input_file and day columns.

set -uo pipefail

INPUT_ROOT="/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/vol2birdinput"
OUTPUT_ROOT="/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/biorad_vp"
LOG_ROOT="/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/biorad_vp_logs"
PARTITION="standard"
QOS="short"
TIME_LIMIT="02:00:00"
ARRAY_MAX=200
MODULE_NAME="jasr"
R_BIN="Rscript"
RERUN_TSV=""
EXCLUDE_TSV=""
R_LIBS_USER_OVERRIDE=""
FORCE=0

usage() {
    cat <<EOF
Usage: $(basename "$0") --rerun-tsv FILE [options]
  Required:
    --rerun-tsv FILE     TSV with retry targets (must include input_file; day recommended).

  Optional:
    --exclude-tsv FILE   TSV/plain list of files to skip (e.g., bad file manifest).
    -p PARTITION         SLURM partition (default: ${PARTITION}).
    -q QOS               SLURM QoS (default: ${QOS}).
    -t TIME              SLURM time limit (default: ${TIME_LIMIT}).
    --array-max N        Max concurrent array tasks (default: ${ARRAY_MAX}).
    --in-root DIR        Override input root (default: ${INPUT_ROOT}).
    --out-root DIR       Override output root (default: ${OUTPUT_ROOT}).
    --log-root DIR       Override log root (default: ${LOG_ROOT}).
    --module NAME        Environment module to load for R (default: ${MODULE_NAME}).
    --rscript PATH       Rscript executable to use (default: ${R_BIN}).
    --r-libs-user DIR    Override R_LIBS_USER.
    -f                   Force processing even if outputs already exist.
    -h                   Show this help.
EOF
}

RUN_TS=$(date +"%Y%m%dT%H%M%S")
ORIG_CMD="$0 $*"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --rerun-tsv) RERUN_TSV="$2"; shift 2 ;;
        --exclude-tsv) EXCLUDE_TSV="$2"; shift 2 ;;
        -p) PARTITION="$2"; shift 2 ;;
        -q) QOS="$2"; shift 2 ;;
        -t) TIME_LIMIT="$2"; shift 2 ;;
        --array-max) ARRAY_MAX="$2"; shift 2 ;;
        --in-root) INPUT_ROOT="$2"; shift 2 ;;
        --out-root) OUTPUT_ROOT="$2"; shift 2 ;;
        --log-root) LOG_ROOT="$2"; shift 2 ;;
        --module) MODULE_NAME="$2"; shift 2 ;;
        --rscript) R_BIN="$2"; shift 2 ;;
        --r-libs-user) R_LIBS_USER_OVERRIDE="$2"; shift 2 ;;
        -f) FORCE=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
done

if [[ -z "$RERUN_TSV" ]]; then
    echo "--rerun-tsv is required" >&2
    usage
    exit 1
fi
if [[ ! -f "$RERUN_TSV" ]]; then
    echo "Rerun TSV not found: $RERUN_TSV" >&2
    exit 1
fi
if [[ -n "$EXCLUDE_TSV" && ! -f "$EXCLUDE_TSV" ]]; then
    echo "Exclude TSV not found: $EXCLUDE_TSV" >&2
    exit 1
fi

if ! [[ "$ARRAY_MAX" =~ ^[0-9]+$ ]] || [[ "$ARRAY_MAX" -le 0 ]]; then
    echo "--array-max must be a positive integer" >&2
    exit 1
fi

INPUT_ROOT="${INPUT_ROOT%/}"
OUTPUT_ROOT="${OUTPUT_ROOT%/}"
LOG_ROOT="${LOG_ROOT%/}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
R_SCRIPT="${SCRIPT_DIR}/run_biorad_vp_for_date.R"
if [[ ! -f "$R_SCRIPT" ]]; then
    echo "Cannot find run_biorad_vp_for_date.R at $R_SCRIPT" >&2
    exit 1
fi

mkdir -p "$LOG_ROOT"
RUN_DIR="${LOG_ROOT}/submit_biorad_vp_retry_failed_files_${RUN_TS}"
SLURM_ROOT="${RUN_DIR}/slurm_logs"
mkdir -p "$SLURM_ROOT"
RUN_LOG="${RUN_DIR}/submit_biorad_vp_retry_failed_files_${RUN_TS}.log"
{
    echo "Started: $(date +"%Y-%m-%d %H:%M:%S %Z")"
    echo "Command: ${ORIG_CMD}"
    echo "Rerun TSV: ${RERUN_TSV}"
    echo "Exclude TSV: ${EXCLUDE_TSV:-<none>}"
} >> "$RUN_LOG"

TASKS_ALL="${RUN_DIR}/retry_tasks_all.tsv"
TASKS_FINAL="${RUN_DIR}/retry_tasks_final.tsv"
EXCLUDE_LIST="${RUN_DIR}/exclude_input_files.list"

# Build all candidate tasks from rerun TSV.
awk -F '\t' '
    BEGIN { OFS="\t"; input_idx=0; day_idx=0; }
    NR==1 {
        for (i=1; i<=NF; i++) {
            if ($i=="input_file") input_idx=i;
            if ($i=="day") day_idx=i;
        }
        if (input_idx==0) {
            print "ERROR: input_file column not found in " FILENAME > "/dev/stderr";
            exit 2;
        }
        next;
    }
    {
        input_file=$input_idx;
        day="";
        if (day_idx>0) day=$day_idx;
        if (day=="" && match(input_file, /\/([0-9]{8})\//, m)) day=m[1];
        if (day=="" && match(input_file, /([0-9]{8})[^0-9]*\.h5$/, m2)) day=m2[1];
        if (input_file!="" && day!="") print day, input_file;
    }
' "$RERUN_TSV" | sort -u > "$TASKS_ALL"

if [[ -n "$EXCLUDE_TSV" ]]; then
    awk -F '\t' '
        NR==1 {
            idx=1;
            for (i=1; i<=NF; i++) if ($i=="input_file") idx=i;
            if (NF>1) {
                print $idx;
                next;
            }
        }
        { print $1; }
    ' "$EXCLUDE_TSV" | sed '/^$/d' | sort -u > "$EXCLUDE_LIST"
else
    : > "$EXCLUDE_LIST"
fi

# Apply exclude file list.
if [[ -s "$EXCLUDE_LIST" ]]; then
    awk -F '\t' 'NR==FNR { ex[$1]=1; next } !($2 in ex) { print $0 }' "$EXCLUDE_LIST" "$TASKS_ALL" > "$TASKS_FINAL"
else
    cp "$TASKS_ALL" "$TASKS_FINAL"
fi

N_TASKS=$(wc -l < "$TASKS_FINAL")
N_ALL=$(wc -l < "$TASKS_ALL")
N_EXCLUDED=$((N_ALL - N_TASKS))
{
    echo "Total retry candidates: ${N_ALL}"
    echo "Excluded by list: ${N_EXCLUDED}"
    echo "Final retry tasks: ${N_TASKS}"
} | tee -a "$RUN_LOG"

if [[ "$N_TASKS" -eq 0 ]]; then
    echo "No tasks to submit after exclude filtering." | tee -a "$RUN_LOG"
    exit 0
fi

TASK_RUNNER="${RUN_DIR}/run_retry_task.sh"
cat > "$TASK_RUNNER" <<EOF
#!/usr/bin/env bash
set -euo pipefail

TASKS_FILE="\$1"
TASK_ID="\${SLURM_ARRAY_TASK_ID:?SLURM_ARRAY_TASK_ID not set}"
LINE=\$(sed -n "\${TASK_ID}p" "\${TASKS_FILE}" || true)
if [[ -z "\${LINE}" ]]; then
  echo "No task line for array index \${TASK_ID}" >&2
  exit 1
fi

IFS=\$'\\t' read -r DAY INPUT_FILE <<< "\${LINE}"

module load ${MODULE_NAME}
${R_BIN} "${R_SCRIPT}" "\${DAY}" --input-file "\${INPUT_FILE}" --disable-hdf5-locking $( [[ ${FORCE} -eq 1 ]] && echo "--force" )
EOF
chmod +x "$TASK_RUNNER"

EXPORT_ENV="ALL,RADAR_IN=${INPUT_ROOT},RADAR_OUT=${OUTPUT_ROOT},FORCE=${FORCE}"
if [[ -n "$R_LIBS_USER_OVERRIDE" ]]; then
    EXPORT_ENV="${EXPORT_ENV},R_LIBS_USER=${R_LIBS_USER_OVERRIDE}"
fi

ARRAY_SPEC="1-${N_TASKS}%${ARRAY_MAX}"
if JOB_STR=$(sbatch \
    --account=ncas_radar \
    --partition="${PARTITION}" \
    --qos="${QOS}" \
    --time="${TIME_LIMIT}" \
    --array="${ARRAY_SPEC}" \
    --export="${EXPORT_ENV}" \
    --job-name="biorad_vp_retry_files" \
    -o "${SLURM_ROOT}/biorad_vp_retry_files-%A_%a.out" \
    -e "${SLURM_ROOT}/biorad_vp_retry_files-%A_%a.err" \
    --wrap="bash '${TASK_RUNNER}' '${TASKS_FINAL}'"); then
    JOB_ID=$(echo "$JOB_STR" | grep -o '[0-9][0-9]*' | tail -n1)
    {
        echo "Submitted array job: ${JOB_ID}"
        echo "Array spec: ${ARRAY_SPEC}"
        echo "Run dir: ${RUN_DIR}"
    } | tee -a "$RUN_LOG"
else
    echo "Failed to submit retry array job." | tee -a "$RUN_LOG" >&2
    exit 1
fi

