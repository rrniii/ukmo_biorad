#!/usr/bin/env Rscript
# Run bioRad/vol2bird for all HDF5 files on a given date directory.

# Keep startup noise down while loading bioRad.
suppressPackageStartupMessages(library(bioRad))

# --------- configuration you may want to edit ----------
# Use environment variables to override defaults without editing this file.
# Root directory containing vol2bird input files.
BASE_IN  <- Sys.getenv("RADAR_IN", unset = "/gws/nopw/j04/ncas_radar_vol2/avocet/ukmo-nimrod/vol2birdinput")

# Output root (mirrors the input tree down to the date directory).
BASE_OUT <- Sys.getenv("RADAR_OUT", unset = "/gws/nopw/j04/ncas_radar_vol2/avocet/ukmo-nimrod/biorad_vp")

# Which input files to consider as PVOL ODIM H5
INPUT_PATTERN <- Sys.getenv("RADAR_PATTERN", unset = "\\.h5$")

# Force re-run even if outputs exist (set FORCE=1 or pass --force).
FORCE_ENV <- Sys.getenv("FORCE", unset = "0")

# bioRad/vol2bird settings (C-band dual-pol baseline).
VP_SETTINGS <- list(
  autoconf  = FALSE,  # NOTE: autoconf=TRUE ignores other settings per bioRad docs.
  dual_pol  = TRUE,
  rho_hv    = 0.95,
  dealias   = TRUE,
  range_min = 5000,
  range_max = 35000,
  h_layer   = 200,
  n_layer   = 30,
  sd_vvp_threshold = 2,
  rcs = 11
)
# ------------------------------------------------------

# Convert common truthy strings to a logical.
is_true <- function(x) {
  x <- tolower(as.character(x))
  x %in% c("1", "true", "yes", "y")
}

# Print CLI usage.
usage <- function() {
  cat("Usage: run_biorad_vp_for_date.R <YYYYMMDD> [--input-root PATH] [--output-root PATH] [--pattern REGEX] [--force]\n")
}

# Parse CLI args; first positional arg must be the date string.
args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1 || args[1] %in% c("-h", "--help")) {
  usage()
  quit(status = 1)
}

# Validate YYYYMMDD format early so errors are explicit.
date_str <- args[1]
if (!grepl("^[0-9]{8}$", date_str)) {
  stop("Date must be YYYYMMDD (e.g., 20250505).", call. = FALSE)
}

# Combine env-based FORCE with CLI --force.
force <- is_true(FORCE_ENV)

# Optional flags that override defaults.
i <- 2
while (i <= length(args)) {
  opt <- args[i]
  if (opt == "--input-root") {
    if (i == length(args)) stop("--input-root requires a value.", call. = FALSE)
    BASE_IN <- args[i + 1]
    i <- i + 2
    next
  }
  if (opt == "--output-root") {
    if (i == length(args)) stop("--output-root requires a value.", call. = FALSE)
    BASE_OUT <- args[i + 1]
    i <- i + 2
    next
  }
  if (opt == "--pattern") {
    if (i == length(args)) stop("--pattern requires a value.", call. = FALSE)
    INPUT_PATTERN <- args[i + 1]
    i <- i + 2
    next
  }
  if (opt == "--force") {
    force <- TRUE
    i <- i + 1
    next
  }
  stop("Unknown option: ", opt, call. = FALSE)
}

# Ensure input root exists before scanning.
if (!dir.exists(BASE_IN)) {
  stop("Input root does not exist: ", BASE_IN, call. = FALSE)
}

# Normalized input root for safe prefix comparisons.
base_in_norm <- sub("/+$", "", normalizePath(BASE_IN, winslash = "/", mustWork = TRUE))

# Locate date directories under the input root; prefer system find for speed.
find_date_dirs <- function(root, date_str) {
  find_bin <- Sys.which("find")
  if (nzchar(find_bin)) {
    res <- suppressWarnings(system2(find_bin, c(root, "-type", "d", "-name", date_str),
                                    stdout = TRUE, stderr = NULL))
    res <- res[nzchar(res)]
    if (length(res) > 0) return(res)
  }
  dirs <- list.dirs(root, recursive = TRUE, full.names = TRUE)
  dirs[basename(dirs) == date_str]
}

# Convert an absolute path to a relative path under a given prefix.
relative_to <- function(path, prefix) {
  path_norm <- sub("/+$", "", normalizePath(path, winslash = "/", mustWork = TRUE))
  prefix_norm <- sub("/+$", "", prefix)
  if (startsWith(path_norm, prefix_norm)) {
    rel <- substr(path_norm, nchar(prefix_norm) + 2, nchar(path_norm))
    if (rel == "") rel <- "."
    return(rel)
  }
  basename(path_norm)
}

# Find matching date directories; this allows nested radar/year/date layouts.
date_dirs <- find_date_dirs(BASE_IN, date_str)
if (length(date_dirs) == 0) {
  stop("No date directories found under input root for date: ", date_str, call. = FALSE)
}

# Wrapper for calculate_vp with dynamic settings list.
run_vp_write <- function(pvol, vpfile, settings) {
  do.call(calculate_vp, c(list(file = pvol, vpfile = vpfile), settings))
}

# Counters for final summary.
total_ok <- 0L
total_fail <- 0L
total_skip <- 0L

# Process each matching date directory.
for (date_dir in sort(date_dirs)) {
  # Gather all HDF5 files (recursive) for this date directory.
  files <- list.files(date_dir, pattern = INPUT_PATTERN, recursive = TRUE, full.names = TRUE)
  if (length(files) == 0) {
    cat("No input files in:", date_dir, "\n")
    next
  }

  # Mirror the input tree under BASE_OUT and add standard output subfolders.
  rel_day <- relative_to(date_dir, base_in_norm)
  out_day <- file.path(BASE_OUT, rel_day)
  out_dir_csv <- file.path(out_day, "vpts_csv")
  out_dir_h5  <- file.path(out_day, "vp_h5")
  dir.create(out_dir_csv, recursive = TRUE, showWarnings = FALSE)
  dir.create(out_dir_h5,  recursive = TRUE, showWarnings = FALSE)

  cat("Date:", date_str, "\n")
  cat("Input directory:", date_dir, "\n")
  cat("Found", length(files), "file(s)\n")
  cat("Output CSV directory:", out_dir_csv, "\n")
  cat("Output H5 directory :", out_dir_h5,  "\n\n")

  for (f in sort(files)) {
    # Build output filenames that align with input basenames.
    base <- tools::file_path_sans_ext(basename(f))
    out_csv <- file.path(out_dir_csv, paste0(base, "_vp.csv"))
    out_h5  <- file.path(out_dir_h5,  paste0(base, "_vp.h5"))

    # Skip if outputs already exist and not forcing a re-run.
    if (!force && file.exists(out_csv) && file.exists(out_h5)) {
      cat("Skipping (outputs exist):", f, "\n")
      total_skip <- total_skip + 1L
      next
    }

    cat("Processing:", f, "\n")
    tryCatch({
      # Write both VPTS CSV and ODIM HDF5 outputs.
      run_vp_write(f, out_csv, VP_SETTINGS)
      run_vp_write(f, out_h5,  VP_SETTINGS)
      cat("  OK ->", out_csv, "\n")
      cat("  OK ->", out_h5,  "\n\n")
      total_ok <- total_ok + 1L
    }, error = function(e) {
      cat("  FAILED:", conditionMessage(e), "\n\n")
      total_fail <- total_fail + 1L
    })
  }
}

# Final summary and non-zero exit when any failures occurred.
cat("Done. Success:", total_ok, " Skipped:", total_skip, " Failed:", total_fail, "\n")
if (total_fail > 0) quit(status = 2)
