#!/usr/bin/env Rscript
# Run bioRad/vol2bird for all HDF5 files on a given date directory.

# Auto-add user library if bioRad is installed there.
user_lib <- file.path(Sys.getenv("HOME", unset = "~"), "R", "library")
if (dir.exists(file.path(user_lib, "bioRad"))) {
  .libPaths(c(user_lib, .libPaths()))
}

# Optionally disable HDF5 file locking before loading bioRad.
args_pre <- commandArgs(trailingOnly = TRUE)
disable_hdf5_locking_env <- Sys.getenv("DISABLE_HDF5_LOCKING", unset = "1")
disable_hdf5_locking <- tolower(disable_hdf5_locking_env) %in% c("1", "true", "yes", "y") ||
  "--disable-hdf5-locking" %in% args_pre
if ("--enable-hdf5-locking" %in% args_pre) {
  disable_hdf5_locking <- FALSE
}
if (disable_hdf5_locking) {
  Sys.setenv(HDF5_USE_FILE_LOCKING = "FALSE")
}

# Keep startup noise down while loading bioRad.
suppressPackageStartupMessages(library(bioRad))

# --------- configuration you may want to edit ----------
# Use environment variables to override defaults without editing this file.
# Root directory containing vol2bird input files.
BASE_IN  <- Sys.getenv("RADAR_IN", unset = "/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/vol2birdinput")

# Output root (mirrors the input tree down to the date directory).
BASE_OUT <- Sys.getenv("RADAR_OUT", unset = "/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/biorad_vp")

# Which input files to consider as PVOL ODIM H5
INPUT_PATTERN <- Sys.getenv("RADAR_PATTERN", unset = "\\.h5$")

# Force re-run even if outputs exist (set FORCE=1 or pass --force).
FORCE_ENV <- Sys.getenv("FORCE", unset = "0")

# Optional override to process a single date directory.
INPUT_DIR <- ""

# Optional override to process a single file (debug mode).
INPUT_FILE <- ""

# Optional mode to skip HDF5 output (CSV-only).
SKIP_H5_ENV <- Sys.getenv("SKIP_H5", unset = "0")

# Quality filter (always on): mask DBZH using SQIH only.
DBZ_PARAM <- "DBZH"
SQI_PARAM <- "SQIH"
SQI_THR <- 0.20
EXTRA_PARAMS <- c("VRADH", "RHOHV", "ZDR", "PHIDP")
PARAMS_TO_READ <- unique(c(DBZ_PARAM, SQI_PARAM, EXTRA_PARAMS))

# bioRad/vol2bird settings (C-band dual-pol baseline).
# NOTE: autoconf must be FALSE to allow per-file nyquist_min overrides.
VP_SETTINGS <- list(
  autoconf  = FALSE,
  dual_pol  = TRUE,
  rho_hv    = 0.97,
  dealias   = TRUE,
  range_min = 5000,
  range_max = 35000,
  h_layer   = 200,
  n_layer   = 25,
  sd_vvp_threshold = 2,
  rcs = 11
)
# ------------------------------------------------------

# Convert common truthy strings to a logical.
is_true <- function(x) {
  x <- tolower(as.character(x))
  x %in% c("1", "true", "yes", "y")
}

# Combine env-based SKIP_H5 with CLI --csv-only.
skip_h5 <- is_true(SKIP_H5_ENV)

# Print CLI usage.
usage <- function() {
  cat("Usage: run_biorad_vp_for_date.R <YYYYMMDD> [--input-root PATH] [--input-dir DIR] [--input-file FILE] [--output-root PATH] [--pattern REGEX] [--csv-only] [--disable-hdf5-locking] [--enable-hdf5-locking] [--force]\n")
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
  if (opt == "--input-dir") {
    if (i == length(args)) stop("--input-dir requires a value.", call. = FALSE)
    INPUT_DIR <- args[i + 1]
    i <- i + 2
    next
  }
  if (opt == "--input-file") {
    if (i == length(args)) stop("--input-file requires a value.", call. = FALSE)
    INPUT_FILE <- args[i + 1]
    i <- i + 2
    next
  }
  if (opt == "--csv-only") {
    skip_h5 <- TRUE
    i <- i + 1
    next
  }
  if (opt == "--disable-hdf5-locking") {
    disable_hdf5_locking <- TRUE
    Sys.setenv(HDF5_USE_FILE_LOCKING = "FALSE")
    i <- i + 1
    next
  }
  if (opt == "--enable-hdf5-locking") {
    disable_hdf5_locking <- FALSE
    Sys.unsetenv("HDF5_USE_FILE_LOCKING")
    i <- i + 1
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

# Prevent conflicting overrides.
if (nzchar(INPUT_DIR) && nzchar(INPUT_FILE)) {
  stop("Use only one of --input-dir or --input-file.", call. = FALSE)
}

# If a specific input directory was provided, ensure it exists.
if (nzchar(INPUT_DIR) && !dir.exists(INPUT_DIR)) {
  stop("Input dir does not exist: ", INPUT_DIR, call. = FALSE)
}

# If a single input file was provided, ensure it exists.
if (nzchar(INPUT_FILE) && !file.exists(INPUT_FILE)) {
  stop("Input file does not exist: ", INPUT_FILE, call. = FALSE)
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

# Find the date directory (YYYYMMDD) that contains a given file.
find_date_dir_for_file <- function(path, date_str) {
  dir <- normalizePath(dirname(path), winslash = "/", mustWork = TRUE)
  while (dir != "/" && dir != ".") {
    if (basename(dir) == date_str) return(dir)
    dir <- dirname(dir)
  }
  ""
}

# Find matching date directories; this allows nested radar/year/date layouts.
if (nzchar(INPUT_FILE)) {
  date_dir <- find_date_dir_for_file(INPUT_FILE, date_str)
  if (date_dir == "") {
    stop("Input file is not under a date directory matching ", date_str, call. = FALSE)
  }
  date_dirs <- date_dir
} else if (nzchar(INPUT_DIR)) {
  input_dir_norm <- normalizePath(INPUT_DIR, winslash = "/", mustWork = TRUE)
  if (basename(input_dir_norm) != date_str) {
    stop("Input dir basename must match date: ", basename(input_dir_norm), " != ", date_str, call. = FALSE)
  }
  date_dirs <- input_dir_norm
} else {
  date_dirs <- find_date_dirs(BASE_IN, date_str)
}
if (length(date_dirs) == 0) {
  stop("No date directories found under input root for date: ", date_str, call. = FALSE)
}

# Determine pulse type from filename/path to support per-pulse settings.
detect_pulse <- function(path) {
  base <- basename(path)
  if (grepl("_lp_", base) || grepl("/lp/", path)) return("lp")
  if (grepl("_sp_", base) || grepl("/sp/", path)) return("sp")
  ""
}

# Apply per-pulse settings: lower nyquist_min for LP; SP uses bioRad defaults.
settings_for_file <- function(path) {
  pulse <- detect_pulse(path)
  if (pulse == "lp") {
    return(c(VP_SETTINGS, list(nyquist_min = 1)))
  }
  VP_SETTINGS
}

# Apply SQIH/CI thresholds to DBZH gates (always).
apply_quality_filter <- function(pvol, dbz_name, sqi_name, sqi_thr, source_path) {
  for (i in seq_along(pvol$scans)) {
    sc <- pvol$scans[[i]]
    missing <- setdiff(c(dbz_name, sqi_name), names(sc$params))
    if (length(missing) > 0) {
      stop("Missing params in ", source_path, ": ", paste(missing, collapse = ", "),
           call. = FALSE)
    }

    dbz <- sc$params[[dbz_name]]
    sqi <- sc$params[[sqi_name]]

    # param objects are matrices with attributes; operate on their numeric values.
    dbz_vals <- unclass(dbz)
    sqi_vals <- unclass(sqi)
    storage.mode(dbz_vals) <- "numeric"
    storage.mode(sqi_vals) <- "numeric"

    bad <- (sqi_vals < sqi_thr)

    # bioRad uses NA for missing gates in pvol objects.
    dbz_vals[bad] <- NA_real_

    dbz[] <- dbz_vals

    sc$params[[dbz_name]] <- dbz
    pvol$scans[[i]] <- sc
  }
  pvol
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
  if (nzchar(INPUT_FILE)) {
    files <- normalizePath(INPUT_FILE, winslash = "/", mustWork = TRUE)
  } else {
    files <- list.files(date_dir, pattern = INPUT_PATTERN, recursive = TRUE, full.names = TRUE)
  }
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
  if (!skip_h5) {
    dir.create(out_dir_h5,  recursive = TRUE, showWarnings = FALSE)
  }

  cat("Date:", date_str, "\n")
  cat("Input directory:", date_dir, "\n")
  cat("Found", length(files), "file(s)\n")
  cat("Output CSV directory:", out_dir_csv, "\n")
  if (skip_h5) {
    cat("Output H5 directory : (disabled)\n\n")
  } else {
    cat("Output H5 directory :", out_dir_h5,  "\n\n")
  }
  cat("Quality filter:", DBZ_PARAM, "with", SQI_PARAM, ">=", SQI_THR, "\n\n")

  for (f in sort(files)) {
    # Build output filenames that align with input basenames.
    base <- tools::file_path_sans_ext(basename(f))
    out_csv <- file.path(out_dir_csv, paste0(base, "_vp.csv"))
    out_h5  <- file.path(out_dir_h5,  paste0(base, "_vp.h5"))

    # Skip if outputs already exist and not forcing a re-run.
    if (!force && ((skip_h5 && file.exists(out_csv)) || (!skip_h5 && file.exists(out_csv) && file.exists(out_h5)))) {
      cat("Skipping (outputs exist):", f, "\n")
      total_skip <- total_skip + 1L
      next
    }

    cat("Processing:", f, "\n")
    settings <- settings_for_file(f)
    tryCatch({
      pvol <- read_pvolfile(f, param = PARAMS_TO_READ)
      pvol_input <- apply_quality_filter(pvol, DBZ_PARAM, SQI_PARAM,
                                         SQI_THR, f)
      # Write both VPTS CSV and ODIM HDF5 outputs.
      run_vp_write(pvol_input, out_csv, settings)
      if (!skip_h5) {
        run_vp_write(pvol_input, out_h5,  settings)
      }
      cat("  OK ->", out_csv, "\n")
      if (skip_h5) {
        cat("  OK -> (H5 disabled)\n\n")
      } else {
        cat("  OK ->", out_h5,  "\n\n")
      }
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
