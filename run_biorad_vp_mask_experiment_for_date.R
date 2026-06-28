#!/usr/bin/env Rscript
# Run bioRad VP generation with experiment-only in-memory masking.
#
# This script does not modify source pvol HDF5 files. It reads pvol files,
# applies a selected mask profile in memory, writes VP outputs under an
# experiment output root, and writes per-file/scan mask diagnostics.

user_lib <- file.path(Sys.getenv("HOME", unset = "~"), "R", "library")
if (dir.exists(file.path(user_lib, "bioRad"))) {
  .libPaths(c(user_lib, .libPaths()))
}

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

suppressPackageStartupMessages(library(bioRad))

BASE_IN <- Sys.getenv(
  "RADAR_IN",
  unset = "/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/vol2birdinput"
)
BASE_OUT <- Sys.getenv(
  "RADAR_OUT",
  unset = "/gws/ssde/j25a/ncas_radar/vol2/avocet/experiments/biorad_mask_threshold_scan/runs/manual/vp"
)
DIAGNOSTICS_ROOT <- Sys.getenv(
  "DIAGNOSTICS_ROOT",
  unset = "/gws/ssde/j25a/ncas_radar/vol2/avocet/experiments/biorad_mask_threshold_scan/runs/manual/diagnostics"
)
INPUT_PATTERN <- Sys.getenv("RADAR_PATTERN", unset = "\\.h5$")
FORCE_ENV <- Sys.getenv("FORCE", unset = "0")
SKIP_H5_ENV <- Sys.getenv("SKIP_H5", unset = "0")
INPUT_DIR <- ""
INPUT_FILE <- ""
PROFILE_ID <- Sys.getenv("MASK_PROFILE_ID", unset = "")
THRESHOLD_GRID <- Sys.getenv("MASK_THRESHOLD_GRID", unset = "")
READ_PARAMS_ENV <- Sys.getenv("PARAMS_TO_READ", unset = "all")
EXCLUDE_FILES_ENV <- Sys.getenv("EXCLUDE_FILES_LIST", unset = "")

DBZ_PARAM <- Sys.getenv("DBZ_PARAM", unset = "DBZH")
SQI_CANDIDATES <- strsplit(Sys.getenv("SQI_PARAMS", unset = "SQIH,SQI,CI"), ",")[[1]]
NCP_CANDIDATES <- strsplit(Sys.getenv(
  "NCP_PARAMS",
  unset = "NCPH,NCP,NCPV,NCPW,normalized_coherent_power,normalised_coherent_power,NORMALISED_COHERENT_POWER"
), ",")[[1]]
VRAD_CANDIDATES <- strsplit(Sys.getenv("VRAD_PARAMS", unset = "VRADH,VRAD,VH"), ",")[[1]]

VP_SETTINGS <- list(
  autoconf = FALSE,
  dual_pol = TRUE,
  rho_hv = 0.97,
  dealias = TRUE,
  range_min = 5000,
  range_max = 35000,
  h_layer = 200,
  n_layer = 25,
  sd_vvp_threshold = 2,
  rcs = 11
)

is_true <- function(x) {
  tolower(as.character(x)) %in% c("1", "true", "yes", "y")
}

skip_h5 <- is_true(SKIP_H5_ENV)
force <- is_true(FORCE_ENV)
exclude_files_path <- EXCLUDE_FILES_ENV

usage <- function() {
  cat(
    "Usage: run_biorad_vp_mask_experiment_for_date.R <YYYYMMDD> ",
    "--profile-id ID --threshold-grid TSV ",
    "[--input-root PATH] [--input-dir DIR] [--input-file FILE] ",
    "[--output-root PATH] [--diagnostics-root PATH] [--pattern REGEX] ",
    "[--exclude-files PATH] [--csv-only] [--force] ",
    "[--disable-hdf5-locking] [--enable-hdf5-locking]\n",
    sep = ""
  )
}

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1 || args[1] %in% c("-h", "--help")) {
  usage()
  quit(status = 1)
}

date_str <- args[1]
if (!grepl("^[0-9]{8}$", date_str)) {
  stop("Date must be YYYYMMDD.", call. = FALSE)
}

i <- 2
while (i <= length(args)) {
  opt <- args[i]
  if (opt == "--input-root") {
    if (i == length(args)) stop("--input-root requires a value.", call. = FALSE)
    BASE_IN <- args[i + 1]
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
  if (opt == "--output-root") {
    if (i == length(args)) stop("--output-root requires a value.", call. = FALSE)
    BASE_OUT <- args[i + 1]
    i <- i + 2
    next
  }
  if (opt == "--diagnostics-root") {
    if (i == length(args)) stop("--diagnostics-root requires a value.", call. = FALSE)
    DIAGNOSTICS_ROOT <- args[i + 1]
    i <- i + 2
    next
  }
  if (opt == "--profile-id") {
    if (i == length(args)) stop("--profile-id requires a value.", call. = FALSE)
    PROFILE_ID <- args[i + 1]
    i <- i + 2
    next
  }
  if (opt == "--threshold-grid") {
    if (i == length(args)) stop("--threshold-grid requires a value.", call. = FALSE)
    THRESHOLD_GRID <- args[i + 1]
    i <- i + 2
    next
  }
  if (opt == "--pattern") {
    if (i == length(args)) stop("--pattern requires a value.", call. = FALSE)
    INPUT_PATTERN <- args[i + 1]
    i <- i + 2
    next
  }
  if (opt == "--exclude-files") {
    if (i == length(args)) stop("--exclude-files requires a value.", call. = FALSE)
    exclude_files_path <- args[i + 1]
    i <- i + 2
    next
  }
  if (opt == "--csv-only") {
    skip_h5 <- TRUE
    i <- i + 1
    next
  }
  if (opt == "--force") {
    force <- TRUE
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
  stop("Unknown option: ", opt, call. = FALSE)
}

if (!dir.exists(BASE_IN)) {
  stop("Input root does not exist: ", BASE_IN, call. = FALSE)
}
if (nzchar(INPUT_DIR) && !dir.exists(INPUT_DIR)) {
  stop("Input dir does not exist: ", INPUT_DIR, call. = FALSE)
}
if (nzchar(INPUT_FILE) && !file.exists(INPUT_FILE)) {
  stop("Input file does not exist: ", INPUT_FILE, call. = FALSE)
}
if (nzchar(INPUT_DIR) && nzchar(INPUT_FILE)) {
  stop("Use only one of --input-dir or --input-file.", call. = FALSE)
}
if (!nzchar(PROFILE_ID)) {
  stop("--profile-id or MASK_PROFILE_ID is required.", call. = FALSE)
}
if (!nzchar(THRESHOLD_GRID) || !file.exists(THRESHOLD_GRID)) {
  stop("--threshold-grid must point to an existing TSV.", call. = FALSE)
}

base_in_norm <- sub("/+$", "", normalizePath(BASE_IN, winslash = "/", mustWork = TRUE))

as_num_or <- function(x, default = NA_real_) {
  if (length(x) == 0 || is.na(x) || !nzchar(trimws(as.character(x)))) return(default)
  val <- suppressWarnings(as.numeric(x))
  if (is.na(val)) default else val
}

as_int_or <- function(x, default = 0L) {
  val <- as_num_or(x, default)
  as.integer(round(val))
}

profile_grid <- read.delim(THRESHOLD_GRID, sep = "\t", stringsAsFactors = FALSE, check.names = FALSE)
required_cols <- c("profile_id", "mask_mode")
missing_cols <- setdiff(required_cols, names(profile_grid))
if (length(missing_cols) > 0) {
  stop("Threshold grid is missing columns: ", paste(missing_cols, collapse = ", "), call. = FALSE)
}
profile_rows <- profile_grid[profile_grid$profile_id == PROFILE_ID, , drop = FALSE]
if (nrow(profile_rows) != 1) {
  stop("Expected exactly one threshold-grid row for profile_id=", PROFILE_ID,
       "; found ", nrow(profile_rows), call. = FALSE)
}
profile <- profile_rows[1, , drop = FALSE]
mask_mode <- tolower(profile$mask_mode[[1]])
valid_modes <- c("baseline", "noise_only", "clutter_only", "combined")
if (!(mask_mode %in% valid_modes)) {
  stop("mask_mode must be one of: ", paste(valid_modes, collapse = ", "), call. = FALSE)
}

num_col <- function(name, default = NA_real_) {
  if (!(name %in% names(profile))) return(default)
  as_num_or(profile[[name]][[1]], default)
}
int_col <- function(name, default = 0L) {
  if (!(name %in% names(profile))) return(default)
  as_int_or(profile[[name]][[1]], default)
}

sqi_thr <- num_col("sqi_thr", 0.20)
ncp_thr <- num_col("ncp_thr", NA_real_)
floor_quantile <- num_col("floor_quantile", NA_real_)
floor_margin_db <- num_col("floor_margin_db", NA_real_)
clutter_dbz_min <- num_col("clutter_dbz_min", NA_real_)
clutter_vrad_abs_max <- num_col("clutter_vrad_abs_max", NA_real_)
clutter_persistence_min <- num_col("clutter_persistence_min", 0)
clutter_min_gates <- int_col("clutter_min_gates", 1L)

if (tolower(READ_PARAMS_ENV) == "all") {
  PARAMS_TO_READ <- "all"
} else {
  PARAMS_TO_READ <- trimws(strsplit(READ_PARAMS_ENV, ",")[[1]])
  PARAMS_TO_READ <- PARAMS_TO_READ[nzchar(PARAMS_TO_READ)]
}

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

find_date_dir_for_file <- function(path, date_str) {
  dir <- normalizePath(dirname(path), winslash = "/", mustWork = TRUE)
  while (dir != "/" && dir != ".") {
    if (basename(dir) == date_str) return(dir)
    dir <- dirname(dir)
  }
  ""
}

load_excluded_files <- function(path) {
  if (!nzchar(path)) return(character(0))
  if (!file.exists(path)) {
    stop("Exclude file list does not exist: ", path, call. = FALSE)
  }
  df <- tryCatch(
    read.delim(path, sep = "\t", stringsAsFactors = FALSE, quote = "", comment.char = ""),
    error = function(e) NULL
  )
  vals <- character(0)
  if (!is.null(df) && ncol(df) >= 1) {
    if ("input_file" %in% names(df)) vals <- df$input_file else vals <- df[[1]]
  } else {
    vals <- readLines(path, warn = FALSE)
  }
  vals <- trimws(as.character(vals))
  vals <- vals[nzchar(vals)]
  unique(normalizePath(vals, winslash = "/", mustWork = FALSE))
}

detect_pulse <- function(path) {
  base <- basename(path)
  if (grepl("_lp_", base) || grepl("/lp/", path)) return("lp")
  if (grepl("_sp_", base) || grepl("/sp/", path)) return("sp")
  ""
}

settings_for_file <- function(path) {
  pulse <- detect_pulse(path)
  if (pulse == "lp") {
    return(c(VP_SETTINGS, list(nyquist_min = 1)))
  }
  VP_SETTINGS
}

param_values <- function(param) {
  vals <- unclass(param)
  storage.mode(vals) <- "numeric"
  vals
}

set_param_values <- function(param, vals) {
  param[] <- vals
  param
}

find_param_name <- function(sc, candidates) {
  candidates <- trimws(candidates)
  candidates <- candidates[nzchar(candidates)]
  present <- names(sc$params)
  for (nm in candidates) {
    if (nm %in% present) return(nm)
  }
  ""
}

safe_sum <- function(x) {
  if (length(x) == 0) return(0L)
  as.integer(sum(x, na.rm = TRUE))
}

quantile_by_col <- function(mat, probs) {
  apply(mat, 2, function(x) {
    x <- x[is.finite(x)]
    if (length(x) == 0) return(NA_real_)
    as.numeric(stats::quantile(x, probs = probs, names = FALSE, na.rm = TRUE))
  })
}

mask_matrix_for_cols <- function(col_flags, nrow, ncol) {
  matrix(rep(col_flags, each = nrow), nrow = nrow, ncol = ncol)
}

apply_mask_profile <- function(pvol, source_path) {
  diag_rows <- list()

  for (scan_i in seq_along(pvol$scans)) {
    sc <- pvol$scans[[scan_i]]
    if (!(DBZ_PARAM %in% names(sc$params))) {
      stop("Missing ", DBZ_PARAM, " in ", source_path, call. = FALSE)
    }

    dbz <- sc$params[[DBZ_PARAM]]
    dbz_vals <- param_values(dbz)
    dims <- dim(dbz_vals)
    if (length(dims) != 2) {
      stop(DBZ_PARAM, " is not a 2D parameter in ", source_path, call. = FALSE)
    }

    sqi_name <- find_param_name(sc, SQI_CANDIDATES)
    ncp_name <- find_param_name(sc, NCP_CANDIDATES)
    vrad_name <- find_param_name(sc, VRAD_CANDIDATES)

    sqi_bad <- matrix(FALSE, nrow = dims[1], ncol = dims[2])
    ncp_bad <- matrix(FALSE, nrow = dims[1], ncol = dims[2])
    floor_bad <- matrix(FALSE, nrow = dims[1], ncol = dims[2])
    clutter_bad <- matrix(FALSE, nrow = dims[1], ncol = dims[2])

    if (nzchar(sqi_name) && is.finite(sqi_thr)) {
      sqi_vals <- param_values(sc$params[[sqi_name]])
      if (identical(dim(sqi_vals), dims)) {
        sqi_bad <- is.finite(sqi_vals) & sqi_vals < sqi_thr
      }
    }

    if (nzchar(ncp_name) && is.finite(ncp_thr)) {
      ncp_vals <- param_values(sc$params[[ncp_name]])
      if (identical(dim(ncp_vals), dims)) {
        ncp_bad <- is.finite(ncp_vals) & ncp_vals < ncp_thr
      }
    }

    if (is.finite(floor_quantile) && is.finite(floor_margin_db)) {
      floor_profile <- quantile_by_col(dbz_vals, floor_quantile)
      floor_threshold <- floor_profile + floor_margin_db
      floor_bad <- sweep(dbz_vals, 2, floor_threshold, FUN = "<=")
      floor_bad[!is.finite(dbz_vals)] <- FALSE
      floor_bad[, !is.finite(floor_threshold)] <- FALSE
    }

    if (nzchar(vrad_name) &&
        is.finite(clutter_dbz_min) &&
        is.finite(clutter_vrad_abs_max)) {
      vrad_vals <- param_values(sc$params[[vrad_name]])
      if (identical(dim(vrad_vals), dims)) {
        raw_clutter <- is.finite(dbz_vals) &
          is.finite(vrad_vals) &
          dbz_vals >= clutter_dbz_min &
          abs(vrad_vals) <= clutter_vrad_abs_max
        if (is.finite(clutter_persistence_min) && clutter_persistence_min > 0) {
          persistent_cols <- colMeans(raw_clutter, na.rm = TRUE) >= clutter_persistence_min &
            colSums(raw_clutter, na.rm = TRUE) >= clutter_min_gates
          clutter_bad <- raw_clutter & mask_matrix_for_cols(persistent_cols, dims[1], dims[2])
        } else {
          clutter_bad <- raw_clutter
        }
      }
    }

    noise_bad <- sqi_bad | ncp_bad | floor_bad
    if (mask_mode == "baseline") {
      combined_bad <- sqi_bad
      if (any(combined_bad, na.rm = TRUE)) {
        dbz_vals[combined_bad] <- NA_real_
        sc$params[[DBZ_PARAM]] <- set_param_values(dbz, dbz_vals)
      }
      masked_param_count <- ifelse(any(combined_bad, na.rm = TRUE), 1L, 0L)
    } else {
      if (mask_mode == "noise_only") {
        combined_bad <- noise_bad
      } else if (mask_mode == "clutter_only") {
        combined_bad <- clutter_bad
      } else {
        combined_bad <- noise_bad | clutter_bad
      }

      masked_param_count <- 0L
      if (any(combined_bad, na.rm = TRUE)) {
        for (param_name in names(sc$params)) {
          param <- sc$params[[param_name]]
          vals <- tryCatch(param_values(param), error = function(e) NULL)
          if (is.null(vals) || !identical(dim(vals), dims)) next
          vals[combined_bad] <- NA_real_
          sc$params[[param_name]] <- set_param_values(param, vals)
          masked_param_count <- masked_param_count + 1L
        }
      }
    }

    diag_rows[[length(diag_rows) + 1L]] <- data.frame(
      input_file = source_path,
      scan_index = scan_i,
      profile_id = PROFILE_ID,
      mask_mode = mask_mode,
      dbz_param = DBZ_PARAM,
      sqi_param = sqi_name,
      ncp_param = ncp_name,
      vrad_param = vrad_name,
      gates_total = as.integer(prod(dims)),
      sqi_bad = safe_sum(sqi_bad),
      ncp_bad = safe_sum(ncp_bad),
      floor_bad = safe_sum(floor_bad),
      clutter_bad = safe_sum(clutter_bad),
      combined_bad = safe_sum(combined_bad),
      masked_param_count = as.integer(masked_param_count),
      stringsAsFactors = FALSE
    )

    pvol$scans[[scan_i]] <- sc
  }

  list(pvol = pvol, diagnostics = do.call(rbind, diag_rows))
}

append_diag <- function(diag_file, rows) {
  dir.create(dirname(diag_file), recursive = TRUE, showWarnings = FALSE)
  write.table(
    rows,
    file = diag_file,
    sep = "\t",
    quote = FALSE,
    row.names = FALSE,
    col.names = !file.exists(diag_file),
    append = file.exists(diag_file)
  )
}

run_vp_write <- function(pvol, vpfile, settings) {
  do.call(calculate_vp, c(list(file = pvol, vpfile = vpfile), settings))
}

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

excluded_files <- load_excluded_files(exclude_files_path)
excluded_set <- NULL
if (length(excluded_files) > 0) {
  excluded_set <- setNames(rep(TRUE, length(excluded_files)), excluded_files)
  cat("Loaded excluded files:", length(excluded_files), "\n")
}

total_ok <- 0L
total_fail <- 0L
total_skip <- 0L
total_excluded <- 0L

for (date_dir in sort(date_dirs)) {
  if (nzchar(INPUT_FILE)) {
    files <- normalizePath(INPUT_FILE, winslash = "/", mustWork = TRUE)
  } else {
    files <- list.files(date_dir, pattern = INPUT_PATTERN, recursive = TRUE, full.names = TRUE)
  }
  if (length(files) == 0) {
    cat("No input files in:", date_dir, "\n")
    next
  }

  rel_day <- relative_to(date_dir, base_in_norm)
  out_day <- file.path(BASE_OUT, rel_day)
  out_dir_csv <- file.path(out_day, "vpts_csv")
  out_dir_h5 <- file.path(out_day, "vp_h5")
  diag_file <- file.path(DIAGNOSTICS_ROOT, rel_day, paste0(PROFILE_ID, "_mask_diagnostics.tsv"))
  dir.create(out_dir_csv, recursive = TRUE, showWarnings = FALSE)
  if (!skip_h5) dir.create(out_dir_h5, recursive = TRUE, showWarnings = FALSE)

  cat("Date:", date_str, "\n")
  cat("Input directory:", date_dir, "\n")
  cat("Found", length(files), "file(s)\n")
  cat("Profile:", PROFILE_ID, " mode:", mask_mode, "\n")
  cat("Output CSV directory:", out_dir_csv, "\n")
  cat("Diagnostics:", diag_file, "\n")
  if (skip_h5) cat("Output H5 directory : (disabled)\n\n") else cat("Output H5 directory :", out_dir_h5, "\n\n")

  for (f in sort(files)) {
    f_norm <- normalizePath(f, winslash = "/", mustWork = FALSE)
    if (!is.null(excluded_set) && isTRUE(excluded_set[[f_norm]])) {
      cat("Skipping (excluded):", f, "\n")
      total_skip <- total_skip + 1L
      total_excluded <- total_excluded + 1L
      next
    }

    base <- tools::file_path_sans_ext(basename(f))
    out_csv <- file.path(out_dir_csv, paste0(base, "_vp.csv"))
    out_h5 <- file.path(out_dir_h5, paste0(base, "_vp.h5"))

    if (!force && ((skip_h5 && file.exists(out_csv)) || (!skip_h5 && file.exists(out_csv) && file.exists(out_h5)))) {
      cat("Skipping (outputs exist):", f, "\n")
      total_skip <- total_skip + 1L
      next
    }

    cat("Processing:", f, "\n")
    settings <- settings_for_file(f)
    tryCatch({
      pvol <- read_pvolfile(f, param = PARAMS_TO_READ)
      masked <- apply_mask_profile(pvol, f)
      run_vp_write(masked$pvol, out_csv, settings)
      if (!skip_h5) run_vp_write(masked$pvol, out_h5, settings)
      append_diag(diag_file, masked$diagnostics)
      cat("  OK ->", out_csv, "\n")
      if (skip_h5) cat("  OK -> (H5 disabled)\n\n") else cat("  OK ->", out_h5, "\n\n")
      total_ok <- total_ok + 1L
    }, error = function(e) {
      cat("  FAILED:", conditionMessage(e), "\n\n")
      total_fail <<- total_fail + 1L
    })
  }
}

cat("Excluded:", total_excluded, "\n")
cat("Done. Success:", total_ok, " Skipped:", total_skip, " Failed:", total_fail, "\n")
if (total_fail > 0) quit(status = 2)
