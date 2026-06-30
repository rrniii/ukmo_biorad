#!/usr/bin/env Rscript
# Build daily VPTS time series outputs from biorad_vp per-file VP CSVs.
# Writes one CSV and optionally one HDF5 file per day and pulse type (lp/sp),
# under a radar/year tree mirroring raw_h5_data_final organization.

# Auto-add user library if bioRad is installed there.
user_lib <- file.path(Sys.getenv("HOME", unset = "~"), "R", "library")
if (dir.exists(file.path(user_lib, "bioRad"))) {
  .libPaths(c(user_lib, .libPaths()))
}

suppressPackageStartupMessages(library(bioRad))

BASE_IN <- Sys.getenv(
  "RADAR_IN",
  unset = "/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/biorad_vp"
)
BASE_OUT <- Sys.getenv(
  "RADAR_OUT",
  unset = "/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/biorad_vpts"
)
FORCE_ENV <- Sys.getenv("FORCE", unset = "0")
CHUNK_SIZE_ENV <- Sys.getenv("CHUNK_SIZE", unset = "100")
WRITE_H5_ENV <- Sys.getenv("WRITE_H5", unset = "1")

INPUT_DIR <- ""
force <- FALSE
write_h5 <- TRUE

is_true <- function(x) {
  x <- tolower(as.character(x))
  x %in% c("1", "true", "yes", "y")
}

usage <- function() {
  cat(
    "Usage: run_biorad_vpts_for_date.R <YYYYMMDD> ",
    "[--input-root PATH] [--output-root PATH] [--input-dir DIR] ",
    "[--chunk-size N] [--force] [--write-h5] [--csv-only]\n",
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
  stop("Date must be YYYYMMDD (e.g., 20250505).", call. = FALSE)
}

force <- is_true(FORCE_ENV)
write_h5 <- is_true(WRITE_H5_ENV)
chunk_size <- suppressWarnings(as.integer(CHUNK_SIZE_ENV))
if (is.na(chunk_size) || chunk_size < 1) chunk_size <- 100L

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
  if (opt == "--input-dir") {
    if (i == length(args)) stop("--input-dir requires a value.", call. = FALSE)
    INPUT_DIR <- args[i + 1]
    i <- i + 2
    next
  }
  if (opt == "--chunk-size") {
    if (i == length(args)) stop("--chunk-size requires a value.", call. = FALSE)
    chunk_size <- as.integer(args[i + 1])
    i <- i + 2
    next
  }
  if (opt == "--force") {
    force <- TRUE
    i <- i + 1
    next
  }
  if (opt == "--write-h5") {
    write_h5 <- TRUE
    i <- i + 1
    next
  }
  if (opt == "--csv-only") {
    write_h5 <- FALSE
    i <- i + 1
    next
  }
  stop("Unknown option: ", opt, call. = FALSE)
}

if (!dir.exists(BASE_IN)) {
  stop("Input root does not exist: ", BASE_IN, call. = FALSE)
}
if (is.na(chunk_size) || chunk_size < 1) {
  stop("--chunk-size must be a positive integer.", call. = FALSE)
}

base_in_norm <- sub("/+$", "", normalizePath(BASE_IN, winslash = "/", mustWork = TRUE))

find_date_dirs <- function(root, date_str) {
  find_bin <- Sys.which("find")
  if (nzchar(find_bin)) {
    res <- suppressWarnings(system2(
      find_bin, c(root, "-type", "d", "-name", date_str),
      stdout = TRUE, stderr = NULL
    ))
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

infer_csv_dir <- function(day_dir) {
  d1 <- file.path(day_dir, "vpts_csv")
  if (dir.exists(d1)) return(d1)
  day_dir
}

read_vpts_chunked <- function(files, chunk_size, pulse_label) {
  files <- sort(files)
  if (length(files) == 0) return(NULL)
  if (length(files) <= chunk_size) {
    return(read_vpts(files))
  }

  chunks <- split(files, ceiling(seq_along(files) / chunk_size))
  chunk_dfs <- vector("list", length(chunks))
  for (i in seq_along(chunks)) {
    cat(sprintf(
      "  %s: reading chunk %d/%d (%d files)\n",
      pulse_label, i, length(chunks), length(chunks[[i]])
    ))
    chunk_dfs[[i]] <- read_vpts(chunks[[i]], data_frame = TRUE)
  }
  all_df <- do.call(rbind, chunk_dfs)
  as.vpts(all_df)
}

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0 || is.na(x[1]) || !nzchar(x[1])) y else x[1]
}

write_vpts_h5 <- function(df, out_file, metadata) {
  if (!requireNamespace("rhdf5", quietly = TRUE)) {
    stop("R package rhdf5 is required for VPTS HDF5 output.", call. = FALSE)
  }

  dir.create(dirname(out_file), recursive = TRUE, showWarnings = FALSE)
  tmp_file <- paste0(out_file, ".tmp")
  if (file.exists(tmp_file)) unlink(tmp_file)
  if (file.exists(out_file)) unlink(out_file)
  rhdf5::h5createFile(tmp_file)
  rhdf5::h5createGroup(tmp_file, "data")
  rhdf5::h5write(names(df), tmp_file, "column_names")

  for (col_name in names(df)) {
    values <- df[[col_name]]
    dataset <- paste0("data/", col_name)
    if (inherits(values, "POSIXt")) {
      rhdf5::h5write(as.numeric(values), tmp_file, dataset)
      rhdf5::h5writeAttribute("POSIXct", tmp_file, "r_class", dataset)
      rhdf5::h5writeAttribute(attr(values, "tzone") %||% "UTC", tmp_file, "timezone", dataset)
    } else if (inherits(values, "Date")) {
      rhdf5::h5write(as.numeric(values), tmp_file, dataset)
      rhdf5::h5writeAttribute("Date", tmp_file, "r_class", dataset)
    } else if (is.factor(values)) {
      rhdf5::h5write(as.character(values), tmp_file, dataset)
      rhdf5::h5writeAttribute("factor", tmp_file, "r_class", dataset)
    } else if (is.logical(values)) {
      rhdf5::h5write(as.integer(values), tmp_file, dataset)
      rhdf5::h5writeAttribute("logical", tmp_file, "r_class", dataset)
    } else {
      rhdf5::h5write(values, tmp_file, dataset)
      rhdf5::h5writeAttribute(class(values)[1], tmp_file, "r_class", dataset)
    }
  }

  for (name in names(metadata)) {
    rhdf5::h5writeAttribute(as.character(metadata[[name]]), tmp_file, name, "/")
  }
  rhdf5::H5close()
  if (!file.rename(tmp_file, out_file)) {
    stop("Failed to move temporary HDF5 output into place: ", out_file, call. = FALSE)
  }
}

if (nzchar(INPUT_DIR)) {
  input_dir_norm <- normalizePath(INPUT_DIR, winslash = "/", mustWork = TRUE)
  if (basename(input_dir_norm) != date_str) {
    stop("Input dir basename must match date: ",
         basename(input_dir_norm), " != ", date_str, call. = FALSE)
  }
  date_dirs <- input_dir_norm
} else {
  date_dirs <- find_date_dirs(BASE_IN, date_str)
}

if (length(date_dirs) == 0) {
  stop("No date directories found under input root for date: ", date_str, call. = FALSE)
}

total_ok <- 0L
total_skip <- 0L
total_fail <- 0L

for (day_dir in sort(date_dirs)) {
  day_dir_norm <- normalizePath(day_dir, winslash = "/", mustWork = TRUE)
  csv_dir <- infer_csv_dir(day_dir_norm)
  files_all <- list.files(csv_dir, pattern = "_vp\\.csv$", recursive = TRUE, full.names = TRUE)
  if (length(files_all) == 0) {
    cat("No VP CSV files found in:", csv_dir, "\n")
    total_skip <- total_skip + 1L
    next
  }

  rel_day <- relative_to(day_dir_norm, base_in_norm)
  rel_parent <- dirname(rel_day)
  out_parent <- file.path(BASE_OUT, rel_parent)
  dir.create(out_parent, recursive = TRUE, showWarnings = FALSE)

  lp_files <- files_all[grepl("_lp_.*_vp\\.csv$", basename(files_all))]
  sp_files <- files_all[grepl("_sp_.*_vp\\.csv$", basename(files_all))]

  out_lp <- file.path(out_parent, paste0(date_str, "_lp_vpts.csv"))
  out_sp <- file.path(out_parent, paste0(date_str, "_sp_vpts.csv"))
  out_lp_h5 <- file.path(out_parent, paste0(date_str, "_lp_vpts.h5"))
  out_sp_h5 <- file.path(out_parent, paste0(date_str, "_sp_vpts.h5"))

  cat("Date:", date_str, "\n")
  cat("Input day directory:", day_dir_norm, "\n")
  cat("Output parent:", out_parent, "\n")
  cat("  lp files:", length(lp_files), "\n")
  cat("  sp files:", length(sp_files), "\n")
  cat("  H5 output:", if (write_h5) "enabled" else "disabled", "\n")

  if (length(lp_files) == 0 && length(sp_files) == 0) {
    cat("No lp/sp files found for day, skipping.\n\n")
    total_skip <- total_skip + 1L
    next
  }

  # Pulse writer wrapper to keep failure accounting per pulse.
  write_pulse <- function(pulse, pulse_files, out_csv, out_h5) {
    if (length(pulse_files) == 0) {
      cat(sprintf("  %s: no files, skip.\n", pulse))
      return("skip")
    }
    csv_exists <- file.exists(out_csv)
    h5_exists <- file.exists(out_h5)
    if (!force && csv_exists && (!write_h5 || h5_exists)) {
      cat(sprintf("  %s: outputs exist, skip -> %s\n", pulse, out_csv))
      return("skip")
    }
    tryCatch({
      v <- read_vpts_chunked(pulse_files, chunk_size, pulse)
      df <- as.data.frame(v)
      df <- df[order(df$datetime, df$height), , drop = FALSE]
      if (force || !csv_exists) {
        write.csv(df, out_csv, row.names = FALSE)
        cat(sprintf("  %s: wrote -> %s\n", pulse, out_csv))
      } else {
        cat(sprintf("  %s: CSV exists -> %s\n", pulse, out_csv))
      }
      if (write_h5 && (force || !h5_exists)) {
        write_vpts_h5(
          df,
          out_h5,
          list(
            product = "vpts",
            pulse = pulse,
            date = date_str,
            source = "BioDAR masked VP CSV",
            created_utc = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
          )
        )
        cat(sprintf("  %s: wrote -> %s\n", pulse, out_h5))
      } else if (write_h5) {
        cat(sprintf("  %s: H5 exists -> %s\n", pulse, out_h5))
      }
      "ok"
    }, error = function(e) {
      cat(sprintf("  %s: FAILED: %s\n", pulse, conditionMessage(e)))
      "fail"
    })
  }

  lp_status <- write_pulse("lp", lp_files, out_lp, out_lp_h5)
  sp_status <- write_pulse("sp", sp_files, out_sp, out_sp_h5)

  if (lp_status == "fail" || sp_status == "fail") {
    total_fail <- total_fail + 1L
  } else if (lp_status == "skip" && sp_status == "skip") {
    total_skip <- total_skip + 1L
  } else {
    total_ok <- total_ok + 1L
  }
  cat("\n")
}

cat("Done. Success:", total_ok, " Skipped:", total_skip, " Failed:", total_fail, "\n")
if (total_fail > 0) quit(status = 2)
