#!/usr/bin/env Rscript
# Build daily VPTS time series outputs from biorad_vp per-file VP CSVs.
# Writes one CSV per day and pulse type (lp/sp), under a radar/year tree
# mirroring raw_h5_data_final organization.

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

INPUT_DIR <- ""
force <- FALSE

is_true <- function(x) {
  x <- tolower(as.character(x))
  x %in% c("1", "true", "yes", "y")
}

usage <- function() {
  cat(
    "Usage: run_biorad_vpts_for_date.R <YYYYMMDD> ",
    "[--input-root PATH] [--output-root PATH] [--input-dir DIR] ",
    "[--chunk-size N] [--force]\n",
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

  cat("Date:", date_str, "\n")
  cat("Input day directory:", day_dir_norm, "\n")
  cat("Output parent:", out_parent, "\n")
  cat("  lp files:", length(lp_files), "\n")
  cat("  sp files:", length(sp_files), "\n")

  if (length(lp_files) == 0 && length(sp_files) == 0) {
    cat("No lp/sp files found for day, skipping.\n\n")
    total_skip <- total_skip + 1L
    next
  }

  # Pulse writer wrapper to keep failure accounting per pulse.
  write_pulse <- function(pulse, pulse_files, out_file) {
    if (length(pulse_files) == 0) {
      cat(sprintf("  %s: no files, skip.\n", pulse))
      return("skip")
    }
    if (!force && file.exists(out_file)) {
      cat(sprintf("  %s: output exists, skip -> %s\n", pulse, out_file))
      return("skip")
    }
    tryCatch({
      v <- read_vpts_chunked(pulse_files, chunk_size, pulse)
      df <- as.data.frame(v)
      df <- df[order(df$datetime, df$height), , drop = FALSE]
      write.csv(df, out_file, row.names = FALSE)
      cat(sprintf("  %s: wrote -> %s\n", pulse, out_file))
      "ok"
    }, error = function(e) {
      cat(sprintf("  %s: FAILED: %s\n", pulse, conditionMessage(e)))
      "fail"
    })
  }

  lp_status <- write_pulse("lp", lp_files, out_lp)
  sp_status <- write_pulse("sp", sp_files, out_sp)

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
