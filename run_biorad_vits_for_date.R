#!/usr/bin/env Rscript
# Build daily VITS outputs from daily VPTS CSV files.
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
  unset = "/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/biorad_vpts"
)
BASE_OUT <- Sys.getenv(
  "RADAR_OUT",
  unset = "/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/biorad_vits"
)
FORCE_ENV <- Sys.getenv("FORCE", unset = "0")
ALT_MIN_ENV <- Sys.getenv("ALT_MIN", unset = "200")
ALT_MAX_ENV <- Sys.getenv("ALT_MAX", unset = "4000")

INPUT_DIR <- ""
force <- FALSE
alt_min <- suppressWarnings(as.numeric(ALT_MIN_ENV))
alt_max <- suppressWarnings(as.numeric(ALT_MAX_ENV))

is_true <- function(x) {
  x <- tolower(as.character(x))
  x %in% c("1", "true", "yes", "y")
}

usage <- function() {
  cat(
    "Usage: run_biorad_vits_for_date.R <YYYYMMDD> ",
    "[--input-root PATH] [--output-root PATH] [--input-dir DIR] ",
    "[--alt-min M] [--alt-max M] [--force]\n",
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
  if (opt == "--alt-min") {
    if (i == length(args)) stop("--alt-min requires a value.", call. = FALSE)
    alt_min <- as.numeric(args[i + 1])
    i <- i + 2
    next
  }
  if (opt == "--alt-max") {
    if (i == length(args)) stop("--alt-max requires a value.", call. = FALSE)
    alt_max <- as.numeric(args[i + 1])
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
if (!is.finite(alt_min) || !is.finite(alt_max) || alt_min >= alt_max) {
  stop("Invalid altitude bounds. Require finite --alt-min < --alt-max.", call. = FALSE)
}

base_in_norm <- sub("/+$", "", normalizePath(BASE_IN, winslash = "/", mustWork = TRUE))

find_input_dirs <- function(root, date_str) {
  find_bin <- Sys.which("find")
  pattern <- paste0("^", date_str, "_(lp|sp)_vpts\\.csv$")
  if (nzchar(find_bin)) {
    res <- suppressWarnings(system2(
      find_bin,
      c(root, "-type", "f", "-name", paste0(date_str, "_*_vpts.csv")),
      stdout = TRUE, stderr = NULL
    ))
    res <- res[nzchar(res)]
    if (length(res) > 0) {
      bases <- basename(res)
      keep <- grepl(pattern, bases)
      return(sort(unique(dirname(res[keep]))))
    }
  }
  files <- list.files(root, pattern = paste0("^", date_str, "_.*_vpts\\.csv$"),
                      recursive = TRUE, full.names = TRUE)
  if (length(files) == 0) return(character(0))
  files <- files[grepl(pattern, basename(files))]
  sort(unique(dirname(files)))
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

if (nzchar(INPUT_DIR)) {
  input_dir_norm <- normalizePath(INPUT_DIR, winslash = "/", mustWork = TRUE)
  if (!dir.exists(input_dir_norm)) {
    stop("Input dir does not exist: ", input_dir_norm, call. = FALSE)
  }
  input_dirs <- input_dir_norm
} else {
  input_dirs <- find_input_dirs(BASE_IN, date_str)
}

if (length(input_dirs) == 0) {
  stop("No input directories with VPTS files found for date: ", date_str, call. = FALSE)
}

total_ok <- 0L
total_skip <- 0L
total_fail <- 0L

for (in_dir in sort(input_dirs)) {
  in_dir_norm <- normalizePath(in_dir, winslash = "/", mustWork = TRUE)
  lp_in <- file.path(in_dir_norm, paste0(date_str, "_lp_vpts.csv"))
  sp_in <- file.path(in_dir_norm, paste0(date_str, "_sp_vpts.csv"))

  rel_parent <- relative_to(in_dir_norm, base_in_norm)
  out_parent <- file.path(BASE_OUT, rel_parent)
  dir.create(out_parent, recursive = TRUE, showWarnings = FALSE)

  lp_out <- file.path(out_parent, paste0(date_str, "_lp_vits.csv"))
  sp_out <- file.path(out_parent, paste0(date_str, "_sp_vits.csv"))

  cat("Date:", date_str, "\n")
  cat("Input directory:", in_dir_norm, "\n")
  cat("Output parent:", out_parent, "\n")
  cat("Altitude integration:", alt_min, "to", alt_max, "m\n")

  write_pulse <- function(pulse, in_file, out_file) {
    if (!file.exists(in_file)) {
      cat(sprintf("  %s: input missing, skip.\n", pulse))
      return("skip")
    }
    if (!force && file.exists(out_file)) {
      cat(sprintf("  %s: output exists, skip -> %s\n", pulse, out_file))
      return("skip")
    }
    tryCatch({
      v <- read_vpts(in_file)
      vits <- integrate_profile(v, alt_min = alt_min, alt_max = alt_max)
      df <- as.data.frame(vits)
      write.csv(df, out_file, row.names = FALSE)
      cat(sprintf("  %s: wrote -> %s\n", pulse, out_file))
      "ok"
    }, error = function(e) {
      cat(sprintf("  %s: FAILED: %s\n", pulse, conditionMessage(e)))
      "fail"
    })
  }

  lp_status <- write_pulse("lp", lp_in, lp_out)
  sp_status <- write_pulse("sp", sp_in, sp_out)

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
