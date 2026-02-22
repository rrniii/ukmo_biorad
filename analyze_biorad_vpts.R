#!/usr/bin/env Rscript

# Analyze bioRad VP CSV outputs for one radar/year.
# Outputs:
# - time-height PNG
# - VPI CSV (integrated migration metrics)
# - monthly MTR CSV

usage <- function() {
  cat(
    "Usage: analyze_biorad_vpts.R --radar NAME --year YYYY [options]\n",
    "\n",
    "Required:\n",
    "  --radar NAME        Radar name (e.g. chenies)\n",
    "  --year YYYY         Year (e.g. 2025)\n",
    "\n",
    "Options:\n",
    "  --vp-root DIR       Root containing radar/year VP outputs\n",
    "                      default: /gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/biorad_vp/single-site\n",
    "  --out-dir DIR       Output directory for analysis products\n",
    "                      default: ./analysis_<radar>_<year>\n",
    "  --alt-min M         Integration minimum altitude in meters (default: 200)\n",
    "  --alt-max M         Integration maximum altitude in meters (default: 4000)\n",
    "  --pulse MODE        Which pulse files to include: both|lp|sp\n",
    "                      default: both\n",
    "  --all-times         Use all profiles (default is night-only)\n",
    "  --chunk-size N      Number of VP CSV files to read per chunk\n",
    "                      default: 100\n",
    "  -h, --help          Show help\n",
    sep = ""
  )
}

parse_args <- function(args) {
  cfg <- list(
    radar = "",
    year = "",
    vp_root = "/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/biorad_vp/single-site",
    out_dir = "",
    alt_min = 200,
    alt_max = 4000,
    night_only = TRUE,
    chunk_size = 100,
    pulse = "both"
  )

  i <- 1
  while (i <= length(args)) {
    opt <- args[[i]]
    if (opt %in% c("-h", "--help")) {
      usage()
      quit(status = 0)
    } else if (opt == "--radar") {
      if (i == length(args)) stop("--radar requires a value.", call. = FALSE)
      cfg$radar <- args[[i + 1]]
      i <- i + 2
      next
    } else if (opt == "--year") {
      if (i == length(args)) stop("--year requires a value.", call. = FALSE)
      cfg$year <- args[[i + 1]]
      i <- i + 2
      next
    } else if (opt == "--vp-root") {
      if (i == length(args)) stop("--vp-root requires a value.", call. = FALSE)
      cfg$vp_root <- args[[i + 1]]
      i <- i + 2
      next
    } else if (opt == "--out-dir") {
      if (i == length(args)) stop("--out-dir requires a value.", call. = FALSE)
      cfg$out_dir <- args[[i + 1]]
      i <- i + 2
      next
    } else if (opt == "--alt-min") {
      if (i == length(args)) stop("--alt-min requires a value.", call. = FALSE)
      cfg$alt_min <- as.numeric(args[[i + 1]])
      i <- i + 2
      next
    } else if (opt == "--alt-max") {
      if (i == length(args)) stop("--alt-max requires a value.", call. = FALSE)
      cfg$alt_max <- as.numeric(args[[i + 1]])
      i <- i + 2
      next
    } else if (opt == "--all-times") {
      cfg$night_only <- FALSE
      i <- i + 1
      next
    } else if (opt == "--pulse") {
      if (i == length(args)) stop("--pulse requires a value.", call. = FALSE)
      cfg$pulse <- tolower(args[[i + 1]])
      i <- i + 2
      next
    } else if (opt == "--chunk-size") {
      if (i == length(args)) stop("--chunk-size requires a value.", call. = FALSE)
      cfg$chunk_size <- as.integer(args[[i + 1]])
      i <- i + 2
      next
    } else {
      stop("Unknown option: ", opt, call. = FALSE)
    }
  }

  if (cfg$radar == "") stop("Missing required --radar.", call. = FALSE)
  if (cfg$year == "") stop("Missing required --year.", call. = FALSE)
  if (!grepl("^[0-9]{4}$", cfg$year)) stop("--year must be YYYY.", call. = FALSE)
  if (!is.finite(cfg$alt_min) || !is.finite(cfg$alt_max) || cfg$alt_min >= cfg$alt_max) {
    stop("Invalid altitude bounds. Require finite --alt-min < --alt-max.", call. = FALSE)
  }
  if (is.na(cfg$chunk_size) || cfg$chunk_size < 1) {
    stop("--chunk-size must be a positive integer.", call. = FALSE)
  }
  if (!(cfg$pulse %in% c("both", "lp", "sp"))) {
    stop("--pulse must be one of: both, lp, sp.", call. = FALSE)
  }
  if (cfg$out_dir == "") {
    cfg$out_dir <- sprintf("analysis_%s_%s", cfg$radar, cfg$year)
  }

  cfg
}

# Prefer user library if available.
user_lib <- file.path(Sys.getenv("HOME", unset = "~"), "R", "library")
if (dir.exists(file.path(user_lib, "bioRad"))) {
  .libPaths(c(user_lib, .libPaths()))
}

suppressPackageStartupMessages(library(bioRad))

cfg <- parse_args(commandArgs(trailingOnly = TRUE))

input_dir <- file.path(cfg$vp_root, cfg$radar, cfg$year)
if (!dir.exists(input_dir)) {
  stop("Input directory does not exist: ", input_dir, call. = FALSE)
}

files <- list.files(
  input_dir,
  pattern = "_vp\\.csv$",
  recursive = TRUE,
  full.names = TRUE
)

if (cfg$pulse == "lp") {
  files <- files[grepl("_lp_.*_vp\\.csv$", basename(files))]
} else if (cfg$pulse == "sp") {
  files <- files[grepl("_sp_.*_vp\\.csv$", basename(files))]
}

if (length(files) == 0) {
  stop("No VP CSV files found under: ", input_dir, " for pulse=", cfg$pulse, call. = FALSE)
}

dir.create(cfg$out_dir, recursive = TRUE, showWarnings = FALSE)

cat("Radar:", cfg$radar, "\n")
cat("Year:", cfg$year, "\n")
cat("Input directory:", input_dir, "\n")
cat("CSV files:", length(files), "\n")
cat("Night-only:", cfg$night_only, "\n")
cat("Altitude integration:", cfg$alt_min, "to", cfg$alt_max, "m\n")
cat("Pulse mode:", cfg$pulse, "\n")
cat("Chunk size:", cfg$chunk_size, "files\n")
cat("Output directory:", cfg$out_dir, "\n\n")

# Avoid "Too many open files" by reading VP CSVs in bounded chunks.
read_vpts_chunked <- function(file_list, chunk_size) {
  total_files <- length(file_list)
  if (total_files <= chunk_size) {
    return(read_vpts(file_list))
  }

  chunk_ids <- ceiling(seq_along(file_list) / chunk_size)
  chunks <- split(file_list, chunk_ids)
  chunk_dfs <- vector("list", length(chunks))

  for (i in seq_along(chunks)) {
    cat(sprintf("Reading chunk %d/%d (%d files)\n", i, length(chunks), length(chunks[[i]])))
    chunk_dfs[[i]] <- read_vpts(chunks[[i]], data_frame = TRUE)
  }

  all_df <- do.call(rbind, chunk_dfs)
  as.vpts(all_df)
}

vpts <- read_vpts_chunked(files, cfg$chunk_size)
vpts <- regularize_vpts(vpts)
if (cfg$night_only) {
  vpts <- filter_vpts(vpts, night = TRUE)
}

plot_path <- file.path(cfg$out_dir, sprintf("%s_%s_timeheight.png", cfg$radar, cfg$year))
png(plot_path, width = 1600, height = 900)
plot(vpts)
dev.off()

vpi <- integrate_profile(vpts, alt_min = cfg$alt_min, alt_max = cfg$alt_max)
vpi_path <- file.path(cfg$out_dir, sprintf("%s_%s_vpi.csv", cfg$radar, cfg$year))
write.csv(vpi, vpi_path, row.names = FALSE)

monthly <- aggregate(
  mtr ~ month,
  data = transform(vpi, month = format(datetime, "%Y-%m")),
  FUN = sum,
  na.rm = TRUE
)
monthly_path <- file.path(cfg$out_dir, sprintf("%s_%s_monthly_mtr.csv", cfg$radar, cfg$year))
write.csv(monthly, monthly_path, row.names = FALSE)

cat("Wrote:\n")
cat(" -", plot_path, "\n")
cat(" -", vpi_path, "\n")
cat(" -", monthly_path, "\n")
