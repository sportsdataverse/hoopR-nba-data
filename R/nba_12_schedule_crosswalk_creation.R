rm(list = ls())
gcol <- gc()

suppressPackageStartupMessages(suppressMessages(library(dplyr)))
suppressPackageStartupMessages(suppressMessages(library(magrittr)))
suppressPackageStartupMessages(suppressMessages(library(jsonlite)))
suppressPackageStartupMessages(suppressMessages(library(purrr)))
suppressPackageStartupMessages(suppressMessages(library(progressr)))
suppressPackageStartupMessages(suppressMessages(library(data.table)))
suppressPackageStartupMessages(suppressMessages(library(arrow)))
suppressPackageStartupMessages(suppressMessages(library(glue)))
suppressPackageStartupMessages(suppressMessages(library(optparse)))
suppressPackageStartupMessages(suppressMessages(library(tibble)))
suppressPackageStartupMessages(suppressMessages(library(rlang)))

option_list <- list(
  make_option(
    c("-s", "--start_year"),
    action = "store",
    default = hoopR:::most_recent_nba_season(),
    type = "integer",
    help = "Start year of the seasons to process"
  ),
  make_option(
    c("-e", "--end_year"),
    action = "store",
    default = hoopR:::most_recent_nba_season(),
    type = "integer",
    help = "End year of the seasons to process"
  )
)
opt <- parse_args(OptionParser(option_list = option_list))
options(stringsAsFactors = FALSE)
options(scipen = 999)
years_vec <- opt$s:opt$e

# --- main loop -------------------------------------------------------------

build_season_schedule_crosswalk <- function(y) {
  cli::cli_progress_step(
    msg = "Compiling {y} NBA schedule crosswalk",
    msg_done = "Compiled {y} NBA schedule crosswalk!"
  )

  xwalk <- tryCatch(
    hoopR::nba_schedule_crosswalk(season = y),
    error = function(e) {
      cli::cli_alert_warning(
        "{Sys.time()}: skip schedule crosswalk {y}: {e$message}"
      )
      NULL
    }
  )
  if (is.null(xwalk) || nrow(xwalk) == 0) {
    cli::cli_alert_warning(
      "{Sys.time()}: no schedule crosswalk rows for {y}"
    )
    return(invisible(NULL))
  }

  ifelse(
    !dir.exists(file.path("nba/crosswalk")),
    dir.create(file.path("nba/crosswalk"), recursive = TRUE),
    FALSE
  )
  ifelse(
    !dir.exists(file.path("nba/crosswalk/rds")),
    dir.create(file.path("nba/crosswalk/rds"), recursive = TRUE),
    FALSE
  )
  ifelse(
    !dir.exists(file.path("nba/crosswalk/parquet")),
    dir.create(file.path("nba/crosswalk/parquet"), recursive = TRUE),
    FALSE
  )

  saveRDS(xwalk, glue::glue("nba/crosswalk/rds/nba_schedule_crosswalk_{y}.rds"))
  arrow::write_parquet(
    xwalk,
    glue::glue("nba/crosswalk/parquet/nba_schedule_crosswalk_{y}.parquet"),
    compression = "zstd",
    compression_level = 22
  )

  cli::cli_progress_step(
    msg = "Updating {y} NBA Schedule Crosswalk GitHub Release",
    msg_done = "Updated {y} NBA Schedule Crosswalk GitHub Release!"
  )

  retry_rate <- purrr::rate_backoff(
    pause_base = 1,
    pause_min = 60,
    max_times = 10
  )
  purrr::insistently(
    sportsdataversedata::sportsdataverse_save,
    rate = retry_rate,
    quiet = FALSE
  )(
    data_frame = xwalk,
    file_name = glue::glue("nba_schedule_crosswalk_{y}"),
    sportsdataverse_type = "schedule crosswalk data",
    release_tag = "nba_crosswalk",
    pkg_function = "hoopR::nba_schedule_crosswalk()",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )

  # --- Manifest row append --------------------------------------------------
  manifest_path <- "nba/crosswalk/nba_schedule_crosswalk_in_data_repo.csv"
  manifest_row <- tibble::tibble(
    season           = as.integer(y),
    row_count        = as.integer(nrow(xwalk)),
    generated_at_utc = format(Sys.time(), tz = "UTC", usetz = TRUE),
    source_endpoint  = "hoopR::nba_schedule_crosswalk()"
  )
  if (file.exists(manifest_path)) {
    data.table::fwrite(manifest_row, manifest_path, append = TRUE)
  } else {
    data.table::fwrite(manifest_row, manifest_path)
  }

  rm(xwalk)
  gc()
  invisible(NULL)
}

tictoc::tic()
purrr::walk(years_vec, function(y) {
  tryCatch(
    build_season_schedule_crosswalk(y),
    error = function(e) {
      cli::cli_alert_danger(
        "{Sys.time()}: schedule crosswalk season {y} failed: {e$message}"
      )
    }
  )
})
tictoc::toc()

# --- Manifest upload (idempotent -- overwrites release asset on each run) ----
tryCatch({
  source(file.path("R", "manifest_upload_helper.R"), local = TRUE)
  upload_nba_manifest(
    manifest_path        = "nba/crosswalk/nba_schedule_crosswalk_in_data_repo.csv",
    release_tag          = "nba_crosswalk",
    file_name            = "nba_schedule_crosswalk_in_data_repo",
    sportsdataverse_type = "schedule crosswalk manifest",
    pkg_function         = "hoopR::load_nba_schedule_crosswalk()"
  )
}, error = function(e) {
  cli::cli_alert_warning(
    "{Sys.time()}: schedule crosswalk manifest upload failed (non-fatal): {e$message}"
  )
})

cli::cli_progress_message("")
rm(years_vec)
gcol <- gc()
