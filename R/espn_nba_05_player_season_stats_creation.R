#!/usr/bin/env Rscript
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
suppressPackageStartupMessages(suppressMessages(library(tidyr)))
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

# Athlete-keyed raw payloads: nba/player_season_stats/json/{athlete_id}.json.
# Each file is the full ESPN /athletes/{id}/stats career response -- the
# per-season breakdown lives in categories[].statistics[] (ESPN ignores the
# season query param), so one file serves every season the athlete played.
raw_base <- "https://raw.githubusercontent.com/sportsdataverse/hoopR-nba-raw/main/nba/player_season_stats/json"

# --- helpers ---------------------------------------------------------------

safe_chr <- function(x) {
  if (is.null(x)) return(NA_character_)
  if (length(x) == 0) return(NA_character_)
  as.character(x[[1]])
}

safe_int <- function(x) {
  if (is.null(x)) return(NA_integer_)
  if (length(x) == 0) return(NA_integer_)
  suppressWarnings(as.integer(x[[1]]))
}

# Coalesce-style for character scalars (NA / "" treated as missing)
`%|%` <- function(a, b) {
  if (is.null(a) || length(a) == 0) return(b)
  if (length(a) == 1 && is.na(a)) return(b)
  if (length(a) == 1 && is.character(a) && !nzchar(a)) return(b)
  a
}

# Build the season-Y athlete identity lookup + id list from the populated
# espn_nba_player_boxscores release -- the authoritative "who played in Y"
# source (ESPN's team-roster endpoint is current-only and cannot supply
# historical rosters). Returns a named list keyed by athlete_id (character).
build_identity_lookup <- function(season) {
  pb <- tryCatch(
    hoopR::load_nba_player_box(seasons = season),
    error = function(e) {
      cli::cli_alert_warning(
        "{Sys.time()}: could not load player_box for {season}: {e$message}"
      )
      NULL
    }
  )
  if (is.null(pb) || nrow(pb) == 0) return(list())
  pb <- as.data.frame(pb)

  pick <- function(df, nm) if (nm %in% colnames(df)) df[[nm]] else NA

  ids <- suppressWarnings(as.integer(pick(pb, "athlete_id")))
  disp <- as.character(pick(pb, "athlete_display_name"))
  pos <- as.character(pick(pb, "athlete_position_abbreviation"))
  jer <- as.character(pick(pb, "athlete_jersey"))
  tid <- suppressWarnings(as.integer(pick(pb, "team_id")))
  tdn <- as.character(pick(pb, "team_display_name"))

  keep <- !is.na(ids)
  d <- data.frame(
    athlete_id = ids[keep],
    display_name = disp[keep],
    position_abbreviation = pos[keep],
    jersey = jer[keep],
    team_id = tid[keep],
    team_display_name = tdn[keep],
    stringsAsFactors = FALSE
  )
  # One identity row per athlete (last appearance wins -- carries their most
  # recent team within the season).
  d <- d[!duplicated(d$athlete_id, fromLast = TRUE), , drop = FALSE]

  lookup <- list()
  for (i in seq_len(nrow(d))) {
    lookup[[as.character(d$athlete_id[i])]] <- list(
      display_name = d$display_name[i],
      position_abbreviation = d$position_abbreviation[i],
      jersey = d$jersey[i],
      team_id = d$team_id[i],
      team_display_name = d$team_display_name[i]
    )
  }
  lookup
}

# From one category, select the statistics[] entry for `season` and emit long
# rows. When a player was traded mid-season ESPN ships one entry per team
# stint plus a "YYYY-YY Totals" row -- prefer the Totals (whole-season)
# aggregate; otherwise take the single stint.
parse_one_category <- function(season, athlete_id, athlete_meta, category) {
  stats_entries <- category[["statistics"]] %||% list()
  if (length(stats_entries) == 0) return(NULL)

  labels <- unlist(category[["labels"]] %||% list(), use.names = FALSE)
  names_ <- unlist(category[["names"]] %||% list(), use.names = FALSE)
  display_names <- unlist(category[["displayNames"]] %||% list(), use.names = FALSE)
  descriptions <- unlist(category[["descriptions"]] %||% list(), use.names = FALSE)

  # Collect entries matching the requested season year.
  matches <- list()
  for (s in stats_entries) {
    yr <- safe_int(s[["season"]][["year"]])
    if (!is.na(yr) && yr == season) matches <- c(matches, list(s))
  }
  if (length(matches) == 0) return(NULL)

  # Prefer the season-Totals row (teamSlug contains "Totals", teamId absent).
  chosen <- NULL
  for (s in matches) {
    slug <- safe_chr(s[["teamSlug"]])
    if (!is.na(slug) && grepl("Totals", slug, ignore.case = TRUE)) {
      chosen <- s
      break
    }
  }
  if (is.null(chosen)) chosen <- matches[[length(matches)]]

  vals <- as.character(unlist(chosen[["stats"]] %||% list(), use.names = FALSE))
  n <- length(vals)
  if (n == 0) return(NULL)

  pad <- function(x, n) {
    if (length(x) == 0) return(rep(NA_character_, n))
    if (length(x) >= n) return(as.character(x[seq_len(n)]))
    c(as.character(x), rep(NA_character_, n - length(x)))
  }

  cat_name <- safe_chr(category[["name"]]) %|%
    safe_chr(category[["displayName"]]) %|%
    NA_character_

  # team for this season: the chosen stint's team, falling back to the
  # box-score identity (the Totals row has no teamId).
  stint_team_id <- safe_int(chosen[["teamId"]])
  team_id <- stint_team_id %|% athlete_meta$team_id
  team_slug <- safe_chr(chosen[["teamSlug"]])

  num_val <- suppressWarnings(as.numeric(vals))

  tibble::tibble(
    season = as.integer(season),
    athlete_id = as.integer(athlete_id),
    athlete_display_name = athlete_meta$display_name,
    athlete_position_abbreviation = athlete_meta$position_abbreviation,
    athlete_jersey = athlete_meta$jersey,
    team_id = team_id,
    team_slug = team_slug,
    team_display_name = athlete_meta$team_display_name,
    category = cat_name,
    stat_label = pad(labels, n),
    stat_name = pad(names_, n),
    stat_display_name = pad(display_names, n),
    stat_description = pad(descriptions, n),
    display_value = vals,
    value = num_val
  )
}

parse_one_athlete <- function(season, athlete_id, identity_lookup, raw_base) {
  url <- glue::glue("{raw_base}/{athlete_id}.json")
  raw <- tryCatch(
    jsonlite::fromJSON(url, simplifyVector = FALSE),
    error = function(e) {
      # athlete may have no scraped stats file -- skip silently-ish
      NULL
    }
  )
  if (is.null(raw)) return(NULL)

  categories <- raw[["categories"]] %||%
    raw[["statCategories"]] %||%
    raw[["splits"]][["categories"]] %||%
    list()
  if (length(categories) == 0) return(NULL)

  meta <- identity_lookup[[as.character(athlete_id)]]
  if (is.null(meta)) {
    meta <- list(
      display_name = NA_character_,
      position_abbreviation = NA_character_,
      jersey = NA_character_,
      team_id = NA_integer_,
      team_display_name = NA_character_
    )
  }

  purrr::map_dfr(categories, function(cat) {
    parse_one_category(season, athlete_id, meta, cat)
  })
}

# --- main loop -------------------------------------------------------------

build_season_player_stats <- function(y) {
  identity_lookup <- build_identity_lookup(y)
  athlete_ids <- suppressWarnings(as.integer(names(identity_lookup)))
  athlete_ids <- athlete_ids[!is.na(athlete_ids)]
  if (length(athlete_ids) == 0) {
    cli::cli_alert_warning(
      "{Sys.time()}: no player_box athletes for {y}; skipping player_season_stats"
    )
    return(invisible(NULL))
  }

  cli::cli_progress_step(
    msg = "Compiling {y} ESPN NBA player season stats ({length(athlete_ids)} athletes)",
    msg_done = "Compiled {y} ESPN NBA player season stats!"
  )

  future::plan("multisession")
  stats <- furrr::future_map_dfr(
    athlete_ids,
    function(a) {
      tryCatch(
        parse_one_athlete(y, a, identity_lookup, raw_base),
        error = function(e) NULL
      )
    },
    .options = furrr::furrr_options(seed = TRUE)
  )

  if (is.null(stats) || nrow(stats) == 0) {
    cli::cli_alert_warning(
      "{Sys.time()}: no player_season_stats rows parsed for {y}"
    )
    return(invisible(NULL))
  }

  stats <- stats %>%
    dplyr::distinct() %>%
    hoopR:::make_hoopR_data(
      "ESPN NBA Player Season Stats from hoopR data repository",
      Sys.time()
    )

  ifelse(
    !dir.exists(file.path("nba/player_season_stats")),
    dir.create(file.path("nba/player_season_stats"), recursive = TRUE),
    FALSE
  )
  ifelse(
    !dir.exists(file.path("nba/player_season_stats/rds")),
    dir.create(file.path("nba/player_season_stats/rds"), recursive = TRUE),
    FALSE
  )
  ifelse(
    !dir.exists(file.path("nba/player_season_stats/parquet")),
    dir.create(file.path("nba/player_season_stats/parquet"), recursive = TRUE),
    FALSE
  )

  saveRDS(
    stats,
    glue::glue("nba/player_season_stats/rds/player_season_stats_{y}.rds")
  )
  arrow::write_parquet(
    stats,
    glue::glue("nba/player_season_stats/parquet/player_season_stats_{y}.parquet")
  )

  cli::cli_progress_step(
    msg = "Updating {y} ESPN NBA Player Season Stats GitHub Release",
    msg_done = "Updated {y} ESPN NBA Player Season Stats GitHub Release!"
  )

  retry_rate <- purrr::rate_backoff(
    pause_base = 1,
    pause_min = 1,
    max_times = 5
  )
  purrr::insistently(
    sportsdataversedata::sportsdataverse_save,
    rate = retry_rate,
    quiet = FALSE
  )(
    data_frame = stats,
    file_name = glue::glue("player_season_stats_{y}"),
    sportsdataverse_type = "player season stats data",
    release_tag = "espn_nba_player_season_stats",
    pkg_function = "hoopR::load_nba_player_stats()",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )

  # --- Manifest row append --------------------------------------------------
  manifest_path <- "nba/player_season_stats/nba_player_season_stats_in_data_repo.csv"
  manifest_row <- tibble::tibble(
    season           = as.integer(y),
    row_count        = as.integer(nrow(stats)),
    generated_at_utc = format(Sys.time(), tz = "UTC", usetz = TRUE),
    source_endpoint  = glue::glue("{raw_base}/<athlete_id>.json")
  )
  if (file.exists(manifest_path)) {
    data.table::fwrite(manifest_row, manifest_path, append = TRUE)
  } else {
    data.table::fwrite(manifest_row, manifest_path)
  }

  rm(stats)
  gc()
  invisible(NULL)
}

tictoc::tic()
purrr::walk(years_vec, function(y) {
  tryCatch(
    build_season_player_stats(y),
    error = function(e) {
      cli::cli_alert_danger(
        "{Sys.time()}: player_season_stats season {y} failed: {e$message}"
      )
    }
  )
})
tictoc::toc()

# --- Manifest upload (idempotent -- overwrites release asset on each run) ----
tryCatch({
  source(file.path("R", "manifest_upload_helper.R"), local = TRUE)
  upload_nba_manifest(
    manifest_path        = "nba/player_season_stats/nba_player_season_stats_in_data_repo.csv",
    release_tag          = "espn_nba_player_season_stats",
    file_name            = "nba_player_season_stats_in_data_repo",
    sportsdataverse_type = "player season stats manifest",
    pkg_function         = "hoopR::load_nba_player_stats_manifest()"
  )
}, error = function(e) {
  cli::cli_alert_warning(
    "{Sys.time()}: player_season_stats manifest upload failed (non-fatal): {e$message}"
  )
})

cli::cli_progress_message("")
rm(years_vec)
gcol <- gc()
