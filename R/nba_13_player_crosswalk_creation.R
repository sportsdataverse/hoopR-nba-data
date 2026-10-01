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
  ),
  make_option(
    c("-d", "--dry_run"),
    action = "store_true",
    default = FALSE,
    help = "Build and write files locally; never upload a release asset or the manifest"
  ),
  make_option(
    c("-o", "--out_dir"),
    action = "store",
    default = "nba/crosswalk",
    type = "character",
    help = "Output directory (rds/ + parquet/ + the manifest csv)"
  )
)
opt <- parse_args(OptionParser(option_list = option_list))
options(stringsAsFactors = FALSE)
options(scipen = 999)
years_vec <- opt$s:opt$e
out_dir <- opt$out_dir
dry_run <- isTRUE(opt$dry_run)
current_season <- hoopR:::most_recent_nba_season()

# The matching engine (game evidence + season-pool fallback). Read its header
# for the rule and for why the live roster builder alone cannot be trusted.
source(file.path("R", "nba_player_crosswalk_helpers.R"))

# ESPN's player box starts with 2001-02; before that there is no ESPN id to map.
FIRST_SEASON <- 2002L

XWALK_COLUMNS <- c(
  "season", "espn_team_id", "team_abbreviation", "player_name",
  "espn_athlete_id", "espn_full_name", "espn_jersey", "espn_position",
  "nba_player_id", "nba_player_name", "nba_jersey_num", "nba_position",
  "fox_athlete_id", "fox_player", "fox_jersey", "fox_position_group",
  "yahoo_player_id", "yahoo_player_name",
  "match_method", "match_confidence", "match_keys"
)
FOX_COLUMNS <- c("fox_athlete_id", "fox_player", "fox_jersey", "fox_position_group")

# Stats-side attributes by id: the season roster first, else the latest box row.
stats_attrs <- function(src_list) {
  rows <- lapply(src_list, function(src) {
    out <- list()
    if (!is.null(src$stats_box) && nrow(src$stats_box)) {
      out[[1]] <- src$stats_box[!is.na(person_id), .(
        nba_player_id = as.character(person_id),
        nba_player_name = trimws(paste(first_name, family_name)),
        nba_jersey_num = trimws(as.character(jersey_num)),
        nba_position = as.character(position), rank = 1L)]
    }
    if (!is.null(src$stats_rosters) && nrow(src$stats_rosters)) {
      out[[2]] <- src$stats_rosters[!is.na(player_id), .(
        nba_player_id = as.character(player_id),
        nba_player_name = as.character(player),
        nba_jersey_num = trimws(as.character(num)),
        nba_position = as.character(position), rank = 2L)]
    }
    data.table::rbindlist(out)
  })
  a <- data.table::rbindlist(rows)
  if (!nrow(a)) return(NULL)
  for (col in c("nba_jersey_num", "nba_position")) a[!nzchar(get(col)), (col) := NA_character_]
  # rank 2 (roster) beats rank 1 (box); within a rank the last row wins.
  a <- a[, .SD[.N], by = .(nba_player_id, rank)]
  a[order(-rank), .(
    nba_player_name = nba_player_name[1L],
    nba_jersey_num = stats::na.omit(nba_jersey_num)[1L],
    nba_position = stats::na.omit(nba_position)[1L]
  ), by = nba_player_id]
}

game_rows_to_schema <- function(rows, y) {
  rows[, .(
    season = as.integer(y), espn_team_id = as.integer(espn_team_id),
    team_abbreviation, player_name = name_key, espn_athlete_id, espn_full_name,
    espn_jersey, espn_position, nba_player_id,
    nba_player_name = NA_character_, nba_jersey_num = NA_character_, nba_position = NA_character_,
    fox_athlete_id = NA_character_, fox_player = NA_character_,
    fox_jersey = NA_character_, fox_position_group = NA_character_,
    yahoo_player_id = NA_character_, yahoo_player_name = NA_character_,
    match_method, match_confidence = as.numeric(match_confidence), match_keys
  )]
}

assemble_player_crosswalk <- function(y, src, src_prev, g, live) {
  base <- NULL
  if (!is.null(live) && nrow(live)) {
    live <- data.table::as.data.table(live)[, ..XWALK_COLUMNS]
    live[, `:=`(espn_athlete_id = as.character(espn_athlete_id),
                nba_player_id = as.character(nba_player_id),
                match_confidence = as.numeric(match_confidence))]
  }
  if (!is.null(g)) {
    gx <- game_rows_to_schema(g$rows, y)
    # A finished season is defined by who actually played for whom; the live
    # roster only feeds Fox ids (athlete-level, so season-safe) into it. While
    # the season runs, the live roster is the base and games fill it.
    if (g$complete || is.null(live) || !nrow(live)) {
      base <- gx
      if (!is.null(live) && nrow(live)) {
        fox <- unique(live[!is.na(fox_athlete_id), c("espn_athlete_id", FOX_COLUMNS), with = FALSE], by = "espn_athlete_id")
        base[, (FOX_COLUMNS) := NULL]
        base <- merge(base, fox, by = "espn_athlete_id", all.x = TRUE)
      }
    } else {
      base <- data.table::copy(live)
      ok <- g$rows[match_method != "unmatched",
                   .(espn_athlete_id, g_id = nba_player_id, g_method = match_method,
                     g_conf = match_confidence, g_keys = match_keys)]
      ok <- unique(ok, by = "espn_athlete_id")
      base <- merge(base, ok, by = "espn_athlete_id", all.x = TRUE)
      n_disagree <- base[!is.na(g_id) & !is.na(nba_player_id) & g_id != nba_player_id, .N]
      if (n_disagree) {
        cli::cli_alert_warning("{y}: {n_disagree} live roster match(es) overridden by game evidence")
      }
      # Game evidence beats a roster name match.
      base[!is.na(g_id), `:=`(nba_player_id = g_id, match_method = g_method,
                              match_confidence = g_conf, match_keys = g_keys,
                              nba_player_name = NA_character_, nba_jersey_num = NA_character_,
                              nba_position = NA_character_)]
      base[, c("g_id", "g_method", "g_conf", "g_keys") := NULL]
      # Players this season who are not on a current roster (traded, waived).
      extra <- gx[!base, on = c("espn_athlete_id", "espn_team_id")]
      base <- data.table::rbindlist(list(base, extra), use.names = TRUE)
    }
  } else if (!is.null(live) && nrow(live)) {
    base <- data.table::copy(live)
  }
  if (is.null(base) || !nrow(base)) return(NULL)

  # Season-pool fallback. The current season also searches last season's Stats
  # players, because its Stats rosters lag ESPN's (two-ways, camp invites).
  pool_src <- Filter(Negate(is.null), list(src, src_prev))
  pool <- xw_name_pool(pool_src)
  base <- xw_fill_from_pool(base, pool, if (!is.null(g)) g$team_map else NULL)

  # One Stats id, one ESPN athlete -- across the whole season, whatever path
  # proposed it. Both claimants are withdrawn and counted, never merged.
  claims <- unique(base[!is.na(nba_player_id), .(nba_player_id, espn_athlete_id)])[, .N, by = nba_player_id][N > 1L]
  if (nrow(claims)) {
    base[nba_player_id %in% claims$nba_player_id, `:=`(
      nba_player_id = NA_character_, nba_player_name = NA_character_,
      nba_jersey_num = NA_character_, nba_position = NA_character_,
      match_method = "unmatched", match_confidence = NA_real_,
      match_keys = "many_to_one: stats id claimed by another ESPN athlete"
    )]
  }

  attrs <- stats_attrs(pool_src)
  if (!is.null(attrs)) {
    base <- merge(base, attrs, by = "nba_player_id", all.x = TRUE, suffixes = c("", ".a"))
    base[is.na(nba_player_name) & !is.na(nba_player_id), `:=`(
      nba_player_name = nba_player_name.a, nba_jersey_num = nba_jersey_num.a,
      nba_position = nba_position.a)]
    base[, c("nba_player_name.a", "nba_jersey_num.a", "nba_position.a") := NULL]
  }
  base[is.na(nba_player_id) & match_method != "unmatched", match_method := "unmatched"]

  out <- base[, ..XWALK_COLUMNS]
  out[, `:=`(season = as.integer(season), espn_team_id = as.integer(espn_team_id))]
  data.table::setorder(out, espn_team_id, player_name, espn_athlete_id)
  hoopR:::make_hoopR_data(
    tibble::as_tibble(out),
    "NBA player crosswalk (ESPN / NBA Stats / Fox)",
    Sys.time()
  )
}

# --- main loop -------------------------------------------------------------

build_season_player_crosswalk <- function(y) {
  cli::cli_progress_step(
    msg = "Compiling {y} NBA player crosswalk",
    msg_done = "Compiled {y} NBA player crosswalk!"
  )
  if (y < FIRST_SEASON) {
    cli::cli_alert_warning("{y}: before {FIRST_SEASON} there is no ESPN player box to map; skipped")
    return(invisible(NULL))
  }

  src <- xw_load_sources(y)
  g <- xw_game_evidence(src, y)
  live <- NULL
  src_prev <- NULL
  if (y >= current_season) {
    # Live ESPN/Stats/Fox rosters are current-state: only meaningful for the
    # season being served now.
    live <- tryCatch(
      hoopR::nba_player_crosswalk(season = y),
      error = function(e) {
        cli::cli_alert_warning("{Sys.time()}: live player crosswalk {y} failed: {e$message}")
        NULL
      }
    )
    src_prev <- xw_load_sources(y - 1L)
  }
  if (is.null(g)) {
    cli::cli_alert_info("{y}: no game evidence (season not started, or a game source is missing)")
  } else {
    s <- g$stats
    cli::cli_alert_info(
      "{y}: linked {s$linked_team_games}/{s$espn_team_games} team-games; player-games matched by name {s$matched_by_name}, by stat line {s$matched_by_statline}"
    )
  }

  xwalk <- assemble_player_crosswalk(y, src, src_prev, g, live)
  if (is.null(xwalk) || nrow(xwalk) == 0) {
    cli::cli_alert_warning(
      "{Sys.time()}: no player crosswalk rows for {y}"
    )
    return(invisible(NULL))
  }
  n_un <- sum(is.na(xwalk$nba_player_id))
  cli::cli_alert_info(
    "{y}: {nrow(xwalk)} rows, {n_un} unmatched ({round(100 * n_un / nrow(xwalk), 1)}%); methods: {paste(names(table(xwalk$match_method)), table(xwalk$match_method), sep = '=', collapse = ', ')}"
  )

  dir.create(file.path(out_dir, "rds"), recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(out_dir, "parquet"), recursive = TRUE, showWarnings = FALSE)

  saveRDS(xwalk, file.path(out_dir, "rds", glue::glue("nba_player_crosswalk_{y}.rds")))
  arrow::write_parquet(
    xwalk,
    file.path(out_dir, "parquet", glue::glue("nba_player_crosswalk_{y}.parquet")),
    compression = "zstd",
    compression_level = 22
  )

  if (dry_run) {
    cli::cli_alert_info("{y}: --dry_run, release upload skipped")
  } else {
    cli::cli_progress_step(
      msg = "Updating {y} NBA Player Crosswalk GitHub Release",
      msg_done = "Updated {y} NBA Player Crosswalk GitHub Release!"
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
      file_name = glue::glue("nba_player_crosswalk_{y}"),
      sportsdataverse_type = "player crosswalk data",
      release_tag = "nba_crosswalk",
      pkg_function = "hoopR::nba_player_crosswalk()",
      file_types = c("rds", "csv", "parquet"),
      .token = Sys.getenv("GITHUB_PAT")
    )
  }

  # --- Manifest row upsert (one row per season, so a rebuild is idempotent) --
  manifest_path <- file.path(out_dir, "nba_player_crosswalk_in_data_repo.csv")
  manifest_row <- data.table::data.table(
    season           = as.integer(y),
    row_count        = as.integer(nrow(xwalk)),
    generated_at_utc = format(Sys.time(), tz = "UTC", usetz = TRUE),
    source_endpoint  = "hoopR::nba_player_crosswalk()"
  )
  manifest <- if (file.exists(manifest_path)) data.table::fread(manifest_path, colClasses = "character") else NULL
  if (!is.null(manifest)) manifest <- manifest[season != as.character(y)]
  manifest <- data.table::rbindlist(list(manifest, manifest_row[, lapply(.SD, as.character)]), use.names = TRUE)
  data.table::setorder(manifest, season)
  data.table::fwrite(manifest, manifest_path)

  rm(xwalk)
  gc()
  invisible(NULL)
}

tictoc::tic()
purrr::walk(years_vec, function(y) {
  tryCatch(
    build_season_player_crosswalk(y),
    error = function(e) {
      cli::cli_alert_danger(
        "{Sys.time()}: player crosswalk season {y} failed: {e$message}"
      )
    }
  )
})
tictoc::toc()

# --- Manifest upload (idempotent -- overwrites release asset on each run) ----
if (!dry_run) {
  tryCatch({
    source(file.path("R", "manifest_upload_helper.R"), local = TRUE)
    upload_nba_manifest(
      manifest_path        = file.path(out_dir, "nba_player_crosswalk_in_data_repo.csv"),
      release_tag          = "nba_crosswalk",
      file_name            = "nba_player_crosswalk_in_data_repo",
      sportsdataverse_type = "player crosswalk manifest",
      pkg_function         = "hoopR::load_nba_player_crosswalk()"
    )
  }, error = function(e) {
    cli::cli_alert_warning(
      "{Sys.time()}: player crosswalk manifest upload failed (non-fatal): {e$message}"
    )
  })
}

cli::cli_progress_message("")
rm(years_vec)
gcol <- gc()
