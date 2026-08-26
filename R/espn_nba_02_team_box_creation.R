rm(list = ls())
gcol <- gc()
# lib_path <- Sys.getenv("R_LIBS")
# if (!requireNamespace("pacman", quietly = TRUE)) {
#   install.packages("pacman", lib = Sys.getenv("R_LIBS"), repos = "http://cran.us.r-project.org")
# }
suppressPackageStartupMessages(suppressMessages(library(dplyr)))
suppressPackageStartupMessages(suppressMessages(library(magrittr)))
suppressPackageStartupMessages(suppressMessages(library(jsonlite)))
suppressPackageStartupMessages(suppressMessages(library(purrr)))
suppressPackageStartupMessages(suppressMessages(library(progressr)))
suppressPackageStartupMessages(suppressMessages(library(data.table)))
suppressPackageStartupMessages(suppressMessages(library(arrow)))
suppressPackageStartupMessages(suppressMessages(library(glue)))
suppressPackageStartupMessages(suppressMessages(library(optparse)))

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

# --- hoopR#164 team-box repair --------------------------------------------
# Mirrors python/nba_data_build/reshapers.py::_repair_team_box. The 2018
# season's ESPN team boxscore block (raw AND live) carries a label-shifted
# stats tail -- the value under `assists` is really blocks, `flagrantFouls`
# is really steals, and the true assists value is absent entirely. The player
# table for those games is correct, so each summable team stat is rebuilt
# from the player sums. Guard rails: the repair fires only when the player
# table is CREDIBLE (player points sum == team_score AND player FGM sum ==
# team FGM -- a stale pre-final snapshot (hoopR#163) or an ESPN dup-athlete
# payload (2019) fails the gate and is left untouched); when >= 3 summable
# stats disagree (the wholesale-shift signature) the team-only tail stats --
# permuted garbage in that payload shape -- are nulled rather than shipped.
repair_summable_stats <- c("assists", "steals", "blocks", "turnovers", "fouls")
repair_unknowable_stats <- c(
  "team_turnovers",
  "total_turnovers",
  "technical_fouls",
  "total_technical_fouls",
  "flagrant_fouls",
  "turnover_points",
  "fast_break_points",
  "points_in_paint",
  "largest_lead"
)

nba_repair_team_box <- function(tb, pb) {
  if (is.null(tb) || nrow(tb) == 0 || is.null(pb) || nrow(pb) == 0) {
    return(tb)
  }
  if (!all(c("team_id", "team_score", "field_goals_made") %in% colnames(tb))) {
    return(tb)
  }
  stats <- intersect(repair_summable_stats, colnames(tb))
  pcols <- c("team_id", "points", "field_goals_made", stats)
  if (length(stats) == 0 || !all(pcols %in% colnames(pb))) {
    return(tb)
  }
  to_int <- function(x) suppressWarnings(as.integer(as.character(x)))
  sums <- pb %>%
    dplyr::mutate(ps_team_id = to_int(.data$team_id)) %>%
    dplyr::group_by(.data$ps_team_id) %>%
    dplyr::summarise(
      ps_points = sum(to_int(.data$points), na.rm = TRUE),
      ps_fgm = sum(to_int(.data$field_goals_made), na.rm = TRUE),
      dplyr::across(
        dplyr::all_of(stats),
        ~ sum(to_int(.x), na.rm = TRUE),
        .names = "ps_{.col}"
      ),
      .groups = "drop"
    )
  idx <- match(to_int(tb$team_id), sums$ps_team_id)
  for (i in seq_len(nrow(tb))) {
    j <- idx[i]
    if (is.na(j)) {
      next
    }
    credible <- isTRUE(to_int(tb$team_score[i]) == sums$ps_points[j]) &&
      isTRUE(to_int(tb$field_goals_made[i]) == sums$ps_fgm[j])
    if (!credible) {
      next
    }
    wrong <- character(0)
    for (s in stats) {
      ps <- sums[[paste0("ps_", s)]][j]
      if (!is.na(ps) && !isTRUE(to_int(tb[[s]][i]) == ps)) {
        wrong <- c(wrong, s)
      }
    }
    for (s in wrong) {
      ps <- sums[[paste0("ps_", s)]][j]
      tb[[s]][i] <- if (is.character(tb[[s]])) as.character(ps) else as.integer(ps)
    }
    if (length(wrong) >= 3) {
      for (s in intersect(repair_unknowable_stats, colnames(tb))) {
        tb[[s]][i] <- if (is.character(tb[[s]])) NA_character_ else NA_integer_
      }
    }
  }
  tb
}

# --- compile into team_box_{year}.parquet ---------

nba_team_box_games <- function(y) {
  espn_df <- data.frame()
  sched <- readRDS(paste0("nba/schedules/rds/nba_schedule_", y, ".rds"))

  season_team_box_list <- sched %>%
    dplyr::filter(.data$game_json == TRUE) %>%
    dplyr::pull("game_id")

  if (length(season_team_box_list) > 0) {
    cli::cli_progress_step(
      msg = "Compiling {y} ESPN NBA Team Boxscores ({length(season_team_box_list)} games)",
      msg_done = "Compiled {y} ESPN NBA Team Boxscores!"
    )

    future::plan("multisession")
    espn_df <- furrr::future_map_dfr(
      season_team_box_list,
      function(x) {
        resp <- glue::glue(
          "https://raw.githubusercontent.com/sportsdataverse/hoopR-nba-raw/main/nba/json/final/{x}.json"
        )
        tb <- tryCatch(
          hoopR:::helper_espn_nba_team_box(resp),
          error = function(e) NULL,
          warning = function(w) NULL
        )
        if (is.null(tb) || nrow(tb) == 0) {
          return(NULL)
        }
        # hoopR#164: player sums from the SAME payload back the repair
        pb <- tryCatch(
          hoopR:::helper_espn_nba_player_box(resp),
          error = function(e) NULL,
          warning = function(w) NULL
        )
        tryCatch(
          nba_repair_team_box(tb, pb),
          error = function(e) tb
        )
      },
      .options = furrr::furrr_options(seed = TRUE)
    )

    if (nrow(espn_df) > 0 && !("largest_lead" %in% colnames(espn_df))) {
      espn_df$largest_lead <- NA_character_
      espn_df <- espn_df %>%
        dplyr::relocate("largest_lead", .after = last_col())
    }

    cli::cli_progress_step(
      msg = "Updating {y} ESPN NBA Team Boxscores GitHub Release",
      msg_done = "Updated {y} ESPN NBA Team Boxscores GitHub Release!"
    )
  }
  if (nrow(espn_df) > 0) {
    espn_df <- espn_df %>%
      dplyr::arrange(dplyr::desc(.data$game_date)) %>%
      hoopR:::make_hoopR_data(
        "ESPN NBA Team Boxscores from hoopR data repository",
        Sys.time()
      )

    ifelse(
      !dir.exists(file.path("nba/team_box")),
      dir.create(file.path("nba/team_box")),
      FALSE
    )

    # ifelse(!dir.exists(file.path("nba/team_box/csv")), dir.create(file.path("nba/team_box/csv")), FALSE)
    # data.table::fwrite(espn_df, file = paste0("nba/team_box/csv/team_box_", y, ".csv.gz"))

    ifelse(
      !dir.exists(file.path("nba/team_box/rds")),
      dir.create(file.path("nba/team_box/rds")),
      FALSE
    )
    saveRDS(espn_df, glue::glue("nba/team_box/rds/team_box_{y}.rds"))

    ifelse(
      !dir.exists(file.path("nba/team_box/parquet")),
      dir.create(file.path("nba/team_box/parquet")),
      FALSE
    )
    arrow::write_parquet(
      espn_df,
      glue::glue("nba/team_box/parquet/team_box_{y}.parquet")
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
      data_frame = espn_df,
      file_name = glue::glue("team_box_{y}"),
      sportsdataverse_type = "team boxscores data",
      release_tag = "espn_nba_team_boxscores",
      pkg_function = "hoopR::load_nba_team_box()",
      file_types = c("rds", "csv", "parquet"),
      .token = Sys.getenv("GITHUB_PAT")
    )
  }

  sched <- sched %>%
    dplyr::mutate(dplyr::across(
      dplyr::any_of(c(
        "id",
        "game_id",
        "type_id",
        "status_type_id",
        "home_id",
        "home_venue_id",
        "home_conference_id",
        "home_score",
        "away_id",
        "away_venue_id",
        "away_conference_id",
        "away_score",
        "season",
        "season_type",
        "groups_id",
        "tournament_id",
        "venue_id"
      )),
      ~ as.integer(.x)
    )) %>%
    dplyr::mutate(
      status_display_clock = as.character(.data$status_display_clock),
      game_date_time = lubridate::ymd_hm(substr(
        .data$date,
        1,
        nchar(.data$date) - 1
      )) %>%
        lubridate::with_tz(tzone = "America/New_York"),
      game_date = as.Date(substr(.data$game_date_time, 1, 10))
    )

  if (nrow(espn_df) > 0) {
    sched <- sched %>%
      dplyr::mutate(
        team_box = ifelse(
          .data$game_id %in% unique(espn_df$game_id),
          TRUE,
          FALSE
        )
      )
  } else {
    cli::cli_alert_info(
      "{length(season_team_box_list)} ESPN NBA Team Boxscores to be compiled for {y}, skipping Team Boxscores compilation"
    )
    sched$team_box <- FALSE
  }

  final_sched <- sched %>%
    dplyr::distinct() %>%
    dplyr::arrange(dplyr::desc(.data$date))

  final_sched <- final_sched %>%
    hoopR:::make_hoopR_data(
      "ESPN NBA Schedule from hoopR data repository",
      Sys.time()
    )

  # data.table::fwrite(final_sched, paste0("nba/schedules/csv/nba_schedule_", y, ".csv"))
  saveRDS(final_sched, glue::glue("nba/schedules/rds/nba_schedule_{y}.rds"))
  arrow::write_parquet(
    final_sched,
    glue::glue("nba/schedules/parquet/nba_schedule_{y}.parquet")
  )
  rm(sched)
  rm(final_sched)
  rm(espn_df)
  gc()
  return(NULL)
}

all_games <- purrr::map(years_vec, function(y) {
  nba_team_box_games(y)
  return(NULL)
})


cli::cli_progress_step(
  msg = "Compiling ESPN NBA master schedule",
  msg_done = "ESPN NBA master schedule compiled and written to disk"
)

sched_list <- list.files(path = glue::glue("nba/schedules/rds/"))
sched_g <- purrr::map_dfr(sched_list, function(x) {
  sched <- readRDS(paste0("nba/schedules/rds/", x)) %>%
    dplyr::mutate(dplyr::across(
      dplyr::any_of(c(
        "id",
        "game_id",
        "type_id",
        "status_type_id",
        "home_id",
        "home_venue_id",
        "home_conference_id",
        "home_score",
        "away_id",
        "away_venue_id",
        "away_conference_id",
        "away_score",
        "season",
        "season_type",
        "groups_id",
        "tournament_id",
        "venue_id"
      )),
      ~ as.integer(.x)
    )) %>%
    dplyr::mutate(
      status_display_clock = as.character(.data$status_display_clock),
      game_date_time = lubridate::ymd_hm(substr(
        .data$date,
        1,
        nchar(.data$date) - 1
      )) %>%
        lubridate::with_tz(tzone = "America/New_York"),
      game_date = as.Date(substr(.data$game_date_time, 1, 10))
    )
  return(sched)
})

sched_g <- sched_g %>%
  hoopR:::make_hoopR_data(
    "ESPN NBA Schedule from hoopR data repository",
    Sys.time()
  )

# data.table::fwrite(sched_g %>%
#                      dplyr::arrange(dplyr::desc(.data$date)), "nba/nba_schedule_master.csv")
# data.table::fwrite(sched_g %>%
#                      dplyr::filter(.data$PBP == TRUE) %>%
#                      dplyr::arrange(dplyr::desc(.data$date)), "nba/nba_games_in_data_repo.csv")

# arrow::write_parquet(sched_g %>%
#                        dplyr::arrange(dplyr::desc(.data$date)), glue::glue("nba/nba_schedule_master.parquet"))
# arrow::write_parquet(sched_g %>%
#                        dplyr::filter(.data$PBP == TRUE) %>%
#                        dplyr::arrange(dplyr::desc(.data$date)), "nba/nba_games_in_data_repo.parquet")

cli::cli_progress_message("")

rm(all_games)
rm(sched_g)
rm(sched_list)
rm(years_vec)
gcol <- gc()
