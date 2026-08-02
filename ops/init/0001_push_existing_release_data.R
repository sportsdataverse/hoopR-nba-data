lib_path <- Sys.getenv("R_LIBS")
if (!requireNamespace("pacman", quietly = TRUE)){
  install.packages("pacman",lib=Sys.getenv("R_LIBS"), repos="http://cran.us.r-project.org")
}
suppressPackageStartupMessages(suppressMessages(library(dplyr, lib.loc=lib_path)))
suppressPackageStartupMessages(suppressMessages(library(magrittr, lib.loc=lib_path)))
suppressPackageStartupMessages(suppressMessages(library(jsonlite, lib.loc=lib_path)))
suppressPackageStartupMessages(suppressMessages(library(purrr, lib.loc=lib_path)))
suppressPackageStartupMessages(suppressMessages(library(progressr, lib.loc=lib_path)))
suppressPackageStartupMessages(suppressMessages(library(data.table, lib.loc=lib_path)))
suppressPackageStartupMessages(suppressMessages(library(arrow, lib.loc=lib_path)))
suppressPackageStartupMessages(suppressMessages(library(glue, lib.loc=lib_path)))
suppressPackageStartupMessages(suppressMessages(library(optparse, lib.loc=lib_path)))



sched_list <- list.files(path = glue::glue("mbb/schedules/rds/"))
sched_g <-  purrr::map(sched_list, function(x) {
  sched <- readRDS(paste0("mbb/schedules/rds/", x)) %>%
    dplyr::mutate(
      id = as.integer(.data$id),
      game_id = as.integer(.data$game_id),
      status_display_clock = as.character(.data$status_display_clock)
    )

  sched <- sched %>%
    hoopR:::make_hoopR_data("ESPN MBB Schedule from hoopR data repository", Sys.time())
  y <- stringr::str_extract(x, "\\d+")
  sportsdataversedata::sportsdataverse_save(
    data_frame = sched,
    file_name =  glue::glue("mbb_schedule_{y}"),
    sportsdataverse_type = "schedule data",
    release_tag = "espn_mens_college_basketball_schedules",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )
})
rm(sched_g)

pbp_list <- list.files(path = glue::glue("mbb/pbp/rds/"))
pbp_g <-  purrr::map(pbp_list, function(x) {
  pbp <- readRDS(paste0("mbb/pbp/rds/", x))

  pbp <- pbp %>%
      hoopR:::make_hoopR_data("ESPN MBB Play-by-Play from hoopR data repository", Sys.time())
  y <- stringr::str_extract(x, "\\d+")
  sportsdataversedata::sportsdataverse_save(
    data_frame = pbp,
    file_name =  glue::glue("play_by_play_{y}"),
    sportsdataverse_type = "Play-by-Play data",
    release_tag = "espn_mens_college_basketball_pbp",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )
})
rm(pbp_g)

team_box_list <- list.files(path = glue::glue("mbb/team_box/rds/"))
team_box_g <-  purrr::map(team_box_list, function(x) {
  team_box <- readRDS(paste0("mbb/team_box/rds/", x))
  team_box <- team_box %>%
      hoopR:::make_hoopR_data("ESPN MBB Team Boxscores from hoopR data repository", Sys.time())
  y <- stringr::str_extract(x, "\\d+")
  sportsdataversedata::sportsdataverse_save(
    data_frame = team_box,
    file_name =  glue::glue("team_box_{y}"),
    sportsdataverse_type = "Team Boxscores data",
    release_tag = "espn_mens_college_basketball_team_boxscores",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )
})

rm(team_box_g)

player_box_list <- list.files(path = glue::glue("mbb/player_box/rds/"))
player_box_g <-  purrr::map(player_box_list, function(x) {
  player_box <- readRDS(paste0("mbb/player_box/rds/", x))
  player_box <- player_box %>%
      hoopR:::make_hoopR_data("ESPN MBB Player Boxscores from hoopR data repository", Sys.time())
  y <- stringr::str_extract(x, "\\d+")
  sportsdataversedata::sportsdataverse_save(
    data_frame = player_box,
    file_name =  glue::glue("player_box_{y}"),
    sportsdataverse_type = "Player Boxscores data",
    release_tag = "espn_mens_college_basketball_player_boxscores",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )
})

rm(player_box_g)

sched_list <- list.files(path = glue::glue("nba/schedules/rds/"))
sched_g <-  purrr::map(sched_list, function(x) {
  sched <- readRDS(paste0("nba/schedules/rds/", x)) %>%
    dplyr::mutate(
      id = as.integer(.data$id),
      game_id = as.integer(.data$game_id),
      status_display_clock = as.character(.data$status_display_clock)
    )

  sched <- sched %>%
    hoopR:::make_hoopR_data("ESPN NBA Schedule from hoopR data repository", Sys.time())
  y <- stringr::str_extract(x, "\\d+")
  sportsdataversedata::sportsdataverse_save(
    data_frame = sched,
    file_name =  glue::glue("nba_schedule_{y}"),
    sportsdataverse_type = "schedule data",
    release_tag = "espn_nba_schedules",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )
})
rm(sched_g)

pbp_list <- list.files(path = glue::glue("nba/pbp/rds/"))
pbp_g <-  purrr::map(pbp_list, function(x) {
  pbp <- readRDS(paste0("nba/pbp/rds/", x))

  pbp <- pbp %>%
      hoopR:::make_hoopR_data("ESPN NBA Play-by-Play from hoopR data repository", Sys.time())
  y <- stringr::str_extract(x, "\\d+")
  sportsdataversedata::sportsdataverse_save(
    data_frame = pbp,
    file_name =  glue::glue("play_by_play_{y}"),
    sportsdataverse_type = "Play-by-Play data",
    release_tag = "espn_nba_pbp",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )
})
rm(pbp_g)

team_box_list <- list.files(path = glue::glue("nba/team_box/rds/"))
team_box_g <-  purrr::map(team_box_list, function(x) {
  team_box <- readRDS(paste0("nba/team_box/rds/", x))
  team_box <- team_box %>%
      hoopR:::make_hoopR_data("ESPN NBA Team Boxscores from hoopR data repository", Sys.time())
  y <- stringr::str_extract(x, "\\d+")
  sportsdataversedata::sportsdataverse_save(
    data_frame = team_box,
    file_name =  glue::glue("team_box_{y}"),
    sportsdataverse_type = "Team Boxscores data",
    release_tag = "espn_nba_team_boxscores",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )
})

rm(team_box_g)

player_box_list <- list.files(path = glue::glue("nba/player_box/rds/"))
player_box_g <-  purrr::map(player_box_list, function(x) {
  player_box <- readRDS(paste0("nba/player_box/rds/", x))
  player_box <- player_box %>%
      hoopR:::make_hoopR_data("ESPN NBA Player Boxscores from hoopR data repository", Sys.time())
  y <- stringr::str_extract(x, "\\d+")
  sportsdataversedata::sportsdataverse_save(
    data_frame = player_box,
    file_name =  glue::glue("player_box_{y}"),
    sportsdataverse_type = "Player Boxscores data",
    release_tag = "espn_nba_player_boxscores",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )
})

rm(player_box_g)
