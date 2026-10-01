# Helpers for R/nba_13_player_crosswalk_creation.R -- sourced, not a stage.
#
# Why this exists: hoopR::nba_player_crosswalk() joins ESPN's team-roster
# endpoint to stats.nba.com's commonteamroster inside each team. ESPN's roster
# endpoint is CURRENT-STATE (it ignores `season`), so the live builder is only
# right for the season ESPN is currently serving. Built for any other season it
# pairs one season's ESPN rosters with another season's Stats rosters -- the
# 2026 asset (built 2026-07-14, ESPN already on 2026-27 rosters) left 149/544
# (27.4%) unmatched: 74 players who changed teams + 73 draftees/new signings.
# And it can never build history, so the release only ever held 2026 + 2027.
#
# The fix builds the crosswalk from game evidence that is already captured for
# every season since 2001-02: the ESPN player box (espn_nba_player_boxscores)
# and the stats.nba.com player box (nba_stats_player_boxscores), bridged per
# game by the stats.nba.com team game logs (nba_stats_player_game_logs).
#
# Matching rule, in order (each step only sees what the previous left open):
#   1. Game link (id join): an ESPN (game, team) is tied to a Stats (game, team)
#      on (ET game date, team points, opponent points); a key that is not
#      unique on BOTH sides is dropped, never guessed.
#   2. Within one linked (game, team): normalized full name (case, diacritics,
#      punctuation, Jr./Sr./II/III/IV folded by hoopR's .bb_normalize_name),
#      unique on both sides.
#   3. Still within that (game, team), for players who did something: the stat
#      line (pts, reb, ast, fgm, fga), unique on both sides. Name-independent,
#      so it resolves "Nene" / "Nene Hilario"-type variants.
#   4. Season vote: an ESPN athlete takes the Stats id that >= 90% of its
#      matched games agree on. Below that it stays unmatched ("ambiguous").
#   5. One-to-one: a Stats id claimed by two ESPN athletes in one season is
#      withdrawn from both (counted as "many_to_one"), never silently merged.
#   6. Season-pool name fallback for rows still open: the normalized name must
#      map to exactly one Stats id in the season's Stats rosters + box scores
#      (team breaks a tie), and that id must not already be taken.

.xw_norm <- function(x) hoopR:::.bb_normalize_name(x)

.xw_release_url <- function(tag, file) {
  glue::glue(
    "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/{tag}/{file}"
  )
}

# Read one season parquet: a local copy first (the committed tree, or a sibling
# checkout via env), else the release asset. NULL when neither is available --
# the caller decides whether that is fatal for the season.
.xw_read <- function(local_path, tag, file) {
  if (!is.na(local_path) && file.exists(local_path)) {
    return(data.table::as.data.table(arrow::read_parquet(local_path)))
  }
  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  ok <- tryCatch(
    {
      utils::download.file(.xw_release_url(tag, file), tmp, mode = "wb", quiet = TRUE)
      TRUE
    },
    error = function(e) FALSE,
    warning = function(w) FALSE
  )
  if (!ok) {
    cli::cli_alert_warning("{Sys.time()}: could not read {tag}/{file}")
    return(NULL)
  }
  data.table::as.data.table(arrow::read_parquet(tmp))
}

# The three game-level sources for season `y` (END-year convention on every tag).
# ESPN box: this repo's own committed tree. Stats: HOOPR_NBA_STATS_DATA_ROOT
# (a hoopR-nba-stats-data/nba_stats dir) when set, else the release.
xw_load_sources <- function(y, espn_root = "nba", stats_root = Sys.getenv("HOOPR_NBA_STATS_DATA_ROOT", NA)) {
  sp <- function(key, stem) {
    if (is.na(stats_root) || !nzchar(stats_root)) return(NA_character_)
    file.path(stats_root, key, "parquet", glue::glue("{stem}_{y}.parquet"))
  }
  list(
    espn_box = .xw_read(
      file.path(espn_root, "player_box", "parquet", glue::glue("player_box_{y}.parquet")),
      "espn_nba_player_boxscores", glue::glue("player_box_{y}.parquet")
    ),
    stats_box = .xw_read(
      sp("player_boxscores", "player_boxscores"),
      "nba_stats_player_boxscores", glue::glue("player_boxscores_{y}.parquet")
    ),
    stats_team_logs = .xw_read(
      sp("player_game_logs", "player_game_logs"),
      "nba_stats_player_game_logs", glue::glue("player_game_logs_{y}.parquet")
    ),
    stats_rosters = .xw_read(
      sp("rosters", "rosters"),
      "nba_stats_rosters", glue::glue("rosters_{y}.parquet")
    )
  )
}

.xw_statline <- function(p, r, a, fgm, fga) {
  key <- paste(p, r, a, fgm, fga, sep = "|")
  # An all-zero (DNP / garbage-time) line says nothing about who it is.
  key[is.na(p) | (p + r + a + fga) == 0] <- NA_character_
  key
}

# Keep only rows whose `key` value occurs once within `by` (uniqueness guard).
.xw_unique_in <- function(dt, by, col) {
  v <- dt[[col]]
  dt <- dt[!is.na(v) & nzchar(v)]
  dt[, .n := .N, by = c(by, col)][.n == 1L][, .n := NULL][]
}

#' Game-evidence crosswalk for one season (steps 1-5 of the rule above).
#' @return list(rows = one row per ESPN athlete per ESPN team, team_map, stats)
#'   or NULL when a source is missing.
xw_game_evidence <- function(src, y, vote_floor = 0.9) {
  `%||%` <- function(a, b) if (is.null(a)) b else a
  if (any(vapply(src[c("espn_box", "stats_box", "stats_team_logs")], function(x) is.null(x) || !nrow(x), TRUE))) {
    return(NULL)
  }
  es <- src$espn_box[!is.na(athlete_id), .(
    espn_game_id = as.character(game_id),
    game_date = as.Date(game_date),
    espn_team_id = as.integer(team_id),
    team_abbreviation = as.character(team_abbreviation),
    pts = as.integer(team_score), opp = as.integer(opponent_team_score),
    espn_athlete_id = as.character(athlete_id),
    espn_full_name = as.character(athlete_display_name),
    espn_jersey = as.character(athlete_jersey),
    espn_position = as.character(athlete_position_abbreviation),
    statline = .xw_statline(points, rebounds, assists, field_goals_made, field_goals_attempted)
  )]
  es[, name_key := .xw_norm(espn_full_name)]

  tl <- src$stats_team_logs[, .(
    nba_game_id = as.character(game_id), nba_team_id = as.character(team_id),
    game_date = as.Date(game_date), pts = as.integer(pts)
  )]
  tl <- merge(tl, tl[, .(nba_game_id, opp_team = nba_team_id, opp = pts)],
              by = "nba_game_id", allow.cartesian = TRUE)[nba_team_id != opp_team]
  tl[, opp_team := NULL]

  # Step 1: (date, pts, opp) must be unique on both sides to link.
  e_tg <- .xw_unique_in(unique(es[, .(espn_game_id, espn_team_id, game_date, pts, opp)])[
    , key := paste(game_date, pts, opp)], character(0), "key")
  n_tg <- .xw_unique_in(unique(tl)[, key := paste(game_date, pts, opp)], character(0), "key")
  link <- merge(e_tg[, .(espn_game_id, espn_team_id, key)],
                n_tg[, .(nba_game_id, nba_team_id, key)], by = "key")[, key := NULL]

  nb <- src$stats_box[!is.na(person_id), .(
    nba_game_id = as.character(game_id), nba_team_id = as.character(team_id),
    nba_player_id = as.character(person_id),
    nba_player_name = trimws(paste(first_name, family_name)),
    nba_jersey_num = trimws(as.character(jersey_num)),
    nba_position = as.character(position),
    statline = .xw_statline(points, rebounds_total, assists, field_goals_made, field_goals_attempted)
  )]
  nb[, name_key := .xw_norm(nba_player_name)]

  el <- merge(es, link, by = c("espn_game_id", "espn_team_id"))
  grp <- c("nba_game_id", "nba_team_id")
  # Step 2: name, unique on both sides within the linked team-game.
  m_name <- merge(.xw_unique_in(el, grp, "name_key")[, .(nba_game_id, nba_team_id, name_key, espn_athlete_id)],
                  .xw_unique_in(nb, grp, "name_key")[, .(nba_game_id, nba_team_id, name_key, nba_player_id)],
                  by = c(grp, "name_key"))[, how := "name"]
  # Step 3: stat line among what step 2 left open on both sides.
  el_open <- el[!m_name, on = c(grp, "espn_athlete_id")]
  nb_open <- nb[!m_name, on = c(grp, "nba_player_id")]
  m_stat <- merge(.xw_unique_in(el_open, grp, "statline")[, .(nba_game_id, nba_team_id, statline, espn_athlete_id)],
                  .xw_unique_in(nb_open, grp, "statline")[, .(nba_game_id, nba_team_id, statline, nba_player_id)],
                  by = c(grp, "statline"))[, how := "statline"]
  m <- data.table::rbindlist(list(m_name, m_stat), use.names = TRUE, fill = TRUE)

  # Step 4: season vote per ESPN athlete.
  votes <- m[, .(n = .N, n_name = sum(how == "name"), n_stat = sum(how == "statline")),
             by = .(espn_athlete_id, nba_player_id)]
  votes[, total := sum(n), by = espn_athlete_id]
  data.table::setorder(votes, espn_athlete_id, -n, nba_player_id)
  best <- votes[, .SD[1L], by = espn_athlete_id]
  best[, share := n / total]
  best[, status := data.table::fifelse(share >= vote_floor, "ok", "ambiguous")]
  # Step 5: one Stats id, one ESPN athlete.
  best[status == "ok", claims := .N, by = nba_player_id]
  best[status == "ok" & claims > 1L, status := "many_to_one"]

  # Output grain: one row per ESPN athlete per ESPN team (latest game's labels).
  data.table::setorder(es, espn_athlete_id, espn_team_id, -game_date)
  rows <- es[, .SD[1L], by = .(espn_athlete_id, espn_team_id),
             .SDcols = c("team_abbreviation", "espn_full_name", "espn_jersey", "espn_position", "name_key")]
  rows <- merge(rows, best[, .(espn_athlete_id, nba_player_id, n, n_name, n_stat, total, share, status)],
                by = "espn_athlete_id", all.x = TRUE)
  rows[is.na(status), status := "no_game_evidence"]
  rows[status != "ok", nba_player_id := NA_character_]
  rows[, `:=`(
    match_method = data.table::fcase(
      status == "ok" & n_name >= n_stat, "game_name",
      status == "ok", "game_statline",
      default = "unmatched"
    ),
    match_confidence = data.table::fifelse(status == "ok", share, NA_real_),
    match_keys = data.table::fcase(
      status == "ok", sprintf("games=%d/%d name=%d statline=%d", n, total, n_name, n_stat),
      status == "ambiguous", sprintf("ambiguous: top id has %d of %d games", n, total),
      status == "many_to_one", "many_to_one: stats id claimed by another ESPN athlete",
      default = "no_game_evidence"
    )
  )]

  # Stats attributes: the season roster first (jersey/position as of the
  # roster), else the latest box-score row for that player.
  nb_attr <- unique(nb[, .(nba_player_id, nba_player_name, nba_jersey_num, nba_position)], by = "nba_player_id", fromLast = TRUE)
  ro <- src$stats_rosters %||% data.table::data.table()
  if (nrow(ro)) {
    ro_attr <- unique(ro[!is.na(player_id), .(nba_player_id = as.character(player_id),
                                              r_name = as.character(player), r_num = trimws(as.character(num)),
                                              r_pos = as.character(position))], by = "nba_player_id", fromLast = TRUE)
    nb_attr <- merge(nb_attr, ro_attr, by = "nba_player_id", all.x = TRUE)
    nb_attr[, `:=`(
      nba_jersey_num = data.table::fifelse(!is.na(r_num) & nzchar(r_num), r_num, nba_jersey_num),
      nba_position = data.table::fifelse(!is.na(r_pos) & nzchar(r_pos), r_pos, nba_position)
    )][, c("r_name", "r_num", "r_pos") := NULL]
  }
  rows <- merge(rows, nb_attr, by = "nba_player_id", all.x = TRUE)

  team_map <- unique(link[, .N, by = .(espn_team_id, nba_team_id)][order(-N)], by = "espn_team_id")
  list(
    rows = rows,
    team_map = team_map[, .(espn_team_id, nba_team_id)],
    complete = any(src$espn_box$season_type == 3L, na.rm = TRUE),
    stats = list(
      espn_team_games = nrow(unique(es[, .(espn_game_id, espn_team_id)])),
      linked_team_games = nrow(link),
      player_games = nrow(el), matched_by_name = nrow(m_name), matched_by_statline = nrow(m_stat)
    )
  )
}

# Season pool of Stats players: name_key -> nba_player_id (+ team for ties).
xw_name_pool <- function(src_list) {
  parts <- lapply(src_list, function(src) {
    out <- list()
    if (!is.null(src$stats_box) && nrow(src$stats_box)) {
      out[[1]] <- src$stats_box[!is.na(person_id), .(
        nba_player_id = as.character(person_id), nba_team_id = as.character(team_id),
        nba_player_name = trimws(paste(first_name, family_name)))]
    }
    if (!is.null(src$stats_rosters) && nrow(src$stats_rosters)) {
      out[[2]] <- src$stats_rosters[!is.na(player_id), .(
        nba_player_id = as.character(player_id), nba_team_id = as.character(team_id),
        nba_player_name = as.character(player))]
    }
    data.table::rbindlist(out)
  })
  pool <- unique(data.table::rbindlist(parts))
  if (!nrow(pool)) return(data.table::data.table(name_key = character(), nba_player_id = character(), nba_team_id = character()))
  pool[, name_key := .xw_norm(nba_player_name)]
  unique(pool[nzchar(name_key), .(name_key, nba_player_id, nba_team_id)])
}

# Step 6: fill open rows from the pool. `rows` needs espn_team_id, player_name
# (normalized), nba_player_id, match_method, match_confidence, match_keys.
xw_fill_from_pool <- function(rows, pool, team_map = NULL) {
  rows <- data.table::as.data.table(rows)
  if (!nrow(pool)) return(rows)
  taken <- unique(rows[!is.na(nba_player_id), nba_player_id])
  open <- which(is.na(rows$nba_player_id) & nzchar(rows$player_name))
  tm <- if (!is.null(team_map) && nrow(team_map)) stats::setNames(team_map$nba_team_id, team_map$espn_team_id) else character()
  picks <- rep(NA_character_, length(open))
  for (k in seq_along(open)) {
    i <- open[k]
    cand <- pool[name_key == rows$player_name[i]]
    ids <- unique(cand$nba_player_id)
    if (length(ids) > 1L) {
      t <- tm[as.character(rows$espn_team_id[i])]
      if (!is.na(t)) ids <- unique(cand[nba_team_id == t, nba_player_id])
    }
    if (length(ids) == 1L) picks[k] <- ids
  }
  # Never hand one Stats id to two athletes: drop ids already taken or picked twice.
  dup <- picks %in% c(taken, picks[duplicated(picks) & !is.na(picks)])
  picks[dup] <- NA_character_
  hit <- !is.na(picks)
  rows[open[hit], `:=`(
    nba_player_id = picks[hit], match_method = "season_name",
    match_confidence = 1, match_keys = "season pool: unique normalized name"
  )]
  rows
}
