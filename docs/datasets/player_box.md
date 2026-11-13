# `player_box`

`player_box` reshaper -- release tag [`espn_nba_player_boxscores`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_player_boxscores).

| | |
|---|---|
| **Builder** | [`python/espn_nba_03_player_box_creation.py`](../../python/espn_nba_03_player_box_creation.py) |
| **Release tag** | [`espn_nba_player_boxscores`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_player_boxscores) |
| **File stem** | `player_box_{season}.{parquet,csv,rds}` |
| **Manifested** | no (no load_nba_*_manifest() loader yet) |
| **Last published** | 2026-11-13 (newest release asset) |
| **Tag created** | 2023-03-29 |
| **Release assets** | 106 |

## Load it

| | |
|---|---|
| **R** | `hoopR::load_nba_player_box()` |
| **Python** | `sportsdataverse.nba.load_nba_player_boxscore()` |

## Join keys

| column | joins to |
|---|---|
| `game_id` | ESPN game id. `schedules` has one row per game; `pbp`, `shots`, `team_box`, `player_box`, `game_rosters` and `officials` key on it. `schedule_crosswalk.espn_game_id` maps it to NBA Stats / Fox / Yahoo game ids. |
| `athlete_id` | ESPN athlete id. `player_core` holds identity + bio; `player_box`, `rosters`, `player_season_stats`, `game_rosters` and `draft` key on it. `player_crosswalk.espn_athlete_id` maps it to NBA Stats / Fox / Yahoo player ids. |
| `team_id` | ESPN team id. Shared by every ESPN dataset here; `team_crosswalk.espn_team_id` maps it to NBA Stats / Fox / Yahoo team ids. |
| `opponent_team_id` | ESPN team id; the same id in every ESPN dataset here (joins `team_id`). |

## Automation

`.github/workflows/daily_nba.yml` -- cron 07:00 UTC in season, plus `repository_dispatch` from `hoopR-nba-raw`, running `scripts/daily_nba_data_processor.sh` (the single entrypoint). `-l python` is the default and builds via `nba_data_build`; `-l R` is the retained rollback path over the R creation scripts. The workflow sets `SDV_SKIP_CROSSWALKS=1`: the three crosswalks are built on the droplet by `scripts/cron_crosswalks.sh`.

## Columns

| col_name | type | description |
|---|---|---|
| `game_id` | Int32 | |
| `season` | Int32 | |
| `season_type` | Int32 | |
| `game_date` | Date | |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') | |
| `athlete_id` | Int32 | |
| `athlete_display_name` | String | |
| `team_id` | Int32 | |
| `team_name` | String | |
| `team_location` | String | |
| `team_short_display_name` | String | |
| `minutes` | Float64 | |
| `field_goals_made` | Int32 | |
| `field_goals_attempted` | Int32 | |
| `three_point_field_goals_made` | Int32 | |
| `three_point_field_goals_attempted` | Int32 | |
| `free_throws_made` | Int32 | |
| `free_throws_attempted` | Int32 | |
| `offensive_rebounds` | Int32 | |
| `defensive_rebounds` | Int32 | |
| `rebounds` | Int32 | |
| `assists` | Int32 | |
| `steals` | Int32 | |
| `blocks` | Int32 | |
| `turnovers` | Int32 | |
| `fouls` | Int32 | |
| `plus_minus` | String | |
| `points` | Int32 | |
| `starter` | Boolean | |
| `ejected` | Boolean | |
| `did_not_play` | Boolean | |
| `reason` | String | |
| `active` | Boolean | |
| `athlete_jersey` | String | |
| `athlete_short_name` | String | |
| `athlete_headshot_href` | String | |
| `athlete_position_name` | String | |
| `athlete_position_abbreviation` | String | |
| `team_display_name` | String | |
| `team_uid` | String | |
| `team_slug` | String | |
| `team_logo` | String | |
| `team_abbreviation` | String | |
| `team_color` | String | |
| `team_alternate_color` | String | |
| `home_away` | String | |
| `team_winner` | Boolean | |
| `team_score` | Int32 | |
| `opponent_team_id` | Int32 | |
| `opponent_team_name` | String | |
| `opponent_team_location` | String | |
| `opponent_team_display_name` | String | |
| `opponent_team_abbreviation` | String | |
| `opponent_team_logo` | String | |
| `opponent_team_color` | String | |
| `opponent_team_alternate_color` | String | |
| `opponent_team_score` | Int32 | |

## Coverage

| season | rows |
|---:|---:|
| 1994 | 28,195 |
| 1995 | 28,219 |
| 1996 | 29,846 |
| 1997 | 29,832 |
| 1998 | 29,959 |
| 1999 | 19,283 |
| 2000 | 40,573 |
| 2001 | 29,941 |
| 2002 | 30,279 |
| 2003 | 30,870 |
| 2004 | 30,606 |
| 2005 | 31,552 |
| 2006 | 37,691 |
| 2007 | 38,234 |
| 2008 | 37,973 |
| 2009 | 37,453 |
| 2010 | 37,079 |
| 2011 | 37,717 |
| 2012 | 31,014 |
| 2013 | 33,448 |
| 2014 | 33,730 |
| 2015 | 33,578 |
| 2016 | 33,715 |
| 2017 | 33,631 |
| 2018 | 33,119 |
| 2019 | 32,985 |
| 2020 | 28,764 |
| 2021 | 31,593 |
| 2022 | 33,880 |
| 2023 | 34,057 |
| 2024 | 35,028 |
| 2025 | 35,250 |
| 2026 | 34,883 |
| 2027 | 4,670 |
