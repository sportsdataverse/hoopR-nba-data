# `team_box`

`team_box` reshaper -- release tag [`espn_nba_team_boxscores`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_team_boxscores).

| | |
|---|---|
| **Builder** | [`python/espn_nba_02_team_box_creation.py`](../../python/espn_nba_02_team_box_creation.py) |
| **Release tag** | [`espn_nba_team_boxscores`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_team_boxscores) |
| **File stem** | `team_box_{season}.{parquet,csv,rds}` |
| **Manifested** | no (no load_nba_*_manifest() loader yet) |
| **Last published** | 2026-11-13 (newest release asset) |
| **Tag created** | 2023-03-29 |
| **Release assets** | 106 |

## Load it

| | |
|---|---|
| **R** | `hoopR::load_nba_team_box()` |
| **Python** | `sportsdataverse.nba.load_nba_team_boxscore()` |

## Join keys

| column | joins to |
|---|---|
| `game_id` | ESPN game id. `schedules` has one row per game; `pbp`, `shots`, `team_box`, `player_box`, `game_rosters` and `officials` key on it. `schedule_crosswalk.espn_game_id` maps it to NBA Stats / Fox / Yahoo game ids. |
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
| `team_id` | Int32 | |
| `team_uid` | String | |
| `team_slug` | String | |
| `team_location` | String | |
| `team_name` | String | |
| `team_abbreviation` | String | |
| `team_display_name` | String | |
| `team_short_display_name` | String | |
| `team_color` | String | |
| `team_alternate_color` | String | |
| `team_logo` | String | |
| `team_home_away` | String | |
| `team_score` | Int32 | |
| `team_winner` | Boolean | |
| `assists` | Int32 | |
| `blocks` | Int32 | |
| `defensive_rebounds` | Int32 | |
| `fast_break_points` | String | |
| `field_goal_pct` | Float64 | |
| `field_goals_made` | Int32 | |
| `field_goals_attempted` | Int32 | |
| `flagrant_fouls` | Int32 | |
| `fouls` | Int32 | |
| `free_throw_pct` | Float64 | |
| `free_throws_made` | Int32 | |
| `free_throws_attempted` | Int32 | |
| `largest_lead` | String | |
| `lead_changes` | String | |
| `lead_percentage` | String | |
| `offensive_rebounds` | Int32 | |
| `points_in_paint` | String | |
| `steals` | Int32 | |
| `team_turnovers` | Int32 | |
| `technical_fouls` | Int32 | |
| `three_point_field_goal_pct` | Float64 | |
| `three_point_field_goals_made` | Int32 | |
| `three_point_field_goals_attempted` | Int32 | |
| `total_rebounds` | Int32 | |
| `total_technical_fouls` | Int32 | |
| `total_turnovers` | Int32 | |
| `turnover_points` | String | |
| `turnovers` | Int32 | |
| `opponent_team_id` | Int32 | |
| `opponent_team_uid` | String | |
| `opponent_team_slug` | String | |
| `opponent_team_location` | String | |
| `opponent_team_name` | String | |
| `opponent_team_abbreviation` | String | |
| `opponent_team_display_name` | String | |
| `opponent_team_short_display_name` | String | |
| `opponent_team_color` | String | |
| `opponent_team_alternate_color` | String | |
| `opponent_team_logo` | String | |
| `opponent_team_score` | Int32 | |

## Coverage

| season | rows |
|---:|---:|
| 1994 | 2,358 |
| 1995 | 2,358 |
| 1996 | 2,346 |
| 1997 | 2,500 |
| 1998 | 2,510 |
| 1999 | 1,582 |
| 2000 | 3,372 |
| 2001 | 2,500 |
| 2002 | 2,520 |
| 2003 | 2,554 |
| 2004 | 2,542 |
| 2005 | 2,628 |
| 2006 | 2,638 |
| 2007 | 2,618 |
| 2008 | 2,632 |
| 2009 | 2,630 |
| 2010 | 2,624 |
| 2011 | 2,622 |
| 2012 | 2,148 |
| 2013 | 2,620 |
| 2014 | 2,638 |
| 2015 | 2,624 |
| 2016 | 2,634 |
| 2017 | 2,618 |
| 2018 | 2,626 |
| 2019 | 2,628 |
| 2020 | 2,290 |
| 2021 | 2,332 |
| 2022 | 2,648 |
| 2023 | 2,642 |
| 2024 | 2,640 |
| 2025 | 2,648 |
| 2026 | 2,652 |
| 2027 | 348 |
