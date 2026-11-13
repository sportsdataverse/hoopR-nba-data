# `game_rosters`

`game_rosters` reshaper -- release tag [`espn_nba_game_rosters`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_game_rosters).

| | |
|---|---|
| **Builder** | [`python/espn_nba_09_game_rosters_creation.py`](../../python/espn_nba_09_game_rosters_creation.py) |
| **Release tag** | [`espn_nba_game_rosters`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_game_rosters) |
| **File stem** | `game_rosters_{season}.{parquet,csv,rds}` |
| **Manifested** | yes |
| **Last published** | 2026-11-13 (newest release asset) |
| **Tag created** | 2026-05-30 |
| **Release assets** | 107 |

## Load it

| | |
|---|---|
| **R** | `hoopR::load_nba_game_rosters()` |
| **Python** | `sportsdataverse.nba.load_nba_game_rosters()` |

## Join keys

| column | joins to |
|---|---|
| `game_id` | ESPN game id. `schedules` has one row per game; `pbp`, `shots`, `team_box`, `player_box`, `game_rosters` and `officials` key on it. `schedule_crosswalk.espn_game_id` maps it to NBA Stats / Fox / Yahoo game ids. |
| `team_id` | ESPN team id. Shared by every ESPN dataset here; `team_crosswalk.espn_team_id` maps it to NBA Stats / Fox / Yahoo team ids. |
| `athlete_id` | ESPN athlete id. `player_core` holds identity + bio; `player_box`, `rosters`, `player_season_stats`, `game_rosters` and `draft` key on it. `player_crosswalk.espn_athlete_id` maps it to NBA Stats / Fox / Yahoo player ids. |

## Automation

`.github/workflows/daily_nba.yml` -- cron 07:00 UTC in season, plus `repository_dispatch` from `hoopR-nba-raw`, running `scripts/daily_nba_data_processor.sh` (the single entrypoint). `-l python` is the default and builds via `nba_data_build`; `-l R` is the retained rollback path over the R creation scripts. The workflow sets `SDV_SKIP_CROSSWALKS=1`: the three crosswalks are built on the droplet by `scripts/cron_crosswalks.sh`.

## Columns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | |
| `game_id` | String | |
| `team_id` | Int32 | |
| `team_slug` | String | |
| `team_abbreviation` | String | |
| `team_display_name` | String | |
| `home_away` | String | |
| `athlete_id` | Int32 | |
| `athlete_uid` | String | |
| `athlete_guid` | String | |
| `athlete_display_name` | String | |
| `athlete_short_name` | String | |
| `athlete_first_name` | String | |
| `athlete_last_name` | String | |
| `athlete_jersey` | String | |
| `athlete_position` | String | |
| `athlete_headshot` | String | |
| `starter` | Boolean | |
| `did_not_play` | Boolean | |
| `active` | Boolean | |
| `ejected` | Boolean | |
| `reason` | String | |

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
| 2003 | 30,894 |
| 2004 | 30,606 |
| 2005 | 31,627 |
| 2006 | 38,092 |
| 2007 | 38,317 |
| 2008 | 37,999 |
| 2009 | 37,479 |
| 2010 | 37,103 |
| 2011 | 37,771 |
| 2012 | 31,038 |
| 2013 | 33,578 |
| 2014 | 33,756 |
| 2015 | 33,578 |
| 2016 | 33,715 |
| 2017 | 33,657 |
| 2018 | 33,144 |
| 2019 | 33,010 |
| 2020 | 28,764 |
| 2021 | 31,593 |
| 2022 | 33,880 |
| 2023 | 34,057 |
| 2024 | 35,028 |
| 2025 | 35,250 |
| 2026 | 34,883 |
| 2027 | 4,670 |
