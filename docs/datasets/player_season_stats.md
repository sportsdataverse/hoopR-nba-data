# `player_season_stats`

`player_season_stats` reshaper -- release tag [`espn_nba_player_season_stats`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_player_season_stats).

| | |
|---|---|
| **Builder** | [`python/espn_nba_05_player_season_stats_creation.py`](../../python/espn_nba_05_player_season_stats_creation.py) |
| **Release tag** | [`espn_nba_player_season_stats`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_player_season_stats) |
| **File stem** | `player_season_stats_{season}.{parquet,csv,rds}` |
| **Manifested** | yes |
| **Last published** | 2026-11-13 (newest release asset) |
| **Tag created** | 2026-05-30 |
| **Release assets** | 107 |

## Load it

| | |
|---|---|
| **R** | `hoopR::load_nba_player_stats()` |
| **Python** | `sportsdataverse.nba.load_nba_player_season_stats()` |

## Join keys

| column | joins to |
|---|---|
| `athlete_id` | ESPN athlete id. `player_core` holds identity + bio; `player_box`, `rosters`, `player_season_stats`, `game_rosters` and `draft` key on it. `player_crosswalk.espn_athlete_id` maps it to NBA Stats / Fox / Yahoo player ids. |
| `team_id` | ESPN team id. Shared by every ESPN dataset here; `team_crosswalk.espn_team_id` maps it to NBA Stats / Fox / Yahoo team ids. |

## Automation

`.github/workflows/daily_nba.yml` -- cron 07:00 UTC in season, plus `repository_dispatch` from `hoopR-nba-raw`, running `scripts/daily_nba_data_processor.sh` (the single entrypoint). `-l python` is the default and builds via `nba_data_build`; `-l R` is the retained rollback path over the R creation scripts. The workflow sets `SDV_SKIP_CROSSWALKS=1`: the three crosswalks are built on the droplet by `scripts/cron_crosswalks.sh`.

## Columns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | |
| `athlete_id` | Int32 | |
| `athlete_display_name` | String | |
| `athlete_position_abbreviation` | String | |
| `athlete_jersey` | String | |
| `team_id` | Int32 | |
| `team_slug` | String | |
| `team_display_name` | String | |
| `category` | String | |
| `stat_label` | String | |
| `stat_name` | String | |
| `stat_display_name` | String | |
| `stat_description` | String | |
| `display_value` | String | |
| `value` | Float64 | |

## Coverage

| season | rows |
|---:|---:|
| 1994 | 5,074 |
| 1995 | 6,106 |
| 1996 | 7,396 |
| 1997 | 9,010 |
| 1998 | 10,320 |
| 1999 | 11,987 |
| 2000 | 13,803 |
| 2001 | 15,910 |
| 2002 | 18,920 |
| 2003 | 18,384 |
| 2004 | 18,976 |
| 2005 | 19,932 |
| 2006 | 19,634 |
| 2007 | 19,654 |
| 2008 | 19,393 |
| 2009 | 19,125 |
| 2010 | 18,971 |
| 2011 | 19,391 |
| 2012 | 20,524 |
| 2013 | 20,117 |
| 2014 | 20,671 |
| 2015 | 21,136 |
| 2016 | 20,468 |
| 2017 | 20,858 |
| 2018 | 23,120 |
| 2019 | 22,675 |
| 2020 | 22,712 |
| 2021 | 23,190 |
| 2022 | 25,863 |
| 2023 | 23,132 |
| 2024 | 24,491 |
| 2025 | 24,447 |
| 2026 | 25,006 |
| 2027 | 19,810 |
