# `pbp`

`pbp` reshaper -- release tag [`espn_nba_pbp`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_pbp).

| | |
|---|---|
| **Builder** | [`python/espn_nba_01_pbp_creation.py`](../../python/espn_nba_01_pbp_creation.py) |
| **Release tag** | [`espn_nba_pbp`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_pbp) |
| **File stem** | `play_by_play_{season}.{parquet,csv,rds}` |
| **Manifested** | no (no load_nba_*_manifest() loader yet) |
| **Last published** | 2026-11-13 (newest release asset) |
| **Tag created** | 2023-03-29 |
| **Release assets** | 82 |

## Load it

| | |
|---|---|
| **R** | `hoopR::load_nba_pbp()` |
| **Python** | `sportsdataverse.nba.load_nba_pbp()` |

## Join keys

| column | joins to |
|---|---|
| `team_id` | ESPN team id. Shared by every ESPN dataset here; `team_crosswalk.espn_team_id` maps it to NBA Stats / Fox / Yahoo team ids. |
| `game_id` | ESPN game id. `schedules` has one row per game; `pbp`, `shots`, `team_box`, `player_box`, `game_rosters` and `officials` key on it. `schedule_crosswalk.espn_game_id` maps it to NBA Stats / Fox / Yahoo game ids. |
| `home_team_id` | ESPN team id; the same id in every ESPN dataset here (joins `team_id`). |
| `away_team_id` | ESPN team id; the same id in every ESPN dataset here (joins `team_id`). |

## Automation

`.github/workflows/daily_nba.yml` -- cron 07:00 UTC in season, plus `repository_dispatch` from `hoopR-nba-raw`, running `scripts/daily_nba_data_processor.sh` (the single entrypoint). `-l python` is the default and builds via `nba_data_build`; `-l R` is the retained rollback path over the R creation scripts. The workflow sets `SDV_SKIP_CROSSWALKS=1`: the three crosswalks are built on the droplet by `scripts/cron_crosswalks.sh`.

## Columns

| col_name | type | description |
|---|---|---|
| `game_play_number` | Int32 | |
| `id` | Int64 | |
| `sequence_number` | Int32 | |
| `type_id` | Int32 | |
| `type_text` | String | |
| `text` | String | |
| `away_score` | Int32 | |
| `home_score` | Int32 | |
| `period_number` | Int32 | |
| `period_display_value` | String | |
| `clock_display_value` | String | |
| `scoring_play` | Boolean | |
| `score_value` | Int32 | |
| `team_id` | Int32 | |
| `athlete_id_1` | Int32 | |
| `athlete_id_2` | Int32 | |
| `athlete_id_3` | Int32 | |
| `wallclock` | String | |
| `shooting_play` | Boolean | |
| `coordinate_x_raw` | Float64 | |
| `coordinate_y_raw` | Float64 | |
| `points_attempted` | Int32 | |
| `short_description` | String | |
| `game_id` | Int32 | |
| `season` | Int32 | |
| `season_type` | Int32 | |
| `home_team_id` | Int32 | |
| `home_team_name` | String | |
| `home_team_mascot` | String | |
| `home_team_abbrev` | String | |
| `home_team_name_alt` | String | |
| `away_team_id` | Int32 | |
| `away_team_name` | String | |
| `away_team_mascot` | String | |
| `away_team_abbrev` | String | |
| `away_team_name_alt` | String | |
| `game_spread` | Float64 | |
| `home_favorite` | Boolean | |
| `game_spread_available` | Boolean | |
| `home_team_spread` | Float64 | |
| `qtr` | Int32 | |
| `time` | String | |
| `clock_minutes` | Int32 | |
| `clock_seconds` | Float64 | |
| `home_timeout_called` | Boolean | |
| `away_timeout_called` | Boolean | |
| `half` | Int32 | |
| `game_half` | Int32 | |
| `lag_qtr` | Int32 | |
| `lead_qtr` | Int32 | |
| `lag_half` | Int32 | |
| `lead_half` | Int32 | |
| `start_quarter_seconds_remaining` | Float64 | |
| `start_half_seconds_remaining` | Float64 | |
| `start_game_seconds_remaining` | Float64 | |
| `end_quarter_seconds_remaining` | Float64 | |
| `end_half_seconds_remaining` | Float64 | |
| `end_game_seconds_remaining` | Float64 | |
| `period` | Int32 | |
| `coordinate_x` | Float64 | |
| `coordinate_y` | Float64 | |
| `game_date` | Date | |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') | |
| `score_source` | String | |
| `athlete_name_1` | String | |
| `athlete_name_2` | String | |
| `athlete_name_3` | String | |
| `type_abbreviation` | String | |

## Coverage

| season | rows |
|---:|---:|
| 2002 | 222,565 |
| 2003 | 461,933 |
| 2004 | 559,098 |
| 2005 | 586,720 |
| 2006 | 589,571 |
| 2007 | 584,903 |
| 2008 | 581,843 |
| 2009 | 582,144 |
| 2010 | 583,489 |
| 2011 | 583,384 |
| 2012 | 476,417 |
| 2013 | 577,054 |
| 2014 | 591,570 |
| 2015 | 592,731 |
| 2016 | 595,518 |
| 2017 | 596,910 |
| 2018 | 601,641 |
| 2019 | 626,302 |
| 2020 | 545,584 |
| 2021 | 545,461 |
| 2022 | 617,779 |
| 2023 | 620,855 |
| 2024 | 614,447 |
| 2025 | 624,997 |
| 2026 | 642,472 |
| 2027 | 87,341 |
