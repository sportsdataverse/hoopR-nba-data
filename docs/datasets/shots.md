# `shots`

`shots` reshaper -- release tag [`espn_nba_shots`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_shots).

| | |
|---|---|
| **Builder** | [`python/espn_nba_15_shots_creation.py`](../../python/espn_nba_15_shots_creation.py) |
| **Release tag** | [`espn_nba_shots`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_shots) |
| **File stem** | `shots_{season}.{parquet,csv,rds}` |
| **Manifested** | yes |
| **Last published** | 2026-11-13 (newest release asset) |
| **Tag created** | 2026-05-30 |
| **Release assets** | 83 |

## Load it

| | |
|---|---|
| **R** | `hoopR::load_nba_shots()` |
| **Python** | `sportsdataverse.nba.load_nba_shots()` |

## Join keys

| column | joins to |
|---|---|
| `game_id` | ESPN game id. `schedules` has one row per game; `pbp`, `shots`, `team_box`, `player_box`, `game_rosters` and `officials` key on it. `schedule_crosswalk.espn_game_id` maps it to NBA Stats / Fox / Yahoo game ids. |
| `team_id` | ESPN team id. Shared by every ESPN dataset here; `team_crosswalk.espn_team_id` maps it to NBA Stats / Fox / Yahoo team ids. |

## Automation

`.github/workflows/daily_nba.yml` -- cron 07:00 UTC in season, plus `repository_dispatch` from `hoopR-nba-raw`, running `scripts/daily_nba_data_processor.sh` (the single entrypoint). `-l python` is the default and builds via `nba_data_build`; `-l R` is the retained rollback path over the R creation scripts. The workflow sets `SDV_SKIP_CROSSWALKS=1`: the three crosswalks are built on the droplet by `scripts/cron_crosswalks.sh`.

## Columns

| col_name | type | description |
|---|---|---|
| `game_id` | Int32 | |
| `season` | Int32 | |
| `period_number` | Int32 | |
| `clock_display_value` | String | |
| `team_id` | Int32 | |
| `athlete_id_1` | Int32 | |
| `athlete_id_2` | Int32 | |
| `type_id` | Int32 | |
| `type_text` | String | |
| `scoring_play` | Boolean | |
| `score_value` | Int32 | |
| `coordinate_x` | Float64 | |
| `coordinate_y` | Float64 | |
| `coordinate_x_raw` | Float64 | |
| `coordinate_y_raw` | Float64 | |
| `athlete_name_1` | String | |
| `athlete_name_2` | String | |
| `team_name` | String | |
| `team_mascot` | String | |
| `team_abbrev` | String | |

## Coverage

| season | rows |
|---:|---:|
| 2002 | 104,335 |
| 2003 | 215,368 |
| 2004 | 260,541 |
| 2005 | 274,226 |
| 2006 | 275,084 |
| 2007 | 273,222 |
| 2008 | 276,582 |
| 2009 | 277,203 |
| 2010 | 277,751 |
| 2011 | 275,611 |
| 2012 | 223,088 |
| 2013 | 272,668 |
| 2014 | 279,947 |
| 2015 | 278,903 |
| 2016 | 279,927 |
| 2017 | 284,186 |
| 2018 | 282,091 |
| 2019 | 295,068 |
| 2020 | 254,613 |
| 2021 | 258,293 |
| 2022 | 290,577 |
| 2023 | 294,914 |
| 2024 | 291,139 |
| 2025 | 292,933 |
| 2026 | 298,411 |
| 2027 | 39,428 |
