# `schedule_crosswalk`

`schedule_crosswalk` reshaper -- release tag [`nba_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_crosswalk).

| | |
|---|---|
| **Builder** | [`python/espn_nba_12_schedule_crosswalk_creation.py`](../../python/espn_nba_12_schedule_crosswalk_creation.py) |
| **Release tag** | [`nba_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_crosswalk) |
| **File stem** | `nba_schedule_crosswalk_{season}.{parquet,csv,rds}` |
| **Manifested** | yes |
| **Last published** | 2026-11-12 (newest release asset) |
| **Tag created** | 2026-06-13 |
| **Release assets** | 241 |

## Load it

| | |
|---|---|
| **R** | `hoopR::load_nba_schedule_crosswalk()` |
| **Python** | `sportsdataverse.nba.load_nba_schedule_crosswalk()` |

## Join keys

| column | joins to |
|---|---|
| `home_espn_team_id` | ESPN team id; the same id in every ESPN dataset here (joins `team_id`). |
| `away_espn_team_id` | ESPN team id; the same id in every ESPN dataset here (joins `team_id`). |
| `espn_game_id` | ESPN game id; joins `game_id`. |
| `nba_game_id` | NBA Stats API id; joins the `nba_stats_*` releases (hoopR-nba-stats-data). |
| `nba_home_team_id` | NBA Stats API id; joins the `nba_stats_*` releases (hoopR-nba-stats-data). |
| `nba_away_team_id` | NBA Stats API id; joins the `nba_stats_*` releases (hoopR-nba-stats-data). |
| `fox_game_id` | Fox Sports id. |
| `fox_home_team_id` | Fox Sports id. |
| `fox_away_team_id` | Fox Sports id. |
| `yahoo_game_id` | Yahoo Sports id. |

## Automation

`.github/workflows/daily_nba.yml` -- cron 07:00 UTC in season, plus `repository_dispatch` from `hoopR-nba-raw`, running `scripts/daily_nba_data_processor.sh` (the single entrypoint). `-l python` is the default and builds via `nba_data_build`; `-l R` is the retained rollback path over the R creation scripts. The workflow sets `SDV_SKIP_CROSSWALKS=1`: the three crosswalks are built on the droplet by `scripts/cron_crosswalks.sh`.

## Columns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | |
| `season_type` | String | |
| `game_date` | Date | |
| `home_espn_team_id` | Int32 | |
| `away_espn_team_id` | Int32 | |
| `espn_game_id` | String | |
| `nba_game_id` | String | |
| `nba_game_code` | String | |
| `nba_home_team_id` | String | |
| `nba_away_team_id` | String | |
| `fox_game_id` | String | |
| `fox_home_team_id` | String | |
| `fox_away_team_id` | String | |
| `yahoo_game_id` | String | |
| `match_method` | String | |
| `match_confidence` | Float64 | |

## Coverage

| season | rows |
|---:|---:|
| 2002 | 1,379 |
| 2003 | 1,392 |
| 2004 | 1,390 |
| 2005 | 1,469 |
| 2006 | 1,442 |
| 2007 | 1,434 |
| 2008 | 1,438 |
| 2009 | 1,432 |
| 2010 | 1,434 |
| 2011 | 1,433 |
| 2012 | 1,107 |
| 2013 | 1,434 |
| 2014 | 1,438 |
| 2015 | 1,432 |
| 2016 | 1,428 |
| 2017 | 1,414 |
| 2018 | 1,393 |
| 2019 | 1,394 |
| 2020 | 1,162 |
| 2021 | 1,253 |
| 2022 | 1,404 |
| 2023 | 1,395 |
| 2024 | 1,398 |
| 2025 | 1,405 |
| 2026 | 1,415 |
| 2027 | 1,207 |
