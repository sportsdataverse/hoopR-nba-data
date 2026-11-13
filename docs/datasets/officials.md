# `officials`

`officials` reshaper -- release tag [`espn_nba_officials`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_officials).

| | |
|---|---|
| **Builder** | [`python/espn_nba_10_officials_creation.py`](../../python/espn_nba_10_officials_creation.py) |
| **Release tag** | [`espn_nba_officials`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_officials) |
| **File stem** | `officials_{season}.{parquet,csv,rds}` |
| **Manifested** | yes |
| **Last published** | 2026-11-13 (newest release asset) |
| **Tag created** | 2026-05-30 |
| **Release assets** | 107 |

## Load it

| | |
|---|---|
| **R** | `hoopR::load_nba_officials()` |
| **Python** | `sportsdataverse.nba.load_nba_officials()` |

## Join keys

| column | joins to |
|---|---|
| `game_id` | ESPN game id. `schedules` has one row per game; `pbp`, `shots`, `team_box`, `player_box`, `game_rosters` and `officials` key on it. `schedule_crosswalk.espn_game_id` maps it to NBA Stats / Fox / Yahoo game ids. |

## Automation

`.github/workflows/daily_nba.yml` -- cron 07:00 UTC in season, plus `repository_dispatch` from `hoopR-nba-raw`, running `scripts/daily_nba_data_processor.sh` (the single entrypoint). `-l python` is the default and builds via `nba_data_build`; `-l R` is the retained rollback path over the R creation scripts. The workflow sets `SDV_SKIP_CROSSWALKS=1`: the three crosswalks are built on the droplet by `scripts/cron_crosswalks.sh`.

## Columns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | |
| `game_id` | Int32 | |
| `official_full_name` | String | |
| `official_display_name` | String | |
| `official_position` | String | |
| `official_position_id` | Int32 | |
| `official_order` | Int32 | |

## Coverage

| season | rows |
|---:|---:|
| 1994 | 3,063 |
| 1995 | 3,167 |
| 1996 | 2,936 |
| 1997 | 3,548 |
| 1998 | 3,647 |
| 1999 | 2,341 |
| 2000 | 4,959 |
| 2001 | 3,722 |
| 2002 | 3,778 |
| 2003 | 3,831 |
| 2004 | 3,812 |
| 2005 | 3,931 |
| 2006 | 3,954 |
| 2007 | 3,927 |
| 2008 | 3,947 |
| 2009 | 3,667 |
| 2010 | 3,684 |
| 2011 | 3,355 |
| 2012 | 3,178 |
| 2013 | 3,946 |
| 2014 | 3,961 |
| 2015 | 3,938 |
| 2016 | 3,972 |
| 2017 | 3,970 |
| 2018 | 3,985 |
| 2019 | 3,983 |
| 2020 | 3,462 |
| 2021 | 3,429 |
| 2022 | 3,961 |
| 2023 | 3,956 |
| 2024 | 3,957 |
| 2025 | 4,042 |
| 2026 | 4,056 |
| 2027 | 522 |
