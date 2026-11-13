# `team_season_stats`

`team_season_stats` reshaper -- release tag [`espn_nba_team_season_stats`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_team_season_stats).

| | |
|---|---|
| **Builder** | [`python/espn_nba_06_team_season_stats_creation.py`](../../python/espn_nba_06_team_season_stats_creation.py) |
| **Release tag** | [`espn_nba_team_season_stats`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_team_season_stats) |
| **File stem** | `team_season_stats_{season}.{parquet,csv,rds}` |
| **Manifested** | yes |
| **Last published** | 2026-11-13 (newest release asset) |
| **Tag created** | 2026-05-30 |
| **Release assets** | 107 |

## Load it

| | |
|---|---|
| **R** | `hoopR::load_nba_team_stats()` |
| **Python** | `sportsdataverse.nba.load_nba_team_season_stats()` |

## Join keys

| column | joins to |
|---|---|
| `team_id` | ESPN team id. Shared by every ESPN dataset here; `team_crosswalk.espn_team_id` maps it to NBA Stats / Fox / Yahoo team ids. |

## Automation

`.github/workflows/daily_nba.yml` -- cron 07:00 UTC in season, plus `repository_dispatch` from `hoopR-nba-raw`, running `scripts/daily_nba_data_processor.sh` (the single entrypoint). `-l python` is the default and builds via `nba_data_build`; `-l R` is the retained rollback path over the R creation scripts. The workflow sets `SDV_SKIP_CROSSWALKS=1`: the three crosswalks are built on the droplet by `scripts/cron_crosswalks.sh`.

## Columns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | |
| `team_id` | Int32 | |
| `team_slug` | String | |
| `team_abbreviation` | String | |
| `team_display_name` | String | |
| `team_short_display_name` | String | |
| `team_color` | String | |
| `team_alternate_color` | String | |
| `team_logo` | String | |
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
| 1994 | 1,242 |
| 1995 | 1,242 |
| 1996 | 1,334 |
| 1997 | 1,334 |
| 1998 | 1,334 |
| 1999 | 1,334 |
| 2000 | 1,334 |
| 2001 | 1,334 |
| 2002 | 1,334 |
| 2003 | 1,334 |
| 2004 | 1,334 |
| 2005 | 1,380 |
| 2006 | 1,380 |
| 2007 | 1,380 |
| 2008 | 1,380 |
| 2009 | 1,380 |
| 2010 | 1,380 |
| 2011 | 1,380 |
| 2012 | 1,380 |
| 2013 | 1,380 |
| 2014 | 1,380 |
| 2015 | 1,380 |
| 2016 | 1,380 |
| 2017 | 1,380 |
| 2018 | 1,380 |
| 2019 | 1,380 |
| 2020 | 1,380 |
| 2021 | 1,380 |
| 2022 | 1,380 |
| 2023 | 1,380 |
| 2024 | 1,380 |
| 2025 | 1,380 |
| 2026 | 1,380 |
| 2027 | 1,380 |
