# `team_crosswalk`

`team_crosswalk` reshaper -- release tag [`nba_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_crosswalk).

| | |
|---|---|
| **Builder** | [`python/espn_nba_11_team_crosswalk_creation.py`](../../python/espn_nba_11_team_crosswalk_creation.py) |
| **Release tag** | [`nba_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_crosswalk) |
| **File stem** | `nba_team_crosswalk_{season}.{parquet,csv,rds}` |
| **Manifested** | yes |
| **Last published** | 2026-11-12 (newest release asset) |
| **Tag created** | 2026-06-13 |
| **Release assets** | 241 |

## Load it

| | |
|---|---|
| **R** | `hoopR::load_nba_team_crosswalk()` |
| **Python** | `sportsdataverse.nba.load_nba_team_crosswalk()` |

## Join keys

| column | joins to |
|---|---|
| `espn_team_id` | ESPN team id; the same id in every ESPN dataset here (joins `team_id`). |
| `nba_team_id` | NBA Stats API id; joins the `nba_stats_*` releases (hoopR-nba-stats-data). |
| `fox_team_id` | Fox Sports id. |
| `yahoo_team_id` | Yahoo Sports id. |

## Automation

`.github/workflows/daily_nba.yml` -- cron 07:00 UTC in season, plus `repository_dispatch` from `hoopR-nba-raw`, running `scripts/daily_nba_data_processor.sh` (the single entrypoint). `-l python` is the default and builds via `nba_data_build`; `-l R` is the retained rollback path over the R creation scripts. The workflow sets `SDV_SKIP_CROSSWALKS=1`: the three crosswalks are built on the droplet by `scripts/cron_crosswalks.sh`.

## Columns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | |
| `espn_team_id` | Int32 | |
| `espn_abbreviation` | String | |
| `espn_display_name` | String | |
| `espn_short_name` | String | |
| `espn_location` | String | |
| `espn_mascot` | String | |
| `nba_team_id` | String | |
| `nba_team_abbreviation` | String | |
| `nba_team_name` | String | |
| `nba_team_city` | String | |
| `nba_team_slug` | String | |
| `nba_conference` | String | |
| `nba_division` | String | |
| `fox_team_id` | String | |
| `fox_team_name` | String | |
| `yahoo_team_id` | String | |
| `yahoo_team_abbreviation` | String | |
| `yahoo_team_name` | String | |
| `match_method` | String | |
| `match_confidence` | Float64 | |

## Coverage

| season | rows |
|---:|---:|
| 2002 | 29 |
| 2003 | 29 |
| 2004 | 29 |
| 2005 | 30 |
| 2006 | 30 |
| 2007 | 30 |
| 2008 | 30 |
| 2009 | 30 |
| 2010 | 30 |
| 2011 | 30 |
| 2012 | 30 |
| 2013 | 30 |
| 2014 | 30 |
| 2015 | 30 |
| 2016 | 30 |
| 2017 | 30 |
| 2018 | 30 |
| 2019 | 30 |
| 2020 | 30 |
| 2021 | 30 |
| 2022 | 30 |
| 2023 | 30 |
| 2024 | 30 |
| 2025 | 30 |
| 2026 | 30 |
| 2027 | 30 |
