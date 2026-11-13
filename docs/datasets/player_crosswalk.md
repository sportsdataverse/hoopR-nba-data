# `player_crosswalk`

`player_crosswalk` reshaper -- release tag [`nba_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_crosswalk).

| | |
|---|---|
| **Builder** | [`python/espn_nba_13_player_crosswalk_creation.py`](../../python/espn_nba_13_player_crosswalk_creation.py) |
| **Release tag** | [`nba_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_crosswalk) |
| **File stem** | `nba_player_crosswalk_{season}.{parquet,csv,rds}` |
| **Manifested** | yes |
| **Last published** | 2026-11-12 (newest release asset) |
| **Tag created** | 2026-06-13 |
| **Release assets** | 241 |

## Load it

| | |
|---|---|
| **R** | `hoopR::load_nba_player_crosswalk()` |
| **Python** | `sportsdataverse.nba.load_nba_player_crosswalk()` |

## Join keys

| column | joins to |
|---|---|
| `espn_team_id` | ESPN team id; the same id in every ESPN dataset here (joins `team_id`). |
| `espn_athlete_id` | ESPN athlete id; joins `athlete_id`. |
| `nba_player_id` | NBA Stats API id; joins the `nba_stats_*` releases (hoopR-nba-stats-data). |
| `fox_athlete_id` | Fox Sports id. |
| `yahoo_player_id` | Yahoo Sports id. |

## Automation

`.github/workflows/daily_nba.yml` -- cron 07:00 UTC in season, plus `repository_dispatch` from `hoopR-nba-raw`, running `scripts/daily_nba_data_processor.sh` (the single entrypoint). `-l python` is the default and builds via `nba_data_build`; `-l R` is the retained rollback path over the R creation scripts. The workflow sets `SDV_SKIP_CROSSWALKS=1`: the three crosswalks are built on the droplet by `scripts/cron_crosswalks.sh`.

## Columns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | |
| `espn_team_id` | Int32 | |
| `team_abbreviation` | String | |
| `player_name` | String | |
| `espn_athlete_id` | String | |
| `espn_full_name` | String | |
| `espn_jersey` | String | |
| `espn_position` | String | |
| `nba_player_id` | String | |
| `nba_player_name` | String | |
| `nba_jersey_num` | String | |
| `nba_position` | String | |
| `fox_athlete_id` | String | |
| `fox_player` | String | |
| `fox_jersey` | String | |
| `fox_position_group` | String | |
| `yahoo_player_id` | String | |
| `yahoo_player_name` | String | |
| `match_method` | String | |
| `match_confidence` | Float64 | |
| `match_keys` | String | |

## Coverage

| season | rows |
|---:|---:|
| 2002 | 477 |
| 2003 | 458 |
| 2004 | 543 |
| 2005 | 546 |
| 2006 | 532 |
| 2007 | 501 |
| 2008 | 546 |
| 2009 | 543 |
| 2010 | 546 |
| 2011 | 565 |
| 2012 | 531 |
| 2013 | 536 |
| 2014 | 559 |
| 2015 | 594 |
| 2016 | 536 |
| 2017 | 550 |
| 2018 | 618 |
| 2019 | 641 |
| 2020 | 602 |
| 2021 | 633 |
| 2022 | 744 |
| 2023 | 621 |
| 2024 | 680 |
| 2025 | 672 |
| 2026 | 673 |
| 2027 | 490 |
