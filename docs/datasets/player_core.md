# `player_core`

`player_core` reshaper -- release tag [`espn_nba_player_core`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_player_core).

| | |
|---|---|
| **Builder** | [`python/espn_nba_16_player_core_creation.py`](../../python/espn_nba_16_player_core_creation.py) |
| **Release tag** | [`espn_nba_player_core`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_player_core) |
| **File stem** | `player_core_{season}.{parquet,csv,rds}` |
| **Manifested** | no (no load_nba_*_manifest() loader yet) |
| **Last published** | 2026-11-13 (newest release asset) |
| **Tag created** | 2026-07-17 |
| **Release assets** | 106 |

## Load it

| | |
|---|---|
| **R** | `hoopR::load_nba_player_core()` |
| **Python** | `sportsdataverse.nba.load_nba_player_core()` |

## Join keys

| column | joins to |
|---|---|
| `athlete_id` | ESPN athlete id. `player_core` holds identity + bio; `player_box`, `rosters`, `player_season_stats`, `game_rosters` and `draft` key on it. `player_crosswalk.espn_athlete_id` maps it to NBA Stats / Fox / Yahoo player ids. |
| `current_team_id` | ESPN team id; the same id in every ESPN dataset here (joins `team_id`). |

## Automation

`.github/workflows/daily_nba.yml` -- cron 07:00 UTC in season, plus `repository_dispatch` from `hoopR-nba-raw`, running `scripts/daily_nba_data_processor.sh` (the single entrypoint). `-l python` is the default and builds via `nba_data_build`; `-l R` is the retained rollback path over the R creation scripts. The workflow sets `SDV_SKIP_CROSSWALKS=1`: the three crosswalks are built on the droplet by `scripts/cron_crosswalks.sh`.

## Columns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | |
| `athlete_id` | Int64 | |
| `guid` | String | |
| `uid` | String | |
| `slug` | String | |
| `type` | String | |
| `first_name` | String | |
| `last_name` | String | |
| `full_name` | String | |
| `display_name` | String | |
| `short_name` | String | |
| `height` | Float64 | |
| `display_height` | String | |
| `weight` | Float64 | |
| `display_weight` | String | |
| `age` | Int32 | |
| `date_of_birth` | String | |
| `birth_city` | String | |
| `birth_state` | String | |
| `birth_country` | String | |
| `jersey` | String | |
| `position_id` | Int32 | |
| `position_name` | String | |
| `position_abbreviation` | String | |
| `position_display_name` | String | |
| `college_id` | Int32 | |
| `current_team_id` | Int32 | |
| `headshot_href` | String | |
| `experience_years` | Int32 | |
| `status_id` | Int32 | |
| `status_name` | String | |
| `status_type` | String | |
| `draft_year` | Int32 | |
| `draft_round` | Int32 | |
| `draft_selection` | Int32 | |
| `active` | Boolean | |

## Coverage

| season | rows |
|---:|---:|
| 1994 | 405 |
| 1995 | 409 |
| 1996 | 432 |
| 1997 | 446 |
| 1998 | 441 |
| 1999 | 446 |
| 2000 | 524 |
| 2001 | 443 |
| 2002 | 443 |
| 2003 | 430 |
| 2004 | 458 |
| 2005 | 469 |
| 2006 | 470 |
| 2007 | 469 |
| 2008 | 468 |
| 2009 | 459 |
| 2010 | 466 |
| 2011 | 467 |
| 2012 | 483 |
| 2013 | 479 |
| 2014 | 490 |
| 2015 | 501 |
| 2016 | 479 |
| 2017 | 490 |
| 2018 | 551 |
| 2019 | 546 |
| 2020 | 538 |
| 2021 | 544 |
| 2022 | 623 |
| 2023 | 549 |
| 2024 | 589 |
| 2025 | 580 |
| 2026 | 591 |
| 2027 | 484 |
