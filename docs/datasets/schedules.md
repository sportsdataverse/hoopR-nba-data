# `schedules`

`schedules` reshaper -- release tag [`espn_nba_schedules`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_schedules).

| | |
|---|---|
| **Builder** | [`python/espn_nba_14_schedules_creation.py`](../../python/espn_nba_14_schedules_creation.py) |
| **Release tag** | [`espn_nba_schedules`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_schedules) |
| **File stem** | `nba_schedule_{season}.{parquet,csv,rds}` |
| **Manifested** | no (no load_nba_*_manifest() loader yet) |
| **Last published** | 2026-11-13 (newest release asset) |
| **Tag created** | 2023-03-29 |
| **Release assets** | 112 |

## Load it

| | |
|---|---|
| **R** | `hoopR::load_nba_schedule()` |
| **Python** | `sportsdataverse.nba.load_nba_schedule()` |

## Join keys

| column | joins to |
|---|---|
| `home_id` | ESPN team id; the same id in every ESPN dataset here (joins `team_id`). |
| `away_id` | ESPN team id; the same id in every ESPN dataset here (joins `team_id`). |
| `game_id` | ESPN game id. `schedules` has one row per game; `pbp`, `shots`, `team_box`, `player_box`, `game_rosters` and `officials` key on it. `schedule_crosswalk.espn_game_id` maps it to NBA Stats / Fox / Yahoo game ids. |

## Automation

`.github/workflows/daily_nba.yml` -- cron 07:00 UTC in season, plus `repository_dispatch` from `hoopR-nba-raw`, running `scripts/daily_nba_data_processor.sh` (the single entrypoint). `-l python` is the default and builds via `nba_data_build`; `-l R` is the retained rollback path over the R creation scripts. The workflow sets `SDV_SKIP_CROSSWALKS=1`: the three crosswalks are built on the droplet by `scripts/cron_crosswalks.sh`.

## Columns

| col_name | type | description |
|---|---|---|
| `id` | Int32 | |
| `uid` | String | |
| `date` | String | |
| `attendance` | Float64 | |
| `time_valid` | Boolean | |
| `neutral_site` | Boolean | |
| `play_by_play_available` | Boolean | |
| `recent` | Boolean | |
| `start_date` | String | |
| `broadcast` | String | |
| `notes_type` | String | |
| `notes_headline` | String | |
| `broadcast_market` | String | |
| `broadcast_name` | String | |
| `type_id` | Int32 | |
| `type_abbreviation` | String | |
| `venue_id` | Int32 | |
| `venue_full_name` | String | |
| `venue_address_city` | String | |
| `venue_address_state` | String | |
| `venue_indoor` | Boolean | |
| `status_clock` | Float64 | |
| `status_display_clock` | String | |
| `status_period` | Float64 | |
| `status_type_id` | Int32 | |
| `status_type_name` | String | |
| `status_type_state` | String | |
| `status_type_completed` | Boolean | |
| `status_type_description` | String | |
| `status_type_detail` | String | |
| `status_type_short_detail` | String | |
| `format_regulation_periods` | Float64 | |
| `home_id` | Int32 | |
| `home_uid` | String | |
| `home_location` | String | |
| `home_name` | String | |
| `home_abbreviation` | String | |
| `home_display_name` | String | |
| `home_short_display_name` | String | |
| `home_color` | String | |
| `home_alternate_color` | String | |
| `home_is_active` | Boolean | |
| `home_venue_id` | Int32 | |
| `home_logo` | String | |
| `home_score` | Int32 | |
| `home_linescores` | String | |
| `home_records` | String | |
| `away_id` | Int32 | |
| `away_uid` | String | |
| `away_location` | String | |
| `away_name` | String | |
| `away_abbreviation` | String | |
| `away_display_name` | String | |
| `away_short_display_name` | String | |
| `away_color` | String | |
| `away_alternate_color` | String | |
| `away_is_active` | Boolean | |
| `away_venue_id` | Int32 | |
| `away_logo` | String | |
| `away_score` | Int32 | |
| `away_linescores` | String | |
| `away_records` | String | |
| `game_id` | Int32 | |
| `season` | Int32 | |
| `season_type` | Int32 | |
| `home_logo_dark` | String | |
| `away_logo_dark` | String | |
| `conference_competition` | Boolean | |
| `highlights` | String | |
| `home_winner` | Boolean | |
| `away_winner` | Boolean | |
| `status_type_alt_detail` | String | |
| `game_json` | Boolean | |
| `game_json_url` | String | |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') | |
| `game_date` | Date | |
| `PBP` | Boolean | |
| `team_box` | Boolean | |
| `player_box` | Boolean | |

## Coverage

| season | rows |
|---:|---:|
| 1994 | 1,185 |
| 1995 | 1,181 |
| 1996 | 1,257 |
| 1997 | 1,263 |
| 1998 | 1,261 |
| 1999 | 842 |
| 2000 | 1,827 |
| 2001 | 1,250 |
| 2002 | 1,261 |
| 2003 | 1,278 |
| 2004 | 1,272 |
| 2005 | 1,315 |
| 2006 | 1,320 |
| 2007 | 1,310 |
| 2008 | 1,317 |
| 2009 | 1,316 |
| 2010 | 1,313 |
| 2011 | 1,312 |
| 2012 | 1,075 |
| 2013 | 1,315 |
| 2014 | 1,320 |
| 2015 | 1,312 |
| 2016 | 1,317 |
| 2017 | 1,310 |
| 2018 | 1,314 |
| 2019 | 1,314 |
| 2020 | 1,157 |
| 2021 | 1,204 |
| 2022 | 1,335 |
| 2023 | 1,322 |
| 2024 | 1,322 |
| 2025 | 1,329 |
| 2026 | 1,330 |
| 2027 | 1,206 |
