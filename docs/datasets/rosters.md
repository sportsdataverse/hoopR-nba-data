# `rosters`

`rosters` reshaper -- release tag [`espn_nba_rosters`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_rosters).

| | |
|---|---|
| **Builder** | [`python/espn_nba_04_rosters_creation.py`](../../python/espn_nba_04_rosters_creation.py) |
| **Release tag** | [`espn_nba_rosters`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_rosters) |
| **File stem** | `rosters_{season}.{parquet,csv,rds}` |
| **Manifested** | yes |
| **Last published** | 2026-11-13 (newest release asset) |
| **Tag created** | 2026-05-30 |
| **Release assets** | 14 |

## Load it

| | |
|---|---|
| **R** | `hoopR::load_nba_rosters()` |
| **Python** | `sportsdataverse.nba.load_nba_rosters()` |

## Join keys

| column | joins to |
|---|---|
| `team_id` | ESPN team id. Shared by every ESPN dataset here; `team_crosswalk.espn_team_id` maps it to NBA Stats / Fox / Yahoo team ids. |
| `athlete_id` | ESPN athlete id. `player_core` holds identity + bio; `player_box`, `rosters`, `player_season_stats`, `game_rosters` and `draft` key on it. `player_crosswalk.espn_athlete_id` maps it to NBA Stats / Fox / Yahoo player ids. |

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
| `athlete_id` | String | |
| `uid` | String | |
| `guid` | String | |
| `full_name` | String | |
| `display_name` | String | |
| `short_name` | String | |
| `first_name` | String | |
| `last_name` | String | |
| `jersey` | String | |
| `position_abbreviation` | String | |
| `position_name` | String | |
| `position_id` | String | |
| `height` | String | |
| `weight` | String | |
| `age` | String | |
| `date_of_birth` | String | |
| `birth_place_city` | String | |
| `birth_place_state` | String | |
| `birth_place_country` | String | |
| `experience_years` | String | |
| `experience_display_value` | String | |
| `headshot_href` | String | |
| `headshot_alt` | String | |
| `link_web` | String | |
| `status_id` | String | |
| `status_name` | String | |
| `status_type` | String | |

## Coverage

| season | rows |
|---:|---:|
| 2025 | 537 |
| 2026 | 537 |
| 2027 | 522 |
