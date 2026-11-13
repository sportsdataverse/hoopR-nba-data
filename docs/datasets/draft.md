# `draft`

`draft` reshaper -- release tag [`espn_nba_draft`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_draft).

| | |
|---|---|
| **Builder** | [`python/espn_nba_08_draft_creation.py`](../../python/espn_nba_08_draft_creation.py) |
| **Release tag** | [`espn_nba_draft`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_draft) |
| **File stem** | `draft_{season}.{parquet,csv,rds}` |
| **Manifested** | yes |
| **Last published** | 2026-10-11 (newest release asset) |
| **Tag created** | 2026-05-30 |
| **Release assets** | 80 |

## Load it

| | |
|---|---|
| **R** | `hoopR::load_nba_draft()` |
| **Python** | `sportsdataverse.nba.load_nba_draft()` |

## Join keys

| column | joins to |
|---|---|
| `athlete_id` | ESPN athlete id. `player_core` holds identity + bio; `player_box`, `rosters`, `player_season_stats`, `game_rosters` and `draft` key on it. `player_crosswalk.espn_athlete_id` maps it to NBA Stats / Fox / Yahoo player ids. |
| `team_id` | ESPN team id. Shared by every ESPN dataset here; `team_crosswalk.espn_team_id` maps it to NBA Stats / Fox / Yahoo team ids. |

## Automation

`.github/workflows/daily_nba.yml` -- cron 07:00 UTC in season, plus `repository_dispatch` from `hoopR-nba-raw`, running `scripts/daily_nba_data_processor.sh` (the single entrypoint). `-l python` is the default and builds via `nba_data_build`; `-l R` is the retained rollback path over the R creation scripts. The workflow sets `SDV_SKIP_CROSSWALKS=1`: the three crosswalks are built on the droplet by `scripts/cron_crosswalks.sh`.

## Columns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | |
| `round` | Int32 | |
| `round_display_name` | String | |
| `pick` | Int32 | |
| `overall_pick` | Int32 | |
| `pick_traded` | String | |
| `pick_notes` | String | |
| `athlete_id` | Int32 | |
| `athlete_uid` | String | |
| `athlete_guid` | String | |
| `athlete_first_name` | String | |
| `athlete_last_name` | String | |
| `athlete_full_name` | String | |
| `athlete_display_name` | String | |
| `athlete_short_name` | String | |
| `athlete_height` | String | |
| `athlete_weight` | String | |
| `athlete_position_abbreviation` | String | |
| `athlete_position_name` | String | |
| `athlete_headshot_href` | String | |
| `college_id` | Int32 | |
| `college_name` | String | |
| `college_short_name` | String | |
| `college_abbreviation` | String | |
| `team_id` | Int32 | |
| `team_uid` | String | |
| `team_slug` | String | |
| `team_location` | String | |
| `team_name` | String | |
| `team_abbreviation` | String | |
| `team_display_name` | String | |
| `team_short_display_name` | String | |
| `team_color` | String | |
| `team_alternate_color` | String | |
| `team_logo` | String | |

## Coverage

| season | rows |
|---:|---:|
| 2023 | 60 |
| 2025 | 60 |
| 2026 | 60 |
| 2027 | 60 |
