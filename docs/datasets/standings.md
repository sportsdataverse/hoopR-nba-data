# `standings`

`standings` reshaper -- release tag [`espn_nba_standings`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_standings).

| | |
|---|---|
| **Builder** | [`python/espn_nba_07_standings_creation.py`](../../python/espn_nba_07_standings_creation.py) |
| **Release tag** | [`espn_nba_standings`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_standings) |
| **File stem** | `standings_{season}.{parquet,csv,rds}` |
| **Manifested** | yes |
| **Last published** | 2026-11-13 (newest release asset) |
| **Tag created** | 2026-05-30 |
| **Release assets** | 83 |

## Load it

| | |
|---|---|
| **R** | `hoopR::load_nba_standings()` |
| **Python** | `sportsdataverse.nba.load_nba_standings()` |

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
| `group_id` | String | |
| `group_name` | String | |
| `group_abbreviation` | String | |
| `group_short_name` | String | |
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
| `stat_name` | String | |
| `stat_display_name` | String | |
| `stat_short_display_name` | String | |
| `stat_description` | String | |
| `stat_abbreviation` | String | |
| `stat_type` | String | |
| `display_value` | String | |
| `value` | Float64 | |

## Coverage

| season | rows |
|---:|---:|
| 2023 | 690 |
| 2025 | 690 |
| 2026 | 690 |
| 2027 | 660 |
