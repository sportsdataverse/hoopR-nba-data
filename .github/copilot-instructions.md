# hoopR-nba-data Copilot Instructions

## Project Context

This repo is the R-side parser/compiler stage for ESPN NBA data. It reads
per-game JSON from `hoopR-nba-raw` via `raw.githubusercontent.com`,
compiles season-level `.rds`/`.csv`/`.parquet` files under `nba/`, and
uploads them as GitHub Releases on `sportsdataverse-data`. Downstream
`hoopR::load_nba_*()` reads from those releases via piggyback URLs.

Pipeline: `ESPN -> hoopR-nba-raw -> hoopR-nba-data [HERE] -> sportsdataverse-data -> hoopR`.

Package name in `DESCRIPTION` is `hoopR.nba`; this repo is not on CRAN
and is not installed as a library by users.

## Repository Workflow

- Branch from `main`; `main` is the default and release branch.
- CI entry point: `scripts/daily_nba_R_processor.sh -s <START> -e <END> -r false`.
- Compile scripts call into `sportsdataverse/hoopR`. Fix ESPN parser bugs
  upstream there, not here.
- Don't reorganize the `nba/` output tree without aligning the matching
  `hoopR::load_nba_*()` loader and the release tag.
- Don't rename release tags on `sportsdataverse-data` —
  `hoopR::load_nba_*()` URLs are load-bearing.

## Build & Development Commands

```sh
# Full daily flow for one or more seasons
bash scripts/daily_nba_R_processor.sh -s 2026 -e 2026 -r false

# Call individual compile scripts directly
Rscript R/espn_nba_01_pbp_creation.R         -s 2026 -e 2026
Rscript R/espn_nba_02_team_box_creation.R    -s 2026 -e 2026
Rscript R/espn_nba_03_player_box_creation.R  -s 2026 -e 2026

# One-time bootstrap of release tags (rare; run only on a fresh org)
Rscript R/0000_create_hoopR_releases_init.R
```

Season is the **end year** (`2026` = 2025-26 season). All compiled
filenames embed the end year only.

Outputs written to:

- `nba/schedules/{rds,parquet}/nba_schedule_{year}.{ext}`
- `nba/pbp/{rds,parquet}/play_by_play_{year}.{ext}`
- `nba/team_box/{rds,parquet}/team_box_{year}.{ext}`
- `nba/player_box/{rds,parquet}/player_box_{year}.{ext}`

Release tags on `sportsdataverse-data`:
`espn_nba_schedules`, `espn_nba_pbp`, `espn_nba_team_boxscores`,
`espn_nba_player_boxscores`. CSV variants are release-only; local
mirror keeps `.rds` + `.parquet`.

## Code Style

- Tidyverse style: `snake_case`, 2-space indent. Pipe operator `%>%`.
- All HTTP / JSON I/O goes through `hoopR:::helper_espn_nba_pbp()` and
  similar helpers in `hoopR`. Do not add bespoke parsing here.
- CLI messaging via `cli::cli_progress_step()` /
  `cli::cli_alert_info()` — no `print()` or bare `message()`.
- Parallelism via `furrr::future_map_dfr()` over
  `future::plan("multisession")`.
- Apply `hoopR:::make_hoopR_data()` to every compiled frame before
  saving / uploading so the `hoopR_data` S3 class + timestamp metadata
  are attached.
- Wrap release uploads with `purrr::insistently()` + a
  `purrr::rate_backoff()` schedule (see existing scripts) — release API
  intermittently 503s on large pushes.

## Daily CI Workflow

`.github/workflows/daily_nba.yml` is the cron + trigger entry point.

- Cron at `0 7 UTC` daily, gated to the NBA in-season windows (late
  October through June plus early July for Summer League).
- `repository_dispatch` event type `daily_nba_data` is fired by
  `hoopR-nba-raw` after its daily push. The dispatch payload's
  `commit_message` is regex-grepped for two integers to derive
  `START_YEAR` / `END_YEAR`. The raw-side commit format `"NBA Raw Update
  (Start: 2026 End: 2026)"` is load-bearing — do not change it without
  updating the regex in the workflow.
- `workflow_dispatch` inputs: `start_year`, `end_year`.
- Empty inputs fall back to `hoopR::most_recent_nba_season()`.

The daily commit message is `"NBA Data Updated (Start: $i End: $i)"`
per season; downstream automation may parse the integers, so keep that
format.

## Cross-Repo References

- Shared conventions: <https://github.com/sportsdataverse/hoopR/blob/main/CLAUDE.md>
- R parsing helpers: <https://github.com/sportsdataverse/hoopR>
- Upstream raw cache: <https://github.com/sportsdataverse/hoopR-nba-raw>
- Release destination: <https://github.com/sportsdataverse/sportsdataverse-data>

## Conventional Commits

Use `type(scope): description`. Common types: `feat`, `fix`, `chore`,
`ci`, `docs`, `refactor`. Common scopes: `compile`, `pbp`, `team_box`,
`player_box`, `release`, `deps`. Use `type!:` or a `BREAKING CHANGE:`
footer for breaking changes (renaming release tags, changing season
conventions).

**Important: Never include AI agents or assistants (e.g., Claude,
Copilot, Cursor, GPT, Gemini) as co-authors on commits.** Omit all
`Co-Authored-By` trailers referencing AI tools. This applies whether the
change was generated, refactored, or reviewed with AI assistance — the
human author is the sole attributable contributor.
