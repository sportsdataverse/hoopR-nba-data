# CLAUDE.md — hoopR-nba-data Development Guide

## Repo Overview

`hoopR.nba` (package name on `DESCRIPTION`) is the R-side parser/compiler
that turns per-game NBA JSON from
[hoopR-nba-raw](https://github.com/sportsdataverse/hoopR-nba-raw) into
season-level compiled ESPN NBA datasets and uploads them as GitHub Releases
on [sportsdataverse-data](https://github.com/sportsdataverse/sportsdataverse-data).
The package depends on `sportsdataverse/hoopR` for parsing helpers and
`ropensci/piggyback` for release uploads. This repo is not on CRAN — it is
a data-processing workspace whose job is to read raw, compile clean, and
push to releases.

The downstream `hoopR::load_nba_*()` helpers in `hoopR` read from those
releases via piggyback URLs, so the per-dataset release tags listed below
are load-bearing.

## Pipeline Position

```
ESPN APIs --[python scrape]--> hoopR-nba-raw
                                    | raw JSON
                                    v
                              hoopR-nba-data [HERE]
                                    | release upload (piggyback)
                                    v
                              sportsdataverse-data (GitHub Releases)
                                    |
                                    v
                              hoopR::load_nba_*()
```

The compile scripts pull per-game `final/{game_id}.json` from
`https://raw.githubusercontent.com/sportsdataverse/hoopR-nba-raw/main`,
extract each per-game field into its corresponding season table, and
upload `.rds` + `.csv` + `.parquet` assets to the release tags below using
`sportsdataversedata::sportsdataverse_save()` (which wraps
`piggyback::pb_upload()`).

## Build & Development Commands

The repo is driven by `scripts/daily_nba_R_processor.sh`, which calls the
three numbered R scripts in `R/` for each season in a range:

```sh
# Daily flow for a single end-year season (the CI entry point)
bash scripts/daily_nba_R_processor.sh -s 2026 -e 2026 -r false

# Range of seasons
bash scripts/daily_nba_R_processor.sh -s 2024 -e 2026 -r false

# Call individual R scripts directly when iterating
Rscript R/espn_nba_01_pbp_creation.R         -s 2026 -e 2026
Rscript R/espn_nba_02_team_box_creation.R    -s 2026 -e 2026
Rscript R/espn_nba_03_player_box_creation.R  -s 2026 -e 2026
```

**Season convention**: `-s` / `-e` are the *end year* of the season (2026
= 2025-26 season). All compiled dataset filenames embed that year:
`play_by_play_{year}.{rds,csv,parquet}`,
`nba_player_box_{year}.{rds,csv,parquet}`, etc.

The shell flag `-r` is passed through but the underlying R scripts always
rebuild the season from the raw cache (no per-game skip-if-exists).

## Repo Layout

```
R/
  espn_nba_01_pbp_creation.R          # Compile schedule + PBP -> nba/pbp/, espn_nba_pbp release
  espn_nba_02_team_box_creation.R     # Compile team boxscores -> nba/team_box/, espn_nba_team_boxscores
  espn_nba_03_player_box_creation.R   # Compile player boxscores -> nba/player_box/, espn_nba_player_boxscores
  0000_create_hoopR_releases_init.R   # One-time bootstrap of release tags on sportsdataverse-data
  0001_push_existing_release_data.R   # One-time backfill of historical seasons into releases
scripts/
  daily_nba_R_processor.sh            # CI entry point; loops seasons, commits + pushes
nba/                                   # Committed compiled output (one folder per dataset)
  schedules/{rds,parquet}/             # Master schedule mirror (also re-released)
  pbp/{rds,parquet}/                   # Per-season play-by-play
  team_box/{rds,parquet}/              # Per-season team boxscores
  player_box/{rds,parquet}/            # Per-season player boxscores
  betting_lines/                       # ESPN betting line snapshots
.github/workflows/daily_nba.yml       # CI cron + repository_dispatch + workflow_dispatch
```

## Compiled Datasets

| File prefix      | Local folder        | Release tag (on sportsdataverse-data) | Loader                       |
|------------------|---------------------|---------------------------------------|------------------------------|
| `nba_schedule`   | `nba/schedules/`    | `espn_nba_schedules`                  | `hoopR::load_nba_schedule()` |
| `play_by_play`   | `nba/pbp/`          | `espn_nba_pbp`                        | `hoopR::load_nba_pbp()`      |
| `team_box`       | `nba/team_box/`     | `espn_nba_team_boxscores`             | `hoopR::load_nba_team_box()` |
| `player_box`     | `nba/player_box/`   | `espn_nba_player_boxscores`           | `hoopR::load_nba_player_box()` |

Add a new compiled dataset by writing a new `R/espn_nba_0N_*.R` script,
appending the matching `nba/<key>/` subdirectory, adding the script to
`scripts/daily_nba_R_processor.sh`, and creating the release tag (one-time
via `R/0000_create_hoopR_releases_init.R`). The corresponding loader on
the `hoopR` package side (`load_nba_<key>()`) also needs a catalog entry.

## Daily CI Workflow

`.github/workflows/daily_nba.yml`:

- **Cron cadence** (UTC):
  - `0 7 18-31 10 *` — late preseason / season opener (Oct 18-31)
  - `0 7 * 11-12 *`  — regular season (Nov-Dec)
  - `0 7 * 1-6 *`    — regular season + postseason (Jan-Jun)
  - `0 7 1-12 7 *`   — Summer League (early Jul)
- **`repository_dispatch`** event type `daily_nba_data` — fired by
  `hoopR-nba-raw` after its daily push. The dispatch payload's
  `commit_message` is regex-grepped for two integers, which become
  `START_YEAR` / `END_YEAR`. The raw-side commit format `"NBA Raw Update
  (Start: 2026 End: 2026)"` is therefore load-bearing — do not change it
  without updating the regex in the `Check hoopR_nba_data_trigger for
  inputs` step.
- **`workflow_dispatch`** inputs: `start_year`, `end_year` strings.
- Empty inputs fall back to `hoopR::most_recent_nba_season()`.
- Calls `bash scripts/daily_nba_R_processor.sh -s $START_YEAR -e $END_YEAR`.

The shell script commits with `"NBA Data Updated (Start: $i End: $i)"`
per season. That message format may also be parsed by downstream
automation; keep the `Start:`/`End:` integers in the subject.

## Conventions

- **Season is end year** everywhere user-facing. `2026` means the 2025-26
  season. File names embed end year only.
- **Compile scripts must be idempotent.** Re-running for a season should
  produce byte-identical output (modulo the timestamp embedded in the S3
  class via `hoopR:::make_hoopR_data()`).
- **`.rds`, `.csv`, and `.parquet`** are uploaded for every per-season
  dataset via `sportsdataversedata::sportsdataverse_save(file_types =
  c("rds", "csv", "parquet"))`. Local `nba/` mirror keeps `.rds` +
  `.parquet`; CSV is release-only.
- **CLI messaging** uses `cli::cli_progress_step()` /
  `cli::cli_alert_info()`. No `print()` or bare `message()` for status
  updates.
- **Parallelism** is via `furrr::future_map_dfr()` over
  `future::plan("multisession")`. Configured at the top of each compile
  script.
- **Schema drift is hoopR's problem, not this repo's.** If ESPN drops or
  renames a field, the fix belongs in `sportsdataverse/hoopR`'s
  `helper_espn_nba_*()` functions. This repo only orchestrates the
  compile.
- **`hoopR:::helper_espn_nba_pbp(url)`** is the per-game parser used by
  `espn_nba_01_pbp_creation.R`. Tag-renaming or signature changes in
  hoopR must be coordinated here.

## Cross-Repo References

- Upstream raw cache: <https://github.com/sportsdataverse/hoopR-nba-raw>
- Parsing functions + loaders: <https://github.com/sportsdataverse/hoopR>
- Release destination: <https://github.com/sportsdataverse/sportsdataverse-data>
- Shared SDV conventions: <https://github.com/sportsdataverse/hoopR/blob/main/CLAUDE.md>
- Sister repos (same shape):
  <https://github.com/sportsdataverse/hoopR-mbb-data>,
  <https://github.com/sportsdataverse/wehoop-wnba-data>,
  <https://github.com/sportsdataverse/fastRhockey-nhl-data>

## Project-Specific Gotchas

- The `Remotes:` field in `DESCRIPTION` pins `sportsdataverse/hoopR`,
  `sportsdataverse/sportsdataverse-data`, and `ropensci/piggyback`. CI
  installs from those; do not add packages here that are not present in
  those upstreams or on CRAN.
- The compile scripts read raw JSON from `raw.githubusercontent.com`, not
  from a local clone of `hoopR-nba-raw`. CI race conditions between the
  raw push landing and the data compile starting are avoided by the cron
  offset (raw scrapes earlier, data compile at `0 7 UTC`) and by the
  `repository_dispatch` mechanism described above.
- Releases on `sportsdataverse-data` are append-only per season — the
  per-season asset is overwritten on re-compile, but the release tag
  itself stays put. Renaming a release tag is a breaking change to all
  downstream `hoopR::load_nba_*()` consumers.
- Each script calls `rm(list = ls())` + `gc()` on entry and rebuilds the
  full per-season frame; do not assume in-memory state survives between
  scripts in the same shell run.
- The package name in `DESCRIPTION` is `hoopR.nba` (with a dot), but no
  one imports it as a library — it's metadata for the workspace. Don't
  bother with `library(hoopR.nba)`.

## Commit Convention

Use [Conventional Commits](https://www.conventionalcommits.org/) for
hand-authored changes:

```
feat(compile): add nba_betting_lines dataset compile script
fix(compile): handle empty game_json field in postseason games
chore(deps): bump hoopR pin in DESCRIPTION Remotes
ci: align cron windows with NBA Summer League calendar
```

The **daily CI commit** uses the load-bearing umbrella format
`"NBA Data Updated (Start: <year> End: <year>)"` — do not retroactively
re-style those commits or downstream year-parsing will break.

Prefer scoped subjects (`feat(compile): ...`, `fix(pbp): ...`). Use
`type!:` or a `BREAKING CHANGE:` footer for breaking changes (renaming
release tags, changing season conventions, etc.).

**Important**: Never include AI agents or assistants (e.g., Claude,
Copilot, Cursor, GPT, Gemini) as co-authors on commits. Omit all
`Co-Authored-By` trailers referencing AI tools. This applies whether the
change was generated, refactored, or reviewed with AI assistance — the
human author is the sole attributable contributor.
