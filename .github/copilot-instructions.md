# hoopR-nba-data Copilot Instructions

## Project Context

This repo is the parser/compiler stage for ESPN NBA data. It reads
per-game JSON from `hoopR-nba-raw` via `raw.githubusercontent.com`,
compiles season-level `.rds`/`.csv`/`.parquet` files under `nba/`, and
uploads them as GitHub Releases on `sportsdataverse-data`. Downstream
`hoopR::load_nba_*()` reads from those releases via piggyback URLs.

**Two pipelines, both maintained (standing policy, 2026-08-03).** Python
(`python/nba_data_build` + the numbered shims beside it) is PRIMARY and gets
the work; the R chain (`R/espn_nba_NN_*_creation.R`) is maintained as the
methodological / language equivalent. **Both move together when either
changes** — adding, renaming or removing a dataset on one side alone is a
defect, and `tests/test_r_python_parity.py` fails the build for it.

The two sides decompose differently on purpose: R is dataset-per-file, Python
is a build package with datasets as `config.REGISTRY` rows. The numbered shims
`python/espn_nba_NN_<key>_creation.py` bridge that — **each carries the same
number as its R twin**, so the stage sequence is comparable by eye and the
directory listing IS the pipeline. A shim is thin: it forces its own
`--dataset` and delegates to the package.

Known unpaired datasets (declared in the parity test, not silently tolerated):
`schedules` and `shots` are emitted inside `R/espn_nba_01_pbp_creation.R`
rather than as their own numbered stages; **`player_core` is an OPEN PARITY
GAP** — Python produces it and no R file references it.

Neither side is automatically authoritative. If the two disagree, that is a
review item: decide which pipeline is methodologically right, then update the
other. Do not "fix" the parity test by editing one side to match.

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

# Python stages (PRIMARY) — the numbered shims mirror the R numbers
uv run python python/espn_nba_01_pbp_creation.py        -s 2026 -e 2026
uv run python python/espn_nba_02_team_box_creation.py   -s 2026 -e 2026
# ...equivalently, the package CLI the shims delegate to:
uv run python -m nba_data_build --dataset pbp -s 2026 -e 2026

# R stages (maintained equivalent) — same numbers, same datasets
Rscript R/espn_nba_01_pbp_creation.R         -s 2026 -e 2026
Rscript R/espn_nba_02_team_box_creation.R    -s 2026 -e 2026
Rscript R/espn_nba_03_player_box_creation.R  -s 2026 -e 2026

# One-time bootstrap of release tags (rare; run only on a fresh org)
Rscript ops/init/0000_create_hoopR_releases_init.R
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
