# nba_data_build — Python season-assembly build

A polars port of the `R/espn_nba_*_creation.R` compile. It reshapes the sibling
[`hoopR-nba-raw`](https://github.com/sportsdataverse/hoopR-nba-raw) per-game JSON
into season-level parquet/csv + a manifest and (optionally) uploads them to the
`espn_nba_*` release tags on
[`sportsdataverse-data`](https://github.com/sportsdataverse/sportsdataverse-data).

**Every reshape delegates to a `sportsdataverse.nba` release producer** — this
package is the season-assembly loop + I/O + publish glue, not the parsing. The R
scripts remain the byte-parity oracle.

```
hoopR-nba-raw ──(per-game JSON)──▶ nba_data_build ──(release upload)──▶ sportsdataverse-data ──▶ hoopR::load_nba_*()
```

This is a **parallel implementation** — the R pipeline (`scripts/daily_nba_R_processor.sh`)
still drives daily CI. Wiring this package into CI is a separate cutover.

## Run

```sh
# from python/ ; needs uv (https://docs.astral.sh/uv/)
uv sync
uv run python -m nba_data_build --dataset pbp -s 2025 -e 2025            # build one dataset locally
uv run python -m nba_data_build --dataset draft -s 2025 -e 2025 --dry-run   # build + show what would publish
uv run python -m nba_data_build --dataset draft -s 2025 -e 2025 --publish   # build + upload release assets
uv run pytest -q                                                        # full-frame parity vs the committed R oracles
```

- `--dataset` — one of the registry keys (`pbp`, `team_box`, `player_box`,
  `schedules`, `shots`, `rosters`, `player_season_stats`, `team_season_stats`,
  `standings`, `draft`, `game_rosters`, `officials`). NBA **has** a `draft`
  dataset; unlike WNBA it runs daily rather than annually.
- `-s`/`-e` — inclusive season range (end year: `2025` = the 2024-25 season).
- `--raw-root` — sibling `hoopR-nba-raw` checkout, or an HTTP base URL
  (`HOOPR_NBA_RAW_ROOT` env is the default; a base URL reads over HTTP).
- `--publish` uploads per-file with `--clobber` (never delete-then-recreate,
  which would open a 404 window for live loaders). `--dry-run` builds without
  uploading.

## Dependency

`sportsdataverse` is pinned to git `main` in `pyproject.toml` (`[tool.uv.sources]`)
— the `helper_nba_*` producers merged there in sdv-py #256.

## Parity

`tests/nba_data_build/test_parity_*.py` build each dataset for a real season into
a temp dir and assert full-frame equality against the committed R oracle parquet
under `nba/<dataset>/parquet/`. The build never writes into the committed `nba/`
tree during tests.

Notes:
- `pbp` season-postprocess appends an all-null `type_abbreviation` column when a
  season ships none (matches R's `NA_character_` fallback). One row of season
  2025 (game 401703391 play 443) is a strict `xfail` — a raw re-scrape drift
  (`type_abbreviation="NA"` in the payload vs null in the oracle), not a producer
  bug.
- `player_season_stats` identity comes from the built `player_box` parquet
  (`build_nba_player_identity_lookup`), iterating the identity-lookup athlete ids
  (R's `names(identity_lookup)`), so pss builds after player_box.
- `officials` are projected from the `game_rosters` sidecar's `gameInfo.officials`
  (there is no `nba/officials/` raw dir). NBA raw coverage is complete
  (2002–present), so — unlike MBB — no `json/final` backfill fallback is needed.
