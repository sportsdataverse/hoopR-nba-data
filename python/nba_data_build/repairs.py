"""Season-level pbp repairs for upstream ESPN data defects.

Three published-data defects tracked on sportsdataverse/hoopR are repaired
here at compile time (the raw ``final.json`` payloads are stored as-scraped
and are NOT rewritten -- the repair is a build step, mirrored in
``R/espn_nba_01_pbp_creation.R``):

* **#178 -- impossible period values.** ESPN occasionally ships a play with a
  garbage period (e.g. game 400579076 carries a period-29 "25OT" row that
  belongs in period 1). Any row with ``period_number > _PERIOD_MAX`` borrows
  its period columns from the nearest valid neighbour in the same game and
  has its half/game seconds-remaining re-derived from that neighbour's
  offsets (the row's own clock-derived quarter seconds are kept).
* **#146 -- plays out of order / stale duplicate rows.** Some games (e.g.
  401616465) ship plays out of chronological order upstream, and some carry
  stale-score duplicate rows (ESPN double-recorded a stretch of the feed
  with earlier-game scores) that no reordering can fix. Two-step repair:
  games whose scores are not monotone non-decreasing in stored order are
  stable-sorted within (game, period) by descending game clock (existing
  order kept for ties) and their order-dependent columns
  (``game_play_number``, ``lag_*``/``lead_*``) recomputed; then
  ``home_score``/``away_score`` are clamped to the per-game running maximum,
  which corrects the stale rows (the true score at any instant is the
  running max) and guarantees monotone cumulative scores everywhere.
* **#140 -- pickcenter default spreads.** ESPN's pickcenter went empty for
  2024+ seasons so every game baked the ``(2.5, home favorite, unavailable)``
  default. Games with ``game_spread_available == False`` get the real
  consensus closing line joined from the committed
  ``nba/betting_lines/closing_lines_odds_api.parquet`` lookup (built by
  ``ops/oneoff/2026-08-26_build_nba_closing_lines.py`` from The Odds API
  historical backfill; ``game_spread_available`` flips to True ONLY when a
  real book line was injected). Games with no line keep the honest default.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from nba_data_build._logging import get_logger

log = get_logger()

# NBA regulation is 4 periods; the longest game ever played reached 6 OTs
# (period 10, 1951). Anything above this is upstream garbage (#178's "25OT").
_PERIOD_MAX = 10

_SPREAD_LOOKUP = Path("betting_lines") / "closing_lines_odds_api.parquet"

# Period-identity columns borrowed wholesale from the donor neighbour row.
_PERIOD_COLS = ("period_number", "period", "qtr", "period_display_value", "half", "game_half")

# (lag/lead column, source column) pairs recomputed after a reorder.
_ORDER_COLS = (
    ("lag_qtr", "qtr", 1),
    ("lead_qtr", "qtr", -1),
    ("lag_half", "half", 1),
    ("lead_half", "half", -1),
    ("lag_game_half", "game_half", 1),
    ("lead_game_half", "game_half", -1),
)


def repair_pbp_season(out: pl.DataFrame, *, base: str | Path = "nba") -> pl.DataFrame:
    """Apply the three pbp repairs (period sanity, resequencing, spreads)."""
    if out.height == 0:
        return out
    out, period_games = _repair_impossible_periods(out)
    out = _resequence_disordered_games(out, force_games=period_games)
    out = _clamp_scores_monotone(out)
    out = _inject_closing_spreads(out, base=Path(base))
    return out


def _bad_period() -> pl.Expr:
    return pl.col("period_number") > _PERIOD_MAX


def _repair_impossible_periods(out: pl.DataFrame) -> tuple[pl.DataFrame, list[int]]:
    """#178: borrow period identity + seconds offsets from the nearest valid row."""
    if "period_number" not in out.columns:
        return out, []
    bad = _bad_period()
    bad_rows = out.filter(bad)
    if bad_rows.height == 0:
        return out, []
    games = bad_rows["game_id"].unique().to_list()
    log.warning(
        "pbp repair (#178): %d impossible-period row(s) in game(s) %s -- repairing from neighbours",
        bad_rows.height,
        games,
    )
    fixes = [
        pl.when(bad)
        .then(None)
        .otherwise(pl.col(c))
        .forward_fill()
        .backward_fill()
        .over("game_id")
        .alias(f"_fix_{c}")
        for c in _PERIOD_COLS
        if c in out.columns
    ]
    sec_cols = (
        "start_quarter_seconds_remaining",
        "start_half_seconds_remaining",
        "start_game_seconds_remaining",
    )
    have_secs = all(c in out.columns for c in sec_cols)
    if have_secs:
        sq = pl.col("start_quarter_seconds_remaining").cast(pl.Float64)
        for tgt in ("start_half_seconds_remaining", "start_game_seconds_remaining"):
            fixes.append(
                pl.when(bad)
                .then(None)
                .otherwise(pl.col(tgt).cast(pl.Float64) - sq)
                .forward_fill()
                .backward_fill()
                .over("game_id")
                .alias(f"_off_{tgt}")
            )
    out = out.with_columns(fixes)
    repl = [
        pl.when(bad).then(pl.col(f"_fix_{c}")).otherwise(pl.col(c)).alias(c)
        for c in _PERIOD_COLS
        if c in out.columns
    ]
    if have_secs:
        sq = pl.col("start_quarter_seconds_remaining").cast(pl.Float64)
        for tgt in ("start_half_seconds_remaining", "start_game_seconds_remaining"):
            repl.append(
                pl.when(bad)
                .then((sq + pl.col(f"_off_{tgt}")).cast(out.schema[tgt]))
                .otherwise(pl.col(tgt))
                .alias(tgt)
            )
        # The garbage row's end_* trio is garbage too (0/0/0); pin it to the
        # repaired start_* values (a zero-duration play) rather than invent one.
        for end_c, start_c in (
            ("end_quarter_seconds_remaining", "start_quarter_seconds_remaining"),
            ("end_half_seconds_remaining", "start_half_seconds_remaining"),
            ("end_game_seconds_remaining", "start_game_seconds_remaining"),
        ):
            if end_c in out.columns:
                repl.append(
                    pl.when(bad)
                    .then(pl.col(start_c).cast(out.schema[end_c]))
                    .otherwise(pl.col(end_c))
                    .alias(end_c)
                )
    out = out.with_columns(repl).drop(
        [c for c in out.columns if c.startswith("_fix_") or c.startswith("_off_")]
    )
    return out, games


def _resequence_disordered_games(
    out: pl.DataFrame, *, force_games: list[int] | None = None
) -> pl.DataFrame:
    """#146: stable-sort disordered games by (period, clock desc), recompute order cols.

    Only games whose cumulative scores are NOT monotone non-decreasing in
    stored order (plus any period-repaired games, whose repaired row is
    misplaced by construction) are touched -- well-ordered games keep ESPN's
    original order, so legitimate clock corrections elsewhere are never
    reshuffled.
    """
    needed = {"game_id", "period_number", "home_score", "away_score"}
    if not needed.issubset(out.columns):
        return out
    viol = (pl.col("home_score").cast(pl.Int64).diff().over("game_id") < 0) | (
        pl.col("away_score").cast(pl.Int64).diff().over("game_id") < 0
    )
    games = set(out.filter(viol)["game_id"].unique().to_list()) | set(force_games or [])
    if not games:
        return out
    log.warning(
        "pbp repair (#146): %d game(s) with non-chronological plays -- resequencing %s",
        len(games),
        sorted(games)[:10],
    )
    out = out.with_row_index("_ri")
    affected = pl.col("game_id").is_in(sorted(games))
    aff = out.filter(affected)
    rest = out.filter(affected.not_())

    # Clock sort key: quarter seconds remaining, falling back to the raw clock;
    # rows with no clock inherit a neighbour's key so the stable sort keeps
    # them in place.
    ck = pl.col("start_quarter_seconds_remaining").cast(pl.Float64, strict=False)
    if "start_quarter_seconds_remaining" not in aff.columns:
        ck = pl.lit(None, dtype=pl.Float64)
    if "clock_minutes" in aff.columns and "clock_seconds" in aff.columns:
        ck = pl.coalesce(
            ck,
            pl.col("clock_minutes").cast(pl.Float64, strict=False) * 60
            + pl.col("clock_seconds").cast(pl.Float64, strict=False),
        )
    aff = aff.sort("_ri").with_columns(
        ck.forward_fill().backward_fill().over(["game_id", "period_number"]).alias("_ck"),
        pl.col("_ri").min().over("game_id").alias("_gmin"),
    )
    aff = aff.sort(
        ["_gmin", "period_number", "_ck", "_ri"],
        descending=[False, False, True, False],
        nulls_last=True,
    )
    # Each game block re-occupies its original _ri range so the season frame's
    # game order (game_date desc) is untouched.
    aff = aff.with_columns(
        (pl.col("_gmin") + pl.int_range(pl.len()).over("_gmin")).cast(pl.UInt32).alias("_ri")
    ).drop(["_ck", "_gmin"])
    out = pl.concat([rest, aff], how="vertical").sort("_ri")

    # Recompute the order-dependent columns for the affected games only.
    repl = []
    if "game_play_number" in out.columns:
        repl.append(
            pl.when(affected)
            .then((pl.int_range(pl.len()).over("game_id") + 1).cast(out.schema["game_play_number"]))
            .otherwise(pl.col("game_play_number"))
            .alias("game_play_number")
        )
    for col, src, shift in _ORDER_COLS:
        if col in out.columns and src in out.columns:
            repl.append(
                pl.when(affected)
                .then(pl.col(src).shift(shift).over("game_id").cast(out.schema[col], strict=False))
                .otherwise(pl.col(col))
                .alias(col)
            )
    return out.with_columns(repl).drop("_ri")


def _clamp_scores_monotone(out: pl.DataFrame) -> pl.DataFrame:
    """#146 step 2: clamp cumulative scores to the per-game running maximum.

    ESPN's stale duplicate rows carry earlier-game scores (401616465: a
    re-recorded stretch of period 1 interleaved at the right clock but with
    scores from minutes earlier). The true score at any instant is the
    running max, so the clamp corrects exactly those rows and is a no-op on
    every well-formed game. Score spikes that would poison the running max
    were measured at <= 7 rows per season (2024-2026) and 0 outside stale
    tails, so the clamp is safe.
    """
    if not {"game_id", "home_score", "away_score"}.issubset(out.columns):
        return out
    n_before = out.filter(
        (pl.col("home_score").cast(pl.Int64).diff().over("game_id") < 0)
        | (pl.col("away_score").cast(pl.Int64).diff().over("game_id") < 0)
    ).height
    if n_before == 0:
        return out
    log.warning(
        "pbp repair (#146): clamping %d stale-score row boundaries to running max", n_before
    )
    return out.with_columns(
        pl.col("home_score").cum_max().over("game_id").alias("home_score"),
        pl.col("away_score").cum_max().over("game_id").alias("away_score"),
    )


def _inject_closing_spreads(out: pl.DataFrame, *, base: Path) -> pl.DataFrame:
    """#140: replace pickcenter-default spreads with real consensus closing lines."""
    lookup_path = base / _SPREAD_LOOKUP
    cols = {"game_id", "game_spread", "home_team_spread", "home_favorite", "game_spread_available"}
    if not cols.issubset(out.columns):
        return out
    if not lookup_path.exists():
        log.warning("pbp repair (#140): %s missing -- spread injection skipped", lookup_path)
        return out
    need = (pl.col("game_spread_available") == False) | pl.col(  # noqa: E712
        "game_spread_available"
    ).is_null()
    if out.filter(need).height == 0:
        return out
    lk = (
        pl.read_parquet(lookup_path)
        .select("game_id", "game_spread", "home_team_spread", "home_favorite")
        .unique(subset=["game_id"], keep="first")
        .rename(
            {
                "game_spread": "_inj_gs",
                "home_team_spread": "_inj_hts",
                "home_favorite": "_inj_hf",
            }
        )
        .with_columns(pl.col("game_id").cast(out.schema["game_id"], strict=False))
    )
    out = out.join(lk, on="game_id", how="left")
    hit = need & pl.col("_inj_gs").is_not_null()
    n_games = out.filter(hit)["game_id"].n_unique()
    if n_games:
        log.info("pbp repair (#140): injected real closing spreads for %d game(s)", n_games)
    out = out.with_columns(
        pl.when(hit)
        .then(pl.col("_inj_gs").cast(out.schema["game_spread"]))
        .otherwise(pl.col("game_spread"))
        .alias("game_spread"),
        pl.when(hit)
        .then(pl.col("_inj_hts").cast(out.schema["home_team_spread"]))
        .otherwise(pl.col("home_team_spread"))
        .alias("home_team_spread"),
        pl.when(hit)
        .then(pl.col("_inj_hf").cast(out.schema["home_favorite"]))
        .otherwise(pl.col("home_favorite"))
        .alias("home_favorite"),
        pl.when(hit)
        .then(pl.lit(True))
        .otherwise(pl.col("game_spread_available"))
        .alias("game_spread_available"),
    )
    return out.drop(["_inj_gs", "_inj_hts", "_inj_hf"])
