"""Pre-publish gate for the 2026-08-26 pbp defect repairs (hoopR #178/#140/#146).

Compares a locally rebuilt ``nba/pbp/parquet/play_by_play_{season}.parquet``
against the currently-published ``espn_nba_pbp`` release asset and enforces:

(a) row count within +/-0.1% of the published asset;
(b) only the intended columns changed (per-column changed-cell counts, rows
    aligned on ``(game_id, id)``);
(c) defect invariants on the rebuilt asset:
    * period_number <= 10 everywhere (no "25OT" garbage);
    * every game's cumulative scores monotone non-decreasing in stored order
      (401616465 called out explicitly for season 2024);
    * for 2024+ seasons: game_spread has realistic variance (> 50 distinct
      values) and > 90% of games carry a real (game_spread_available) line.

Usage (repo root):

    uv run python ops/oneoff/2026-08-26_pbp_defect_gate.py --season 2024
    uv run python ops/oneoff/2026-08-26_pbp_defect_gate.py --season 2015 --published path/to/cached.parquet

Exit code 0 = all gates pass.
"""

from __future__ import annotations

import argparse
import sys
import tempfile
import urllib.request
from pathlib import Path

import polars as pl

RELEASE_URL = (
    "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
    "espn_nba_pbp/play_by_play_{season}.parquet"
)

# Columns the repairs are ALLOWED to change.
INTENDED = {
    # 178 period repair
    "period_number",
    "period",
    "qtr",
    "period_display_value",
    "half",
    "game_half",
    "start_quarter_seconds_remaining",
    "start_half_seconds_remaining",
    "start_game_seconds_remaining",
    "end_quarter_seconds_remaining",
    "end_half_seconds_remaining",
    "end_game_seconds_remaining",
    # 146 resequencing (order-dependent recomputes)
    "game_play_number",
    "lag_qtr",
    "lead_qtr",
    "lag_half",
    "lead_half",
    "lag_game_half",
    "lead_game_half",
    # 146 stale-duplicate score clamp (running max)
    "home_score",
    "away_score",
    # 140 spread injection
    "game_spread",
    "home_team_spread",
    "home_favorite",
    "game_spread_available",
}

# Known benign drift: the raw repo is re-scraped continuously, and a handful of
# rows carry a literal "NA" type_abbreviation the older R-built assets show as
# null (same class as the divergence documented in tests/test_parity_pbp.py).
KNOWN_DRIFT_MAX = {"type_abbreviation": 10}


def fetch_published(season: int, cache: Path) -> Path:
    cache.mkdir(parents=True, exist_ok=True)
    dest = cache / f"published_play_by_play_{season}.parquet"
    if not dest.exists():
        url = RELEASE_URL.format(season=season)
        print(f"downloading published asset: {url}")
        urllib.request.urlretrieve(url, dest)  # noqa: S310
    return dest


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--published", type=Path, default=None, help="cached published parquet")
    ap.add_argument("--rebuilt", type=Path, default=None)
    a = ap.parse_args()

    rebuilt_path = a.rebuilt or Path(f"nba/pbp/parquet/play_by_play_{a.season}.parquet")
    pub_path = a.published or fetch_published(
        a.season, Path(tempfile.gettempdir()) / "nba_pbp_gate"
    )

    new = pl.read_parquet(rebuilt_path)
    old = pl.read_parquet(pub_path)
    failures: list[str] = []

    # (a) row count
    delta = abs(new.height - old.height) / max(old.height, 1)
    print(f"[a] rows: published={old.height} rebuilt={new.height} delta={delta:.5%}")
    if delta > 0.001:
        failures.append(f"row count delta {delta:.4%} > 0.1%")

    # (b) per-column changed cells, aligned on (game_id, id)
    key = ["game_id", "id"]
    o = old.with_columns(pl.col("id").cast(pl.Int64), pl.col("game_id").cast(pl.Int64))
    n = new.with_columns(pl.col("id").cast(pl.Int64), pl.col("game_id").cast(pl.Int64))
    if o.select(key).is_duplicated().any() or n.select(key).is_duplicated().any():
        # fall back to a within-key ordinal so duplicated play ids still align
        o = o.with_columns(pl.int_range(pl.len()).over(key).alias("_dup"))
        n = n.with_columns(pl.int_range(pl.len()).over(key).alias("_dup"))
        key = key + ["_dup"]
    shared = [c for c in o.columns if c in n.columns and c not in key]
    j = o.join(n, on=key, how="inner", suffix="_new")
    print(f"[b] aligned rows: {j.height} (old {o.height} / new {n.height})")
    changed: dict[str, int] = {}
    for c in shared:
        if f"{c}_new" not in j.columns:
            continue
        lhs = pl.col(c)
        rhs = pl.col(f"{c}_new")
        if j.schema[c] != j.schema[f"{c}_new"]:
            # dtype-only contract changes (R Int32 -> Python Float64, Float64
            # id -> Int64) compare numerically, not lexically
            if j.schema[c].is_numeric() and j.schema[f"{c}_new"].is_numeric():
                lhs = lhs.cast(pl.Float64, strict=False)
                rhs = rhs.cast(pl.Float64, strict=False)
            else:
                lhs = lhs.cast(pl.Utf8, strict=False)
                rhs = rhs.cast(pl.Utf8, strict=False)
        ne = j.filter(lhs.ne_missing(rhs)).height
        if ne:
            changed[c] = ne
    print("[b] changed columns:")
    for c, cnt in sorted(changed.items(), key=lambda kv: -kv[1]):
        if c in INTENDED:
            tag = "intended"
        elif c in KNOWN_DRIFT_MAX and cnt <= KNOWN_DRIFT_MAX[c]:
            tag = "known raw-refresh drift"
        else:
            tag = "UNEXPECTED"
        print(f"    {c}: {cnt} cells ({tag})")
        if tag == "UNEXPECTED":
            failures.append(f"unexpected column changed: {c} ({cnt} cells)")
    if not changed:
        print("    (none)")

    # (c) invariants
    bad_p = new.filter(pl.col("period_number") > 10)
    print(f"[c] period_number > 10 rows: {bad_p.height}")
    if bad_p.height:
        failures.append(f"{bad_p.height} rows with period_number > 10")

    dec = new.with_columns(
        (
            (pl.col("home_score").cast(pl.Int64).diff().over("game_id") < 0)
            | (pl.col("away_score").cast(pl.Int64).diff().over("game_id") < 0)
        ).alias("_dec")
    ).filter(pl.col("_dec") == True)  # noqa: E712
    n_dec_games = dec["game_id"].n_unique() if dec.height else 0
    print(f"[c] score-decrease rows: {dec.height} in {n_dec_games} game(s)")
    if a.season == 2024:
        g = dec.filter(pl.col("game_id") == 401616465)
        print(f"[c] 401616465 score-decrease rows: {g.height}")
        if g.height:
            failures.append(f"401616465 still has {g.height} score-decrease rows")
    if dec.height:
        # The running-max clamp guarantees monotone scores; any residual
        # decrease is a repair bug.
        failures.append(f"{dec.height} score-decrease rows survived the clamp")

    if a.season >= 2024:
        games = new.unique(subset=["game_id"])
        n_games = games.height
        avail = games.filter(pl.col("game_spread_available") == True).height  # noqa: E712
        distinct = games["game_spread"].n_unique()
        print(
            f"[c] spread: {avail}/{n_games} games with a real line "
            f"({100 * avail / n_games:.1f}%), {distinct} distinct spreads"
        )
        if avail / n_games < 0.90:
            failures.append(f"only {100 * avail / n_games:.1f}% of games carry a real line")
        if distinct <= 50:
            failures.append(f"only {distinct} distinct game_spread values")

    print()
    if failures:
        print("GATE FAILED:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print(f"GATE PASSED for season {a.season}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
