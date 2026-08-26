"""Gate for the hoopR#163/#164 box-score repairs -- run BEFORE any publish.

Per (dataset, season): download the currently-published release parquet,
compare it to the freshly rebuilt local parquet, and check:

  (a) row count within +-0.1% of the published asset (or the delta is the
      documented team_box-2021 backfill of games absent from the release);
  (b) the cell diff is confined to the intended columns / games;
  (c) defect invariants:
        team_box   -- credible-row assists-vs-player-sum mismatches ~ 0
                      (the exact residual is printed for the issue comment);
        player_box -- zero games where every player row has minutes 0/NA,
                      and game 401307882's minutes match ESPN live values.

Usage (repo root):

    uv run python ops/oneoff/2026-08-26_gate_box_repairs.py --dataset team_box --season 2018
    uv run python ops/oneoff/2026-08-26_gate_box_repairs.py --dataset player_box --season 2021 \
        --changed-games-file <ids.txt>      # confine the diff to re-scraped games

Exits 0 on PASS, 1 on FAIL. Post-publish verification: re-run with
--published-only to re-download the fresh asset and re-check the invariants
against what is actually being served.
"""

from __future__ import annotations

import argparse
import io
import sys
import urllib.request
from pathlib import Path

import polars as pl

RELEASE = "https://github.com/sportsdataverse/sportsdataverse-data/releases/download"
TAGS = {"team_box": "espn_nba_team_boxscores", "player_box": "espn_nba_player_boxscores"}
SUMMABLE = ["assists", "steals", "blocks", "turnovers", "fouls"]
UNKNOWABLE = [
    "team_turnovers",
    "total_turnovers",
    "technical_fouls",
    "total_technical_fouls",
    "flagrant_fouls",
    "turnover_points",
    "fast_break_points",
    "points_in_paint",
    "largest_lead",
]
# ESPN live minutes for game 401307882 (CHI @ MIA 2021-05-01), captured
# 2026-08-26 from site.api.espn.com summary -- the #163 acceptance example.
GAME_401307882_MIN = {
    "Coby White": 31,
    "Jeff Teague": 34,
    "Elijah Bryant": 32,
}


def published_frame(dataset: str, season: int) -> pl.DataFrame:
    url = f"{RELEASE}/{TAGS[dataset]}/{dataset}_{season}.parquet"
    with urllib.request.urlopen(url, timeout=120) as r:
        return pl.read_parquet(io.BytesIO(r.read()))


def keys_for(dataset: str) -> list[str]:
    return ["game_id", "team_id"] if dataset == "team_box" else ["game_id", "team_id", "athlete_id"]


def cell_diff(old: pl.DataFrame, new: pl.DataFrame, keys: list[str]) -> tuple[dict, pl.DataFrame]:
    """Per-column changed-cell counts + the joined frame of common rows."""
    shared = [c for c in old.columns if c in new.columns]
    o = old.select(shared).with_columns([pl.col(k).cast(pl.Int64) for k in keys])
    n = new.select(shared).with_columns([pl.col(k).cast(pl.Int64) for k in keys])
    j = o.join(n, on=keys, how="inner", suffix="__new")
    counts = {}
    for c in shared:
        if c in keys:
            continue
        a, b = pl.col(c), pl.col(f"{c}__new")
        # string-compare so dtype-widening (Int32 vs Int64) is not a "change"
        changed = j.filter(
            (a.cast(pl.Utf8, strict=False) != b.cast(pl.Utf8, strict=False)).fill_null(
                a.is_null() != b.is_null()
            )
        ).height
        if changed:
            counts[c] = changed
    return counts, j


def team_box_invariant(tb: pl.DataFrame, pb: pl.DataFrame) -> tuple[int, int, pl.DataFrame]:
    """(credible-row assist mismatches, all-row assist mismatches, residual rows)."""
    ps = (
        pb.group_by("game_id", "team_id")
        .agg(
            pl.col("points").cast(pl.Int64, strict=False).sum().alias("p_pts"),
            pl.col("field_goals_made").cast(pl.Int64, strict=False).sum().alias("p_fgm"),
            pl.col("assists").cast(pl.Int64, strict=False).sum().alias("p_ast"),
        )
        .with_columns(pl.col("game_id").cast(pl.Int64), pl.col("team_id").cast(pl.Int64))
    )
    j = (
        tb.select("game_id", "team_id", "team_score", "field_goals_made", "assists")
        .with_columns(pl.col("game_id").cast(pl.Int64), pl.col("team_id").cast(pl.Int64))
        .join(ps, on=["game_id", "team_id"], how="left")
    )
    bad_all = j.filter(pl.col("assists").cast(pl.Int64) != pl.col("p_ast"))
    credible = (pl.col("team_score").cast(pl.Int64) == pl.col("p_pts")) & (
        pl.col("field_goals_made").cast(pl.Int64) == pl.col("p_fgm")
    )
    bad_credible = bad_all.filter(credible)
    return bad_credible.height, bad_all.height, bad_all


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True, choices=["team_box", "player_box"])
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--base", default="nba")
    ap.add_argument(
        "--changed-games-file",
        default=None,
        help="file of game_ids (one per line) the diff is allowed to touch",
    )
    ap.add_argument(
        "--published-only",
        action="store_true",
        help="post-publish mode: run the invariants on the downloaded release asset itself",
    )
    a = ap.parse_args()
    ds, season, base = a.dataset, a.season, Path(a.base)
    failures: list[str] = []

    new = pl.read_parquet(base / ds / "parquet" / f"{ds}_{season}.parquet")
    old = published_frame(ds, season)
    frame = old if a.published_only else new
    print(f"== {ds} {season}: published rows={old.height} local rows={new.height}")

    if not a.published_only:
        # (a) row count
        delta = new.height - old.height
        if abs(delta) > max(1, round(0.001 * old.height)):
            missing = set(new["game_id"].cast(pl.Int64)) - set(old["game_id"].cast(pl.Int64))
            print(
                f"   row delta {delta:+d} exceeds 0.1% -- games absent from the "
                f"published asset: {len(missing)} (backfill of release-missing games)"
            )
            if delta < 0 or len(missing) == 0:
                failures.append(f"row count shrank or delta unexplained ({delta:+d})")
        # (b) cell diff
        counts, j = cell_diff(old, new, keys_for(ds))
        print(f"   changed cells by column: {counts if counts else '(none)'}")
        if ds == "team_box" and a.changed_games_file is None:
            allowed = set(SUMMABLE) | set(UNKNOWABLE)
            stray = [c for c in counts if c not in allowed]
            if stray:
                failures.append(f"unexpected columns changed: {stray}")
        if a.changed_games_file:
            allowed_games = {
                int(x) for x in Path(a.changed_games_file).read_text().split() if x.strip()
            }
            keys = keys_for(ds)
            # team_box: the #164 repair may legitimately touch its own columns
            # in games OUTSIDE the re-scrape list; other columns may not.
            repair_cols = set(SUMMABLE) | set(UNKNOWABLE) if ds == "team_box" else set()
            shared = [
                c
                for c in old.columns
                if c in new.columns and c not in keys and c not in repair_cols
            ]
            any_changed = pl.any_horizontal(
                [
                    (
                        pl.col(c).cast(pl.Utf8, strict=False)
                        != pl.col(f"{c}__new").cast(pl.Utf8, strict=False)
                    ).fill_null(pl.col(c).is_null() != pl.col(f"{c}__new").is_null())
                    for c in shared
                ]
            )
            stray_games = (
                j.filter(any_changed)
                .filter(pl.col("game_id").is_in(sorted(allowed_games)) == False)  # noqa: E712
                .get_column("game_id")
                .unique()
                .to_list()
            )
            if stray_games:
                failures.append(f"cells changed outside the re-scraped games: {stray_games[:10]}")

    # (c) invariants
    pb = pl.read_parquet(base / "player_box" / "parquet" / f"player_box_{season}.parquet")
    if ds == "team_box":
        n_cred, n_all, bad = team_box_invariant(frame, pb)
        print(f"   assists-vs-player-sum mismatches: credible-rows={n_cred} all-rows={n_all}")
        if n_all:
            print(bad.head(30))
        if n_cred:
            failures.append(f"{n_cred} credible rows still mismatch player-sum assists")
    else:
        m = frame.with_columns(pl.col("minutes").cast(pl.Float64, strict=False).alias("__m"))
        allzero = (
            m.group_by("game_id")
            .agg(pl.col("__m").fill_null(0).sum().alias("s"))
            .filter(pl.col("s") == 0)
        )
        print(f"   all-zero-minutes games: {allzero.height} {allzero['game_id'].to_list()[:10]}")
        if allzero.height:
            failures.append(f"{allzero.height} games still have all-zero minutes")
        if season == 2021:
            g = m.filter(pl.col("game_id").cast(pl.Int64) == 401307882)
            got = {
                r["athlete_display_name"]: int(r["__m"] or 0)
                for r in g.select("athlete_display_name", "__m").to_dicts()
            }
            bad_ref = {k: v for k, v in GAME_401307882_MIN.items() if got.get(k) != v}
            print(f"   401307882 spot-check ({len(got)} rows): mismatches={bad_ref or 'none'}")
            if bad_ref:
                failures.append(f"401307882 minutes disagree with ESPN live: {bad_ref}")

    if failures:
        print("GATE FAIL:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("GATE PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
