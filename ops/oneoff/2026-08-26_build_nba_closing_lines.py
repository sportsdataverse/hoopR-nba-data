"""Build nba/betting_lines/closing_lines_odds_api.parquet from the odds-data checkout.

One-off (dated, durable-pipelines convention): extracts a per-ESPN-game
consensus closing spread lookup that the pbp build's spread-injection repair
(hoopR #140, ``nba_data_build.repairs._inject_closing_spreads``) reads at
compile time. The source is the PRIVATE ``oddsapiR-dev/odds-data`` checkout
(The Odds API historical backfill, 2020-06+):

* ``odds/nba/lines/{snapshot}_{chunk}.json`` -- raw closing-line snapshots
  (``{timestamp, data: [event...]}`` with per-bookmaker ``spreads`` /
  ``totals`` markets). The LATEST snapshot per event is the closing line.
* ``odds/espn_crosswalk.parquet`` -- ``(sport_key, event_id) -> espn_game_id``.

Consensus = median across bookmakers of the home team's spread ``point``
(Vegas sign: negative when home favored). hoopR pbp convention (verified on
the 2023 asset, 1320/1320 games): ``game_spread`` = favorite magnitude,
``home_favorite`` = home favored, ``home_team_spread`` = +game_spread when
home favored else -game_spread (i.e. ``-home_point``).

Usage (from the hoopR-nba-data repo root):

    uv run python ops/oneoff/2026-08-26_build_nba_closing_lines.py

Env: ODDS_DATA_DIR overrides the odds-data checkout location.
"""

from __future__ import annotations

import json
import os
import statistics
from pathlib import Path

import polars as pl

ODDS_ROOT = Path(
    os.environ.get(
        "ODDS_DATA_DIR",
        "c:/Users/saiem/Documents/GitHub-Data/sdv-dev/oddsapiR-dev/odds-data",
    )
)
OUT = Path("nba/betting_lines/closing_lines_odds_api.parquet")


def main() -> None:
    lines_dir = ODDS_ROOT / "odds" / "nba" / "lines"
    files = sorted(lines_dir.glob("*.json"))
    if not files:
        raise SystemExit(f"no lines snapshots under {lines_dir}")

    # latest snapshot per event id wins (files are named by snapshot ts and
    # sorted ascending, so a plain overwrite keeps the closing snapshot).
    best: dict[str, dict] = {}
    for f in files:
        payload = json.loads(f.read_text(encoding="utf-8"))
        ts = payload.get("timestamp")
        for ev in payload.get("data") or []:
            eid = ev.get("id")
            if not eid:
                continue
            home = ev.get("home_team")
            points = []
            totals = []
            for bk in ev.get("bookmakers") or []:
                for mk in bk.get("markets") or []:
                    if mk.get("key") == "spreads":
                        for o in mk.get("outcomes") or []:
                            if o.get("name") == home and o.get("point") is not None:
                                points.append(float(o["point"]))
                    elif mk.get("key") == "totals":
                        for o in mk.get("outcomes") or []:
                            if o.get("name") == "Over" and o.get("point") is not None:
                                totals.append(float(o["point"]))
            if not points:
                continue
            prev = best.get(eid)
            if prev is None or str(ts) >= str(prev["snapshot"]):
                best[eid] = {
                    "event_id": eid,
                    "snapshot": ts,
                    "commence_time": ev.get("commence_time"),
                    "home_team": home,
                    "away_team": ev.get("away_team"),
                    "home_point": statistics.median(points),
                    "over_under": statistics.median(totals) if totals else None,
                    "n_books": len(points),
                }
    print(f"events with a closing spread: {len(best)} (from {len(files)} snapshot files)")

    events = pl.DataFrame(list(best.values()))
    cw = (
        pl.read_parquet(ODDS_ROOT / "odds" / "espn_crosswalk.parquet")
        .filter(pl.col("sport_key") == "basketball_nba")
        .select("event_id", "espn_game_id", "match_method")
    )
    lk = (
        events.join(cw, on="event_id", how="inner")
        .with_columns(pl.col("espn_game_id").cast(pl.Int64).alias("game_id"))
        .with_columns(
            pl.col("home_point").abs().alias("game_spread"),
            (pl.col("home_point") < 0).alias("home_favorite"),
            (-pl.col("home_point")).alias("home_team_spread"),
            pl.lit("odds_api_consensus_close").alias("spread_source"),
        )
        # a re-scheduled game can appear as two events -> keep the latest
        .sort("commence_time")
        .unique(subset=["game_id"], keep="last", maintain_order=True)
        .select(
            "game_id",
            "game_spread",
            "home_team_spread",
            "home_favorite",
            "over_under",
            "home_point",
            "n_books",
            "commence_time",
            "snapshot",
            "match_method",
            "spread_source",
        )
        .sort("game_id")
    )
    print(f"espn-crosswalked games with a closing spread: {lk.height}")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    lk.write_parquet(OUT)
    print(f"wrote {OUT} ({lk.height} rows)")

    # coverage per committed pbp season
    for pq in sorted(Path("nba/pbp/parquet").glob("play_by_play_*.parquet")):
        season = int(pq.stem.rsplit("_", 1)[1])
        if season < 2020:
            continue
        ids = pl.read_parquet(pq, columns=["game_id"]).unique()
        n = ids.height
        hit = ids.filter(pl.col("game_id").cast(pl.Int64).is_in(lk["game_id"].implode())).height
        print(f"  season {season}: {hit}/{n} games covered ({100 * hit / n:.1f}%)")


if __name__ == "__main__":
    main()
