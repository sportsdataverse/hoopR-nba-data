"""game_rosters / officials read the processed json/final summary when the sidecar is absent.

The game_rosters/json scrape starts in 2002; json/final holds the same blocks back to 1994.
"""

from pathlib import Path

import polars as pl
from nba_data_build import ingest, reshapers


def _fake_raw(monkeypatch, have: dict[tuple[int, str], dict]):
    reads = []

    def read_final(gid, *, raw_root, subdir):
        reads.append((gid, subdir))
        return have.get((gid, subdir))

    monkeypatch.setattr(ingest, "read_final", read_final)
    monkeypatch.setattr(ingest, "season_completed_game_ids", lambda season, raw_root: [1, 2])
    monkeypatch.setattr(ingest, "parallel_map", lambda f, xs: [f(x) for x in xs])
    return reads


def _helper(payload, *, season, game_id):
    return pl.DataFrame({"game_id": [game_id], "src": [payload["src"]]})


def test_missing_sidecar_falls_back_to_json_final(monkeypatch):
    reads = _fake_raw(
        monkeypatch,
        {(1, "game_rosters/json"): {"src": "sidecar"}, (2, "json/final"): {"src": "final"}},
    )
    build = reshapers._sidecar_builder("game_rosters/json", _helper, fallback_subdir="json/final")
    out = build(1994, raw_root=Path("."), base=Path("."))
    assert out.sort("game_id").rows() == [(1, "sidecar"), (2, "final")]
    # a game with its sidecar never reads the fallback
    assert (1, "json/final") not in reads


def test_no_fallback_without_the_argument(monkeypatch):
    _fake_raw(monkeypatch, {(2, "json/final"): {"src": "final"}})
    build = reshapers._sidecar_builder("game_rosters/json", _helper)
    assert build(1994, raw_root=Path("."), base=Path(".")).height == 0
