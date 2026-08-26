"""Unit tests for reshapers._repair_team_box (hoopR#164 / #163 guard rails).

Shapes taken from real payloads: the 2018 label-shift (repair + tail nulling),
the 2019 duplicate-athlete inflation (gate must block), and a stale pre-final
snapshot (gate must block). See tests/nba_data_build/test_parity_team_box.py
for the full-frame parity coverage.
"""

from __future__ import annotations

import polars as pl
from nba_data_build.reshapers import _repair_team_box


def _team(assists: int, *, score: int = 100, fgm: int = 40) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "team_id": [1],
            "team_score": [score],
            "field_goals_made": [fgm],
            "assists": [assists],
            "steals": [7],
            "blocks": [3],
            "turnovers": [12],
            "fouls": [20],
            "total_turnovers": [13],
            "flagrant_fouls": [9],
            "largest_lead": ["4"],
        }
    )


def _players(*, assists: int = 25, points: int = 100, fgm: int = 40) -> pl.DataFrame:
    # two player rows summing to the given team totals
    return pl.DataFrame(
        {
            "team_id": [1, 1],
            "points": [points - 10, 10],
            "field_goals_made": [fgm - 4, 4],
            "assists": [assists - 5, 5],
            "steals": [4, 3],
            "blocks": [2, 1],
            "turnovers": [10, 2],
            "fouls": [15, 5],
        }
    )


def test_single_stat_repair_leaves_tail_alone():
    out = _repair_team_box(_team(assists=99), _players(assists=25))
    assert out["assists"][0] == 25
    # < 3 mismatches: unknowable tail untouched
    assert out["total_turnovers"][0] == 13
    assert out["flagrant_fouls"][0] == 9
    assert out["largest_lead"][0] == "4"


def test_wholesale_shift_repairs_and_nulls_tail():
    corrupt = _team(assists=3).with_columns(
        pl.lit(1).alias("steals"), pl.lit(15).alias("blocks"), pl.lit(0).alias("fouls")
    )
    out = _repair_team_box(corrupt, _players(assists=25))
    assert out["assists"][0] == 25
    assert out["steals"][0] == 7
    assert out["blocks"][0] == 3
    assert out["fouls"][0] == 20
    # >= 3 mismatches: the team-only tail is unknowable -> null
    assert out["total_turnovers"][0] is None
    assert out["flagrant_fouls"][0] is None
    assert out["largest_lead"][0] is None


def test_gate_blocks_inflated_player_table():
    # 2019 dup-athlete shape: player sums exceed the final score
    out = _repair_team_box(_team(assists=35), _players(assists=42, points=108))
    assert out["assists"][0] == 35


def test_gate_blocks_stale_snapshot():
    # #163 shape: players all zero while the team block has real values
    stale = pl.DataFrame(
        {
            "team_id": [1, 1],
            "points": [0, 0],
            "field_goals_made": [0, 0],
            "assists": [0, 0],
            "steals": [0, 0],
            "blocks": [0, 0],
            "turnovers": [0, 0],
            "fouls": [0, 0],
        }
    )
    out = _repair_team_box(_team(assists=30), stale)
    assert out["assists"][0] == 30


def test_matching_rows_unchanged():
    team = _team(assists=25)
    out = _repair_team_box(team, _players(assists=25))
    assert out.equals(team)
