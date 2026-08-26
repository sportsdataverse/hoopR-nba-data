"""Unit checks for the pbp upstream-defect repairs (hoopR #178 / #146 / #140)."""

import polars as pl
from nba_data_build.repairs import repair_pbp_season


def _game(game_id, rows):
    """rows: (seq, period, clock_secs, home, away)."""
    return pl.DataFrame(
        {
            "game_id": pl.Series([game_id] * len(rows), dtype=pl.Int32),
            "sequence_number": pl.Series([r[0] for r in rows], dtype=pl.Int32),
            "period_number": pl.Series([r[1] for r in rows], dtype=pl.Int32),
            "period": pl.Series([r[1] for r in rows], dtype=pl.Int32),
            "qtr": pl.Series([r[1] for r in rows], dtype=pl.Int32),
            "period_display_value": [f"{r[1]}th Quarter" for r in rows],
            "half": pl.Series([1 if r[1] <= 2 else 2 for r in rows], dtype=pl.Int32),
            "game_half": pl.Series([1 if r[1] <= 2 else 2 for r in rows], dtype=pl.Int32),
            "start_quarter_seconds_remaining": pl.Series([r[2] for r in rows], dtype=pl.Float64),
            "start_half_seconds_remaining": pl.Series(
                [r[2] + (720 if r[1] in (1, 3) else 0) for r in rows], dtype=pl.Float64
            ),
            "start_game_seconds_remaining": pl.Series(
                [r[2] + (4 - min(r[1], 4)) * 720 for r in rows], dtype=pl.Float64
            ),
            "end_quarter_seconds_remaining": pl.Series([r[2] for r in rows], dtype=pl.Float64),
            "end_half_seconds_remaining": pl.Series([r[2] for r in rows], dtype=pl.Float64),
            "end_game_seconds_remaining": pl.Series([r[2] for r in rows], dtype=pl.Float64),
            "clock_minutes": pl.Series([r[2] // 60 for r in rows], dtype=pl.Int32),
            "clock_seconds": pl.Series([r[2] % 60 for r in rows], dtype=pl.Float64),
            "home_score": pl.Series([r[3] for r in rows], dtype=pl.Int32),
            "away_score": pl.Series([r[4] for r in rows], dtype=pl.Int32),
            "game_play_number": pl.Series(range(1, len(rows) + 1), dtype=pl.Int32),
            "lag_qtr": pl.Series([None] + [r[1] for r in rows[:-1]], dtype=pl.Int32),
            "lead_qtr": pl.Series([r[1] for r in rows[1:]] + [None], dtype=pl.Int32),
            "game_spread": pl.Series([2.5] * len(rows), dtype=pl.Float64),
            "home_team_spread": pl.Series([2.5] * len(rows), dtype=pl.Float64),
            "home_favorite": pl.Series([True] * len(rows), dtype=pl.Boolean),
            "game_spread_available": pl.Series([False] * len(rows), dtype=pl.Boolean),
        }
    )


def test_impossible_period_repaired_from_neighbours(tmp_path):
    g = _game(1, [(1, 1, 700, 0, 0), (2, 1, 650, 2, 0), (3, 1, 600, 2, 2), (4, 2, 700, 4, 2)])
    # corrupt row 2 to a "25OT" period with garbage seconds
    g = g.with_columns(
        pl.when(pl.col("sequence_number") == 3)
        .then(pl.lit(29, dtype=pl.Int32))
        .otherwise(pl.col("period_number"))
        .alias("period_number")
    )
    out = repair_pbp_season(g, base=tmp_path)
    assert out["period_number"].max() <= 8
    fixed = out.filter(pl.col("sequence_number") == 3)
    assert fixed["period_number"][0] == 1
    assert fixed["start_game_seconds_remaining"][0] == 600 + 3 * 720


def test_disordered_game_resequenced(tmp_path):
    # a period-1 play misplaced at the end of the game (score bounces back)
    g = _game(
        2,
        [(1, 1, 700, 0, 0), (2, 1, 650, 2, 0), (3, 2, 700, 4, 2), (4, 1, 660, 2, 0)],
    )
    out = repair_pbp_season(g, base=tmp_path)
    assert out["sequence_number"].to_list() == [1, 4, 2, 3]
    hs = out["home_score"].to_list()
    assert all(b >= a for a, b in zip(hs, hs[1:]))
    assert out["game_play_number"].to_list() == [1, 2, 3, 4]
    assert out["lag_qtr"].to_list() == [None, 1, 1, 1]
    assert out["lead_qtr"].to_list() == [1, 1, 2, None]


def test_ordered_game_untouched(tmp_path):
    g = _game(3, [(1, 1, 700, 0, 0), (2, 1, 650, 2, 0), (3, 2, 700, 4, 2)])
    out = repair_pbp_season(g, base=tmp_path)
    assert out.equals(g)


def test_spread_injection_from_lookup(tmp_path):
    g = _game(4, [(1, 1, 700, 0, 0), (2, 1, 650, 2, 0)])
    (tmp_path / "betting_lines").mkdir(parents=True)
    pl.DataFrame(
        {
            "game_id": pl.Series([4], dtype=pl.Int64),
            "game_spread": [7.5],
            "home_team_spread": [-7.5],
            "home_favorite": [False],
        }
    ).write_parquet(tmp_path / "betting_lines" / "closing_lines_odds_api.parquet")
    out = repair_pbp_season(g, base=tmp_path)
    assert out["game_spread"].to_list() == [7.5, 7.5]
    assert out["home_team_spread"].to_list() == [-7.5, -7.5]
    assert out["home_favorite"].to_list() == [False, False]
    assert out["game_spread_available"].to_list() == [True, True]


def test_spread_untouched_when_available_or_unmatched(tmp_path):
    g = _game(5, [(1, 1, 700, 0, 0)]).with_columns(
        pl.lit(True).alias("game_spread_available"), pl.lit(9.0).alias("game_spread")
    )
    g2 = _game(6, [(1, 1, 700, 0, 0)])
    both = pl.concat([g, g2])
    (tmp_path / "betting_lines").mkdir(parents=True)
    pl.DataFrame(
        {
            "game_id": pl.Series([5], dtype=pl.Int64),
            "game_spread": [1.0],
            "home_team_spread": [1.0],
            "home_favorite": [True],
        }
    ).write_parquet(tmp_path / "betting_lines" / "closing_lines_odds_api.parquet")
    out = repair_pbp_season(both, base=tmp_path)
    # game 5 already had a real line -> untouched; game 6 has no lookup row -> default kept
    assert out.filter(pl.col("game_id") == 5)["game_spread"].to_list() == [9.0]
    assert out.filter(pl.col("game_id") == 6)["game_spread"].to_list() == [2.5]
    assert out.filter(pl.col("game_id") == 6)["game_spread_available"].to_list() == [False]
