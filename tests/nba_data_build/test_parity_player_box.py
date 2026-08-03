"""Parity: Python player_box vs the R-released parquet oracle, FULL 2025 season.

Port provenance: ``hoopR:::helper_espn_nba_player_box``. Oracle:
``hoopR-nba-data/nba/player_box/parquet/player_box_2025.parquet`` (35,250
rows, 1,324 games incl. DNP athletes), built from the real sibling
``hoopR-nba-raw`` checkout.
"""

import polars as pl

from tests.nba_data_build._parity_helpers import assert_parquet_parity
from tests.nba_data_build.conftest import oracle_path

KEYS = ["game_id", "athlete_id"]


def test_player_box_parity_full_2025(built_base):
    py = pl.read_parquet(built_base / "player_box" / "parquet" / "player_box_2025.parquet")
    oracle = oracle_path("player_box", "player_box")
    sample = [c for c in pl.read_parquet_schema(str(oracle)) if c not in KEYS]
    assert_parquet_parity(py, oracle, keys=KEYS, sample_cols=sample)
