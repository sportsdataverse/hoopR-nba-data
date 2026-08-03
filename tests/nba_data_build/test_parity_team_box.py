"""Parity: Python team_box vs the R-released parquet oracle, FULL 2025 season.

Port provenance: ``hoopR:::helper_espn_nba_team_box``. Oracle:
``hoopR-nba-data/nba/team_box/parquet/team_box_2025.parquet`` (2,648 rows,
1,324 games), built from the real sibling ``hoopR-nba-raw`` checkout.
"""

import polars as pl

from tests.nba_data_build._parity_helpers import assert_parquet_parity
from tests.nba_data_build.conftest import oracle_path

KEYS = ["game_id", "team_id"]


def test_team_box_parity_full_2025(built_base):
    py = pl.read_parquet(built_base / "team_box" / "parquet" / "team_box_2025.parquet")
    oracle = oracle_path("team_box", "team_box")
    all_cols = [c for c in pl.read_parquet_schema(str(oracle)) if c not in KEYS]
    assert_parquet_parity(py, oracle, keys=KEYS, sample_cols=all_cols)
