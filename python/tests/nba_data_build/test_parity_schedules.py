"""Parity: Python schedules vs the R-released parquet oracle, FULL 2025 season.

Port provenance: the schedule blocks of ``espn_nba_0{1,2,3}_*_creation.R``
(casts + game_date_time/game_date + PBP/team_box/player_box flag stamping;
script 03 uploads). Oracle:
``hoopR-nba-data/nba/schedules/parquet/nba_schedule_2025.parquet`` (1,329
rows), built from the real sibling ``hoopR-nba-raw`` checkout with the flags
stamped from the ACTUAL built pbp/team_box/player_box datasets (not a
fixture-derived id list).
"""

import polars as pl

from tests.nba_data_build._parity_helpers import assert_parquet_parity
from tests.nba_data_build.conftest import oracle_path


def test_schedules_parity_full_2025(built_base):
    py = pl.read_parquet(built_base / "schedules" / "parquet" / "nba_schedule_2025.parquet")
    oracle = oracle_path("schedules", "nba_schedule")
    sample = [c for c in pl.read_parquet_schema(str(oracle)) if c != "game_id"]
    assert_parquet_parity(py, oracle, keys=["game_id"], sample_cols=sample)
