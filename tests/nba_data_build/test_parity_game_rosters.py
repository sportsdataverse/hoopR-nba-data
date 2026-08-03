"""Parity: Python game_rosters + officials vs the R-released parquet oracles,
FULL 2025 season.

Port provenance: script-local parsers in
``hoopR-nba-data/R/espn_nba_09_game_rosters_creation.R`` and
``espn_nba_10_officials_creation.R``. NBA delta: officials have no dedicated
raw directory -- both datasets read the SAME ``game_rosters/json`` sidecar
(``reshapers._officials_builder``); ``officials.game_id`` is Int32 (R
``safe_int``), diverging from the WBB/WNBA officials release's String.
"""

import polars as pl
import pytest

from tests.nba_data_build._parity_helpers import assert_parquet_parity
from tests.nba_data_build.conftest import oracle_path


@pytest.mark.parametrize(
    ("dataset", "stem", "keys"),
    [
        ("game_rosters", "game_rosters", ["game_id", "athlete_id"]),
        ("officials", "officials", ["game_id", "official_order"]),
    ],
)
def test_sidecar_parity_full_2025(dataset, stem, keys, built_base):
    py = pl.read_parquet(built_base / dataset / "parquet" / f"{stem}_2025.parquet")
    oracle = oracle_path(dataset, stem)
    sample = [c for c in pl.read_parquet_schema(str(oracle)) if c not in keys]
    assert_parquet_parity(py, oracle, keys=keys, sample_cols=sample)
