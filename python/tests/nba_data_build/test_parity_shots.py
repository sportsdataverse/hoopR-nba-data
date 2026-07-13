"""Parity: Python shots vs the R-released parquet oracle, FULL 2025 season.

Port provenance: the shots block of ``espn_nba_01_pbp_creation.R`` (filter
``shooting_play == TRUE`` on the compiled season pbp, project the 15 shot
columns). Oracle: ``hoopR-nba-data/nba/shots/parquet/shots_2025.parquet``
(292,933 rows), derived from the full 2025 pbp build. The shot rows have no
unique key, so ALL columns are sort keys (total order; duplicate rows compare
fine as multisets).
"""

import polars as pl

from tests.nba_data_build._parity_helpers import assert_parquet_parity
from tests.nba_data_build.conftest import oracle_path


def test_shots_parity_full_2025(built_base):
    py = pl.read_parquet(built_base / "shots" / "parquet" / "shots_2025.parquet")
    oracle = oracle_path("shots", "shots")
    keys = list(pl.read_parquet_schema(str(oracle)))
    assert_parquet_parity(py, oracle, keys=keys, sample_cols=[])
