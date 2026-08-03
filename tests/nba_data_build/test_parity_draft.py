"""Parity: Python draft vs the R-released parquet oracle, FULL 2025 season.

Port provenance: ``parse_one_pick`` in
``hoopR-nba-data/R/espn_nba_08_draft_creation.R``. Oracle:
``hoopR-nba-data/nba/draft/parquet/draft_2025.parquet`` (60x35, unfiltered).
NBA draft runs DAILY (not annual like WNBA), but the builder shape and raw
source are identical.
"""

import polars as pl

from tests.nba_data_build._parity_helpers import assert_parquet_parity
from tests.nba_data_build.conftest import oracle_path


def test_draft_parity_full_2025(built_base):
    py = pl.read_parquet(built_base / "draft" / "parquet" / "draft_2025.parquet")
    oracle = oracle_path("draft", "draft")
    keys = ["overall_pick", "round", "pick"]
    sample = [c for c in pl.read_parquet_schema(str(oracle)) if c not in keys]
    assert_parquet_parity(py, oracle, keys=keys, sample_cols=sample)


def test_draft_row_order_is_r_arrange_order(built_base):
    # assert_parquet_parity sorts both sides, so it cannot see row order. R
    # ends in arrange(overall_pick, round, pick) -- pin it unsorted.
    py = pl.read_parquet(built_base / "draft" / "parquet" / "draft_2025.parquet")
    oracle = pl.read_parquet(oracle_path("draft", "draft"))
    assert py.get_column("overall_pick").to_list() == oracle.get_column("overall_pick").to_list()
    assert py.get_column("athlete_id").to_list() == oracle.get_column("athlete_id").to_list()
