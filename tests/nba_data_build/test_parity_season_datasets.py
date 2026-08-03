"""Parity: rosters / team_season_stats / player_season_stats / standings vs
the R-released parquet oracles, FULL 2025 season.

Port provenance: the script-local parsers in
``hoopR-nba-data/R/espn_nba_0{4,5,6,7}_*_creation.R``. ``player_season_stats``
is the NBA-novel builder (identity backfilled from the season's built
player_box, not team rosters -- see ``reshapers.player_season_stats_builder``);
the other three delegate to the shared WBB implementation after league
normalization. Long-format frames have no compact unique key, so ALL columns
act as sort keys (total order; duplicate rows compare as multisets).
"""

import polars as pl
import pytest

from tests.nba_data_build._parity_helpers import assert_parquet_parity
from tests.nba_data_build.conftest import oracle_path


@pytest.mark.parametrize(
    ("dataset", "stem", "keys"),
    [
        ("rosters", "rosters", ["team_id", "athlete_id"]),
        ("player_season_stats", "player_season_stats", None),
        ("team_season_stats", "team_season_stats", None),
        ("standings", "standings", None),
    ],
)
def test_season_dataset_parity_full_2025(dataset, stem, keys, built_base):
    py = pl.read_parquet(built_base / dataset / "parquet" / f"{stem}_2025.parquet")
    oracle = oracle_path(dataset, stem)
    all_cols = list(pl.read_parquet_schema(str(oracle)))
    keys = keys if keys is not None else all_cols
    sample = [c for c in all_cols if c not in keys]
    assert_parquet_parity(py, oracle, keys=keys, sample_cols=sample)
