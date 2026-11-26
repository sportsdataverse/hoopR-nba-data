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


def _split_count_names(df: pl.DataFrame) -> list[str]:
    """Stat names of the numeric count rows sportsdataverse-py #1201 derives.

    Each ``fooMade-fooAttempted`` text row is followed by numeric ``fooMade``
    and ``fooAttempted`` rows. The R oracle predates them.
    """
    names = df.get_column("stat_name").drop_nulls().unique().to_list()
    return sorted({part for n in names if n.count("-") == 1 for part in n.split("-")})


def test_player_season_stats_parity_full_2025(built_base):
    # ponytail: the R chain (espn_nba_05) does not emit the #1201 count rows yet;
    # this asserts them against their text and keeps full parity on every other row.
    py = pl.read_parquet(
        built_base / "player_season_stats" / "parquet" / "player_season_stats_2025.parquet"
    )
    oracle = oracle_path("player_season_stats", "player_season_stats")
    r = pl.read_parquet(oracle)

    split_names = _split_count_names(py)
    assert split_names, "expected the #1201 made/attempted count rows"
    assert r.filter(pl.col("stat_name").is_in(split_names)).is_empty()

    is_split = pl.col("stat_name").is_in(split_names)
    derived, base = py.filter(is_split), py.filter(~is_split)
    parents = py.filter(pl.col("stat_name").str.count_matches("-") == 1)
    assert derived.height == 2 * parents.height

    # each derived count equals its half of the parent's "made-attempted" text
    key = ["athlete_id", "team_id", "category"]
    halves = parents.with_columns(
        _name=pl.col("stat_name").str.split("-"),
        _val=pl.col("display_value").str.split("-"),
    )
    got = derived.select(*key, sn=pl.col("stat_name"), v=pl.col("value"))
    for i in (0, 1):
        expected = halves.select(
            *key,
            sn=pl.col("_name").list.get(i),
            v=pl.col("_val").list.get(i).cast(pl.Float64),
        )
        missing = expected.join(got, on=[*key, "sn", "v"], how="anti")
        assert missing.is_empty(), f"derived counts disagree with text:\n{missing.head()}"

    cols = list(r.columns)
    assert_parquet_parity(base, oracle, keys=cols, sample_cols=[])
