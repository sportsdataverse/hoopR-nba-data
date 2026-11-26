"""Parity: rosters / team_season_stats / player_season_stats / standings vs
the committed 2025 release parquet, FULL 2025 season.

The oracle is this repo's published ``nba/`` tree, written by whichever chain last
published it. As of 2026-11-26 every NBA 2025 file is Polars-written (the Python build,
republished 2026-11-13), so these tests are a Python-vs-release regression check, not R
parity. The R chain's arrow writer leaves an ``r`` key in the footer; ``_oracle_kind``
reads it so a failure says which one it is. The R port of the #1201 split is checked
offline in ``tests/test_split_made_attempted_r.py``.

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


def _oracle_kind(path) -> str:
    """What a comparison against ``path`` checks, from the writer recorded in its footer."""
    if "r" in pl.read_parquet_metadata(path):
        return "R parity (oracle written by the R chain)"
    return "Python-vs-release regression (oracle is not R-written)"


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
    and ``fooAttempted`` rows. A release published before #1201 lacks them.
    """
    names = df.get_column("stat_name").drop_nulls().unique().to_list()
    return sorted({part for n in names if n.count("-") == 1 for part in n.split("-")})


def test_player_season_stats_parity_full_2025(built_base, tmp_path):
    # Both pipelines emit the #1201 count rows (espn_nba_05 via split_made_attempted); a
    # release published before #1201 lacks them. Assert them against their text; compare
    # the full frame once the release carries them, and every other row until then.
    py = pl.read_parquet(
        built_base / "player_season_stats" / "parquet" / "player_season_stats_2025.parquet"
    )
    r = pl.read_parquet(oracle_path("player_season_stats", "player_season_stats"))

    split_names = _split_count_names(py)
    assert split_names, "expected the #1201 made/attempted count rows"

    is_split = pl.col("stat_name").is_in(split_names)
    derived, base = py.filter(is_split), py.filter(~is_split)
    parents = py.filter(pl.col("stat_name").str.count_matches("-") == 1)
    assert derived.height == 2 * parents.height

    # each derived row is its half of the parent's "made-attempted" row: name, count and
    # text (label + "M"/"A", display name with "Made"/"Attempted"; null when empty)
    key = ["athlete_id", "team_id", "category"]
    halves = parents.with_columns(
        _name=pl.col("stat_name").str.split("-"),
        _val=pl.col("display_value").str.split("-"),
    )
    text = ["stat_name", "value", "display_value", "stat_label", "stat_display_name"]
    got = derived.select(*key, *text)
    label = pl.col("stat_label")
    for i, (suffix, word) in enumerate((("M", "Made"), ("A", "Attempted"))):
        half = pl.col("_val").list.get(i)
        dn = pl.col("stat_display_name").str.replace_all("Made-Attempted", word, literal=True)
        expected = halves.select(
            *key,
            stat_name=pl.col("_name").list.get(i),
            value=half.cast(pl.Float64),
            display_value=half,
            stat_label=pl.when(label.is_null() | (label == ""))
            .then(None)
            .otherwise(label + suffix),
            stat_display_name=pl.when(dn == "").then(None).otherwise(dn),
        )
        missing = expected.join(got, on=[*key, *text], how="anti", nulls_equal=True)
        assert missing.is_empty(), f"derived rows disagree with their parent:\n{missing.head()}"

    cols = list(r.columns)
    release = oracle_path("player_season_stats", "player_season_stats")
    kind = _oracle_kind(release)
    if r.filter(is_split).is_empty():
        # the release predates #1201: compare every other row
        oracle = tmp_path / "oracle_without_split_counts.parquet"
        r.filter(~is_split).write_parquet(oracle)
        py, kind = base, f"{kind}, #1201 count rows stripped"
    else:
        # the release carries them: the full frame, count rows included. This checks
        # R's split_made_attempted() only when the release was written by the R chain.
        oracle = release
    try:
        assert_parquet_parity(py, oracle, keys=cols, sample_cols=[])
    except AssertionError as e:
        raise AssertionError(f"{kind}: {e}") from e
