import polars as pl

from nba_data_build import io
from nba_data_build.config import REGISTRY


def test_write_dataset_emits_parquet_and_plain_csv(tmp_path):
    df = pl.DataFrame({"game_id": [1, 2], "score": [70, 65]})
    spec = REGISTRY["rosters"]  # write_tree_csv=True, plain .csv
    paths = io.write_dataset(df, spec, 2025, base=tmp_path)
    names = sorted(p.name for p in paths)
    # rds is emitted alongside the parquet in the SAME pass: it is
    # hoopR::load_*'s only read path, and leaving it to a separate step is
    # what let the two drift (NBA published fresh parquet against a
    # 3-day-stale rds after the python cutover).
    assert names == ["rosters_2025.csv", "rosters_2025.parquet", "rosters_2025.rds"]
    assert (tmp_path / "rosters" / "parquet" / "rosters_2025.parquet").exists()
    assert (tmp_path / "rosters" / "csv" / "rosters_2025.csv").exists()
    assert (tmp_path / "rosters" / "rds" / "rosters_2025.rds").exists()


def test_write_dataset_skips_tree_csv_for_write_tree_csv_false(tmp_path):
    # NBA/MBB delta: pbp/team_box/player_box never commit a local csv (R's
    # fwrite lines for these are commented out) -- only the parquet is written.
    df = pl.DataFrame({"game_id": [1]})
    spec = REGISTRY["team_box"]
    assert spec.write_tree_csv is False
    paths = io.write_dataset(df, spec, 2025, base=tmp_path)
    # ... but the rds is NOT optional -- write_tree_csv only gates the csv.
    assert [p.name for p in paths] == ["team_box_2025.parquet", "team_box_2025.rds"]
    assert not (tmp_path / "team_box" / "csv").exists()
    assert (tmp_path / "team_box" / "rds" / "team_box_2025.rds").exists()


def test_no_manifest_for_datasets_r_does_not_manifest(tmp_path):
    # R manifests exactly the per-entity/season datasets; team_box is not one
    # of them -- writing (and publishing) a manifest for it would create an
    # asset nothing reads.
    spec = REGISTRY["team_box"]
    io.write_dataset(pl.DataFrame({"game_id": [1]}), spec, 2025, base=tmp_path)
    assert not io.manifest_path(spec, tmp_path).exists()


def test_manifest_is_an_append_log_with_source_endpoint(tmp_path):
    spec = REGISTRY["standings"]  # one of the manifested datasets
    io.write_dataset(pl.DataFrame({"team_id": [1]}), spec, 2025, base=tmp_path)
    io.write_dataset(pl.DataFrame({"team_id": [1, 2, 3]}), spec, 2025, base=tmp_path)
    m = pl.read_csv(io.manifest_path(spec, tmp_path))
    # R fwrite(append=TRUE): one row per RUN, not per season.
    assert m.columns == ["season", "row_count", "generated_at_utc", "source_endpoint"]
    assert m.height == 2
    assert m["row_count"].to_list() == [1, 3]
    assert (
        m["source_endpoint"].to_list()
        == [
            "https://raw.githubusercontent.com/sportsdataverse/hoopR-nba-raw/main/nba/standings/json/2025.json"
        ]
        * 2
    )
