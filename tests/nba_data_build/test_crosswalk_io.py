"""The crosswalk output contract: shared ``nba/crosswalk/`` dir, no tree csv,
bespoke rds type strings, and a manifest that UPSERTS instead of appending.

R's ``nba_1{1,2,3}_*_crosswalk_creation.R`` hard-code one shared directory.
One row per season is the manifest contract; if the Python producer appended
like the per-game datasets do, a daily re-run would grow a duplicate row per
season -- which is exactly how the committed
``nba_team_crosswalk_in_data_repo.csv`` ended up with nine identical 2026 rows.
"""

import polars as pl
from nba_data_build import io, publish
from nba_data_build.config import REGISTRY

CROSSWALKS = ("team_crosswalk", "schedule_crosswalk", "player_crosswalk")


def _frame() -> pl.DataFrame:
    return pl.DataFrame({"season": [2026, 2026], "espn_team_id": [1, 2]})


def test_all_three_crosswalks_share_one_directory(tmp_path):
    for ds in CROSSWALKS:
        assert io.dataset_dir(REGISTRY[ds], tmp_path) == tmp_path / "crosswalk"


def test_manifest_file_name_still_carries_the_dataset(tmp_path):
    assert io.manifest_path(REGISTRY["team_crosswalk"], tmp_path) == (
        tmp_path / "crosswalk" / "nba_team_crosswalk_in_data_repo.csv"
    )


def test_crosswalks_write_no_tree_csv_the_tree_csv_is_the_manifest():
    for ds in CROSSWALKS:
        assert REGISTRY[ds].write_tree_csv is False, ds


def test_write_lands_under_crosswalk_not_under_the_dataset_name(tmp_path):
    spec = REGISTRY["team_crosswalk"]
    paths = io.write_dataset(_frame(), spec, 2026, base=tmp_path)
    assert (tmp_path / "crosswalk" / "parquet" / "nba_team_crosswalk_2026.parquet").exists()
    assert (tmp_path / "crosswalk" / "rds" / "nba_team_crosswalk_2026.rds").exists()
    assert not (tmp_path / "team_crosswalk").exists()
    assert not any(p.suffix == ".csv" and p.parent.name == "csv" for p in paths)


def test_rerunning_a_season_upserts_the_manifest_row(tmp_path):
    spec = REGISTRY["player_crosswalk"]
    for _ in range(3):
        io.write_dataset(_frame(), spec, 2026, base=tmp_path)
    io.write_dataset(_frame(), spec, 2025, base=tmp_path)
    m = pl.read_csv(io.manifest_path(spec, tmp_path))
    assert m["season"].to_list() == [2025, 2026]  # sorted, one row per season
    assert m["source_endpoint"].unique().to_list() == ["hoopR::nba_player_crosswalk()"]


def test_per_game_datasets_still_append_their_manifest_log(tmp_path):
    spec = REGISTRY["rosters"]
    for _ in range(3):
        io._append_manifest(spec, 2026, 5, tmp_path)
    assert pl.read_csv(io.manifest_path(spec, tmp_path)).height == 3


def test_publish_finds_the_crosswalk_files_under_the_shared_dir(tmp_path):
    spec = REGISTRY["schedule_crosswalk"]
    io.write_dataset(_frame(), spec, 2026, base=tmp_path)
    names = [p.name for p in publish._dataset_files(spec, 2026, tmp_path)]
    assert "nba_schedule_crosswalk_2026.parquet" in names
    assert "nba_schedule_crosswalk_2026.rds" in names
    # No tree csv, but the release contract still ships one (built from parquet).
    assert "nba_schedule_crosswalk_2026.csv" in names
    assert "nba_schedule_crosswalk_in_data_repo.csv" in names


def test_rds_carries_the_bespoke_crosswalk_type_not_the_generic_template():
    assert REGISTRY["team_crosswalk"].rds_type == "NBA team crosswalk (ESPN / NBA Stats / Fox)"
    assert REGISTRY["schedule_crosswalk"].rds_type == "NBA schedule crosswalk (ESPN / NBA Stats)"
    assert REGISTRY["player_crosswalk"].rds_type == "NBA player crosswalk (ESPN / NBA Stats / Fox)"
