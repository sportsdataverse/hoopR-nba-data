import polars as pl

from nba_data_build import ingest


def test_read_final_missing_returns_none(tmp_path):
    assert ingest.read_final(999, raw_root=tmp_path) is None


def test_season_game_ids_filters_game_json(tmp_path):
    sched_dir = tmp_path / "nba" / "schedules" / "parquet"
    sched_dir.mkdir(parents=True)
    pl.DataFrame({"game_id": [1, 2, 3], "game_json": [True, False, True]}).write_parquet(
        sched_dir / "nba_schedule_2025.parquet"
    )
    assert ingest.season_game_ids(2025, raw_root=tmp_path) == [1, 3]


def test_flat_dir_ids_lists_ids_with_no_season_partition(tmp_path):
    d = tmp_path / "nba" / "player_season_stats" / "json"
    d.mkdir(parents=True)
    (d / "10.json").write_text("{}", encoding="utf-8")
    (d / "1000.json").write_text("{}", encoding="utf-8")
    (d / "README.md").write_text("not json", encoding="utf-8")
    assert ingest.flat_dir_ids("player_season_stats", raw_root=tmp_path) == [10, 1000]
