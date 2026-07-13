"""The pipeline narrates itself: timestamped stdout lines that the daily
processor tees into the per-season rotating logfile, incl. explicit gh-release
upload confirmations."""

import polars as pl

from nba_data_build import io as build_io
from nba_data_build import publish
from nba_data_build.config import REGISTRY


def test_publish_logs_upload_confirmations(tmp_path, capsys):
    spec = REGISTRY["schedules"]  # not manifested -> exactly 2 assets
    build_io.write_dataset(pl.DataFrame({"game_id": [1]}), spec, 2025, base=tmp_path)
    capsys.readouterr()  # drop the write lines
    publish.publish_dataset(
        spec,
        2025,
        base=tmp_path,
        runner=lambda args: None,
        exists_check=lambda tag, repo: True,
    )
    out = capsys.readouterr().out
    assert "uploading nba_schedule_2025.parquet" in out
    assert f"uploaded nba_schedule_2025.csv -> {spec.tag} (asset 2/2)" in out
    assert "[INFO] nba_data_build:" in out  # timestamped, labeled lines


def test_publish_logs_the_manifest_upload(tmp_path, capsys):
    spec = REGISTRY["standings"]
    build_io.write_dataset(pl.DataFrame({"team_id": [1]}), spec, 2025, base=tmp_path)
    capsys.readouterr()
    publish.publish_dataset(
        spec,
        2025,
        base=tmp_path,
        runner=lambda args: None,
        exists_check=lambda tag, repo: True,
    )
    out = capsys.readouterr().out
    assert f"uploaded nba_standings_in_data_repo.csv -> {spec.tag} (asset 3/3)" in out


def test_publish_dry_run_logs_would_be_uploads(tmp_path, capsys):
    spec = REGISTRY["schedules"]
    build_io.write_dataset(pl.DataFrame({"game_id": [1]}), spec, 2025, base=tmp_path)
    capsys.readouterr()
    publish.publish_dataset(spec, 2025, base=tmp_path, dry_run=True)
    out = capsys.readouterr().out
    assert "[dry-run] upload" in out and spec.tag in out
