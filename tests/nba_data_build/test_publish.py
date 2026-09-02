from pathlib import Path

import polars as pl
from nba_data_build import io, publish
from nba_data_build.config import PKG_FUNCTION, REGISTRY

_SIDECARS = ("timestamp.", "package_function.")


def _data_assets(calls):
    """Uploaded asset names minus the release sidecars."""
    return sorted(
        Path(c[3]).name
        for c in calls
        if c[:2] == ["release", "upload"] and not Path(c[3]).name.startswith(_SIDECARS)
    )


def test_publish_uploads_each_file_with_clobber(tmp_path):
    spec = REGISTRY["schedules"]  # write_tree_csv=True, not manifested
    io.write_dataset(pl.DataFrame({"game_id": [1]}), spec, 2025, base=tmp_path)
    calls = []
    res = publish.publish_dataset(
        spec,
        2025,
        base=tmp_path,
        runner=lambda args: calls.append(args),
        exists_check=lambda tag, repo: True,  # release already exists
    )
    uploads = [c for c in calls if c[:2] == ["release", "upload"]]
    assets = _data_assets(calls)  # gh release upload <tag> <path>
    assert assets == ["nba_schedule_2025.csv", "nba_schedule_2025.parquet", "nba_schedule_2025.rds"]
    assert all("--clobber" in c for c in uploads)
    assert res["tag"] == spec.tag


def test_publish_generates_csv_on_the_fly_for_write_tree_csv_false(tmp_path):
    # team_box has no committed tree csv -- publish must still ship a plain
    # .csv release asset, built from the parquet at publish time.
    spec = REGISTRY["team_box"]
    io.write_dataset(pl.DataFrame({"game_id": [1], "team_id": [2]}), spec, 2025, base=tmp_path)
    assert not (tmp_path / "team_box" / "csv").exists()
    calls = []
    publish.publish_dataset(
        spec,
        2025,
        base=tmp_path,
        runner=lambda args: calls.append(args),
        exists_check=lambda tag, repo: True,
    )
    uploads = [c for c in calls if c[:2] == ["release", "upload"]]
    assets = _data_assets(calls)
    assert assets == ["team_box_2025.csv", "team_box_2025.parquet", "team_box_2025.rds"]
    # the on-the-fly csv actually carries the data (not an empty placeholder).
    csv_path = next(Path(c[3]) for c in uploads if Path(c[3]).name == "team_box_2025.csv")
    assert pl.read_csv(csv_path).height == 1


def test_publish_uploads_manifest_for_manifested_datasets(tmp_path):
    spec = REGISTRY["standings"]
    io.write_dataset(pl.DataFrame({"team_id": [1]}), spec, 2025, base=tmp_path)
    calls = []
    publish.publish_dataset(
        spec,
        2025,
        base=tmp_path,
        runner=lambda args: calls.append(args),
        exists_check=lambda tag, repo: True,
    )
    assets = _data_assets(calls)
    assert assets == [
        "nba_standings_in_data_repo.csv",
        "standings_2025.csv",
        "standings_2025.parquet",
        "standings_2025.rds",
    ]


def test_published_manifest_collapses_the_log_to_the_latest_run_per_season(tmp_path):
    spec = REGISTRY["standings"]
    io.write_dataset(pl.DataFrame({"team_id": [1]}), spec, 2024, base=tmp_path)
    io.write_dataset(pl.DataFrame({"team_id": [1]}), spec, 2025, base=tmp_path)
    io.write_dataset(pl.DataFrame({"team_id": [1, 2, 3]}), spec, 2025, base=tmp_path)  # rerun

    asset = publish._manifest_asset(spec, tmp_path)
    m = pl.read_csv(asset)
    assert m["season"].to_list() == [2024, 2025]
    assert m["row_count"].to_list() == [1, 3]


def test_publish_creates_release_when_missing(tmp_path):
    spec = REGISTRY["schedules"]
    io.write_dataset(pl.DataFrame({"game_id": [1]}), spec, 2025, base=tmp_path)
    calls = []
    publish.publish_dataset(
        spec,
        2025,
        base=tmp_path,
        runner=lambda args: calls.append(args),
        exists_check=lambda tag, repo: False,
    )
    assert any(c[:2] == ["release", "create"] for c in calls)


def test_publish_stamps_the_tag_with_timestamp_and_package_function(tmp_path):
    """Every publish re-stamps the four sidecars R's sportsdataverse_save() writes."""
    spec = REGISTRY["schedules"]
    io.write_dataset(pl.DataFrame({"game_id": [1]}), spec, 2025, base=tmp_path)
    calls = []
    publish.publish_dataset(
        spec,
        2025,
        base=tmp_path,
        runner=lambda args: calls.append(args),
        exists_check=lambda tag, repo: True,
    )
    uploads = [c for c in calls if c[:2] == ["release", "upload"]]
    sidecars = [Path(c[3]).name for c in uploads if Path(c[3]).name.startswith(_SIDECARS)]
    assert sidecars == [
        "timestamp.txt",
        "timestamp.json",
        "package_function.txt",
        "package_function.json",
    ]
    # stamped LAST -- the timestamp must describe a finished upload
    assert [Path(c[3]).name for c in uploads][-4:] == sidecars
    assert all(c[-1] == "--clobber" and c[2] == spec.tag for c in uploads)


def test_dry_run_stamps_nothing(tmp_path):
    spec = REGISTRY["schedules"]
    io.write_dataset(pl.DataFrame({"game_id": [1]}), spec, 2025, base=tmp_path)
    calls = []
    publish.publish_dataset(
        spec, 2025, base=tmp_path, dry_run=True, runner=lambda args: calls.append(args)
    )
    assert calls == []


def test_every_registry_tag_has_a_package_function():
    """A new dataset must not publish an unnamed tag."""
    missing = sorted({s.tag for s in REGISTRY.values()} - set(PKG_FUNCTION))
    assert missing == [], f"tags with no PKG_FUNCTION entry: {missing}"


def test_no_upload_means_no_stamp(tmp_path):
    """A run that published nothing must not move the timestamp."""
    spec = REGISTRY["schedules"]  # nothing written under tmp_path
    calls = []
    publish.publish_dataset(
        spec,
        2025,
        base=tmp_path,
        runner=lambda args: calls.append(args),
        exists_check=lambda tag, repo: True,
    )
    assert calls == []
