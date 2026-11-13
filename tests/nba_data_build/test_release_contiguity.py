"""The post-publish season-contiguity and rds-coverage check."""

import polars as pl
import pytest
from nba_data_build import cli, publish
from nba_data_build.config import REGISTRY

SPEC = REGISTRY["team_box"]


def _assets(*seasons: int, rds: bool = True) -> list[str]:
    exts = ("parquet", "rds", "csv") if rds else ("parquet", "csv")
    names = [f"{SPEC.stem}_{s}.{ext}" for s in seasons for ext in exts]
    return [*names, "timestamp.json", f"other_{seasons[0] + 1}.parquet"]


def test_contiguous_release_has_no_missing_season():
    assert publish.missing_interior_seasons(SPEC, _assets(2019, 2020, 2021)) == []
    assert publish.seasons_missing_rds(SPEC, _assets(2019, 2020, 2021)) == []


def test_interior_hole_is_reported_and_edges_are_not():
    assert publish.missing_interior_seasons(SPEC, _assets(2019, 2022, 2023)) == [2020, 2021]


def test_parquet_without_rds_is_reported():
    names = _assets(2019, 2020) + _assets(2021, rds=False)
    assert publish.seasons_missing_rds(SPEC, names) == [2021]
    with pytest.raises(publish.SeasonGapError, match=r"\[2021\] have parquet but no .rds"):
        publish.check_release_contiguous(SPEC, lister=lambda tag, repo: names)


def test_check_raises_on_a_hole():
    with pytest.raises(publish.SeasonGapError, match=r"interior season\(s\) \[2020\]"):
        publish.check_release_contiguous(SPEC, lister=lambda tag, repo: _assets(2019, 2021))


def test_known_gaps_name_real_datasets():
    assert set(publish.KNOWN_GAPS) <= set(REGISTRY)
    for name, seasons in publish.KNOWN_GAPS.items():
        spec = REGISTRY[name]
        lo, hi = min(seasons) - 1, max(seasons) + 1
        names = [f"{spec.stem}_{s}.{e}" for s in (lo, hi) for e in ("parquet", "rds")]
        assert publish.missing_interior_seasons(spec, names) == []


@pytest.fixture
def no_build(monkeypatch):
    monkeypatch.setattr(cli, "build_season", lambda *a, **k: pl.DataFrame({"x": [1]}))
    calls = []

    def _check(spec, *a, **k):
        calls.append(spec.dataset)
        raise publish.SeasonGapError("team_box: interior season(s) [2020] missing")

    monkeypatch.setattr(cli.publish, "check_release_contiguous", _check)
    return calls


def test_publish_run_fails_on_a_hole(no_build):
    assert cli.main(["--dataset", "team_box", "-s", "2025", "-e", "2025", "--publish"]) == 1
    assert no_build == ["team_box"]


def test_allow_gaps_skips_the_check(no_build):
    argv = ["--dataset", "team_box", "-s", "2025", "-e", "2025", "--publish", "--allow-gaps"]
    assert cli.main(argv) == 0
    assert no_build == []


def test_no_publish_no_check(no_build):
    assert cli.main(["--dataset", "team_box", "-s", "2025", "-e", "2025", "--dry-run"]) == 0
    assert no_build == []
