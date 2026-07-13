"""Season-2025 full-build fixture shared by the parity tests.

Unlike the wehoop-wnba-data template (three-game fixtures baked into the
repo), these parity tests build the WHOLE 2025 season from the real sibling
``hoopR-nba-raw`` checkout and compare full-frame against the real committed
oracle under ``hoopR-nba-data/nba/{dataset}/parquet/``. Building the season
once per test session (not once per test) keeps the suite from re-parsing
~1300 games' worth of JSON for every dataset that shares an input (schedules/
shots/player_season_stats all read another dataset's just-built parquet).

Skips (not fails) when the sibling raw checkout isn't present -- that's a
local dev-machine dependency, not something CI can clone (the raw repo is
large; see ``nba_data_build/ingest.py``).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from nba_data_build.build import build_season

SEASON = 2025

# hoopR-nba-data/python/tests/nba_data_build/conftest.py -> hoopR-nba-data
_THIS_REPO = Path(__file__).resolve().parents[3]
ORACLE_ROOT = _THIS_REPO / "nba"
RAW_ROOT = _THIS_REPO.parent / "hoopR-nba-raw"

_SEASON_BUILD_ORDER = [
    "team_box",
    "player_box",  # player_season_stats identity depends on this
    "schedules",  # depends on pbp/team_box/player_box game-id sets
    "shots",  # depends on pbp
    "rosters",
    "team_season_stats",
    "player_season_stats",
    "standings",
    "draft",
    "game_rosters",
    "officials",
]


def _skip_if_no_raw_checkout() -> None:
    if not RAW_ROOT.is_dir():
        pytest.skip(f"sibling hoopR-nba-raw checkout not found at {RAW_ROOT}")
    if not ORACLE_ROOT.is_dir():
        pytest.skip(f"oracle nba/ tree not found at {ORACLE_ROOT}")


@pytest.fixture(scope="session")
def built_base(tmp_path_factory) -> Path:
    """Build every raw-derived dataset for season 2025 into a shared tmp dir."""
    _skip_if_no_raw_checkout()
    base = tmp_path_factory.mktemp("nba_2025_build")
    # pbp first (nothing depends on schedules/team_box for it).
    build_season("pbp", SEASON, base=base, raw_root=RAW_ROOT)
    for dataset in _SEASON_BUILD_ORDER:
        build_season(dataset, SEASON, base=base, raw_root=RAW_ROOT)
    return base


def oracle_path(dataset: str, stem: str, season: int = SEASON) -> Path:
    return ORACLE_ROOT / dataset / "parquet" / f"{stem}_{season}.parquet"
