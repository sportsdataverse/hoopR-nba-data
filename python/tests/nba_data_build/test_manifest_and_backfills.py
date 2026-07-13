"""The release-asset contracts no season-level oracle happens to exercise in
isolation: manifest endpoints (incl. the two NBA deltas -- officials pointing
at game_rosters, player_season_stats carrying no {season} segment) and the
team_box ``largest_lead`` backfill.
"""

import polars as pl

from nba_data_build.config import REGISTRY
from nba_data_build.reshapers import team_box_season_postprocess

_RAW = "https://raw.githubusercontent.com/sportsdataverse/hoopR-nba-raw/main/nba"

EXPECTED_ENDPOINT = {
    "shots": "derived from espn_nba pbp",
    "rosters": f"{_RAW}/team_rosters/json/2026/<team_id>.json",
    # NBA delta: flat raw payload -- no {season} segment.
    "player_season_stats": f"{_RAW}/player_season_stats/json/<athlete_id>.json",
    "team_season_stats": f"{_RAW}/team_stats/json/2026/<team_id>.json",
    "standings": f"{_RAW}/standings/json/2026.json",
    "draft": f"{_RAW}/draft/json/2026.json",
    "game_rosters": f"{_RAW}/game_rosters/json/<game_id>.json",
    # NBA delta: officials have no dedicated raw dir -- projected from
    # game_rosters.
    "officials": f"{_RAW}/game_rosters/json/<game_id>.json",
}


def test_exactly_the_manifested_datasets_have_a_manifest():
    manifested = {k for k, v in REGISTRY.items() if v.manifest_endpoint is not None}
    assert manifested == set(EXPECTED_ENDPOINT)


def test_manifest_endpoints_match_the_committed_r_output():
    for dataset, expected in EXPECTED_ENDPOINT.items():
        spec = REGISTRY[dataset]
        assert spec.manifest_endpoint is not None
        assert spec.manifest_endpoint.format(season=2026) == expected, dataset


def test_pbp_team_box_player_box_never_write_a_tree_csv():
    # R's fwrite for these three is commented out in the NBA/MBB scripts.
    for dataset in ("pbp", "team_box", "player_box"):
        assert REGISTRY[dataset].write_tree_csv is False, dataset


def test_team_box_backfills_largest_lead_when_the_payload_union_lacks_it():
    without = pl.DataFrame({"game_id": [1, 2], "team_id": [3, 4]})
    out = team_box_season_postprocess(without)
    assert out.columns[-1] == "largest_lead"  # R relocates it to last
    assert out.schema["largest_lead"] == pl.Utf8
    assert out.get_column("largest_lead").to_list() == [None, None]


def test_team_box_backfill_leaves_a_season_that_ships_largest_lead_alone():
    with_ll = pl.DataFrame({"game_id": [1], "largest_lead": ["12"], "team_id": [3]})
    out = team_box_season_postprocess(with_ll)
    assert out.columns == ["game_id", "largest_lead", "team_id"]  # untouched
    assert out.get_column("largest_lead").to_list() == ["12"]
