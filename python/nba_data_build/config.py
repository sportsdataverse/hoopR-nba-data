"""Dataset registry -- one row per released NBA dataset.

Mirrors each ``espn_nba_NN_*_creation.R`` script: ``(dataset, stem, tag,
reshaper)`` where ``reshaper`` keys into ``nba_data_build.reshapers.RESHAPERS``.
Tags are verbatim from ``hoopR::load_nba_*`` URL builders -- do not rename.

League deltas vs the wehoop-wnba-data template (see hoopR-nba-data/R):

* NBA officials have no dedicated raw directory -- they are projected from
  the ``game_rosters/json`` sidecar's ``gameInfo.officials[]`` (script 10),
  so the officials manifest endpoint points at ``game_rosters``, not
  ``officials``.
* ``player_season_stats`` raw payloads are flat (``<athlete_id>.json``, no
  ``{season}/`` partition -- ESPN ships the athlete's whole career), so its
  manifest endpoint carries no ``{season}`` segment either.
* NBA/MBB creation scripts never write the local ``.csv``/``.csv.gz`` tree
  copy for the per-game datasets (the R ``fwrite`` lines are commented out --
  only ``.rds``/``.parquet`` are committed); the release asset still ships a
  plain ``.csv``, generated on the fly at publish time. ``write_tree_csv``
  captures that: ``False`` for pbp/team_box/player_box.
* Draft runs DAILY for NBA (not annual like WNBA) but the builder shape is
  identical.
"""

from __future__ import annotations

from dataclasses import dataclass

RAW_ROOT_ENV = "HOOPR_NBA_RAW_ROOT"  # sibling hoopR-nba-raw checkout root
_T = "espn_nba_"

# The manifest's source_endpoint records the PUBLIC raw URL the dataset was
# compiled from -- verbatim what the R scripts glue -- regardless of whether
# this run actually read from a local checkout or over HTTP.
_RAW = "https://raw.githubusercontent.com/sportsdataverse/hoopR-nba-raw/main/nba"

# --- rds contract -------------------------------------------------------------
# hoopR::load_nba_* reads .rds EXCLUSIVELY (11 rds_from_url call sites in
# hoopR/R/load_nba.R, zero .parquet references), so the rds is not a courtesy
# format -- it is the R package's entire read path. Python writes it natively
# via sportsdataverse._rds.write_rds (byte-validated against R's saveRDS);
# there is no R serialize step.
#
# These reproduce hoopR:::make_hoopR_data() + sportsdataversedata::
# sportsdataverse_save() exactly, in the attribute order every published asset
# already carries: class, hoopR_timestamp, hoopR_type, sportsdataverse_type,
# sportsdataverse_timestamp. The class is load-bearing -- hoopR registers
# print.hoopR_data on it (hoopR/R/utils.R:645).
RDS_CLASS: tuple[str, ...] = ("hoopR_data", "tbl_df", "tbl", "data.table", "data.frame")
RDS_ATTR_PREFIX = "hoopR"
RDS_TYPE_TEMPLATE = "ESPN NBA {dataset} from hoopR data repository"


@dataclass(frozen=True)
class DatasetSpec:
    """How to build one released dataset.

    Attributes:
        dataset: directory name under ``nba/`` and the manifest key.
        stem: output file stem (``{stem}_{season}.parquet`` / ``.csv``).
        tag: the ``sportsdataverse-data`` release tag (load-bearing).
        reshaper: key into ``reshapers.RESHAPERS``.
        csv_suffix: tree csv extension for datasets that DO write one.
        write_tree_csv: whether ``io.write_dataset`` commits a local csv
            copy at all. NBA/MBB never commit the per-game datasets' csv
            (R's fwrite lines are commented out) -- the release asset is
            still plain ``.csv``, produced from the parquet at publish time.
        manifest_endpoint: ``source_endpoint`` template for the dataset's
            manifest row (``{season}`` is substituted), or None for the
            datasets R does NOT manifest.
    """

    dataset: str
    stem: str
    tag: str
    reshaper: str
    csv_suffix: str = ".csv"
    write_tree_csv: bool = True
    manifest_endpoint: str | None = None


REGISTRY: dict[str, DatasetSpec] = {
    "pbp": DatasetSpec("pbp", "play_by_play", _T + "pbp", "pbp", write_tree_csv=False),
    "schedules": DatasetSpec("schedules", "nba_schedule", _T + "schedules", "schedules"),
    "shots": DatasetSpec(
        "shots",
        "shots",
        _T + "shots",
        "shots",
        manifest_endpoint="derived from espn_nba pbp",
    ),
    "team_box": DatasetSpec(
        "team_box", "team_box", _T + "team_boxscores", "team_box", write_tree_csv=False
    ),
    "player_box": DatasetSpec(
        "player_box", "player_box", _T + "player_boxscores", "player_box", write_tree_csv=False
    ),
    "rosters": DatasetSpec(
        "rosters",
        "rosters",
        _T + "rosters",
        "rosters",
        manifest_endpoint=_RAW + "/team_rosters/json/{season}/<team_id>.json",
    ),
    "player_season_stats": DatasetSpec(
        "player_season_stats",
        "player_season_stats",
        _T + "player_season_stats",
        "player_season_stats",
        # NB: no {season} segment -- the raw payload is flat/full-career.
        manifest_endpoint=_RAW + "/player_season_stats/json/<athlete_id>.json",
    ),
    # Athlete identity + bio. NEW dataset -- no R creation script exists, and
    # nothing published this before: the player_season_stats payload carries no
    # identity at all (not even the athlete id -- only the filename does).
    # Raw is flat/athlete-keyed like player_season_stats, so no {season}
    # segment; "who played in season Y" comes from the built player_box.
    "player_core": DatasetSpec(
        "player_core",
        "player_core",
        _T + "player_core",
        "player_core",
        # NO manifest_endpoint: a manifest is the contract for an R
        # load_nba_<ds>_manifest() loader, and player_core has no loader yet --
        # manifesting it would publish an asset nothing reads.
    ),
    "team_season_stats": DatasetSpec(
        "team_season_stats",
        "team_season_stats",
        _T + "team_season_stats",
        "team_season_stats",
        # NB: the raw dir is team_stats, not team_season_stats.
        manifest_endpoint=_RAW + "/team_stats/json/{season}/<team_id>.json",
    ),
    "standings": DatasetSpec(
        "standings",
        "standings",
        _T + "standings",
        "standings",
        manifest_endpoint=_RAW + "/standings/json/{season}.json",
    ),
    "game_rosters": DatasetSpec(
        "game_rosters",
        "game_rosters",
        _T + "game_rosters",
        "game_rosters",
        manifest_endpoint=_RAW + "/game_rosters/json/<game_id>.json",
    ),
    "officials": DatasetSpec(
        "officials",
        "officials",
        _T + "officials",
        "officials",
        # NBA has no nba/officials/ raw dir -- officials are projected from
        # the game_rosters sidecar (espn_nba_10_officials_creation.R).
        manifest_endpoint=_RAW + "/game_rosters/json/<game_id>.json",
    ),
    # NBA draft runs DAILY (not annual like WNBA), but the raw source and
    # builder shape are identical: a single nba/draft/json/{year}.json.
    "draft": DatasetSpec(
        "draft",
        "draft",
        _T + "draft",
        "draft",
        manifest_endpoint=_RAW + "/draft/json/{season}.json",
    ),
    # crosswalks -- all three publish to the shared release tag "nba_crosswalk"
    # (not the per-dataset espn_nba_* prefix used by the per-game datasets
    # above); stems match each script's `file_name = glue::glue("nba_{...}_crosswalk_{y}")`.
    "team_crosswalk": DatasetSpec(
        "team_crosswalk", "nba_team_crosswalk", "nba_crosswalk", "team_crosswalk"
    ),
    "schedule_crosswalk": DatasetSpec(
        "schedule_crosswalk", "nba_schedule_crosswalk", "nba_crosswalk", "schedule_crosswalk"
    ),
    "player_crosswalk": DatasetSpec(
        "player_crosswalk", "nba_player_crosswalk", "nba_crosswalk", "player_crosswalk"
    ),
}
