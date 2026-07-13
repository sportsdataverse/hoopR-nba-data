"""Release publishing -- per-file ``gh release upload --clobber`` (create-if-missing).

Port of the R ``sportsdataverse_save`` upload. Multi-asset globs silently drop
large files, so upload one file at a time. ``runner``/``exists_check`` are
injectable for hermetic tests.

NBA deltas vs the WNBA publisher: pbp/team_box/player_box never commit a
local csv (``spec.write_tree_csv=False`` -- R's ``fwrite`` for those is
commented out), so their release-asset csv is generated from the parquet into
a temp file at publish time instead of read off the tree; the per-dataset
``nba_<ds>_in_data_repo.csv`` manifest is uploaded alongside for the
manifested datasets.
"""

from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path
from typing import Callable

import polars as pl

from nba_data_build import io as build_io
from nba_data_build._logging import get_logger, human_size
from nba_data_build.config import DatasetSpec

_LEAGUE = "nba"

DEFAULT_REPO = "sportsdataverse/sportsdataverse-data"

log = get_logger()


def _gh(args: list[str]) -> None:
    # timeout so a hung gh (auth prompt, network stall, rate-limit backoff) can't
    # block an unattended pipeline step indefinitely. Args are internal literals /
    # controlled fields passed as a list (no shell=True) -- the SAST injection flag
    # is a false positive.
    subprocess.run(["gh", *args], check=True, timeout=120)


def _gh_release_exists(tag: str, repo: str) -> bool:
    return (
        subprocess.run(
            ["gh", "release", "view", tag, "--repo", repo],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=60,
        ).returncode
        == 0
    )


def _manifest_asset(spec: DatasetSpec, base: Path) -> Path | None:
    """Collapse the manifest append-log to one row per season for the release.

    R's manifest upload helper uploads
    ``distinct(season, .keep_all = TRUE) %>% arrange(season)`` -- and dplyr's
    distinct keeps the FIRST occurrence, so the published row_count freezes at
    a season's first-ever run. We keep the LATEST row per season instead, so
    the manifest describes the asset actually published alongside it. That is
    a deliberate divergence from R; completed seasons are unaffected (their
    first run == their last).
    """
    src = build_io.manifest_path(spec, base)
    if spec.manifest_endpoint is None or not src.exists():
        return None
    latest = (
        pl.read_csv(src).unique(subset=["season"], keep="last", maintain_order=True).sort("season")
    )
    tmp = Path(tempfile.mkdtemp(prefix="nba_manifest_")) / src.name
    latest.write_csv(tmp)
    return tmp


def _dataset_files(spec: DatasetSpec, season: int, base: Path) -> list[Path]:
    root = base / spec.dataset
    pq = root / "parquet" / f"{spec.stem}_{season}.parquet"
    files = [pq] if pq.exists() else []
    if spec.write_tree_csv:
        csv = root / "csv" / f"{spec.stem}_{season}.csv"
        if csv.exists():
            files.append(csv)
    elif pq.exists():
        # No committed tree csv (pbp/team_box/player_box) -- the release
        # asset contract still ships a plain .csv, generated on the fly.
        tmp = Path(tempfile.mkdtemp(prefix="nba_publish_")) / f"{spec.stem}_{season}.csv"
        pl.read_parquet(pq).write_csv(tmp)
        files.append(tmp)
    # Manifest asset name == file name (R manifest_upload_helper contract).
    manifest = _manifest_asset(spec, base)
    if manifest is not None:
        files.append(manifest)
    return files


def publish_dataset(
    spec: DatasetSpec,
    season: int,
    *,
    base: str | Path = "nba",
    repo: str = DEFAULT_REPO,
    dry_run: bool = False,
    runner: Callable[[list[str]], None] | None = None,
    exists_check: Callable[[str, str], bool] | None = None,
) -> dict:
    """Upload a dataset/season's parquet + csv to the release, creating it if missing.

    Args:
        spec: Dataset spec (``dataset``/``stem``/``tag``) from ``config.REGISTRY``.
        season: Season year; must match the files already written by ``io.write_dataset``.
        base: Root directory containing ``{dataset}/{parquet,csv}/...``.
        repo: ``owner/repo`` slug for the release target.
        dry_run: If True, skip all ``gh`` calls and print the would-be uploads.
        runner: Injectable ``gh`` arg-list executor; defaults to a real subprocess call.
        exists_check: Injectable ``(tag, repo) -> bool`` release-existence check.

    Returns:
        dict: ``{"tag": ..., "files": [...], "uploaded": <count>}``.

    Example:
        Quick start::

            from nba_data_build.config import REGISTRY
            from nba_data_build import publish
            publish.publish_dataset(REGISTRY["team_box"], 2025)
    """
    run = runner or _gh
    exists = exists_check or _gh_release_exists
    files = _dataset_files(spec, season, Path(base))
    if not files:
        log.warning("%s %s: no files to publish under %s", spec.dataset, season, base)
    if not dry_run and not exists(spec.tag, repo):
        log.info("release %s missing on %s -- creating it", spec.tag, repo)
        run(
            [
                "release",
                "create",
                spec.tag,
                "--repo",
                repo,
                "--title",
                spec.tag,
                "--notes",
                f"{spec.tag} (NBA dataset, Python-built).",
            ]
        )
    count = 0
    for f in files:
        size = human_size(f.stat().st_size)
        if dry_run:
            log.info("[dry-run] upload %s (%s) -> %s:%s", f, size, repo, spec.tag)
            continue
        log.info("uploading %s (%s) -> %s:%s", f.name, size, repo, spec.tag)
        run(["release", "upload", spec.tag, str(f), "--repo", repo, "--clobber"])
        count += 1
        log.info("uploaded %s -> %s (asset %d/%d)", f.name, spec.tag, count, len(files))
    return {"tag": spec.tag, "files": [str(f) for f in files], "uploaded": count}


def publish_files(
    tag: str,
    files: list[Path],
    *,
    repo: str = DEFAULT_REPO,
    dry_run: bool = False,
    runner: Callable[[list[str]], None] | None = None,
) -> dict:
    """Upload arbitrary already-written files to a release tag (extras path).

    Used for the season-independent schedule extras
    (``nba_schedule_master`` + ``nba_games_in_data_repo``) that
    ``espn_nba_03_player_box_creation.R`` publishes to the schedules tag.
    The tag is assumed to exist (the per-season publish creates it).

    Args:
        tag: Release tag to upload to.
        files: Files to upload; the file name becomes the asset name.
        repo: ``owner/repo`` slug for the release target.
        dry_run: If True, log the would-be uploads without calling ``gh``.
        runner: Injectable ``gh`` arg-list executor for hermetic tests.

    Returns:
        dict: ``{"tag": ..., "files": [...], "uploaded": <count>}``.
    """
    run = runner or _gh
    count = 0
    for f in files:
        size = human_size(f.stat().st_size)
        if dry_run:
            log.info("[dry-run] upload %s (%s) -> %s:%s", f, size, repo, tag)
            continue
        log.info("uploading %s (%s) -> %s:%s", f.name, size, repo, tag)
        run(["release", "upload", tag, str(f), "--repo", repo, "--clobber"])
        count += 1
        log.info("uploaded %s -> %s (asset %d/%d)", f.name, tag, count, len(files))
    return {"tag": tag, "files": [str(f) for f in files], "uploaded": count}
