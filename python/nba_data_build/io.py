"""Dataset IO -- polars port of the R write + ``.append_manifest`` steps.

Writes ``{base}/{dataset}/parquet/{stem}_{season}.parquet`` and, for datasets
that write one (``spec.write_tree_csv``),
``{base}/{dataset}/csv/{stem}_{season}{csv_suffix}``. NBA/MBB never commit a
local csv for the per-game datasets (pbp/team_box/player_box) -- the R
``fwrite`` lines for those are commented out (only ``.rds``/``.parquet`` are
tree-committed); the release asset still ships a plain ``.csv``, generated
from the parquet at publish time (see ``publish._dataset_files``). Also
upserts the ``{league}_{dataset}_in_data_repo.csv`` manifest. ``.rds`` is R's
native format and is produced by the retained R serialize step (cutover); the
parity bar here is the parquet.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from nba_data_build._logging import get_logger, human_size
from nba_data_build.config import DatasetSpec

_LEAGUE = "nba"

log = get_logger()


def _utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def manifest_path(spec: DatasetSpec, base: Path) -> Path:
    return base / spec.dataset / f"{_LEAGUE}_{spec.dataset}_in_data_repo.csv"


def _append_manifest(spec: DatasetSpec, season: int, row_count: int, base: Path) -> Path | None:
    """Append one run's row to the dataset's manifest log (R ``fwrite(append=TRUE)``).

    The tree file is an append LOG -- one row per run, not per season (the real
    committed manifests carry one row per historical run). ``publish`` is what
    collapses it to one row per season for the release asset. Rewriting the
    tree file as an upsert here would silently destroy that published history.
    """
    if spec.manifest_endpoint is None:
        return None  # R does not manifest this dataset; see DatasetSpec
    f = manifest_path(spec, base)
    f.parent.mkdir(parents=True, exist_ok=True)
    row = pl.DataFrame(
        {
            "season": [int(season)],
            "row_count": [int(row_count)],
            "generated_at_utc": [_utc_now_str()],
            "source_endpoint": [spec.manifest_endpoint.format(season=season)],
        }
    )
    if f.exists():
        row = pl.concat([pl.read_csv(f), row], how="diagonal_relaxed")
    row.write_csv(f)
    return f


def write_dataset(df: pl.DataFrame, spec: DatasetSpec, season: int, *, base: str | Path = "nba") -> list[Path]:
    """Write parquet (+ csv when ``spec.write_tree_csv``) + manifest for one
    dataset/season; return the written paths."""
    base = Path(base)
    pq_dir = base / spec.dataset / "parquet"
    pq_dir.mkdir(parents=True, exist_ok=True)
    pq = pq_dir / f"{spec.stem}_{season}.parquet"
    df.write_parquet(pq)
    paths = [pq]
    csv_note = ""
    if spec.write_tree_csv:
        csv_dir = base / spec.dataset / "csv"
        csv_dir.mkdir(parents=True, exist_ok=True)
        csv = csv_dir / f"{spec.stem}_{season}{spec.csv_suffix}"
        df.write_csv(csv)
        paths.append(csv)
        csv_note = f" + {csv.name} ({human_size(csv.stat().st_size)})"
    manifest = _append_manifest(spec, season, df.height, base)
    log.info(
        "wrote %s (%s)%s, %d rows x %d cols; manifest %s",
        pq,
        human_size(pq.stat().st_size),
        csv_note,
        df.height,
        df.width,
        f"{manifest.name} appended" if manifest else "n/a (not manifested)",
    )
    return paths


def write_schedule_extras(master: pl.DataFrame, games: pl.DataFrame, *, base: str | Path = "nba") -> list[Path]:
    """Write the master-schedule extras under ``{base}/schedules/``.

    R never committed these (``sportsdataverse_save`` uploaded straight from
    the frame); the tree copy exists so the R serialize tail can produce the
    ``.rds`` assets. Files sit at the ``schedules/`` root -- NOT inside
    ``parquet/`` -- so the per-season glob in ``build_schedule_extras`` never
    picks the master back up.
    """
    root = Path(base) / "schedules"
    root.mkdir(parents=True, exist_ok=True)
    out: list[Path] = []
    for name, df in (("nba_schedule_master", master), ("nba_games_in_data_repo", games)):
        pq = root / f"{name}.parquet"
        csv = root / f"{name}.csv"
        df.write_parquet(pq)
        df.write_csv(csv)
        log.info(
            "wrote %s (%s), %d rows x %d cols",
            pq,
            human_size(pq.stat().st_size),
            df.height,
            df.width,
        )
        out.extend([pq, csv])
    return out
