#!/usr/bin/env bash
# Droplet step after /mnt/sdv_repos/bin/alt_daily.sh parses a season into hoopR-nba-alt-data:
# fill the committed schedule + player crosswalks' null Fox / Yahoo legs from it
# (`--fill-alt`, offline), then commit + publish ONLY the crosswalks that changed.
# cron_crosswalks.sh applies the same alt legs when it rebuilds the current season; this
# step makes a newly parsed day count without waiting for that rebuild, and backfills
# past seasons (-s 2002 -e 2026).
#
#   bash scripts/cron_alt_fill.sh -s YYYY [-e YYYY] [-n]    # -n = fill a temp copy + compare; no lock, no commit
#
# A season without a parsed Fox / Yahoo season or a committed crosswalk is a no-op.
set -uo pipefail
cd "$(dirname "$0")/.."
echo "=== cron_alt_fill $(date -u +%FT%TZ)"
LG=nba; PREFIX=NBA; START="" END="" DRY=0
ALT=${HOOPR_NBA_ALT_DATA_ROOT:-../hoopR-nba-alt-data}
while getopts s:e:n flag; do
  case "$flag" in s) START=$OPTARG;; e) END=$OPTARG;; n) DRY=1;; *) echo "usage: $0 -s YYYY [-e YYYY] [-n]" >&2; exit 2;; esac
done
[ -n "$START" ] || { echo "usage: $0 -s YYYY [-e YYYY] [-n]" >&2; exit 2; }
END=${END:-$START}
# The fill re-stamps the manifest csv on every run: never leave it (or a half-filled parquet) dirty,
# or the next cron_crosswalks.sh refuses to build.
restore() { [ "$DRY" = 1 ] || git checkout -q -- "$LG/crosswalk" 2>/dev/null; }
fail() { restore; echo "::error ::$*"; echo "EXIT=1"; exit 1; }

base="../$LG"
if [ "$DRY" = 1 ]; then   # a temp copy of the crosswalks: no lock, the tree is never touched
  tmp=$(mktemp -d); mkdir -p "$tmp/crosswalk/parquet"; base=$tmp
else
  # Same lock discipline as cron_crosswalks.sh: the git_pull sweep would autostash our outputs.
  exec 9>/tmp/git_pull_sdv.lock
  flock -w 3600 9 || fail "git_pull sweep held its lock for 1h"
  branch=$(git rev-parse --abbrev-ref HEAD)
  if [ "$branch" != main ] || [ -n "$(git status --porcelain --untracked-files=no)" ]; then
    git status --short --untracked-files=no | head -20
    fail "checkout is on '$branch' or has tracked changes; refusing to fill"
  fi
  git pull -q --ff-only 9>&- || fail "git pull --ff-only failed"
fi

export PYTHONUNBUFFERED=1 PYTHONIOENCODING=utf-8
fill() {  # dataset season [--publish]
  (cd python && uv run --quiet python -m ${LG}_data_build --dataset "$1" -s "$2" -e "$2" \
     --fill-alt --base "$base" --archive-root "../$LG" ${3:-}) 9>&-
}
changed=()
for season in $(seq "$START" "$END"); do
  ls "$ALT"/{fox,yahoo}/game_rosters/parquet/${LG}_*_game_rosters_"$season".parquet >/dev/null 2>&1 \
    || { echo "$season: no parsed Fox / Yahoo season; skipped"; continue; }
  for ds in schedule_crosswalk player_crosswalk; do
    f="$LG/crosswalk/parquet/${LG}_${ds}_$season.parquet"
    [ -f "$f" ] || { echo "$ds $season: no committed crosswalk; skipped"; continue; }
    if [ "$DRY" = 1 ]; then
      cp "$f" "$tmp/crosswalk/parquet/"
      fill "$ds" "$season" || fail "$ds $season fill failed"
      cmp -s "$f" "$tmp/crosswalk/parquet/${f##*/}" && echo "$ds $season: unchanged" || echo "$ds $season: WOULD CHANGE"
      continue
    fi
    fill "$ds" "$season" || fail "$ds $season fill failed"
    if git diff --quiet -- "$f"; then
      echo "$ds $season: unchanged"
      continue
    fi
    git diff --stat -- "$f"
    fill "$ds" "$season" --publish || fail "$ds $season publish failed"
    changed+=("$f")
  done
done
[ "$DRY" = 1 ] && { rm -rf "$tmp"; echo "dry run: no commit"; echo "EXIT=0"; exit 0; }
[ ${#changed[@]} -gt 0 ] || { restore; echo "crosswalks unchanged"; echo "EXIT=0"; exit 0; }

git add -- "${changed[@]}" "$LG"/crosswalk/*_in_data_repo.csv
git diff --cached --quiet && { echo "crosswalks unchanged"; echo "EXIT=0"; exit 0; }
git commit -q -m "$PREFIX Data Updated (Start: $START End: $END)" 9>&- || fail "commit failed"
for attempt in 1 2 3; do   # daily_nba.yml may push meanwhile; rebase --merge (parquet blobs)
  git push -q origin HEAD 9>&- && { echo "pushed $PREFIX $START-$END alt fill"; echo "EXIT=0"; exit 0; }
  git fetch -q origin main 9>&- && git rebase -q --merge origin/main 9>&- \
    || { git rebase --abort 2>/dev/null; fail "cannot rebase onto origin/main"; }
done
fail "push rejected 3 times"
