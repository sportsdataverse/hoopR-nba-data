#!/bin/bash
# Compile hoopR-nba-data datasets, per season (Python-first cutover).
#
# The 13 raw-derived datasets are built by `nba_data_build` (parity-validated
# port of espn_nba_01..10). Build order matters: shots project the built pbp
# parquet; schedules stamp flags from the built pbp/team_box/player_box
# parquets; player_season_stats reads the built player_box for identity.
# NBA has a draft dataset (MBB does not). All three crosswalks (nba_11-13)
# build in Python; `-l R` still runs their .R originals (see PY_CROSSWALKS).
# `.rds` is written natively by io.write_dataset in the same pass as the
# parquet, so there is no separate serialize step.
#
# Usage: bash scripts/daily_nba_data_processor.sh -s 2026 -e 2026 [-l python|R]
set -uo pipefail

# -l selects the language for the raw-derived datasets. Python is the
# production default; `-l R` is the rollback path that used to live in a
# separate daily_nba_R_processor.sh. One script, so the two paths cannot drift
# in season handling, logging or the load-bearing commit format.
while getopts s:e:l: flag; do
  case "${flag}" in
    s) START_YEAR=${OPTARG};;
    e) END_YEAR=${OPTARG};;
    l) LANG_MODE=${OPTARG};;
    *) echo "usage: $0 -s <start> [-e <end>] [-l python|R]" >&2; exit 2;;
  esac
done
START_YEAR=${START_YEAR:-}
END_YEAR=${END_YEAR:-$START_YEAR}
LANG_MODE=${LANG_MODE:-python}
if [ -z "$START_YEAR" ]; then
  echo "usage: $0 -s <start_year> [-e <end_year>] [-l python|R]" >&2
  exit 1
fi
case "$LANG_MODE" in
  python|R) ;;
  *) echo "::error ::unknown -l '$LANG_MODE' (expected python or R)" >&2; exit 2;;
esac

# CI has no local hoopR-nba-raw checkout -- read raw over HTTP (the reason the
# python builders are dual-mode Path|str). Override with HOOPR_NBA_RAW_ROOT.
RAW_ROOT="${HOOPR_NBA_RAW_ROOT:-https://raw.githubusercontent.com/sportsdataverse/hoopR-nba-raw/main}"

# Scrape-log conventions: unbuffered + utf-8 so nba_data_build's timestamped
# log lines land in the Actions console AND the tee'd season logfile live.
export PYTHONUNBUFFERED=1
export PYTHONIOENCODING=utf-8

# Dependency order: pbp/team_box/player_box first (schedules reads their
# game-id sets; shots read the pbp parquet), then the rest.
PY_DATASETS=(
    pbp
    team_box
    player_box
    player_core
    schedules
    shots
    rosters
    player_season_stats
    team_season_stats
    standings
    game_rosters
    officials
    draft
)
# The `-l R` rollback path. R has no counterpart for player_core, schedules or
# shots (espn_nba_01 writes the schedules + shots subsets inline) -- hence the
# gaps, which are deliberate. Draft (08) is annual cadence but cheap to re-run,
# and stays last to match the python dataset order.
R_DATASETS=(
    R/espn_nba_01_pbp_creation.R
    R/espn_nba_02_team_box_creation.R
    R/espn_nba_03_player_box_creation.R
    R/espn_nba_04_rosters_creation.R
    R/espn_nba_05_player_season_stats_creation.R
    R/espn_nba_06_team_season_stats_creation.R
    R/espn_nba_07_standings_creation.R
    R/espn_nba_09_game_rosters_creation.R
    R/espn_nba_10_officials_creation.R
    R/espn_nba_08_draft_creation.R
)
# Crosswalks (stages 11-13). All three now build in Python. 11 (team) and 13
# (player) were held back until sdv-py grew `fox_nba_teams` and populated
# `nba_team_abbreviation` from leaguegamelog (sdv-py #349); both then re-ran
# live-parity-clean against the R goldens in nba/crosswalk/parquet/:
#
#   team    30 rows, 30/30 on every one of the 21 columns; fox_team_id +
#           fox_team_name + nba_team_abbreviation populated 30/30.
#   player  548 live / 544 R, 536 joined, 529/536 (98.69%) on nba_player_id,
#           match_method and espn_team_id. The 7 are mid-season ESPN team
#           moves (Dort, Nembhard, Risacher, Broome, Caldwell-Pope, Tonje,
#           Jemison) whose ESPN team id moved after the golden was cut, so
#           the (team, name) key misses -- provider drift, not a port defect.
#
# `-l R` still runs ALL THREE R scripts unchanged (design D20 rollback).
PY_CROSSWALKS=(
    team_crosswalk
    schedule_crosswalk
    player_crosswalk
)
R_CROSSWALKS=(
    R/nba_11_team_crosswalk_creation.R
    R/nba_12_schedule_crosswalk_creation.R
    R/nba_13_player_crosswalk_creation.R
)

mkdir -p logs
ANY_FAILED=0

# Commit + push, surviving a remote that moved while the build was running.
#
# The previous form pulled BEFORE staging, which can only ever abort: the build
# has just rewritten the tracked parquet/csv files, so `git pull` refuses with
# "Your local changes would be overwritten by merge". It then committed anyway,
# pushed into a non-fast-forward rejection, and swallowed all of it -- a GREEN
# job that published nothing. Observed on hoopR-nba-data run 32204419012
# (2026-08-19), and on wehoop-wnba-data runs 32192069433 + 32192069566.
#
# Order matters: stage and commit FIRST so the tree is clean, and only then
# reconcile with origin. `rebase --merge` rather than `pull --rebase` because
# git's default am backend base64-encodes every parquet blob it replays.
sdv_commit_push() {
  local msg="$1"; shift
  git add -- "$@" >/dev/null 2>&1 || true
  if git diff --cached --quiet; then
    echo "nothing to commit for: $msg"
    return 0
  fi
  git commit -m "$msg" >/dev/null || { echo "::warning ::commit failed: $msg"; return 1; }
  local attempt
  for attempt in 1 2 3; do
    if git push origin HEAD >/dev/null 2>&1; then
      echo "pushed: $msg (attempt $attempt)"
      return 0
    fi
    echo "push rejected (attempt $attempt); syncing with origin"
    git fetch --quiet origin main || true
    if ! git rebase --merge origin/main >/dev/null 2>&1; then
      git rebase --abort >/dev/null 2>&1 || true
      echo "::error ::cannot rebase onto origin/main for: $msg"
      return 1
    fi
  done
  echo "::error ::push still rejected after 3 attempts: $msg"
  return 1
}

for i in $(seq "${START_YEAR}" "${END_YEAR}")
do
    LOGFILE="logs/hoopR_nba_data_logfile_${i}.log"
    TMPLOG=$(mktemp "/tmp/hoopR_nba_data_logfile_${i}.XXXXXX.log")
    echo "=== Processing NBA data ($LANG_MODE) for season $i ==="
    # Tee inside the block writes to /tmp (untracked) so the `git pull` calls
    # don't trip over their own log output being written to a tracked file.
    # The block records the worst exit code (RSCRIPT_RC) so a failed compile
    # is surfaced to the workflow rather than masked by a successful git push.
    {
        git pull >> /dev/null
        git config --local user.email "action@github.com"
        git config --local user.name "Github Action"
        SEASON_RC=0

        # ::group:: markers collapse each dataset in the Actions UI; in the
        # tee'd season logfile they read as plain section headers.
        run_py() {
            local ds="$1"
            echo "::group::nba_data_build $ds $i"
            # Run inside python/ so the flat nba_data_build package is importable
            # (it is not pip-installed; found via CWD/pythonpath). --base ../nba
            # writes into the repo-root nba/ tree.
            ( cd python && uv run python -m nba_data_build \
                --dataset "$ds" -s "$i" -e "$i" --base ../nba --raw-root "$RAW_ROOT" --publish ) || {
                rc=$?
                echo "::warning ::nba_data_build $ds for season $i exited with code $rc"
                SEASON_RC=$rc
            }
            echo "::endgroup::"
        }
        run_r() {
            local script="$1"
            echo "::group::$script $i"
            Rscript "$script" -s "$i" -e "$i" || {
                rc=$?
                echo "::warning ::$script for season $i exited with code $rc"
                SEASON_RC=$rc
            }
            echo "::endgroup::"
        }

        if [ "$LANG_MODE" = "R" ]; then
            for SCRIPT in "${R_DATASETS[@]}"; do run_r "$SCRIPT"; done
        else
            for DS in "${PY_DATASETS[@]}"; do run_py "$DS"; done
        fi

        # Crosswalks build from LIVE ESPN+NBA Stats+Fox sources and are
        # known-fragile (timeouts on external flakiness). Best-effort in both
        # modes: a crosswalk failure warns but does NOT fail the run -- the
        # core datasets are the daily deliverable and publish independently
        # above.
        if [ "$LANG_MODE" = "R" ]; then
            for SCRIPT in "${R_CROSSWALKS[@]}"
            do
                echo "::group::$SCRIPT $i"
                Rscript "$SCRIPT" -s "$i" -e "$i" || echo "::warning ::$SCRIPT for season $i exited with code $? (crosswalk; non-fatal, live external source)"
                echo "::endgroup::"
            done
        else
            for DS in "${PY_CROSSWALKS[@]}"
            do
                echo "::group::nba_data_build $DS $i"
                ( cd python && uv run python -m nba_data_build \
                    --dataset "$DS" -s "$i" -e "$i" --base ../nba --raw-root "$RAW_ROOT" --publish ) \
                    || echo "::warning ::nba_data_build $DS for season $i exited with code $? (crosswalk; non-fatal, live external source)"
                echo "::endgroup::"
            done
        fi

        echo "RSCRIPT_RC=$SEASON_RC" > "/tmp/_rscript_rc_${i}"
        # Grep-able terminal line for the season logfile (scrape-log convention).
        echo "season $i EXIT=$SEASON_RC"
        # Commit whatever datasets succeeded even if one step errored -- the
        # per-dataset error handling keeps partial output usable.
        # Load-bearing subject: downstream tooling parses the years out of it.
        sdv_commit_push "NBA Data Updated (Start: $i End: $i)" nba . || PUSH_RC=1
    } 2>&1 | tee "$TMPLOG"
    RSCRIPT_RC=$(sed 's/RSCRIPT_RC=//' "/tmp/_rscript_rc_${i}" 2>/dev/null)
    rm -f "/tmp/_rscript_rc_${i}"

    # Block is finished and pushed; tee has closed $TMPLOG. Now copy the log
    # into its tracked location and commit/push it on its own.
    cp "$TMPLOG" "$LOGFILE"
    git stash -u --quiet 2>/dev/null || true
    git pull --rebase >> /dev/null || true
    git stash pop --quiet 2>/dev/null || true
    git add "$LOGFILE"
    git commit -m "NBA Data log update (Start: $i End: $i)" >> /dev/null || echo "No log changes to commit"
    git push >> /dev/null
    rm -f "$TMPLOG"

    # Propagate any non-zero exit code so the workflow reports failure.
    # Don't `exit` immediately -- iterate the rest of the requested seasons.
    if [ "${RSCRIPT_RC:-0}" != "0" ]; then
        echo "::error ::At least one creation step for season $i exited with code $RSCRIPT_RC"
        ANY_FAILED=1
    fi
done

# ---- Run summary: updated releases + remaining warnings/errors ----
# Prints a cli summary to the Action log and (when set) writes markdown to
# $GITHUB_STEP_SUMMARY so the run's Summary tab shows what landed and what didn't.
if [ "$LANG_MODE" = "R" ]; then
    Rscript R/run_summary.R -s "$START_YEAR" -e "$END_YEAR" || true
else
    ( cd python && uv run python -m nba_data_build.summary --logs ../logs -s "$START_YEAR" -e "$END_YEAR" ) || true
fi

if [ "${ANY_FAILED:-0}" != "0" ]; then
    echo "::error ::At least one season's creation step exited non-zero. See per-season logs."
    exit 1
fi

# A rejected push is a FAILED run, not a green one. Release assets upload on a
# separate path and can succeed while the repo mirror is left stale.
if [ "${PUSH_RC:-0}" != "0" ]; then
  echo "::error ::At least one commit failed to reach origin; the repo mirror is stale."
  exit 1
fi
