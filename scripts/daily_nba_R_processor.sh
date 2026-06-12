#!/bin/bash
# Process ESPN NBA datasets from hoopR-nba-raw repo
# Usage: bash scripts/daily_nba_R_processor.sh -s 2025 -e 2025

while getopts s:e: flag
do
    case "${flag}" in
        s) START_YEAR=${OPTARG};;
        e) END_YEAR=${OPTARG};;
    esac
done

if [ -z "$START_YEAR" ] || [ -z "$END_YEAR" ]; then
    echo "Usage: $0 -s <start_year> -e <end_year>"
    exit 1
fi

# Creation scripts run, in order, per season. The pbp script (01) also writes
# the schedules + shots subsets; officials (10) reuses the game_rosters
# summary JSON; draft (08) is annual cadence but cheap to re-run.
SCRIPTS=(
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
    R/nba_11_team_crosswalk_creation.R
    R/nba_12_schedule_crosswalk_creation.R
    R/nba_13_player_crosswalk_creation.R
)

mkdir -p logs
for i in $(seq "${START_YEAR}" "${END_YEAR}")
do
    LOGFILE="logs/hoopR_nba_data_logfile_${i}.log"
    TMPLOG=$(mktemp "/tmp/hoopR_nba_data_logfile_${i}.XXXXXX.log")
    echo "=== Processing NBA data for season $i ==="
    # Tee inside the block writes to /tmp (untracked) so the `git pull` calls
    # don't trip over their own log output being written to a tracked file.
    # The block records the worst R exit code (RSCRIPT_RC) so a failed compile
    # is surfaced to the workflow rather than masked by a successful git push.
    {
        git pull >> /dev/null
        git config --local user.email "action@github.com"
        git config --local user.name "Github Action"
        SEASON_RC=0
        for SCRIPT in "${SCRIPTS[@]}"
        do
            Rscript "$SCRIPT" -s $i -e $i || {
                rc=$?
                echo "::warning ::$SCRIPT for season $i exited with code $rc"
                SEASON_RC=$rc
            }
        done
        echo "RSCRIPT_RC=$SEASON_RC" > "/tmp/_rscript_rc_${i}"
        # Commit whatever datasets succeeded even if one script errored -- the
        # per-dataset tryCatch in each R script keeps partial output usable.
        git pull >> /dev/null
        git add nba/* >> /dev/null
        git pull >> /dev/null
        git add . >> /dev/null
        git commit -m "NBA Data Updated (Start: $i End: $i)" || echo "No changes to commit"
        git pull >> /dev/null
        git push >> /dev/null
    } 2>&1 | tee "$TMPLOG"
    RSCRIPT_RC=$(cat "/tmp/_rscript_rc_${i}" 2>/dev/null | sed 's/RSCRIPT_RC=//')
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

    # Propagate any non-zero R exit code so the workflow reports failure.
    # Don't `exit` immediately -- iterate the rest of the requested seasons.
    if [ "${RSCRIPT_RC:-0}" != "0" ]; then
        echo "::error ::At least one creation script for season $i exited with code $RSCRIPT_RC"
        ANY_FAILED=1
    fi
done

# ---- Run summary: updated releases + remaining warnings/errors ----
# Prints a cli summary to the Action log and (when set) writes markdown to
# $GITHUB_STEP_SUMMARY so the run's Summary tab shows what landed and what didn't.
Rscript R/run_summary.R -s "$START_YEAR" -e "$END_YEAR" || true

if [ "${ANY_FAILED:-0}" != "0" ]; then
    echo "::error ::At least one season's creation script exited non-zero. See per-season logs."
    exit 1
fi
