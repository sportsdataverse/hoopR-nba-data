#!/usr/bin/env bash
# Weekly R <-> Python OUTPUT parity for one dataset/season.
#
# Port of wehoop-wnba-data/ops/output_parity.sh (the proven WNBA sibling).
# tests/test_r_python_parity.py is the CONTRACT gate: do both languages declare
# the same datasets under the same stage numbers. It never opens the data. This
# is the other half -- do the two pipelines produce the same VALUES -- and it is
# weekly rather than per-push because the R side is not cheap (see below).
#
# Why both sides are rebuilt into temp dirs rather than read from the repo:
# the two chains write to the SAME `nba/<dataset>/{rds,parquet}/` path and
# clobber each other, so the checked-in tree only ever holds whichever ran last.
# Comparing it against the release asset compares a build to itself.
#
# Why the R side runs a chain and not one stage: the numbered R stages feed each
# other. espn_nba_02_team_box_creation.R (and 03_player_box) read
# `nba/schedules/rds/nba_schedule_{y}.rds`, which only stage 01 (pbp) writes,
# and the repo does not retain schedules/rds. So every stage up to and
# including the target must run, in order. That is the cost that makes this
# weekly. (Verified by reading each stage's readRDS/saveRDS calls -- not
# assumed from the WNBA sibling's shape.)
#
# Stage 08 (draft) runs AFTER stage 10 (officials) in the daily driver
# (scripts/daily_nba_R_processor.sh) for cadence reasons -- draft is annual.
# That reordering does not affect this script: team_box/player_box/pbp are
# stages 01-03, well below the 08/10 swap, so the plain numeric-order chain
# below is correct for every dataset this script currently supports.
#
# STEM differs from DATASET for pbp: the dataset key is "pbp" but both R and
# Python write `play_by_play_{season}.parquet` (verified in both
# python/nba_data_build/config.py and the R stage's saveRDS calls) -- NOT
# `pbp_{season}.parquet`. Getting this wrong silently degrades to "expected
# artifact missing" for the one dataset most worth checking.
#
# Neither pipeline is authoritative. A divergence is a review item: decide which
# side is right, then fix the other. Do not "fix" it by editing one to match.
#
# Usage:
#   ops/output_parity.sh -d team_box -s 2025
#   RSCRIPT="/c/Program Files/R/R-4.6.1/bin/Rscript.exe" ops/output_parity.sh -d team_box -s 2025
#
# Env:
#   RSCRIPT              R interpreter (default: Rscript on PATH). On the dev
#                        box, bare Rscript is 4.5.3 whose library lacks
#                        rlang/dplyr/arrow/hoopR -- point this at 4.6.x.
#   HOOPR_NBA_RAW_ROOT   raw store (default: the raw.githubusercontent base).
set -uo pipefail

DATASET=""
SEASON=""
while getopts "d:s:" flag; do
  case "${flag}" in
    d) DATASET=${OPTARG} ;;
    s) SEASON=${OPTARG} ;;
    *) echo "Usage: $0 -d <dataset> -s <season>" >&2; exit 2 ;;
  esac
done
if [ -z "${DATASET}" ] || [ -z "${SEASON}" ]; then
  echo "Usage: $0 -d <dataset> -s <season>" >&2
  exit 2
fi

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RSCRIPT="${RSCRIPT:-Rscript}"
export HOOPR_NBA_RAW_ROOT="${HOOPR_NBA_RAW_ROOT:-https://raw.githubusercontent.com/sportsdataverse/hoopR-nba-raw/main}"

# Join keys are MEASURED, not assumed -- each pair below was verified unique on
# a real season (2010/2015/2024/2025 team_box/player_box/pbp parquets in this
# repo's own nba/ tree). Getting this wrong is not a soft failure: a
# non-identifying key fans the comparison join into a cross product.
# (game_id, athlete_id) IS unique for NBA player_box (unlike hoopR-mbb-data /
# wehoop-wbb-data, where a small number of games list the same athlete_id under
# two different team_ids and the key needs team_id added -- verified different
# per repo, not copied from the WNBA sibling).
case "${DATASET}" in
  team_box)   JOIN_KEYS="game_id team_id" ;;
  player_box) JOIN_KEYS="game_id athlete_id" ;;
  pbp)        JOIN_KEYS="game_id id" ;;
  *)
    echo "::error ::no verified join key for '${DATASET}'." >&2
    echo "Add one only after confirming it is unique on a real season -- do not guess." >&2
    exit 2
    ;;
esac

# The output file STEM can differ from the dataset key (pbp -> play_by_play).
case "${DATASET}" in
  pbp) STEM="play_by_play" ;;
  *)   STEM="${DATASET}" ;;
esac

STAGE_FILE="$(ls "${REPO_DIR}"/R/espn_nba_[0-9][0-9]_"${DATASET}"_creation.R 2>/dev/null | head -1)"
if [ -z "${STAGE_FILE}" ]; then
  echo "::error ::no R stage found for dataset '${DATASET}'" >&2
  exit 2
fi
TARGET_NN="$(basename "${STAGE_FILE}" | sed -E 's/^espn_nba_([0-9]{2})_.*/\1/')"

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT
PY_OUT="${WORK}/py"
R_OUT="${WORK}/r"
mkdir -p "${PY_OUT}" "${R_OUT}"

echo "=== python build: ${DATASET} ${SEASON} ==="
# nba_data_build is not pip-installed; it is only importable with python/ as
# cwd (see scripts/daily_nba_python_processor.sh). --base is an absolute temp
# path so `cd` does not disturb it.
( cd "${REPO_DIR}/python" && uv run python -m nba_data_build \
    --dataset "${DATASET}" --base "${PY_OUT}" -s "${SEASON}" -e "${SEASON}" ) || {
  echo "::error ::python build failed for ${DATASET} ${SEASON}" >&2; exit 1; }

echo "=== R chain: stages 01..${TARGET_NN} (they feed each other) ==="
# The R stages call dir.create() one level at a time, which is NOT recursive, so
# `nba/schedules` fails outright when `nba/` is absent. In the repo that dir is
# tracked and always present; in a clean temp tree it is not. Seed it.
mkdir -p "${R_OUT}/nba"
cd "${R_OUT}" || exit 1
for f in "${REPO_DIR}"/R/espn_nba_[0-9][0-9]_*_creation.R; do
  nn="$(basename "$f" | sed -E 's/^espn_nba_([0-9]{2})_.*/\1/')"
  # 10#: force base-10 so a leading zero is not read as octal.
  if [ "$((10#${nn}))" -le "$((10#${TARGET_NN}))" ]; then
    echo "--- Rscript $(basename "$f")"
    # NEVER invoke the stage directly: it publishes to the live release with no
    # dry-run gate. The wrapper replaces the publisher before sourcing it and
    # aborts if that swap fails, so this fails closed.
    SDV_PARITY_STAGE="$f" "${RSCRIPT}" "${REPO_DIR}/ops/_r_no_publish.R" \
      -s "${SEASON}" -e "${SEASON}" || {
      echo "::error ::R stage $(basename "$f") failed" >&2; exit 1; }
  fi
done

R_PARQUET="${R_OUT}/nba/${DATASET}/parquet/${STEM}_${SEASON}.parquet"
PY_PARQUET="${PY_OUT}/${DATASET}/parquet/${STEM}_${SEASON}.parquet"
for p in "${R_PARQUET}" "${PY_PARQUET}"; do
  [ -f "$p" ] || { echo "::error ::expected artifact missing: $p" >&2; exit 1; }
done

echo "=== compare ==="
# --json, then parse. The CLI exits 1 for "findings were reported" AND python
# exits 1 for "the tool blew up", so the exit code alone cannot tell a real
# divergence from a crash. An earlier version of this script (WNBA sibling)
# announced "R and Python disagree" for a ModuleNotFoundError -- reporting a
# broken tool as a data defect, which is the exact confusion this harness
# exists to avoid.
FINDINGS="${WORK}/findings.json"
# JOIN_KEYS is an intentional multi-word list, so it must stay unquoted here.
# shellcheck disable=SC2086
( cd "${REPO_DIR}/python" && uv run python -m tools.validation.cli compare \
    --dataset "nba_${DATASET}" --domain nba \
    --r-parquet "${R_PARQUET}" --py-parquet "${PY_PARQUET}" \
    --join-keys ${JOIN_KEYS} --json ) > "${FINDINGS}" 2> "${WORK}/compare.err"
rc=$?

if ! python -c "import json,sys; json.load(open(sys.argv[1]))" "${FINDINGS}" 2>/dev/null; then
  echo "::error ::the parity CHECK ITSELF failed to run -- this is a tooling problem, not a data divergence"
  sed -n '1,25p' "${WORK}/compare.err" >&2
  exit 2
fi

if [ $rc -eq 0 ]; then
  echo "R and Python agree on ${DATASET} ${SEASON}."
  exit 0
fi

echo "::error ::R and Python disagree on ${DATASET} ${SEASON} -- neither side is automatically right; decide which pipeline is correct, then fix the other"
python - "${FINDINGS}" <<'PY'
import json, sys
for f in json.load(open(sys.argv[1])):
    print(f"  {f['severity'].upper():5} {f['message']}")
PY
exit 1
