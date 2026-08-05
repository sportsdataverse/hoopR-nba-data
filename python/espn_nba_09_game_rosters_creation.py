"""Stage 09 -- game_rosters.

Mirrors ``R/espn_nba_09_game_rosters_creation.R`` -- same stage number, same dataset.

Thin shim over the tested build package: the pipeline logic lives in
``nba_data_build``; this file exists so the stage sequence is readable from a
directory listing. It lines up with ``R/espn_nba_09_game_rosters_creation.R``.

Equivalent to::

    python -m nba_data_build --dataset game_rosters -s <start> -e <end>
"""

from __future__ import annotations

import sys

from nba_data_build.cli import main

DATASET = "game_rosters"

if __name__ == "__main__":
    # DATASET is appended, not prepended: argparse takes the last value for a
    # single-value option, so a stray --dataset on the command line cannot make
    # stage 09 build something other than game_rosters.
    sys.exit(main([*sys.argv[1:], "--dataset", DATASET]))
