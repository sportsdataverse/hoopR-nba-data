"""Stage 13 -- player_crosswalk.

Mirrors ``R/nba_13_player_crosswalk_creation.R`` -- same stage number, same dataset.

Thin shim over the tested build package: the pipeline logic lives in
``nba_data_build``; this file exists so the stage sequence is readable from a
directory listing. It lines up with ``R/nba_13_player_crosswalk_creation.R``.

Equivalent to::

    python -m nba_data_build --dataset player_crosswalk -s <start> -e <end>
"""

from __future__ import annotations

import sys

from nba_data_build.cli import main

DATASET = "player_crosswalk"

if __name__ == "__main__":
    # DATASET is appended, not prepended: argparse takes the last value for a
    # single-value option, so a stray --dataset on the command line cannot make
    # stage 13 build something other than player_crosswalk.
    sys.exit(main([*sys.argv[1:], "--dataset", DATASET]))
