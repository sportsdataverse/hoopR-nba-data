"""Offline parity: the R port ``split_made_attempted()`` vs sdv-py ``_split_made_attempted``.

The season parity test only reaches R's output when the committed oracle was written by the
R chain, so this runs the R function itself on a committed fixture
(``tests/fixtures/split_made_attempted_rows.json``: decimals, 0-0, null display_value, empty
and null labels and display names, a two-hyphen name, a split name its category already has,
descriptions with zero, one and two ``.-``, and values that do not split) and compares it
row-for-row with the installed sdv-py implementation. Byte-identical across the hoopR
``-data`` repos that carry the R chain.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest
from sportsdataverse.wbb.wbb_season_producers import _split_made_attempted

REPO = Path(__file__).resolve().parents[1]
FIXTURE = REPO / "tests" / "fixtures" / "split_made_attempted_rows.json"

# Evaluate only the split_made_attempted assignment: sourcing the whole stage script runs it.
_R = r"""
args <- commandArgs(trailingOnly = TRUE)
for (e in parse(args[[1]])) {
  if (is.call(e) && identical(e[[1]], as.name("<-")) &&
      identical(e[[2]], as.name("split_made_attempted"))) eval(e)
}
stopifnot(exists("split_made_attempted"))
`%>%` <- dplyr::`%>%`
df <- tibble::as_tibble(jsonlite::fromJSON(args[[2]]))
out <- split_made_attempted(df)
writeLines(jsonlite::toJSON(out, dataframe = "rows", na = "null", digits = NA), args[[3]])
"""


def test_r_split_made_attempted_matches_sdv_py(tmp_path):
    rscript = shutil.which("Rscript")
    if rscript is None:
        pytest.skip("Rscript is not on PATH; the R port of split_made_attempted() is unchecked")
    (r_file,) = sorted((REPO / "R").glob("espn_*_05_player_season_stats_creation.R"))
    script, out = tmp_path / "split.R", tmp_path / "out.json"
    script.write_text(_R, encoding="utf-8")
    subprocess.run(
        [rscript, "--vanilla", str(script), str(r_file), str(FIXTURE), str(out)],
        check=True,
        timeout=300,
    )

    rows = json.loads(FIXTURE.read_text(encoding="utf-8"))
    expected = _split_made_attempted(rows)
    got = json.loads(out.read_text(encoding="utf-8"))
    assert len(expected) > len(rows), "the fixture must exercise the split"
    assert len(got) == len(expected), f"row count: R={len(got)} py={len(expected)}"
    for i, (g, e) in enumerate(zip(got, expected, strict=True)):
        assert g == e, f"row {i}: R={g} py={e}"
