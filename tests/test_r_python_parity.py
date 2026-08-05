"""R <-> Python stage parity.

Standing policy (2026-08-03): this repo carries BOTH pipelines. Python is
primary and gets the work; the R chain is maintained as the methodological /
language equivalent; **both move together when either changes.**

The two sides decompose differently on purpose — R is dataset-per-file
(``R/espn_nba_NN_<key>_creation.R``), Python is a build package with datasets
as ``config.REGISTRY`` rows. The numbered shims in ``python/`` bridge that:
``python/espn_nba_NN_<key>_creation.py`` carries the SAME number as its R twin,
so the stage sequence is comparable by eye and by test.

**Neither side is authoritative.** A failure here means the pipelines disagree
about what they produce; a human decides which is right. The messages are
written to support that decision, not to pre-empt it.

Scope: this is the contract-level guard (which datasets, which numbers). It
does NOT prove the two produce the same values — that is the output-parity
harness, a separate and heavier phase.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
CONFIG = REPO / "python" / "nba_data_build" / "config.py"

_R_STAGE = re.compile(r"^(?:espn_)?nba_(?P<num>\d{2})_(?P<key>.+)_creation$")
_PY_STAGE = re.compile(r"^espn_nba_(?P<num>\d{2})_(?P<key>.+)_creation$")

# Datasets Python declares that no NUMBERED R stage owns. Each entry must say
# WHY, so that a genuinely new divergence fails instead of blending in.
# Removing a dataset from this map is how a parity gap gets closed.
KNOWN_UNPAIRED: dict[str, str] = {
    "schedules": "R emits it inside espn_nba_01_pbp_creation.R (bundled with pbp), not as its own numbered stage.",
    "shots": "R emits it inside espn_nba_01_pbp_creation.R (bundled with pbp), not as its own numbered stage.",
    "player_core": "OPEN PARITY GAP — no R file references player_core at all, yet Python produces it.",
}


def _registry_keys() -> list[str]:
    tree = ast.parse(CONFIG.read_text(encoding="utf-8"))
    for node in tree.body:
        tgs = [node.target] if isinstance(node, ast.AnnAssign) else getattr(node, "targets", [])
        if any(isinstance(t, ast.Name) and t.id == "REGISTRY" for t in tgs):
            return [ast.literal_eval(k) for k in node.value.keys]
    raise AssertionError(f"no REGISTRY assignment found in {CONFIG}")


def _r_stages() -> dict[str, str]:
    """key -> NN, from the R filenames."""
    out = {}
    for p in sorted((REPO / "R").glob("*.R")):
        m = _R_STAGE.match(p.stem)
        if m:
            out[m.group("key")] = m.group("num")
    return out


def _py_stages() -> dict[str, str]:
    """key -> NN, from the numbered python shims."""
    out = {}
    for p in sorted((REPO / "python").glob("*.py")):
        m = _PY_STAGE.match(p.stem)
        if m:
            out[m.group("key")] = m.group("num")
    return out


def test_parsers_find_something():
    """Guard the guard — a regex that matched nothing would pass everything below."""
    assert _registry_keys(), "registry parsed empty"
    assert _r_stages(), "no numbered R stages found"
    assert _py_stages(), "no numbered python shims found"


def test_every_r_stage_has_a_python_shim():
    r, py = _r_stages(), _py_stages()
    missing = sorted(set(r) - set(py))
    assert not missing, (
        f"R stages with no numbered python shim: {missing}\n"
        "Every R stage needs its Python twin — that is the point of the numbering."
    )


def test_stage_numbers_agree():
    """The number must mean the same dataset in both languages."""
    r, py = _r_stages(), _py_stages()
    clashes = [(k, r[k], py[k]) for k in sorted(set(r) & set(py)) if r[k] != py[k]]
    assert not clashes, (
        "Same dataset, different stage number:\n"
        + "\n".join(f"  {k}: R={rn} python={pn}" for k, rn, pn in clashes)
        + "\nRenumbering one side alone breaks the comparison the numbers exist for."
    )


def test_every_registry_dataset_has_a_shim():
    """A dataset the package can build but no shim exposes is invisible in the
    directory listing, which defeats 'the listing IS the pipeline'."""
    missing = sorted(set(_registry_keys()) - set(_py_stages()))
    assert not missing, f"REGISTRY datasets with no numbered shim: {missing}"


def test_no_shim_without_a_registry_entry():
    """The inverse: a shim for a dataset the package cannot build would fail only
    when someone ran it."""
    extra = sorted(set(_py_stages()) - set(_registry_keys()))
    assert not extra, f"numbered shims with no REGISTRY entry: {extra}"


def test_unpaired_datasets_are_declared():
    """Python-only datasets must be listed in KNOWN_UNPAIRED with a reason.

    This is the test that turns 'we know about these three' into something
    enforceable: a NEW python-only dataset fails until someone writes down
    whether R bundles it elsewhere or it is a real gap.
    """
    unpaired = sorted(set(_registry_keys()) - set(_r_stages()))
    undeclared = [k for k in unpaired if k not in KNOWN_UNPAIRED]
    assert not undeclared, (
        f"Python produces {undeclared} with no numbered R stage and no entry in "
        "KNOWN_UNPAIRED.\nEither add the R stage, or declare why it is unpaired "
        "(bundled elsewhere in R? genuine gap?) — do not leave it ambiguous."
    )
    stale = [k for k in KNOWN_UNPAIRED if k not in unpaired]
    assert not stale, (
        f"KNOWN_UNPAIRED still lists {stale}, but R now pairs them. "
        "Remove the entry — a closed gap should not keep its excuse."
    )
