"""Offline smoke test for the D40/D43 docs generator (nba_data_build.docs).

Runs against the real repo tree (docs.REPO_ROOT resolves to the actual
checkout; ported from hoopR-mbb-data's equivalent test) rather than a synthetic
fixture -- the generator's whole job is to read real committed files
(README.md/CLAUDE.md, the committed nba/ parquet tree), so a fixture would
just test the fixture. --no-live everywhere: no `gh` calls in CI.
"""

from __future__ import annotations

from nba_data_build import docs


def test_builder_covers_exactly_the_registry():
    assert set(docs.BUILDER) == set(docs.REGISTRY)
    assert set(docs.PAGES) == set(docs.REGISTRY)


def test_column_table_renders_without_crashing_for_every_dataset():
    for dataset in docs.PAGES:
        table = docs.column_table(dataset)
        assert table  # either a real schema table or the honest placeholder


def test_coverage_seasons_are_seasons_not_prefix_collisions():
    """``nba_schedule`` (schedules) is a PREFIX of ``nba_schedule_crosswalk``.
    Globbing ``{stem}_*`` therefore let the committed
    ``nba_schedule_crosswalk_2026.parquet`` render as a ``crosswalk_2026``
    season of the schedules dataset. Every season cell must be a bare year."""
    for dataset in docs.PAGES:
        for line in docs.coverage_table(dataset).splitlines():
            if line.startswith("| ") and "---" not in line and "season" not in line:
                season = line.split("|")[1].strip()
                assert season.isdigit(), f"{dataset}: non-season coverage row {season!r}"


def test_dataset_page_is_well_formed():
    page = docs.dataset_page("pbp", live=False)
    assert page.startswith("# `pbp`")
    assert "## Columns" in page
    assert "## Coverage" in page


def test_generate_check_is_idempotent(tmp_path, monkeypatch):
    """generate() then generate(check=True) must agree -- no ratchet, no drift."""
    monkeypatch.setattr(docs, "DOCS_DIR", tmp_path / "datasets")
    monkeypatch.setattr(docs, "REPO_ROOT", tmp_path.parent)  # no README/CLAUDE.md here
    assert docs.generate(check=False, live=False) == 0
    assert docs.generate(check=True, live=False) == 0


def test_replace_block_is_idempotent():
    text = "# Title\n\nsome prose\n"
    once = docs._replace_block(text, "row1")
    twice = docs._replace_block(once, "row1")
    assert once == twice
    assert docs.BEGIN in once and docs.END in once


def test_every_page_names_both_loaders():
    assert set(docs.LOADERS) == set(docs.REGISTRY)
    for dataset in docs.PAGES:
        page = docs.dataset_page(dataset, live=False)
        r_loader, py_loader = docs.LOADERS[dataset]
        assert f"`{r_loader}`" in page and f"`{py_loader}`" in page
        assert "## Join keys" in page


def test_join_keys_follow_the_schema(monkeypatch):
    """Schema-driven: only mapped id columns render. PR CI has no nba/ tree,
    so the schema is stubbed here."""
    import polars as pl

    schema = pl.Schema(
        {
            "season": pl.Int32,
            "espn_athlete_id": pl.Int32,
            "nba_player_id": pl.Int32,
            "x_id": pl.Int32,
        }
    )
    monkeypatch.setattr(docs, "_schema", lambda dataset: schema)
    table = docs.join_table("player_crosswalk")
    assert "| `espn_athlete_id` |" in table and "| `nba_player_id` | NBA Stats" in table
    assert "`season`" not in table and "`x_id`" not in table
