"""Python producer for the ESPN NBA release datasets.

Parity port of ``hoopR-nba-data/R/espn_nba_*_creation.R``. Reshapes the sibling
``hoopR-nba-raw`` per-game JSON into season-level parquet/csv + manifest and
publishes to the ``espn_nba_*`` release tags. R is retained
as the byte-parity oracle.
"""

__all__ = ["config", "ingest", "io", "build", "publish", "reshapers"]
