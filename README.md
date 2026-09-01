# hoopR-nba-data
hoopR NBA Data 2002 - Present

```mermaid
  graph LR;
    A[hoopR-nba-raw]-->B[hoopR-nba-data];
    B[hoopR-nba-data]-->C1[espn_nba_pbp];
    B[hoopR-nba-data]-->C2[espn_nba_team_boxscores];
    B[hoopR-nba-data]-->C3[espn_nba_player_boxscores];

```

## hoopR ESPN NBA workflow diagram

```mermaid
flowchart TB;
    subgraph A[hoopR-nba-raw];
        direction TB;
        A1[python/scrape_nba_schedules.py]-->A2[python/scrape_nba_json.py];
    end;

    subgraph B[hoopR-nba-data];
        direction TB;
        B1[R/espn_nba_01_pbp_creation.R]-->B2[R/espn_nba_02_team_box_creation.R];
        B2[R/espn_nba_02_team_box_creation.R]-->B3[R/espn_nba_03_player_box_creation.R];
    end;

    subgraph C[sportsdataverse Releases];
        direction TB;
        C1[espn_nba_pbp];
        C2[espn_nba_team_boxscores];
        C3[espn_nba_player_boxscores];
    end;

    A-->B;
    B-->C1;
    B-->C2;
    B-->C3;

```

[hoopR-nba-raw data repository (source: ESPN)](https://github.com/sportsdataverse/hoopR-nba-raw)

[hoopR-nba-data repository (source: ESPN)](https://github.com/sportsdataverse/hoopR-nba-data)

[hoopR-nba-stats-data Repo (source: NBA Stats)](https://github.com/sportsdataverse/hoopR-nba-stats-data)

[hoopR-mbb-raw data repository (source: ESPN)](https://github.com/sportsdataverse/hoopR-mbb-raw)

[hoopR-mbb-data repository (source: ESPN)](https://github.com/sportsdataverse/hoopR-mbb-data)

[hoopR-kp-data Repo (source: KenPom)](https://github.com/sportsdataverse/hoopR-kp-data)

## Automation & status

<!-- BEGIN GENERATED: status -->

| workflow | schedule | last run |
|---|---|---|
| [![daily_nba.yml](https://github.com/sportsdataverse/hoopR-nba-data/actions/workflows/daily_nba.yml/badge.svg)](https://github.com/sportsdataverse/hoopR-nba-data/actions/workflows/daily_nba.yml) | days 18-31 07:00 UTC in Oct; daily 07:00 UTC in Nov-Dec; daily 07:00 UTC in Jan-Jun; days 1-12 07:00 UTC in Jul | 2026-08-19 |
| [![orphan_scripts.yml](https://github.com/sportsdataverse/hoopR-nba-data/actions/workflows/orphan_scripts.yml/badge.svg)](https://github.com/sportsdataverse/hoopR-nba-data/actions/workflows/orphan_scripts.yml) | on push / PR / dispatch | 2026-08-26 |
| [![tests.yml](https://github.com/sportsdataverse/hoopR-nba-data/actions/workflows/tests.yml/badge.svg)](https://github.com/sportsdataverse/hoopR-nba-data/actions/workflows/tests.yml) | on push / PR / dispatch | 2026-08-27 |

| release tag | assets | size | last publish |
|---|---:|---:|---|
| [`espn_nba_schedules`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_schedules) | 88 | 122.7 MB | 2026-08-26 |
| [`espn_nba_pbp`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_pbp) | 79 | 6,081.8 MB | 2026-08-26 |
| [`espn_nba_team_boxscores`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_team_boxscores) | 79 | 33.5 MB | 2026-08-26 |
| [`espn_nba_player_boxscores`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_player_boxscores) | 79 | 420.7 MB | 2026-08-26 |
| [`espn_nba_rosters`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_rosters) | 14 | 1.0 MB | 2026-08-19 |
| [`espn_nba_game_rosters`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_game_rosters) | 80 | 204.9 MB | 2026-08-12 |
| [`espn_nba_player_core`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_player_core) | 75 | 6.6 MB | 2026-08-12 |
| [`espn_nba_player_season_stats`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_player_season_stats) | 80 | 106.6 MB | 2026-08-12 |
| [`espn_nba_team_season_stats`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_team_season_stats) | 80 | 8.1 MB | 2026-08-12 |
| [`espn_nba_standings`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_standings) | 80 | 4.3 MB | 2026-08-12 |
| [`espn_nba_officials`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_officials) | 80 | 5.9 MB | 2026-08-12 |
| [`espn_nba_shots`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_shots) | 80 | 896.7 MB | 2026-08-26 |
| [`espn_nba_draft`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_nba_draft) | 80 | 0.9 MB | 2026-08-19 |
| [`nba_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_crosswalk) | 25 | 0.7 MB | 2026-08-19 |

<!-- END GENERATED: status -->
