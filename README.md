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