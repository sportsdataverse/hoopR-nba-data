#!/bin/bash
while getopts s:e:r: flag
do
    case "${flag}" in
        s) START_YEAR=${OPTARG};;
        e) END_YEAR=${OPTARG};;
        r) RESCRAPE=${OPTARG};;
    esac
done
for i in $(seq "${START_YEAR}" "${END_YEAR}")
do
    echo "$i"
    git pull  >> /dev/null
    git config --local user.email "action@github.com"
    git config --local user.name "Github Action"
    Rscript R/espn_nba_01_pbp_creation.R -s $i -e $i
    Rscript R/espn_nba_02_team_box_creation.R -s $i -e $i
    Rscript R/espn_nba_03_player_box_creation.R -s $i -e $i
    git pull  >> /dev/null
    git add nba/* >> /dev/null
    git pull >> /dev/null
    git add . >> /dev/null
    git commit -m "NBA Data Updated (Start: $i End: $i)" || echo "No changes to commit"
    git pull >> /dev/null
    git push >> /dev/null
done