# nba-data-etl-pipeline

An end-to-end ETL (Extract, Transform, Load) pipeline for collecting, processing, and analyzing NBA data. The goal of this is to be able to have a place where any and all fans can look up a player, past or present and be able to gather all vital information about their career (points per game, assists per game, rebounds per game, efficiency, field goal percentage, etc). The way this is done is by using a dataset downloaded from Kaggle and transform it into usable data and upload onto an app in Streamlit. 

## Features
- Extracts NBA game and player data from APIs or CSV sources
- Cleans and transforms data for analysis
- Loads data into a database or analytics-ready format
- Generates dashboards and visualizations for insights

## Tech Stack
- Python (pandas, requests, sqlalchemy)
- PostgreSQL / SQLite
- Streamlit for visualisations


# Epics

## Epic 1:
> I want to gather NBA datasets from Kaggle so that I can store them in a raw data folder,
> then extract them to use to track past and present NBA player's stats.

## Epic 2
> I want to clean and standardise my data so that it is accurate, consistent as has as few null values/errors and is ready for analysis.

## Epic 3 
>I want to load my data into a single SQL table so that I can quickly generate insights on any NBA players of choice.

## Epic 4
>I want to calculate advanced statistics and analytics so I can evaluate players and teams in more depth.

## Epic 5 
>I want to visualize my NBA data in charts and dashboards so insights are easier to share.


# Epic breakdowns


## Epic 1 Story 1
>I  want to download the NBA datasets (games, players, stats) from Kaggle and save them locally so I can extract data from them.

## Epic 1 Story 2 
>I'm going to store all raw files in the appropriate directory so they’re easy to locate.

## Epic Story 3
> I am going to document my data sources, file formats, and schema so I know exactly what I’m working with.


## Epic 2 Story 1 
> I am going to remove duplicate rows and handle missing values in my datasets in order to clean up the data and increase efficiency in insight generation

## Epic 2 Story 2 
>I am going to create calculated columns like Player Efficiency Rating (PER) or win percentages so that users can have advanced metrics to create judgements about players

## Epic 2 Story 3 
>I save my cleaned data in the transform file for the next step in the pipeline.


## Epic 3 Story 1
>I design a database schema for storing players, games, and statistics.

## Epic 3 Story 2 
>I create tables in my database that match the cleaned data structure sothat SQL query is as quick as possible

## Epic 3 Story 3 
>I write a script to load processed CSV files into the database automatically.

## Epic 3 Story 4
>I verify that the data loads without errors and matches the original source.

## Epic 4 Story 1
>I compute advanced player metrics like True Shooting %, Usage Rate, and Win Shares.

## Epic 4 Story 2
>I compare team performance across multiple seasons using custom metrics.

## Epic 5 Story 1
>I build interactive dashboards using Streamlit or Plotly Dash.

## Epic 5 Story 2
>I add filters for selecting specific teams, players, or seasons.

## Epic 5 Story 3
>I deploy my dashboard so others can view it without installing anything.
