import pandas as pd
from sqlalchemy import create_engine, text
import math
import os

# DB credentials
DB_USER = "de13_sadh"
DB_PASS = "CpKSyW7F"
DB_HOST = "data-sandbox.c1tykfvfhpit.eu-west-2.rds.amazonaws.com"
DB_PORT = "5432"
DB_NAME = "pagila"
DB_SCHEMA = "de_2506_a"


engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")


team_details = pd.read_csv("csv_extracted/team_details.csv")


team_details = team_details.rename(columns={
    "year_founded": "yearfounded",
    "arena_capacity": "arenacapacity",
    "arena_name": "arena"   
})


db_columns = ["team_id", "owner", "generalmanager", "headcoach", 
              "arenacapacity", "yearfounded", "arena"]
team_details = team_details[[col for col in team_details.columns if col in db_columns]]


with engine.begin() as conn:  
    for _, row in team_details.iterrows():
        row_dict = row.to_dict()

        
        if isinstance(row_dict.get("yearfounded"), float):
            row_dict["yearfounded"] = int(row_dict["yearfounded"]) if not math.isnan(row_dict["yearfounded"]) else None
        if isinstance(row_dict.get("arenacapacity"), float):
            row_dict["arenacapacity"] = int(row_dict["arenacapacity"]) if not math.isnan(row_dict["arenacapacity"]) else None

        update_stmt = text("""
            UPDATE de_2506_a.sd_dim_teams
            SET owner = :owner,
                generalmanager = :generalmanager,
                headcoach = :headcoach,
                arenacapacity = :arenacapacity,
                yearfounded = :yearfounded,
                arena = :arena
            WHERE team_id = :team_id
        """)

        conn.execute(update_stmt, row_dict)


df = pd.read_csv("csv_extracted/common_player_info.csv")

df = df.rename(columns={
    "person_id": "player_id",
    "rosterstatus": "roster_status"
})

db_columns = [
    "player_id", "first_name", "last_name", "display_fi_last", "birthdate",
    "school", "country", "last_affiliation", "height", "weight", "season_exp",
    "position", "roster_status", "team_id", "team_name", "team_abbreviation",
    "from_year", "to_year", "dleague_flag", "nba_flag", "games_played_flag",
    "draft_year", "draft_round", "draft_number"
]
df = df[[col for col in df.columns if col in db_columns]]


for col in ["height", "weight", "season_exp", "from_year", "to_year", "draft_year", "draft_round", "draft_number"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

with engine.begin() as conn:
    for _, row in df.iterrows():
        row_dict = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}

# was getting some errors with mapping this into tables so i had chatgpt help me with some errors and whether i should use upsert statements and to use on conflict.
        upsert_stmt = text("""
            INSERT INTO de_2506_a.sd_dim_players (
                player_id, first_name, last_name, display_fi_last, birthdate,
                school, country, last_affiliation, height, weight, season_exp,
                position, roster_status, team_id, team_name, team_abbreviation,
                from_year, to_year, dleague_flag, nba_flag, games_played_flag,
                draft_year, draft_round, draft_number
            )
            VALUES (
                :player_id, :first_name, :last_name, :display_fi_last, :birthdate,
                :school, :country, :last_affiliation, :height, :weight, :season_exp,
                :position, :roster_status, :team_id, :team_name, :team_abbreviation,
                :from_year, :to_year, :dleague_flag, :nba_flag, :games_played_flag,
                :draft_year, :draft_round, :draft_number
            )
            ON CONFLICT (player_id) DO UPDATE
            SET first_name = EXCLUDED.first_name,                                               
                last_name = EXCLUDED.last_name,
                display_fi_last = EXCLUDED.display_fi_last,
                birthdate = EXCLUDED.birthdate,
                school = EXCLUDED.school,
                country = EXCLUDED.country,
                last_affiliation = EXCLUDED.last_affiliation,
                height = EXCLUDED.height,
                weight = EXCLUDED.weight,
                season_exp = EXCLUDED.season_exp,
                position = EXCLUDED.position,
                roster_status = EXCLUDED.roster_status,
                team_id = EXCLUDED.team_id,
                team_name = EXCLUDED.team_name,
                team_abbreviation = EXCLUDED.team_abbreviation,
                from_year = EXCLUDED.from_year,
                to_year = EXCLUDED.to_year,
                dleague_flag = EXCLUDED.dleague_flag,
                nba_flag = EXCLUDED.nba_flag,
                games_played_flag = EXCLUDED.games_played_flag,
                draft_year = EXCLUDED.draft_year,
                draft_round = EXCLUDED.draft_round,
                draft_number = EXCLUDED.draft_number
        """)

        conn.execute(upsert_stmt, row_dict)
        


scoring = pd.read_csv("C:/Users/hanna/etl-capstone/csv_extracted/scoring.csv")
assists_turnovers = pd.read_csv("C:/Users/hanna/etl-capstone/csv_extracted/assists-turnovers.csv")


engine = create_engine("postgresql+psycopg2://de13_sadh:CpKSyW7F@data-sandbox.c1tykfvfhpit.eu-west-2.rds.amazonaws.com:5432/pagila")
teams_dim = pd.read_sql(
    "SELECT team_id, abbreviation, nickname FROM de_2506_a.sd_dim_teams",
    engine
)

scoring = pd.merge(
    scoring,
    teams_dim,
    left_on="Team",
    right_on="abbreviation",
    how="left"
)

assists_turnovers = pd.merge(
    assists_turnovers,
    teams_dim,
    left_on="Team",
    right_on="abbreviation",
    how="left"
)

player_stats = pd.merge(
    scoring,
    assists_turnovers,
    on=['Player', 'Team', 'team_id', 'Position'],
    how='outer'
)


player_stats['player_id'] = range(1, len(player_stats) + 1)


player_stats = player_stats.rename(columns={
    "GP  Games played_x": "games_played",
    "GS  Games started_x": "games_started",
    "MPG  Minutes Per Game_x": "minutes_per_game",
    "PPG  Points Per Game": "points_per_game",
    "FGM  Field Goals Made": "fgm",
    "FGA  Field Goals Attempted": "fga",
    "FG%  Field Goal Percentage": "fg_pct",
    "3FGM  Three-Point Field Goals Made": "fg3m",
    "3FGA  Three-Point Field Goals Attempted": "fg3a",
    "3FG%  Three-Point Field Goal Percentage": "fg3_pct",
    "FTM  Free Throws Made": "ftm",
    "FTA  Free Throws Attempted": "fta",
    "FT%  Free Throw Percentage": "ft_pct",
    "AST  Total Assists": "ast",
    "TO  Turnovers": "turnovers",
    "player_name": "player_name",
    "position": "position",
    "team_id": "team_id"
})



columns_to_keep = [
    'player_id', 'team_id', 'games_played', 'games_started', 
    'minutes_per_game', 'points_per_game', 'fgm', 'fga', 'fg_pct',
    'fg3m', 'fg3a', 'fg3_pct', 'ftm', 'fta', 'ft_pct',
    'ast', 'turnovers'
]

player_stats = player_stats[columns_to_keep]



player_stats.to_sql(
    'sd_facts_player_season_stats',
    engine,
    schema='de_2506_a',
    if_exists='append',
    index=False
)



df = pd.read_csv("csv_extracted/team_summaries.csv")


df.columns = df.columns.str.strip().str.lower()


df = df.rename(columns={
    "team": "team_name",
    "w": "wins",
    "l": "losses",
    "age": "avg_age",
    "playoffs": "playoffs",
    "season": "season"
})


df["wins"] = pd.to_numeric(df["wins"], errors="coerce").astype("Int64")
df["losses"] = pd.to_numeric(df["losses"], errors="coerce").astype("Int64")
df["avg_age"] = pd.to_numeric(df["avg_age"], errors="coerce")
df["season"] = pd.to_numeric(df["season"], errors="coerce").astype("Int64")  
df["playoffs"] = df["playoffs"].astype(str).str.lower().map({
    "1": True, "true": True, "yes": True,
    "0": False, "false": False, "no": False
})

# had gpt help me figure out why i was getting null values and errors when loading this data into the table. 
with engine.begin() as conn:
    for _, row in df.iterrows():
        row_dict = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
        insert_stmt = text("""
            INSERT INTO de_2506_a.sd_fact_team_summary (team_name, season, wins, losses, avg_age, playoffs)
            VALUES (:team_name, :season, :wins, :losses, :avg_age, :playoffs)
            ON CONFLICT (team_name, season) DO UPDATE SET
                wins = EXCLUDED.wins,
                losses = EXCLUDED.losses,
                avg_age = EXCLUDED.avg_age,
                playoffs = EXCLUDED.playoffs
        """)
        conn.execute(insert_stmt, row_dict)


player_stats['high_usage'] = player_stats['minutes_per_game'] > 30



