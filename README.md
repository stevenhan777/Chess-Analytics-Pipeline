# Chess Analytics Pipeline

### An end-to-end data pipeline that ingests online chess game data from the Lichess API, stores it in BigQuery, transforms it using dbt, and orchestrates daily refreshes with Apache Airflow:

* Defining table schemas and fetching raw game data with computer analysis from the Lichess API
* Ingesting data into BigQuery across 3 tables: `games`, `moves`, and `move_evals`
* Saving CSV files locally before loading to BigQuery
* Cleaning and casting raw data in the dbt staging layer
* Engineering move-level features in the intermediate layer such as piece type, boolean move flags, eval delta, and rolling eval volatility
* Reconstructing opening move sequences using Lichess's own classification
* Computing rating trajectories using `LAG`/`LEAD` window functions across a player's game history
* Building mart tables: `fct_matches`, `dim_players`, and `dim_openings`
* Orchestrating the full pipeline with Apache Airflow running locally via Docker on a daily schedule

#### 1) Motivation

I have a strong interest in chess and wanted to go beyond what Lichess, a popular online chess platform, offers. Lichess offers player analytics such as win rate by opening, rating gain if castled, etc. I wanted to extract data from the Lichess API, starting first with my own games, and then the games of other players. This project builds the data foundation to answer questions like:
- When and why do I blunder?
- Which openings do I blunder the most in?
- How has my rating changed over time, and what drives those changes?

---

## Data Ingestion

The ingestion script fetches games from the Lichess API and loads them into BigQuery as 3 tables. All transformation logic is done in dbt.

Key behaviour:
- Only fetches games that have computer analysis ran on Lichess
- Supports any Lichess username
- Supports `blitz` or slow games (`rapid` + `classical`)
- Accepts a `max_games` limit
- Saves CSVs locally before loading to BigQuery

Default configuration:
I use my lichess username: `stevenhan`, game type: `rapid+classical` and 5000 `max_games` limit.

Raw tables created: 

`games`: One row per game: metadata, result, opening, time control
`moves`: One row per ply (move): move SAN, move number, color, username
`move_evals`: One row per ply (move): engine eval, judgment, clock data

`moves` and `move_evals` share the composite key `(game_id, move_offset)` and are joined in dbt staging.

---

## dbt Transformations

### Staging Layer: `stg_`
Materialized as views. Cleans and casts raw data, no business logic.

- `stg_lichess__games`: casts timestamps, derives time control label, filters untimed games
- `stg_lichess__moves`: joins `moves` and `move_evals` on `(game_id, move_offset)`

### Intermediate Layer: `int_`
Materialized as tables. All calculations before the marts.

- `int_move_sequences`: classifies piece type, adds boolean move flags (capture, check, checkmate, promotion, castling), computes eval delta and 3-move rolling eval volatility
- `int_opening_patterns`: reconstructs opening move sequence per game using Lichess's `opening_ply` as the cutoff
- `int_rating_trajectories`: pivots white/black into one row per player per game, applies `LAG`/`LEAD` for rating history, rolling 10-game win rate and accuracy

### Marts Layer: `dim_` / `fct_`
Materialized as tables. Final output for analytics.

- `fct_matches`: one row per game, all metrics pre-aggregated (blunders, mistakes, captures, eval swings, time pressure)
- `dim_players`: lifetime stats per player including win rate by color, peak rating, avg accuracy
- `dim_openings`: one row per ECO code with win rates, avg game length, and move sequence

---

## Airflow Orchestration

UI at `http://localhost:8080`, login: `airflow` / `airflow`

DAG: `chess_pipeline`: Configured to run daily at 6am

DAG Flow:
ingest_lichess_games -> dbt_staging -> dbt_intermediate -> dbt_marts -> dbt_test

---

### Initial Setup

1. Create a GCP service account key and save to `~/.dbt/chess-pipeline-key.json`
2. Configure `~/.dbt/profiles.yml`
3. Create `airflow/.env` with AIRFLOW_UID and FERNET_KEY

---

### Conclusion

This project started out of a personal interest and curiosity. I wanted to understand what factors went into my chess mistakes beyond what Lichess offered. Building this pipeline gave me a solid foundation to do exactly that.

The pipeline ingests raw game data from the Lichess API, loads it into three BigQuery tables, and transforms it through a structured dbt layer into analytics-ready models covering move sequences, opening patterns, rating trajectories, and game-level metrics. Airflow ties it together with daily automated refreshes, so the data updates as the user plays more games.

A few things I learned along the way:

- Keeping Python ingestion to the minimum and pushing all transformation logic into dbt made the pipeline easier to maintain and debug
- Modeling the data at the move level (instead of game level) allowed for much richer analysis
- Running dbt inside Docker introduced some real-world friction around PATH, Python environments, and volume mounts and working through those made the setup more robust

The natural next step is to use the mart tables produced here as the data for a machine learning model that predicts blunder probability on any given move for any given player.