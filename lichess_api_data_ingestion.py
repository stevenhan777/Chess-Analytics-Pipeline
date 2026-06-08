"""
Fetches Lichess games, writes 3 CSVs, and loads each to BigQuery.
No transformation logic; arrays are expanded into rows here so dbt can
work with plain scalar columns and simple JOINs.
"""

import json
import logging
import os
from datetime import datetime, timezone

import pandas as pd
import requests
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataIngestion:

    # ── BigQuery schemas ────────────────────────────────────────────────────

    GAMES_SCHEMA = [
        bigquery.SchemaField("game_id", "STRING"),
        bigquery.SchemaField("white_username", "STRING"),
        bigquery.SchemaField("black_username", "STRING"),
        bigquery.SchemaField("white_rating", "INTEGER"),
        bigquery.SchemaField("black_rating", "INTEGER"),
        bigquery.SchemaField("white_rating_diff", "INTEGER"),
        bigquery.SchemaField("black_rating_diff", "INTEGER"),
        bigquery.SchemaField("white_accuracy", "FLOAT"),
        bigquery.SchemaField("black_accuracy", "FLOAT"),
        bigquery.SchemaField("winner", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("perf_type", "STRING"),
        bigquery.SchemaField("variant", "STRING"),
        bigquery.SchemaField("opening_eco", "STRING"),
        bigquery.SchemaField("opening_name", "STRING"),
        bigquery.SchemaField("opening_ply", "INTEGER"),
        bigquery.SchemaField("time_control_initial", "INTEGER"),  # seconds
        bigquery.SchemaField("time_control_increment", "INTEGER"),  # seconds
        bigquery.SchemaField("created_at", "INTEGER"),  # unix ms
        bigquery.SchemaField("last_move_at", "INTEGER"),  # unix ms
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]

    MOVES_SCHEMA = [
        bigquery.SchemaField("game_id", "STRING"),
        bigquery.SchemaField("move_offset", "INTEGER"),   # 0-indexed ply number
        bigquery.SchemaField("move_number", "INTEGER"),   # 1-indexed full-move number
        bigquery.SchemaField("color", "STRING"),    # 'white' | 'black'
        bigquery.SchemaField("username", "STRING"),
        bigquery.SchemaField("move_san", "STRING"),    # e.g. 'Nf3', 'O-O', 'exd5'
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]

    EVALS_SCHEMA = [
        bigquery.SchemaField("game_id", "STRING"),
        bigquery.SchemaField("move_offset", "INTEGER"),
        bigquery.SchemaField("eval_score", "FLOAT"),    # centipawns / 100; null if mate
        bigquery.SchemaField("mate_in", "INTEGER"),  # null if no forced mate
        bigquery.SchemaField("best_move", "STRING"),   # engine's top SAN choice
        bigquery.SchemaField("variation", "STRING"),   # engine PV line
        bigquery.SchemaField("judgment", "STRING"),   # Blunder/Mistake/Inaccuracy/null
        bigquery.SchemaField("time_left_cs", "INTEGER"),  # centiseconds remaining
        bigquery.SchemaField("time_left_ratio", "FLOAT"),    # fraction of initial time left (0–1)
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]

    def __init__(
        self,
        bq_project: str,
        bq_dataset: str = "lichess_raw",
        output_dir: str = "data",
    ):
        self.bq_project = bq_project
        self.bq_dataset = bq_dataset
        self.output_dir = output_dir
        self.bq_client = bigquery.Client(project=bq_project)
        os.makedirs(output_dir, exist_ok=True)

    # ── Public entry points ─────────────────────────────────────────────────

    def run_user_games(
        self,
        username: str,
        perf_type: str,
        max_games: int,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Fetch analyzed games for a user, write 3 CSVs, load to BigQuery.
        Returns (games_df, moves_df, evals_df).
        """
        perf_types = ["blitz"] if perf_type == "blitz" else ["rapid", "classical"]
        logger.info(f"Fetching up to {max_games} games for '{username}'…")

        raw = self._fetch_games(username, perf_types, max_games)
        with_analysis = [g for g in raw if g.get("analysis")]
        logger.info(f"Total: {len(raw)} | With analysis: {len(with_analysis)}")

        if not with_analysis:
            raise ValueError("No analyzed games found, run computer analysis on Lichess first.")

        games_df, moves_df, evals_df = self._parse(with_analysis)
        self._save_csvs(games_df, moves_df, evals_df, label=username)
        self._load_all(games_df, moves_df, evals_df)
        return games_df, moves_df, evals_df

    def run_single_game(self, game_id: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Fetch one game by ID, write CSVs, load to BigQuery."""
        resp = requests.get(
            f"https://lichess.org/game/export/{game_id}",
            headers={"Accept": "application/json"},
            params={"pgnInJson": "true", "clocks": "true", "evals": "true", "accuracy": "true"},
            timeout=30,
        )
        if resp.status_code == 404:
            raise ValueError(f"Game '{game_id}' not found.")
        resp.raise_for_status()
        game = resp.json()
        if not game.get("analysis"):
            raise ValueError("Game has no computer analysis on Lichess.")

        games_df, moves_df, evals_df = self._parse([game])
        self._save_csvs(games_df, moves_df, evals_df, label=game_id)
        self._load_all(games_df, moves_df, evals_df)
        return games_df, moves_df, evals_df

    # ── Core parsing ────────────────────────────────────────────────────────

    def _parse(
        self,
        games: list,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Expand a list of Lichess game dicts into 3 DataFrames.
        Arrays (moves / analysis / clocks) are split into individual rows.
        """
        game_rows: list[dict] = []
        move_rows: list[dict] = []
        eval_rows: list[dict] = []
        #ingested_at = datetime.now(timezone.utc).isoformat()
        ingested_at = datetime.now(timezone.utc)

        for g in games:
            white = g.get("players", {}).get("white", {})
            black = g.get("players", {}).get("black", {})
            clock = g.get("clock", {})
            opening = g.get("opening", {})
            game_id = g.get("id")
            initial = clock.get("initial", 0)   # seconds

            white_name = white.get("user", {}).get("name")
            black_name = black.get("user", {}).get("name")

            # ── games row (one per game, scalar fields only) ────────────────
            game_rows.append({
                "game_id": game_id,
                "white_username": white_name,
                "black_username": black_name,
                "white_rating": white.get("rating"),
                "black_rating": black.get("rating"),
                "white_rating_diff": white.get("ratingDiff"),
                "black_rating_diff": black.get("ratingDiff"),
                "white_accuracy": white.get("analysis", {}).get("accuracy"),
                "black_accuracy": black.get("analysis", {}).get("accuracy"),
                "winner": g.get("winner"),
                "status": g.get("status"),
                "perf_type": g.get("perf"),
                "variant": g.get("variant"),
                "opening_eco": opening.get("eco"),
                "opening_name": opening.get("name"),
                "opening_ply": opening.get("ply"),
                "time_control_initial": initial,
                "time_control_increment": clock.get("increment", 0),
                "created_at": g.get("createdAt"),
                "last_move_at": g.get("lastMoveAt"),
                "ingested_at": ingested_at,
            })

            # ── moves + evals rows (one per ply) ────────────────────────────
            moves_list = g.get("moves", "").split()
            analysis_list = g.get("analysis", [])
            clocks_list = g.get("clocks",   [])

            for i, move_san in enumerate(moves_list):
                color = "white" if i % 2 == 0 else "black"
                username = white_name if color == "white" else black_name

                move_rows.append({
                    "game_id": game_id,
                    "move_offset": i,                    # 0-indexed ply
                    "move_number": (i // 2) + 1,         # 1-indexed full move
                    "color": color,
                    "username": username,
                    "move_san": move_san,
                    "ingested_at": ingested_at,
                })

                eval_data = analysis_list[i] if i < len(analysis_list) else {}
                time_left_cs = clocks_list[i]   if i < len(clocks_list)   else None

                eval_rows.append({
                    "game_id": game_id,
                    "move_offset": i,
                    "eval_score": eval_data.get("eval"),
                    "mate_in": eval_data.get("mate"),
                    "best_move": eval_data.get("best"),
                    "variation": eval_data.get("variation"),
                    "judgment": eval_data.get("judgment", {}).get("name"),
                    "time_left_cs": time_left_cs,
                    "time_left_ratio": (
                        round((time_left_cs / 100) / initial, 4)
                        if time_left_cs is not None and initial > 0
                        else None
                    ),
                    "ingested_at": ingested_at,
                })

        return pd.DataFrame(game_rows), pd.DataFrame(move_rows), pd.DataFrame(eval_rows)

    # ── CSV output ──────────────────────────────────────────────────────────

    def _save_csvs(
        self,
        games_df: pd.DataFrame,
        moves_df: pd.DataFrame,
        evals_df: pd.DataFrame,
        label: str = "export",
    ) -> None:
        """Write 3 CSVs to self.output_dir."""
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        paths = {
            "games": os.path.join(self.output_dir, f"{label}_games_{stamp}.csv"),
            "moves": os.path.join(self.output_dir, f"{label}_moves_{stamp}.csv"),
            "move_evals": os.path.join(self.output_dir, f"{label}_move_evals_{stamp}.csv"),
        }

        games_df.to_csv(paths["games"],      index=False)
        moves_df.to_csv(paths["moves"],      index=False)
        evals_df.to_csv(paths["move_evals"], index=False)

        logger.info(f"Saved CSVs:\n  {paths['games']}\n  {paths['moves']}\n  {paths['move_evals']}")

    # ── BigQuery loading ────────────────────────────────────────────────────

    def _load_all(
        self,
        games_df: pd.DataFrame,
        moves_df: pd.DataFrame,
        evals_df: pd.DataFrame,
    ) -> None:
        self._load_table(games_df, "games", self.GAMES_SCHEMA)
        self._load_table(moves_df, "moves", self.MOVES_SCHEMA)
        self._load_table(evals_df, "move_evals", self.EVALS_SCHEMA)

    def _load_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: list,
    ) -> None:
        table_id = f"{self.bq_project}.{self.bq_dataset}.{table_name}"
        job = self.bq_client.load_table_from_dataframe(
            df,
            table_id,
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                write_disposition="WRITE_APPEND",
            ),
        )
        job.result()
        logger.info(f"Loaded {len(df):,} rows → {table_id}")

    # ── Lichess API ─────────────────────────────────────────────────────────

    def _fetch_games(self, username: str, perf_types: list, max_games: int) -> list:
        resp = requests.get(
            f"https://lichess.org/api/games/user/{username}",
            headers={"Accept": "application/x-ndjson"},
            params={
                "max": max_games,
                "perfType": ",".join(perf_types),
                "pgnInJson": "true",
                "clocks": "true",
                "evals": "true",
                "opening": "true",
                "accuracy": "true",
            },
            timeout=120,
        )
        resp.raise_for_status()
        return [
            json.loads(line)
            for line in resp.content.decode("utf-8").strip().split("\n")
            if line.strip()
        ]


if __name__ == "__main__":
    DataIngestion(
        bq_project="chess-497919",
        bq_dataset="lichess_raw",
        output_dir="data",
    ).run_user_games(
        username="stevenhan",
        perf_type="rapid+classical",
        max_games=5000,
    )