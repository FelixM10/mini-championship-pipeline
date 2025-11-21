import pandas as pd

from src.config import RAW_DATA_DIR, CURATED_DATA_DIR, GCS_BUCKET
from src.utils.gcp import upload_file_to_gcs
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


def load_raw_data():
    tm_league = pd.read_csv(RAW_DATA_DIR / "transfermarkt_league_table_2024_25.csv")
    fb_players = pd.read_csv(RAW_DATA_DIR / "fbref_players_standard_2024_25.csv")
    return tm_league, fb_players


def build_dim_team(tm_league: pd.DataFrame) -> pd.DataFrame:
    # You may want to inspect tm_league to adjust column names
    potential_cols = ["club", "squad", "goals", "points"]
    cols = [c for c in potential_cols if c in tm_league.columns]

    dim_team = tm_league[cols].copy()
    if "club" in dim_team.columns:
        dim_team.rename(columns={"club": "team_name"}, inplace=True)
    elif "squad" in dim_team.columns:
        dim_team.rename(columns={"squad": "team_name"}, inplace=True)

    dim_team["team_id"] = (
        dim_team["team_name"]
        .str.lower()
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .str.replace(" ", "_")
    )
    dim_team = dim_team.drop_duplicates("team_id")
    dim_team = dim_team[["team_id"] + [c for c in dim_team.columns if c != "team_id"]]
    return dim_team


def build_dim_player(fb_players: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in ["player", "nation", "pos", "age"] if c in fb_players.columns]
    dim_player = fb_players[cols].drop_duplicates()
    dim_player.rename(
        columns={
            "player": "player_name",
            "nation": "nationality",
            "pos": "position",
        },
        inplace=True,
    )

    dim_player["player_id"] = (
        dim_player["player_name"]
        .str.lower()
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .str.replace(" ", "_")
    )
    dim_player = dim_player[["player_id"] + [c for c in dim_player.columns if c != "player_id"]]
    return dim_player


def build_fact_player_season(
    fb_players: pd.DataFrame, dim_team: pd.DataFrame, dim_player: pd.DataFrame
) -> pd.DataFrame:
    df = fb_players.copy()
    # Standard FBRef column names
    if "squad" in df.columns:
        df.rename(columns={"squad": "team_name"}, inplace=True)
    if "player" in df.columns:
        df.rename(columns={"player": "player_name"}, inplace=True)

    df = df.merge(
        dim_team[["team_id", "team_name"]],
        how="left",
        on="team_name",
    ).merge(
        dim_player[["player_id", "player_name"]],
        how="left",
        on="player_name",
    )

    df["season"] = "2024-2025"

    # Move keys to front
    key_cols = ["season", "team_id", "player_id"]
    other_cols = [c for c in df.columns if c not in key_cols and c not in ["team_name", "player_name"]]
    fact_df = df[key_cols + other_cols]

    return fact_df


def save_parquet_and_upload(df: pd.DataFrame, name: str):
    CURATED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    path_parquet = CURATED_DATA_DIR / f"{name}.parquet"
    df.to_parquet(path_parquet, index=False)
    upload_file_to_gcs(path_parquet, f"curated/{name}.parquet")


def run():
    tm_league, fb_players = load_raw_data()

    dim_team = build_dim_team(tm_league)
    dim_player = build_dim_player(fb_players)
    fact_player_season = build_fact_player_season(fb_players, dim_team, dim_player)

    save_parquet_and_upload(dim_team, "dim_team")
    save_parquet_and_upload(dim_player, "dim_player")
    save_parquet_and_upload(fact_player_season, "fact_player_season")

    logger.info("Transform step finished")


if __name__ == "__main__":
    run()
