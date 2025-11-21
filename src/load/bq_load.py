from src.config import GCS_BUCKET
from src.utils.gcp import load_parquet_to_bq

def run():
    base_uri = f"gs://{GCS_BUCKET}/curated"
    load_parquet_to_bq("dim_team", f"{base_uri}/dim_team.parquet")
    load_parquet_to_bq("dim_player", f"{base_uri}/dim_player.parquet")
    load_parquet_to_bq("fact_player_season", f"{base_uri}/fact_player_season.parquet")


if __name__ == "__main__":
    run()
