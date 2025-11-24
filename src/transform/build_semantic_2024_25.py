"""
Build curated semantic tables for the 2024/25 Championship.

Inputs (in GCS):

- gs://<GCS_BUCKET>/fbref/championship_2024_25/raw/
    * fbref_championship_player_standard_stats_2024_25.csv
    * fbref_championship_squad_standard_stats_2024_25.csv

- gs://<GCS_BUCKET>/fbref/championship_2024_25/transform/
    * fbref_championship_player_advanced_stats_2024_25.csv

- gs://<GCS_BUCKET>/transfermarkt/championship_2024_25/raw/
    * transfermarkt_league_table_2024_25.csv
    * transfermarkt_transfers_in_2024_25.csv
    * transfermarkt_transfers_out_2024_25.csv

Outputs (locally + in GCS under 'curated/'):

- player_stats_semantic_2024_25.csv
- player_advanced_stats_2024_25.csv
- transfers_in_semantic_2024_25.csv
- transfers_out_semantic_2024_25.csv
- league_table_enhanced_2024_25.csv
"""

from __future__ import annotations

from pathlib import Path
import io
import re
from typing import Tuple

import pandas as pd
from google.cloud import storage

from src.config import RAW_DATA_DIR, CURATED_DATA_DIR, GCS_BUCKET, GCP_PROJECT_ID
from src.utils.logging_utils import get_logger
from src.utils.dim_club_24_25 import (
    load_dim_club,
    attach_club_id,
    standardize_club_name,
)
from src.utils.dim_country import normalize_country
from src.utils.gcp import upload_df_to_gcs  # cloud-first upload

logger = get_logger(__name__)

# ---------------------------------------------------------
# Local paths for curated outputs (optional, for debugging)
# ---------------------------------------------------------

DATA_DIR = RAW_DATA_DIR.parent           # e.g. data/
CURATED_DIR = CURATED_DATA_DIR           # configured in config.py
TRANSFORM_DIR = DATA_DIR / "transform"   # kept for backwards compat if needed


def ensure_curated_dir() -> Path:
    CURATED_DIR.mkdir(parents=True, exist_ok=True)
    return CURATED_DIR


# ---------------------------------------------------------
# GCS config + helpers
# ---------------------------------------------------------

FBREF_RAW_PREFIX_GCS = "fbref/championship_2024_25/raw"
FBREF_TRANSFORM_PREFIX_GCS = "fbref/championship_2024_25/transform"
TM_RAW_PREFIX_GCS = "transfermarkt/championship_2024_25/raw"
CURATED_PREFIX_GCS = "curated"  # where semantic outputs are written in the bucket

_storage_client = storage.Client(project=GCP_PROJECT_ID)


def read_csv_from_gcs(blob_name: str) -> pd.DataFrame:
    """
    Read a CSV from GCS into a DataFrame.

    blob_name: path within the bucket, e.g.
      'fbref/championship_2024_25/raw/fbref_championship_player_standard_stats_2024_25.csv'
    """
    bucket = _storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(blob_name)

    logger.info("Reading CSV from gs://%s/%s", GCS_BUCKET, blob_name)
    csv_text = blob.download_as_text(encoding="utf-8")
    return pd.read_csv(io.StringIO(csv_text))


# =========================================================
# Helpers
# =========================================================

def parse_goals_for_against(value: str) -> Tuple[float, float]:
    """
    Parse '95:30' -> (95.0, 30.0).
    """
    if not isinstance(value, str):
        return float("nan"), float("nan")
    parts = value.split(":")
    if len(parts) != 2:
        return float("nan"), float("nan")
    try:
        gf = float(parts[0])
        ga = float(parts[1])
    except ValueError:
        gf, ga = float("nan"), float("nan")
    return gf, ga


def parse_transfer_fee_to_eur(value: str) -> float:
    """
    Parse Transfermarkt fee strings into a numeric EUR amount.

    Examples:
        "€1.80m"           -> 1_800_000
        "€400k"            -> 400_000
        "Loan fee: €500k"  -> 500_000
        "free transfer"    -> 0
        "-" or "?"         -> 0
        "End of loan ..."  -> 0 (returns are not new spending)
    """
    if not isinstance(value, str):
        return 0.0

    s = value.strip()
    if not s or s in {"-", "?", "–"}:
        return 0.0

    low = s.lower()

    # End of loan / internal adjustments -> treat as zero spend
    if "end of loan" in low:
        return 0.0

    if "free" in low and "€" not in s:
        return 0.0

    # If there's a "Loan fee: €X", extract the €X part
    if "loan fee" in low and "€" in s:
        s = s[s.index("€"):].strip()

    m = re.search(r"€\s*([\d\.]+)\s*([mk])?", s, flags=re.IGNORECASE)
    if not m:
        return 0.0

    number = float(m.group(1))
    suffix = (m.group(2) or "").lower()

    if suffix == "m":
        return number * 1_000_000
    if suffix == "k":
        return number * 1_000

    return number


# =========================================================
# Player stats semantic (standard FBRef player table)
# =========================================================

def build_player_stats_semantic(dim_club: pd.DataFrame) -> pd.DataFrame:
    """
    Attach club_id and canonical club name ('club') to FBRef player stats,
    and normalise nationality to canonical full country names.

    Source (GCS):
      gs://<GCS_BUCKET>/fbref/championship_2024_25/raw/
        fbref_championship_player_standard_stats_2024_25.csv
    """
    blob_name = (
        f"{FBREF_RAW_PREFIX_GCS}/"
        "fbref_championship_player_standard_stats_2024_25.csv"
    )
    df = read_csv_from_gcs(blob_name)

    # FBRef column is usually 'nation' with ISO-like codes (e.g. "GAM", "CUW")
    # Rename for consistency across tables
    if "nation" in df.columns:
        df.rename(columns={"nation": "nationality"}, inplace=True)
    if "pos" in df.columns:
        df.rename(columns={"pos": "position"}, inplace=True)
    if "player" in df.columns:
        df.rename(columns={"player": "player_name"}, inplace=True)

    # Normalise nationality before attaching club_id / merging
    if "nationality" in df.columns:
        df["nationality"] = df["nationality"].astype(str).apply(normalize_country)

    # Attach club_id based on FBRef 'squad' labels
    df = attach_club_id(df, col="squad", dim_club=dim_club)

    # Add canonical club name and drop raw squad label
    df = df.merge(
        dim_club[["club_id", "canonical_club_name"]],
        on="club_id",
        how="left",
    )

    df = df.rename(columns={"canonical_club_name": "club"})

    if "squad" in df.columns:
        df = df.drop(columns=["squad"])

    if "rk" in df.columns:
        df = df.drop(columns=["rk"])

    # Order: club_id, club near the front
    front_cols = ["club_id", "club"]
    other_cols = [c for c in df.columns if c not in front_cols]
    df = df[front_cols + other_cols]

    logger.info(
        "Built player_stats_semantic with shape=%s and columns=%s",
        df.shape,
        list(df.columns),
    )
    return df


# =========================================================
# Player advanced stats semantic
# =========================================================

def build_player_advanced_semantic(
    dim_club: pd.DataFrame,
    player_standard_sem: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build a merged player advanced stats table.

    Advanced stats source (GCS):
      gs://<GCS_BUCKET>/fbref/championship_2024_25/transform/
        fbref_championship_player_advanced_stats_2024_25.csv
    """
    adv_blob = (
        f"{FBREF_TRANSFORM_PREFIX_GCS}/"
        "fbref_championship_player_advanced_stats_2024_25.csv"
    )
    adv = read_csv_from_gcs(adv_blob)

    # Advanced table currently has 'player' and 'squad'
    if "player" in adv.columns:
        adv.rename(columns={"player": "player_name"}, inplace=True)

    # Attach club_id based on FBRef 'squad' labels
    adv = attach_club_id(adv, col="squad", dim_club=dim_club)

    # Add canonical club name as 'club' (same as standard)
    adv = adv.merge(
        dim_club[["club_id", "canonical_club_name"]],
        on="club_id",
        how="left",
    ).rename(columns={"canonical_club_name": "club"})

    # We don't need 'squad' anymore after club_id/canonical handling
    if "squad" in adv.columns:
        adv = adv.drop(columns=["squad"])

    # Ensure consistent column order for advanced table too:
    front_adv = ["club_id", "club", "player_name"]
    other_adv = [c for c in adv.columns if c not in front_adv]
    adv = adv[front_adv + other_adv]

    # For merging: drop 'club' from advanced table and merge on ['club_id', 'player_name']
    adv_for_merge = adv.drop(columns=["club"])

    merged = player_standard_sem.merge(
        adv_for_merge,
        on=["club_id", "player_name"],
        how="left",
        suffixes=("", "_adv"),
    )

    logger.info(
        "Built player_advanced_semantic with shape=%s and columns=%s",
        merged.shape,
        list(merged.columns),
    )

    return merged


# =========================================================
# Transfers semantic
# =========================================================

def build_transfers_semantic(dim_club: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build semantic transfers_in and transfers_out tables from GCS:

      gs://<GCS_BUCKET>/transfermarkt/championship_2024_25/raw/
        transfermarkt_transfers_in_2024_25.csv
        transfermarkt_transfers_out_2024_25.csv
    """
    tin_blob = f"{TM_RAW_PREFIX_GCS}/transfermarkt_transfers_in_2024_25.csv"
    tout_blob = f"{TM_RAW_PREFIX_GCS}/transfermarkt_transfers_out_2024_25.csv"

    tin = read_csv_from_gcs(tin_blob)
    tout = read_csv_from_gcs(tout_blob)

    # Attach club_id based on Transfermarkt 'Club' labels
    tin = attach_club_id(tin, col="Club", dim_club=dim_club)
    tout = attach_club_id(tout, col="Club", dim_club=dim_club)

    # Add canonical club name for the Championship club itself
    tin = tin.merge(
        dim_club[["club_id", "canonical_club_name"]],
        on="club_id",
        how="left",
    ).rename(columns={"canonical_club_name": "club"})

    tout = tout.merge(
        dim_club[["club_id", "canonical_club_name"]],
        on="club_id",
        how="left",
    ).rename(columns={"canonical_club_name": "club"})

    # Parse fees
    tin["fee_eur"] = tin["Fee"].apply(parse_transfer_fee_to_eur)
    tout["fee_eur"] = tout["Fee"].apply(parse_transfer_fee_to_eur)

    # Drop original 'Club' column (we now have canonical 'club')
    if "Club" in tin.columns:
        tin = tin.drop(columns=["Club"])
    if "Club" in tout.columns:
        tout = tout.drop(columns=["Club"])

    # Rename remaining columns for a cleaner schema
    tin = tin.rename(
        columns={
            "In": "player_name",
            "Age": "age",
            "Nat.": "nationality",
            "Nationality": "nationality",
            "Position": "position",
            "Market value": "market_value",
            "Left": "from_club_name",
            "Fee": "fee_raw",
        }
    )

    tout = tout.rename(
        columns={
            "Out": "player_name",
            "Age": "age",
            "Nat.": "nationality",
            "Nationality": "nationality",
            "Position": "position",
            "Market value": "market_value",
            "Joined": "to_club_name",
            "Fee": "fee_raw",
        }
    )

    # Normalise nationality in transfers
    if "nationality" in tin.columns:
        tin["nationality"] = tin["nationality"].astype(str).apply(normalize_country)
    if "nationality" in tout.columns:
        tout["nationality"] = tout["nationality"].astype(str).apply(normalize_country)

    # --------------------------------------------------------
    # STANDARDISE from_club_name / to_club_name WHEN POSSIBLE
    # --------------------------------------------------------

    def standardize_external_club(raw_name: str) -> str:
        """
        Replace with canonical Championship club name if available,
        otherwise keep original raw string.
        """
        if not isinstance(raw_name, str):
            return raw_name
        try:
            return standardize_club_name(raw_name)
        except KeyError:
            return raw_name

    # Transfers IN
    if "from_club_name" in tin.columns:
        tin["from_club_name"] = tin["from_club_name"].apply(standardize_external_club)

    # Transfers OUT
    if "to_club_name" in tout.columns:
        tout["to_club_name"] = tout["to_club_name"].apply(standardize_external_club)

    # Reorder: club_id, club near the front
    def reorder(df: pd.DataFrame) -> pd.DataFrame:
        front = ["club_id", "club"]
        other = [c for c in df.columns if c not in front]
        return df[front + other]

    tin = reorder(tin)
    tout = reorder(tout)

    logger.info(
        "Built transfers_in_semantic with shape=%s, columns=%s",
        tin.shape,
        list(tin.columns),
    )
    logger.info(
        "Built transfers_out_semantic with shape=%s, columns=%s",
        tout.shape,
        list(tout.columns),
    )

    return tin, tout


# =========================================================
# League enhanced semantic
# =========================================================

def build_league_table_enhanced(
    dim_club: pd.DataFrame,
    transfers_in_sem: pd.DataFrame,
    transfers_out_sem: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build an enhanced league table from GCS sources:

      - base: Transfermarkt league table
      - attach club_id + canonical club (column 'club')
      - parse goals_for/goals_against
      - aggregate transfers_in/out (count + fee_eur)
      - attach FBRef squad summary (prefixed 'squad_')
    """
    league_blob = f"{TM_RAW_PREFIX_GCS}/transfermarkt_league_table_2024_25.csv"
    squad_blob = (
        f"{FBREF_RAW_PREFIX_GCS}/"
        "fbref_championship_squad_standard_stats_2024_25.csv"
    )

    league = read_csv_from_gcs(league_blob)
    squad = read_csv_from_gcs(squad_blob)

    # Attach club_id based on league "club" labels
    league = attach_club_id(league, col="club", dim_club=dim_club)

    # Parse numeric columns
    for col in ["#", "played", "w", "d", "l", "gd", "pts"]:
        if col in league.columns:
            league[col] = pd.to_numeric(league[col], errors="coerce")

    if "goals" in league.columns:
        gf, ga = zip(*league["goals"].map(parse_goals_for_against))
        league["goals_for"] = gf
        league["goals_against"] = ga

    # Attach club_id to FBRef squad stats
    squad = attach_club_id(squad, col="squad", dim_club=dim_club)

    # Build squad summary with 'squad_' prefix
    squad_cols = [c for c in squad.columns if c not in ("club_id", "squad")]
    squad_prefixed = squad[["club_id"] + squad_cols].rename(
        columns={c: f"squad_{c}" for c in squad_cols}
    )

    # Aggregate transfers by club_id
    tin = transfers_in_sem.copy()
    tout = transfers_out_sem.copy()

    if "fee_eur" not in tin.columns:
        tin["fee_eur"] = 0.0
    if "fee_eur" not in tout.columns:
        tout["fee_eur"] = 0.0

    tin_agg = (
        tin.groupby("club_id", as_index=False)
        .agg(
            transfers_in_count=("player_name", "nunique"),
            transfers_in_fees=("fee_eur", "sum"),
        )
    )

    tout_agg = (
        tout.groupby("club_id", as_index=False)
        .agg(
            transfers_out_count=("player_name", "nunique"),
            transfers_out_fees=("fee_eur", "sum"),
        )
    )

    transfers_agg = tin_agg.merge(tout_agg, on="club_id", how="outer").fillna(0.0)
    transfers_agg["net_spend_eur"] = (
        transfers_agg["transfers_in_fees"] - transfers_agg["transfers_out_fees"]
    )

    # Attach canonical club name
    clubs_small = dim_club[["club_id", "canonical_club_name"]]

    # Final join: league + squad + transfers + canonical name
    enhanced = (
        league
        .merge(squad_prefixed, on="club_id", how="left")
        .merge(transfers_agg, on="club_id", how="left")
        .merge(clubs_small, on="club_id", how="left")
    )

    # Replace raw league 'club' with canonical 'club'
    if "club" in enhanced.columns:
        enhanced = enhanced.drop(columns=["club"])

    enhanced = enhanced.rename(columns={"canonical_club_name": "club"})

    # Fill NaNs for transfer metrics with 0
    for col in [
        "transfers_in_count",
        "transfers_in_fees",
        "transfers_out_count",
        "transfers_out_fees",
        "net_spend_eur",
    ]:
        if col in enhanced.columns:
            enhanced[col] = enhanced[col].fillna(0.0)

    # Order: club_id, club near the front
    front = ["club_id", "club"]
    other = [c for c in enhanced.columns if c not in front]
    enhanced = enhanced[front + other]

    logger.info(
        "Built league_table_enhanced with shape=%s and columns=%s",
        enhanced.shape,
        list(enhanced.columns),
    )

    return enhanced


# =========================================================
# Main orchestrator
# =========================================================

def main() -> None:
    ensure_curated_dir()

    # 0) Load dim_club (from CSV, or build+write it if missing)
    dim_club = load_dim_club()
    logger.info("Loaded dim_club with %d rows", len(dim_club))

    # 1) Player stats semantic (standard)
    player_sem = build_player_stats_semantic(dim_club)
    player_path = CURATED_DIR / "player_stats_semantic_2024_25.csv"
    # Optional local write for debugging:
    #player_sem.to_csv(player_path, index=False)
    # Cloud-first upload:
    upload_df_to_gcs(player_sem, f"{CURATED_PREFIX_GCS}/{player_path.name}")
    logger.info(
        "Player stats semantic written locally to %s and uploaded to gs://%s/%s",
        player_path,
        GCS_BUCKET,
        f"{CURATED_PREFIX_GCS}/{player_path.name}",
    )

    # 1b) Player advanced stats semantic (standard + advanced merged)
    player_adv_sem = build_player_advanced_semantic(dim_club, player_sem)
    player_adv_path = CURATED_DIR / "player_advanced_stats_2024_25.csv"
    #player_adv_sem.to_csv(player_adv_path, index=False)
    upload_df_to_gcs(
        player_adv_sem, f"{CURATED_PREFIX_GCS}/{player_adv_path.name}"
    )
    logger.info(
        "Player advanced stats semantic written locally to %s and uploaded to gs://%s/%s",
        player_adv_path,
        GCS_BUCKET,
        f"{CURATED_PREFIX_GCS}/{player_adv_path.name}",
    )

    # 2) Transfers semantic
    transfers_in_sem, transfers_out_sem = build_transfers_semantic(dim_club)
    tin_path = CURATED_DIR / "transfers_in_semantic_2024_25.csv"
    tout_path = CURATED_DIR / "transfers_out_semantic_2024_25.csv"
    #transfers_in_sem.to_csv(tin_path, index=False)
    #transfers_out_sem.to_csv(tout_path, index=False)
    upload_df_to_gcs(transfers_in_sem, f"{CURATED_PREFIX_GCS}/{tin_path.name}")
    upload_df_to_gcs(transfers_out_sem, f"{CURATED_PREFIX_GCS}/{tout_path.name}")
    logger.info(
        "Transfers semantic written locally to %s, %s and uploaded to GCS",
        tin_path,
        tout_path,
    )

    # 3) League table enhanced
    league_enhanced = build_league_table_enhanced(
        dim_club=dim_club,
        transfers_in_sem=transfers_in_sem,
        transfers_out_sem=transfers_out_sem,
    )
    league_path = CURATED_DIR / "league_table_enhanced_2024_25.csv"
    #league_enhanced.to_csv(league_path, index=False)
    upload_df_to_gcs(league_enhanced, f"{CURATED_PREFIX_GCS}/{league_path.name}")
    logger.info(
        "League table enhanced written locally to %s and uploaded to gs://%s/%s",
        league_path,
        GCS_BUCKET,
        f"{CURATED_PREFIX_GCS}/{league_path.name}",
    )

    logger.info("Curated semantic build complete.")


if __name__ == "__main__":
    main()
