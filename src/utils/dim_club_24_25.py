"""
Explicit dim_club for Championship 2024/25.

- One canonical club name per club.
- Hard-mapped from:
  * Transfermarkt league table   -> column "club"
  * FBRef squad standard         -> column "squad"
  * Transfermarkt transfers      -> column "Club"

This avoids any fuzzy/heuristic matching and makes
alignment completely deterministic.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd

try:
    from src.config import RAW_DATA_DIR
    from src.utils.logging_utils import get_logger
except ImportError:
    # fallback for standalone usage / testing
    RAW_DATA_DIR = Path("data/raw")
    import logging

    logging.basicConfig(level=logging.INFO)
    def get_logger(name: str):
        return logging.getLogger(name)


logger = get_logger(__name__)


# ---------- Canonical clubs (Championship 2024/25) ----------

CANONICAL_CLUBS = [
    "Blackburn Rovers",
    "Bristol City",
    "Burnley",
    "Cardiff City",
    "Coventry City",
    "Derby County",
    "Hull City",
    "Leeds United",
    "Luton Town",
    "Middlesbrough",
    "Millwall",
    "Norwich City",
    "Oxford United",
    "Plymouth Argyle",
    "Portsmouth",
    "Preston North End",
    "Queens Park Rangers",
    "Sheffield United",
    "Sheffield Wednesday",
    "Stoke City",
    "Sunderland",
    "Swansea City",
    "Watford",
    "West Bromwich Albion",
]

# ---------- Source -> canonical mapping ----------

# Transfermarkt league table "club" column
LEAGUE_TO_CANONICAL = {
    "Leeds": "Leeds United",
    "Burnley": "Burnley",
    "Sheff Utd": "Sheffield United",
    "Sunderland": "Sunderland",
    "Coventry": "Coventry City",
    "Bristol City": "Bristol City",
    "Blackburn": "Blackburn Rovers",
    "Millwall": "Millwall",
    "West Brom": "West Bromwich Albion",
    "Middlesbrough": "Middlesbrough",
    "Swansea": "Swansea City",
    "Sheff Wed": "Sheffield Wednesday",
    "Norwich": "Norwich City",
    "Watford": "Watford",
    "QPR": "Queens Park Rangers",
    "Portsmouth": "Portsmouth",
    "Oxford United": "Oxford United",
    "Stoke City": "Stoke City",
    "Derby": "Derby County",
    "Preston": "Preston North End",
    "Hull City": "Hull City",
    "Luton": "Luton Town",
    "Plymouth": "Plymouth Argyle",
    "Cardiff": "Cardiff City",
}

# FBRef squad standard "squad" column
FBREF_SQUAD_TO_CANONICAL = {
    "Blackburn": "Blackburn Rovers",
    "Bristol City": "Bristol City",
    "Burnley": "Burnley",
    "Cardiff City": "Cardiff City",
    "Coventry City": "Coventry City",
    "Derby County": "Derby County",
    "Hull City": "Hull City",
    "Leeds United": "Leeds United",
    "Luton Town": "Luton Town",
    "Middlesbrough": "Middlesbrough",
    "Millwall": "Millwall",
    "Norwich City": "Norwich City",
    "Oxford United": "Oxford United",
    "Plymouth Argyle": "Plymouth Argyle",
    "Portsmouth": "Portsmouth",
    "Preston": "Preston North End",
    "QPR": "Queens Park Rangers",
    "Sheffield Utd": "Sheffield United",
    "Sheffield Weds": "Sheffield Wednesday",
    "Stoke City": "Stoke City",
    "Sunderland": "Sunderland",
    "Swansea City": "Swansea City",
    "Watford": "Watford",
    "West Brom": "West Bromwich Albion",
}

# Transfermarkt transfers "Club" column
# (from your transfers_in snippet; same idea will apply to transfers_out)
TM_TRANSFERS_CLUB_TO_CANONICAL = {
    "Blackburn Rovers": "Blackburn Rovers",
    "Bristol City": "Bristol City",
    "Burnley FC": "Burnley",
    "Burnley": "Burnley",  # safety
    "Cardiff City": "Cardiff City",
    "Coventry City": "Coventry City",
    "Derby County": "Derby County",
    "Hull City": "Hull City",
    "Leeds United": "Leeds United",
    "Luton Town": "Luton Town",
    "Middlesbrough FC": "Middlesbrough",
    "Middlesbrough": "Middlesbrough",
    "Millwall FC": "Millwall",
    "Millwall": "Millwall",
    "Norwich City": "Norwich City",
    "Oxford United": "Oxford United",
    "Plymouth Argyle": "Plymouth Argyle",
    "Portsmouth FC": "Portsmouth",
    "Portsmouth": "Portsmouth",
    "Preston North End": "Preston North End",
    "Queens Park Rangers": "Queens Park Rangers",
    "Sheffield United": "Sheffield United",
    "Sheffield Wednesday": "Sheffield Wednesday",
    "Stoke City": "Stoke City",
    "Sunderland AFC": "Sunderland",
    "Sunderland": "Sunderland",
    "Swansea City": "Swansea City",
    "Watford FC": "Watford",
    "Watford": "Watford",
    "West Bromwich Albion": "West Bromwich Albion",
}


SourceType = Literal["league", "fbref_squad", "tm_transfers"]


@dataclass
class DimClubConfig:
    canonical_clubs: list[str]
    league_to_canonical: dict[str, str]
    fbref_to_canonical: dict[str, str]
    tm_to_canonical: dict[str, str]


CONFIG = DimClubConfig(
    canonical_clubs=CANONICAL_CLUBS,
    league_to_canonical=LEAGUE_TO_CANONICAL,
    fbref_to_canonical=FBREF_SQUAD_TO_CANONICAL,
    tm_to_canonical=TM_TRANSFERS_CLUB_TO_CANONICAL,
)


# ---------- Core helpers ----------

def canonical_from_source(name: str, source: SourceType) -> str:
    """
    Map a raw club name from a given source to the canonical club name.
    Raises KeyError if the mapping is unknown -> forces you to explicitly
    handle any new/changed labels.
    """
    if name is None or name == "":
        raise KeyError(f"Empty club name for source={source}")

    if source == "league":
        mapping = CONFIG.league_to_canonical
    elif source == "fbref_squad":
        mapping = CONFIG.fbref_to_canonical
    elif source == "tm_transfers":
        mapping = CONFIG.tm_to_canonical
    else:
        raise ValueError(f"Unknown source type: {source}")

    try:
        return mapping[name]
    except KeyError:
        raise KeyError(f"Unknown club name for source={source}: {name!r}") from None


def build_dim_club() -> pd.DataFrame:
    """
    Build dim_club as a static, explicit mapping table.

    Columns:
      - club_id (1..24)
      - canonical_club_name
      - league_raw_name        (string used in Transfermarkt league table)
      - fbref_raw_name         (string used in FBRef squad table)
      - tm_transfers_raw_name  (string used in Transfermarkt transfers Club column)
    """
    df = pd.DataFrame(
        {
            "club_id": range(1, len(CONFIG.canonical_clubs) + 1),
            "canonical_club_name": CONFIG.canonical_clubs,
        }
    )

    # reverse lookups: canonical -> raw name
    league_rev = {v: k for k, v in CONFIG.league_to_canonical.items()}
    fbref_rev = {v: k for k, v in CONFIG.fbref_to_canonical.items()}
    tm_rev = {v: k for k, v in CONFIG.tm_to_canonical.items()}

    df["league_raw_name"] = df["canonical_club_name"].map(league_rev)
    df["fbref_raw_name"] = df["canonical_club_name"].map(fbref_rev)
    df["tm_transfers_raw_name"] = df["canonical_club_name"].map(tm_rev)

    # Optional: add a slug / key if you like
    df["club_key"] = (
        df["canonical_club_name"]
        .str.lower()
        .str.replace(r"[^a-z0-9]+", "-", regex=True)
        .str.strip("-")
    )

    logger.info("Built dim_club with %d rows", len(df))
    return df


def attach_club_id_from_source(
    df: pd.DataFrame,
    club_col: str,
    source: SourceType,
    dim_club: pd.DataFrame,
    new_col: str = "club_id",
) -> pd.DataFrame:
    """
    Deterministically attach club_id to any dataframe with a club column
    from a known source type.

    Example:
        league_df = attach_club_id_from_source(
            league_df, club_col="club", source="league", dim_club=dim_club
        )
    """
    if club_col not in df.columns:
        logger.warning("attach_club_id_from_source: column %r not in df; returning unchanged", club_col)
        return df

    tmp = df.copy()
    tmp["canonical_club_name"] = tmp[club_col].apply(
        lambda x: canonical_from_source(x, source=source)
    )

    tmp = tmp.merge(
        dim_club[["club_id", "canonical_club_name"]],
        on="canonical_club_name",
        how="left",
    )
    tmp = tmp.drop(columns=["canonical_club_name"])
    tmp = tmp.rename(columns={"club_id": new_col})

    missing = tmp[new_col].isna().sum()
    if missing > 0:
        logger.warning(
            "attach_club_id_from_source: %d rows in source=%s had no club_id (unexpected)",
            missing,
            source,
        )

    return tmp


# ---------- Optional: write dim_club to CSV ----------

# ========================================================
# CLI ENTRYPOINT
# ========================================================

DIM_OUTPUT_DIR = Path("data/utils")

def main() -> None:
    """Generate dim_club_2024_25.csv inside data/utils."""
    dim = build_dim_club()

    DIM_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = DIM_OUTPUT_DIR / "dim_club_2024_25.csv"

    dim.to_csv(output_path, index=False)
    logger.info(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
