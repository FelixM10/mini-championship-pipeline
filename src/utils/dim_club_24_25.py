"""
Unified deterministic club mapping for Championship 2024/25.

We define ONE canonical mapping:
    CANONICAL_TO_ALIASES = {
        "Leeds United": ["Leeds", "Leeds Utd", ...],
        ...
    }

From this we generate:
    - dim_club (club_id, canonical_club_name, aliases, club_key)
    - alias → canonical lookup
    - standardize_club_name()
    - attach_club_id() for any dataframe

When run directly, this script writes:
    data/utils/dim_club_2024_25.csv
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import pandas as pd

try:
    from src.utils.logging_utils import get_logger
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)

    def get_logger(name: str):
        return logging.getLogger(name)

logger = get_logger(__name__)

# Where the dim_club CSV will live
DIM_CLUB_CSV = Path("data/utils/dim_club_2024_25.csv")

# ============================================================
# 1) Canonical → aliases mapping (authoritative)
# ============================================================

CANONICAL_TO_ALIASES: Dict[str, List[str]] = {
    "Blackburn Rovers": ["Blackburn", "Blackburn Rovers", "Blackburn Rovers FC"],
    "Bristol City": ["Bristol City"],
    "Burnley": ["Burnley", "Burnley FC"],
    "Cardiff City": ["Cardiff City", "Cardiff"],
    "Coventry City": ["Coventry City", "Coventry"],
    "Derby County": ["Derby County", "Derby"],
    "Hull City": ["Hull City", "Hull"],
    "Leeds United": ["Leeds United", "Leeds", "Leeds Utd"],
    "Luton Town": ["Luton Town", "Luton"],
    "Middlesbrough": ["Middlesbrough", "Middlesbrough FC", "Boro"],
    "Millwall": ["Millwall", "Millwall FC"],
    "Norwich City": ["Norwich City", "Norwich"],
    "Oxford United": ["Oxford United"],
    "Plymouth Argyle": ["Plymouth Argyle", "Plymouth"],
    "Portsmouth": ["Portsmouth", "Portsmouth FC", "Pompey"],
    "Preston North End": ["Preston North End", "Preston"],
    "Queens Park Rangers": ["Queens Park Rangers", "QPR"],
    "Sheffield United": ["Sheffield United", "Sheff Utd", "Sheffield Utd"],
    "Sheffield Wednesday": ["Sheffield Wednesday", "Sheff Wed", "Sheffield Weds"],
    "Stoke City": ["Stoke City", "Stoke"],
    "Sunderland": ["Sunderland", "Sunderland AFC"],
    "Swansea City": ["Swansea City", "Swansea"],
    "Watford": ["Watford", "Watford FC"],
    "West Bromwich Albion": ["West Bromwich Albion", "West Brom", "WBA"],
}

# ============================================================
# 2) Reverse lookup: alias → canonical
# ============================================================

ALIAS_TO_CANONICAL: Dict[str, str] = {}

for canonical, aliases in CANONICAL_TO_ALIASES.items():
    for alias in aliases:
        ALIAS_TO_CANONICAL[alias] = canonical

# Ensure canonical names map to themselves
for canonical in CANONICAL_TO_ALIASES.keys():
    ALIAS_TO_CANONICAL[canonical] = canonical


# ============================================================
# 3) Standardisation function
# ============================================================

def standardize_club_name(raw_name: str) -> str:
    """
    Convert any alias into its canonical club name.
    Raises KeyError if not known (forces explicit handling).
    """
    if not isinstance(raw_name, str):
        raise KeyError(f"Invalid club name (not string): {raw_name!r}")

    raw = raw_name.strip()
    if raw in ALIAS_TO_CANONICAL:
        return ALIAS_TO_CANONICAL[raw]

    raise KeyError(f"Unknown club name: {raw_name!r}")


# ============================================================
# 4) Build / write / load dim_club
# ============================================================

def build_dim_club() -> pd.DataFrame:
    """
    Create dim_club with:
      - club_id
      - canonical_club_name
      - aliases
      - club_key (slug)
    """
    rows = []
    for idx, (canonical, aliases) in enumerate(CANONICAL_TO_ALIASES.items(), start=1):
        rows.append(
            {
                "club_id": idx,
                "canonical_club_name": canonical,
                "aliases": ", ".join(aliases),
                "club_key": canonical.lower().replace(" ", "-"),
            }
        )

    df = pd.DataFrame(rows)
    logger.info("Built dim_club with %d rows", len(df))
    return df


def write_dim_club_csv(path: Path = DIM_CLUB_CSV) -> Path:
    """
    Build dim_club and write it to CSV.
    """
    df = build_dim_club()
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    logger.info("Wrote dim_club to %s", path)
    return path


def load_dim_club(path: Path = DIM_CLUB_CSV) -> pd.DataFrame:
    """
    Load dim_club from CSV if present; otherwise build + write it.
    """
    if path.exists():
        logger.info("Loading dim_club from %s", path)
        return pd.read_csv(path)

    logger.info("dim_club CSV not found, rebuilding...")
    write_dim_club_csv(path)
    return pd.read_csv(path)


# ============================================================
# 5) Generic club_id attachment
# ============================================================

def attach_club_id(df: pd.DataFrame, col: str, dim_club: pd.DataFrame) -> pd.DataFrame:
    """
    Add club_id by standardising club names in df[col].
    """
    if col not in df.columns:
        logger.warning("Column %r missing, returning unchanged.", col)
        return df

    tmp = df.copy()

    tmp["canonical_club_name"] = tmp[col].apply(standardize_club_name)

    tmp = tmp.merge(
        dim_club[["club_id", "canonical_club_name"]],
        on="canonical_club_name",
        how="left",
    )

    return tmp.drop(columns=["canonical_club_name"])


# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":
    write_dim_club_csv()
