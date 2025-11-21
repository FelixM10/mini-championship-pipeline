# src/extract/fbref_championship.py

from pathlib import Path
from typing import List, Dict

import pandas as pd
from bs4 import BeautifulSoup, Comment

from src.config import RAW_DATA_DIR
from src.utils.gcp import upload_file_to_gcs
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

FBREF_HTML_FILENAME = "fbref_championship_standard_2024_25.html"
FBREF_HTML_PATH = RAW_DATA_DIR / FBREF_HTML_FILENAME

FBREF_BASE_URL = "https://fbref.com"


# ---------- HTML loading ----------


def load_fbref_html_from_file() -> str:
    """
    Load the locally saved FBRef HTML snapshot.

    We do NOT request fbref.com here (robots + 403).
    You should manually download the 2024-2025 Championship
    'Player Standard Stats' page and save it as:

        data/raw/fbref_championship_standard_2024_25.html
    """
    if not FBREF_HTML_PATH.exists():
        raise FileNotFoundError(
            f"FBRef HTML file not found at {FBREF_HTML_PATH}. "
            "Download the 2024-2025 Championship 'Player Standard Stats' page "
            "and save its HTML to this path."
        )

    logger.info(f"Loading FBRef HTML from {FBREF_HTML_PATH}")
    return FBREF_HTML_PATH.read_text(encoding="utf-8")


# ---------- Table extraction helpers ----------


def extract_player_table(html: str):
    """
    Player standard stats table lives inside div#all_stats_standard
    as a commented-out <table>. Extract and return that <table>.
    """
    soup = BeautifulSoup(html, "lxml")

    container = soup.find("div", id="all_stats_standard")
    if container is None:
        raise RuntimeError("Could not find div#all_stats_standard in FBRef HTML")

    table_comment = None
    for c in container.find_all(string=lambda t: isinstance(t, Comment)):
        if "<table" in c:
            table_comment = c
            break

    if table_comment is None:
        raise RuntimeError("No commented <table> found inside div#all_stats_standard")

    inner_soup = BeautifulSoup(table_comment, "lxml")
    table = inner_soup.find("table")
    if table is None:
        raise RuntimeError("Could not find <table> inside commented player block")

    return table


def extract_squad_table(html: str):
    """
    Squad standard stats table is a direct <table> inside
    div#div_stats_squads_standard_for (not commented).

    We grab that table explicitly.
    """
    soup = BeautifulSoup(html, "lxml")

    container = soup.find("div", id="div_stats_squads_standard_for")
    if container is None:
        # Fallback: try table by id if structure changes slightly
        table = soup.find("table", id=lambda v: v and "stats_squads_standard_for" in v)
        if table is None:
            raise RuntimeError(
                "Could not find div#div_stats_squads_standard_for or "
                "table with id containing 'stats_squads_standard_for' in FBRef HTML"
            )
        return table

    table = container.find("table")
    if table is None:
        raise RuntimeError("div#div_stats_squads_standard_for has no <table>")

    return table


# ---------- Core parsing: PLAYER TABLE ----------


def parse_player_standard_stats(table) -> pd.DataFrame:
    """
    Parse the FBRef *player* standard stats <table> into a raw DataFrame
    using data-stat attributes as column keys.

    For 'matches' cells we store the full URL instead of the text 'Matches'.
    """
    tbody = table.find("tbody")
    if tbody is None:
        raise RuntimeError("Player standard stats table has no <tbody>")

    records: List[Dict[str, str]] = []

    for tr in tbody.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if not cells:
            continue

        row_data: Dict[str, str] = {}
        for cell in cells:
            stat = cell.get("data-stat")
            if not stat:
                continue

            if stat == "matches":
                a = cell.find("a")
                href = a.get("href") if a else ""
                if href and href.startswith("/"):
                    href = FBREF_BASE_URL + href
                row_data[stat] = href
            else:
                text = cell.get_text(" ", strip=True)
                row_data[stat] = text

        player = row_data.get("player", "")
        ranker = row_data.get("ranker", "")

        # Skip rows without a player name or numeric rank (e.g. totals/header rows)
        if not player or not ranker.isdigit():
            continue

        records.append(row_data)

    if not records:
        raise RuntimeError("No player rows parsed from FBRef player standard stats table")

    df = pd.DataFrame(records)
    logger.info("Parsed raw FBRef player table with shape=%s and columns=%s", df.shape, list(df.columns))
    return df


# ---------- Core parsing: SQUAD TABLE ----------


def parse_squad_standard_stats(table) -> pd.DataFrame:
    """
    Parse the FBRef *squad* standard stats <table> into a raw DataFrame,
    using each cell's data-stat attribute as the column key.
    """
    tbody = table.find("tbody")
    if tbody is None:
        raise RuntimeError("Squad standard stats table has no <tbody>")

    records: List[Dict[str, str]] = []

    for tr in tbody.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if not cells:
            continue

        row_data: Dict[str, str] = {}
        for cell in cells:
            stat = cell.get("data-stat")
            if not stat:
                continue
            text = cell.get_text(" ", strip=True)
            row_data[stat] = text

        squad = row_data.get("team", "")
        if not squad:
            continue

        records.append(row_data)

    if not records:
        raise RuntimeError("No squad rows parsed from FBRef squad standard stats table")

    df = pd.DataFrame(records)
    logger.info("Parsed raw FBRef squad table with shape=%s and columns=%s", df.shape, list(df.columns))
    return df


# ---------- Cleaning & renaming: PLAYER DF ----------


def tidy_player_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and rename player-level FBRef stats:
      - rename data-stat columns to a tidy schema
      - normalise nation
      - convert numeric columns to numbers
      - keep matches as URL
    """
    rename_map = {
        # core identity
        "ranker": "rk",
        "player": "player",
        "nationality": "nation",
        "position": "pos",
        "team": "squad",
        "age": "age",
        "birth_year": "born",
        # playing time
        "games": "mp",
        "games_starts": "starts",
        "minutes": "min",
        "minutes_90s": "90s",
        # raw performance
        "goals": "gls",
        "assists": "ast",
        "goals_assists": "g+a",
        "goals_pens": "g-pk",
        "pens_made": "pk",
        "pens_att": "pkatt",
        "cards_yellow": "crdy",
        "cards_red": "crdr",
        # expected
        "xg": "xg",
        "npxg": "npxg",
        "xg_assist": "xag",
        "npxg_xg_assist": "npxg+xag",
        # progression
        "progressive_carries": "prgc",
        "progressive_passes": "prgp",
        "progressive_passes_received": "prgr",
        # per 90 – add p90_ prefix
        "goals_per90": "p90_gls",
        "assists_per90": "p90_ast",
        "goals_assists_per90": "p90_g+a",
        "goals_pens_per90": "p90_g-pk",
        "goals_assists_pens_per90": "p90_g+a-pk",
        "xg_per90": "p90_xg",
        "xg_assist_per90": "p90_xag",
        "xg_xg_assist_per90": "p90_xg+xag",
        "npxg_per90": "p90_npxg",
        "npxg_xg_assist_per90": "p90_npxg+xag",
        # matches link
        "matches": "matches",
    }

    df = df.rename(columns=rename_map)

    # Normalise nationality -> 3-letter country code in upper case
    if "nation" in df.columns:
        df["nation"] = (
            df["nation"]
            .astype(str)
            .str.strip()
            .str.split()
            .str[-1]  # 'us USA' -> 'USA'
            .str.upper()
        )

    # Convert numeric columns (remove ',' and parse)
    non_numeric_cols = {"player", "nation", "pos", "squad", "matches"}

    for col in df.columns:
        if col in non_numeric_cols:
            continue
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(",", "", regex=False),
            errors="coerce",
        )

    # Column order similar to your sample
    desired_order = [
        "rk",
        "player",
        "nation",
        "pos",
        "squad",
        "age",
        "born",
        "mp",
        "starts",
        "min",
        "90s",
        "gls",
        "ast",
        "g+a",
        "g-pk",
        "pk",
        "pkatt",
        "crdy",
        "crdr",
        "xg",
        "npxg",
        "xag",
        "npxg+xag",
        "prgc",
        "prgp",
        "prgr",
        "p90_gls",
        "p90_ast",
        "p90_g+a",
        "p90_g-pk",
        "p90_g+a-pk",
        "p90_xg",
        "p90_xag",
        "p90_xg+xag",
        "p90_npxg",
        "p90_npxg+xag",
        "matches",
    ]
    ordered = [c for c in desired_order if c in df.columns]
    remaining = [c for c in df.columns if c not in ordered]
    df = df[ordered + remaining]

    logger.info("Tidied FBRef player stats; final columns=%s", list(df.columns))
    return df


# ---------- Cleaning & renaming: SQUAD DF ----------


def tidy_squad_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and rename squad-level FBRef stats to match the Squad Standard
    Stats table on the site.

    The THEAD snippet you shared shows:
      data-stat="team"         -> Squad
      data-stat="players_used" -> # Pl
      data-stat="avg_age"      -> Age
      data-stat="possession"   -> Poss
      ... plus the usual games, goals, xg, per90, etc.
    """
    rename_map = {
        "team": "squad",
        "players_used": "players",
        "players": "players",  # safety
        "avg_age": "age",
        "possession": "poss",
        # playing time
        "games": "mp",
        "games_starts": "starts",
        "minutes": "min",
        "minutes_90s": "90s",
        # raw performance
        "goals": "gls",
        "assists": "ast",
        "goals_assists": "g+a",
        "goals_pens": "g-pk",
        "pens_made": "pk",
        "pens_att": "pkatt",
        "cards_yellow": "crdy",
        "cards_red": "crdr",
        # expected
        "xg": "xg",
        "npxg": "npxg",
        "xg_assist": "xag",
        "npxg_xg_assist": "npxg+xag",
        # progression
        "progressive_carries": "prgc",
        "progressive_passes": "prgp",
        "progressive_passes_received": "prgr",
        # per 90
        "goals_per90": "p90_gls",
        "assists_per90": "p90_ast",
        "goals_assists_per90": "p90_g+a",
        "goals_pens_per90": "p90_g-pk",
        "goals_assists_pens_per90": "p90_g+a-pk",
        "xg_per90": "p90_xg",
        "xg_assist_per90": "p90_xag",
        "xg_xg_assist_per90": "p90_xg+xag",
        "npxg_per90": "p90_npxg",
        "npxg_xg_assist_per90": "p90_npxg+xag",
    }

    df = df.rename(columns=rename_map)

    # Convert numeric columns (remove ',' and parse)
    non_numeric_cols = {"squad"}

    for col in df.columns:
        if col in non_numeric_cols:
            continue
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(",", "", regex=False),
            errors="coerce",
        )

    desired_order = [
        "squad",
        "players",
        "age",
        "poss",
        "mp",
        "starts",
        "min",
        "90s",
        "gls",
        "ast",
        "g+a",
        "g-pk",
        "pk",
        "pkatt",
        "crdy",
        "crdr",
        "xg",
        "npxg",
        "xag",
        "npxg+xag",
        "prgc",
        "prgp",
        "prgr",
        "p90_gls",
        "p90_ast",
        "p90_g+a",
        "p90_g-pk",
        "p90_g+a-pk",
        "p90_xg",
        "p90_xag",
        "p90_xg+xag",
        "p90_npxg",
        "p90_npxg+xag",
    ]
    ordered = [c for c in desired_order if c in df.columns]
    remaining = [c for c in df.columns if c not in ordered]
    df = df[ordered + remaining]

    logger.info("Tidied FBRef squad stats; final columns=%s", list(df.columns))
    return df


# ---------- Main entrypoint ----------


def run() -> pd.DataFrame:
    """
    Main entrypoint:
      - read local FBRef HTML snapshot
      - parse BOTH squad and player standard stats tables
      - clean & normalise each
      - write TWO CSVs:
          * fbref_championship_player_standard_stats_2024_25.csv
          * fbref_championship_squad_standard_stats_2024_25.csv
      - upload both to GCS
    """
    html = load_fbref_html_from_file()

    # Player table
    player_table = extract_player_table(html)
    raw_player_df = parse_player_standard_stats(player_table)
    player_df = tidy_player_df(raw_player_df)

    # Squad table
    squad_table = extract_squad_table(html)
    raw_squad_df = parse_squad_standard_stats(squad_table)
    squad_df = tidy_squad_df(raw_squad_df)

    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    player_out = RAW_DATA_DIR / "fbref_championship_player_standard_stats_2024_25.csv"
    squad_out = RAW_DATA_DIR / "fbref_championship_squad_standard_stats_2024_25.csv"

    player_df.to_csv(player_out, index=False)
    squad_df.to_csv(squad_out, index=False)

    upload_file_to_gcs(player_out, "raw/fbref/player_standard_stats_2024_25.csv")
    upload_file_to_gcs(squad_out, "raw/fbref/squad_standard_stats_2024_25.csv")

    logger.info(
        "FBRef standard stats extract complete. Players=%d, Squads=%d",
        len(player_df),
        len(squad_df),
    )

    return player_df


if __name__ == "__main__":
    run()
