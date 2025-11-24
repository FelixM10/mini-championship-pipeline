from functools import reduce
from pathlib import Path
from typing import List, Dict

import pandas as pd
from bs4 import BeautifulSoup, Comment

from src.config import RAW_DATA_DIR, GCS_BUCKET
from src.utils.gcp import upload_df_to_gcs
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

FBREF_HTML_FILENAME = "html/fbref_championship_standard_2024_25.html"
FBREF_HTML_PATH = RAW_DATA_DIR / FBREF_HTML_FILENAME

FBREF_BASE_URL = "https://fbref.com"

# Advanced tables
ADVANCED_PLAYER_TABLES = ("passing", "shooting", "gca", "possession")

# GCS layout for this extractor
GCS_RAW_PREFIX = "fbref/championship_2024_25/raw"
GCS_TRANSFORM_PREFIX = "fbref/championship_2024_25/transform"


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


def load_fbref_html_for_table(table_type: str) -> str:
    """
    Load the locally saved FBRef HTML snapshot for a given advanced player table.

    We expect filenames of the form:
        data/raw/html/fbref_championship_<table_type>_2024_25.html
    """
    html_path = RAW_DATA_DIR / "html" / f"fbref_championship_{table_type}_2024_25.html"

    if not html_path.exists():
        raise FileNotFoundError(
            f"FBRef HTML file for table '{table_type}' not found at {html_path}. "
            f"Download the 'Player {table_type.title()} Stats' page and save it here."
        )

    logger.info("Loading FBRef %s HTML from %s", table_type, html_path)
    return html_path.read_text(encoding="utf-8")


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
    """
    soup = BeautifulSoup(html, "lxml")

    container = soup.find("div", id="div_stats_squads_standard_for")
    if container is None:
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


def extract_advanced_player_table(html: str, table_type: str):
    """
    Extract the *player* table for an 'advanced' stats page
    (passing, shooting, gca, possession).
    """
    soup = BeautifulSoup(html, "lxml")

    container_id = f"all_stats_{table_type}"
    container = soup.find("div", id=container_id)
    if container is None:
        raise RuntimeError(f"Could not find div#{container_id} in FBRef HTML")

    # 1) Try commented-out table
    table_comment = None
    for c in container.find_all(string=lambda t: isinstance(t, Comment)):
        if "<table" in c:
            table_comment = c
            break

    if table_comment is not None:
        inner_soup = BeautifulSoup(table_comment, "lxml")
        table = inner_soup.find("table")
        if table is not None:
            return table

    # 2) Fallback: direct table
    table = container.find("table")
    if table is None:
        raise RuntimeError(f"div#{container_id} has no <table> for table_type='{table_type}'")

    return table


# ---------- Core parsing: PLAYER TABLE ----------

def parse_player_standard_stats(table) -> pd.DataFrame:
    """
    Parse the FBRef *player* standard stats <table> into a raw DataFrame.
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

        if not player or not ranker.isdigit():
            continue

        records.append(row_data)

    if not records:
        raise RuntimeError("No player rows parsed from FBRef player standard stats table")

    df = pd.DataFrame(records)
    logger.info(
        "Parsed raw FBRef player table with shape=%s and columns=%s",
        df.shape,
        list(df.columns),
    )
    return df


# ---------- Core parsing: SQUAD TABLE ----------

def parse_squad_standard_stats(table) -> pd.DataFrame:
    """
    Parse the FBRef *squad* standard stats <table> into a raw DataFrame.
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
    logger.info(
        "Parsed raw FBRef squad table with shape=%s and columns=%s",
        df.shape,
        list(df.columns),
    )
    return df


# ---------- Cleaning & renaming: PLAYER DF ----------

def tidy_player_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and rename player-level FBRef stats.
    """
    rename_map = {
        "ranker": "rk",
        "player": "player",
        "nationality": "nation",
        "position": "pos",
        "team": "squad",
        "age": "age",
        "birth_year": "born",
        "games": "mp",
        "games_starts": "starts",
        "minutes": "min",
        "minutes_90s": "90s",
        "goals": "gls",
        "assists": "ast",
        "goals_assists": "g+a",
        "goals_pens": "g-pk",
        "pens_made": "pk",
        "pens_att": "pkatt",
        "cards_yellow": "crdy",
        "cards_red": "crdr",
        "xg": "xg",
        "npxg": "npxg",
        "xg_assist": "xag",
        "npxg_xg_assist": "npxg+xag",
        "progressive_carries": "prgc",
        "progressive_passes": "prgp",
        "progressive_passes_received": "prgr",
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
        "matches": "matches",
    }

    df = df.rename(columns=rename_map)

    if "nation" in df.columns:
        df["nation"] = (
            df["nation"]
            .astype(str)
            .str.strip()
            .str.split()
            .str[-1]
            .str.upper()
        )

    non_numeric_cols = {"player", "nation", "pos", "squad", "matches"}

    for col in df.columns:
        if col in non_numeric_cols:
            continue
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(",", "", regex=False),
            errors="coerce",
        )

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
    Clean and rename squad-level FBRef stats.
    """
    rename_map = {
        "team": "squad",
        "players_used": "players",
        "players": "players",
        "avg_age": "age",
        "possession": "poss",
        "games": "mp",
        "games_starts": "starts",
        "minutes": "min",
        "minutes_90s": "90s",
        "goals": "gls",
        "assists": "ast",
        "goals_assists": "g+a",
        "goals_pens": "g-pk",
        "pens_made": "pk",
        "pens_att": "pkatt",
        "cards_yellow": "crdy",
        "cards_red": "crdr",
        "xg": "xg",
        "npxg": "npxg",
        "xg_assist": "xag",
        "npxg_xg_assist": "npxg+xag",
        "progressive_carries": "prgc",
        "progressive_passes": "prgp",
        "progressive_passes_received": "prgr",
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


# ---------- Advanced player stats helpers ----------

def _to_numeric(series: pd.Series) -> pd.Series:
    """Convert a column to numeric, stripping commas; errors -> NaN."""
    return pd.to_numeric(series.astype(str).str.replace(",", "", regex=False), errors="coerce")


def extract_passing_features(df: pd.DataFrame, table_type: str = "passing") -> pd.DataFrame:
    df_num = df.copy()
    df_num["passes_completed"] = _to_numeric(df_num["passes_completed"])
    df_num["passes"] = _to_numeric(df_num["passes"])

    df_num["misplaced_passes"] = df_num["passes"] - df_num["passes_completed"]

    out = df_num[["player", "team", "misplaced_passes"]].rename(columns={"team": "squad"})
    return out


def extract_shooting_features(df: pd.DataFrame, table_type: str = "shooting") -> pd.DataFrame:
    df_num = df.copy()
    df_num["shots"] = _to_numeric(df_num["shots"])

    out = df_num[["player", "team", "shots"]].rename(columns={"team": "squad"})
    return out


def extract_gca_features(df: pd.DataFrame, table_type: str = "gca") -> pd.DataFrame:
    df_num = df.copy()
    for col in ["sca", "sca_per90", "gca", "gca_per90"]:
        if col not in df_num.columns:
            raise RuntimeError(
                f"Expected column '{col}' not found in FBRef '{table_type}' table. "
                f"Available columns: {list(df_num.columns)}"
            )
        df_num[col] = _to_numeric(df_num[col])

    out = df_num[["player", "team", "sca", "sca_per90", "gca", "gca_per90"]].rename(
        columns={
            "team": "squad",
            "sca_per90": "sca90",
            "gca_per90": "gca90",
        }
    )
    return out


def extract_possession_features(df: pd.DataFrame, table_type: str = "possession") -> pd.DataFrame:
    df_num = df.copy()
    for col in ["touches", "miscontrols", "dispossessed", "take_ons", "take_ons_won"]:
        if col not in df_num.columns:
            raise RuntimeError(
                f"Expected column '{col}' not found in FBRef '{table_type}' table. "
                f"Available columns: {list(df_num.columns)}"
            )
        df_num[col] = _to_numeric(df_num[col])

    df_num["failed_take_ons"] = df_num["take_ons"] - df_num["take_ons_won"]

    out = df_num[
        ["player", "team", "touches", "miscontrols", "dispossessed", "failed_take_ons"]
    ].rename(columns={"team": "squad"})
    return out


ADVANCED_EXTRACTORS = {
    "passing": extract_passing_features,
    "shooting": extract_shooting_features,
    "gca": extract_gca_features,
    "possession": extract_possession_features,
}


def build_player_advanced_stats() -> pd.DataFrame:
    """
    Build the merged player advanced stats table and upload directly to GCS.
    """
    feature_dfs: List[pd.DataFrame] = []

    for table_type in ADVANCED_PLAYER_TABLES:
        logger.info("Processing FBRef advanced player table: %s", table_type)

        html = load_fbref_html_for_table(table_type)
        table = extract_advanced_player_table(html, table_type)
        raw_df = parse_player_standard_stats(table)

        extractor = ADVANCED_EXTRACTORS[table_type]
        feat_df = extractor(raw_df, table_type=table_type)

        logger.info(
            "Extracted features for '%s'; shape=%s, columns=%s",
            table_type,
            feat_df.shape,
            list(feat_df.columns),
        )
        feature_dfs.append(feat_df)

    if not feature_dfs:
        raise RuntimeError("No advanced feature DataFrames were built for player stats.")

    advanced_df = reduce(
        lambda left, right: pd.merge(left, right, on=["player", "squad"], how="outer"),
        feature_dfs,
    )

    adv_blob = f"{GCS_TRANSFORM_PREFIX}/fbref_championship_player_advanced_stats_2024_25.csv"
    upload_df_to_gcs(advanced_df, adv_blob)

    logger.info(
        "FBRef advanced player stats uploaded to gs://%s/%s with shape=%s and columns=%s",
        GCS_BUCKET,
        adv_blob,
        advanced_df.shape,
        list(advanced_df.columns),
    )

    return advanced_df


# ---------- Main entrypoint ----------

def run() -> pd.DataFrame:
    """
    Main entrypoint:
      - read local FBRef HTML snapshot
      - parse BOTH squad and player standard stats tables
      - clean & normalise each
      - upload TWO CSVs to GCS:
          * fbref_championship_player_standard_stats_2024_25.csv
          * fbref_championship_squad_standard_stats_2024_25.csv
      - also build advanced player stats and upload to:
          * fbref_championship_player_advanced_stats_2024_25.csv
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

    # Optional: keep local CSVs by uncommenting:
    # RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    # player_out = RAW_DATA_DIR / "fbref_championship_player_standard_stats_2024_25.csv"
    # squad_out = RAW_DATA_DIR / "fbref_championship_squad_standard_stats_2024_25.csv"
    # player_df.to_csv(player_out, index=False)
    # squad_df.to_csv(squad_out, index=False)

    # Cloud-first: upload directly to GCS
    player_blob = f"{GCS_RAW_PREFIX}/fbref_championship_player_standard_stats_2024_25.csv"
    squad_blob = f"{GCS_RAW_PREFIX}/fbref_championship_squad_standard_stats_2024_25.csv"

    upload_df_to_gcs(player_df, player_blob)
    upload_df_to_gcs(squad_df, squad_blob)

    logger.info(
        "FBRef standard stats extract complete. "
        "Players=%d (gs://%s/%s), Squads=%d (gs://%s/%s)",
        len(player_df),
        GCS_BUCKET,
        player_blob,
        len(squad_df),
        GCS_BUCKET,
        squad_blob,
    )

    # Build and upload advanced player stats table
    try:
        adv_df = build_player_advanced_stats()
        logger.info(
            "FBRef advanced player stats extract complete. Players=%d",
            len(adv_df),
        )
    except Exception:
        logger.exception("Failed to build advanced player stats table.")

    return player_df, squad_df


if __name__ == "__main__":
    run()
