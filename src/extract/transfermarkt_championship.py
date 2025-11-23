# src/extract/transfermarkt_championship.py

import time
from typing import Tuple, List, Dict, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.config import RAW_DATA_DIR
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

# Custom UA to identify this as a one-off educational script (not GPTBot etc.)
HEADERS = {
    "User-Agent": (
        "championship-pipeline/1.0 "
        "(contact: mutiufelix@gmail.com; non-commercial, educational use)"
    )
}

REQUEST_TIMEOUT = 20
REQUEST_DELAY = 2  # polite delay between requests (robots.txt spirit)


# ---------- Generic helpers ----------

def fetch_html(url: str) -> str:
    """
    Fetch HTML from a Transfermarkt URL with a polite delay.
    Transfermarkt robots.txt allows '/' for generic user-agents, so this is permitted.
    """
    logger.info(f"Fetching {url}")
    resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    time.sleep(REQUEST_DELAY)
    return resp.text


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize dataframe column names to safe snake_case strings.
    (Used for league table only.)
    """
    df.columns = [
        str(c).strip().lower().replace("\xa0", " ").replace(" ", "_")
        for c in df.columns
    ]
    return df


def parse_bs_table_generic(table) -> pd.DataFrame:
    """
    Generic HTML table -> DataFrame, used for the league table.
    """
    if table is None:
        return pd.DataFrame()

    thead = table.find("thead")
    tbody = table.find("tbody")
    if not thead or not tbody:
        return pd.DataFrame()

    header_rows = thead.find_all("tr")
    if not header_rows:
        return pd.DataFrame()

    header_cells = header_rows[-1].find_all(["th", "td"])
    headers = [cell.get_text(" ", strip=True) for cell in header_cells]

    rows: List[List[str]] = []
    for tr in tbody.find_all("tr"):
        cells = tr.find_all("td")
        if not cells:
            continue
        row = [cell.get_text(" ", strip=True) for cell in cells]

        if len(row) < len(headers):
            row += [""] * (len(headers) - len(row))
        elif len(row) > len(headers):
            row = row[:len(headers)]

        rows.append(row)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=headers)
    df = normalize_columns(df)
    return df


# ---------- League table parsing (div id="yw1") ----------

def parse_league_table(html: str) -> pd.DataFrame:
    """
    Extract the Championship league table from the Transfermarkt 'tabelle' page,
    using the exact structure observed:

    Row <td> order:
      0: rank #
      1: crest (ignored)
      2: club name
      3: matches played
      4: W
      5: D
      6: L
      7: Goals (e.g. '95:30')
      8: +/-  (goal difference)
      9: Pts  (points)

    Returns a dataframe with columns:
      ['#', 'club', 'played', 'w', 'd', 'l', 'goals', '+/-', 'pts']
    """
    soup = BeautifulSoup(html, "lxml")

    holder = soup.find("div", id="yw1")
    if holder is None:
        raise ValueError("Could not find div with id='yw1' for league table")

    table = holder.find("table")
    if table is None:
        raise ValueError("Could not find <table> inside div#yw1 for league table")

    tbody = table.find("tbody")
    if tbody is None:
        raise ValueError("Could not find <tbody> in league table")

    records: List[dict] = []

    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 10:
            continue  # skip weird/summary rows

        # rank: first number in the first cell
        rank_text = tds[0].get_text(" ", strip=True)
        rank_token = rank_text.split()[0]
        if not rank_token.isdigit():
            continue  # skip anything that isn't a normal row

        # club name is in td[2], inside an <a>
        club_a = tds[2].find("a")
        club_name = club_a.get_text(" ", strip=True) if club_a else tds[2].get_text(" ", strip=True)

        played = tds[3].get_text(" ", strip=True)
        w = tds[4].get_text(" ", strip=True)
        d = tds[5].get_text(" ", strip=True)
        l = tds[6].get_text(" ", strip=True)
        goals = tds[7].get_text(" ", strip=True)
        goal_diff = tds[8].get_text(" ", strip=True)
        pts = tds[9].get_text(" ", strip=True)

        records.append(
            {
                "#": rank_token,
                "club": club_name,
                "played": played,
                "w": w,
                "d": d,
                "l": l,
                "goals": goals,
                "gd": goal_diff,
                "pts": pts,
            }
        )

    if not records:
        raise ValueError("No data rows parsed from league table")

    league_df = pd.DataFrame(records)

    logger.info(
        "Parsed league table with columns=%s and shape=%s",
        list(league_df.columns),
        league_df.shape,
    )
    return league_df


# ---------- Transfers helpers ----------

def get_club_name_for_container(container) -> Optional[str]:
    """
    For a given 'responsive-table' div, find the nearest preceding <h2>
    (club header) and return its text.
    """
    h2 = container.find_previous("h2")
    if h2:
        text = h2.get_text(" ", strip=True)
        if text:
            return text
    return None


def nat_from_td(td) -> str:
    """Get nationality from <img title=...> or alt in the nationality cell."""
    if td is None:
        return ""
    img = td.find("img")
    if img:
        return img.get("title") or img.get("alt") or ""
    return ""


def parse_transfer_row(tr, club_name: str, in_or_out: str) -> Optional[Dict[str, str]]:
    """
    Parse a single <tr> for either an 'In' or 'Out' table, using the
    actual CSS classes seen in the HTML.
    """
    tds = tr.find_all("td")
    if not tds:
        return None

    # Player is the first <td>, take first <a> text if present
    player_td = tds[0]
    a = player_td.find("a")
    player_name = a.get_text(" ", strip=True) if a else player_td.get_text(" ", strip=True)
    if not player_name:
        return None
    if "average age" in player_name.lower():
        return None

    age = ""
    nat = ""
    pos = ""
    mv = ""
    other_club = ""
    fee = ""

    for td in tds:
        classes = td.get("class", [])

        if "alter-transfer-cell" in classes:
            age = td.get_text(" ", strip=True)

        elif "nat-transfer-cell" in classes:
            nat = nat_from_td(td)

        elif "kurzpos-transfer-cell" in classes:
            # short position code: CF, LW, etc.
            pos = td.get_text(" ", strip=True)

        elif "pos-transfer-cell" in classes and not pos:
            # fallback to long position if short missing
            pos = td.get_text(" ", strip=True)

        elif "mw-transfer-cell" in classes:
            mv = td.get_text(" ", strip=True)

        elif "verein-flagge-transfer-cell" in classes:
            # club name (Left/Joined)
            club_a = td.find("a")
            other_club = club_a.get_text(" ", strip=True) if club_a else td.get_text(" ", strip=True)

        elif "rechts" in classes and "mw-transfer-cell" not in classes and "no-border" not in " ".join(classes):
            # fee cell: class 'rechts', but not the market value cell and not the crest cells
            fee = td.get_text(" ", strip=True)

    if in_or_out == "in":
        return {
            "Club": club_name,
            "In": player_name,
            "Age": age,
            "Nationality": nat,
            "Position": pos,
            "Market value": mv,
            "Left": other_club,
            "Fee": fee,
        }
    else:
        return {
            "Club": club_name,
            "Out": player_name,
            "Age": age,
            "Nationality": nat,
            "Position": pos,
            "Market value": mv,
            "Joined": other_club,
            "Fee": fee,
        }


def parse_transfers(html: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extract Championship transfers from the Transfermarkt 'transfers' page.

    For each <div class="responsive-table">:
      - Determine club name from nearest preceding <h2>.
      - Look at the first header cell ('In' or 'Out').
      - Parse each <tr> using CSS classes as in the provided HTML snippet.

    Returns:
      transfers_in_df, transfers_out_df
    """
    soup = BeautifulSoup(html, "lxml")

    containers = soup.find_all("div", class_="responsive-table")
    logger.info(
        "Found %d div.responsive-table blocks on transfers page", len(containers)
    )

    in_rows: List[Dict[str, str]] = []
    out_rows: List[Dict[str, str]] = []

    for idx_c, container in enumerate(containers):
        table = container.find("table")
        if table is None:
            logger.debug("responsive-table %d has no <table>; skipping", idx_c)
            continue

        club_name = get_club_name_for_container(container) or ""
        if not club_name:
            logger.debug(
                "responsive-table %d has no detectable club header; skipping", idx_c
            )
            continue

        thead = table.find("thead")
        if not thead:
            logger.debug("responsive-table %d has no <thead>; skipping", idx_c)
            continue

        header_row = thead.find_all("tr")[-1]
        header_cells = header_row.find_all(["th", "td"])
        if not header_cells:
            logger.debug("responsive-table %d has no header cells; skipping", idx_c)
            continue

        first_header = header_cells[0].get_text(" ", strip=True).lower()

        tbody = table.find("tbody") or table
        trs = tbody.find_all("tr")

        if first_header.startswith("in"):
            logger.debug(
                "responsive-table %d detected as 'In' table for club %s",
                idx_c,
                club_name,
            )
            for tr in trs:
                row = parse_transfer_row(tr, club_name, "in")
                if row:
                    in_rows.append(row)

        elif first_header.startswith("out"):
            logger.debug(
                "responsive-table %d detected as 'Out' table for club %s",
                idx_c,
                club_name,
            )
            for tr in trs:
                row = parse_transfer_row(tr, club_name, "out")
                if row:
                    out_rows.append(row)

        else:
            logger.debug(
                "responsive-table %d header=%r not recognised as In/Out; skipping",
                idx_c,
                first_header,
            )

    in_columns = ["Club", "In", "Age", "Nationality", "Position", "Market value", "Left", "Fee"]
    out_columns = [
        "Club",
        "Out",
        "Age",
        "Nationality",
        "Position",
        "Market value",
        "Joined",
        "Fee",
    ]

    transfers_in_df = pd.DataFrame(in_rows, columns=in_columns)
    transfers_out_df = pd.DataFrame(out_rows, columns=out_columns)

    logger.info(
        "Parsed transfers_in shape=%s, columns=%s",
        transfers_in_df.shape,
        list(transfers_in_df.columns),
    )
    logger.info(
        "Parsed transfers_out shape=%s, columns=%s",
        transfers_out_df.shape,
        list(transfers_out_df.columns),
    )

    return transfers_in_df, transfers_out_df


# ---------- Main entrypoint ----------

def run() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Extract:
      - league table
      - transfers in
      - transfers out
    for the 2024/25 Championship from Transfermarkt.

    Save raw CSVs locally.
    """
    league_url = (
        "https://www.transfermarkt.co.uk/championship/"
        "tabelle/wettbewerb/GB2/saison_id/2024"
    )
    transfers_url = (
        "https://www.transfermarkt.co.uk/championship/"
        "transfers/wettbewerb/GB2/saison_id/2024"
    )

    league_html = fetch_html(league_url)
    transfers_html = fetch_html(transfers_url)

    league_df = parse_league_table(league_html)
    transfers_in_df, transfers_out_df = parse_transfers(transfers_html)

    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    league_path = RAW_DATA_DIR / "transfermarkt_league_table_2024_25.csv"
    transfers_in_path = RAW_DATA_DIR / "transfermarkt_transfers_in_2024_25.csv"
    transfers_out_path = RAW_DATA_DIR / "transfermarkt_transfers_out_2024_25.csv"

    league_df.to_csv(league_path, index=False)
    transfers_in_df.to_csv(transfers_in_path, index=False)
    transfers_out_df.to_csv(transfers_out_path, index=False)

    logger.info("Transfermarkt extract complete.")
    return league_df, transfers_in_df, transfers_out_df


if __name__ == "__main__":
    run()
