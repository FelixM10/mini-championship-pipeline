#Paste this in a Jupyter notebook cell to explore the structure of the league table HTML.

import requests
from bs4 import BeautifulSoup

url = "https://www.transfermarkt.co.uk/championship/tabelle/wettbewerb/GB2/saison_id/2024"
headers = {"User-Agent": "Mozilla/5.0 (compatible; mini-pipeline/1.0)"}

html = requests.get(url, headers=headers).text
soup = BeautifulSoup(html, "lxml")

# --- find the main table ---
holder = soup.find("div", id="yw1")
table = holder.find("table")

print("=== THEAD STRUCTURE ===")
thead = table.find("thead")
print(thead.prettify())

# --- find the first real row ---
tbody = table.find("tbody")
first_row = tbody.find("tr")

print("\n=== FIRST ROW (full HTML) ===")
print(first_row.prettify())

# --- list each <td>'s raw text value ---
print("\n=== ORDERED <td> TEXT CONTENTS ===")
tds = first_row.find_all("td")
for i, td in enumerate(tds):
    print(f"[{i}] => {td.get_text(' ', strip=True)}")

# --- list each <td>'s class structure ---
print("\n=== ORDERED <td> CLASSES ===")
for i, td in enumerate(tds):
    print(f"[{i}] => {td.get('class')}")
