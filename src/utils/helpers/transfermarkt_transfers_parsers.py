#Paste this in a Jupyter notebook cell to explore the structure of the league table HTML.

from bs4 import BeautifulSoup
import requests

URL = "https://www.transfermarkt.co.uk/championship/transfers/wettbewerb/GB2/saison_id/2024"
html = requests.get(URL, headers={"User-Agent": "mini-championship-pipeline/1.0"}).text
soup = BeautifulSoup(html, "lxml")

# Find the first 'In' table
for container in soup.find_all("div", class_="responsive-table"):
    h2 = container.find_previous("h2")
    club = h2.get_text(" ", strip=True) if h2 else "?"
    table = container.find("table")
    thead = table.find("thead")
    header_texts = [th.get_text(" ", strip=True) for th in thead.find_all("th")]
    if any(h.strip().lower().startswith("in") for h in header_texts):
        print("Club:", club)
        print("Headers:", header_texts)
        first_body_row = table.find("tbody").find("tr")
        print(first_body_row.prettify())
        break
