import requests
from bs4 import BeautifulSoup
import json
import re
from pathlib import Path

IMDB_REGEX = re.compile(r"https?://(?:www\.)?imdb\.com/title/(tt\d+)", re.IGNORECASE)

def scrape_adp_page(page: int, session: requests.Session = None) ->dict:
    if session is None:
        session = requests.Session()
    URL = f"https://adp.acb.org/adp-search?page={page}&order=field_release_year&sort=desc"
    print(f"Scraping {URL}")
    response = session.get(URL)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    table_container = soup.find("div", class_="table-responsive")
    if not table_container:
        raise RuntimeError("Couldn't find <div class='table-responsive'>")
    table = table_container.find("table")
    if not table:
        raise RuntimeError("Couldn't find <table> inside the div")

    headers = [th.get_text(strip=True) for th in table.find("thead").find_all("th")]
    value_keys = ["Title", "Release Year", "Media Type", "Rating", "Genre", "Providers"]
    imdb_dict = {}

    for tr in table.find("tbody").find_all("tr"):
        cells = tr.find_all("td")
        row_data = {}
        imdb_id = None

        for i, cell in enumerate(cells):
            header = headers[i]
            text = cell.get_text(separator=", ", strip=True)
            links = cell.find_all("a", href=True)

            if header == "IMDb" and links:
                for a in links:
                    match = IMDB_REGEX.match(a["href"])
                    if match:
                        imdb_id = match.group(1)
                        break
            elif header in value_keys:
                if header == "Providers":
                    providers = []
                    for a in links:
                        providers.append({a.get_text(strip=True): a["href"]})
                    row_data[header] = providers
                    row_data["in_theaters"] = "Cinema" in text
                else:
                    row_data[header] = text

        if imdb_id:
            imdb_dict[imdb_id] = row_data

    return imdb_dict

def save_data(data: dict):
    try:
        with open("adp.json", "r") as f:
            old = json.load(f)
    except FileNotFoundError:
        old = {}
    old.update(data)

    output_dir = Path("adp_database")
    output_dir.mkdir(parents=True, exist_ok=True)
    for imdbid, info in old.items():
        with (output_dir / f"{imdbid}.json").open('w', encoding='utf-8') as f:
            json.dump(info, f, ensure_ascii=False, indent=2)
    with open("adp.json", "w", encoding="utf-8") as f:
        json.dump(old, f, indent=2, ensure_ascii=False)

def main():
    data = {}
    page_number = 1
    http_errors = 0

    with requests.Session() as session:
        while True:
            try:
                data = data | scrape_adp_page(page_number, session)
            except requests.exceptions.HTTPError:
                http_errors += 1
                if http_errors > 3:
                    print("Encountered too many successive HTTP errors, skipping...")
                    continue
            except RuntimeError:
                print("Reached end of database")
                break
            page_number += 1
            http_errors = 0
            break

    save_data(data)
    print(f"Found {len(data)} titles")