import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime, timedelta
import logging
import os

# Configure logging
LOG_FILE = "scripts/fetch_lake_data.log"
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG)

# URLs for data
URLS = {
    "beaver_lake": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/beaver.htm",
    "white_river": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/fayettev.htm",
    "war_eagle": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/hindsvil.htm",
    "kings_river": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/berryvil.htm",
    "james_river": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/galena.htm",
    "table_rock_lake": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/tabrock.htm",
    "turkey_creek": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/hollister.htm",
    "bull_creek": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/bullcrk.htm",
    "bear_creek": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/omaha.htm",
    "little_north_fork": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/thornf.htm",
    "beaver_creek": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/bradleyv.htm",
    "bull_shoals": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/bulsdam.htm",
    "dam_data": "https://www.swl-wc.usace.army.mil/WM_Reports/current_conditions.html",
}

# CSV file paths
CSV_DIR = "data/"
CSV_FILES = {key: os.path.join(CSV_DIR, f"{key}.csv") for key in URLS.keys()}

# Headers for CSV files
HEADERS = {
    "beaver_lake": ["Date", "Time", "Elevation", "Tailwater", "Generation", "Turbine Release", "Spillway Release", "Total Release"],
    "white_river": ["Date", "Time", "Stage", "Flow"],
    "war_eagle": ["Date", "Time", "Stage", "Flow"],
    "kings_river": ["Date", "Time", "Stage", "Flow"],
    "james_river": ["Date", "Time", "Stage", "Flow"],
    "table_rock_lake": ["Date", "Time", "Elevation", "Tailwater", "Generation", "Turbine Release", "Spillway Release", "Total Release"],
    "turkey_creek": ["Date", "Time", "Stage", "Flow"],
    "bull_creek": ["Date", "Time", "Stage", "Flow"],
    "bear_creek": ["Date", "Time", "Stage", "Flow"],
    "little_north_fork": ["Date", "Time", "Stage", "Flow"],
    "beaver_creek": ["Date", "Time", "Stage", "Flow"],
    "bull_shoals": ["Date", "Time", "Elevation", "Tailwater", "Generation", "Turbine Release", "Spillway Release", "Total Release"],
    "dam_data": ["Dam", "Pool Elevation", "1hr Change", "24hr Change", "Total Outflow", "Top of Conservation Pool", "Top of Flood Pool", "Percent Storage", "Percent Full"],
}

# Ensure data directory exists
os.makedirs(CSV_DIR, exist_ok=True)

# Normalize timestamps, correcting "2400" to "0000"
def normalize_timestamp(raw_date, raw_time):
    if raw_time == "2400":
        raw_time = "0000"
        date_obj = datetime.strptime(raw_date, "%d%b%Y") + timedelta(days=1)
        raw_date = date_obj.strftime("%d%b%Y")
    return raw_date, raw_time

# Validate if a row is valid
def is_valid_row(row):
    if len(row) < 2 or any(val in ["---", "--", "-"] for val in row):
        return False
    try:
        datetime.strptime(f"{row[0]} {row[1]}", "%d%b%Y %H%M")
        return True
    except ValueError:
        return False

# Fetch and parse data
def fetch_data(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        rows = soup.find_all("pre")[0].text.strip().splitlines()
        parsed_data = []

        start_parsing = False
        for row in rows:
            row = row.strip()
            if not start_parsing and any(char.isdigit() for char in row) and "JAN" in row:
                start_parsing = True
            if start_parsing:
                columns = row.split()
                if "7-Day" in row or "Plot" in row:
                    break
                try:
                    raw_date, raw_time = normalize_timestamp(columns[0], columns[1])
                    other_columns = columns[2:]
                    full_row = [raw_date, raw_time] + other_columns
                    if is_valid_row(full_row):
                        parsed_data.append(full_row)
                except (IndexError, ValueError):
                    logging.warning(f"Skipped malformed row: {row}")
        return parsed_data
    except Exception as e:
        logging.error(f"Error fetching data from {url}: {e}")
        return []

# Write data to CSV
def write_to_csv(file_path, data, headers):
    try:
        with open(file_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            writer.writerows(data)
        logging.info(f"{len(data)} rows written to {file_path}.")
    except Exception as e:
        logging.error(f"Error writing to {file_path}: {e}")

# Fetch and parse dam data separately
def fetch_dam_data():
    try:
        response = requests.get(URLS["dam_data"], timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        rows = soup.find_all("tr")

        parsed_data = []
        for row in rows[1:]:  # Skip header row
            cols = [col.text.strip() for col in row.find_all("td")]
            if len(cols) >= 8:
                parsed_data.append(cols)

        return parsed_data
    except Exception as e:
        logging.error(f"Error fetching dam data: {e}")
        return []

# Main function
def main():
    for key, url in URLS.items():
        if key == "dam_data":
            data = fetch_dam_data()
        else:
            data = fetch_data(url)
        write_to_csv(CSV_FILES[key], data, HEADERS[key])

if __name__ == "__main__":
    main()
