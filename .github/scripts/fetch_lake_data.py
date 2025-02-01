import requests
from bs4 import BeautifulSoup
import csv
import os
import logging
from datetime import datetime, timedelta
import time

# Set log file path in a writable directory for GitHub Actions
LOG_FILE = "/tmp/fetch_lake_data.log"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# Configure logging
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

# URLs for data
URLS = {
    "beaver_lake": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/beaver.htm",
    "white_river": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/fayettev.htm",
    "war_eagle": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/hindsvil.htm",
    "kings_river": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/berryvil.htm",
    "james_river": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/galena.htm",
    "table_rock": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/tabrock.htm",
    "bull_shoals": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/bulsdam.htm",
    "dam_data": "https://www.swl-wc.usace.army.mil/WM_Reports/current_conditions.html"
}

# CSV file paths (in the repo for GitHub Actions)
CSV_DIR = "./data/"
CSV_FILES = {
    "beaver_lake": CSV_DIR + "beaver_lake.csv",
    "white_river": CSV_DIR + "white_river.csv",
    "war_eagle": CSV_DIR + "war_eagle.csv",
    "kings_river": CSV_DIR + "kings_river.csv",
    "james_river": CSV_DIR + "james_river.csv",
    "table_rock": CSV_DIR + "table_rock.csv",
    "bull_shoals": CSV_DIR + "bull_shoals.csv",
    "dam_data": CSV_DIR + "dam_data.csv"
}

# Ensure CSV directory exists
os.makedirs(CSV_DIR, exist_ok=True)

# Headers for each CSV file
HEADERS = {
    "beaver_lake": ["Date", "Time CST/CDT", "Elevation (ft)", "Tailwater (ft)", "Generation (mwh)", "Turbine Release (cfs)", "Spillway Release (cfs)", "Total Release (cfs)"],
    "white_river": ["Date", "Time CST/CDT", "Stage (feet)", "Flow (cfs)"],
    "war_eagle": ["Date", "Time CST/CDT", "Stage (feet)", "Flow (cfs)"],
    "kings_river": ["Date", "Time CST/CDT", "Stage (feet)", "Flow (cfs)"],
    "james_river": ["Date", "Time CST/CDT", "Stage (feet)", "Flow (cfs)"],
    "table_rock": ["Date", "Time CST/CDT", "Elevation (ft)", "Tailwater (ft)", "Generation (mwh)", "Turbine Release (cfs)", "Spillway Release (cfs)", "Total Release (cfs)"],
    "bull_shoals": ["Date", "Time CST/CDT", "Elevation (ft)", "Tailwater (ft)", "Generation (mwh)", "Turbine Release (cfs)", "Spillway Release (cfs)", "Total Release (cfs)"],
    "dam_data": ["Dam", "Pool Elevation", "1hr Change", "24hr Change", "Total Outflow", "Top of Cons Pool", "Top of Flood Pool", "Percent Storage", "Feet from Cons Full"]
}

# Normalize timestamps (handles "2400" as "0000" on the next day)
def normalize_timestamp(raw_date, raw_time):
    if raw_time == "2400":
        raw_time = "0000"
        date_obj = datetime.strptime(raw_date, "%d%b%Y") + timedelta(days=1)
        raw_date = date_obj.strftime("%d%b%Y")
    return raw_date, raw_time

# Validate if a row is valid
def is_valid_row(row):
    if len(row) < 3:
        return False
    if any(value.strip() in ["---", "----", "--", "-"] for value in row):
        return False
    try:
        datetime.strptime(f"{row[0]} {row[1]}", "%d%b%Y %H%M")
    except ValueError:
        return False
    return True

# Fetch and parse data from the website with retry logic
def fetch_data(url, retries=3, delay=5):
    for attempt in range(retries):
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
                        if not is_valid_row(full_row):
                            logging.warning(f"Skipped invalid or malformed row: {full_row}")
                            continue
                        parsed_data.append(full_row)
                    except (IndexError, ValueError):
                        logging.warning(f"Skipped malformed row: {row}")
                        continue
            return parsed_data
        except requests.RequestException as e:
            logging.error(f"Attempt {attempt+1}: Error fetching data from {url}: {e}")
            time.sleep(delay)
    return []

# Sort rows chronologically
def sort_rows(data):
    try:
        return sorted(data, key=lambda x: datetime.strptime(f"{x[0]} {x[1]}", "%d%b%Y %H%M"))
    except Exception as e:
        logging.error(f"Error sorting rows: {e}")
        return data

# Remove duplicates and limit to last 5 days
def clean_and_limit_data(data, cutoff_date):
    unique_data = {}
    for row in data:
        if len(row) < 2:
            continue
        timestamp = f"{row[0]} {row[1]}"
        if datetime.strptime(row[0], "%d%b%Y") >= cutoff_date:
            unique_data[timestamp] = row
    return sort_rows(unique_data.values())

# Write data to CSV
def write_to_csv(file_path, data, headers):
    try:
        cutoff_date = datetime.now() - timedelta(days=5)
        cleaned_data = clean_and_limit_data(data, cutoff_date)

        with open(file_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            writer.writerows(cleaned_data)

        logging.info(f"{len(cleaned_data)} rows written to {file_path}.")
    except Exception as e:
        logging.error(f"Error writing to {file_path}: {e}")

# Main function
def main():
    for key, url in URLS.items():
        data = fetch_data(url)
        if data:
            write_to_csv(CSV_FILES[key], data, HEADERS[key])

if __name__ == "__main__":
    main()
