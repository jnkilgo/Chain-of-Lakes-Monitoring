import os
import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler

# Define paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Go up one level to the repository root
LOG_DIR = os.path.join(BASE_DIR, ".github", "logs")
DATA_DIR = os.path.join(BASE_DIR, ".github", "data")

# Ensure directories exist
for directory in [LOG_DIR, DATA_DIR]:
    os.makedirs(directory, exist_ok=True)

# Configure logging
LOG_FILE = os.path.join(LOG_DIR, "fetch_lake_data.log")
logging.basicConfig(
    handlers=[RotatingFileHandler(LOG_FILE, maxBytes=1024 * 1024, backupCount=5)],
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logging.info("🚀 Starting lake data fetch script.")

# URLs for data
URLS = {
    "lake_data": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/beaver.htm",
    "white_river_data": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/fayettev.htm",
    "war_eagle_data": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/hindsvil.htm",
    "table_rock_data": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/tabrock.htm",
    "kings_river_data": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/berryvil.htm",
    "james_river_data": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/galena.htm",
}

# CSV file paths
CSV_FILES = {
    "lake_data": os.path.join(DATA_DIR, "lake_data.csv"),
    "white_river_data": os.path.join(DATA_DIR, "white_river_data.csv"),
    "war_eagle_data": os.path.join(DATA_DIR, "war_eagle_data.csv"),
    "table_rock_data": os.path.join(DATA_DIR, "table_rock_data.csv"),
    "kings_river_data": os.path.join(DATA_DIR, "kings_river_data.csv"),
    "james_river_data": os.path.join(DATA_DIR, "james_river_data.csv"),
}

# Correct headers for each CSV file
HEADERS = {
    "lake_data": ["Date", "Time CS/CDT", "Elevation (ft-NGVD29)", "Tailwater (ft-NGVD29)",
                  "Generation (mwh)", "Turbine Release (cfs)", "Spillway Release (cfs)", "Total Release (cfs)"],
    "white_river_data": ["Date", "Time CST/CDT", "Stage (feet)", "Flow (cfs)"],
    "war_eagle_data": ["Date", "Time CST/CDT", "Stage (feet)", "Flow (cfs)"],
    "table_rock_data": ["Date", "Time CST/CDT", "Elevation (ft-NGVD29)", "Tailwater (ft-NGVD29)",
                        "Generation (mwh)", "Turbine Release (cfs)", "Spillway Release (cfs)", "Total Release (cfs)"],
    "kings_river_data": ["Date", "Time CST/CDT", "Stage (feet)", "Flow (cfs)"],
    "james_river_data": ["Date", "Time CST/CDT", "Stage (feet)", "Flow (cfs)"],
}

# Normalize timestamps, correcting "2400" to "0000"
def normalize_timestamp(raw_date, raw_time):
    if raw_time == "2400":
        raw_time = "0000"
        date_obj = datetime.strptime(raw_date, "%d%b%Y") + timedelta(days=1)
        raw_date = date_obj.strftime("%d%b%Y")
    return raw_date, raw_time

# Validate if a row is valid
def is_valid_row(row):
    if len(row) < 4:
        logging.debug(f"Row has too few columns: {row}")
        return False
    if any(value.strip() in ["---", "----", "--", "-"] for value in row):
        logging.debug(f"Row contains invalid values: {row}")
        return False
    try:
        datetime.strptime(f"{row[0]} {row[1]}", "%d%b%Y %H%M")
    except ValueError:
        logging.debug(f"Row has invalid date/time format: {row}")
        return False
    return True

# Fetch and parse data from the website
def fetch_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        pre_tag = soup.find("pre")
        if not pre_tag:
            logging.error(f"❌ No <pre> tag found in {url}.")
            return []

        rows = pre_tag.text.strip().splitlines()
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
                except (IndexError, ValueError) as e:
                    logging.warning(f"Skipped malformed row: {row}. Error: {e}")
                    continue

        logging.debug(f"Parsed data from {url}: {parsed_data}")
        return parsed_data
    except Exception as e:
        logging.error(f"Error fetching data from {url}: {e}")
        return []

# Sort rows chronologically
def sort_rows(data):
    try:
        return sorted(data, key=lambda x: datetime.strptime(f"{x[0]} {x[1]}", "%d%b%Y %H%M"))
    except Exception as e:
        logging.error(f"Error sorting rows: {e}")
        return data

# Remove duplicates and limit to 5 days
def clean_and_limit_data(data, cutoff_date):
    unique_data = {}
    for row in data:
        if len(row) < 2:
            logging.debug(f"Skipping row with insufficient columns: {row}")
            continue
        timestamp = f"{row[0]} {row[1]}"
        try:
            row_date = datetime.strptime(row[0], "%d%b%Y")
            if row_date >= cutoff_date:
                unique_data[timestamp] = row
            else:
                logging.debug(f"Skipping row older than cutoff date: {row}")
        except ValueError as e:
            logging.debug(f"Skipping row with invalid date format: {row}. Error: {e}")
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

# Main script function
def main():
    for key, url in URLS.items():
        data = fetch_data(url)
        file_path = CSV_FILES[key]
        headers = HEADERS[key]
        write_to_csv(file_path, data, headers)

if __name__ == "__main__":
    main()
