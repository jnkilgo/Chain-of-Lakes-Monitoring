import os
import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler

# Define paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
DATA_DIR = os.path.join(BASE_DIR, "data")

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
CSV_FILES = {key: os.path.join(DATA_DIR, f"{key}.csv") for key in URLS.keys()}

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
        return False
    if any(value.strip() in ["---", "----", "--", "-"] for value in row):
        return False
    try:
        datetime.strptime(f"{row[0]} {row[1]}", "%d%b%Y %H%M")
    except ValueError:
        return False
    return True

# Fetch and parse data from the website
def fetch_data(url, key):
    """
    Fetches data from the URL and logs response details.
    """
    try:
        logging.info(f"🌍 Fetching data from {url}")

        response = requests.get(url, timeout=15, verify=False)  # Disable SSL verification
        logging.info(f"🔍 Response Status Code: {response.status_code}")
        
        if response.status_code == 403:
            logging.error(f"⛔ ACCESS BLOCKED! Server returned 403 Forbidden for {url}")
            return []

        response.raise_for_status()

        # Log first 500 characters for debugging
        raw_text = response.text[:500]
        logging.debug(f"📝 First 500 chars of response ({key}):\n{raw_text}")

        # Check if response contains expected data
        if "JAN" not in raw_text and "FEB" not in raw_text:
            logging.warning(f"⚠️ Unexpected response content from {url}. Possible CAPTCHA or site block.")
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        pre_tag = soup.find("pre")
        if not pre_tag:
            logging.error(f"❌ No <pre> tag found in {url}. Data format may have changed.")
            return []

        return pre_tag.text.strip().splitlines()

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error fetching {url}: {e}")
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

# Main script function
def main():
    logging.info("🚀 Fetching data for all sources.")
    for key, url in URLS.items():
        data = fetch_data(url, key)
        if data:
            file_path = CSV_FILES[key]
            headers = HEADERS[key]
            write_to_csv(file_path, data, headers)
        else:
            logging.warning(f"⚠️ No data fetched for {key}")

if __name__ == "__main__":
    main()
    logging.info("✅ Data fetch completed.")
