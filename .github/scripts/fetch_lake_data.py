import os
import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler
import time

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
    "lake_data": ["Date", "Time", "Elevation (ft)", "Tailwater (ft)", "Generation (mwh)", "Turbine Release (cfs)", "Spillway Release (cfs)", "Total Release (cfs)"],
    "white_river_data": ["Date", "Time", "Stage (ft)", "Flow (cfs)"],
    "war_eagle_data": ["Date", "Time", "Stage (ft)", "Flow (cfs)"],
    "table_rock_data": ["Date", "Time", "Elevation (ft)", "Tailwater (ft)", "Generation (mwh)", "Turbine Release (cfs)", "Spillway Release (cfs)", "Total Release (cfs)"],
    "kings_river_data": ["Date", "Time", "Stage (ft)", "Flow (cfs)"],
    "james_river_data": ["Date", "Time", "Stage (ft)", "Flow (cfs)"],
}

# Request Headers to mimic a browser
HEADERS_REQUEST = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.swl-wc.usace.army.mil/",
    "Connection": "keep-alive",
}

# Fetch and parse data
def fetch_data(url, key):
    try:
        logging.info(f"🌍 Fetching data from {url}")

        response = requests.get(url, headers=HEADERS_REQUEST, timeout=15, verify=False)
        response.raise_for_status()

        # Decode explicitly as UTF-8
        response.encoding = 'utf-8'

        # Log first 500 characters for debugging
        raw_text = response.text[:500]
        logging.debug(f"📝 First 500 chars of response ({key}):\n{raw_text}")

        # Parse HTML explicitly with 'html.parser'
        soup = BeautifulSoup(response.text, "html.parser")

        # Find <pre> tag where the data is located
        pre_tag = soup.find("pre")

        if not pre_tag:
            logging.error(f"❌ No <pre> tag found in {url}. Saving raw response for debugging.")
            with open(f"{DATA_DIR}/{key}_raw.html", "w", encoding="utf-8") as f:
                f.write(response.text)
            return []

        rows = pre_tag.text.strip().splitlines()
        if not rows:
            logging.warning(f"⚠️ No data found in response from {url}.")
            return []

        logging.info(f"✅ Successfully fetched data from {url}")
        return rows

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error fetching {url}: {e}")
        return []

# Write data to CSV
def write_to_csv(file_path, data, headers):
    try:
        if not data:
            logging.warning(f"⚠️ No data to write for {file_path}")
            return

        with open(file_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            writer.writerows(data)

        logging.info(f"✅ {len(data)} rows written to {file_path}")
    except Exception as e:
        logging.error(f"❌ Error writing to {file_path}: {e}")

# Main script function
def main():
    logging.info("🚀 Fetching data for all sources.")
    for key, url in URLS.items():
        data = fetch_data(url, key)
        if data:
            write_to_csv(CSV_FILES[key], data, HEADERS[key])
        else:
            logging.warning(f"⚠️ No data fetched for {key}")

if __name__ == "__main__":
    main()
    logging.info("✅ Data fetch completed.")
