import os
import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime, timedelta
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context

# --------------------
# CONFIGURATION SETUP
# --------------------

# Define paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Base script directory
LOG_DIR = os.path.join(BASE_DIR, "..", "logs")  # Log storage
DATA_DIR = os.path.join(BASE_DIR, "..", "data")  # CSV storage

# Ensure directories exist
for directory in [LOG_DIR, DATA_DIR]:
    os.makedirs(directory, exist_ok=True)

# Logging setup
LOG_FILE = os.path.join(LOG_DIR, "fetch_lake_data.log")
logging.basicConfig(
    filename=LOG_FILE, level=logging.DEBUG, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("🔵 Starting data fetch script.")

# --------------------
# HTTPS SESSION WITH TLS 1.2+
# --------------------

class TLSAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        context = create_urllib3_context()
        context.set_ciphers("DEFAULT@SECLEVEL=1")
        kwargs["ssl_context"] = context
        super().init_poolmanager(*args, **kwargs)

session = requests.Session()
session.mount("https://", TLSAdapter())

# --------------------
# DATA SOURCES
# --------------------

URLS = {
    "white_river": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/fayettev.htm",
    "war_eagle": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/hindsvil.htm",
    "beaver_lake": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/beaver.htm",
}

# CSV file paths
CSV_FILES = {key: os.path.join(DATA_DIR, f"{key}_data.csv") for key in URLS.keys()}

# Headers for CSVs
HEADERS = {
    "beaver_lake": ["Date", "Time", "Elevation", "Turbine Release", "Spillway Release", "Total Release"],
    "white_river": ["Date", "Time", "Stage", "Flow"],
    "war_eagle": ["Date", "Time", "Stage", "Flow"],
}

# --------------------
# FETCH DATA FUNCTION
# --------------------

def fetch_data(url, key):
    """
    Fetches data from the specified URL and returns parsed lines.
    Implements error handling for SSL issues and connection problems.
    """
    try:
        logging.info(f"🌍 Fetching data from {url}...")
        response = session.get(url, timeout=15)
        response.raise_for_status()

        # Print raw response for debugging if needed
        raw_text = response.text[:500]  # Show first 500 characters
        logging.debug(f"📝 First 500 chars of response ({key}):\n{raw_text}")

        # Ensure content is valid before parsing
        if "JAN" not in raw_text and "FEB" not in raw_text:
            logging.error(f"⚠️ Unexpected content from {url}. Skipping parsing.")
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        return soup.find("pre").text.strip().splitlines()
    
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error fetching data from {url}: {e}")
        return []

# --------------------
# PROCESS AND WRITE TO CSV
# --------------------

def write_to_csv(file_path, data, headers, key):
    """
    Writes data to a CSV file with the provided headers.
    """
    try:
        if not data:
            logging.warning(f"⚠️ No data to write for {key}. Skipping file update.")
            return
        
        with open(file_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            writer.writerows(data)
        
        logging.info(f"✅ {len(data)} rows written to {file_path}")
    
    except Exception as e:
        logging.error(f"❌ Error writing to {file_path}: {e}")

# --------------------
# MAIN SCRIPT EXECUTION
# --------------------

def main():
    """
    Main function that fetches data, processes it, and saves it.
    """
    logging.info("🚀 Fetching data for all sources.")
    
    for key, url in URLS.items():
        raw_data = fetch_data(url, key)

        if raw_data:
            write_to_csv(CSV_FILES[key], raw_data, HEADERS[key], key)
        else:
            logging.warning(f"⚠️ No data fetched for {key}.")

    logging.info("🎯 Data fetch process completed.")

if __name__ == "__main__":
    main()
