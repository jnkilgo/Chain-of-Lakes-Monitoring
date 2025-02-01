import os
import requests
from bs4 import BeautifulSoup
import csv
import time
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler

# Define paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "..", "logs")
DATA_DIR = os.path.join(BASE_DIR, "..", "data")

# Ensure directories exist
for directory in [LOG_DIR, DATA_DIR]:
    os.makedirs(directory, exist_ok=True)

# Logging setup
LOG_FILE = os.path.join(LOG_DIR, "fetch_lake_data.log")
logging.basicConfig(
    handlers=[RotatingFileHandler(LOG_FILE, maxBytes=1024 * 1024, backupCount=5)],
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logging.info("🚀 Starting lake data fetch script.")

# URLs for data
URLS = {
    "white_river": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/fayettev.htm",
    "war_eagle": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/hindsvil.htm",
    "beaver_lake": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/beaver.htm",
    "kings_river": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/berryvil.htm",
    "james_river": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/galena.htm",
    "table_rock": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/tabrock.htm",
    "bull_shoals": "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/bulsdam.htm",
}

# CSV file paths
CSV_FILES = {key: os.path.join(DATA_DIR, f"{key}_data.csv") for key in URLS.keys()}

# Correct headers for each CSV file
HEADERS = {
    "beaver_lake": ["Date", "Time", "Elevation", "Tailwater", "Generation", "Turbine Release", "Spillway Release", "Total Release"],
    "table_rock": ["Date", "Time", "Elevation", "Tailwater", "Generation", "Turbine Release", "Spillway Release", "Total Release"],
    "bull_shoals": ["Date", "Time", "Elevation", "Tailwater", "Generation", "Turbine Release", "Spillway Release", "Total Release"],
    "white_river": ["Date", "Time", "Stage", "Flow"],
    "war_eagle": ["Date", "Time", "Stage", "Flow"],
    "kings_river": ["Date", "Time", "Stage", "Flow"],
    "james_river": ["Date", "Time", "Stage", "Flow"],
}

# Spoofed headers
HEADERS_REQUEST = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.swl-wc.usace.army.mil/",
    "Connection": "keep-alive",
}

# Normalize timestamps, correcting "2400" to "0000"
def normalize_timestamp(raw_date, raw_time):
    try:
        if raw_time == "2400":
            raw_time = "0000"
            date_obj = datetime.strptime(raw_date, "%d%b%Y") + timedelta(days=1)
            raw_date = date_obj.strftime("%d%b%Y")
        return raw_date, raw_time
    except ValueError as e:
        logging.error(f"❌ Error normalizing timestamp: {e}")
        return None, None

# Fetch and parse data from the website
def fetch_data(url, key):
    attempt = 0
    while attempt < 5:
        try:
            logging.info(f"🌍 Fetching data from {url} (Attempt {attempt + 1})")
            
            # 🔹 Spoof request headers
            response = requests.get(url, headers=HEADERS_REQUEST, timeout=15, verify=False)

            response.raise_for_status()

            # Log first 500 characters for debugging
            raw_text = response.text[:500]
            logging.debug(f"📝 First 500 chars of response ({key}):\n{raw_text}")

            soup = BeautifulSoup(response.text, "html.parser")
            pre_tag = soup.find("pre")
            
            if not pre_tag:
                logging.error(f"❌ No <pre> tag found in {url}. First 500 chars:\n{raw_text}")
                return []
                
            return pre_tag.text.strip().splitlines()

        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Error fetching {url}: {e}. Retrying in 10 seconds...")
            attempt += 1
            time.sleep(10)  # 🔹 Fixed 10s wait (no exponential backoff)

    logging.error(f"⛔ Failed to fetch data from {url} after {attempt} attempts.")
    return []

# Write data to CSV
def write_to_csv(file_path, data, headers):
    try:
        cutoff_date = datetime.now() - timedelta(days=5)
        unique_data = {}
        for row in data:
            timestamp = f"{row[0]} {row[1]}"
            if datetime.strptime(row[0], "%d%b%Y") >= cutoff_date:
                unique_data[timestamp] = row

        sorted_data = sorted(unique_data.values(), key=lambda x: datetime.strptime(f"{x[0]} {x[1]}", "%d%b%Y %H%M"))

        with open(file_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            writer.writerows(sorted_data)

        logging.info(f"✅ {len(sorted_data)} rows written to {file_path}.")

    except Exception as e:
        logging.error(f"❌ Error writing to {file_path}: {e}")

# Main script function
def main():
    logging.info("🚀 Fetching data for all sources.")
    for key, url in URLS.items():
        data = fetch_data(url, key)
        if data:
            write_to_csv(CSV_FILES[key], data, HEADERS.get(key, ["Date", "Time", "Data"]))
        else:
            logging.warning(f"⚠️ No data fetched for {key}")

if __name__ == "__main__":
    main()
    logging.info("✅ Data fetch completed.")
