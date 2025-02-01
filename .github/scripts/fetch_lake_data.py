import os
import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime, timedelta
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context

# 🔹 Force TLS 1.2 Compatibility
class TLSAdapter(HTTPAdapter):
    """ 
    Custom adapter that forces TLS 1.2 for requests.
    """
    def init_poolmanager(self, *args, **kwargs):
        context = create_urllib3_context()
        context.set_ciphers("DEFAULT@SECLEVEL=1")  # Lower security level to allow older SSL/TLS
        kwargs["ssl_context"] = context
        super().init_poolmanager(*args, **kwargs)

# 🔹 Create a session and apply the TLS adapter
session = requests.Session()
session.mount("https://", TLSAdapter())

# Define paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Base directory of script
LOG_DIR = os.path.join(BASE_DIR, "..", "logs")  # Log directory (move outside script folder)
DATA_DIR = os.path.join(BASE_DIR, "..", "data")  # Store CSV files outside scripts directory

# Ensure all required directories exist
for directory in [LOG_DIR, DATA_DIR]:
    os.makedirs(directory, exist_ok=True)

# Logging setup
LOG_FILE = os.path.join(LOG_DIR, "fetch_lake_data.log")
logging.basicConfig(
    filename=LOG_FILE, level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
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

# Fetch and parse data from the website using TLS 1.2
def fetch_data(url, key):
    """
    Fetches data from the specified URL using TLS 1.2 compatibility.
    """
    try:
        logging.info(f"🌍 Fetching data from {url}...")

        response = session.get(url, timeout=15)  # 🔹 Use TLS 1.2 session

        response.raise_for_status()

        raw_text = response.text[:500]  # Log first 500 characters
        logging.debug(f"📝 First 500 chars of response ({key}):\n{raw_text}")

        if "JAN" not in raw_text and "FEB" not in raw_text:
            logging.error(f"⚠️ Unexpected content from {url}. Skipping parsing.")
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        return soup.find("pre").text.strip().splitlines()

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error fetching data from {url}: {e}")
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
            headers = HEADERS.get(key, ["Date", "Time", "Data"])
            write_to_csv(file_path, data, headers)
        else:
            logging.warning(f"⚠️ No data fetched for {key}")

if __name__ == "__main__":
    main()
    logging.info("✅ Data fetch completed.")
