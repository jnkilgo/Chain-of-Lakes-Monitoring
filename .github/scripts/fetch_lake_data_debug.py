import ssl
import requests
import logging

# Logging setup
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

# USACE Website
USACE_URL = "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/beaver.htm"

# Function to check OpenSSL version
def check_openssl_version():
    try:
        openssl_version = ssl.OPENSSL_VERSION
        logging.info(f"✅ OpenSSL Version: {openssl_version}")
    except Exception as e:
        logging.error(f"❌ Failed to check OpenSSL version: {e}")

# Function to check SSL/TLS handshake details
def check_ssl_handshake():
    try:
        logging.info(f"🔍 Checking SSL/TLS connection to {USACE_URL}...")
        context = ssl.create_default_context()
        with context.wrap_socket(ssl.SSLSocket(), server_hostname="www.swl-wc.usace.army.mil") as sock:
            sock.connect(("www.swl-wc.usace.army.mil", 443))
            logging.info(f"✅ Successfully connected using {sock.version()}")
    except Exception as e:
        logging.error(f"❌ SSL Handshake Failed: {e}")

# Function to attempt a GET request to the USACE URL
def test_https_request():
    try:
        response = requests.get(USACE_URL, timeout=10)
        response.raise_for_status()
        logging.info(f"✅ HTTPS Request Success - Status Code: {response.status_code}")
    except requests.exceptions.SSLError as e:
        logging.error(f"❌ SSL ERROR: {e}")
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ REQUEST ERROR: {e}")

if __name__ == "__main__":
    logging.info("🚀 Running SSL Debugging Tests")
    
    check_openssl_version()
    check_ssl_handshake()
    test_https_request()

    logging.info("✅ SSL Debugging Completed.")
