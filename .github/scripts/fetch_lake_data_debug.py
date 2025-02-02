import ssl
import requests
import logging

# Logging setup
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

# USACE Website
USACE_URL = "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/beaver.htm"

# Spoofed browser headers (to make the request look normal)
HEADERS_REQUEST = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.swl-wc.usace.army.mil/",
    "Connection": "keep-alive",
}

# Function to check OpenSSL version
def check_openssl_version():
    try:
        openssl_version = ssl.OPENSSL_VERSION
        logging.info(f"✅ OpenSSL Version: {openssl_version}")
    except Exception as e:
        logging.error(f"❌ Failed to check OpenSSL version: {e}")

# Function to check SSL/TLS handshake using TLSv1.2
def check_ssl_handshake():
    try:
        logging.info(f"🔍 Checking SSL/TLS connection to {USACE_URL} using TLSv1.2...")
        context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)  # Force TLSv1.2
        with context.wrap_socket(socket.socket(), server_hostname="www.swl-wc.usace.army.mil") as sock:
            sock.connect(("www.swl-wc.usace.army.mil", 443))
            logging.info(f"✅ Successfully connected using {sock.version()}")
    except Exception as e:
        logging.error(f"❌ SSL Handshake Failed: {e}")

# Function to attempt a GET request to the USACE URL
def test_https_request():
    try:
        logging.info(f"🌍 Sending HTTPS request to {USACE_URL} with TLSv1.2 and spoofed headers...")
        session = requests.Session()
        session.mount("https://", requests.adapters.HTTPAdapter())  # Enforce HTTPS
        response = session.get(USACE_URL, headers=HEADERS_REQUEST, timeout=15, verify=True)  # Enforce headers
        response.raise_for_status()
        logging.info(f"✅ HTTPS Request Success - Status Code: {response.status_code}")
        logging.debug(f"📝 Response Preview:\n{response.text[:500]}")
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
