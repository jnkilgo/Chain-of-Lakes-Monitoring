import requests
import logging

# Logging setup
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

# Test URLs
TEST_HTTP_URL = "http://httpforever.com"  # A site that does NOT use HTTPS
USACE_URL = "https://www.swl-wc.usace.army.mil/pages/data/tabular/htm/fayettev.htm"  # Target URL

# Function to test HTTP request
def test_http():
    try:
        response = requests.get(TEST_HTTP_URL, timeout=10)
        response.raise_for_status()
        logging.info(f"✅ HTTP TEST SUCCESS - Response Length: {len(response.text)}")
    except Exception as e:
        logging.error(f"❌ HTTP TEST FAILED: {e}")

# Function to test HTTPS request with SSL disabled
def test_https():
    try:
        response = requests.get(USACE_URL, timeout=10, verify=False)  # Disable SSL verification
        response.raise_for_status()
        logging.info(f"✅ HTTPS TEST SUCCESS - Response Length: {len(response.text)}")
    except Exception as e:
        logging.error(f"❌ HTTPS TEST FAILED: {e}")

if __name__ == "__main__":
    logging.info("🚀 Starting HTTP/HTTPS tests")
    
    logging.info("🌐 Testing HTTP Request...")
    test_http()  # Test HTTP request
    
    logging.info("🌍 Testing HTTPS Request with SSL Disabled...")
    test_https()  # Test HTTPS request

    logging.info("✅ Debugging complete.")
