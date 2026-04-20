import os
import requests
from dotenv import load_dotenv

load_dotenv()

USDA_API_KEY = os.getenv("USDA_API_KEY")
USDA_FOODS_LIST_URL = "https://api.nal.usda.gov/fdc/v1/foods/list"

def main():
    if not USDA_API_KEY:
        raise ValueError("USDA_API_KEY is missing in .env")

    params = {
        "api_key": USDA_API_KEY,
        "pageSize": 1,
        "pageNumber": 1,
    }

    response = requests.get(USDA_FOODS_LIST_URL, params=params, timeout=60)

    print("Status code:", response.status_code)
    print("Final URL:", response.url)
    print("Response preview:", response.text[:500])

if __name__ == "__main__":
    main()