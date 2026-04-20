import requests

from common.config import get_env
from usda.constants import USDA_FOODS_LIST_URL


def main():
    api_key = get_env("USDA_API_KEY", required=True)

    params = {"api_key": api_key, "pageSize": 1, "pageNumber": 1}
    response = requests.get(USDA_FOODS_LIST_URL, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    print("Top-level type:", type(data).__name__)

    if isinstance(data, list):
        print("Number of records in page:", len(data))
        if data:
            print("Sample record keys:", list(data[0].keys()))
    elif isinstance(data, dict):
        print("Top-level keys:", list(data.keys()))


if __name__ == "__main__":
    main()
