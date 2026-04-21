from common.file_utils import read_json_file
from fda.constants import RAW_JSON_PATH


def main():
    data = read_json_file(RAW_JSON_PATH)

    print("Top-level type:", type(data).__name__)

    if isinstance(data, dict):
        print("Top-level keys:", list(data.keys()))

        if "results" in data:
            results = data["results"]
            print("Results type:", type(results).__name__)
            print("Number of records in results:", len(results))

            if results:
                print("Sample record keys:", list(results[0].keys()))
        else:
            print("No 'results' key found.")

    elif isinstance(data, list):
        print("Top-level list length:", len(data))
        if data:
            print("Sample record keys:", list(data[0].keys()))


if __name__ == "__main__":
    main()