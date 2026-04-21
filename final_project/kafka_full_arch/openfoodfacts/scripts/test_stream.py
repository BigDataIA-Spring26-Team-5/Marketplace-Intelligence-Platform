from datasets import load_dataset
from openfoodfacts.constants import OPENFOODFACTS_DATASET_NAME, OPENFOODFACTS_SPLIT


def main():
    dataset = load_dataset(
        OPENFOODFACTS_DATASET_NAME,
        split=OPENFOODFACTS_SPLIT,
        streaming=True,
    )

    for idx, record in enumerate(dataset, start=1):
        print(f"Record {idx}:")
        print(record)
        if idx >= 3:
            break


if __name__ == "__main__":
    main()