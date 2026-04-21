from datasets import load_dataset

from openfoodfacts.constants import OFF_CONFIG, OFF_DATASET, OFF_SPLIT


def main():
    print(
        f"Loading HuggingFace dataset {OFF_DATASET} "
        f"(config={OFF_CONFIG}, split={OFF_SPLIT}) in streaming mode..."
    )
    ds = load_dataset(
        OFF_DATASET,
        name=OFF_CONFIG,
        split=OFF_SPLIT,
        streaming=True,
    )

    first = next(iter(ds))
    print("Sample record type:", type(first).__name__)
    print("Sample record keys count:", len(first))
    print("Sample record keys:", list(first.keys()))
    print()
    print("Preview (first 15 fields, values truncated):")
    for field, value in list(first.items())[:15]:
        preview = str(value)
        if len(preview) > 80:
            preview = preview[:77] + "..."
        print(f"  {field}: {preview}")


if __name__ == "__main__":
    main()
