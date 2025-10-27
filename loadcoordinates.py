from __future__ import annotations
import csv
from pathlib import Path
from typing import Set, Tuple

INPUT_PATH = Path("./data/sk_zip_coordinates.txt")
OUTPUT_PATH = Path("./data/sk_zip_coordinates_clean.csv")


def normalize_zip(raw_zip: str) -> str:
    return raw_zip.replace(" ", "").strip()


def extract_coordinates(line: str) -> Tuple[str, float, float] | None:
    parts = line.strip().split("\t")
    if len(parts) < 3:
        return None
    try:
        zip_code = normalize_zip(parts[1])
        latitude = float(parts[-3])
        longitude = float(parts[-2])
    except (ValueError, IndexError):
        return None
    if not zip_code or len(zip_code) < 3:
        return None
    return zip_code, longitude, latitude


def main() -> None:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_PATH}")
    unique_rows: Set[Tuple[str, float, float]] = set()
    with INPUT_PATH.open(encoding="utf-8") as src:
        for line in src:
            if not line.strip():
                continue
            record = extract_coordinates(line)
            if record is None:
                continue
            unique_rows.add(record)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as dst:
        writer = csv.writer(dst)
        writer.writerow(["zip_code", "longitude", "latitude"])
        for row in sorted(unique_rows):
            writer.writerow(row)


if __name__ == "__main__":
    main()