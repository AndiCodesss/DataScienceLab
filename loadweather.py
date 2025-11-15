#!/usr/bin/env python3
"""
Builds hourly (and optional 15-minute) weather data for each given ZIP code in Slovakia for 2016.

Input:
    A CSV file with columns: zip_code, longitude, latitude

Output:
    sk_weather_hourly_2016_by_zip.csv
    sk_weather_15min_2016_by_zip.csv (optional)

Dependencies:
    pip install meteostat pandas tqdm

Notes:
    - Data source: Meteostat (https://dev.meteostat.net/, CC BY-NC 4.0)
    - For research / university (non-commercial) use only.
"""

import os
import time
from datetime import datetime

import pandas as pd
from tqdm import tqdm
from meteostat import Hourly, Point

INPUT_ZIP_FILE = "./data/sk_zip_coordinates_clean.csv"  # <-- set this to your file name

YEAR = 2016
START = datetime(YEAR, 1, 1)
END = datetime(YEAR, 12, 31, 23)

OUTPUT_HOURLY = "sk_weather_hourly_2016_by_zip.csv"
OUTPUT_15MIN = "sk_weather_15min_2016_by_zip.csv"

PAUSE_EVERY_N = 50        # short pause every N ZIP codes (polite, avoids spikes)
PAUSE_SECONDS = 2         # seconds to pause
UPSAMPLE_TO_15MIN = True  # set False if you only need hourly data

# ---------------------------------------
# LOAD ZIP CODES
# ---------------------------------------

def load_zip_coordinates(path: str) -> pd.DataFrame:
    """
    Load ZIP, longitude, latitude from user-provided file.
    Expects columns: zip_code, longitude, latitude
    Aggregates to one mean coordinate per ZIP if duplicates exist.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"ZIP input file not found: {path}")

    df = pd.read_csv(path)

    required_cols = {"zip_code", "longitude", "latitude"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in input file: {missing}")

    # Ensure correct types
    df["zip_code"] = df["zip_code"].astype(str)
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")

    # Drop invalid coordinates
    df = df.dropna(subset=["longitude", "latitude"])

    # Aggregate duplicates: mean coordinate per ZIP
    grouped = (
        df.groupby("zip_code", as_index=False)
        .agg({"latitude": "mean", "longitude": "mean"})
        .rename(columns={"latitude": "lat", "longitude": "lon"})
    )

    print(f"Loaded {len(grouped)} unique ZIP codes from {path}")
    return grouped

def fetch_hourly_for_zip(zip_code: str, lat: float, lon: float) -> pd.DataFrame:
    """
    Fetch hourly Meteostat data for a single ZIP code location.
    Returns a DataFrame with columns:
        zip_code, datetime, temp, dwpt, rhum, prcp, snow, wdir, wspd, pres, tsun, etc.
    """
    location = Point(lat, lon)
    data = Hourly(location, START, END).fetch()

    if data.empty:
        return pd.DataFrame()

    data = data.reset_index().rename(columns={"time": "datetime"})
    data["zip_code"] = zip_code

    cols = ["zip_code", "datetime"] + [c for c in data.columns if c not in ("zip_code", "datetime")]
    return data[cols]

def main():
    zips = load_zip_coordinates(INPUT_ZIP_FILE)

    # Simple resume support:
    # If hourly output exists, skip already processed ZIP codes.
    processed_zips = set()
    if os.path.exists(OUTPUT_HOURLY):
        print(f"Found existing {OUTPUT_HOURLY}, resuming from it...")
        existing = pd.read_csv(OUTPUT_HOURLY, usecols=["zip_code"])
        processed_zips = set(existing["zip_code"].astype(str).unique())
        print(f"Already processed ZIP codes: {len(processed_zips)}")

    zips_to_process = zips[~zips["zip_code"].isin(processed_zips)].reset_index(drop=True)

    all_chunks = []

    # If resuming, keep existing data in memory to append later
    if processed_zips:
        existing_full = pd.read_csv(OUTPUT_HOURLY, parse_dates=["datetime"])
        all_chunks.append(existing_full)

    print(f"Fetching hourly data for {len(zips_to_process)} remaining ZIP codes...")

    for i, row in tqdm(zips_to_process.iterrows(), total=len(zips_to_process)):
        zip_code = row["zip_code"]
        lat = float(row["lat"])
        lon = float(row["lon"])

        try:
            df_zip = fetch_hourly_for_zip(zip_code, lat, lon)
            if not df_zip.empty:
                all_chunks.append(df_zip)
        except Exception as e:
            # Log and continue with next ZIP
            print(f"Error for ZIP {zip_code}: {e}")

        if PAUSE_EVERY_N > 0 and (i + 1) % PAUSE_EVERY_N == 0:
            time.sleep(PAUSE_SECONDS)

    if not all_chunks:
        print("No data fetched. Check your input file, network, or Meteostat service.")
        return

    # Concatenate all partial results
    result = pd.concat(all_chunks, ignore_index=True)

    # Sort by ZIP and datetime
    result["zip_code"] = result["zip_code"].astype(str)
    result["datetime"] = pd.to_datetime(result["datetime"])
    result.sort_values(by=["zip_code", "datetime"], inplace=True)

    # Save hourly dataset
    result.to_csv(OUTPUT_HOURLY, index=False)
    print(f"Saved hourly data to {OUTPUT_HOURLY}")
    print(f"Rows: {len(result):,}, ZIP codes: {result['zip_code'].nunique()}")

    # Optional: upsample to 15 minutes via linear interpolation
    if UPSAMPLE_TO_15MIN:
        print("Upsampling to 15-minute intervals (linear interpolation)...")

        df = result.copy()
        df = df.set_index("datetime")

        # Resample per ZIP; linear interpolation for numeric columns
        df_15 = (
            df.groupby("zip_code")
            .apply(lambda x: x.resample("15T").interpolate(method="linear"))
            .reset_index(level=0, drop=True)
            .reset_index()
        )

        # Ensure column order
        cols = ["zip_code", "datetime"] + [c for c in df_15.columns if c not in ("zip_code", "datetime")]
        df_15 = df_15[cols]

        df_15.to_csv(OUTPUT_15MIN, index=False)
        print(f"Saved 15-minute data to {OUTPUT_15MIN}")
        print(f"Rows: {len(df_15):,}, ZIP codes: {df_15['zip_code'].nunique()}")

    print("Done.")

if __name__ == "__main__":
    main()