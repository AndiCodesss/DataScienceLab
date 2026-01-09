#!/usr/bin/env python3
"""
Builds hourly (and optional 15-minute) weather data for each given ZIP code in Slovakia for 2016 and January 2017.

Input:
    A CSV file with columns: zip_code, longitude, latitude

Output:
    sk_weather_hourly_2016_2017.csv
    sk_weather_15min_2016_2017.csv (optional)

Dependencies:
    pip install meteostat pandas tqdm

Notes:
    - Data source: Meteostat (https://dev.meteostat.net/, CC BY-NC 4.0)
    - For research / university (non-commercial) use only.
    - REFACTORED: Uses subprocesses to guarantee memory reclamation.
"""

import os
import sys
import time
import gc
import argparse
import subprocess
import math
from datetime import datetime
from typing import List, Tuple

import pandas as pd
from tqdm import tqdm
from meteostat import hourly, Point, stations

# Configuration
INPUT_ZIP_FILE = "./data/sk_zip_coordinates_clean.csv"
YEAR = 2016
START = datetime(YEAR, 1, 1)
END = datetime(2017, 1, 31, 23)

OUTPUT_HOURLY = "sk_weather_hourly_2016_2017.csv"
OUTPUT_15MIN = "sk_weather_15min_2016_2017.csv"

UPSAMPLE_TO_15MIN = False  # set False if you only need hourly data
BATCH_SIZE = 50            # Number of ZIPs per subprocess
PAUSE_BETWEEN_BATCHES = 2  # Seconds to wait between batches

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

    return grouped

def fetch_hourly_for_zip(zip_code: str, lat: float, lon: float) -> pd.DataFrame:
    """
    Fetch hourly Meteostat data for a single ZIP code location.
    
    Uses stations.nearby() to find the closest station explicitly, 
    then fetches data for that station ID.
    """
    try:
        point = Point(lat, lon)
        
        # specific station lookup, check top 5 nearby
        nearby_stations = stations.nearby(point)
        # nearby() returns a DataFrame, use head() NOT fetch()
        stations_df = nearby_stations.head(5)
        
        if stations_df.empty:
            return pd.DataFrame()
        
        # Iterate through stations to find one with data
        for station_id in stations_df.index:
            try:
                # fetch data for station
                hourly_obj = hourly(station_id, START, END)
                data = hourly_obj.fetch()

                if data is not None and not data.empty:
                    # Found valid data!
                    data = data.reset_index().rename(columns={"time": "datetime"})
                    data["zip_code"] = zip_code

                    # Reorder columns
                    cols = ["zip_code", "datetime"] + [c for c in data.columns if c not in ("zip_code", "datetime")]
                    return data[cols]
            except Exception:
                continue
                
        # If no data found in any of top 5
        return pd.DataFrame()
    except Exception as e:
        print(f"Error fetching ZIP {zip_code}: {e}")
        return pd.DataFrame()

def worker_process(zip_codes: List[str], zips_df: pd.DataFrame, output_file: str):
    """
    Worker function running in a separate process.
    Fetches data for a list of ZIP codes and appends to the output file.
    """
    # Filter DataFrame for just these ZIPs
    batch_df = zips_df[zips_df["zip_code"].isin(zip_codes)]
    
    results = []
    for _, row in batch_df.iterrows():
        zip_code = row["zip_code"]
        lat = float(row["lat"])
        lon = float(row["lon"])
        
        df_zip = fetch_hourly_for_zip(zip_code, lat, lon)
        if not df_zip.empty:
            results.append(df_zip)
    
    if results:
        # Concatenate all results for this batch
        batch_result = pd.concat(results, ignore_index=True)
        
        # Append to CSV
        # Check if file exists to determine header
        file_exists = os.path.exists(output_file)
        header = not file_exists
        
        batch_result.to_csv(output_file, mode='a', header=header, index=False)
        print(f"Worker: Processed batch of {len(results)} ZIPs.")
    else:
        print("Worker: No data found for this batch.")

def upsample_chunked(input_file: str, output_file: str):
    """
    Reads the hourly input file in chunks, groups by ZIP, upsamples to 15min,
    and writes to output file incrementally.
    Optimized to avoid iterrows.
    """
    print(f"Upsampling {input_file} to {output_file} in chunks...")
    
    if os.path.exists(output_file):
        os.remove(output_file)
        
    CHUNK_SIZE = 500000 # Larger chunk size for vectorization
    reader = pd.read_csv(input_file, chunksize=CHUNK_SIZE)
    
    # Buffer to handle ZIPs spanning chunks
    buffer_df = pd.DataFrame()
    first_write = True
    
    for chunk in tqdm(reader, desc="Upsampling"):
        chunk["zip_code"] = chunk["zip_code"].astype(str)
        chunk["datetime"] = pd.to_datetime(chunk["datetime"])
        
        # Combine with buffer
        if not buffer_df.empty:
            chunk = pd.concat([buffer_df, chunk], ignore_index=True)
            buffer_df = pd.DataFrame()
        
        # Identify the last ZIP code in the chunk
        last_zip = chunk.iloc[-1]["zip_code"]
        
        # Split into 'ready to process' and 'buffer'
        # We keep all rows of the last ZIP in the buffer to ensure we have its full history 
        # (or at least the boundary) for the next chunk, unless it's the very end of file.
        # However, since we don't know if it's the end of file here easily without complexity,
        # we'll assume the stream continues.
        
        # Mask for rows that are NOT the last zip
        mask = chunk["zip_code"] != last_zip
        
        ready_df = chunk[mask]
        buffer_df = chunk[~mask]
        
        if not ready_df.empty:
            _process_and_write_group(ready_df, output_file, first_write)
            first_write = False
            
    # Process remaining buffer
    if not buffer_df.empty:
        _process_and_write_group(buffer_df, output_file, first_write)

    print(f"Finished upsampling to {output_file}")

def _process_and_write_group(df: pd.DataFrame, output_file: str, write_header: bool):
    """
    Vectorized processing of a dataframe containing multiple complete ZIP codes.
    """
    # Group by ZIP and resample
    # This is tricky to do purely vectorially with different start/ends, 
    # but we can iterate over the groups which is much faster than iterrows.
    
    results = []
    
    for zip_code, group in df.groupby("zip_code"):
        group = group.set_index("datetime").sort_index()
        
        # Drop non-numeric
        numeric_cols = group.select_dtypes(include=['number']).columns
        group_numeric = group[numeric_cols]
        
        # Resample and interpolate
        resampled = group_numeric.resample("15T").interpolate(method="linear")
        
        # Restore ZIP
        resampled["zip_code"] = zip_code
        
        results.append(resampled)
        
    if results:
        final_df = pd.concat(results).reset_index()
        
        # Reorder
        cols = ["zip_code", "datetime"] + [c for c in final_df.columns if c not in ("zip_code", "datetime")]
        final_df = final_df[cols]
        
        mode = 'w' if write_header else 'a'
        final_df.to_csv(output_file, mode=mode, header=write_header, index=False)
        
        # Explicit cleanup
        del final_df
        del results
        gc.collect()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-zips", help="Comma-separated list of ZIPs for worker process")
    parser.add_argument("--test-limit", type=int, help="Limit total ZIPs for testing")
    args = parser.parse_args()

    # --- WORKER MODE ---
    if args.worker_zips:
        try:
            zip_list = args.worker_zips.split(",")
            zips_df = load_zip_coordinates(INPUT_ZIP_FILE)
            worker_process(zip_list, zips_df, OUTPUT_HOURLY)
        except Exception as e:
            print(f"Worker failed: {e}")
            sys.exit(1)
        return

    # --- CONTROLLER MODE ---
    print("Starting robust weather downloader (Multi-process)...")
    zips_df = load_zip_coordinates(INPUT_ZIP_FILE)
    all_zips = zips_df["zip_code"].unique().tolist()
    
    if args.test_limit:
        print(f"TEST MODE: Limiting to first {args.test_limit} ZIPs")
        all_zips = all_zips[:args.test_limit]

    # Check existing progress
    processed_zips = set()
    if os.path.exists(OUTPUT_HOURLY):
        print(f"Found existing {OUTPUT_HOURLY}, checking progress...")
        try:
            # Read just the unique ZIPs, chunked to save memory
            for chunk in pd.read_csv(OUTPUT_HOURLY, usecols=["zip_code"], chunksize=100000):
                processed_zips.update(chunk["zip_code"].astype(str).unique())
            print(f"Already processed {len(processed_zips)} ZIP codes.")
        except Exception as e:
            print(f"Could not read existing file (might be corrupt or empty): {e}")

    # Filter remaining
    remaining_zips = [z for z in all_zips if z not in processed_zips]
    print(f"Remaining ZIPs to process: {len(remaining_zips)}")

    if not remaining_zips:
        print("No ZIPs to process.")
    else:
        # Process in batches
        total_batches = math.ceil(len(remaining_zips) / BATCH_SIZE)
        
        with tqdm(total=len(remaining_zips), desc="Overall Progress") as pbar:
            for i in range(0, len(remaining_zips), BATCH_SIZE):
                batch = remaining_zips[i : i + BATCH_SIZE]
                
                # Convert batch to comma-separated string
                batch_str = ",".join(batch)
                
                # Launch subprocess
                # We call the same script with --worker-zips
                cmd = [sys.executable, sys.argv[0], "--worker-zips", batch_str]
                
                try:
                    subprocess.run(cmd, check=True)
                except subprocess.CalledProcessError as e:
                    print(f"Batch failed: {e}")
                    # Optionally continue or break? 
                    # We continue to try next batches.
                
                pbar.update(len(batch))
                
                # Pause to be polite to API and let system settle
                if PAUSE_BETWEEN_BATCHES > 0:
                    time.sleep(PAUSE_BETWEEN_BATCHES)

    # Upsample if requested
    if UPSAMPLE_TO_15MIN:
        upsample_chunked(OUTPUT_HOURLY, OUTPUT_15MIN)
    
    print("All done.")

if __name__ == "__main__":
    main()