"""
01_merge_data.py - Main ETL Pipeline

This script merges all data sources into a unified hourly dataset:
- 1000 meter JSON files (15-min intervals → aggregated to hourly)
- Weather data (from Meteostat API)
- Meter info with ZIP codes
- Slovak holidays
- Population and income data by NUTS-3 region
- Geographic coordinates

Output: data/processed/merged_data_hourly_with_weather.csv

Usage:
    python src/01_merge_data.py
"""

import pandas as pd
import json
from pathlib import Path
import numpy as np

# --- Configuration -----------------------------------------------------------
print("=" * 80)
print("MERGE ALL DATA TO HOURLY WITH WEATHER")
print("=" * 80)

# Define paths (relative to project root)
DATA_DIR = Path("data/raw/energy-data")
METER_INFO_CSV = Path("data/external/meter_info.csv")
HOLIDAY_CSV = Path("data/external/sk_holidays_2016.csv")
COORDINATES_CSV = Path("data/external/sk_zip_coordinates_clean.csv")
POPULATION_CSV = Path("data/external/sk_population.csv")
INCOME_CSV = Path("data/external/sk_income.csv")
WEATHER_CSV = Path("data/processed/sk_weather_hourly_2016_2017_by_zip.csv")
OUTPUT_CSV = Path("data/processed/merged_data_hourly_with_weather.csv")

# --- Delete old output file to prevent duplicates -----------------------------
if OUTPUT_CSV.exists():
    print(f"\n⚠ Deleting existing {OUTPUT_CSV} to prevent duplicates...")
    OUTPUT_CSV.unlink()
    print("   Done.")


# --- Load meter info ---------------------------------------------------------
print("\n1. Loading meter info...")
if not METER_INFO_CSV.exists():
    raise SystemExit(f"Error: {METER_INFO_CSV} not found")

meter_info = pd.read_csv(METER_INFO_CSV)
print(f"   Loaded {len(meter_info)} meters")

# Rename meterID to meter_id for consistency
if "meterID" in meter_info.columns:
    meter_info = meter_info.rename(columns={"meterID": "meter_id"})
    print("   Renamed meterID to meter_id")

# Ensure meter_id is string
meter_info["meter_id"] = meter_info["meter_id"].astype(str)
meter_info = meter_info.drop_duplicates(subset=["meter_id"]).reset_index(drop=True)

# Standardize ZIP column if present
if "ZIP" in meter_info.columns:
    meter_info = meter_info.rename(columns={"ZIP": "zip_code"})
    meter_info["zip_code"] = meter_info["zip_code"].astype(str).str.strip()

# --- Load holidays -----------------------------------------------------------
print("\n2. Loading holidays...")
if not HOLIDAY_CSV.exists():
    raise SystemExit(f"Error: {HOLIDAY_CSV} not found")

holidays = pd.read_csv(HOLIDAY_CSV, parse_dates=["date"])
holidays["date"] = holidays["date"].dt.date
print(f"   Loaded {len(holidays)} holidays")

# Keep only date and create is_holiday flag
holiday_dates = holidays[["date"]].drop_duplicates()
holiday_dates["is_holiday"] = 1

# --- Load coordinates --------------------------------------------------------
print("\n3. Loading coordinates...")
coordinates = None
if COORDINATES_CSV.exists():
    coordinates = pd.read_csv(COORDINATES_CSV)
    coordinates["zip_code"] = coordinates["zip_code"].astype(str).str.strip()
    coordinates = coordinates.drop_duplicates(subset=["zip_code"]).reset_index(drop=True)
    print(f"   Loaded {len(coordinates)} postal codes with coordinates")
else:
    print("   Warning: coordinates file not found, skipping")

# --- Load population ---------------------------------------------------------
print("\n4. Loading population data...")
population = None
if POPULATION_CSV.exists():
    population = pd.read_csv(POPULATION_CSV)
    print(f"   Loaded {len(population)} regions")
else:
    print("   Warning: population file not found, skipping")

# --- Load income -------------------------------------------------------------
print("\n5. Loading income data...")
income = None
if INCOME_CSV.exists():
    # Skip the metadata rows and read from the actual data (starting at row 6)
    income = pd.read_csv(INCOME_CSV, skiprows=6)
    # Rename columns for clarity
    income.columns = ['region_code', 'year', 'income_bracket', 'measurement_type', 'value']

    # Filter for household counts only (not percentages) and pivot to wide format
    income_counts = income[income['measurement_type'] == 'mj_pocet_dom'].copy()
    income_pivot = income_counts.pivot_table(
        index='region_code',
        columns='income_bracket',
        values='value',
        aggfunc='first'
    ).reset_index()

    print(f"   Loaded {len(income)} income records")
    print(f"   Processed into {len(income_pivot)} regional income profiles")
    income = income_pivot
else:
    print("   Warning: income file not found, skipping")

# --- Load weather data -------------------------------------------------------
print("\n6. Loading weather data...")
weather = None
if WEATHER_CSV.exists():
    print("   Reading weather file (this may take a moment due to file size)...")
    # Read weather data with datetime parsing
    weather = pd.read_csv(WEATHER_CSV, parse_dates=['datetime'])

    # Ensure zip_code is string for merging
    weather['zip_code'] = weather['zip_code'].astype(str).str.strip()

    # Create date and hour columns for easier merging
    weather['date'] = weather['datetime'].dt.date
    weather['hour'] = weather['datetime'].dt.hour

    # Rename columns to avoid conflicts
    weather_cols_rename = {
        'temp': 'temperature',
        'dwpt': 'dew_point',
        'rhum': 'relative_humidity',
        'prcp': 'precipitation',
        'snow': 'snow_depth',
        'wdir': 'wind_direction',
        'wspd': 'wind_speed',
        'wpgt': 'wind_gust',
        'pres': 'pressure',
        'tsun': 'sunshine',
        'coco': 'weather_condition'
    }
    weather = weather.rename(columns=weather_cols_rename)

    print(f"   Loaded {len(weather):,} hourly weather records")
    print(f"   Unique ZIP codes in weather data: {weather['zip_code'].nunique()}")
    print(f"   Date range: {weather['date'].min()} to {weather['date'].max()}")
else:
    print("   Warning: weather file not found, skipping")

# --- Get meter JSON files ----------------------------------------------------
print("\n7. Scanning for meter JSON files...")
files = sorted(
    DATA_DIR.glob("meters_*_measurement.json"),
    key=lambda p: int(p.stem.split("_")[1])
)
if not files:
    raise SystemExit(f"No files like {DATA_DIR}/meters_*_measurement.json found")

print(f"   Found {len(files)} meter JSON files")

# --- Function to expand one JSON file ---------------------------------------
def expand_meter_file(filepath: Path) -> pd.DataFrame:
    """Convert one meter JSON file with daily records into 1-hour interval rows."""
    # Read JSON file
    with open(filepath, 'r') as f:
        data = json.load(f)

    # If data is not a list, try to convert it to DataFrame directly
    if not isinstance(data, list):
        days = pd.DataFrame([data])
    else:
        days = pd.DataFrame(data)

    rows = []

    for _, record in days.iterrows():
        # Extract basic info
        year = int(record.get("year", 2016))
        month = int(record.get("month", 1))
        day = int(record.get("day", 1))
        meter_id = str(record.get("meterID", filepath.stem.split("_")[1]))

        base_timestamp = pd.Timestamp(year, month, day)

        # Separate array fields from scalar fields
        array_fields = {}
        scalar_fields = {}

        for key, value in record.items():
            if isinstance(value, (list, tuple)):
                array_fields[key] = value
            else:
                scalar_fields[key] = value

        # Remove already processed fields
        for k in ["meterID", "year", "month", "day"]:
            scalar_fields.pop(k, None)

        # Determine number of 15-minute intervals
        n_15min_intervals = max((len(v) for v in array_fields.values()), default=0)
        if n_15min_intervals == 0:
            continue

        # Create row for each 1-hour interval (24 per day)
        for hour_idx in range(24):  # 24 hours per day
            row = dict(scalar_fields)

            # Calculate the range of 15-minute intervals for this hour
            start_idx = hour_idx * 4
            end_idx = min(start_idx + 4, n_15min_intervals)

            # Aggregate array values for this hour
            for key, values in array_fields.items():
                hour_values = values[start_idx:end_idx] if start_idx < len(values) else []
                if hour_values:
                    row[key] = sum(hour_values)
                else:
                    row[key] = None

            # Add metadata
            row["meter_id"] = meter_id
            row["interval_index"] = hour_idx
            row["timestamp"] = base_timestamp + pd.Timedelta(hours=hour_idx)
            row["date"] = base_timestamp.date()

            rows.append(row)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    return df

# --- Process and merge all data ---------------------------------------------
print("\n8. Processing meter files and merging data...")
print("-" * 80)

# Process in chunks to avoid memory issues
CHUNK_SIZE = 100
first_chunk = True
total_rows = 0

# Process files in chunks
for chunk_start in range(0, len(files), CHUNK_SIZE):
    chunk_end = min(chunk_start + CHUNK_SIZE, len(files))
    chunk_files = files[chunk_start:chunk_end]

    print(f"\n   Processing chunk: meters {chunk_start+1} to {chunk_end} of {len(files)}")
    print("   " + "-" * 60)

    all_data = []  # Reset for each chunk
    chunk_rows = 0

    for i, filepath in enumerate(chunk_files, chunk_start + 1):
        print(f"   Processing {filepath.name} ({i}/{len(files)})...", end="")

        # 1. Expand JSON to DataFrame
        meter_data = expand_meter_file(filepath)
        if meter_data.empty:
            print(" (empty/skipped)")
            continue

        # 1b. Merge with meter_info to get ZIP code (CRITICAL FIX)
        # Ensure meter_id is string in both
        meter_data["meter_id"] = meter_data["meter_id"].astype(str)
        if "meter_id" in meter_info.columns:
            meter_data = meter_data.merge(meter_info[["meter_id", "zip_code"]], on="meter_id", how="left")

        # 2. Add holiday flag
        meter_data = meter_data.merge(holiday_dates, on="date", how="left")
        meter_data["is_holiday"] = meter_data["is_holiday"].fillna(0).astype(int)

        # 3. Add weekend and weekday
        meter_data["is_weekend"] = (meter_data["timestamp"].dt.weekday >= 5).astype(int)
        meter_data["weekday"] = meter_data["timestamp"].dt.day_name()
        meter_data["hour"] = meter_data["timestamp"].dt.hour
        meter_data["month"] = meter_data["timestamp"].dt.month
        meter_data["day_of_month"] = meter_data["timestamp"].dt.day

        # 4. Merge with coordinates if available
        if coordinates is not None and "zip_code" in meter_data.columns:
            meter_data = meter_data.merge(coordinates, on="zip_code", how="left")

        # 5. Add region from ZIP code for population merge
        if "zip_code" in meter_data.columns:
            # PLZ prefix to NUTS-3 region mapping (based on actual Slovak postal codes)
            zip_to_sk_region = {
                '81': 'SK010', '82': 'SK010', '83': 'SK010', '84': 'SK010', '85': 'SK010',  # Bratislava
                '90': 'SK021',  # Trnava region
                '91': 'SK022',  # Trenčín region
                '92': 'SK023',  # Nitra region (west)
                '93': 'SK023',  # Nitra region (east)
                '94': 'SK032',  # Banská Bystrica region
                '95': 'SK031',  # Žilina region
            }
            region_names = {
                'SK010': 'Bratislavský kraj',
                'SK021': 'Trnavský kraj',
                'SK022': 'Trenčiansky kraj', 
                'SK023': 'Nitriansky kraj',
                'SK031': 'Žilinský kraj',
                'SK032': 'Banskobystrický kraj',
            }
            region_to_city = {
                'SK010': 'Bratislava',
                'SK021': 'Trnava',
                'SK022': 'Trenčín',
                'SK023': 'Nitra',
                'SK031': 'Žilina',
                'SK032': 'Banská Bystrica',
            }

            meter_data["sk_region_code"] = meter_data["zip_code"].str[:2].map(zip_to_sk_region)
            meter_data["region_name"] = meter_data["sk_region_code"].map(region_names)
            meter_data["region_city"] = meter_data["sk_region_code"].map(region_to_city)

            # 6. Merge with population data
            if population is not None and "region_city" in meter_data.columns:
                meter_data = meter_data.merge(population, left_on="region_city", right_on="Region", how="left")
                columns_to_drop = [c for c in ["Region", "Year"] if c in meter_data.columns]
                if columns_to_drop:
                    meter_data = meter_data.drop(columns=columns_to_drop)

            # 7. Merge income data
            if income is not None and "sk_region_code" in meter_data.columns:
                meter_data = meter_data.merge(income, left_on="sk_region_code", right_on="region_code", how="left")
                if "region_code" in meter_data.columns:
                    meter_data = meter_data.drop(columns=["region_code"])

                income_rename = {
                    'P_INT_1': 'households_income_bracket_1',
                    'P_INT_2': 'households_income_bracket_2',
                    'P_INT_3': 'households_income_bracket_3',
                    'P_INT_4': 'households_income_bracket_4',
                    'P_INT_5': 'households_income_bracket_5',
                    'P_INT_6': 'households_income_bracket_6',
                    'P_INT_7': 'households_income_bracket_7',
                    'P_INT_8': 'households_income_bracket_8',
                    'P_INT_9': 'households_income_bracket_9',
                    'P_INT_10': 'households_income_bracket_10',
                    'P_INT_11': 'households_income_bracket_11',
                    'P_INT_spolu': 'households_total'
                }
                meter_data = meter_data.rename(columns=income_rename)

        all_data.append(meter_data)
        chunk_rows += len(meter_data)
        print()

    # --- Process chunk data --------------------------------------------------------
    if all_data:
        print(f"\n   Combining chunk data ({chunk_rows} rows)...")
        chunk_data = pd.concat(all_data, ignore_index=True)
        chunk_data = chunk_data.sort_values(["meter_id", "timestamp"]).reset_index(drop=True)

        # 8. MERGE WEATHER DATA (Optimized: Merge once per chunk)
        if weather is not None and "zip_code" in chunk_data.columns:
            print(f"   Merging weather data for chunk...", end="")
            chunk_data = chunk_data.merge(
                weather[['zip_code', 'date', 'hour', 'temperature', 'dew_point',
                        'relative_humidity', 'precipitation', 'snow_depth',
                        'wind_direction', 'wind_speed', 'wind_gust',
                        'pressure', 'sunshine', 'weather_condition']],
                on=['zip_code', 'date', 'hour'],
                how='left'
            )
            print(f" Done.")

        # Define EXACT column order to ensure consistency across chunks
        final_columns = [
            "meter_id", "timestamp", "date", "interval_index",
            "consumption", "laggingReactivePower", "leadingReactivePower",
            "temperature", "dew_point", "relative_humidity", "precipitation", "snow_depth",
            "wind_direction", "wind_speed", "wind_gust", "pressure", "sunshine", "weather_condition",
            "weekday", "hour", "month", "day_of_month", "is_weekend", "is_holiday",
            "zip_code", "sk_region_code", "region_name", "region_city",
            "latitude", "longitude", "Population",
            "households_income_bracket_1", "households_income_bracket_2", "households_income_bracket_3",
            "households_income_bracket_4", "households_income_bracket_5", "households_income_bracket_6",
            "households_income_bracket_7", "households_income_bracket_8", "households_income_bracket_9",
            "households_income_bracket_10", "households_income_bracket_11", "households_total"
        ]

        # Add any missing columns as NaN
        for col in final_columns:
            if col not in chunk_data.columns:
                chunk_data[col] = pd.NA

        # Select and reorder columns
        chunk_data = chunk_data[final_columns]

        # Ensure output directory exists
        OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

        # Save to CSV
        if first_chunk:
            print(f"   Writing chunk to {OUTPUT_CSV} (new file)...")
            chunk_data.to_csv(OUTPUT_CSV, index=False, mode='w')
            first_chunk = False
        else:
            print(f"   Appending chunk to {OUTPUT_CSV}...")
            chunk_data.to_csv(OUTPUT_CSV, index=False, mode='a', header=False)

        total_rows += chunk_rows
        print(f"   Total rows processed so far: {total_rows:,}")

        del chunk_data
        del all_data

# --- Final summary --------------------------------------------------------
print("\n" + "=" * 80)
print("MERGE COMPLETE - Summary Statistics:")
print("=" * 80)
print(f"Output file: {OUTPUT_CSV}")
print(f"Total rows processed: {total_rows:,}")
print(f"Total meters: {len(files)}")
print("\n" + "=" * 80)
print("Script completed successfully!")
print("Weather data has been merged based on ZIP code and hour.")
print("=" * 80)
