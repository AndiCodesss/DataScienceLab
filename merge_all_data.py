#!/usr/bin/env python3
"""
merge_all_data.py - Comprehensive data merger (all data except weather)
Merges:
- Meter JSON files from mergingtest/
- meter_info.csv (meter metadata)
- sk_holidays_2016.csv (holidays)
- sk_zip_coordinates_clean.csv (coordinates)
- sk_population.csv (population)
- sk_income.csv (income)
"""

import pandas as pd
from pathlib import Path
import json

# --- Configuration -----------------------------------------------------------
DATA_DIR = Path("data/energy-data")  # Full dataset with 1000 meters
INFO_CSV = Path("./data/meter_info.csv")
HOLIDAY_CSV = Path("./data/sk_holidays_2016.csv")
COORDINATES_CSV = Path("./data/sk_zip_coordinates_clean.csv")
POPULATION_CSV = Path("./data/sk_population.csv")
INCOME_CSV = Path("./data/sk_income.csv")
OUTPUT_CSV = "data/merged_all_data.csv"

print("=" * 80)
print("Starting comprehensive data merge (all data except weather)")
print("=" * 80)

# --- Load meter info ---------------------------------------------------------
print("\n1. Loading meter info...")
if not INFO_CSV.exists():
    raise SystemExit(f"Error: {INFO_CSV} not found")

meter_info = pd.read_csv(INFO_CSV)
print(f"   Loaded {len(meter_info)} meters")
print(f"   Columns: {meter_info.columns.tolist()}")

# Standardize meter_id column name
if "meter_id" not in meter_info.columns:
    for alt in ["meterID", "household_id", "id"]:
        if alt in meter_info.columns:
            meter_info = meter_info.rename(columns={alt: "meter_id"})
            print(f"   Renamed {alt} to meter_id")
            break

if "meter_id" not in meter_info.columns:
    raise SystemExit("No meter_id column found in meter_info.csv")

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
print(f"   Columns: {holidays.columns.tolist()}")

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
    print(f"   Columns: {coordinates.columns.tolist()}")
else:
    print("   Warning: coordinates file not found, skipping")

# --- Load population ---------------------------------------------------------
print("\n4. Loading population data...")
population = None
if POPULATION_CSV.exists():
    population = pd.read_csv(POPULATION_CSV)
    print(f"   Loaded {len(population)} regions")
    print(f"   Columns: {population.columns.tolist()}")
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

# --- Get meter JSON files ----------------------------------------------------
print("\n6. Scanning for meter JSON files...")
files = sorted(
    DATA_DIR.glob("meters_*_measurement.json"),
    key=lambda p: int(p.stem.split("_")[1])
)
if not files:
    raise SystemExit(f"No files like {DATA_DIR}/meters_*_measurement.json found")

print(f"   Found {len(files)} meter JSON files")

# --- Function to expand one JSON file ---------------------------------------
def expand_meter_file(filepath: Path) -> pd.DataFrame:
    """Convert one meter JSON file with daily records into 15-minute interval rows."""

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

        # Determine number of intervals
        n_intervals = max((len(v) for v in array_fields.values()), default=0)
        if n_intervals == 0:
            continue

        # Create row for each 15-minute interval
        for i in range(n_intervals):
            row = dict(scalar_fields)

            # Add array values
            for key, values in array_fields.items():
                row[key] = values[i] if i < len(values) else None

            # Add metadata
            row["meter_id"] = meter_id
            row["interval_index"] = i
            row["timestamp"] = base_timestamp + pd.Timedelta(minutes=15 * i)
            row["date"] = base_timestamp.date()

            rows.append(row)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    return df

# --- Process and merge all data ---------------------------------------------
print("\n7. Processing meter files and merging data...")
print("-" * 80)

# Process in chunks to avoid memory issues
CHUNK_SIZE = 100  # Process 100 meters at a time
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

        # Expand meter file
        meter_data = expand_meter_file(filepath)

        if meter_data.empty:
            print(f" skipped (no data)")
            continue

        # Ensure meter_id is string
        meter_data["meter_id"] = meter_data["meter_id"].astype(str)

        # 1. Merge with meter info
        meter_data = meter_data.merge(meter_info, on="meter_id", how="left")

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

        # 5. Add region from ZIP code for population merge (first 2 digits of ZIP)
        if "zip_code" in meter_data.columns:
            # Map ZIP codes to SK region codes for income/population merge
            zip_to_sk_region = {
                '01': 'SK01', '02': 'SK01', '90': 'SK01',  # Bratislavský kraj
                '91': 'SK02', '92': 'SK02', '93': 'SK02',   # Trnavský kraj (added 93)
                '94': 'SK04', '95': 'SK04',                 # Nitriansky kraj
                '96': 'SK03', '97': 'SK03',                 # Trenčiansky kraj
                '98': 'SK06', '99': 'SK06',                 # Banskobystrický kraj
                '03': 'SK05', '04': 'SK05', '05': 'SK05',   # Žilinský kraj
                '06': 'SK07', '07': 'SK07', '08': 'SK07',   # Prešovský kraj
                '09': 'SK08',                               # Košický kraj
            }

            # Map region names for income data
            region_names = {
                'SK01': 'Bratislavský kraj',
                'SK02': 'Trnavský kraj',
                'SK03': 'Trenčiansky kraj',
                'SK04': 'Nitriansky kraj',
                'SK05': 'Žilinský kraj',
                'SK06': 'Banskobystrický kraj',
                'SK07': 'Prešovský kraj',
                'SK08': 'Košický kraj'
            }

            # Map to city names for population data merge
            region_to_city = {
                'SK01': 'Bratislava',
                'SK02': 'Trnava',
                'SK03': 'Trenčín',
                'SK04': 'Nitra',
                'SK05': 'Žilina',
                'SK06': 'Banská Bystrica',
                'SK07': 'Prešov',
                'SK08': 'Košice'
            }

            # Get first 2 digits of ZIP and map to SK region code
            meter_data["sk_region_code"] = meter_data["zip_code"].str[:2].map(zip_to_sk_region)
            meter_data["region_name"] = meter_data["sk_region_code"].map(region_names)
            meter_data["region_city"] = meter_data["sk_region_code"].map(region_to_city)

            # 6. Merge with population data if available
            if population is not None and "region_city" in meter_data.columns:
                meter_data = meter_data.merge(population, left_on="region_city", right_on="Region", how="left")
                # Drop duplicate Region column (we already have region_city) and Year (always 2016)
                columns_to_drop = []
                if "Region" in meter_data.columns:
                    columns_to_drop.append("Region")
                if "Year" in meter_data.columns:
                    columns_to_drop.append("Year")
                if columns_to_drop:
                    meter_data = meter_data.drop(columns=columns_to_drop)

            # 7. Merge income data if available
            if income is not None and "sk_region_code" in meter_data.columns:
                # Merge at the main regional level (SK01, SK02, etc.)
                meter_data = meter_data.merge(income, left_on="sk_region_code", right_on="region_code", how="left")
                # Drop the duplicate region_code column from income data
                if "region_code" in meter_data.columns:
                    meter_data = meter_data.drop(columns=["region_code"])

                # Rename P_INT columns to be more descriptive
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
        print(f" {len(meter_data)} rows")

    # --- Process chunk data --------------------------------------------------------
    if all_data:
        print(f"\n   Combining chunk data ({chunk_rows} rows)...")
        chunk_data = pd.concat(all_data, ignore_index=True)

        # Sort by meter_id and timestamp
        chunk_data = chunk_data.sort_values(["meter_id", "timestamp"]).reset_index(drop=True)

        # Reorder columns for better readability
        priority_cols = ["meter_id", "timestamp", "date", "interval_index",
                        "consumption", "laggingReactivePower", "leadingReactivePower",
                        "weekday", "hour", "is_weekend", "is_holiday",
                        "zip_code", "sk_region_code", "region_name", "region_city",
                        "latitude", "longitude", "Population"]

        available_priority = [col for col in priority_cols if col in chunk_data.columns]
        other_cols = [col for col in chunk_data.columns if col not in available_priority]
        chunk_data = chunk_data[available_priority + other_cols]

        # Save to CSV (append mode after first chunk)
        if first_chunk:
            print(f"   Writing chunk to {OUTPUT_CSV} (new file)...")
            chunk_data.to_csv(OUTPUT_CSV, index=False, mode='w')
            first_chunk = False
        else:
            print(f"   Appending chunk to {OUTPUT_CSV}...")
            chunk_data.to_csv(OUTPUT_CSV, index=False, mode='a', header=False)

        total_rows += chunk_rows
        print(f"   Total rows processed so far: {total_rows:,}")

        # Free memory
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
print("=" * 80)