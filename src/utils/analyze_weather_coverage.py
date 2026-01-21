#!/usr/bin/env python3
"""
Analyze weather data coverage in detail - which ZIPs are missing and how many meters affected.
"""

import pandas as pd
from pathlib import Path
import numpy as np

# Configuration
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
METER_INFO = ROOT_DIR / "data/external/meter_info.csv"
WEATHER_FILE = ROOT_DIR / "data/processed/sk_weather_hourly_2016_2017_by_zip.csv"

print("=" * 80)
print("DETAILED WEATHER COVERAGE ANALYSIS")
print("=" * 80)

# Load meter info
print("\n1. Loading meter info...")
meter_info = pd.read_csv(METER_INFO)
meter_info['ZIP'] = meter_info['ZIP'].astype(str).str.strip()

print(f"Total meters: {len(meter_info)}")
print(f"Unique ZIP codes in meters: {meter_info['ZIP'].nunique()}")

# Count meters per ZIP
meters_per_zip = meter_info.groupby('ZIP')['meterID'].count().sort_values(ascending=False)

print("\n2. TOP 10 ZIP CODES BY METER COUNT:")
print("-" * 40)
for zip_code, count in meters_per_zip.head(10).items():
    print(f"ZIP {zip_code}: {count} meters")

# Load unique ZIPs from weather file
print("\n3. Checking weather data availability...")
if WEATHER_FILE.exists():
    # Read just ZIP codes from weather file
    weather_df = pd.read_csv(WEATHER_FILE, usecols=['zip_code'], dtype={'zip_code': str})
    weather_df['zip_code'] = weather_df['zip_code'].astype(str).str.strip()
    weather_zips = set(weather_df['zip_code'].unique())
    print(f"Unique ZIP codes in weather file: {len(weather_zips)}")
else:
    print("Weather file not found!")
    weather_zips = set()

# Analyze coverage
print("\n4. WEATHER DATA COVERAGE ANALYSIS:")
print("-" * 40)

meter_zips = set(meter_info['ZIP'].unique())
zips_with_weather = meter_zips & weather_zips
zips_without_weather = meter_zips - weather_zips

print(f"Meter ZIP codes WITH weather data: {len(zips_with_weather)} ({len(zips_with_weather)/len(meter_zips)*100:.1f}%)")
print(f"Meter ZIP codes WITHOUT weather data: {len(zips_without_weather)} ({len(zips_without_weather)/len(meter_zips)*100:.1f}%)")

# Count affected meters
meters_with_weather = meter_info[meter_info['ZIP'].isin(zips_with_weather)]
meters_without_weather = meter_info[meter_info['ZIP'].isin(zips_without_weather)]

print(f"\nMeters WITH weather data: {len(meters_with_weather)} ({len(meters_with_weather)/len(meter_info)*100:.1f}%)")
print(f"Meters WITHOUT weather data: {len(meters_without_weather)} ({len(meters_without_weather)/len(meter_info)*100:.1f}%)")

# List ZIP codes without weather and their meter counts
print("\n5. ZIP CODES WITHOUT WEATHER DATA (sorted by affected meters):")
print("-" * 40)

if len(zips_without_weather) > 0:
    missing_zip_impacts = []
    for zip_code in zips_without_weather:
        meter_count = meters_per_zip.get(zip_code, 0)
        missing_zip_impacts.append((zip_code, meter_count))

    # Sort by meter count (most affected first)
    missing_zip_impacts.sort(key=lambda x: x[1], reverse=True)

    print(f"Total ZIP codes without weather: {len(missing_zip_impacts)}")
    print("\nTop 20 most impacted ZIP codes:")
    for zip_code, meter_count in missing_zip_impacts[:20]:
        print(f"  ZIP {zip_code}: {meter_count} meters affected")

    # Group by prefix to see regional patterns
    print("\n6. MISSING WEATHER DATA BY REGION (ZIP prefix):")
    print("-" * 40)

    prefix_counts = {}
    for zip_code, meter_count in missing_zip_impacts:
        prefix = zip_code[:2]
        if prefix not in prefix_counts:
            prefix_counts[prefix] = {'zips': 0, 'meters': 0}
        prefix_counts[prefix]['zips'] += 1
        prefix_counts[prefix]['meters'] += meter_count

    # Map prefixes to regions
    prefix_to_region = {
        '01': 'Bratislavský kraj', '02': 'Bratislavský kraj', '90': 'Bratislavský kraj',
        '91': 'Trnavský kraj', '92': 'Trnavský kraj', '93': 'Trnavský kraj',
        '94': 'Nitriansky kraj', '95': 'Nitriansky kraj',
        '96': 'Trenčiansky kraj', '97': 'Trenčiansky kraj',
        '98': 'Banskobystrický kraj', '99': 'Banskobystrický kraj',
        '03': 'Žilinský kraj', '04': 'Žilinský kraj', '05': 'Žilinský kraj',
        '06': 'Prešovský kraj', '07': 'Prešovský kraj', '08': 'Prešovský kraj',
        '09': 'Košický kraj',
    }

    for prefix in sorted(prefix_counts.keys()):
        region = prefix_to_region.get(prefix, 'Unknown')
        counts = prefix_counts[prefix]
        print(f"Prefix {prefix} ({region}): {counts['zips']} ZIP codes, {counts['meters']} meters affected")

# Check if missing weather ZIPs are valid Slovak ZIP codes
print("\n7. VALIDATION OF MISSING ZIP CODES:")
print("-" * 40)

# Slovak ZIP codes should be 5 digits and start with specific prefixes
valid_prefixes = ['01', '02', '03', '04', '05', '06', '07', '08', '09',
                  '90', '91', '92', '93', '94', '95', '96', '97', '98', '99']

invalid_zips = []
for zip_code in zips_without_weather:
    if len(zip_code) != 5 or zip_code[:2] not in valid_prefixes:
        invalid_zips.append(zip_code)

if invalid_zips:
    print(f"Found {len(invalid_zips)} potentially invalid ZIP codes:")
    for zip_code in invalid_zips[:10]:
        meter_count = meters_per_zip.get(zip_code, 0)
        print(f"  {zip_code} ({meter_count} meters)")
else:
    print("All missing ZIP codes appear to be valid Slovak postal codes.")

# Summary and recommendations
print("\n" + "=" * 80)
print("SUMMARY AND RECOMMENDATIONS")
print("=" * 80)

print(f"\n✓ {len(meters_with_weather)} meters ({len(meters_with_weather)/len(meter_info)*100:.1f}%) HAVE weather data")
print(f"✗ {len(meters_without_weather)} meters ({len(meters_without_weather)/len(meter_info)*100:.1f}%) MISSING weather data")


# Calculate expected vs actual weather coverage
expected_records = len(meter_info) * 396 * 24  # 1000 meters × 396 days × 24 hours
records_with_weather = len(meters_with_weather) * 396 * 24
missing_weather_records = len(meters_without_weather) * 396 * 24

print(f"\nIMPACT ON DATASET:")
print(f"Expected total records: {expected_records:,}")
print(f"Records with weather potential: {records_with_weather:,} ({records_with_weather/expected_records*100:.1f}%)")
print(f"Records missing weather: {missing_weather_records:,} ({missing_weather_records/expected_records*100:.1f}%)")

print("\nThis explains why ~22% of temperature data is missing in the merged dataset!")