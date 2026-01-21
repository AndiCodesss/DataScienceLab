#!/usr/bin/env python3
"""
check_weather_data.py - Weather Data Quality Check

Quick analysis tool to examine the hourly weather data file,
checking coverage, completeness, and basic statistics.
"""

import os
from pathlib import Path
from datetime import datetime

print("=" * 80)
print("WEATHER DATA QUALITY CHECK")
print("=" * 80)

# Configuration
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
WEATHER_FILE = ROOT_DIR / "data/processed/sk_weather_hourly_2016_2017_by_zip.csv"

# -----------------------------------------------------------------------------
# 1. FILE INFO
# -----------------------------------------------------------------------------
print("\n1. FILE INFORMATION")
print("-" * 40)

if WEATHER_FILE.exists():
    file_size = os.path.getsize(WEATHER_FILE)
    print(f"File: {WEATHER_FILE}")
    print(f"Size: {file_size:,} bytes ({file_size / (1024**3):.2f} GB)")
else:
    print(f"ERROR: {WEATHER_FILE} not found!")
    exit(1)

# -----------------------------------------------------------------------------
# 2. PREVIEW DATA (HEAD)
# -----------------------------------------------------------------------------
print("\n2. DATA PREVIEW (First 10 rows)")
print("-" * 40)

with open(WEATHER_FILE, 'r') as f:
    for i, line in enumerate(f):
        if i < 10:
            if i == 0:
                print("\nHeader:")
                print(line.strip())
                # Parse header for column analysis
                headers = line.strip().split(',')
                print(f"\nTotal columns: {len(headers)}")
                print(f"Columns: {', '.join(headers)}")
                print("\nFirst 9 data rows:")
            else:
                print(f"{i}: {line.strip()[:120]}..." if len(line) > 120 else f"{i}: {line.strip()}")
        else:
            break

# -----------------------------------------------------------------------------
# 3. DATA ANALYSIS
# -----------------------------------------------------------------------------
print("\n3. DATA ANALYSIS")
print("-" * 40)

# Read file in chunks to analyze without loading entire file
print("Analyzing data (this may take a moment for large file)...")

# Variables to track
unique_zips = set()
all_zips_estimate = set()  # Track ZIP codes throughout file
date_range = {'min': None, 'max': None}
total_rows = 0
missing_counts = {col: 0 for col in headers}
sample_values = {col: [] for col in headers}

# Read in chunks
chunk_size = 100000
rows_analyzed = 0
sample_interval = 1000  # Sample every 1000th row for ZIP diversity

with open(WEATHER_FILE, 'r') as f:
    # Skip header
    header_line = f.readline()

    for line_num, line in enumerate(f, 1):
        total_rows += 1

        # Parse line
        values = line.strip().split(',')

        # Sample ZIP codes throughout the file for better estimate
        if total_rows % sample_interval == 0 and len(values) > 0:
            all_zips_estimate.add(values[0])

        # Only analyze first chunk in detail for performance
        if rows_analyzed < chunk_size:
            rows_analyzed += 1

            # Track ZIP codes
            if len(values) > 0:
                unique_zips.add(values[0])

            # Track dates (assuming datetime is second column)
            if len(values) > 1 and values[1]:
                try:
                    # Parse datetime
                    dt = datetime.strptime(values[1], '%Y-%m-%d %H:%M:%S')
                    if date_range['min'] is None or dt < date_range['min']:
                        date_range['min'] = dt
                    if date_range['max'] is None or dt > date_range['max']:
                        date_range['max'] = dt
                except:
                    pass

            # Count missing values
            for i, val in enumerate(values):
                if i < len(headers):
                    if val == '' or val == 'NA':
                        missing_counts[headers[i]] += 1
                    # Store sample values for numeric columns
                    if rows_analyzed <= 10 and val and val != 'NA':
                        sample_values[headers[i]].append(val)

        # Progress indicator
        if total_rows % 100000 == 0:
            print(f"  Processed {total_rows:,} rows...", end='\r')

print(f"\nTotal rows: {total_rows:,}")
print(f"Rows analyzed in detail: {rows_analyzed:,}")

# -----------------------------------------------------------------------------
# 4. COVERAGE ANALYSIS
# -----------------------------------------------------------------------------
print("\n4. COVERAGE ANALYSIS")
print("-" * 40)

print(f"Unique ZIP codes (first 100k rows): {len(unique_zips)}")
print(f"Estimated unique ZIP codes (sampled throughout file): {len(all_zips_estimate)}")

if date_range['min'] and date_range['max']:
    print(f"\nDate range (from sample):")
    print(f"  First: {date_range['min'].strftime('%Y-%m-%d %H:%M')}")
    print(f"  Last:  {date_range['max'].strftime('%Y-%m-%d %H:%M')}")

    # Calculate expected hours
    days_span = (date_range['max'] - date_range['min']).days + 1
    expected_hours = days_span * 24
    print(f"  Span: {days_span} days")
    print(f"  Expected hours per ZIP: {expected_hours:,}")

# -----------------------------------------------------------------------------
# 5. MISSING DATA ANALYSIS
# -----------------------------------------------------------------------------
print("\n5. MISSING DATA ANALYSIS (from sample)")
print("-" * 40)

print("Missing values per column:")
for col in headers:
    if col in missing_counts:
        missing_pct = (missing_counts[col] / rows_analyzed * 100) if rows_analyzed > 0 else 0
        print(f"  {col:20s}: {missing_counts[col]:6d} ({missing_pct:5.2f}%)")

# -----------------------------------------------------------------------------
# 6. WEATHER VARIABLES
# -----------------------------------------------------------------------------
print("\n6. WEATHER VARIABLES OVERVIEW")
print("-" * 40)

# Map column names to descriptions
weather_vars = {
    'temp': 'Temperature (°C)',
    'dwpt': 'Dew Point (°C)',
    'rhum': 'Relative Humidity (%)',
    'prcp': 'Precipitation (mm)',
    'snow': 'Snow Depth (cm)',
    'wdir': 'Wind Direction (degrees)',
    'wspd': 'Wind Speed (km/h)',
    'wpgt': 'Wind Gust (km/h)',
    'pres': 'Pressure (hPa)',
    'tsun': 'Sunshine (minutes)',
    'coco': 'Weather Condition Code'
}

print("Available weather variables:")
for col in headers:
    if col in weather_vars:
        print(f"  • {col:6s}: {weather_vars[col]}")
        # Show sample values
        if col in sample_values and sample_values[col]:
            sample_str = ', '.join(sample_values[col][:5])
            print(f"           Sample values: {sample_str}")

# -----------------------------------------------------------------------------
# 7. DATA QUALITY SUMMARY
# -----------------------------------------------------------------------------
print("\n" + "=" * 80)
print("DATA QUALITY SUMMARY")
print("=" * 80)

print(f"\n📊 Key Metrics:")
print(f"  • File size: {file_size / (1024**3):.2f} GB")
print(f"  • Total rows: {total_rows:,}")
print(f"  • ZIP codes in first 100k rows: {len(unique_zips)}")
print(f"  • Estimated total ZIP codes: ~{len(all_zips_estimate)} (sampled throughout)")
print(f"  • Weather variables: {len([h for h in headers if h in weather_vars])}")

# Estimate average rows per ZIP using better estimate
best_zip_estimate = len(all_zips_estimate) if len(all_zips_estimate) > len(unique_zips) else len(unique_zips)
if best_zip_estimate > 0:
    est_rows_per_zip = total_rows / best_zip_estimate
    print(f"  • Estimated rows per ZIP: ~{int(est_rows_per_zip):,}")

    # Check if this matches expected hourly data
    if date_range['min'] and date_range['max']:
        expected_hourly = (date_range['max'] - date_range['min']).total_seconds() / 3600 + 1
        coverage_pct = (est_rows_per_zip / expected_hourly * 100) if expected_hourly > 0 else 0
        print(f"  • Temporal coverage: ~{coverage_pct:.1f}% of expected hours")

        # Add interpretation
        if coverage_pct > 150:
            print(f"    ⚠️  Coverage >100% suggests ZIP sampling underestimated unique ZIPs")
            # Recalculate with expected coverage
            implied_zips = total_rows / expected_hourly
            print(f"    → Implied ZIP count for 100% coverage: ~{int(implied_zips):,}")

print("\n✅ Weather Data Check Complete!")
print("=" * 80)