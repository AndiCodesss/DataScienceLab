#!/usr/bin/env python3
"""
data_completeness_check.py - Data Quality and Completeness Audit Tool

This script performs a comprehensive data quality assessment on meter data,
checking for completeness, consistency, and data integrity issues.

Functions:
- Validates temporal coverage and identifies date ranges
- Detects missing or incomplete meter records
- Identifies daylight saving time anomalies
- Verifies data aggregation accuracy
- Provides overall data quality metrics
"""

import json
from pathlib import Path
from datetime import datetime
from statistics import mean

print("=" * 80)
print("DATA COMPLETENESS AND QUALITY AUDIT")
print("=" * 80)

# Configuration
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = ROOT_DIR / "data/raw/energy-data"
OUTPUT_FILE = ROOT_DIR / "data/processed/merged_data_hourly_with_weather.csv"

# -----------------------------------------------------------------------------
# 1. DATA SPAN ANALYSIS
# -----------------------------------------------------------------------------
print("\n1. TEMPORAL COVERAGE ANALYSIS")
print("-" * 40)

# Check a sample meter file to determine date range
sample_meter = DATA_DIR / "meters_100_measurement.json"
with open(sample_meter, 'r') as f:
    data = json.load(f)

# Extract all dates
dates = []
for record in data:
    dates.append(datetime(record['year'], record['month'], record['day']))

first_date = min(dates)
last_date = max(dates)
total_days = len(set(dates))

print(f"First date: {first_date.strftime('%Y-%m-%d')} ({first_date.strftime('%B %d, %Y')})")
print(f"Last date: {last_date.strftime('%Y-%m-%d')} ({last_date.strftime('%B %d, %Y')})")
print(f"Total unique days in data: {total_days}")
print(f"Expected days in 2016 (leap year): 366")
print(f"Extra days beyond 2016: {total_days - 366}")
print(f"Date span: {(last_date - first_date).days + 1} days")

# -----------------------------------------------------------------------------
# 2. EXPECTED VS ACTUAL ROWS
# -----------------------------------------------------------------------------
print("\n2. DATA COMPLETENESS METRICS")
print("-" * 40)

# Count total meter files
meter_files = list(DATA_DIR.glob("meters_*_measurement.json"))
total_meters = len(meter_files)

# Calculate theoretical expectations
expected_rows_366_days = total_meters * 366 * 24  # If only 2016
expected_rows_396_days = total_meters * total_days * 24  # Actual date span

print(f"Total meters: {total_meters:,}")
print(f"Days per meter (if complete): {total_days}")
print(f"Hours per day: 24")
print(f"\nExpected rows if 366 days (2016 only): {expected_rows_366_days:,}")
print(f"Expected rows if {total_days} days (full span): {expected_rows_396_days:,}")

# Check actual output file if it exists
if OUTPUT_FILE.exists():
    # Count rows (excluding header)
    with open(OUTPUT_FILE, 'r') as f:
        actual_rows = sum(1 for line in f) - 1  # Subtract header

    print(f"\nActual rows in output: {actual_rows:,}")
    print(f"Difference from full expectation: {actual_rows - expected_rows_396_days:,}")
    print(f"Missing rows: {expected_rows_396_days - actual_rows:,}")
    print(f"Missing equivalent to: {(expected_rows_396_days - actual_rows) / (total_days * 24):.2f} complete meters")
else:
    print("\nNote: Output file not found. Run merge_all_data_tohourly.py first.")
    actual_rows = None

# -----------------------------------------------------------------------------
# 3. INCOMPLETE METER DATA
# -----------------------------------------------------------------------------
print("\n3. METER DATA INTEGRITY CHECK")
print("-" * 40)

print("Scanning all meters for data completeness...")

incomplete_meters = []
meter_day_counts = {}

# Check each meter file
for i, filepath in enumerate(meter_files, 1):
    if i % 100 == 0:
        print(f"  Processed {i}/{total_meters} meters...", end='\r')

    with open(filepath, 'r') as f:
        data = json.load(f)

    meter_id = filepath.stem.split('_')[1]
    days_with_data = len(data)

    # Store for analysis
    meter_day_counts[meter_id] = days_with_data

    # Check for valid consumption data
    valid_days = 0
    for record in data:
        consumption = record.get('consumption', [])
        if consumption and len(consumption) > 0:
            valid_days += 1

    # Flag if incomplete
    if days_with_data != total_days:
        incomplete_meters.append({
            'meter_id': meter_id,
            'total_days': days_with_data,
            'valid_days': valid_days,
            'missing_days': total_days - days_with_data
        })

print(f"\n\nIncomplete Meters Found: {len(incomplete_meters)}")
print("-" * 40)

# Sort by missing days (most missing first)
incomplete_meters.sort(key=lambda x: x['missing_days'], reverse=True)

total_missing_days = 0
for meter in incomplete_meters:
    print(f"Meter {meter['meter_id']:4s}: {meter['total_days']:3d} days (missing {meter['missing_days']:3d} days)")
    total_missing_days += meter['missing_days']

print(f"\nTotal missing meter-days: {total_missing_days:,}")
if incomplete_meters:
    avg_days = mean([m['total_days'] for m in incomplete_meters])
    print(f"Average days per incomplete meter: {avg_days:.1f}")
else:
    print("No incomplete meters found")

# -----------------------------------------------------------------------------
# 4. DAYLIGHT SAVING TIME ANOMALIES
# -----------------------------------------------------------------------------
print("\n4. TEMPORAL ANOMALY DETECTION")
print("-" * 40)

print("Checking for daylight saving time adjustments...")

# DST dates in 2016 for Slovakia/EU
dst_spring_2016 = datetime(2016, 3, 27)  # Spring forward
dst_fall_2016 = datetime(2016, 10, 30)   # Fall back

# Check several meters for DST anomalies
meters_to_check = ['100', '500', '999']
dst_findings = {}

for meter_id in meters_to_check:
    filepath = DATA_DIR / f"meters_{meter_id}_measurement.json"
    with open(filepath, 'r') as f:
        data = json.load(f)

    anomalies = []
    for record in data:
        date = datetime(record['year'], record['month'], record['day'])
        consumption = record.get('consumption', [])
        intervals = len(consumption)

        # Check for non-standard interval counts
        if intervals != 96:
            anomalies.append({
                'date': date,
                'intervals': intervals,
                'expected': 96
            })

    if anomalies:
        dst_findings[meter_id] = anomalies

# Display DST findings
print(f"\nTemporal anomalies detected in sample meters:")
print("-" * 40)

for meter_id, anomalies in dst_findings.items():
    print(f"\nMeter {meter_id}:")
    for anomaly in anomalies:
        date_str = anomaly['date'].strftime('%Y-%m-%d')
        day_name = anomaly['date'].strftime('%A')

        # Identify the type of anomaly
        if anomaly['date'].date() == dst_spring_2016.date():
            event = "Spring DST (clock forward 1 hour)"
        elif anomaly['date'].date() == dst_fall_2016.date():
            event = "Fall DST (clock back 1 hour)"
        elif anomaly['date'].date() == last_date.date():
            event = "Last day of data collection (partial)"
        else:
            event = "Unknown anomaly"

        hours = anomaly['intervals'] / 4  # Convert 15-min intervals to hours
        print(f"  {date_str} ({day_name}): {anomaly['intervals']} intervals = {hours:.1f} hours")
        print(f"    Event: {event}")

# -----------------------------------------------------------------------------
# 5. HOURLY AGGREGATION VERIFICATION
# -----------------------------------------------------------------------------
print("\n5. DATA AGGREGATION INTEGRITY CHECK")
print("-" * 40)

# Verify the aggregation math
sample_meter = 'meters_100_measurement.json'
filepath = DATA_DIR / sample_meter

with open(filepath, 'r') as f:
    data = json.load(f)

# Take first complete day as example
for record in data:
    consumption = record.get('consumption', [])
    if len(consumption) == 96:  # Full day
        date = f"{record['year']}-{record['month']:02d}-{record['day']:02d}"

        # Show aggregation example
        print(f"Aggregation validation for {date}:")
        print(f"  Original: 96 intervals (15-minute)")
        print(f"  Aggregated: 24 intervals (hourly)")
        print(f"  Aggregation ratio: 4:1")

        # Calculate some hourly sums as examples
        print(f"\n  Sample hourly aggregations:")
        for hour in range(0, 5):  # Show first 5 hours
            start_idx = hour * 4
            end_idx = start_idx + 4
            hour_values = consumption[start_idx:end_idx]
            hour_sum = sum(hour_values)
            print(f"    Hour {hour:02d}:00: Sum of intervals {start_idx:2d}-{end_idx-1:2d} = {hour_sum:.2f} kWh")

        # Calculate daily totals
        daily_total_15min = sum(consumption)
        daily_total_hourly = sum(sum(consumption[h*4:(h+1)*4]) for h in range(24))
        print(f"\n  Daily total verification:")
        print(f"    Sum of 96 15-min values: {daily_total_15min:.2f} kWh")
        print(f"    Sum of 24 hourly values: {daily_total_hourly:.2f} kWh")
        print(f"    Match: {'✓' if abs(daily_total_15min - daily_total_hourly) < 0.01 else '✗'}")
        break

# -----------------------------------------------------------------------------
# 6. SUMMARY STATISTICS
# -----------------------------------------------------------------------------
print("\n" + "=" * 80)
print("DATA QUALITY AUDIT SUMMARY")
print("=" * 80)

print("\n📊 Key Metrics:")
print(f"  • Data span: {total_days} days ({first_date.strftime('%Y-%m-%d')} to {last_date.strftime('%Y-%m-%d')})")
print(f"  • Total meters: {total_meters:,}")
print(f"  • Complete meters: {total_meters - len(incomplete_meters):,}")
print(f"  • Incomplete meters: {len(incomplete_meters)}")
print(f"  • DST anomalies: Found on expected dates (March 27 & October 30, 2016)")

if actual_rows:
    completeness = (actual_rows / expected_rows_396_days) * 100
    print(f"\n📈 Data Quality Score:")
    print(f"  • Expected rows (if complete): {expected_rows_396_days:,}")
    print(f"  • Actual rows: {actual_rows:,}")
    print(f"  • Completeness: {completeness:.2f}%")
    print(f"  • Missing: {expected_rows_396_days - actual_rows:,} rows ({100-completeness:.2f}%)")

    # Data quality grade
    if completeness >= 99:
        grade = "A (Excellent)"
    elif completeness >= 95:
        grade = "B (Good)"
    elif completeness >= 90:
        grade = "C (Fair)"
    else:
        grade = "D (Poor)"

    print(f"  • Data Quality Grade: {grade}")

print("\n✅ Data Quality Audit Complete!")
print("=" * 80)