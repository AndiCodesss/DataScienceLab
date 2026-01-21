#!/usr/bin/env python3
"""
Verify the merged data file for completeness and quality.
"""

import pandas as pd
from pathlib import Path
import sys

# Configuration
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
MERGED_FILE = ROOT_DIR / "data/processed/merged_data_hourly_with_weather.csv"

def verify_merged_data():
    """Comprehensive verification of the merged dataset."""

    print("=" * 80)
    print("MERGED DATA VERIFICATION")
    print("=" * 80)

    if not MERGED_FILE.exists():
        print(f"ERROR: {MERGED_FILE} not found!")
        return False

    print(f"\nReading {MERGED_FILE}...")
    print("(This may take a moment due to file size...)")

    # Read sample for initial analysis
    df_sample = pd.read_csv(MERGED_FILE, nrows=100000)

    # Get total row count
    total_rows = sum(1 for line in open(MERGED_FILE)) - 1

    print("\n1. BASIC STATISTICS")
    print("-" * 40)
    print(f"Total rows: {total_rows:,}")
    print(f"Total columns: {len(df_sample.columns)}")
    print(f"File size: {MERGED_FILE.stat().st_size / (1024**3):.2f} GB")

    print("\n2. COLUMN VERIFICATION")
    print("-" * 40)
    expected_columns = [
        # Core meter data
        "meter_id", "timestamp", "date", "interval_index",
        "consumption", "laggingReactivePower", "leadingReactivePower",

        # Weather data
        "temperature", "dew_point", "relative_humidity", "precipitation",
        "snow_depth", "wind_direction", "wind_speed", "wind_gust",
        "pressure", "sunshine", "weather_condition",

        # Time features
        "weekday", "hour", "month", "day_of_month", "is_weekend", "is_holiday",

        # Location data
        "zip_code", "sk_region_code", "region_name", "region_city",
        "latitude", "longitude", "Population",

        # Income data
        "households_income_bracket_1", "households_income_bracket_2",
        "households_income_bracket_3", "households_income_bracket_4",
        "households_income_bracket_5", "households_income_bracket_6",
        "households_income_bracket_7", "households_income_bracket_8",
        "households_income_bracket_9", "households_income_bracket_10",
        "households_income_bracket_11", "households_total"
    ]

    actual_columns = df_sample.columns.tolist()

    if set(actual_columns) == set(expected_columns):
        print("✓ All expected columns present")
    else:
        missing = set(expected_columns) - set(actual_columns)
        extra = set(actual_columns) - set(expected_columns)
        if missing:
            print(f"✗ Missing columns: {missing}")
        if extra:
            print(f"✗ Extra columns: {extra}")

    print("\n3. DATA COVERAGE")
    print("-" * 40)

    # Parse dates
    df_sample['timestamp'] = pd.to_datetime(df_sample['timestamp'])
    df_sample['date'] = pd.to_datetime(df_sample['date'])

    # Unique counts
    print(f"Unique meters: {df_sample['meter_id'].nunique()}")
    print(f"Date range: {df_sample['date'].min()} to {df_sample['date'].max()}")
    print(f"Hours per day: {df_sample['interval_index'].nunique()} (should be 24)")

    # Check for proper hourly aggregation
    hours_per_meter_day = df_sample.groupby(['meter_id', 'date']).size().value_counts().head()
    print(f"\nHours per meter-day distribution:")
    print(hours_per_meter_day)

    print("\n4. MERGE SUCCESS VERIFICATION")
    print("-" * 40)

    # Check weather data merge
    weather_cols = ['temperature', 'relative_humidity', 'wind_speed']
    weather_coverage = {}
    for col in weather_cols:
        non_null_pct = (df_sample[col].notna().sum() / len(df_sample)) * 100
        weather_coverage[col] = non_null_pct
        print(f"Weather {col}: {non_null_pct:.1f}% coverage")

    # Check location data merge
    location_cols = ['zip_code', 'latitude', 'longitude', 'region_name']
    location_coverage = {}
    for col in location_cols:
        if col in df_sample.columns:
            non_null_pct = (df_sample[col].notna().sum() / len(df_sample)) * 100
            location_coverage[col] = non_null_pct
            print(f"Location {col}: {non_null_pct:.1f}% coverage")

    # Check holiday merge
    if 'is_holiday' in df_sample.columns:
        holiday_days = df_sample[df_sample['is_holiday'] == 1]['date'].nunique()
        print(f"Holiday days found: {holiday_days}")

    # Check weekend merge
    if 'is_weekend' in df_sample.columns:
        weekend_pct = (df_sample['is_weekend'].sum() / len(df_sample)) * 100
        print(f"Weekend records: {weekend_pct:.1f}% (expected ~28.6%)")

    print("\n5. DATA QUALITY CHECKS")
    print("-" * 40)

    # Check for duplicates
    duplicate_count = df_sample.duplicated(subset=['meter_id', 'timestamp']).sum()
    print(f"Duplicate meter-timestamp pairs: {duplicate_count}")

    # Check consumption values
    print(f"Consumption range: {df_sample['consumption'].min():.2f} to {df_sample['consumption'].max():.2f}")
    print(f"Null consumption values: {df_sample['consumption'].isna().sum()}")

    # Check for negative consumption
    negative_consumption = (df_sample['consumption'] < 0).sum()
    if negative_consumption > 0:
        print(f"⚠ Warning: {negative_consumption} negative consumption values found")

    print("\n6. REGIONAL DATA COVERAGE")
    print("-" * 40)

    if 'sk_region_code' in df_sample.columns:
        region_counts = df_sample['sk_region_code'].value_counts()
        print(f"Regions found: {len(region_counts)}")
        print("\nTop regions by record count:")
        print(region_counts.head())

    print("\n7. INCOME DATA COVERAGE")
    print("-" * 40)

    income_cols = [col for col in df_sample.columns if 'households_income_bracket' in col]
    if income_cols:
        income_coverage = df_sample[income_cols[0]].notna().sum() / len(df_sample) * 100
        print(f"Income data coverage: {income_coverage:.1f}%")

    print("\n8. EXPECTED VALUES")
    print("-" * 40)

    # Calculate expected values
    expected_meters = 500  # Based on files found
    expected_days = 396  # Jan 1, 2016 to Jan 30, 2017
    expected_hours_per_day = 24
    expected_total_rows = expected_meters * expected_days * expected_hours_per_day

    print(f"Expected meters: {expected_meters}")
    print(f"Expected days: {expected_days}")
    print(f"Expected total rows: {expected_total_rows:,}")
    print(f"Actual total rows: {total_rows:,}")
    print(f"Difference: {total_rows - expected_total_rows:,} ({(total_rows/expected_total_rows - 1)*100:+.2f}%)")

    # Success criteria
    print("\n" + "=" * 80)
    print("VERIFICATION SUMMARY")
    print("=" * 80)

    success_criteria = [
        ("File exists and readable", True),
        ("All expected columns present", set(actual_columns) == set(expected_columns)),
        ("Row count reasonable", abs(total_rows - expected_total_rows) / expected_total_rows < 0.05),
        ("Weather data merged", min(weather_coverage.values()) > 50),
        ("Location data merged", min(location_coverage.values()) > 90),
        ("No duplicate timestamps", duplicate_count == 0),
        ("Hourly aggregation correct", 24 in hours_per_meter_day.index)
    ]

    all_passed = True
    for criterion, passed in success_criteria:
        status = "✓" if passed else "✗"
        print(f"{status} {criterion}")
        if not passed:
            all_passed = False

    if all_passed:
        print("\n✓✓✓ MERGE VERIFICATION SUCCESSFUL ✓✓✓")
        print("The data merge completed successfully with all major criteria met.")
    else:
        print("\n⚠ Some verification criteria not met. Review the details above.")

    return all_passed

if __name__ == "__main__":
    try:
        success = verify_merged_data()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\nERROR during verification: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)