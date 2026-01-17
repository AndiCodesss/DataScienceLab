import pandas as pd
import numpy as np

print("=" * 80)
print("EDA: MERGED DATA QUALITY CHECK")
print("=" * 80)

# Load sample for quick analysis
print("\n1. LOADING DATA...")
df = pd.read_csv("merged_data_hourly_with_weather.csv", nrows=500000)
print(f"   Sample loaded: {len(df):,} rows")

# Full row count
total = sum(1 for _ in open("merged_data_hourly_with_weather.csv")) - 1
print(f"   Total rows in file: {total:,}")

print("\n2. BASIC STATS")
print("-" * 40)
print(f"   Columns: {len(df.columns)}")
print(f"   Unique meters: {df['meter_id'].nunique()}")

print("\n3. COLUMN COVERAGE (% non-null)")
print("-" * 40)
for col in df.columns:
    pct = (df[col].notna().sum() / len(df)) * 100
    status = "✓" if pct > 90 else ("⚠" if pct > 50 else "✗")
    print(f"   {status} {col}: {pct:.1f}%")

print("\n4. REGION MAPPING CHECK")
print("-" * 40)
print("   Unique sk_region_codes:", df["sk_region_code"].dropna().unique().tolist())
print("   Unique region_names:", df["region_name"].dropna().unique().tolist())
print("   Region distribution:")
print(df["sk_region_code"].value_counts())

print("\n5. INCOME DATA CHECK")
print("-" * 40)
income_cols = [c for c in df.columns if "income_bracket" in c]
if income_cols:
    sample_income = df[income_cols[0]].dropna()
    if len(sample_income) > 0:
        print(f"   Income data present: Yes")
        print(f"   Sample values: {sample_income.head(3).tolist()}")
        print(f"   Income coverage: {len(sample_income) / len(df) * 100:.1f}%")
    else:
        print(f"   Income data present: No (all NaN)")
else:
    print("   Income columns not found")

print("\n6. WEATHER DATA CHECK")
print("-" * 40)
print(f"   Temperature range: {df['temperature'].min():.1f} to {df['temperature'].max():.1f} C")
print(f"   Temperature coverage: {df['temperature'].notna().sum() / len(df) * 100:.1f}%")

print("\n7. COORDINATES CHECK")
print("-" * 40)
lat_valid = df["latitude"].notna()
print(f"   Latitude range: {df.loc[lat_valid, 'latitude'].min():.4f} to {df.loc[lat_valid, 'latitude'].max():.4f}")
print(f"   Longitude range: {df.loc[lat_valid, 'longitude'].min():.4f} to {df.loc[lat_valid, 'longitude'].max():.4f}")
print(f"   Coordinates coverage: {lat_valid.sum() / len(df) * 100:.1f}%")

print("\n8. DUPLICATE CHECK")
print("-" * 40)
dups = df.duplicated(subset=["meter_id", "timestamp"]).sum()
print(f"   Duplicates (meter_id + timestamp): {dups}")

print("\n9. CONSUMPTION STATS")
print("-" * 40)
print(f"   Mean: {df['consumption'].mean():.3f} kWh")
print(f"   Std: {df['consumption'].std():.3f} kWh")
print(f"   Min: {df['consumption'].min():.3f} kWh")
print(f"   Max: {df['consumption'].max():.3f} kWh")
print(f"   Null values: {df['consumption'].isna().sum()}")

print("\n10. POPULATION CHECK")
print("-" * 40)
print(f"   Population coverage: {df['Population'].notna().sum() / len(df) * 100:.1f}%")
if df["Population"].notna().any():
    print(f"   Population values: {df['Population'].dropna().unique().tolist()}")

print("\n" + "=" * 80)
print("EDA COMPLETE")
print("=" * 80)
