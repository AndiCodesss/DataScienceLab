import pandas as pd
from pathlib import Path

files = sorted(Path("/mnt/data").glob("meters_*_measurement.json"))
out_path = "all_meters.csv"
first = True

def read_any(path):
    try:
        return pd.read_json(path, lines=True)
    except ValueError:
        return pd.read_json(path)

for p in files:
    df = read_any(p)
    if not any(c in df.columns for c in ["meter_id","household_id"]):
        df["meter_id"] = p.stem.split("_")[1]
    df.to_csv(out_path, mode="w" if first else "a", header=first, index=False, encoding="utf-8")
    first = False

print("Wrote", out_path)
