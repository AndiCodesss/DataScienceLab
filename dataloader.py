import pandas as pd
from pathlib import Path

# 1) where the files are
DATA_DIR = Path("mergingtest")
INFO_CSV = Path("meter_info.csv") 

# 2) load meter info and make sure the ID column is called 'meter_id' (as string)
if not INFO_CSV.exists():
    raise SystemExit("meter_info.csv not found. Put it next to this script.")
info = pd.read_csv(INFO_CSV)

# try to find the id column and rename it to meter_id
if "meter_id" not in info.columns:
    for alt in ["meterID", "MeterID", "meter", "household_id", "id"]:
        if alt in info.columns:
            info = info.rename(columns={alt: "meter_id"})
            break
if "meter_id" not in info.columns:
    raise SystemExit("Could not find an ID column in meter_info.csv (looked for meter_id/meterID/household_id/id).")

# keep only the first row per meter_id if there are duplicates
info["meter_id"] = info["meter_id"].astype(str)
info = info.drop_duplicates(subset=["meter_id"], keep="first").reset_index(drop=True)

# sort files by meter number, e.g. meters_1_, meters_2_, ... meters_1000_
files = sorted(
    DATA_DIR.glob("meters_*_measurement.json"),
    key=lambda p: int(p.stem.split("_")[1])  # meters_<ID>_measurement.json
)
if not files:
    raise SystemExit("No files found like mergingtest/meters_*_measurement.json")

out_csv = "raw_data.csv"
first_file = True  # write header once

def expand_one_file(fp: Path) -> pd.DataFrame:
    # Each file: list of days; each day has arrays of length 96 (15-min values)
    day_table = pd.read_json(fp)
    rows = []

    for _, day in day_table.iterrows():
        # figure out which keys are arrays vs scalars
        arr_keys = [k for k in day.index if isinstance(day[k], (list, tuple))]
        scalars  = {k: day[k] for k in day.index if k not in arr_keys}

        # read date + meter to make timestamps
        year  = int(day.get("year", 2016))
        month = int(day.get("month", 1))
        daynum= int(day.get("day", 1))
        meter = str(day.get("meterID", fp.stem.split("_")[1]))

        # drop duplicate ID/date columns (we'll keep meter_id + timestamp instead)
        for k in ["meterID", "year", "month", "day"]:
            if k in scalars:
                scalars.pop(k)

        # number of 15-min slots (usually 96)
        n = max((len(day[k]) for k in arr_keys), default=0)
        if n == 0:
            continue

        base = pd.Timestamp(year, month, daynum)
        for i in range(n):
            rec = dict(scalars)  # copy all daily (scalar) fields
            # fill array fields for this 15-min slot
            for k in arr_keys:
                v = day[k]
                rec[k] = v[i] if i < len(v) else None
            rec["meter_id"]  = meter
            rec["slot_15m"]  = i               # 0..95
            rec["timestamp"] = base + pd.Timedelta(minutes=15*i)
            rows.append(rec)

    # make DataFrame and put key columns first
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.sort_values(["timestamp"]).reset_index(drop=True)  # time order inside this meter
    cols_first = [c for c in ["meter_id", "timestamp", "slot_15m"] if c in df.columns]
    other_cols = [c for c in df.columns if c not in cols_first]
    df = df[cols_first + other_cols]

    # MERGE meter_info columns onto this chunk (by meter_id)
    df["meter_id"] = df["meter_id"].astype(str)
    df = df.merge(info, on="meter_id", how="left")

    return df

# 3) build the CSV by appending meter by meter (keeps memory low)
for fp in files:
    part = expand_one_file(fp)
    if part.empty:
        print(f"Skipped {fp.name} (no rows)")
        continue
    part.to_csv(out_csv, mode="w" if first_file else "a",
                header=first_file, index=False, encoding="utf-8")
    first_file = False
    print(f"Added {fp.name}: {len(part)} rows")

print(f"✅ Done. Wrote {out_csv}")
