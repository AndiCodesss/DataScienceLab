# dataloader.py — minimal & readable
# - reads meter JSON files from mergingtest/
# - merges meter_info.csv (by meter_id)
# - merges sk_holidays_2016.csv (date -> is_holiday)
# - adds is_weekend and weekday (Mon–Sun)
import pandas as pd
from pathlib import Path

# --- paths -------------------------------------------------------------------
DATA_DIR    = Path("mergingtest")
INFO_CSV    = Path("./data/meter_info.csv")
HOLIDAY_CSV = Path("./data/sk_holidays_2016.csv")
OUT_CSV     = "data/raw_data.csv"

# --- meter info --------------------------------------------------------------
if not INFO_CSV.exists():
    raise SystemExit("meter_info.csv not found next to this script.")

info = pd.read_csv(INFO_CSV)

# Try a few common ID names, rename to 'meter_id'
if "meter_id" not in info.columns:
    for alt in ["meterID", "household_id", "id"]:
        if alt in info.columns:
            info = info.rename(columns={alt: "meter_id"})
            break
if "meter_id" not in info.columns:
    raise SystemExit("No meter_id in meter_info.csv (looked for meter_id/meterID/household_id/id).")

info["meter_id"] = info["meter_id"].astype(str)
info = info.drop_duplicates(subset=["meter_id"]).reset_index(drop=True)

# --- holidays -> keep ONLY (date, is_holiday) --------------------------------
if not HOLIDAY_CSV.exists():
    raise SystemExit("sk_holidays_2016.csv not found next to this script.")

hol = pd.read_csv(HOLIDAY_CSV, parse_dates=["date"])
hol["date"] = hol["date"].dt.date
hol = hol[["date"]].drop_duplicates()
hol["is_holiday"] = 1  # 1 on holiday dates

# --- source files ------------------------------------------------------------
files = sorted(
    DATA_DIR.glob("meters_*_measurement.json"),
    key=lambda p: int(p.stem.split("_")[1])  # meters_<ID>_measurement.json
)
if not files:
    raise SystemExit("No files like mergingtest/meters_*_measurement.json found.")

# --- helper to expand one JSON file -----------------------------------------
def expand_file(fp: Path) -> pd.DataFrame:
    """Turn one meter JSON file (days with 15-min arrays) into 15-min rows."""
    days = pd.read_json(fp)
    rows = []

    for _, rec in days.iterrows():
        # basic date + meter
        year  = int(rec.get("year", 2016))
        month = int(rec.get("month", 1))
        day   = int(rec.get("day", 1))
        meter = str(rec.get("meterID", fp.stem.split("_")[1]))
        base  = pd.Timestamp(year, month, day)

        # split list-fields vs scalar-fields
        arrays  = {k: v for k, v in rec.items() if isinstance(v, (list, tuple))}
        scalars = {k: v for k, v in rec.items() if k not in arrays}
        for k in ["meterID", "year", "month", "day"]:
            scalars.pop(k, None)

        n = max((len(v) for v in arrays.values()), default=0)
        if n == 0:
            continue

        # build 15-min rows
        for i in range(n):
            row = dict(scalars)
            for k, v in arrays.items():
                row[k] = v[i] if i < len(v) else None
            row["meter_id"]  = meter
            row["slot_15m"]  = i
            row["timestamp"] = base + pd.Timedelta(minutes=15 * i)
            rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # sort, attach meter info
    df = df.sort_values("timestamp").reset_index(drop=True)
    df["meter_id"] = df["meter_id"].astype(str)
    df = df.merge(info, on="meter_id", how="left")

    # date for joining holidays
    df["date"] = df["timestamp"].dt.date
    df = df.merge(hol, on="date", how="left")
    df["is_holiday"] = df["is_holiday"].fillna(0).astype(int)

    # weekend + weekday (Mon–Sun)
    df["is_weekend"] = (df["timestamp"].dt.weekday >= 5).astype(int)
    df["weekday"]    = df["timestamp"].dt.day_name().str[:3]  # Mon, Tue, ...

    # put key columns first
    first = [c for c in ["meter_id", "timestamp", "date", "weekday", "is_weekend", "is_holiday", "slot_15m"] if c in df.columns]
    rest  = [c for c in df.columns if c not in first]
    return df[first + rest]

# --- write output incrementally (low memory) ---------------------------------
first_chunk = True
for fp in files:
    part = expand_file(fp)
    if part.empty:
        print(f"Skipped {fp.name} (no rows)")
        continue
    part.to_csv(OUT_CSV, mode="w" if first_chunk else "a",
                header=first_chunk, index=False, encoding="utf-8")
    first_chunk = False
    print(f"Added {fp.name}: {len(part)} rows")

print(f"Done. Wrote {OUT_CSV}")
