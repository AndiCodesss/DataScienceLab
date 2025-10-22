# save as: sk_holidays_2016.py
import requests, csv, datetime as dt

# for Slovakia (SK) and year 2016
url = "https://date.nager.at/api/v3/PublicHolidays/2016/SK"

res = requests.get(url)
data = res.json()  # list of holidays

with open("sk_holidays_2016.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["date", "weekday", "localName", "name", "types", "is_weekend"])
    for h in data:
        d = dt.datetime.strptime(h["date"], "%Y-%m-%d").date()
        weekday = d.strftime("%A")
        is_weekend = 1 if weekday in ("Saturday", "Sunday") else 0
        # handle both "types" (new API) or "type" (older)
        types = h.get("types") or ([h.get("type")] if h.get("type") else [])
        w.writerow([h["date"], weekday, h.get("localName",""), h.get("name",""), ";".join(types), is_weekend])

print("Saved sk_holidays_2016.csv")
