import csv
import logging
import time
from datetime import date
from itertools import zip_longest
from pathlib import Path
from typing import Iterable, Tuple

import requests

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
ZIP_DATA_FILE = Path("./data/sk_zip_coordinates_clean.csv")
OUTPUT_FILE = Path("./data/sk_zip_weather_2016.csv")
HOURLY_VARS = ["temperature_2m", "relativehumidity_2m", "precipitation"]
MAX_RETRIES = 5
BASE_SLEEP = 2.0
SESSION = requests.Session()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def iter_zip_coordinates(path: Path) -> Iterable[Tuple[str, float, float]]:
    with path.open(newline="", encoding="utf-8") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            yield row["zip_code"], float(row["latitude"]), float(row["longitude"])


def _sleep(attempt: int, retry_after: float | None) -> None:
    delay = retry_after if retry_after else BASE_SLEEP * (2 ** (attempt - 1))
    logger.info("Retrying in %.1f seconds", delay)
    time.sleep(delay)


def fetch_weather(latitude: float, longitude: float) -> dict:
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": date(2016, 1, 1).isoformat(),
        "end_date": date(2016, 12, 31).isoformat(),
        "hourly": HOURLY_VARS,
        "timezone": "UTC",
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = SESSION.get(ARCHIVE_URL, params=params, timeout=60)
            if response.status_code == 429:
                _sleep(attempt, _retry_after(response))
                continue
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as err:
            if response.status_code >= 500 and attempt < MAX_RETRIES:
                logger.warning("Server error %s on (%s, %s)", err, latitude, longitude)
                _sleep(attempt, None)
                continue
            raise
        except requests.RequestException as err:
            if attempt == MAX_RETRIES:
                raise
            logger.warning("Network error %s on (%s, %s)", err, latitude, longitude)
            _sleep(attempt, None)
    raise RuntimeError("Max retries exceeded")


def _retry_after(response: requests.Response) -> float | None:
    try:
        return float(response.headers.get("Retry-After", ""))
    except (TypeError, ValueError):
        return None


def write_enriched_rows(writer: csv.DictWriter, zip_code: str, lat: float, lon: float, data: dict) -> None:
    hourly = data.get("hourly", {})
    rows = zip_longest(
        hourly.get("time", []),
        hourly.get("temperature_2m", []),
        hourly.get("relativehumidity_2m", []),
        hourly.get("precipitation", []),
        fillvalue="",
    )
    for ts, temp, rh, prec in rows:
        writer.writerow(
            {
                "zip_code": zip_code,
                "longitude": lon,
                "latitude": lat,
                "time": ts,
                "temperature_2m": temp,
                "relativehumidity_2m": rh,
                "precipitation": prec,
            }
        )


if __name__ == "__main__":
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as fp:
        fieldnames = ["zip_code", "longitude", "latitude", "time", *HOURLY_VARS]
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for zip_code, lat, lon in iter_zip_coordinates(ZIP_DATA_FILE):
            weather = fetch_weather(lat, lon)
            write_enriched_rows(writer, zip_code, lat, lon, weather)