import requests
import json
import pandas as pd
from datetime import datetime, timezone
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Load cities
with open(r"C:\Users\MC823AX\OneDrive - EY\Documents\Power BI Learning\Weather App\mp_only.json", "r", encoding="utf-8") as f:
    cities = json.load(f)

url = "https://api.openweathermap.org/data/2.5/weather"

rows = []

current_time = datetime.now(timezone.utc).isoformat()
session = requests.Session()

retry_strategy = Retry(
    total=5,
    backoff_factor=1,  # 1s, 2s, 4s, 8s, ...
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)

adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)

for city in cities:
    params = {
        "lat": float(city['lat']),
        "lon": float(city['lon']),
        "appid": "487ae2216ffdc9fd729cab338dbf483f",
        "units": "metric" }
    try:
        response = session.get(
            url,
            params=params,
            verify=r"C:\Users\MC823AX\ZscalerRootCertificate-2048-SHA256-Feb2025 (2).pem",
            timeout=20
        )
        data = response.json()
    except Exception as e:
       print(f"Failed for city {city}: {e}")
       continue

    row = {
        "city": data["name"],
        "lat": data["coord"]["lat"],
        "lon": data["coord"]["lon"],
        "weather": data["weather"][0]["description"],
        "temp": data["main"]["temp"],
        "temp_min": data["main"]["temp_min"],
        "temp_max": data["main"]["temp_max"],
        "humidity": data["main"]["humidity"],
        "wind_speed": data["wind"]["speed"],
        "recorded_at": current_time
    }

    rows.append(row)

# This is what Power BI will import
df = pd.DataFrame(rows)

df
