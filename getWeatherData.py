import requests
import json
import sqlite3
from datetime import datetime, timezone
import os

BASE_DIR = r"C:\Users\MC823AX\OneDrive - EY\Documents\Power BI Learning\Weather App"
DB_PATH = os.path.join(BASE_DIR, "weatherData.db")

current_time = datetime.now(timezone.utc)


with open("mp_only.json", "r", encoding="utf-8") as f:
 cities = json.load(f)

connection = sqlite3.Connection("weatherData.db")
cursor = connection.cursor()

def insertWatherData(city, desc, temp, tempMin, tempMax, humidity, wind_speed,sunrise, sunset, timezone,recorded_at):
  cursor.execute("""
        SELECT 1
        FROM weather_data
        WHERE city = ?
          AND recorded_at >= datetime('now', '-1 hour')
    """, (city,))
  exists = cursor.fetchone()
  if exists:
        print(f"Skipping insert for {city} (already exists in last 1 hour)")
        return
  cursor.execute("INSERT INTO weather_data VALUES(:name, :Weather_description,:temp,:temp_min,:temp_max,:humidity,:wind_spee,:sunrise,:sunset,:timezone)",{"name":city, "Weather_description": desc,"temp" :temp,"temp_min" :tempMin,"temp_max":tempMax,"humidity" :humidity,"wind_spee" : wind_speed, "sunrise":sunrise,"sunset" :sunset,"timezone":timezone})
  
url = "https://api.openweathermap.org/data/2.5/weather"

for city in cities:
    params ={
    "lat": float(city['lat']),
    "lon": float(city['lon']),
    "appid": "487ae2216ffdc9fd729cab338dbf483f"
    }

    response = requests.get(url,params=params,verify=r"C:\Users\MC823AX\ZscalerRootCertificate-2048-SHA256-Feb2025 (2).pem")
    data = response.json()
    insertWatherData(
    data['name'],
    data['weather'][0]['description'],
    data['main']['temp'],
    data['main']['temp_min'],
    data['main']['temp_max'],
    data['main']['humidity'],
    data['wind']['speed'],
    data['sys']['sunrise'],
    data['sys']['sunset'],
    data['timezone'],
    current_time
    )

connection.commit()
connection.close()
