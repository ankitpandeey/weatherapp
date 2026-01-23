
import pyodbc
import requests
import json
import os
import platform
from datetime import datetime, timezone

current_time = datetime.now(timezone.utc)
os_name = platform.system()
if os_name == "Windows":
    cert = r"C:\Users\MC823AX\ZscalerRootCertificate-2048-SHA256-Feb2025 (2).pem"

with open('mp_only.json',"r", encoding = "utf-8") as f:
    cities = json.load(f)


DRIVER="{ODBC Driver 18 for SQL Server}"
SERVER="tcp:weatherappp.database.windows.net"
DATABASE="free-sql-db-9236210;"
UID= "localhost"
PWD="A@p8103101921"

conn = pyodbc.connect(f'driver={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={UID};PWD={PWD}')


cursor = conn.cursor()

def insertWatherData(
    cityName,
    temp,
    tempMin,
    tempMax,
    humidity,
    sunrise,
    sunset,
    wind_speed,
    recorded_at,
    weather_desc,
    city_id
    ):
    sql = """
       INSERT INTO weather_data(city,temperature,temperature_min,temperature_max,humidity,sunrise,sunset,wind_speed,recorded_at,weather_desc,city_id)
       Values(?,?,?,?,?,?,?,?,?,?,?)"""
    cursor.execute(
       sql,
       cityName, temp, tempMin, tempMax, humidity, sunrise, sunset, wind_speed, recorded_at,weather_desc,city_id
    )


for city in cities:
    url = "https://api.openweathermap.org/data/2.5/weather"
    params ={
    "lat": float(city['lat']),
    "lon": float(city['lon']),
    "appid": "487ae2216ffdc9fd729cab338dbf483f"
    }
    if os_name == "Windows":
        response = requests.get(url, params = params,verify = cert)
    else:
        response = requests.get(url, params = params)
    data = response.json()
    insertWatherData(
       data["name"],
       data["main"]["temp"],
       data["main"]["temp_min"],
       data["main"]["temp_max"],
       data["main"]["humidity"],
       datetime.fromtimestamp(data["sys"]["sunrise"]) ,
       datetime.fromtimestamp(data["sys"]["sunset"]),
       data["wind"]["speed"],
       current_time,
       data["weather"][0]["description"],
       data["id"]
    )


conn.commit()
cursor.close()
conn.close()




