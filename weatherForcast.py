
import pyodbc
import requests
import json
import os
import platform
from datetime import datetime, timezone
from dotenv import load_dotenv


load_dotenv()
current_time = datetime.now(timezone.utc)
API_KEY = os.getenv("OPENWEATHER_API_KEY")
UID = os.getenv("UID"),
SERVER = os.getenv("SERVER")
DATABASE = os.getenv("DATABASE")
PWD = os.getenv("PWD")
os_name = platform.system()
if os_name == "Windows":
    cert = r"C:\Users\MC823AX\ZscalerRootCertificate-2048-SHA256-Feb2025 (2).pem"

with open('mp_only.json',"r", encoding = "utf-8") as f:
    cities = json.load(f)


DRIVER="{ODBC Driver 18 for SQL Server}"
SERVER= SERVER
DATABASE=DATABASE
UID= UID
PWD=PWD

conn = pyodbc.connect(f'driver={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={UID};PWD={PWD}')


cursor = conn.cursor()

def insertForecastedWeatherData(
        cityID,
        cityName,
        temp_min_day1,
        temp_max_day1,
        temp_min_day2,
        temp_max_day2, 
        temp_min_day3,
        temp_max_day3, 
        temp_min_day4,
        temp_max_day4,
        recorded_at,
        day1,
        day2,
        day3,
        day4
   ):
    sql = """
       INSERT INTO forecasted_weather(cityID,cityName,temp_min_day1,temp_max_day1,temp_min_day2,temp_max_day2,temp_min_day3,temp_max_day3,temp_min_day4,temp_max_day4,recorded_at,day1,day2,day3,day4)
       Values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
    cursor.execute(
       sql,
       cityID, cityName, temp_min_day1, temp_max_day1, temp_min_day2, temp_max_day2, temp_min_day3, temp_max_day3, temp_min_day4,temp_max_day4,recorded_at,day1,day2,day3,day4
    )


for city in cities:
    url = "https://api.openweathermap.org/data/2.5/forecast/daily"
    params ={
    "lat": float(city['lat']),
    "lon": float(city['lon']),
    "appid": API_KEY,
    "cnt" : 4
    }
    if os_name == "Windows":
        response = requests.get(url, params = params,verify = cert)
    else:
        response = requests.get(url, params = params)
    data = response.json()
    print(data)
    break
    # insertForecastedWeatherData(
    #    data["city"]["id"],
    #    data["city"]["name"],
    #    data["list"][0]["temp"]["min"],
    #    data["list"][0]["temp"]["max"],
    #    data["list"][1]["temp"]["min"],
    #    data["list"][1]["temp"]["max"],
    #    data["list"][2]["temp"]["min"],
    #    data["list"][2]["temp"]["max"],
    #    data["list"][3]["temp"]["min"],
    #    data["list"][3]["temp"]["max"],
    #    current_time,
    #    data["list"][0]["dt"],
    #    data["list"][1]["dt"],
    #    data["list"][2]["dt"],
    #    data["list"][3]["dt"]
    #    )
    # break

conn.commit()
cursor.close()
conn.close()




