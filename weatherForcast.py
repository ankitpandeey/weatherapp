
import pyodbc
import requests
import json
import os
import platform
from datetime import datetime, timezone
from dotenv import load_dotenv
from text_utils import clean_text
from utcToLocalTime import utc_to_ist

def forecasted_weather_pipeline():
    load_dotenv()
    current_time = datetime.now(timezone.utc)
    API_KEY = os.getenv("OPENWEATHER_API_KEY")
    UID = os.getenv("UID")
    PWD = os.getenv("PWD")
    SERVER=os.getenv("SERVER")
    DATABASE=os.getenv("DATABASE")
    os_name = platform.system()
    if os_name == "Windows":
        cert = r"C:\Users\MC823AX\ZscalerRootCertificate-2048-SHA256-Feb2025 (2).pem"

    with open('mp_only.json',"r", encoding = "utf-8") as f:
        cities = json.load(f)

    DRIVER="{ODBC Driver 18 for SQL Server}"
    SERVER = SERVER
    DATABASE = DATABASE
    UID = UID
    PWD = PWD

    conn = pyodbc.connect(f'driver={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={UID};PWD={PWD}')
    cursor = conn.cursor()
    cursor.execute("""TRUNCATE TABLE forecasted_weather """)

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
        INSERT INTO forecasted_weather(cityID,cityName,temp_min_day1,temp_max_day1,temp_min_day2,temp_max_day2,temp_min_day3,temp_max_day3,temp_min_day4,temp_max_day4,day0,day1,day2,day3,day4)
        Values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
        cursor.execute(
        sql,
        cityID, cityName, temp_min_day1, temp_max_day1, temp_min_day2, temp_max_day2, temp_min_day3, temp_max_day3, temp_min_day4,temp_max_day4,recorded_at,day1,day2,day3,day4
        )


    for city in cities:
        url = "https://api.openweathermap.org/data/3.0/onecall?"
        params ={
        "lat": float(city['lat']),
        "lon": float(city['lon']),
        "exclude":["hourly","minutely","current"],
        "appid": API_KEY

        }
        if os_name == "Windows":
            response = requests.get(url, params = params,verify = cert)
        else:
            response = requests.get(url, params = params)
        data = response.json()
        insertForecastedWeatherData(
        city["id"],
        clean_text(city["name"]),
        data["daily"][0]["temp"]["min"] -273.15,
        data["daily"][0]["temp"]["max"] -273.15,
        data["daily"][1]["temp"]["min"] -273.15,
        data["daily"][1]["temp"]["max"] -273.15,
        data["daily"][2]["temp"]["min"] -273.15,
        data["daily"][2]["temp"]["max"] -273.15,
        data["daily"][3]["temp"]["min"] -273.15,
        data["daily"][3]["temp"]["max"] -273.15,
        utc_to_ist(data["daily"][0]["dt"]),
        utc_to_ist(data["daily"][1]["dt"]),
        utc_to_ist(data["daily"][2]["dt"]),
        utc_to_ist(data["daily"][3]["dt"]),
        utc_to_ist(data["daily"][4]["dt"])
        )
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
  forecasted_weather_pipeline()

