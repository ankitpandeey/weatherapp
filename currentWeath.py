import pyodbc
import requests
import json
import os
import platform
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from text_utils import clean_text
from utcToLocalTime import utc_to_ist

def run_current_weather_pipeline():
    load_dotenv()
    IST = timezone(timedelta(hours=5, minutes=30))
    current_time = datetime.now(timezone.utc).astimezone(IST)
    os_name = platform.system()
    cert = None
    if os_name == "Windows":
        cert = r"C:\Users\MC823AX\ZscalerRootCertificate-2048-SHA256-Feb2025 (2).pem"

    with open('mp_only.json', "r", encoding="utf-8") as f:
        cities = json.load(f)

    DRIVER = "{ODBC Driver 18 for SQL Server}"
    API_KEY = os.getenv("OPENWEATHER_API_KEY")
    UID = os.getenv("UID")
    PWD = os.getenv("PWD")
    SERVER=os.getenv("SERVER")
    DATABASE=os.getenv("DATABASE")

    conn = pyodbc.connect(
        f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={UID};PWD={PWD}"
    )
    cursor = conn.cursor()

    cursor.execute("TRUNCATE TABLE weather_data")

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
            INSERT INTO weather_data
            (city, temperature, temperature_min, temperature_max, humidity, sunrise, sunset, wind_speed, recorded_at, weather_desc, city_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """
        cursor.execute(
            sql,
            cityName, temp, tempMin, tempMax, humidity, sunrise, sunset, wind_speed, recorded_at, weather_desc, city_id
        )

    for city in cities:
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {
            "lat": float(city["lat"]),
            "lon": float(city["lon"]),
            "appid": API_KEY
        }

        if os_name == "Windows" and cert:
            response = requests.get(url, params=params, verify=cert, timeout=20)
        else:
            response = requests.get(url, params=params, timeout=20)

        data = response.json()

        insertWatherData(
            clean_text(city["name"]),
            data["main"]["temp"] - 273.15,
            data["main"]["temp_min"] - 273.15,
            data["main"]["temp_max"] - 273.15,
            data["main"]["humidity"] / 100,
            utc_to_ist(data["sys"]["sunrise"]),
            utc_to_ist(data["sys"]["sunset"]),
            data["wind"]["speed"],
            current_time,
            data["weather"][0]["description"],
            city["id"]
        )

    conn.commit()
    cursor.close()
    conn.close()

    print("Current weather pipeline completed successfully.")


if __name__ == "__main__":
    run_current_weather_pipeline()
