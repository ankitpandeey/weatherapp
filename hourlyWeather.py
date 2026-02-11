import pyodbc
import requests
import json
import os
import platform
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from text_utils import clean_text
from utcToLocalTime import utc_to_ist
from fetchWeather import fetchWeatherData

def run_hourly_weather_pipeline():
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

    cursor.execute("TRUNCATE TABLE fact_hourly_forecast")

    def insertWatherData(
        city_id,
        forecast_hour,
        collected_at,
        temp,
        feels_like,
        humidity,
        pressure,
        wind_speed,
        clouds,
        pop,
        weather_main,
        weather_description,

    ):
        sql = """
            INSERT INTO fact_hourly_forecast
            (city_id, forecast_hour, collected_at, temp, feels_like, humidity,pressure, wind_speed,clouds, pop,weather_main,weather_description)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """
        cursor.execute(
            sql,
            city_id, forecast_hour, collected_at, temp, feels_like, humidity, pressure, wind_speed, clouds, pop, weather_main,weather_description
        )

    for city in cities:
       Data = fetchWeatherData(city["lat"], city["lon"])
       for i in range(0,4):
        insertWatherData(
        city["id"],
        utc_to_ist(Data["hourly"][i]["dt"]),
        current_time,
        Data["hourly"][i]["temp"],
        Data["hourly"][i]["feels_like"],
        Data["hourly"][i]["humidity"],
        Data["hourly"][i]["pressure"],
        Data["hourly"][i]["wind_speed"],
        Data["hourly"][i]["clouds"],
        Data["hourly"][i]["pop"],
        Data["daily"][i]["weather"][0]["main"],
        Data["daily"][i]["weather"][0]["description"],
        )

    conn.commit()
    cursor.close()
    conn.close()

    print("Hourly weather forecast pipeline completed successfully.")


if __name__ == "__main__":
    run_hourly_weather_pipeline()
