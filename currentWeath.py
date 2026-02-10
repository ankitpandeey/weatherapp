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

    cursor.execute("TRUNCATE TABLE fact_daily_forecast")

    def insertWatherData(
        cityID,
        observed_at,
        collected_at,
        temp,
        feels_like,
        humidity,
        pressure,
        wind_speed,
        wind_deg,
        clouds,
        weather_main,
        weather_description,

    ):
        sql = """
            INSERT INTO weather_data
            (cityID, observed_at, collected_at, temp, feels_like, humidity,pressure, wind_speed, wind_deg, clouds, weather_main,weather_description)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """
        cursor.execute(
            sql,
            cityID, observed_at, collected_at, temp, feels_like, humidity, pressure, wind_speed, wind_deg, clouds,weather_main,weather_description
        )

    for city in cities:
        data = fetchWeatherData()
        insertWatherData(
            city["id"],
            utc_to_ist(data["current"]["dt"]),
            current_time,
            data["current"]["temp"],
            data["current"]["feels_like"],
            data["current"]["humidity"]/100,
            data["current"]["pressure"],
            data["current"]["wind_speed"],
            data["current"]["wind_deg"],
            data["current"]["clouds"],
            data["current"]["weather"]["main"],
            data["current"]["weather"]["description"]
   
        )

    conn.commit()
    cursor.close()
    conn.close()

    print("Current weather pipeline completed successfully.")


if __name__ == "__main__":
    run_current_weather_pipeline()
