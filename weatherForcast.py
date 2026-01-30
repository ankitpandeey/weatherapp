
import pyodbc
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from utcToLocalTime import utc_to_ist
from fetchWeather import fetchWeatherData
import json

def forecasted_weather_pipeline():
    with open('mp_only.json', "r", encoding="utf-8") as f:
        cities = json.load(f)
    load_dotenv()
    current_time = datetime.now(timezone.utc)
    DRIVER="{ODBC Driver 18 for SQL Server}"
    SERVER = os.getenv("SERVER")
    DATABASE =os.getenv("DATABASE")
    UID = os.getenv("UID")
    PWD = os.getenv("PWD")

    conn = pyodbc.connect(f'driver={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={UID};PWD={PWD}')
    cursor = conn.cursor()
    cursor.execute("""TRUNCATE TABLE fact_daily_forecast""")

    def insertForecastedWeatherData(
            city_id,
            forecast_date,
            collected_at,
            temp_min,
            temp_max,
            humidity, 
            wind_speed,
            clouds, 
            pop,
            sunrise_time,
            sunset_time,
            weather_main,
            weather_description,
            icon_id
    ):
        sql = """
        INSERT INTO fact_daily_forecast(city_id, forecast_date,collected_at,temp_min,temp_max,humidity,wind_speed,clouds,pop,sunrise_time,sunset_time,weather_main,weather_description,icon_id)
        Values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
        cursor.execute(
        sql,
        city_id, forecast_date,collected_at, temp_min, temp_max, humidity, wind_speed, clouds, pop,sunrise_time,sunset_time,weather_main,weather_description,icon_id
        )


    for city in cities:
       Data = fetchWeatherData()
       for i in range(1,5):
        insertForecastedWeatherData(
        city["id"],
        utc_to_ist(Data["daily"][i]["dt"]),
        current_time,
        Data["daily"][i]["temp"]["min"],
        Data["daily"][i]["temp"]["max"],
        Data["daily"][i]["humidity"],
        Data["daily"][i]["wind_speed"],
        Data["daily"][i]["clouds"],
        Data["daily"][i]["pop"],
        utc_to_ist(Data["daily"][i]["sunrise"]),
        utc_to_ist(Data["daily"][i]["sunset"]),
        Data["daily"][i]["weather"][0]["main"],
        Data["daily"][i]["weather"][0]["description"],
        Data["daily"][i]["weather"][0]["icon"]
        )
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
  forecasted_weather_pipeline()

