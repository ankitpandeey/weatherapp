
import pyodbc
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from text_utils import clean_text
from utcToLocalTime import utc_to_ist
from fetchWeather import fetchWeatherData

data = fetchWeatherData
def forecasted_weather_pipeline():
    load_dotenv()
    current_time = utc_to_ist(datetime.now(timezone.utc))
    DRIVER="{ODBC Driver 18 for SQL Server}"
    SERVER = os.getenv("UID")
    DATABASE =os.getenv("DATABASE")
    UID = os.getenv("SERVER")
    PWD = os.getenv("PWD")

    conn = pyodbc.connect(f'driver={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={UID};PWD={PWD}')
    cursor = conn.cursor()
    cursor.execute("""TRUNCATE TABLE forecasted_weather """)

    def insertForecastedWeatherData(
            cityID,
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
    ):
        sql = """
        INSERT INTO fact_daily_forecast(cityID, forecast_date,collected_at,temp_min,temp_max,humidity,wind_speed,clouds,pop,sunrise_time,sunset_time,weather_main,weather_description)
        Values(?,?,?,?,?,?,?,?,?,?,?,?,?)"""
        cursor.execute(
        sql,
        cityID, forecast_date,collected_at, temp_min, temp_max, humidity, wind_speed, clouds, pop,sunrise_time,sunset_time,weather_main,weather_description
        )


    for city in data:
       for i in range(1,4):
        insertForecastedWeatherData(
        city["id"],
        utc_to_ist(city["daily"][i]["dt"]),
        current_time,
        city["daily"][i]["temp"]["min"],
        city["daily"][i]["temp"]["max"],
        city["daily"][i]["humidity"],
        city["daily"][i]["wind_speed"],
        city["daily"][i]["clouds"],
        city["daily"][i]["pop"],
        utc_to_ist(city["daily"][i]["sunrise"]),
        utc_to_ist(city["daily"][i]["sunset"]),
        city["daily"][i]["weather"][i]["main"],
        city["daily"][i]["weather"][i]["description"]
        )
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
  forecasted_weather_pipeline()

