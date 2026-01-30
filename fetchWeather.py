import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
import platform
import json

cert = r"C:\Users\MC823AX\ZscalerRootCertificate-2048-SHA256-Feb2025 (2).pem"
def fetchWeatherData():
    load_dotenv()
    API_KEY = os.getenv("OPENWEATHER_API_KEY")
    os_name = platform.system()
    with open('mp_only.json',"r", encoding = "utf-8") as f:
        cities = json.load(f)
        for city in cities:
            url = "https://api.openweathermap.org/data/3.0/onecall?"
            params ={
            "lat": float(city['lat']),
            "lon": float(city['lon']),
            "exclude":"minutely",
            "appid": API_KEY

            }
            if os_name == "Windows":
                response = requests.get(url, params = params, verify = cert)
            else:
                response = requests.get(url, params = params)
            data = response.json()
            return data