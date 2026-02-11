import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
import platform
import json

cert = r"C:\Users\MC823AX\ZscalerRootCertificate-2048-SHA256-Feb2025 (2).pem"
def fetchWeatherData(lat, lon):
    load_dotenv()
    API_KEY = os.getenv("OPENWEATHER_API_KEY")
    os_name = platform.system()
    url = "https://api.openweathermap.org/data/3.0/onecall?"
    params ={
            "lat": lat,
            "lon": lon,
            "exclude":"minutely",
            "appid": API_KEY,
            "units" : "metric"
}
          
    if os_name == "Windows":
        response = requests.get(url, params = params, verify = cert)
    else:
        response = requests.get(url, params = params)
    data = response.json()
    return data