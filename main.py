from currentWeath import run_current_weather_pipeline
from weatherForcast import forecasted_weather_pipeline
from hourlyWeather import run_hourly_weather_pipeline

def run_pipeline():
    run_current_weather_pipeline()
    forecasted_weather_pipeline()
    run_hourly_weather_pipeline()
    


if __name__ == "__main__":
    run_pipeline()