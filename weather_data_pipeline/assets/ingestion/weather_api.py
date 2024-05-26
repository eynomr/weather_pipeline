from dagster import (
    asset, 
    op,
    AssetMaterialization, 
    AssetExecutionContext,
    Output, 
    Failure
    )
from ...resources import OpenWeatherMapResource, PostgresResource
import pandas as pd
import datetime
import requests
import os


@asset(
        compute_kind="python",
        description="Fetches all locations from the database"
)
def all_locations(postgres: PostgresResource) -> pd.DataFrame:
    data = postgres.execute_query("SELECT location_id, latitude, longitude FROM public_analytics.dim_location")
    yield AssetMaterialization(asset_key="all_locations", description="Fetched all locations from the database")
    yield Output(value=data, metadata={"Number of Locations" : len(data)})


@asset(
        compute_kind="python",
        description="Fetches weather data from OpenWeather API"
)
def fetch_weather_actual(context: AssetExecutionContext, all_locations, open_weather_map: OpenWeatherMapResource) -> pd.DataFrame:
    weather_data = []
    for location in all_locations:
        location_id, lat, long = location
        context.log.info(location)
        try:
            response = open_weather_map.get_actual_weather(lat=lat, long=long)
            response.raise_for_status()            
            data = response.json()
            weather_data.append({
                "location_id": location_id,
                "weather_type": data['weather'][0]['main'],
                "weather_description": data['weather'][0]['description'],                
                "temp": data['main']['temp'],
                "feels_like": data['main']['feels_like'],
                "temp_min": data['main']['temp_min'],
                "temp_max": data['main']['temp_max'],
                "pressure": data['main']['pressure'],
                "humidity": data['main']['humidity'],
                "sea_level": data.get('main', {}).get('sea_level', None), 
                "ground_level": data.get('main', {}).get('grnd_level', None),
                "visibility": data['visibility'],
                "wind_speed": data['wind']['speed'],
                "wind_degree": data['wind']['deg'],
                "wind_gust": data.get('wind', {}).get('gust', None),
                "cloudiness": data.get('clouds', {}).get('all', None),
                "rain_1h": data.get('rain', {}).get('1h', None),
                "rain_3h": data.get('rain', {}).get('3h', None),
                "snow_1h": data.get('snow', {}).get('1h', None),
                "snow_3h": data.get('snow', {}).get('3h', None),
                "observation_datetime": data['dt'],
                "sunrise": data['sys']['sunrise'],
                "sunset": data['sys']['sunset'],
            })
        except Exception as e:
            raise Failure(f"Failed to fetch locations: {str(e)}")
        
    yield AssetMaterialization(asset_key="weather_actual", description="Fetched weather data from OpenWeather API")
    yield Output(value=weather_data, metadata={"Number of records" : len(weather_data), "Sample Data": weather_data[:5]})

@asset(
        compute_kind="python",
        description="Loads raw weather data into the database"
)
def raw_weather_actual(context: AssetExecutionContext, fetch_weather_actual, postgres: PostgresResource):
    query = """
        INSERT INTO public_stage.raw_weather_actual(
            location_id, 
            weather_type, 
            weather_description, 
            temp, 
            feels_like, 
            temp_min, 
            temp_max, 
            pressure, 
            humidity, 
            sea_level, 
            ground_level, 
            visibility, 
            wind_speed, 
            wind_degree, 
            wind_gust, 
            cloudiness,
            rain_1h,
            rain_3h,
            snow_1h,
            snow_3h,
            observation_datetime, 
            sunrise, 
            sunset
        ) VALUES (
            %(location_id)s, 
            %(weather_type)s,
            %(weather_description)s,
            %(temp)s,
            %(feels_like)s,
            %(temp_min)s,
            %(temp_max)s,
            %(pressure)s,
            %(humidity)s,
            %(sea_level)s,
            %(ground_level)s,
            %(visibility)s,
            %(wind_speed)s,
            %(wind_degree)s,
            %(wind_gust)s,
            %(cloudiness)s,
            %(rain_1h)s,
            %(rain_3h)s,
            %(snow_1h)s,
            %(snow_3h)s,
            to_timestamp(%(observation_datetime)s),
            to_timestamp(%(sunrise)s),
            to_timestamp(%(sunset)s)
        )
    """
    for data in fetch_weather_actual:
        try:
            postgres.execute_query(query, data)
        except Exception as e:
            raise Failure(f"Failed to load data into the database: {str(e)}")
    yield AssetMaterialization(asset_key="raw_weather_actual", description="Loaded raw weather data into the database")
    yield Output(value=fetch_weather_actual, metadata={"Number of records" : len(fetch_weather_actual), "Sample Data": fetch_weather_actual[:5]})
    
# @asset
# def fetch_weather_forecast(open_weather_map: OpenWeatherMapResource) -> pd.DataFrame:
#     try:
#         response = open_weather_map.get_forecast_weather(lat=40.7128, long=-74.0060)
#         response.raise_for_status()

#         data = response.json()

#         df = pd.DataFrame([{
#             "weather_id": data['list'][0]['weather'][0]['id'],
#             "temp": data['list'][0]['main']['temp'],
#             "feels_like": data['list'][0]['main']['feels_like'],
#             "temp_min": data['list'][0]['main']['temp_min'],
#             "temp_max": data['list'][0]['main']['temp_max'],
#         }])
#         yield AssetMaterialization(asset_key="weather_forecast", description="Fetched weather data from OpenWeather API")
#         yield Output(df)
#     except Exception as e:
#         raise e
        
