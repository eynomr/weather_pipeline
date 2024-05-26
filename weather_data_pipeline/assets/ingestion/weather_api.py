from dagster import (
    asset, 
    AssetMaterialization, 
    AssetExecutionContext,
    Output, 
    Failure
    )
from ...resources import OpenWeatherMapResource, PostgresResource
import pandas as pd
import requests
import os


@asset()
def all_locations(postgres: PostgresResource) -> pd.DataFrame:
    data = postgres.execute_query("SELECT * FROM public.dim_location")
    df = pd.DataFrame(data, columns=["location_id", "country_code", "city_name", "latitude", "longitude"])
    yield AssetMaterialization(asset_key="all_locations", description="Fetched all locations from the database")
    yield Output(value=df, metadata={"Number of Locations" : len(df), "Columns" : df.columns.to_list(), "Sample Data" : df.head().to_dict(orient="records")})


@asset()
def fetch_weather_actual(context: AssetExecutionContext, all_locations, open_weather_map: OpenWeatherMapResource) -> pd.DataFrame:
    weather_data = []
    for index, location in all_locations.iterrows():
        context.log.info(location)
        context.log.info(f"Fetching weather data for location: {location['city_name']}")
        location_id = location["location_id"]
        lat = location["latitude"]
        long = location["longitude"]
        try:
            response = open_weather_map.get_actual_weather(lat=lat, long=long)
            response.raise_for_status()            
            data = response.json()
            weather_data.append({
                "location_id": location_id,
                "weather_id": data['weather'][0]['id'],
                "temp": data['main']['temp'],
                "feels_like": data['main']['feels_like'],
                "temp_min": data['main']['temp_min'],
                "temp_max": data['main']['temp_max'],
            })
        except Exception as e:
            raise Failure(f"Failed to fetch locations: {str(e)}")
        
    df = pd.DataFrame(weather_data)
    yield AssetMaterialization(asset_key="weather_actual", description="Fetched weather data from OpenWeather API")
    yield Output(value=df, metadata={"Number of records" : len(df), "Columns" : df.columns.to_list(), "Sample Data" : df.head().to_dict(orient="records")})
    
@asset
def fetch_weather_forecast(open_weather_map: OpenWeatherMapResource) -> pd.DataFrame:
    try:
        response = open_weather_map.get_forecast_weather(lat=40.7128, long=-74.0060)
        response.raise_for_status()

        data = response.json()

        df = pd.DataFrame([{
            "weather_id": data['list'][0]['weather'][0]['id'],
            "temp": data['list'][0]['main']['temp'],
            "feels_like": data['list'][0]['main']['feels_like'],
            "temp_min": data['list'][0]['main']['temp_min'],
            "temp_max": data['list'][0]['main']['temp_max'],
        }])
        yield AssetMaterialization(asset_key="weather_forecast", description="Fetched weather data from OpenWeather API")
        yield Output(df)
    except Exception as e:
        raise e
        
