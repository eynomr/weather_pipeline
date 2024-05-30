from dagster import (
    asset, 
    AssetMaterialization, 
    AssetExecutionContext,    
    Output, 
    Failure,
    RetryRequested,
    )

from ...resources import OpenWeatherMapResource, PostgresResource
import pandas as pd
import datetime
from typing import List
from requests import Timeout

@asset()
def test_asset(context: AssetExecutionContext):
    try:
        raise Timeout
    except Timeout as e:
        context.log.error(f"Failed to fetch data for location due to timeout: {str(e)}")

@asset(        
        compute_kind="python",
        description="Fetches all locations from the database",
        deps=[["dbt_schema", "analytics", "dim_location"]]
)
def all_locations(postgres: PostgresResource) -> pd.DataFrame:
    """
    Fetches all locations from the database.
    Resource: PostgresResource
    """
    data = postgres.execute_query("SELECT location_id, latitude, longitude FROM public_analytics.dim_location")
    # df = all_locations
    yield AssetMaterialization(asset_key="all_locations", description="Fetched all locations from the database")
    yield Output(value=data, metadata={"Number of Locations" : len(data)})


@asset(
        compute_kind="python",
        description="Fetches weather data from OpenWeather API",
        metadata={"api": "OpenWeatherMap API v2.5"}
)
def fetch_weather_actual(context: AssetExecutionContext, all_locations: List, open_weather_map: OpenWeatherMapResource) -> pd.DataFrame:
    """
    Fetches actual data from OpenWeather API.
    Resource: OpenWeatherMapResource
    """
    weather_data = []
    context.log.info(f"Fetching actual data for {len(all_locations)} locations")
    for location in all_locations:
        location_id, lat, long = location        
        try:
            data = open_weather_map.get_actual_weather(lat=lat, long=long) 
            context.log.info(f"Actual data for location {location_id} fetched")                     
            weather_data.append({
                "location_id": location_id, # from dim_location
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
                "visibility": data.get('visibility', None),
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
                "ingestion_datetime": datetime.datetime.now().timestamp()
            })
        except Timeout as e:
            context.log.warn(f"Failed to fetch data for location {location} due to timeout: {str(e)}")
        except KeyError as e:
            context.log.error(f"Failed to fetch data for location {location} due to key error: {str(e)}")
            raise Failure(f"Failed to fetch data for location {location} with error: {str(e)}")
        except Failure as e:
            context.log.error(f"Failed to fetch data for location {location} with error: {str(e)}")
            raise e
        except Exception as e:
            context.log.error(f"Failed to fetch data for location {location}: {str(e)}")
            raise e
        
    yield AssetMaterialization(asset_key="weather_actual", description="Fetched weather data from OpenWeather API")
    yield Output(value=weather_data, metadata={"Number of records" : len(weather_data), "Sample Data": weather_data[:5]})


@asset(
        compute_kind="python",
        description="Loads raw weather data into the database",
        metadata={"schema": "public_stage", "table": "raw_weather_actual"}
)
def raw_weather_actual(context: AssetExecutionContext, fetch_weather_actual: List, postgres: PostgresResource):
    """
    Loads the raw fetched actual data into the database.
    Resource: PostgresResource
    """
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
            sunset,
            ingestion_datetime
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
            to_timestamp(%(sunset)s),
            to_timestamp(%(ingestion_datetime)s)
        )
    """
    for data in fetch_weather_actual:
        try:
            postgres.execute_query(query, data)
        except Exception as e:
            context.log.error(f"Failed to load raw actual data into the database: {str(e)}")
            raise Failure(f"Failed to load data into the database: {str(e)}")
    yield AssetMaterialization(asset_key="raw_weather_actual", description="Loaded raw weather data into the database")
    yield Output(value=fetch_weather_actual, metadata={"Number of records" : len(fetch_weather_actual), "Sample Data": fetch_weather_actual[:5]})
    


@asset(
        compute_kind="python",
        description="Fetches forecast data from OpenWeather API",
        metadata={"api": "OpenWeatherMap API v2.5"}
)
def fetch_weather_forecast(context: AssetExecutionContext, all_locations: List, open_weather_map: OpenWeatherMapResource) -> pd.DataFrame:
    """
    Fetches forecast data from OpenWeather API.
    Resource: OpenWeatherMapResource
    """
    weather_data = []
    context.log.info(f"Fetching forecast data for {len(all_locations)} locations")
    for location in all_locations:
        location_id, lat, long = location        
        try:
            data = open_weather_map.get_forecast_weather(lat=lat, long=long)                      
            context.log.info(f"Forecast data for location {location_id} fetched")
            data = data['list']
            for data in data:
                weather_data.append({
                    "location_id": location_id, # from dim_location
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
                    "visibility": data.get('visibility', None),
                    "wind_speed": data['wind']['speed'],
                    "wind_degree": data['wind']['deg'],
                    "wind_gust": data.get('wind', {}).get('gust', None),
                    "cloudiness": data.get('clouds', {}).get('all', None),                    
                    "rain_3h": data.get('rain', {}).get('3h', None),                    
                    "snow_3h": data.get('snow', {}).get('3h', None),
                    "precipitation_probability": data.get('pop', None),
                    "forecast_datetime": data['dt'],
                    "ingestion_datetime": datetime.datetime.now().timestamp()
                })
        except Timeout as e:
            context.log.warn(f"Failed to fetch data for location {location} due to timeout: {str(e)}")
        except KeyError as e:
            context.log.error(f"Failed to fetch data for location {location} due to key error: {str(e)}")
            raise Failure(f"Failed to fetch data for location {location} with error: {str(e)}")
        except Failure as e:
            context.log.error(f"Failed to fetch data for location {location} with error: {str(e)}")
            raise e
        except Exception as e:
            context.log.error(f"Failed to fetch data for location {location}: {str(e)}")
            raise e
        
    yield AssetMaterialization(asset_key="weather_actual", description="Fetched weather data from OpenWeather API")
    yield Output(value=weather_data, metadata={"Number of records" : len(weather_data), "Sample Data": weather_data[:5]})

        

@asset(
        compute_kind="python",
        description="Loads raw forecast data into the database",
        metadata={"schema": "public_stage", "table": "raw_weather_forecast"}
)
def raw_weather_forecast(context: AssetExecutionContext, fetch_weather_forecast: List, postgres: PostgresResource):
    """
    Loads the raw fetched forecast data into the database.
    Resouce: PostgresResource
    """
    query = """
        INSERT INTO public_stage.raw_weather_forecast(
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
            rain_3h,
            snow_3h,
            precipitation_probability,
            forecast_datetime,
            ingestion_datetime 
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
            %(rain_3h)s,
            %(snow_3h)s,
            %(precipitation_probability)s,
            to_timestamp(%(forecast_datetime)s),
            to_timestamp(%(ingestion_datetime)s)
        )
    """
    for data in fetch_weather_forecast:
        try:
            postgres.execute_query(query, data)
        except Exception as e:
            context.log.error(f"Failed to load raw forecast data into the database: {str(e)}")
            raise Failure(f"Failed to load data into the database: {str(e)}")
    yield AssetMaterialization(asset_key="raw_weather_actual", description="Loaded raw weather data into the database")
    yield Output(value=fetch_weather_forecast, metadata={"Number of records" : len(fetch_weather_forecast), "Sample Data": fetch_weather_forecast[:5]})
    
