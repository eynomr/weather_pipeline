from dagster import build_asset_context, ResourceDefinition, AssetMaterialization, Output
from unittest import mock
from weather_data_pipeline.assets.ingestion import (
  all_locations,
  fetch_weather_actual,
)
from datetime import datetime
from weather_data_pipeline.resources import OpenWeatherMapResource
import pandas as pd
import json



def test_fetch_weather_actual():
    # Create a mock WeatherAPIResource
    all_locations_mock = (
      (1, 40.6635, -73.9387),
    )
    mock_weather_api = mock.MagicMock(spec=OpenWeatherMapResource)
    mock_weather_api.get_actual_weather.return_value = {
          "coord": {
              "lon": -73.9387,
              "lat": 40.6635
          },
          "weather": [
              {
                  "id": 701,
                  "main": "Mist",
                  "description": "mist",
                  "icon": "50n"
              }
          ],
          "base": "stations",
          "main": {
              "temp": 292.52,
              "feels_like": 292.91,
              "temp_min": 291.43,
              "temp_max": 295.08,
              "pressure": 1007,
              "humidity": 92
          },
          "visibility": 10000,
          "wind": {
              "speed": 3.6,
              "deg": 210
          },
          "clouds": {
              "all": 100
          },
          "dt": 1716862617,
          "sys": {
              "type": 2,
              "id": 2037026,
              "country": "US",
              "sunrise": 1716802166,
              "sunset": 1716855412
          },
          "timezone": -14400,
          "id": 5110302,
          "name": "Brooklyn",
          "cod": 200
      }

    mock_resource_def = ResourceDefinition.hardcoded_resource(mock_weather_api)
    context = build_asset_context(resources={"open_weather_map": mock_resource_def})
    result = fetch_weather_actual(context, all_locations_mock)
    
    materializations = []
    output = None
    for res in result:
        if isinstance(res, AssetMaterialization):
            materializations.append(res)
        elif isinstance(res, Output):
            output = res
    
    # Assert the expected results
    print("Materializations: ", materializations)
    print("Output: ", output)
    

    expected_output = [{
        "location_id": 1,
        "weather_type": "Mist",
        "weather_description": "mist",
        "temp": 292.52,
        "feels_like": 292.91,
        "temp_min": 291.43,
        "temp_max": 295.08,
        "pressure": 1007,
        "humidity": 92,
        "sea_level": None,
        "ground_level": None,
        "visibility": 10000,
        "wind_speed": 3.6,
        "wind_degree": 210,
        "wind_gust": None,
        "cloudiness": 100,
        "rain_1h": None,
        "rain_3h": None,
        "snow_1h": None,
        "snow_3h": None,
        "observation_datetime": 1716862617,
        "sunrise": 1716802166,
        "sunset": 1716855412,
        "ingestion_datetime": mock.ANY 
    }]
    
    assert output.value == expected_output
    assert len(materializations) == 1    
    assert materializations[0].asset_key[0] == ["weather_actual"]



