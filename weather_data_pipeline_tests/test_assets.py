from dagster import build_asset_context
from unittest.mock import MagicMock
from weather_data_pipeline.assets.ingestion import (
  all_locations,
  fetch_weather_actual,
)
from datetime import datetime
from weather_data_pipeline.resources import OpenWeatherMapResource
import pandas as pd


def test_fetch_actual():
  all_locations_mock = (
    (1, 40.6635, -73.9387),
  )
  context = build_asset_context()

  result = fetch_weather_actual(context, all_locations_mock)
  
  assert result[0]['location_id'] == 1
  

from unittest import mock
from dagster import build_asset_context

def test_fetch_weather_actual():
    # Create a mock WeatherAPIResource
    all_locations_mock = (
      (1, 40.6635, -73.9387),
    )
    mock_weather_api = mock.MagicMock(spec=OpenWeatherMapResource)
    mock_weather_api.fetch_weather.return_value = {"temperature": 65, "condition": "cloudy"}

    context = build_asset_context(resources={"weather_api": mock_weather_api})
    result = fetch_weather_actual(context.resources.weather_api)
    
    # Assert the expected result
    assert result == {"temperature": 65, "condition": "cloudy"}


