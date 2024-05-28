from dagster import ConfigurableResource, RetryRequested, Failure, MetadataValue
from dagster_dbt import DbtCliResource

import os
import requests
from requests import Response, RequestException
from tenacity import retry, stop_after_attempt, wait_fixed
from ratelimit import limits, sleep_and_retry
import psycopg2

from ..assets.constants import DBT_DIRECTORY

# dbt resource to interact with the dbt.
dbt_resource = DbtCliResource(
  project_dir=DBT_DIRECTORY,
)

class PostgresResource(ConfigurableResource):
  """
  A Posrgres resource to connect the database and execute queries.
  """
  host: str
  port: int
  database: str
  user: str
  password: str

  def get_connection(self):
    return psycopg2.connect(
      host=self.host,
      port=self.port,
      database=self.database,
      user=self.user,
      password=self.password
    )
  
  def execute_query(self, query: str, params: tuple = None):
    connection = self.get_connection()
    with connection.cursor() as cursor:
      cursor.execute(query, params)
      if cursor.description:
        return cursor.fetchall()
      else:
        connection.commit()
        return None
  

class OpenWeatherMapResource(ConfigurableResource):
  """
  OpenWeather API resource to interact with the api.
  Rate limited to 60 calls per minute.
  Retry 3 times with a fixed wait of 5 minutes in case of connectivity issues or API downtime.
  """
  api_key: str

  @sleep_and_retry
  @limits(calls=60, period=60)
  @retry(wait=wait_fixed(5*60), stop=stop_after_attempt(3))
  def get_actual_weather(self, lat: float, long: float) -> Response:
    try:
      response = requests.get(
        f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={long}&appid={self.api_key}&units=imperial"
      )
      response.raise_for_status()
      return response
    except RequestException as e:
      raise RetryRequested(max_retries=3, seconds_to_wait=60*5) from e
    except Exception as e:
      raise Failure(
         description=f"Failed to fetch actual weather data: {str(e)}",
         metadata={"error": MetadataValue.text(str(e))}
        )
  
  @sleep_and_retry
  @limits(calls=60, period=60)
  @retry(wait=wait_fixed(5*60), stop=stop_after_attempt(3))
  def get_forecast_weather(self, lat: float, long: float) -> Response:
    try:
      response = requests.get(
        f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={long}&appid={self.api_key}&units=imperial"
      )
      response.raise_for_status()
      return response
    except RequestException as e:
      raise RetryRequested(max_retries=3, seconds_to_wait=60*5) from e
    except Exception as e:
      raise Failure(
         description=f"Failed to fetch forecast weather data: {str(e)}",
         metadata={"error": MetadataValue.text(str(e))}
        )
           
