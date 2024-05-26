from dagster import ConfigurableResource
from dagster_dbt import DbtCliResource

import requests
from requests import Response
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
  """
  api_key: str

  def get_actual_weather(self, lat: float, long: float) -> Response:
    return requests.get(
      f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={long}&appid={self.api_key}&units=imperial"
    )
  
  def get_forecast_weather(self, lat: float, long: float) -> Response:
    return requests.get(
      f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={long}&appid={self.api_key}&units=imperial"
    )