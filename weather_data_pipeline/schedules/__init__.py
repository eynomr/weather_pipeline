from dagster import ScheduleDefinition
from ..jobs import actual_weather_job, forecast_weather_job

"""SCHEDULE DEFINITIONS"""
hourly_actual_weather_schedule = ScheduleDefinition(
  name="hourly_actual_weather_schedule",
  cron_schedule="0 * * * *",
  job=actual_weather_job
)

daily_forecast_weather_schedule = ScheduleDefinition(
  name="daily_forecast_weather_schedule",
  cron_schedule="0 0 * * *",
  job=forecast_weather_job
)
