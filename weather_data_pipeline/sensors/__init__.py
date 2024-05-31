from dagster import (
  run_status_sensor,
  DagsterRunStatus, 
  RunRequest,
  RunFailureSensorContext,
)
from dagster_slack import make_slack_on_run_failure_sensor
import os
from ..jobs import forecast_weather_job, daily_forecast_job

@run_status_sensor(run_status=DagsterRunStatus.SUCCESS, request_job=daily_forecast_job)
def daily_forecast_sensor(context):
    if context.dagster_run.job_name == forecast_weather_job.name:
      run_config = {}
      return RunRequest(run_key=None, run_config=run_config)
    else:
        return None
      
      
def slack_message_fn(context: RunFailureSensorContext):
  return (
    f"Job {context.dagster_run.job_name} failed!"
    f"Error: {context.failure_event.message}"
  )
  
slack_on_run_failure = make_slack_on_run_failure_sensor(
  channel="#analytics",
  slack_token=os.getenv("SLACK_TOKEN"),
  text_fn=slack_message_fn,
)
