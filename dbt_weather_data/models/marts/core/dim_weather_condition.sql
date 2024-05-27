{{
  config(
    materialized='incremental',
    unique_key='condition_id',
  )
}}

with distinct_conditions as (
  select distinct weather_condition, condition_description from (
    select distinct
    weather_condition,
    condition_description
    from {{ ref('stg_weather_actual') }}
    union all
    select distinct
    weather_condition,
    condition_description
    from {{ ref('stg_weather_forecast') }}
  ) a
)
select
  row_number() over() as condition_id,
  weather_condition as condition,
  condition_description as description
from distinct_conditions
{% if is_incremental() %}
where weather_condition || condition_description not in (select weather_condition || condition_description from {{ this }})
{% endif %}