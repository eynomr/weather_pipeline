{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['location_id', 'city_name']},
    ]
  )
}}
SELECT
location_id,
country_code,
city_name,
latitude,
longitude
from {{ ref('stg_location') }}