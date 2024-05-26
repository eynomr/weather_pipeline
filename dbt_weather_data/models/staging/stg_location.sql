with raw_locations as (
  select *
  from {{ ref('locations') }}
)

select
  location_id,
  country_code,
  city_name,
  latitude,
  longitude
from raw_locations