
SELECT
location_id,
country_code,
city_name,
latitude,
longitude
from {{ ref('stg_location') }}