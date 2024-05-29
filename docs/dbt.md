<img src='https://www.getdbt.com/ui/img/logos/dbt-logo.svg' alt='dbt logo' width='100' height='40' />

## Overview
We use dbt to transform the raw data into dimenstional and fact tables. The dbt project is integrated with the dagster project and utilizes dbt assets for data transformation.

### Raw Tables
**raw_weather_actual**
- Description: Raw actual weather data fetched by `fetch_weather_actual` asset. This table is not managed by dbt.

**raw_weather_forecast**
- Description: Raw forecast weather data fetched by `fetch_weather_forecast` asset. This table is not managed by dbt.
---
### Staging Tables
**stg_weather_actual**
- Description: Staging table for actual weather data. This table is managed by dbt and is used to store the transformed data from `raw_weather_actual`.

**stg_weather_forecast**
- Description: Staging table for forecast weather data. This table is managed by dbt and is used to store the transformed data from `raw_weather_forecast`.
---
### Fact and Dimension Tables
**dim_datetime**
- Materialization: Incremental
- Unique Key: `datetime_id`
- Index: `date`
- Description: This dimension table provides a comprehensive time dimension for weather data, broken down into half-hour intervals. This table includes detailed date and time components such as year, month, day, hour, minute, and day of the week, along with flags indicating weekends. The table is incrementally updated to efficiently handle new or updated records, ensuring accurate and up-to-date time references for weather data analysis.

**dim_location**
- Materialization: Table
- Index: `location_id`, `city_name`
- Description: This dimension table stores detailed location information, including location IDs, country codes, city names, and geographical coordinates (latitude and longitude). The data is sourced from a seed in dbt (csv file), ensuring a consistent and reliable reference for location-based analyses. Indexes on location_id and city_name improve query performance for location-related queries.

**dim_weather_condition**
- Materialization: Incremental
- Unique Key: `condition_id`
- Index: `condition_id`, `condition_name`
- Description: This dimension table captures unique weather conditions and their descriptions, sourced from both actual and forecast weather data. It ensures comprehensive and distinct entries for each weather condition. The table is incrementally updated to efficiently incorporate new or modified conditions, maintaining up-to-date and accurate weather condition references. Indexes on condition_id and condition enhance query performance for condition-based queries.


**fact_weather_actual**
- Materialization: Incremental
- Unique Key: `location_id`, `datetime_id`
- Index: `location_id`, `datetime_id`
- Description: This fact table consolidates detailed actual weather data, linking them to specific locations and time intervals. It includes various weather metrics such as temperature, pressure, humidity, visibility, wind speed, and precipitation. Data is sourced from the stg_weather_actual staging table and joined with dim_datetime and dim_weather_condition tables to provide comprehensive weather data points. The table is incrementally updated to efficiently handle new or updated weather observations (based on the unique_key and ingestion_datetime), ensuring timely and accurate data for analysis. Indexes on location_id and datetime_id enhance query performance for location and time-based queries.

**fact_weather_forecast**
- Materialization: Incremental
- Unique Key: `location_id`, `datetime_id`
- Index: `location_id`, `datetime_id`
- Description: This fact table consolidates detailed forecast weather data, linking them to specific locations and time intervals. It includes various weather metrics such as temperature, pressure, humidity, visibility, wind speed, and precipitation. Data is sourced from the stg_weather_forecast staging table and joined with dim_datetime and dim_weather_condition tables to provide comprehensive weather data points. The table is incrementally updated to efficiently handle new or updated forecast data (based on the unique_key and ingestion_datetime), ensuring timely and accurate data for analysis. Indexes on location_id and datetime_id enhance query performance for location and time-based queries.
---
### Views
**vw_actual_vs_forecast**
- Description: This view provides a comprehensive comparison between actual weather conditions and forecasts. It includes data points such as location, observation date, actual and forecasted weather conditions, and temperatures. The view calculates metrics like `condition_match`, `forecast_error`, and `forecast_accuracy_percentage`. By joining the actual weather data (fact_weather_actual) with the forecast data (fact_weather_forecast) and various dimension tables, this view offers insights into the accuracy of weather forecasts and helps identify discrepancies between actual and predicted weather conditions.
