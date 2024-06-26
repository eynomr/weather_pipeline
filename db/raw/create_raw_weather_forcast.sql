CREATE TABLE IF NOT EXISTS public_stage.raw_weather_forecast (
    forecast_id SERIAL PRIMARY KEY,
    location_id INT,
    weather_type VARCHAR(50),
    weather_description VARCHAR(255),
    temp FLOAT,
    feels_like FLOAT,
    temp_min FLOAT,
    temp_max FLOAT,
    pressure INT,
    humidity INT,
    sea_level INT,
    ground_level INT,
    visibility INT,
    wind_speed FLOAT,
    wind_degree INT,
    wind_gust FLOAT,
    cloudiness INT,
    rain_3h FLOAT,
    snow_3h FLOAT,
    precipitation_probability FLOAT,
    forecast_datetime TIMESTAMP,
    ingestion_datetime TIMESTAMP
);

CREATE INDEX idx_forecast_id ON public_stage.raw_weather_actual(weather_id);
CREATE INDEX idx_forecast_datetime ON public_stage.raw_weather_actual(observation_datetime);
CREATE INDEX idx_ingestion_datetime ON public_stage.raw_weather_actual(ingestion_datetime);

