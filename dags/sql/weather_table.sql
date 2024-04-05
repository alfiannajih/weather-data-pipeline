CREATE TABLE IF NOT EXISTS weather (
    "date" TIMESTAMP,
    "weather_code" FLOAT,
    "temperature_2m_max" FLOAT,
    "temperature_2m_min" FLOAT,
    "temperature_2m_mean" FLOAT,
    "apparent_temperature_max" FLOAT,
    "apparent_temperature_min" FLOAT,
    "apparent_temperature_mean" FLOAT,
    "sunrise" INTEGER,
    "sunset" INTEGER,
    "daylight_duration" FLOAT,
    "sunshine_duration" FLOAT,
    "precipitation_sum" FLOAT,
    "rain_sum" FLOAT,
    "snowfall_sum" FLOAT,
    "precipitation_hours" FLOAT,
    "wind_speed_10m_max" FLOAT,
    "wind_gusts_10m_max" FLOAT,
    "wind_direction_10m_dominant" FLOAT,
    "shortwave_radiation_sum" FLOAT,
    "et0_fao_evapotranspiration" FLOAT,
    "city" TEXT,
    "longitude" TEXT,
    "latitude" TEXT
);