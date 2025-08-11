CREATE TABLE IF NOT EXISTS weather_observations (
    observed_at TIMESTAMPTZ NOT NULL,
    city TEXT NOT NULL,
    temp_c NUMERIC,
    humidity NUMERIC,
    wind_speed NUMERIC,
    weather_desc TEXT,
    PRIMARY KEY (observed_at, city)
);
CREATE INDEX IF NOT EXISTS ix_weather_observations_city_time
ON weather_observations (city, observed_at);