CREATE TABLE IF NOT EXISTS weather_stats (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    avg_temperature DOUBLE PRECISION,
    avg_feels_like DOUBLE PRECISION,
    avg_humidity DOUBLE PRECISION,
    avg_precipitation DOUBLE PRECISION,
    avg_wind_speed DOUBLE PRECISION,
    avg_pressure DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_weather_stats_window_start
ON weather_stats(window_start DESC);

CREATE INDEX IF NOT EXISTS idx_weather_stats_created_at
ON weather_stats(created_at DESC);