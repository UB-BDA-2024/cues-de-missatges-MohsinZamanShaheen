
CREATE TABLE IF NOT EXISTS sensor_data (
    time TIMESTAMP NOT NULL,
    sensor_id int NOT NULL,
    velocity float,
    temperature float,
    humidity float,
    battery_level float NOT NULL,
    PRIMARY KEY (time, sensor_id)
);

SELECT create_hypertable('sensor_data', 'time', if_not_exists => true);

CREATE Materialized VIEW IF NOT EXISTS sensor_data_hourly (
    hour,
    sensor_id,
    avg_velocity,
    avg_temperature,
    avg_humidity,
    avg_battery_level
)
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS hour,
    sensor_id,
    avg(velocity) AS avg_velocity,
    avg(temperature) AS avg_temperature,
    avg(humidity) AS avg_humidity,
    avg(battery_level) AS avg_battery_level
FROM sensor_data
GROUP BY hour, sensor_id
WITH NO DATA;

CREATE Materialized VIEW IF NOT EXISTS sensor_data_daily (
    day,
    sensor_id,
    avg_velocity,
    avg_temperature,
    avg_humidity,
    avg_battery_level
)
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time) AS day,
    sensor_id,
    avg(velocity) AS avg_velocity,
    avg(temperature) AS avg_temperature,
    avg(humidity) AS avg_humidity,
    avg(battery_level) AS avg_battery_level
FROM sensor_data
GROUP BY day, sensor_id
WITH NO DATA;

CREATE Materialized VIEW IF NOT EXISTS sensor_data_weekly(
    week,
    sensor_id,
    avg_velocity,
    avg_temperature,
    avg_humidity,
    avg_battery_level

)
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 week', time) as week,
    sensor_id,
    avg(velocity) as avg_velocity,
    avg(temperature) as avg_temperature,
    avg(humidity) as avg_humidity,
    avg(battery_level) as avg_battery_level
FROM sensor_data
GROUP BY week, sensor_id
WITH NO DATA;

CREATE Materialized VIEW IF NOT EXISTS sensor_data_monthly(
    month,
    sensor_id,
    avg_velocity,
    avg_temperature,
    avg_humidity,
    avg_battery_level
)
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 month', time) AS month,
    sensor_id,
    avg(velocity) AS avg_velocity,
    avg(temperature) AS avg_temperature,
    avg(humidity) AS avg_humidity,
    avg(battery_level) AS avg_battery_level
FROM sensor_data
GROUP BY month, sensor_id
WITH NO DATA;

CREATE Materialized VIEW IF NOT EXISTS sensor_data_yearly(
    year,
    sensor_id,
    avg_velocity,
    avg_temperature,
    avg_humidity,
    avg_battery_level
)
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 year', time) AS year,
    sensor_id,
    avg(velocity) AS avg_velocity,
    avg(temperature) AS avg_temperature,
    avg(humidity) AS avg_humidity,
    avg(battery_level) AS avg_battery_level
FROM sensor_data
GROUP BY year, sensor_id
WITH NO DATA;