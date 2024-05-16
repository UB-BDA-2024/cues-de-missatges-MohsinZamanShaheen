USE sensor;

CREATE TABLE IF NOT EXISTS sensor.sensor_data_tbl (
    sensor_id int,
    timestamp timestamp,
    temperature float,
    humidity float,
    velocity float,
    battery_level float,
    PRIMARY KEY (sensor_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

CREATE TABLE IF NOT EXISTS sensor.temperature_statistics (
    sensor_id int,
    max_temperature float,
    min_temperature float,
    avg_temperature float,
    total_temperature float,
    temperature_count int,
    PRIMARY KEY (sensor_id)
);


CREATE TABLE IF NOT EXISTS sensor.sensor_count_by_type (
    sensor_type text,
    count counter,
    PRIMARY KEY(sensor_type)
);

CREATE TABLE IF NOT EXISTS sensor.low_battery_sensors (
    battery_level float,
    sensor_id int,
    last_update timestamp,
    PRIMARY KEY (battery_level, sensor_id)
);
