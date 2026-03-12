CREATE DATABASE IF NOT EXISTS iot_data;
USE iot_data;

CREATE TABLE IF NOT EXISTS sensor_readings (
    id INT AUTO_INCREMENT PRIMARY KEY,
    device_id VARCHAR(255) NOT NULL,
    timestamp_utc DATETIME NOT NULL,
    temperature_celsius FLOAT NOT NULL,
    humidity_percent FLOAT NOT NULL,
    processing_timestamp_utc DATETIME NOT NULL
);
