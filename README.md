# Real-Time IoT Sensor Data Pipeline

## Overview
This project implements a robust, real-time data ingestion and validation pipeline for simulated IoT sensor data. It utilizes an event-driven microservices architecture via Google Cloud Pub/Sub and is containerized using Docker, allowing it to run easily in a local environment using a Pub/Sub emulator and MySQL.

The pipeline comprises two main components:
1.  **Producer Service**: Simulates IoT devices, generating sensor readings (temperature and humidity) and publishing them to a Pub/Sub topic as JSON messages. It includes randomized anomalies to test the consumer's error handling.
2.  **Consumer Service**: Subscribes to the Pub/Sub topic, parses incoming messages, and enforces strict data quality rules using `pydantic`. Valid records are batched and inserted into a MySQL database. Invalid records (failing schema or range checks) are gracefully handled, logged, and routed to a Dead Letter Queue (DLQ) topic to prevent data loss or pipeline interruption.

## Architecture & Data Flow

```mermaid
graph LR
    A[Producer (Simulated IoT)] -->|Publish JSON| B((Pub/Sub: iot-sensor-data-raw))
    B -->|Subscribe| C[Consumer Service]
    C -->|Valid Data| D[(MySQL: sensor_readings)]
    C -->|Invalid Data| E((Pub/Sub DLQ: iot-sensor-data-dlq))
```

1.  **Data Generation**: The `producer` container generates fake sensor data.
2.  **Streaming**: Data is pushed to the `iot-sensor-data-raw` topic on the local `pubsub-emulator` container.
3.  **Processing**: The `consumer` container listens to the subscription. It validates each message.
4.  **Storage**: 
    *   Valid messages are inserted into the `mysql` container's `iot_data` database.
    *   Invalid messages are published to the `iot-sensor-data-dlq` topic.

## Data Quality Rules
The consumer strictly enforces the following data constraints using Pydantic:
*   `device_id`: Must be a non-empty string.
*   `timestamp_utc`: Must be a valid ISO 8601 datetime string.
*   `temperature_celsius`: Must be a float within the plausible range of `-50.0` to `100.0`.
*   `humidity_percent`: Must be a float within the plausible range of `0.0` to `100.0`.

Records failing these checks are caught via `ValidationError`, acknowledged to prevent reprocessing loops, and immediately sent to the DLQ.

## Prerequisites
*   Docker
*   Docker Compose

## Setup & Running Instructions

1.  **Clone the repository** (or navigate to the project root):
    ```bash
    cd iot-pipeline
    ```

2.  **Configure Environment**:
    Create a `.env` file from the example (though Docker Compose defaults are provided for local testing):
    ```bash
    cp .env.example .env
    ```

3.  **Build and Start Services**:
    Run the following command to build the producer and consumer images, and start all services in detached mode:
    ```bash
    docker-compose up -d --build
    ```

4.  **Monitor Logs**:
    You can view the data flowing through the system by observing the container logs:
    ```bash
    # View all logs
    docker-compose logs -f
    
    # Or view specific services
    docker-compose logs -f producer
    docker-compose logs -f consumer
    ```

## Verifying the Data

Once the pipeline is running, you can connect to the MySQL database to verify that clean data is landing successfully.

1.  **Connect to the MySQL container**:
    ```bash
    docker exec -it iot-pipeline-mysql-1 mysql -u root -piot_secure_password iot_data
    ```
    *(Note: Your actual container name might differ slightly, check `docker ps`)*

2.  **Run SQL Queries**:
    ```sql
    -- Check total number of ingested readings
    SELECT COUNT(*) FROM sensor_readings;
    
    -- View the most recent readings
    SELECT * FROM sensor_readings ORDER BY timestamp_utc DESC LIMIT 10;
    
    -- Verify no bad data bypassed validation
    SELECT COUNT(*) FROM sensor_readings WHERE temperature_celsius < -50 OR temperature_celsius > 100;
    ```

## Teardown
To stop the pipeline and remove the containers, networks, and volumes:
```bash
docker-compose down -v
```

## Automated Tests (Bonus)
The consumer service includes automated unit tests utilizing `pytest` to verify the `pydantic` data validation and quality rules directly. 

To run the tests locally (requires Python 3.9+ and `pytest`):
```bash
pip install -r consumer/requirements.txt
pytest consumer/test_app.py -v
```
