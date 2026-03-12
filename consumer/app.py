import os
import json
import time
from datetime import datetime, timezone
import mysql.connector
from mysql.connector import Error
from google.cloud import pubsub_v1
from pydantic import BaseModel, ValidationError, Field

# Configuration
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "local-iot-project")
PUBSUB_TOPIC_RAW = os.environ.get("PUBSUB_TOPIC_RAW", "iot-sensor-data-raw")
PUBSUB_TOPIC_DLQ = os.environ.get("PUBSUB_TOPIC_DLQ", "iot-sensor-data-dlq")
SUBSCRIPTION_ID = f"{PUBSUB_TOPIC_RAW}-sub"

MYSQL_HOST = os.environ.get("MYSQL_HOST", "localhost")
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "iot_secure_password")
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE", "iot_data")

class SensorData(BaseModel):
    device_id: str
    timestamp_utc: datetime
    temperature_celsius: float = Field(ge=-50.0, le=100.0)
    humidity_percent: float = Field(ge=0.0, le=100.0)

def connect_to_db(max_retries=10, delay=5):
    """Wait for database connection."""
    for attempt in range(max_retries):
        try:
            connection = mysql.connector.connect(
                host=MYSQL_HOST,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DATABASE
            )
            if connection.is_connected():
                print("Successfully connected to MySQL database")
                return connection
        except Error as e:
            print(f"Error connecting to MySQL: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
    
    raise Exception("Could not connect to MySQL database after multiple attempts")

def setup_pubsub():
    """Initializes the Pub/Sub subscriber and ensures topics and subscriptions exist for local testing."""
    subscriber = pubsub_v1.SubscriberClient()
    publisher = pubsub_v1.PublisherClient()
    
    subscription_path = subscriber.subscription_path(GCP_PROJECT_ID, SUBSCRIPTION_ID)
    topic_path = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC_RAW)
    dlq_topic_path = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC_DLQ)
    
    if os.environ.get("PUBSUB_EMULATOR_HOST"):
        try:
            publisher.create_topic(request={"name": topic_path})
            print(f"Ensured topic {topic_path} exists")
        except Exception:
            pass
            
        try:
            publisher.create_topic(request={"name": dlq_topic_path})
            print(f"Ensured DLQ topic {dlq_topic_path} exists")
        except Exception:
            pass

        try:
            subscriber.create_subscription(
                request={"name": subscription_path, "topic": topic_path}
            )
            print(f"Created subscription {subscription_path}")
        except Exception:
            pass
            
    return subscriber, subscription_path, publisher, dlq_topic_path

def insert_to_db(connection, valid_records):
    """Inserts a batch of valid records into the database."""
    if not valid_records:
        return
        
    try:
        cursor = connection.cursor()
        sql = """
            INSERT INTO sensor_readings 
            (device_id, timestamp_utc, temperature_celsius, humidity_percent, processing_timestamp_utc)
            VALUES (%s, %s, %s, %s, %s)
        """
        
        # Prepare values
        process_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        values = []
        for r in valid_records:
            t = r.timestamp_utc.strftime('%Y-%m-%d %H:%M:%S')
            values.append((r.device_id, t, r.temperature_celsius, r.humidity_percent, process_time))
            
        cursor.executemany(sql, values)
        connection.commit()
        print(f"Successfully inserted {cursor.rowcount} records into database")
        
    except Error as e:
        print(f"Failed to insert record into MySQL table: {e}")
        # Depending on requirements, we might want to handle this differently, but right now just log
        connection.rollback()
    finally:
        if connection.is_connected():
            cursor.close()

def send_to_dlq(publisher, dlq_topic_path, original_data, error_msg):
    """Sends invalid data to the Dead Letter Queue."""
    dlq_payload = {
        "original_message": original_data,
        "error": error_msg,
        "processing_time": datetime.utcnow().isoformat() + "Z"
    }
    
    data_bytes = json.dumps(dlq_payload).encode("utf-8")
    
    try:
        publisher.publish(dlq_topic_path, data=data_bytes).result()
        print(f"Sent invalid message to DLQ: {error_msg}")
    except Exception as e:
        print(f"Failed to publish to DLQ: {e}")

def process_message(message, publisher, dlq_topic_path, db_connection):
    """Processes a single message: validates, and routes to DB or DLQ."""
    raw_data = message.data.decode("utf-8")
    
    try:
        parsed_data = json.loads(raw_data)
        
        # Validate data using Pydantic
        validated_data = SensorData(**parsed_data)
        
        # Valid data - insert to DB
        insert_to_db(db_connection, [validated_data])
        
        # Ack message regardless of DB insert success for now to avoid endless loop, 
        # in prod we might NACK if DB is down.
        message.ack()
        
    except ValidationError as e:
        print(f"Data Quality Validation error: {e}")
        send_to_dlq(publisher, dlq_topic_path, raw_data, str(e))
        message.ack() # Ack it since we handled it by sending to DLQ
        
    except json.JSONDecodeError as e:
        print(f"JSON Parse error: {e}")
        send_to_dlq(publisher, dlq_topic_path, raw_data, f"Invalid JSON: {str(e)}")
        message.ack()
        
    except Exception as e:
        print(f"Unexpected error processing message: {e}")
        message.nack() # NACK unexpected errors to retry
        

def main():
    print("Starting IoT Consumer Subscribing service")
    
    # Wait for MySQL to be ready
    db_connection = connect_to_db()
    
    # Wait a bit for emulator
    time.sleep(5)
    
    subscriber, subscription_path, publisher, dlq_topic_path = setup_pubsub()
    
    def callback(message):
        process_message(message, publisher, dlq_topic_path, db_connection)

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result()
        except Exception as e:
            print(f"Listening completed with exception: {e}")
            streaming_pull_future.cancel()  # Trigger the shutdown.

if __name__ == "__main__":
    main()
