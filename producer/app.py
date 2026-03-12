import os
import json
import time
import random
import uuid
from datetime import datetime
from google.cloud import pubsub_v1
from google.api_core import retry

# Configuration
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "local-iot-project")
PUBSUB_TOPIC_RAW = os.environ.get("PUBSUB_TOPIC_RAW", "iot-sensor-data-raw")

# Note: PUBSUB_EMULATOR_HOST environment variable must be set for local testing
# Example: export PUBSUB_EMULATOR_HOST=localhost:8085

def generate_sensor_data(device_id: str) -> dict:
    """Generates simulated IoT sensor readings."""
    # Sometimes generate bad data for testing DLQ
    if random.random() < 0.05:
        return {
            "device_id": device_id,
            "timestamp_utc": datetime.utcnow().isoformat() + "Z",
            "temperature_celsius": random.uniform(150.0, 300.0), # invalid temp
            "humidity_percent": random.uniform(-50.0, -10.0), # invalid humidity
        }

    return {
        "device_id": device_id,
        "timestamp_utc": datetime.utcnow().isoformat() + "Z",
        "temperature_celsius": round(random.uniform(-20.0, 50.0), 2),
        "humidity_percent": round(random.uniform(0.0, 100.0), 2),
    }

def setup_pubsub():
    """Initializes the Pub/Sub publisher and ensures the topic exists."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC_RAW)

    # In local emulator, we should create the topic if it doesn't exist
    if os.environ.get("PUBSUB_EMULATOR_HOST"):
        try:
            publisher.create_topic(request={"name": topic_path})
            print(f"Created topic {topic_path}")
        except Exception as e:
            # Topic might already exist
            print(f"Topic creation check: {e}")

    return publisher, topic_path

def publish_message(publisher, topic_path, data: dict):
    """Publishes a JSON-encoded message to Pub/Sub with retry logic."""
    data_str = json.dumps(data)
    data_bytes = data_str.encode("utf-8")

    # Retry settings for publishing
    custom_retry = retry.Retry(
        initial=1.0, 
        maximum=60.0, 
        multiplier=2.0, 
        deadline=120.0
    )

    try:
        # Publish future
        future = publisher.publish(topic_path, data=data_bytes, retry=custom_retry)
        message_id = future.result()
        print(f"Published message {message_id}: {data_str}")
        return True
    except Exception as e:
        print(f"Failed to publish message: {e}")
        return False

def main():
    print(f"Starting IoT Producer for Project {GCP_PROJECT_ID}, Topic {PUBSUB_TOPIC_RAW}")
    
    # Wait a bit for emulator to start when running via docker-compose
    time.sleep(5)
    
    publisher, topic_path = setup_pubsub()
    
    devices = [f"sensor-{uuid.uuid4().hex[:4]}" for _ in range(5)]
    
    while True:
        try:
            device_id = random.choice(devices)
            sensor_data = generate_sensor_data(device_id)
            publish_message(publisher, topic_path, sensor_data)
            
            # Publish 1-5 messages per second
            time.sleep(random.uniform(0.2, 1.0))
        except KeyboardInterrupt:
            print("Producer stopped.")
            break
        except Exception as e:
            print(f"Unexpected error in main loop: {e}")
            time.sleep(5) # Delay before next iteration if there's a serious error

if __name__ == "__main__":
    main()
