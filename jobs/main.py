import os
import logging
import random
import time
import uuid

import simplejson as json

from confluent_kafka import SerializingProducer
from datetime import datetime, timedelta

from dotenv import load_dotenv


load_dotenv()

logger = logging.getLogger(__name__)


SAN_FRANCISCO_COORDINATES = {"latitude": 51.5074, "longitude": -0.1278}
NEW_YORK_COORDINATES = {"latitude": 52.4862, "longitude": -1.8904}


# As the vehicles move we calculate its increment
LATITUDE_INCREMENT = (
    NEW_YORK_COORDINATES["latitude"] - SAN_FRANCISCO_COORDINATES["latitude"]
) / 100

LONGITUDE_INCREMENT = (
    NEW_YORK_COORDINATES["longitude"] - SAN_FRANCISCO_COORDINATES["longitude"]
) / 100

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
VEHICLE_TOPIC = os.getenv("VEHICLE_TOPIC", "vehicle_data")
GPS_TOPIC = os.getenv("GPS_TOPIC", "gps_data")
TRAFFIC_TOPIC = os.getenv("TRAFFIC_TOPIC", "traffic_data")
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather_data")
EMERGENCY_TOPIC = os.getenv("EMERGENCY_TOPIC", "emergency_data")

random.seed(45)
start_time = datetime.now()
start_location = SAN_FRANCISCO_COORDINATES.copy()


def get_next_time():
    global start_time

    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time


def simulate_vehicle_movement():
    global start_location

    # move to new york
    start_location["latitude"] += LATITUDE_INCREMENT
    start_location["longitude"] += LONGITUDE_INCREMENT

    # adding randomness

    start_location["latitude"] += random.uniform(-0.0005, 0.0005)
    start_location["longitude"] += random.uniform(-0.0005, 0.0005)
    return start_location


def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()

    return {
        "id": uuid.uuid4(),
        "device_id": device_id,
        "timestamp": get_next_time().isoformat(),
        "location": (location["latitude"], location["longitude"]),
        "speed": random.uniform(10, 80),
        "direction": "North-East",
        "make": "Mercedes",
        "model": "C200",
        "year": 2020,
        "fuel_type": "hybrid",
    }


def generate_gps_data(device_id, timestamp, vehicle_type="private"):
    return {
        "id": uuid.uuid4(),
        "device_id": device_id,
        "timestamp": timestamp,
        "speed": random.uniform(0, 80),
        "direction": "North-East",
        "vehicle_type": vehicle_type,
    }


def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    return {
        "id": uuid.uuid4(),
        "device_id": device_id,
        "camera_id": camera_id,
        "timestamp": timestamp,
        "location": location,
        "snapshot": "Base64EncodedString",
    }


def generate_weather_data(device_id, timestamp, location):
    return {
        "id": uuid.uuid4(),
        "device_id": device_id,
        "location": location,
        "timestamp": timestamp,
        "temperature": random.uniform(-5, 28),
        "weather_condition": random.choice(["sunny", "cloudy", "rainy", "snowy"]),
        "precipitation": random.uniform(0, 25),
        "wind_speed": random.uniform(0, 100),
        "humidity": random.randint(0, 100),
        "air_quality_index": random.uniform(0, 200),
    }


def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        "id": uuid.uuid4(),
        "device_id": device_id,
        "incident_id": uuid.uuid4(),
        "type": random.choice(["Accident", "Fire", "Medical", "Police Stop", "None"]),
        "timestamp": timestamp,
        "location": location,
        "status": random.choice(["Active", "Resolved"]),
        "description": "incident description",
    }


def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f"object of type{obj.__class__.__name__} is not serialized")


def delivery_report(err, msg):
    if err:
        logger.error(f"Message delivery failed - {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data["id"]),
        value=json.dumps(data, default=json_serializer).encode("utf8"),
        on_delivery=delivery_report,
    )

    producer.flush()


def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data["timestamp"])
        traffic_camera_data = generate_traffic_camera_data(
            device_id, vehicle_data["timestamp"], vehicle_data["location"], "camera-1"
        )
        weather_data = generate_weather_data(
            device_id, vehicle_data["timestamp"], vehicle_data["location"]
        )
        emergency_incident_data = generate_emergency_incident_data(
            device_id, vehicle_data["timestamp"], vehicle_data["location"]
        )
        if (
            vehicle_data["location"][0] >= NEW_YORK_COORDINATES["latitude"]
            and vehicle_data["location"][1] <= NEW_YORK_COORDINATES["longitude"]
        ):
            logger.info("Vehicle has arrived End simulation")
            break
        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)
        time.sleep(5)


if __name__ == "__main__":
    producer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "error_cb": lambda err: logger.error(f"Kafka error:{err}"),
    }

    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, "Vehicle-1")
    except KeyboardInterrupt:
        logger.error("Simulation stopped by user")
    except Exception as e:
        logger.exception(f"An Exception occurred; {e}")
