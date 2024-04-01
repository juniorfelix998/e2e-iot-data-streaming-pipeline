import os
import logging
import random
import uuid

import simplejson as json

from confluent_kafka import SerializingProducer
from datetime import datetime, timedelta

from dotenv import load_dotenv


load_dotenv()

logger = logging.getLogger(__name__)


SAN_FRANCISCO_COORDINATES = {"latitude": 37.773972, "longitude": -122.431297}
NEW_YORK_COORDINATES = {"latitude": 40.730610, "longitude": -73.935242}


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


def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        print(vehicle_data)
        break


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
