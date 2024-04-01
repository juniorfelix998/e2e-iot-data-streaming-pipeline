import os

from confluent_kafka import SerializingProducer
import simplejson as json

from dotenv import load_dotenv


load_dotenv()


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
