import json
import os
import time
import random
import logging
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ======================
# Configuration
# ======================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "raw-turbine-data")

EVENTS_PER_MINUTE = int(os.getenv("EVENTS_PER_MINUTE", "12"))
SLEEP_TIME = 60 / EVENTS_PER_MINUTE

RETRY_DELAY = 5

TURBINE_IDS = ["T123", "T124", "T125"]

# ======================
# Logging 
# ======================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | producer | %(message)s",
)
logger = logging.getLogger(__name__)

# ======================
# Kafka producer init 
# ======================
def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            logger.info("Kafka producer connected")
            return producer

        except NoBrokersAvailable:
            logger.warning("Kafka not ready, retrying in 5 seconds...")
            time.sleep(RETRY_DELAY)

# ======================
# Main loop
# ======================
def main():
    producer = create_producer()
    logger.info(f"Producing to topic '{TOPIC}' at {EVENTS_PER_MINUTE} events/min")

    while True:
        data = {
            "turbine_id": random.choice(TURBINE_IDS),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "value": round(random.uniform(50.0, 200.0), 2),
            "unit": "kW",
        }

        producer.send(TOPIC, value=data)
        producer.flush()

        logger.info(f"Sent data: {data}")
        time.sleep(SLEEP_TIME)


if __name__ == "__main__":
    main()
