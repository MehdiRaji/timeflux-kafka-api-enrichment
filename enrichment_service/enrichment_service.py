import json
import os
import time
import logging
import requests

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from requests.exceptions import RequestException

# ======================
# Configuration
# ======================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

INPUT_TOPIC = os.getenv("INPUT_TOPIC", "raw-turbine-data")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "enriched-turbine-data")

REFERENTIAL_API_URL = os.getenv(
    "REFERENTIAL_API_URL",
    "http://referential-api:5000/turbine"
)

RETRY_DELAY = 5

# ======================
# Logging
# ======================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | enrichment | %(message)s",
)
logger = logging.getLogger(__name__)

# ======================
# Kafka init producer + consumer
# ======================
def create_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="enrichment-service",
            )
            logger.info("Kafka consumer connected")
            return consumer
        except NoBrokersAvailable:
            logger.warning("Kafka not ready (consumer), retrying...")
            time.sleep(RETRY_DELAY)


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
            logger.warning("Kafka not ready (producer), retrying...")
            time.sleep(RETRY_DELAY)

# ======================
# API call
# ======================
def fetch_metadata(turbine_id: str) -> dict | None:
    try:
        response = requests.get(f"{REFERENTIAL_API_URL}/{turbine_id}", timeout=5)
        if response.status_code == 200:
            return response.json()
        else:
            logger.warning(f"Metadata not found for turbine {turbine_id}")
            return None
    except RequestException as e:
        logger.error(f"API error for turbine {turbine_id}: {e}")
        return None

# ======================
# Main loop
# ======================
def main():
    consumer = create_consumer()
    producer = create_producer()

    logger.info(
        f"Enrichment service started | {INPUT_TOPIC} -> {OUTPUT_TOPIC}"
    )

    for message in consumer:
        raw_data = message.value
        turbine_id = raw_data.get("turbine_id")

        if not turbine_id:
            logger.warning("Invalid message (missing turbine_id)")
            continue

        metadata = fetch_metadata(turbine_id)

        if not metadata:
            logger.warning(f"Skipping message for turbine {turbine_id}")
            continue

        enriched_data = {
            **raw_data,
            "country": metadata.get("country"),
        }

        producer.send(OUTPUT_TOPIC, value=enriched_data)
        producer.flush()

        logger.info(f"Enriched message sent: {enriched_data}")

# ======================
if __name__ == "__main__":
    main()
