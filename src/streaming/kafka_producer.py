import argparse
import json
import os
import random
from datetime import datetime, timezone
from typing import Dict

from kafka import KafkaProducer


def utc_now_iso() -> str:
    """Return current UTC time in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_event(sensor_id: str, site: str) -> Dict:
    """
    Build an event following the official contract.
    """
    return {
        "sensor_id": sensor_id,
        "site": site,
        "temperature": round(random.uniform(20.0, 100.0), 2),
        "vibration": round(random.uniform(0.1, 5.0), 2),
        "pressure": round(random.uniform(1.0, 10.0), 2),
        "event_time": utc_now_iso(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="EIDP Kafka producer (test utility).")
    parser.add_argument(
        "--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "sensor-data"))
    parser.add_argument("--count", type=int, default=10, help="Number of valid events to send.")
    parser.add_argument(
        "--send-invalid", action="store_true", help="Send one invalid JSON message."
    )
    parser.add_argument(
        "--send-duplicate", action="store_true", help="Send a duplicate event to test dedup."
    )
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: (
            json.dumps(v).encode("utf-8") if isinstance(v, dict) else str(v).encode("utf-8")
        ),
        key_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
        acks="all",
        retries=3,
    )

    sites = ["BR-PR", "US-TX", "IN-PN"]
    first_event = None

    for i in range(args.count):
        sensor_id = f"SENSOR_{random.randint(0, 24):02d}"
        site = random.choice(sites)
        event = build_event(sensor_id=sensor_id, site=site)

        if first_event is None:
            first_event = event

        producer.send(args.topic, key=sensor_id, value=event)
        print(f"Sent valid event {i + 1}/{args.count}: {event}")

    if args.send_duplicate and first_event is not None:
        # Send the same event again to validate dedup in streaming
        producer.send(args.topic, key=first_event["sensor_id"], value=first_event)
        print(f"Sent duplicate event: {first_event}")

    if args.send_invalid:
        # This should go to quarantine (invalid parsing)
        producer.send(args.topic, key="INVALID", value="{not_json")
        print("Sent invalid message (expected to be quarantined).")

    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
