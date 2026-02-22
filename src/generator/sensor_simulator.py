import json
import os
import random
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from kafka import KafkaProducer


def utc_now_iso() -> str:
    """Return current UTC time in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class SensorSimulator:
    """
    Simulates industrial IoT sensors producing telemetry events to Kafka.

    Official event schema:
    {
      "sensor_id": "SENSOR_01",
      "site": "BR-PR",
      "temperature": 55.2,
      "vibration": 1.7,
      "pressure": 4.1,
      "event_time": "2026-02-13T16:00:00Z"
    }
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        sites: Optional[List[str]] = None,
        sensors: int = 25,
    ):
        self.topic = topic
        self.sensors = sensors
        self.sites = sites or ["BR-PR", "US-TX", "IN-PN"]

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
            acks="all",
            retries=3,
        )

    def generate_event(self) -> Dict:
        """Generate a realistic sensor telemetry event."""
        sensor_id = f"SENSOR_{random.randint(0, self.sensors - 1):02d}"
        site = random.choice(self.sites)

        # Basic realistic ranges (you can refine later for "industrial realism")
        temperature = round(random.uniform(20.0, 100.0), 2)
        vibration = round(random.uniform(0.1, 5.0), 2)
        pressure = round(random.uniform(1.0, 10.0), 2)

        return {
            "sensor_id": sensor_id,
            "site": site,
            "temperature": temperature,
            "vibration": vibration,
            "pressure": pressure,
            "event_time": utc_now_iso(),
        }

    def run(self, interval_seconds: float = 1.0):
        """
        Continuously send events to Kafka.

        If running on the host machine (outside Docker):
          - use bootstrap_servers=localhost:9092

        If running inside Docker network:
          - use bootstrap_servers=kafka:29092
        """
        while True:
            event = self.generate_event()
            key = event["sensor_id"]
            self.producer.send(self.topic, key=key, value=event)
            print(f"Sent event: {event}")
            time.sleep(interval_seconds)


if __name__ == "__main__":
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "sensor-data")
    interval = float(os.getenv("SIM_INTERVAL_SECONDS", "1.0"))

    simulator = SensorSimulator(bootstrap_servers=bootstrap, topic=topic)
    simulator.run(interval_seconds=interval)
