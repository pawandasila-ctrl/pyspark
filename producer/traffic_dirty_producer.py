from kafka import KafkaProducer
from faker import Faker
import json
import random
import time
from datetime import datetime, timedelta, timezone

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
    retries=5,
)

roads = ["R100", "R200", "R300", "R400"]
zones = ["CBD", "AIRPORT", "TECHPARK", "SUBURB", "TRAINSTATION"]
weather = ["CLEAR", "RAIN", "FOG", "STORM"]

vehicle_ids = [fake.uuid4() for _ in range(100)]

def now_utc():
    return datetime.now(timezone.utc)

def generate_clean_event():
    return {
        "vehicle_id": random.choice(vehicle_ids),
        "road_id": random.choice(roads),
        "city_zone": random.choice(zones),
        "speed": random.randint(20, 100),
        "congestion_level": random.randint(1, 5),
        "weather": random.choice(weather),
        "event_time": now_utc().isoformat(),
    }

def generate_dirty_event():
    dirty_type = random.choice([
        "null_speed",
        "negative_speed",
        "extreme_speed",
        "duplicate_vehicle",
        "late_event",
        "future_event",
        "wrong_datatype",
        "schema_drift",
        "corrupt_json",
    ])

    event = generate_clean_event()

    if dirty_type == "null_speed":
        event["speed"] = None

    elif dirty_type == "negative_speed":
        event["speed"] = -40

    elif dirty_type == "extreme_speed":
        event["speed"] = 420

    elif dirty_type == "duplicate_vehicle":
        event["vehicle_id"] = random.choice(vehicle_ids)

    elif dirty_type == "late_event":
        event["event_time"] = (
            now_utc() - timedelta(minutes=random.randint(10, 120))
        ).isoformat()

    elif dirty_type == "future_event":
        event["event_time"] = (
            now_utc() + timedelta(minutes=random.randint(5, 60))
        ).isoformat()

    elif dirty_type == "wrong_datatype":
        event["speed"] = "FAST"

    elif dirty_type == "schema_drift":
        event["road_condition"] = random.choice([
            "GOOD", "BAD", "UNDER_CONSTRUCTION"
        ])

    elif dirty_type == "corrupt_json":
        # Special marker used by the JSON serializer below.
        return {"raw": "###CORRUPTED_EVENT###"}

    event["injected_issue"] = dirty_type
    return event

def on_success(record_metadata):
    print(
        f"DELIVERED topic={record_metadata.topic} "
        f"partition={record_metadata.partition} "
        f"offset={record_metadata.offset}"
    )

def on_error(exception):
    print(f"KAFKA DELIVERY FAILED: {exception}")

try:
    while True:
        if random.random() < 0.7:
            event = generate_clean_event()
        else:
            event = generate_dirty_event()

        future = producer.send("traffic-topic", value=event)
        future.add_callback(on_success)
        future.add_errback(on_error)

        print("SENT:", event)
        time.sleep(random.uniform(0.5, 1.5))

except KeyboardInterrupt:
    print("Stopping producer...")

finally:
    producer.flush()
    producer.close()
    print("Producer stopped.")
