import argparse
import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

PAGE_TITLES = [
    "Apache_Spark",
    "Python",
    "Machine_Learning",
    "Big_Data",
    "Kubernetes",
    "Data_Engineering",
    "AI",
    "Open_Source",
    "Cloud_Computing",
    "Streaming_Data",
]

USERS = ["alice", "bob", "carol", "dave", "eve", "frank"]


def build_event(index: int):
    return {
        "page_title": random.choice(PAGE_TITLES),
        "user": random.choice(USERS),
        "edit_size": random.randint(10, 5000),
        "event_time": datetime.utcnow().isoformat() + "Z",
        "edit_id": f"edit-{index:05d}",
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Produce sample Wikipedia-edit events to Kafka.")
    parser.add_argument("--bootstrap", default="kafka:9092", help="Kafka bootstrap server")
    parser.add_argument("--topic", default="wikipedia-edits", help="Kafka topic name")
    parser.add_argument("--count", type=int, default=100, help="Number of events to send")
    parser.add_argument("--delay", type=float, default=0.2, help="Seconds to wait between events")
    return parser.parse_args()


def main():
    args = parse_args()

    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        linger_ms=10,
    )

    print(f"Producing {args.count} events to topic '{args.topic}' at {args.bootstrap}")

    for idx in range(1, args.count + 1):
        event = build_event(idx)
        producer.send(args.topic, event)
        print(f"Sent event {idx}: {event['page_title']} by {event['user']}")
        time.sleep(args.delay)

    producer.flush()
    producer.close()
    print("Finished producing events.")


if __name__ == "__main__":
    main()
