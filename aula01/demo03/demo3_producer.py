import argparse
import time
import uuid

import orjson
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


def ensure_topic(bootstrap, topic):
    admin = AdminClient({"bootstrap.servers": bootstrap})
    md = admin.list_topics(timeout=10)
    if topic in md.topics:
        return
    futures = admin.create_topics(
        [
            NewTopic(
                topic,
                num_partitions=1,
                replication_factor=1,
                config={"min.insync.replicas": "1"},
            )
        ]
    )
    for future in futures.values():
        future.result()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="rt.events.watermark")
    parser.add_argument("--sleep_scale", type=float, default=1.0)
    args = parser.parse_args()

    ensure_topic(args.bootstrap, args.topic)

    producer = Producer({"bootstrap.servers": args.bootstrap, "acks": 1})

    base_sec = (int(time.time()) // 10) * 10
    base_ms = base_sec * 1000

    events = [
        {"user": "alice", "amount": 5.0, "event_time_offset": 0, "sleep": 0},
        {"user": "bob", "amount": 3.0, "event_time_offset": 4, "sleep": 1},
        {"user": "alice", "amount": 7.0, "event_time_offset": 12, "sleep": 1},
        {"user": "alice", "amount": 2.0, "event_time_offset": 8, "sleep": 1},
        {"user": "bob", "amount": 4.0, "event_time_offset": 16, "sleep": 1},
        {"user": "bob", "amount": 6.0, "event_time_offset": 5, "sleep": 1},
        {"user": "alice", "amount": 1.0, "event_time_offset": 26, "sleep": 1},
        {"user": "alice", "amount": 10.0, "event_time_offset": 2, "sleep": 1},
    ]

    total = 0
    for ev in events:
        if ev["sleep"] > 0:
            time.sleep(ev["sleep"] * args.sleep_scale)
        sent_at_ms = int(time.time() * 1000)
        event_time_ms = base_ms + int(ev["event_time_offset"] * 1000)
        payload = {
            "event_id": str(uuid.uuid4()),
            "user": ev["user"],
            "amount": ev["amount"],
            "event_time_ms": event_time_ms,
            "processing_time_ms": sent_at_ms,
        }
        producer.produce(
            args.topic,
            key=ev["user"].encode(),
            value=orjson.dumps(payload),
        )
        producer.poll(0)
        total += 1
        print(
            f"sent user={ev['user']} amount={ev['amount']} "
            f"event_time_ms={event_time_ms} processing_time_ms={sent_at_ms}"
        )

    producer.flush(5)
    print(f"total_sent={total}")


if __name__ == "__main__":
    main()
