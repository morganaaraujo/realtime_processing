import argparse
import time

import orjson
from confluent_kafka import Consumer, KafkaException, KafkaError


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--group", default="read_compacted")
    parser.add_argument("--max_idle_sec", type=int, default=5)
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(["rt.agg.windows"])

    state = {}
    last_msg_time = time.time()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if time.time() - last_msg_time >= args.max_idle_sec:
                    break
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            key = msg.key().decode() if msg.key() else None
            value = orjson.loads(msg.value()) if msg.value() else None
            if key is not None:
                state[key] = value
            last_msg_time = time.time()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    keys = sorted(state.keys())
    print(f"keys={len(keys)}")
    for k in keys[:10]:
        print(f"{k} {orjson.dumps(state[k]).decode()}")


if __name__ == "__main__":
    main()
