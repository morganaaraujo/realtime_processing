import argparse
import time

import orjson
from confluent_kafka import Consumer, KafkaException, KafkaError


def percentile(values, p):
    if not values:
        return 0.0
    values = sorted(values)
    k = int((p / 100) * (len(values) - 1))
    return float(values[k])


def delay_bucket(delay_ms):
    if delay_ms < 1000:
        return 0
    if delay_ms < 5000:
        return 1
    if delay_ms < 15000:
        return 2
    if delay_ms < 60000:
        return 3
    return 4


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--group", default="demo1_naive")
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(["rt.events.raw"])

    seen = set()
    delays = []
    buckets = [0, 0, 0, 0, 0]
    last_report = time.perf_counter()
    msgs_since = 0
    dup_since = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            data = orjson.loads(msg.value())
            event_id = data.get("event_id")
            now_ms = int(time.time() * 1000)
            delay_ms = now_ms - int(data.get("event_time_ms", now_ms))

            if event_id in seen:
                dup_since += 1
            else:
                seen.add(event_id)

            delays.append(delay_ms)
            buckets[delay_bucket(delay_ms)] += 1
            msgs_since += 1

            now_report = time.perf_counter()
            if now_report - last_report >= 2.0:
                elapsed = now_report - last_report
                rate = msgs_since / elapsed if elapsed > 0 else 0.0
                p50 = percentile(delays, 50)
                p95 = percentile(delays, 95)
                dist = (
                    f"<1s:{buckets[0]} 1-5s:{buckets[1]} 5-15s:{buckets[2]} "
                    f"15-60s:{buckets[3]} >60s:{buckets[4]}"
                )
                print(
                    f"rate={rate:.1f}/s p50={p50:.0f}ms p95={p95:.0f}ms "
                    f"dups={dup_since} dist={dist}"
                )
                delays.clear()
                buckets = [0, 0, 0, 0, 0]
                msgs_since = 0
                dup_since = 0
                last_report = now_report
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
