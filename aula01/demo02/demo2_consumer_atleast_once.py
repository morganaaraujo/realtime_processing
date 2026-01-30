import argparse
import os
import time

import orjson
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError


def window_start_ms(ts_ms, size_ms):
    return (ts_ms // size_ms) * size_ms


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--group", default="demo2_atleast_once")
    parser.add_argument("--crash_after", type=int)
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    producer = Producer({"bootstrap.servers": args.bootstrap, "acks": 1})
    consumer.subscribe(["rt.events.raw"])

    state = {}
    processed = 0
    produced = 0
    last_report = time.perf_counter()
    last_partition = None
    last_offset = None

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
            symbol = data.get("symbol")
            event_time_ms = data.get("event_time_ms")
            qty = data.get("qty")
            price = data.get("price")
            if symbol is None or event_time_ms is None or qty is None or price is None:
                consumer.commit(message=msg, asynchronous=False)
                continue
            event_time_ms = int(event_time_ms)
            qty = float(qty)
            price = float(price)
            win_start = window_start_ms(event_time_ms, 60000)
            win_end = win_start + 60000
            key = f"{win_start}|{symbol}"

            agg = state.get(key)
            if agg is None:
                agg = {
                    "symbol": symbol,
                    "window_start_ms": win_start,
                    "window_end_ms": win_end,
                    "count": 0,
                    "sum_qty": 0.0,
                    "gmv": 0.0,
                }

            agg["count"] += 1
            agg["sum_qty"] += qty
            agg["gmv"] += price * qty
            state[key] = agg

            producer.produce(
                "rt.agg.windows", key=key.encode(), value=orjson.dumps(agg)
            )
            producer.poll(0)
            produced += 1

            processed += 1
            last_partition = msg.partition()
            last_offset = msg.offset()

            if args.crash_after and processed >= args.crash_after:
                os._exit(1)

            consumer.commit(message=msg, asynchronous=False)

            now_report = time.perf_counter()
            if now_report - last_report >= 2.0:
                part = "-" if last_partition is None else str(last_partition)
                off = "-" if last_offset is None else str(last_offset)
                print(
                    f"processed={processed} produced={produced} "
                    f"partition={part} offset={off} state={len(state)}"
                )
                last_report = now_report
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(5)
        consumer.close()


if __name__ == "__main__":
    main()
