import argparse
import os
import signal
import time

import orjson
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError


def window_start_ms(ts_ms, size_ms):
    return (ts_ms // size_ms) * size_ms


def load_checkpoint(path, ttl_ms):
    if not os.path.exists(path):
        return {}, {}, 0, 0
    try:
        with open(path, "rb") as f:
            data = orjson.loads(f.read())
        state = data.get("state", {})
        dedup = data.get("dedup", {})
        processed = int(data.get("processed", 0))
        produced = int(data.get("produced", 0))
    except Exception:
        return {}, {}, 0, 0

    now_ms = int(time.time() * 1000)
    cutoff = now_ms - ttl_ms
    dedup = {k: v for k, v in dedup.items() if v >= cutoff}
    return state, dedup, processed, produced


def save_checkpoint(path, state, dedup, processed, produced):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    payload = {
        "state": state,
        "dedup": dedup,
        "processed": processed,
        "produced": produced,
    }
    with open(tmp, "wb") as f:
        f.write(orjson.dumps(payload))
    os.replace(tmp, path)


def cleanup_dedup(dedup, ttl_ms):
    now_ms = int(time.time() * 1000)
    cutoff = now_ms - ttl_ms
    to_delete = [k for k, v in dedup.items() if v < cutoff]
    for k in to_delete:
        del dedup[k]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--group", default="demo2_effectively_once")
    parser.add_argument("--dedup_ttl_sec", type=int, default=600)
    parser.add_argument(
        "--checkpoint_path",
        default="realtime/aula01/checkpoints/demo2.json",
    )
    parser.add_argument("--checkpoint_every", type=int, default=2000)
    parser.add_argument("--crash_after", type=int)
    args = parser.parse_args()

    ttl_ms = args.dedup_ttl_sec * 1000
    state, dedup, processed, produced = load_checkpoint(args.checkpoint_path, ttl_ms)

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

    last_report = time.perf_counter()
    last_partition = None
    last_offset = None
    last_checkpoint = processed

    should_stop = False

    def handle_signal(signum, frame):
        nonlocal should_stop
        should_stop = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        while not should_stop:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            data = orjson.loads(msg.value())
            event_id = data.get("event_id")
            symbol = data.get("symbol")
            event_time_ms = data.get("event_time_ms")
            qty = data.get("qty")
            price = data.get("price")
            if (
                event_id is None
                or symbol is None
                or event_time_ms is None
                or qty is None
                or price is None
            ):
                consumer.commit(message=msg, asynchronous=False)
                continue
            processed += 1
            now_ms = int(time.time() * 1000)

            if event_id in dedup:
                consumer.commit(message=msg, asynchronous=False)
            else:
                dedup[event_id] = now_ms
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

                if args.crash_after and processed >= args.crash_after:
                    save_checkpoint(
                        args.checkpoint_path, state, dedup, processed, produced
                    )
                    os._exit(1)

                consumer.commit(message=msg, asynchronous=False)

            last_partition = msg.partition()
            last_offset = msg.offset()

            if processed - last_checkpoint >= args.checkpoint_every:
                save_checkpoint(args.checkpoint_path, state, dedup, processed, produced)
                last_checkpoint = processed

            if processed % 1000 == 0:
                cleanup_dedup(dedup, ttl_ms)

            now_report = time.perf_counter()
            if now_report - last_report >= 2.0:
                part = "-" if last_partition is None else str(last_partition)
                off = "-" if last_offset is None else str(last_offset)
                print(
                    f"processed={processed} produced={produced} "
                    f"partition={part} offset={off} state={len(state)} dedup={len(dedup)}"
                )
                last_report = now_report
    finally:
        save_checkpoint(args.checkpoint_path, state, dedup, processed, produced)
        producer.flush(5)
        consumer.close()


if __name__ == "__main__":
    main()
