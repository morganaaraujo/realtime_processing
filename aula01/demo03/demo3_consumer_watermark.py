import argparse
import time

import orjson
from confluent_kafka import Consumer, KafkaError, KafkaException


def window_start_ms(ts_ms, size_ms):
    return (ts_ms // size_ms) * size_ms


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="rt.events.watermark")
    parser.add_argument("--wait_for_system_duration", type=int, default=0)
    parser.add_argument("--window_size_sec", type=int, default=10)
    parser.add_argument("--group")
    parser.add_argument("--max_idle_sec", type=int, default=5)
    args = parser.parse_args()

    wait_ms = args.wait_for_system_duration * 1000
    window_ms = args.window_size_sec * 1000
    group = args.group or f"demo3_wm_{args.wait_for_system_duration}s"

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([args.topic])

    state = {}
    max_event_time_ms = None
    late_count = 0
    closed_count = 0
    last_msg_time = time.time()

    def close_windows(watermark_ms):
        nonlocal closed_count
        to_close = []
        for key, agg in state.items():
            if agg["window_end_ms"] <= watermark_ms:
                to_close.append((key, agg))
        for key, agg in to_close:
            now_ms = int(time.time() * 1000)
            close_lag = now_ms - agg["window_end_ms"]
            print(
                f"window_closed user={agg['user']} start={agg['window_start_ms']} "
                f"end={agg['window_end_ms']} count={agg['count']} sum={agg['sum']:.1f} "
                f"close_lag_ms={close_lag} watermark_ms={watermark_ms} state_size={len(state)-1}"
            )
            del state[key]
            closed_count += 1

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

            last_msg_time = time.time()
            data = orjson.loads(msg.value())
            user = data.get("user")
            amount = data.get("amount")
            event_time_ms = data.get("event_time_ms")
            if user is None or amount is None or event_time_ms is None:
                consumer.commit(message=msg, asynchronous=False)
                continue

            event_time_ms = int(event_time_ms)
            amount = float(amount)

            if max_event_time_ms is None or event_time_ms > max_event_time_ms:
                max_event_time_ms = event_time_ms
            watermark_ms = max_event_time_ms - wait_ms

            if event_time_ms < watermark_ms:
                late_count += 1
                print(
                    f"late user={user} event_time_ms={event_time_ms} "
                    f"watermark_ms={watermark_ms} state_size={len(state)}"
                )
            else:
                win_start = window_start_ms(event_time_ms, window_ms)
                win_end = win_start + window_ms
                key = f"{user}|{win_start}"
                agg = state.get(key)
                if agg is None:
                    agg = {
                        "user": user,
                        "window_start_ms": win_start,
                        "window_end_ms": win_end,
                        "count": 0,
                        "sum": 0.0,
                    }
                agg["count"] += 1
                agg["sum"] += amount
                state[key] = agg
                print(
                    f"event user={user} event_time_ms={event_time_ms} watermark_ms={watermark_ms} "
                    f"state_size={len(state)}"
                )

            close_windows(watermark_ms)
            consumer.commit(message=msg, asynchronous=False)
    finally:
        if max_event_time_ms is not None:
            final_watermark = max_event_time_ms + wait_ms + window_ms
            close_windows(final_watermark)
        consumer.close()

    print(
        f"summary wait_s={args.wait_for_system_duration} late_dropped={late_count} "
        f"windows_closed={closed_count} state_size={len(state)}"
    )


if __name__ == "__main__":
    main()
