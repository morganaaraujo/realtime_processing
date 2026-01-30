import argparse
import random
import time
import uuid

import orjson
from confluent_kafka import Producer


def parse_symbols(value):
    value = value.strip()
    if value.isdigit():
        n = int(value)
        return [f"SYM{i+1}" for i in range(n)]
    return [s.strip() for s in value.split(",") if s.strip()]


def percentile(values, p):
    if not values:
        return 0.0
    values = sorted(values)
    k = int((p / 100) * (len(values) - 1))
    return float(values[k])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--rate", type=float, default=100.0)
    parser.add_argument("--late_rate", type=float, default=0.08)
    parser.add_argument("--late_max_sec", type=int, default=120)
    parser.add_argument("--dup_rate", type=float, default=0.02)
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT,SOLUSDT")
    parser.add_argument("--seed", type=int)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    symbols = parse_symbols(args.symbols)
    producer = Producer({"bootstrap.servers": args.bootstrap, "acks": 1})

    sent_total = 0
    dup_total = 0
    delays = []
    last_event = None

    interval = 1.0 / max(args.rate, 0.1)
    next_send = time.perf_counter()
    last_report = time.perf_counter()
    sent_since = 0

    try:
        while True:
            now = time.perf_counter()
            if now < next_send:
                time.sleep(min(next_send - now, 0.001))
                continue
            next_send += interval

            if last_event is not None and rng.random() < args.dup_rate:
                event = last_event
                dup_total += 1
            else:
                sent_at_ms = int(time.time() * 1000)
                if rng.random() < args.late_rate:
                    offset_ms = int(rng.uniform(1, max(args.late_max_sec, 1)) * 1000)
                    event_time_ms = sent_at_ms - offset_ms
                else:
                    event_time_ms = sent_at_ms
                event = {
                    "event_id": str(uuid.uuid4()),
                    "symbol": rng.choice(symbols),
                    "price": round(rng.uniform(10, 50000), 2),
                    "qty": round(rng.uniform(0.001, 5.0), 6),
                    "event_time_ms": event_time_ms,
                    "sent_at_ms": sent_at_ms,
                }
                last_event = event

            producer.produce(
                "rt.events.raw",
                key=event["symbol"].encode(),
                value=orjson.dumps(event),
            )
            producer.poll(0)

            delay_ms = event["sent_at_ms"] - event["event_time_ms"]
            delays.append(delay_ms)
            sent_total += 1
            sent_since += 1

            now_report = time.perf_counter()
            if now_report - last_report >= 2.0:
                elapsed = now_report - last_report
                rate = sent_since / elapsed if elapsed > 0 else 0.0
                dmin = min(delays) if delays else 0.0
                davg = sum(delays) / len(delays) if delays else 0.0
                dp95 = percentile(delays, 95)
                print(
                    f"sent={sent_total} rate={rate:.1f}/s dup={dup_total} "
                    f"delay_ms min={dmin:.0f} avg={davg:.0f} p95={dp95:.0f}"
                )
                delays.clear()
                sent_since = 0
                last_report = now_report
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(5)


if __name__ == "__main__":
    main()
