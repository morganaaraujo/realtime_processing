import argparse
from confluent_kafka.admin import AdminClient, NewTopic


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    args = parser.parse_args()

    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    existing = admin.list_topics(timeout=10).topics

    topics = []
    if "rt.events.raw" not in existing:
        topics.append(
            NewTopic(
                "rt.events.raw",
                num_partitions=3,
                replication_factor=1,
                config={"min.insync.replicas": "1"},
            )
        )
    if "rt.agg.windows" not in existing:
        topics.append(
            NewTopic(
                "rt.agg.windows",
                num_partitions=3,
                replication_factor=1,
                config={
                    "cleanup.policy": "compact",
                    "min.insync.replicas": "1",
                },
            )
        )
    if "rt.events.late" not in existing:
        topics.append(
            NewTopic(
                "rt.events.late",
                num_partitions=3,
                replication_factor=1,
                config={"min.insync.replicas": "1"},
            )
        )
    if "rt.events.watermark" not in existing:
        topics.append(
            NewTopic(
                "rt.events.watermark",
                num_partitions=1,
                replication_factor=1,
                config={"min.insync.replicas": "1"},
            )
        )

    if not topics:
        print("topics already exist")
        return

    futures = admin.create_topics(topics)
    for name, future in futures.items():
        try:
            future.result()
            print(f"created {name}")
        except Exception as exc:
            print(f"{name} error: {exc}")


if __name__ == "__main__":
    main()
