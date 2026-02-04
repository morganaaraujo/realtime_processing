import argparse
import time

import matplotlib.pyplot as plt

import orjson
from confluent_kafka import Consumer, KafkaError, KafkaException


def classify(event_time_ms, processing_time_ms, watermark_ms, on_time_ms):
    delay = processing_time_ms - event_time_ms
    if event_time_ms < watermark_ms:
        return "red"
    if delay > on_time_ms:
        return "orange"
    return "green"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="rt.events.etpt")
    parser.add_argument("--group", default="demo5_exercise")
    parser.add_argument("--records", type=int, default=50)
    parser.add_argument("--watermark_min", type=int, default=5)
    parser.add_argument("--window_min", type=int, default=10)
    parser.add_argument("--on_time_sec", type=int, default=60)
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([args.topic])

    watermark_ms = args.watermark_min * 60 * 1000
    on_time_ms = args.on_time_sec * 1000

    max_event_time_ms = None
    counts = {"green": 0, "orange": 0, "red": 0}
    
    # listas de dados
    plot_data = { "x": [], "y": [], "colors": []}

    try:
        processed = 0
        while processed < args.records:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            data = orjson.loads(msg.value())
            event_time_ms = data.get("event_time_ms")
            processing_time_ms = data.get("processing_time_ms")
            if event_time_ms is None or processing_time_ms is None:
                consumer.commit(message=msg, asynchronous=False)
                continue

            event_time_ms = int(event_time_ms)
            processing_time_ms = int(processing_time_ms)

            if max_event_time_ms is None or event_time_ms > max_event_time_ms:
                max_event_time_ms = event_time_ms

            current_watermark = max_event_time_ms - watermark_ms
            color = classify(event_time_ms, processing_time_ms, current_watermark, on_time_ms)
            # Dados de cada mensagem processada
            plot_data["x"].append(processing_time_ms)
            plot_data["y"].append(event_time_ms)
            plot_data["colors"].append(color)
            counts[color] += 1
            processed += 1

            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()

    print(f"green={counts['green']} orange={counts['orange']} red={counts['red']}")
    print(f"dlq_count={counts['red']}")

    # Gráfico
    plt.figure(figsize=(10, 6))
    plt.scatter(plot_data["x"], plot_data["y"], c=plot_data["colors"], alpha=0.6)
    plt.title("Processing Time vs Event Time")
    plt.xlabel("Processing Time (ms)")
    plt.ylabel("Event Time (ms)")
    plt.grid(True)
    plt.show()

    # Gráfico
    plt.figure(figsize=(10, 6))
    
    # 1. Desenha os pontos
    plt.scatter(plot_data["x"], plot_data["y"], c=plot_data["colors"], alpha=0.6, label="Eventos")
    
    # 2. Adiciona a linha de referência y = x 
    if plot_data["x"]:
        lims = [
            min(min(plot_data["x"]), min(plot_data["y"])),  
            max(max(plot_data["x"]), max(plot_data["y"])),
        ]
        plt.plot(lims, lims, color='gray', linestyle='--', alpha=0.5, label="Referência y=x")

    plt.title("Processing Time vs Event Time")
    plt.xlabel("Processing Time (ms)")
    plt.ylabel("Event Time (ms)")
    plt.grid(True)
    plt.legend()

    # 3. Salva o arquivo
    plt.savefig("resultado_processamento.png")
    print("Gráfico salvo:'resultado_processamento.png'")
    
    plt.show()


if __name__ == "__main__":
    main()
