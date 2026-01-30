# Aula 01 — Streaming com Kafka (Python)

## Requisitos

- Python 3.10+
- Docker

Instalar dependencias:

```bash
python -m pip install -r aula01/requirements.txt
```

## 1. Subir Kafka

```bash
cd realtime
docker compose up -d
```

## 2. Criar topicos

```bash
python aula01/create_topics.py
```

## 3. Demo 1 — Produtor com atraso + consumidor ingenuo

Terminal A:

```bash
python aula01/demo01/demo1_consumer_naive.py
```

Terminal B:

```bash
python aula01/demo01/demo1_producer.py --rate 300
```

Caso desafio:

```bash
python aula01/demo01/demo1_producer.py --rate 300 --late_rate 0.15 --late_max_sec 300 --dup_rate 0.05
```

Medir:

- quantos duplicados chegaram
- distribuicao de atraso (now - event_time)

## 4. Demo 2 — At-least-once

Terminal A:

```bash
python aula01/demo02/demo2_consumer_atleast_once.py --crash_after 500
```

Terminal B:

```bash
python aula01/demo02/demo2_producer.py --rate 300
```

Reinicie o consumer apos o crash e observe duplicacao.

Leitura do compacted:

```bash
python aula01/demo02/read_compacted_agg.py
```

## 5. Demo 2 — Effectively-once

Remover checkpoint:

```bash
rm -f realtime/aula01/checkpoints/demo2.json
```

Terminal A:

```bash
python aula01/demo02/demo2_consumer_effectively_once.py --crash_after 500
```

Terminal B:

```bash
python aula01/demo02/demo2_producer.py --rate 300 --dup_rate 0.05 --late_rate 0.15 --late_max_sec 300
```

Reinicie o consumer e verifique consistencia com:

```bash
python aula01/demo02/read_compacted_agg.py
```

## 6. Demo 3 — Watermark na pratica (event-time, janelas, late stream)

Gerar o stream deterministico:

```bash
python aula01/demo03/demo3_producer.py
```

Rodar com wait_for_system_duration = 0s:

```bash
python aula01/demo03/demo3_consumer_watermark.py --wait_for_system_duration 0
```

Rodar com wait_for_system_duration = 5s:

```bash
python aula01/demo03/demo3_consumer_watermark.py --wait_for_system_duration 5
```

Rodar com wait_for_system_duration = 20s:

```bash
python aula01/demo03/demo3_consumer_watermark.py --wait_for_system_duration 20
```

Observe nos prints:

- janelas fechadas
- eventos marcados como late
- state_size (quantas janelas abertas)

## Stack usada nas demos

Python 3.10+ + Kafka (confluent-kafka) + orjson em todas as 3 demos.
