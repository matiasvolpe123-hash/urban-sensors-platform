"""Productor: simula sensores urbanos y envía sus lecturas al tópico de Kafka.

Cada lectura es un JSON con: sensor_id, temperature, humidity,
air_quality_index y timestamp (UTC, ISO 8601).

Toda la configuración llega por variables de entorno (ConfigMaps en Kubernetes):
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, PRODUCER_NUM_SENSORS,
    PRODUCER_INTERVAL_SECONDS
"""

import json
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient


def env(name, default=None):
    """Lee una variable de entorno; si es obligatoria y falta, aborta con un mensaje claro."""
    value = os.environ.get(name, default)
    if value is None:
        print(f"[ERROR] Falta la variable de entorno {name}", file=sys.stderr)
        sys.exit(2)
    return value


def make_sensors(count):
    """Cada sensor tiene valores base propios para que los promedios por sensor sean distintos."""
    sensors = {}
    for i in range(1, count + 1):
        sensors[f"sensor-{i:03d}"] = {
            "temperature": random.uniform(12, 30),
            "humidity": random.uniform(35, 75),
            "air_quality_index": random.uniform(20, 120),
        }
    return sensors


def build_reading(sensor_id, base, rng=random):
    """Genera una lectura con ruido alrededor de los valores base del sensor."""
    return {
        "sensor_id": sensor_id,
        "temperature": round(base["temperature"] + rng.gauss(0, 1.5), 2),
        "humidity": round(min(100.0, max(0.0, base["humidity"] + rng.gauss(0, 4))), 2),
        "air_quality_index": max(0, int(base["air_quality_index"] + rng.gauss(0, 12))),
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def wait_for_topic(bootstrap, topic):
    """Espera a que el broker responda y el tópico exista (lo crea el Job kafka-create-topic)."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    while True:
        try:
            metadata = admin.list_topics(timeout=5)
            if topic in metadata.topics and metadata.topics[topic].error is None:
                partitions = len(metadata.topics[topic].partitions)
                print(f"Tópico '{topic}' disponible con {partitions} partición(es).", flush=True)
                return
            print(f"Esperando a que exista el tópico '{topic}'...", flush=True)
        except KafkaException as exc:
            print(f"Esperando al broker en {bootstrap}: {exc}", flush=True)
        time.sleep(3)


def main():
    bootstrap = env("KAFKA_BOOTSTRAP_SERVERS")
    topic = env("KAFKA_TOPIC")
    num_sensors = int(env("PRODUCER_NUM_SENSORS", "5"))
    interval = float(env("PRODUCER_INTERVAL_SECONDS", "1"))

    running = True

    def stop(_signum, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    wait_for_topic(bootstrap, topic)

    producer = Producer({"bootstrap.servers": bootstrap, "client.id": "urban-sensors-producer"})
    sensors = make_sensors(num_sensors)
    print(f"Enviando lecturas de {num_sensors} sensores a '{topic}' cada {interval} s.", flush=True)

    def on_delivery(err, msg):
        if err is not None:
            print(f"[ERROR] Entrega fallida: {err}", file=sys.stderr, flush=True)
        else:
            print(
                f"[ENVIADO] partición={msg.partition()} offset={msg.offset()} "
                f"{msg.value().decode()}",
                flush=True,
            )

    sent = 0
    try:
        while running:
            sensor_id = random.choice(list(sensors))
            reading = build_reading(sensor_id, sensors[sensor_id])
            producer.produce(
                topic,
                key=sensor_id.encode(),
                value=json.dumps(reading).encode(),
                on_delivery=on_delivery,
            )
            producer.poll(0)
            sent += 1
            time.sleep(interval)
    finally:
        producer.flush(10)
        print(f"Productor finalizado. Lecturas enviadas: {sent}.", flush=True)


if __name__ == "__main__":
    main()
