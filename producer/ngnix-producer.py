import subprocess
import json

from kafka import KafkaProducer

from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

process = subprocess.Popen(
    ["docker", "logs", "-f", "nginx-test"],
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True
)

print("Streaming nginx logs to Kafka...")

for line in process.stdout:

    line = line.strip()

    if not line:
        continue

    # Ignore startup logs
    if line.startswith("/docker-entrypoint"):
        continue

    # Ignore nginx notice logs
    if "[notice]" in line:
        continue

    producer.send(
        KAFKA_TOPIC,
        {
            "raw_log": line
        }
    )


    producer.flush()

    print(line.strip())