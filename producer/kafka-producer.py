from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x).encode()
)

producer.send(
    "nginx_logs",
    {
        "ip":"192.168.1.10",
        "status":500
    }
)

producer.flush()

print("Message Sent")