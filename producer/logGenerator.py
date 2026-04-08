import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

LOG_LEVELS = ["INFO", "WARN", "ERROR"]
URLS = ["/api/login", "/api/products", "/api/cart", "/api/checkout"]
USER_AGENTS = ["Chrome", "Firefox", "Safari", "Edge"]
IPS = ["10.0.0." + str(i) for i in range(1, 50)]

def generate_log():
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "level": random.choice(LOG_LEVELS),
        "ip": random.choice(IPS),
        "url": random.choice(URLS),
        "user_agent": random.choice(USER_AGENTS),
        "latency_ms": random.randint(10, 4000)
    }

while True:
    msg = generate_log()
    producer.send("logs_topic", msg)
    print("Sent:", msg)
    time.sleep(0.5)

    