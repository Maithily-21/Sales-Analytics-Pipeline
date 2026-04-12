from kafka import KafkaProducer
import json, time, random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="host.docker.internal:29092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {
        "order_id": random.randint(1000,9999),
        "order_date": str(datetime.now()),
        "region": random.choice(["West","East","South","Central"]),
        "sales": round(random.uniform(100,500),2),
        "profit": round(random.uniform(-50,150),2)
    }

    producer.send("sales_topic", data)
    print("Sent:", data)
    time.sleep(2)