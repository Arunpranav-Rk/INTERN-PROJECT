from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sensors = ["temp_sensor_1", "temp_sensor_2", "humidity_sensor_1"]

i = 1
while True:
    data = {
        "sensor": random.choice(sensors),
        "value": round(random.uniform(20.0, 30.0), 2)
    }
    print(f"Message {i}: {data}")
    producer.send("ts-stream", value=data)
    i += 1
    time.sleep(2)
