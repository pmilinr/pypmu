import json
import psycopg2
from confluent_kafka import Consumer

# PostgreSQL connection string
CONNECTION = "postgres://postgres:password@localhost:30000/postgres"

# SQL INSERT statement
insert_query = """INSERT INTO pmu_test 
                (pmu_id, time, measurements) 
                VALUES (%s, to_timestamp(%s), %s);"""

# Redpanda/Kafka consumer configuration
conf = {
    'bootstrap.servers': '127.0.0.1:9092',  # Use the Kafka port, not Pandaproxy
    'group.id': 'pmu_measurements-group',
    'auto.offset.reset': 'earliest'
    # Remove SASL configs unless you have authentication enabled
}

consumer = Consumer(conf)
topic = "pmu_measurements"  # Change to your topic name
consumer.subscribe([topic])

print("Listening for messages on Redpanda topic...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        try:
            payload = msg.value().decode('utf-8')
            print(f"Message received: {payload}")
            data = json.loads(payload)
            record = (
                data['pmu_id'],
                data['time'],
                json.dumps(data['measurements'])
            )
            with psycopg2.connect(CONNECTION) as conn:
                with conn.cursor() as cur:
                    cur.execute(insert_query, record)
                conn.commit()
            print(f"Inserted row: {record}")
        except Exception as e:
            print(f"Failed to insert data: {e}")

except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()