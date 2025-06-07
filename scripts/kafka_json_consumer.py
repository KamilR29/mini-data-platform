from kafka import KafkaConsumer
import json

# konfiguracja consumer-a
consumer = KafkaConsumer(
    'postgres.public.test',
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id='json-consumer-group-2',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# print("Listening to topic 'postgres.public.test'... Press Ctrl+C to exit.\n")

try:
    for message in consumer:
        print("=== New message ===")
        print(f"Partition: {message.partition}")
        print(f"Offset: {message.offset}")
        print(f"Key: {message.key}")
        print(f"Value: {json.dumps(message.value, indent=4)}")  # pretty print JSON
        print("===================")
except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()
