from kafka import KafkaConsumer
import json

# Tworzymy consumer'a
consumer = KafkaConsumer(
        'dbserver1.public.customers',
    'dbserver1.public.orders',
    'dbserver1.public.products',  # topic, którego słuchamy
    bootstrap_servers=['localhost:9092'],  # bo python uruchamiasz z hosta
    auto_offset_reset='earliest',  # czytamy od początku
    enable_auto_commit=True,
    group_id='my-group',  # nazwa grupy (może być dowolna)
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # deserializacja JSON
)

print("✅ Listening for messages on topic 'dbserver1.public.customers'...")

# Pętla odbierająca wiadomości
for message in consumer:
    print("----- New Message -----")
    print(f"Partition: {message.partition}")
    print(f"Offset: {message.offset}")
    print(f"Key: {message.key}")
    print(f"Value: {json.dumps(message.value, indent=2)}")  # ładny JSON
    print("-----------------------\n")
