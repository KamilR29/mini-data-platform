from kafka import KafkaConsumer
import json


consumer = KafkaConsumer(
        'dbserver1.public.customers',
    'dbserver1.public.orders',
    'dbserver1.public.products',  
    bootstrap_servers=['localhost:9092'], 
    auto_offset_reset='earliest', 
    enable_auto_commit=True,
    group_id='my-group', 
    value_deserializer=lambda m: json.loads(m.decode('utf-8')) 
)

print("Listening for messages on topic 'dbserver1.public.customers'...")


for message in consumer:
    print("----- New Message -----")
    print(f"Partition: {message.partition}")
    print(f"Offset: {message.offset}")
    print(f"Key: {message.key}")
    print(f"Value: {json.dumps(message.value, indent=2)}")  
    print("-----------------------\n")
