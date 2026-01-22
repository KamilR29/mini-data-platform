import json
import psycopg2
from kafka import KafkaProducer

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "mydb",
    "user": "postgres",
    "password": "postgres"
}

KAFKA_TOPIC = "customers"
KAFKA_BOOTSTRAP = "localhost:9092"


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        SELECT
            customer_id,
            age,
            country,
            gender,
            annual_income_usd,
            total_orders,
            avg_order_value,
            churn
        FROM customers;
    """)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    for row in cur.fetchall():
        message = {
            "customer_id": row[0],
            "age": int(row[1]),
            "country": row[2],
            "gender": row[3],
            "annual_income_usd": float(row[4]),
            "total_orders": int(row[5]),
            "avg_order_value": float(row[6]),
            "churn": int(row[7])
        }

        producer.send(KAFKA_TOPIC, value=message)

    producer.flush()
    cur.close()
    conn.close()

    print("Customers sent to Kafka topic 'customers'")


if __name__ == "__main__":
    main()
