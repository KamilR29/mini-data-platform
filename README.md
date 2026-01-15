# Mini Data Platform (Kafka • Debezium • Spark • PostgreSQL)

Projekt przedstawia prostą platformę danych opartą na architekturze streamingowej. Dane trafiają do PostgreSQL, są przechwytywane przez Debezium, publikowane w Apache Kafka i przetwarzane strumieniowo przez Apache Spark.

Całość uruchamiana jest lokalnie za pomocą Docker Compose.

## Architektura

1. PostgreSQL przechowuje dane źródłowe (customers, products, orders)
2. Debezium monitoruje zmiany w bazie i publikuje je do Kafka
3. Apache Kafka pełni rolę brokera zdarzeń
4. Apache Spark (Structured Streaming) konsumuje dane z Kafka
5. Dane są wyświetlane w czasie rzeczywistym w konsoli

## Struktura katalogów

```
kamilr29-mini-data-platform/
├── docker-compose.yml
├── requirements.txt
├── app/
│   ├── spark_customers_streaming.py
│   ├── spark_orders_streaming.py
│   └── spark_products_streaming.py
├── data/
│   ├── customers.csv
│   ├── orders.csv
│   └── products.csv
└── scripts/
    ├── kafka_json_consumer.py
    └── load_data.py
```

## Wymagania

- Docker + Docker Compose
- Python 3.9+
- Wolne porty: 5432, 8080, 8081, 8083, 9092

## Uruchomienie projektu

### 1. Start infrastruktury

```bash
docker-compose up -d
```

Spowoduje to uruchomienie:
- PostgreSQL
- Zookeeper
- Kafka
- Schema Registry
- Debezium
- Spark Master + Worker

### 2. Instalacja zależności Pythona

```bash
pip install -r requirements.txt
```

### 3. Załadowanie danych do PostgreSQL

```bash
cd scripts
python load_data.py
```

Po tej operacji tabele zostaną utworzone i wypełnione danymi z plików CSV.

### 4. Sprawdzenie danych w Kafka (opcjonalnie)

```bash
python kafka_json_consumer.py
```

Skrypt nasłuchuje topiców tworzonych przez Debezium i wypisuje zdarzenia w formacie JSON.

### 5. Uruchomienie Spark Streaming

W osobnych terminalach:

```bash
docker exec -it spark spark-submit /app/spark_customers_streaming.py
docker exec -it spark spark-submit /app/spark_products_streaming.py
docker exec -it spark spark-submit /app/spark_orders_streaming.py
```

W konsoli pojawią się dane przetwarzane w czasie rzeczywistym.

## Kafka Topics

Debezium publikuje zdarzenia do topiców:
- dbserver1.public.customers
- dbserver1.public.products
- dbserver1.public.orders

Każdy komunikat zawiera strukturę `payload.after` z aktualnym stanem rekordu.

## Spark

Spark używa Structured Streaming:
- źródło: Kafka
- format danych: JSON
- tryb: append
- output: console

## Cel projektu

Projekt edukacyjny pokazujący:
- CDC (Change Data Capture) z Debezium
- przetwarzanie strumieniowe w Spark
- integrację Kafka + PostgreSQL
- pracę z Docker Compose

## Autor

Projekt wykonany w ramach nauki platform danych i systemów streamingowych.
