# Mini Data Platform – Customers Pipeline

Projekt przedstawia prostą, lokalną platformę danych opartą o Dockera. Pokazuje pełny przepływ danych od źródła, przez warstwy Bronze/Silver/Gold, walidację jakości, aż po trening modelu i logowanie wyników w MLflow.

Całość działa lokalnie i jest uruchamiana przez `docker-compose`.

---

## Architektura (w skrócie)

- PostgreSQL – źródło danych (opcjonalnie)
- Kafka – transport zdarzeń (customers)
- MinIO (S3) – warstwy danych:
  - bronze – dane surowe
  - silver – dane oczyszczone
  - gold – dane do ML
  - mlflow – artefakty modeli
- Spark – transformacje danych
- Great Expectations – walidacja jakości danych
- Airflow – orkiestracja pipeline’u
- MLflow – tracking eksperymentów i modeli

---

## Wymagania

- Docker
- Docker Compose
- Wolne porty: 8080, 8088, 9000, 9001, 5001

---

## Uruchomienie projektu

### 1. Zatrzymanie starych kontenerów

```bash
docker compose down
```

Aby wystartować całkowicie od zera (usunąć wolumeny):

```bash
docker compose down -v
```

---

### 2. Budowa obrazów Airflow

```bash
docker compose build airflow-init airflow-webserver airflow-scheduler
```

---

### 3. Start platformy

```bash
docker compose up -d
```

Sprawdzenie statusu:

```bash
docker compose ps
```

---

## Dostępne interfejsy

- Airflow: http://localhost:8088  
  login: admin / admin

- Spark UI: http://localhost:8080

- MinIO: http://localhost:9001  
  login: minio / minio12345

- MLflow: http://localhost:5001

---

## Buckety w MinIO

Przed uruchomieniem pipeline’u należy utworzyć buckety:

- bronze
- silver
- gold
- mlflow

Można to zrobić ręcznie przez UI MinIO.

---

## Pipeline Airflow

DAG: `customers_end_to_end`

Kolejne kroki:
1. Bronze → Silver (Spark)
2. Walidacja danych Silver (Great Expectations)
3. Silver → Gold (Spark)
4. Trening modelu + logowanie do MLflow

Pipeline można uruchomić ręcznie lub działa on cyklicznie co godzinę.

---

## Walidacja danych

Walidacja wykonywana jest na danych Silver przy użyciu Great Expectations.
W przypadku błędu jakości danych DAG zostaje zatrzymany.

---

## Trening modelu

Model trenowany jest na danych Gold.
Metryki oraz artefakty zapisywane są w MLflow, a artefakty trafiają do MinIO.

---

## Charakter projektu

Projekt ma charakter edukacyjny i demonstracyjny.
Pokazuje pełny, spójny flow danych w architekturze typu data platform.
