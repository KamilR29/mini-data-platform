import json
import time
from datetime import datetime, timezone
from io import BytesIO

from kafka import KafkaConsumer
from minio import Minio

BOOTSTRAP = "localhost:9092"
TOPICS = ["customers"]

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio12345"
MINIO_BUCKET = "bronze"

FLUSH_EVERY = 50
FLUSH_EVERY_SECONDS = 30

def ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

def now_prefix() -> str:
    dt = datetime.now(timezone.utc)
    return dt.strftime("%Y/%m/%d/%H")

def put_jsonl(client: Minio, bucket: str, object_name: str, rows: list[dict]) -> None:
    data = ("\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n").encode("utf-8")
    bio = BytesIO(data)
    client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=bio,
        length=len(data),
        content_type="application/x-ndjson",
    )

def main():
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )
    ensure_bucket(minio_client, MINIO_BUCKET)

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="bronze-writer-v1",  # ważne: stały group id, ale przy debug możesz zmienić na nowy
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    buffers: dict[str, list[dict]] = {t: [] for t in TOPICS}
    last_flush = time.time()
    file_seq = 0

    print("Listening to Kafka topics and writing to MinIO bucket 'bronze'...")

    for msg in consumer:
        topic = msg.topic
        value = msg.value  # tu masz już dict

        # Zwykły rekord customers + metadane
        record = {
            "topic": topic,
            "kafka_ts": msg.timestamp,
            "data": value,
        }
        buffers[topic].append(record)

        due_by_count = len(buffers[topic]) >= FLUSH_EVERY
        due_by_time = (time.time() - last_flush) >= FLUSH_EVERY_SECONDS

        if due_by_count or due_by_time:
            prefix = now_prefix()
            table = topic  # "customers"
            object_name = f"{table}/{prefix}/part-{file_seq:06d}.jsonl"

            rows_to_write = buffers[topic]
            buffers[topic] = []

            put_jsonl(minio_client, MINIO_BUCKET, object_name, rows_to_write)
            print(f"Wrote {len(rows_to_write)} rows to s3://{MINIO_BUCKET}/{object_name}")

            file_seq += 1
            last_flush = time.time()

if __name__ == "__main__":
    main()
