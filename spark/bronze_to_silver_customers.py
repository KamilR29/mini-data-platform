from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, LongType

MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "minio"
SECRET_KEY = "minio12345"

BRONZE_PATH = "s3a://bronze/customers/"
SILVER_PATH = "s3a://silver/customers/"

def main():
    spark = (
        SparkSession.builder
        .appName("bronze-to-silver-customers")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    schema = StructType([
        StructField("topic", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("op", StringType(), True),
        StructField("data", StructType([
            StructField("customer_id", StringType(), True),
            StructField("age", StringType(), True),
            StructField("country", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("annual_income_usd", StringType(), True),
            StructField("total_orders", StringType(), True),
            StructField("avg_order_value", StringType(), True),
            StructField("churn", StringType(), True),
        ]), True),
    ])

    bronze = (
        spark.read
        .schema(schema)
        .option("recursiveFileLookup", "true")
        .json(BRONZE_PATH)
    )

    silver = (
        bronze
        .select(
            col("data.customer_id").cast("int").alias("customer_id"),
            col("data.age").cast("int").alias("age"),
            col("data.country").alias("country"),
            col("data.gender").alias("gender"),
            col("data.annual_income_usd").cast("double").alias("annual_income_usd"),
            col("data.total_orders").cast("int").alias("total_orders"),
            col("data.avg_order_value").cast("double").alias("avg_order_value"),
            col("data.churn").cast("int").alias("churn"),
            col("op").alias("op"),
            col("ts_ms").alias("ts_ms"),
        )
        .dropna(subset=["customer_id"])
        .dropDuplicates(["customer_id", "ts_ms"])
    )

    silver.write.mode("overwrite").parquet(SILVER_PATH)
    print("Saved silver customers to:", SILVER_PATH)

    spark.stop()

if __name__ == "__main__":
    main()
