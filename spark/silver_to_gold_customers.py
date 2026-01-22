from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    countDistinct,
    min as fmin,
    max as fmax,
    avg,
    sum as fsum,
    expr,
)
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType, LongType
)

MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "minio"
SECRET_KEY = "minio12345"

SILVER_PATH = "s3a://silver/customers/"
GOLD_PATH = "s3a://gold/customers_kpis/"

def main():
    spark = (
        SparkSession.builder
        .appName("silver-to-gold-customers")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    # Schema zgodny z Twoim silver
    silver_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("age", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("annual_income_usd", DoubleType(), True),
        StructField("total_orders", IntegerType(), True),
        StructField("avg_order_value", DoubleType(), True),
        StructField("churn", IntegerType(), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True),
    ])

    df = spark.read.schema(silver_schema).parquet(SILVER_PATH)

    # Podstawowe KPI + kilka sensownych pod ML / raport
    kpis = df.agg(
        count("*").alias("rows_total"),
        countDistinct("customer_id").alias("customers_distinct"),
        fsum(expr("case when churn = 1 then 1 else 0 end")).alias("churned_customers"),
        avg(expr("case when churn in (0,1) then churn else null end")).alias("churn_rate"),
        avg("age").alias("avg_age"),
        avg("annual_income_usd").alias("avg_income_usd"),
        avg("total_orders").alias("avg_total_orders"),
        avg("avg_order_value").alias("avg_order_value_mean"),
        fmin("ts_ms").alias("min_ts_ms"),
        fmax("ts_ms").alias("max_ts_ms"),
    )

    # Możesz też dopisać KPI per kraj lub płeć
    # np. df.groupBy("country").agg(count("*").alias("rows")).write...

    kpis.write.mode("overwrite").json(GOLD_PATH)

    print("Saved gold customers KPIs to:", GOLD_PATH)
    spark.stop()

if __name__ == "__main__":
    main()
