from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


spark = SparkSession.builder \
    .appName("KafkaSparkCustomersStreaming") \
    .getOrCreate()


json_schema = StructType([
    StructField("payload", StructType([
        StructField("after", StructType([
            StructField("id", StringType()),
            StructField("name", StringType()),
            StructField("email", StringType())
        ]))
    ]))
])


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "dbserver1.public.customers") \
    .option("startingOffsets", "earliest") \
    .load()


df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), json_schema).alias("data")) \
    .select("data.payload.after.*")


query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
